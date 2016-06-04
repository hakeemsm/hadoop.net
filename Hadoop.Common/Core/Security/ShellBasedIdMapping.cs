using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// A simple shell-based implementation of
	/// <see cref="IdMappingServiceProvider"/>
	/// 
	/// Map id to user name or group name. It does update every 15 minutes. Only a
	/// single instance of this class is expected to be on the server.
	/// The maps are incrementally updated as described below:
	/// 1. Initialize the maps as empty.
	/// 2. Incrementally update the maps
	/// - When ShellBasedIdMapping is requested for user or group name given
	/// an ID, or for ID given a user or group name, do look up in the map
	/// first, if it doesn't exist, find the corresponding entry with shell
	/// command, and insert the entry to the maps.
	/// - When group ID is requested for a given group name, and if the
	/// group name is numerical, the full group map is loaded. Because we
	/// don't have a good way to find the entry for a numerical group name,
	/// loading the full map helps to get in all entries.
	/// 3. Periodically refresh the maps for both user and group, e.g,
	/// do step 1.
	/// Note: for testing purpose, step 1 may initial the maps with full mapping
	/// when using constructor
	/// <see cref="ShellBasedIdMapping(Configuration, bool)"/>
	/// .
	/// </summary>
	public class ShellBasedIdMapping : IdMappingServiceProvider
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.ShellBasedIdMapping
			));

		private static readonly string Os = Runtime.GetProperty("os.name");

		/// <summary>Shell commands to get users and groups</summary>
		internal const string GetAllUsersCmd = "getent passwd | cut -d: -f1,3";

		internal const string GetAllGroupsCmd = "getent group | cut -d: -f1,3";

		internal const string MacGetAllUsersCmd = "dscl . -list /Users UniqueID";

		internal const string MacGetAllGroupsCmd = "dscl . -list /Groups PrimaryGroupID";

		private readonly FilePath staticMappingFile;

		private ShellBasedIdMapping.StaticMapping staticMapping = null;

		private long lastModificationTimeStaticMap = 0;

		private bool constructFullMapAtInit = false;

		private static readonly Sharpen.Pattern EmptyLine = Sharpen.Pattern.Compile("^\\s*$"
			);

		private static readonly Sharpen.Pattern CommentLine = Sharpen.Pattern.Compile("^\\s*#.*$"
			);

		private static readonly Sharpen.Pattern MappingLine = Sharpen.Pattern.Compile("^(uid|gid)\\s+(\\d+)\\s+(\\d+)\\s*(#.*)?$"
			);

		private readonly long timeout;

		private BiMap<int, string> uidNameMap = HashBiMap.Create();

		private BiMap<int, string> gidNameMap = HashBiMap.Create();

		private long lastUpdateTime = 0;

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public ShellBasedIdMapping(Configuration conf, bool constructFullMapAtInit)
		{
			// Last time the static map was modified, measured time difference in
			// milliseconds since midnight, January 1, 1970 UTC
			// Used for parsing the static mapping file.
			// Maps for id to name map. Guarded by this object monitor lock
			// Last time maps were updated
			/*
			* Constructor
			* @param conf the configuration
			* @param constructFullMapAtInit initialize the maps with full mapping when
			*        true, otherwise initialize the maps to empty. This parameter is
			*        intended for testing only, its default is false.
			*/
			this.constructFullMapAtInit = constructFullMapAtInit;
			long updateTime = conf.GetLong(IdMappingConstant.UsergroupidUpdateMillisKey, IdMappingConstant
				.UsergroupidUpdateMillisDefault);
			// Minimal interval is 1 minute
			if (updateTime < IdMappingConstant.UsergroupidUpdateMillisMin)
			{
				Log.Info("User configured user account update time is less" + " than 1 minute. Use 1 minute instead."
					);
				timeout = IdMappingConstant.UsergroupidUpdateMillisMin;
			}
			else
			{
				timeout = updateTime;
			}
			string staticFilePath = conf.Get(IdMappingConstant.StaticIdMappingFileKey, IdMappingConstant
				.StaticIdMappingFileDefault);
			staticMappingFile = new FilePath(staticFilePath);
			UpdateStaticMapping();
			UpdateMaps();
		}

		/// <exception cref="System.IO.IOException"/>
		public ShellBasedIdMapping(Configuration conf)
			: this(conf, false)
		{
		}

		/*
		* Constructor
		* initialize user and group maps to empty
		* @param conf the configuration
		*/
		[VisibleForTesting]
		public virtual long GetTimeout()
		{
			return timeout;
		}

		[VisibleForTesting]
		public virtual BiMap<int, string> GetUidNameMap()
		{
			return uidNameMap;
		}

		[VisibleForTesting]
		public virtual BiMap<int, string> GetGidNameMap()
		{
			return gidNameMap;
		}

		[VisibleForTesting]
		public virtual void ClearNameMaps()
		{
			lock (this)
			{
				uidNameMap.Clear();
				gidNameMap.Clear();
				lastUpdateTime = Time.MonotonicNow();
			}
		}

		private bool IsExpired()
		{
			lock (this)
			{
				return Time.MonotonicNow() - lastUpdateTime > timeout;
			}
		}

		// If can't update the maps, will keep using the old ones
		private void CheckAndUpdateMaps()
		{
			if (IsExpired())
			{
				Log.Info("Update cache now");
				try
				{
					UpdateMaps();
				}
				catch (IOException e)
				{
					Log.Error("Can't update the maps. Will use the old ones," + " which can potentially cause problem."
						, e);
				}
			}
		}

		private const string DuplicateNameIdDebugInfo = "NFS gateway could have problem starting with duplicate name or id on the host system.\n"
			 + "This is because HDFS (non-kerberos cluster) uses name as the only way to identify a user or group.\n"
			 + "The host system with duplicated user/group name or id might work fine most of the time by itself.\n"
			 + "However when NFS gateway talks to HDFS, HDFS accepts only user and group name.\n"
			 + "Therefore, same name means the same user or same group. To find the duplicated names/ids, one can do:\n"
			 + "<getent passwd | cut -d: -f1,3> and <getent group | cut -d: -f1,3> on Linux systems,\n"
			 + "<dscl . -list /Users UniqueID> and <dscl . -list /Groups PrimaryGroupID> on MacOS.";

		private static void ReportDuplicateEntry(string header, int key, string value, int
			 ekey, string evalue)
		{
			Log.Warn("\n" + header + string.Format("new entry (%d, %s), existing entry: (%d, %s).%n%s%n%s"
				, key, value, ekey, evalue, "The new entry is to be ignored for the following reason."
				, DuplicateNameIdDebugInfo));
		}

		/// <summary>uid and gid are defined as uint32 in linux.</summary>
		/// <remarks>
		/// uid and gid are defined as uint32 in linux. Some systems create
		/// (intended or unintended) <nfsnobody, 4294967294> kind of <name,Id>
		/// mapping, where 4294967294 is 2**32-2 as unsigned int32. As an example,
		/// https://bugzilla.redhat.com/show_bug.cgi?id=511876.
		/// Because user or group id are treated as Integer (signed integer or int32)
		/// here, the number 4294967294 is out of range. The solution is to convert
		/// uint32 to int32, so to map the out-of-range ID to the negative side of
		/// Integer, e.g. 4294967294 maps to -2 and 4294967295 maps to -1.
		/// </remarks>
		private static int ParseId(string idStr)
		{
			long longVal = long.Parse(idStr);
			int intVal = longVal;
			return Sharpen.Extensions.ValueOf(intVal);
		}

		/// <summary>
		/// Get the list of users or groups returned by the specified command,
		/// and save them in the corresponding map.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		[VisibleForTesting]
		public static bool UpdateMapInternal(BiMap<int, string> map, string mapName, string
			 command, string regex, IDictionary<int, int> staticMapping)
		{
			bool updated = false;
			BufferedReader br = null;
			try
			{
				SystemProcess process = Runtime.GetRuntime().Exec(new string[] { "bash", "-c", command
					 });
				br = new BufferedReader(new InputStreamReader(process.GetInputStream(), Encoding.
					Default));
				string line = null;
				while ((line = br.ReadLine()) != null)
				{
					string[] nameId = line.Split(regex);
					if ((nameId == null) || (nameId.Length != 2))
					{
						throw new IOException("Can't parse " + mapName + " list entry:" + line);
					}
					Log.Debug("add to " + mapName + "map:" + nameId[0] + " id:" + nameId[1]);
					// HDFS can't differentiate duplicate names with simple authentication
					int key = staticMapping[ParseId(nameId[1])];
					string value = nameId[0];
					if (map.Contains(key))
					{
						string prevValue = map[key];
						if (value.Equals(prevValue))
						{
							// silently ignore equivalent entries
							continue;
						}
						ReportDuplicateEntry("Got multiple names associated with the same id: ", key, value
							, key, prevValue);
						continue;
					}
					if (map.ContainsValue(value))
					{
						int prevKey = map.Inverse()[value];
						ReportDuplicateEntry("Got multiple ids associated with the same name: ", key, value
							, prevKey, value);
						continue;
					}
					map[key] = value;
					updated = true;
				}
				Log.Debug("Updated " + mapName + " map size: " + map.Count);
			}
			catch (IOException e)
			{
				Log.Error("Can't update " + mapName + " map");
				throw;
			}
			finally
			{
				if (br != null)
				{
					try
					{
						br.Close();
					}
					catch (IOException e1)
					{
						Log.Error("Can't close BufferedReader of command result", e1);
					}
				}
			}
			return updated;
		}

		private bool CheckSupportedPlatform()
		{
			if (!Os.StartsWith("Linux") && !Os.StartsWith("Mac"))
			{
				Log.Error("Platform is not supported:" + Os + ". Can't update user map and group map and"
					 + " 'nobody' will be used for any user and group.");
				return false;
			}
			return true;
		}

		private static bool IsInteger(string s)
		{
			try
			{
				System.Convert.ToInt32(s);
			}
			catch (FormatException)
			{
				return false;
			}
			// only got here if we didn't return false
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		private void UpdateStaticMapping()
		{
			lock (this)
			{
				bool init = (staticMapping == null);
				//
				// if the static mapping file
				//   - was modified after last update, load the map again;
				//   - did not exist but was added since last update, load the map;
				//   - existed before but deleted since last update, clear the map
				//
				if (staticMappingFile.Exists())
				{
					// check modification time, reload the file if the last modification
					// time changed since prior load.
					long lmTime = staticMappingFile.LastModified();
					if (lmTime != lastModificationTimeStaticMap)
					{
						Log.Info(init ? "Using " : "Reloading " + "'" + staticMappingFile + "' for static UID/GID mapping..."
							);
						lastModificationTimeStaticMap = lmTime;
						staticMapping = ParseStaticMap(staticMappingFile);
					}
				}
				else
				{
					if (init)
					{
						staticMapping = new ShellBasedIdMapping.StaticMapping(new Dictionary<int, int>(), 
							new Dictionary<int, int>());
					}
					if (lastModificationTimeStaticMap != 0 || init)
					{
						// print the following log at initialization or when the static
						// mapping file was deleted after prior load
						Log.Info("Not doing static UID/GID mapping because '" + staticMappingFile + "' does not exist."
							);
					}
					lastModificationTimeStaticMap = 0;
					staticMapping.Clear();
				}
			}
		}

		/*
		* Refresh static map, and reset the other maps to empty.
		* For testing code, a full map may be re-constructed here when the object
		* was created with constructFullMapAtInit being set to true.
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void UpdateMaps()
		{
			lock (this)
			{
				if (!CheckSupportedPlatform())
				{
					return;
				}
				if (constructFullMapAtInit)
				{
					LoadFullMaps();
					// set constructFullMapAtInit to false to allow testing code to
					// do incremental update to maps after initial construction
					constructFullMapAtInit = false;
				}
				else
				{
					UpdateStaticMapping();
					ClearNameMaps();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadFullUserMap()
		{
			lock (this)
			{
				BiMap<int, string> uMap = HashBiMap.Create();
				if (Os.StartsWith("Mac"))
				{
					UpdateMapInternal(uMap, "user", MacGetAllUsersCmd, "\\s+", staticMapping.uidMapping
						);
				}
				else
				{
					UpdateMapInternal(uMap, "user", GetAllUsersCmd, ":", staticMapping.uidMapping);
				}
				uidNameMap = uMap;
				lastUpdateTime = Time.MonotonicNow();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadFullGroupMap()
		{
			lock (this)
			{
				BiMap<int, string> gMap = HashBiMap.Create();
				if (Os.StartsWith("Mac"))
				{
					UpdateMapInternal(gMap, "group", MacGetAllGroupsCmd, "\\s+", staticMapping.gidMapping
						);
				}
				else
				{
					UpdateMapInternal(gMap, "group", GetAllGroupsCmd, ":", staticMapping.gidMapping);
				}
				gidNameMap = gMap;
				lastUpdateTime = Time.MonotonicNow();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadFullMaps()
		{
			lock (this)
			{
				LoadFullUserMap();
				LoadFullGroupMap();
			}
		}

		// search for id with given name, return "<name>:<id>"
		// return
		//     getent group <name> | cut -d: -f1,3
		// OR
		//     id -u <name> | awk '{print "<name>:"$1 }'
		//
		private string GetName2IdCmdLinux(string name, bool isGrp)
		{
			string cmd;
			if (isGrp)
			{
				cmd = "getent group " + name + " | cut -d: -f1,3";
			}
			else
			{
				cmd = "id -u " + name + " | awk '{print \"" + name + ":\"$1 }'";
			}
			return cmd;
		}

		// search for name with given id, return "<name>:<id>"
		private string GetId2NameCmdLinux(int id, bool isGrp)
		{
			string cmd = "getent ";
			cmd += isGrp ? "group " : "passwd ";
			cmd += id.ToString() + " | cut -d: -f1,3";
			return cmd;
		}

		// "dscl . -read /Users/<name> | grep UniqueID" returns "UniqueId: <id>",
		// "dscl . -read /Groups/<name> | grep PrimaryGroupID" returns "PrimaryGoupID: <id>"
		// The following method returns a command that uses awk to process the result,
		// of these commands, and returns "<name> <id>", to simulate one entry returned by 
		// MAC_GET_ALL_USERS_CMD or MAC_GET_ALL_GROUPS_CMD.
		// Specificially, this method returns:
		// id -u <name> | awk '{print "<name>:"$1 }'
		// OR
		// dscl . -read /Groups/<name> | grep PrimaryGroupID | awk '($1 == "PrimaryGroupID:") { print "<name> " $2 }'
		//
		private string GetName2IdCmdMac(string name, bool isGrp)
		{
			string cmd;
			if (isGrp)
			{
				cmd = "dscl . -read /Groups/" + name;
				cmd += " | grep PrimaryGroupID | awk '($1 == \"PrimaryGroupID:\") ";
				cmd += "{ print \"" + name + "  \" $2 }'";
			}
			else
			{
				cmd = "id -u " + name + " | awk '{print \"" + name + "  \"$1 }'";
			}
			return cmd;
		}

		// "dscl . -search /Users UniqueID <id>" returns 
		//    <name> UniqueID = (
		//      <id>
		//    )
		// "dscl . -search /Groups PrimaryGroupID <id>" returns
		//    <name> PrimaryGroupID = (
		//      <id>
		//    )
		// The following method returns a command that uses sed to process the
		// the result and returns "<name> <id>" to simulate one entry returned
		// by MAC_GET_ALL_USERS_CMD or MAC_GET_ALL_GROUPS_CMD.
		// For certain negative id case like nfsnobody, the <id> is quoted as
		// "<id>", added one sed section to remove the quote.
		// Specifically, the method returns:
		// dscl . -search /Users UniqueID <id> | sed 'N;s/\\n//g;N;s/\\n//g' | sed 's/UniqueID =//g' | sed 's/)//g' | sed 's/\"//g'
		// OR
		// dscl . -search /Groups PrimaryGroupID <id> | sed 'N;s/\\n//g;N;s/\\n//g' | sed 's/PrimaryGroupID =//g' | sed 's/)//g' | sed 's/\"//g'
		//
		private string GetId2NameCmdMac(int id, bool isGrp)
		{
			string cmd = "dscl . -search /";
			cmd += isGrp ? "Groups PrimaryGroupID " : "Users UniqueID ";
			cmd += id.ToString();
			cmd += " | sed 'N;s/\\n//g;N;s/\\n//g' | sed 's/";
			cmd += isGrp ? "PrimaryGroupID" : "UniqueID";
			cmd += " = (//g' | sed 's/)//g' | sed 's/\\\"//g'";
			return cmd;
		}

		/// <exception cref="System.IO.IOException"/>
		private void UpdateMapIncr(string name, bool isGrp)
		{
			lock (this)
			{
				if (!CheckSupportedPlatform())
				{
					return;
				}
				if (IsInteger(name) && isGrp)
				{
					LoadFullGroupMap();
					return;
				}
				bool updated = false;
				UpdateStaticMapping();
				if (Os.StartsWith("Linux"))
				{
					if (isGrp)
					{
						updated = UpdateMapInternal(gidNameMap, "group", GetName2IdCmdLinux(name, true), 
							":", staticMapping.gidMapping);
					}
					else
					{
						updated = UpdateMapInternal(uidNameMap, "user", GetName2IdCmdLinux(name, false), 
							":", staticMapping.uidMapping);
					}
				}
				else
				{
					// Mac
					if (isGrp)
					{
						updated = UpdateMapInternal(gidNameMap, "group", GetName2IdCmdMac(name, true), "\\s+"
							, staticMapping.gidMapping);
					}
					else
					{
						updated = UpdateMapInternal(uidNameMap, "user", GetName2IdCmdMac(name, false), "\\s+"
							, staticMapping.uidMapping);
					}
				}
				if (updated)
				{
					lastUpdateTime = Time.MonotonicNow();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void UpdateMapIncr(int id, bool isGrp)
		{
			lock (this)
			{
				if (!CheckSupportedPlatform())
				{
					return;
				}
				bool updated = false;
				UpdateStaticMapping();
				if (Os.StartsWith("Linux"))
				{
					if (isGrp)
					{
						updated = UpdateMapInternal(gidNameMap, "group", GetId2NameCmdLinux(id, true), ":"
							, staticMapping.gidMapping);
					}
					else
					{
						updated = UpdateMapInternal(uidNameMap, "user", GetId2NameCmdLinux(id, false), ":"
							, staticMapping.uidMapping);
					}
				}
				else
				{
					// Mac
					if (isGrp)
					{
						updated = UpdateMapInternal(gidNameMap, "group", GetId2NameCmdMac(id, true), "\\s+"
							, staticMapping.gidMapping);
					}
					else
					{
						updated = UpdateMapInternal(uidNameMap, "user", GetId2NameCmdMac(id, false), "\\s+"
							, staticMapping.uidMapping);
					}
				}
				if (updated)
				{
					lastUpdateTime = Time.MonotonicNow();
				}
			}
		}

		[System.Serializable]
		internal sealed class PassThroughMap<K> : Dictionary<K, K>
		{
			public PassThroughMap()
				: this(new Dictionary<K, K>())
			{
			}

			public PassThroughMap(IDictionary<K, K> mapping)
				: base()
			{
				foreach (KeyValuePair<K, K> entry in mapping)
				{
					base.Put;
				}
			}

			public override K Get(object key)
			{
				if (base.Contains(key))
				{
					return base.Get;
				}
				else
				{
					return (K)key;
				}
			}
		}

		internal sealed class StaticMapping
		{
			internal readonly IDictionary<int, int> uidMapping;

			internal readonly IDictionary<int, int> gidMapping;

			public StaticMapping(IDictionary<int, int> uidMapping, IDictionary<int, int> gidMapping
				)
			{
				this.uidMapping = new ShellBasedIdMapping.PassThroughMap<int>(uidMapping);
				this.gidMapping = new ShellBasedIdMapping.PassThroughMap<int>(gidMapping);
			}

			public void Clear()
			{
				uidMapping.Clear();
				gidMapping.Clear();
			}

			public bool IsNonEmpty()
			{
				return uidMapping.Count > 0 || gidMapping.Count > 0;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static ShellBasedIdMapping.StaticMapping ParseStaticMap(FilePath staticMapFile
			)
		{
			IDictionary<int, int> uidMapping = new Dictionary<int, int>();
			IDictionary<int, int> gidMapping = new Dictionary<int, int>();
			BufferedReader @in = new BufferedReader(new InputStreamReader(new FileInputStream
				(staticMapFile), Charsets.Utf8));
			try
			{
				string line = null;
				while ((line = @in.ReadLine()) != null)
				{
					// Skip entirely empty and comment lines.
					if (EmptyLine.Matcher(line).Matches() || CommentLine.Matcher(line).Matches())
					{
						continue;
					}
					Matcher lineMatcher = MappingLine.Matcher(line);
					if (!lineMatcher.Matches())
					{
						Log.Warn("Could not parse line '" + line + "'. Lines should be of " + "the form '[uid|gid] [remote id] [local id]'. Blank lines and "
							 + "everything following a '#' on a line will be ignored.");
						continue;
					}
					// We know the line is fine to parse without error checking like this
					// since it matched the regex above.
					string firstComponent = lineMatcher.Group(1);
					int remoteId = ParseId(lineMatcher.Group(2));
					int localId = ParseId(lineMatcher.Group(3));
					if (firstComponent.Equals("uid"))
					{
						uidMapping[localId] = remoteId;
					}
					else
					{
						gidMapping[localId] = remoteId;
					}
				}
			}
			finally
			{
				@in.Close();
			}
			return new ShellBasedIdMapping.StaticMapping(uidMapping, gidMapping);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int GetUid(string user)
		{
			lock (this)
			{
				CheckAndUpdateMaps();
				int id = uidNameMap.Inverse()[user];
				if (id == null)
				{
					UpdateMapIncr(user, false);
					id = uidNameMap.Inverse()[user];
					if (id == null)
					{
						throw new IOException("User just deleted?:" + user);
					}
				}
				return id;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int GetGid(string group)
		{
			lock (this)
			{
				CheckAndUpdateMaps();
				int id = gidNameMap.Inverse()[group];
				if (id == null)
				{
					UpdateMapIncr(group, true);
					id = gidNameMap.Inverse()[group];
					if (id == null)
					{
						throw new IOException("No such group:" + group);
					}
				}
				return id;
			}
		}

		public virtual string GetUserName(int uid, string unknown)
		{
			lock (this)
			{
				CheckAndUpdateMaps();
				string uname = uidNameMap[uid];
				if (uname == null)
				{
					try
					{
						UpdateMapIncr(uid, false);
					}
					catch (Exception)
					{
					}
					uname = uidNameMap[uid];
					if (uname == null)
					{
						Log.Warn("Can't find user name for uid " + uid + ". Use default user name " + unknown
							);
						uname = unknown;
					}
				}
				return uname;
			}
		}

		public virtual string GetGroupName(int gid, string unknown)
		{
			lock (this)
			{
				CheckAndUpdateMaps();
				string gname = gidNameMap[gid];
				if (gname == null)
				{
					try
					{
						UpdateMapIncr(gid, true);
					}
					catch (Exception)
					{
					}
					gname = gidNameMap[gid];
					if (gname == null)
					{
						Log.Warn("Can't find group name for gid " + gid + ". Use default group name " + unknown
							);
						gname = unknown;
					}
				}
				return gname;
			}
		}

		// When can't map user, return user name's string hashcode
		public virtual int GetUidAllowingUnknown(string user)
		{
			CheckAndUpdateMaps();
			int uid;
			try
			{
				uid = GetUid(user);
			}
			catch (IOException)
			{
				uid = user.GetHashCode();
				Log.Info("Can't map user " + user + ". Use its string hashcode:" + uid);
			}
			return uid;
		}

		// When can't map group, return group name's string hashcode
		public virtual int GetGidAllowingUnknown(string group)
		{
			CheckAndUpdateMaps();
			int gid;
			try
			{
				gid = GetGid(group);
			}
			catch (IOException)
			{
				gid = group.GetHashCode();
				Log.Info("Can't map group " + group + ". Use its string hashcode:" + gid);
			}
			return gid;
		}
	}
}
