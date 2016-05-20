using Sharpen;

namespace org.apache.hadoop.security
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
	/// <see cref="ShellBasedIdMapping(org.apache.hadoop.conf.Configuration, bool)"/>
	/// .
	/// </summary>
	public class ShellBasedIdMapping : org.apache.hadoop.security.IdMappingServiceProvider
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.ShellBasedIdMapping
			)));

		private static readonly string OS = Sharpen.Runtime.getProperty("os.name");

		/// <summary>Shell commands to get users and groups</summary>
		internal const string GET_ALL_USERS_CMD = "getent passwd | cut -d: -f1,3";

		internal const string GET_ALL_GROUPS_CMD = "getent group | cut -d: -f1,3";

		internal const string MAC_GET_ALL_USERS_CMD = "dscl . -list /Users UniqueID";

		internal const string MAC_GET_ALL_GROUPS_CMD = "dscl . -list /Groups PrimaryGroupID";

		private readonly java.io.File staticMappingFile;

		private org.apache.hadoop.security.ShellBasedIdMapping.StaticMapping staticMapping
			 = null;

		private long lastModificationTimeStaticMap = 0;

		private bool constructFullMapAtInit = false;

		private static readonly java.util.regex.Pattern EMPTY_LINE = java.util.regex.Pattern
			.compile("^\\s*$");

		private static readonly java.util.regex.Pattern COMMENT_LINE = java.util.regex.Pattern
			.compile("^\\s*#.*$");

		private static readonly java.util.regex.Pattern MAPPING_LINE = java.util.regex.Pattern
			.compile("^(uid|gid)\\s+(\\d+)\\s+(\\d+)\\s*(#.*)?$");

		private readonly long timeout;

		private com.google.common.collect.BiMap<int, string> uidNameMap = com.google.common.collect.HashBiMap
			.create();

		private com.google.common.collect.BiMap<int, string> gidNameMap = com.google.common.collect.HashBiMap
			.create();

		private long lastUpdateTime = 0;

		/// <exception cref="System.IO.IOException"/>
		[com.google.common.annotations.VisibleForTesting]
		public ShellBasedIdMapping(org.apache.hadoop.conf.Configuration conf, bool constructFullMapAtInit
			)
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
			long updateTime = conf.getLong(org.apache.hadoop.security.IdMappingConstant.USERGROUPID_UPDATE_MILLIS_KEY
				, org.apache.hadoop.security.IdMappingConstant.USERGROUPID_UPDATE_MILLIS_DEFAULT
				);
			// Minimal interval is 1 minute
			if (updateTime < org.apache.hadoop.security.IdMappingConstant.USERGROUPID_UPDATE_MILLIS_MIN)
			{
				LOG.info("User configured user account update time is less" + " than 1 minute. Use 1 minute instead."
					);
				timeout = org.apache.hadoop.security.IdMappingConstant.USERGROUPID_UPDATE_MILLIS_MIN;
			}
			else
			{
				timeout = updateTime;
			}
			string staticFilePath = conf.get(org.apache.hadoop.security.IdMappingConstant.STATIC_ID_MAPPING_FILE_KEY
				, org.apache.hadoop.security.IdMappingConstant.STATIC_ID_MAPPING_FILE_DEFAULT);
			staticMappingFile = new java.io.File(staticFilePath);
			updateStaticMapping();
			updateMaps();
		}

		/// <exception cref="System.IO.IOException"/>
		public ShellBasedIdMapping(org.apache.hadoop.conf.Configuration conf)
			: this(conf, false)
		{
		}

		/*
		* Constructor
		* initialize user and group maps to empty
		* @param conf the configuration
		*/
		[com.google.common.annotations.VisibleForTesting]
		public virtual long getTimeout()
		{
			return timeout;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual com.google.common.collect.BiMap<int, string> getUidNameMap()
		{
			return uidNameMap;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual com.google.common.collect.BiMap<int, string> getGidNameMap()
		{
			return gidNameMap;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual void clearNameMaps()
		{
			lock (this)
			{
				uidNameMap.clear();
				gidNameMap.clear();
				lastUpdateTime = org.apache.hadoop.util.Time.monotonicNow();
			}
		}

		private bool isExpired()
		{
			lock (this)
			{
				return org.apache.hadoop.util.Time.monotonicNow() - lastUpdateTime > timeout;
			}
		}

		// If can't update the maps, will keep using the old ones
		private void checkAndUpdateMaps()
		{
			if (isExpired())
			{
				LOG.info("Update cache now");
				try
				{
					updateMaps();
				}
				catch (System.IO.IOException e)
				{
					LOG.error("Can't update the maps. Will use the old ones," + " which can potentially cause problem."
						, e);
				}
			}
		}

		private const string DUPLICATE_NAME_ID_DEBUG_INFO = "NFS gateway could have problem starting with duplicate name or id on the host system.\n"
			 + "This is because HDFS (non-kerberos cluster) uses name as the only way to identify a user or group.\n"
			 + "The host system with duplicated user/group name or id might work fine most of the time by itself.\n"
			 + "However when NFS gateway talks to HDFS, HDFS accepts only user and group name.\n"
			 + "Therefore, same name means the same user or same group. To find the duplicated names/ids, one can do:\n"
			 + "<getent passwd | cut -d: -f1,3> and <getent group | cut -d: -f1,3> on Linux systems,\n"
			 + "<dscl . -list /Users UniqueID> and <dscl . -list /Groups PrimaryGroupID> on MacOS.";

		private static void reportDuplicateEntry(string header, int key, string value, int
			 ekey, string evalue)
		{
			LOG.warn("\n" + header + string.format("new entry (%d, %s), existing entry: (%d, %s).%n%s%n%s"
				, key, value, ekey, evalue, "The new entry is to be ignored for the following reason."
				, DUPLICATE_NAME_ID_DEBUG_INFO));
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
		private static int parseId(string idStr)
		{
			long longVal = long.Parse(idStr);
			int intVal = longVal;
			return int.Parse(intVal);
		}

		/// <summary>
		/// Get the list of users or groups returned by the specified command,
		/// and save them in the corresponding map.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		[com.google.common.annotations.VisibleForTesting]
		public static bool updateMapInternal(com.google.common.collect.BiMap<int, string>
			 map, string mapName, string command, string regex, System.Collections.Generic.IDictionary
			<int, int> staticMapping)
		{
			bool updated = false;
			java.io.BufferedReader br = null;
			try
			{
				java.lang.Process process = java.lang.Runtime.getRuntime().exec(new string[] { "bash"
					, "-c", command });
				br = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream
					(), java.nio.charset.Charset.defaultCharset()));
				string line = null;
				while ((line = br.readLine()) != null)
				{
					string[] nameId = line.split(regex);
					if ((nameId == null) || (nameId.Length != 2))
					{
						throw new System.IO.IOException("Can't parse " + mapName + " list entry:" + line);
					}
					LOG.debug("add to " + mapName + "map:" + nameId[0] + " id:" + nameId[1]);
					// HDFS can't differentiate duplicate names with simple authentication
					int key = staticMapping[parseId(nameId[1])];
					string value = nameId[0];
					if (map.Contains(key))
					{
						string prevValue = map[key];
						if (value.Equals(prevValue))
						{
							// silently ignore equivalent entries
							continue;
						}
						reportDuplicateEntry("Got multiple names associated with the same id: ", key, value
							, key, prevValue);
						continue;
					}
					if (map.containsValue(value))
					{
						int prevKey = map.inverse()[value];
						reportDuplicateEntry("Got multiple ids associated with the same name: ", key, value
							, prevKey, value);
						continue;
					}
					map[key] = value;
					updated = true;
				}
				LOG.debug("Updated " + mapName + " map size: " + map.Count);
			}
			catch (System.IO.IOException e)
			{
				LOG.error("Can't update " + mapName + " map");
				throw;
			}
			finally
			{
				if (br != null)
				{
					try
					{
						br.close();
					}
					catch (System.IO.IOException e1)
					{
						LOG.error("Can't close BufferedReader of command result", e1);
					}
				}
			}
			return updated;
		}

		private bool checkSupportedPlatform()
		{
			if (!OS.StartsWith("Linux") && !OS.StartsWith("Mac"))
			{
				LOG.error("Platform is not supported:" + OS + ". Can't update user map and group map and"
					 + " 'nobody' will be used for any user and group.");
				return false;
			}
			return true;
		}

		private static bool isInteger(string s)
		{
			try
			{
				System.Convert.ToInt32(s);
			}
			catch (java.lang.NumberFormatException)
			{
				return false;
			}
			// only got here if we didn't return false
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		private void updateStaticMapping()
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
				if (staticMappingFile.exists())
				{
					// check modification time, reload the file if the last modification
					// time changed since prior load.
					long lmTime = staticMappingFile.lastModified();
					if (lmTime != lastModificationTimeStaticMap)
					{
						LOG.info(init ? "Using " : "Reloading " + "'" + staticMappingFile + "' for static UID/GID mapping..."
							);
						lastModificationTimeStaticMap = lmTime;
						staticMapping = parseStaticMap(staticMappingFile);
					}
				}
				else
				{
					if (init)
					{
						staticMapping = new org.apache.hadoop.security.ShellBasedIdMapping.StaticMapping(
							new System.Collections.Generic.Dictionary<int, int>(), new System.Collections.Generic.Dictionary
							<int, int>());
					}
					if (lastModificationTimeStaticMap != 0 || init)
					{
						// print the following log at initialization or when the static
						// mapping file was deleted after prior load
						LOG.info("Not doing static UID/GID mapping because '" + staticMappingFile + "' does not exist."
							);
					}
					lastModificationTimeStaticMap = 0;
					staticMapping.clear();
				}
			}
		}

		/*
		* Refresh static map, and reset the other maps to empty.
		* For testing code, a full map may be re-constructed here when the object
		* was created with constructFullMapAtInit being set to true.
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void updateMaps()
		{
			lock (this)
			{
				if (!checkSupportedPlatform())
				{
					return;
				}
				if (constructFullMapAtInit)
				{
					loadFullMaps();
					// set constructFullMapAtInit to false to allow testing code to
					// do incremental update to maps after initial construction
					constructFullMapAtInit = false;
				}
				else
				{
					updateStaticMapping();
					clearNameMaps();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void loadFullUserMap()
		{
			lock (this)
			{
				com.google.common.collect.BiMap<int, string> uMap = com.google.common.collect.HashBiMap
					.create();
				if (OS.StartsWith("Mac"))
				{
					updateMapInternal(uMap, "user", MAC_GET_ALL_USERS_CMD, "\\s+", staticMapping.uidMapping
						);
				}
				else
				{
					updateMapInternal(uMap, "user", GET_ALL_USERS_CMD, ":", staticMapping.uidMapping);
				}
				uidNameMap = uMap;
				lastUpdateTime = org.apache.hadoop.util.Time.monotonicNow();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void loadFullGroupMap()
		{
			lock (this)
			{
				com.google.common.collect.BiMap<int, string> gMap = com.google.common.collect.HashBiMap
					.create();
				if (OS.StartsWith("Mac"))
				{
					updateMapInternal(gMap, "group", MAC_GET_ALL_GROUPS_CMD, "\\s+", staticMapping.gidMapping
						);
				}
				else
				{
					updateMapInternal(gMap, "group", GET_ALL_GROUPS_CMD, ":", staticMapping.gidMapping
						);
				}
				gidNameMap = gMap;
				lastUpdateTime = org.apache.hadoop.util.Time.monotonicNow();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void loadFullMaps()
		{
			lock (this)
			{
				loadFullUserMap();
				loadFullGroupMap();
			}
		}

		// search for id with given name, return "<name>:<id>"
		// return
		//     getent group <name> | cut -d: -f1,3
		// OR
		//     id -u <name> | awk '{print "<name>:"$1 }'
		//
		private string getName2IdCmdLinux(string name, bool isGrp)
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
		private string getId2NameCmdLinux(int id, bool isGrp)
		{
			string cmd = "getent ";
			cmd += isGrp ? "group " : "passwd ";
			cmd += Sharpen.Runtime.getStringValueOf(id) + " | cut -d: -f1,3";
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
		private string getName2IdCmdMac(string name, bool isGrp)
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
		private string getId2NameCmdMac(int id, bool isGrp)
		{
			string cmd = "dscl . -search /";
			cmd += isGrp ? "Groups PrimaryGroupID " : "Users UniqueID ";
			cmd += Sharpen.Runtime.getStringValueOf(id);
			cmd += " | sed 'N;s/\\n//g;N;s/\\n//g' | sed 's/";
			cmd += isGrp ? "PrimaryGroupID" : "UniqueID";
			cmd += " = (//g' | sed 's/)//g' | sed 's/\\\"//g'";
			return cmd;
		}

		/// <exception cref="System.IO.IOException"/>
		private void updateMapIncr(string name, bool isGrp)
		{
			lock (this)
			{
				if (!checkSupportedPlatform())
				{
					return;
				}
				if (isInteger(name) && isGrp)
				{
					loadFullGroupMap();
					return;
				}
				bool updated = false;
				updateStaticMapping();
				if (OS.StartsWith("Linux"))
				{
					if (isGrp)
					{
						updated = updateMapInternal(gidNameMap, "group", getName2IdCmdLinux(name, true), 
							":", staticMapping.gidMapping);
					}
					else
					{
						updated = updateMapInternal(uidNameMap, "user", getName2IdCmdLinux(name, false), 
							":", staticMapping.uidMapping);
					}
				}
				else
				{
					// Mac
					if (isGrp)
					{
						updated = updateMapInternal(gidNameMap, "group", getName2IdCmdMac(name, true), "\\s+"
							, staticMapping.gidMapping);
					}
					else
					{
						updated = updateMapInternal(uidNameMap, "user", getName2IdCmdMac(name, false), "\\s+"
							, staticMapping.uidMapping);
					}
				}
				if (updated)
				{
					lastUpdateTime = org.apache.hadoop.util.Time.monotonicNow();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void updateMapIncr(int id, bool isGrp)
		{
			lock (this)
			{
				if (!checkSupportedPlatform())
				{
					return;
				}
				bool updated = false;
				updateStaticMapping();
				if (OS.StartsWith("Linux"))
				{
					if (isGrp)
					{
						updated = updateMapInternal(gidNameMap, "group", getId2NameCmdLinux(id, true), ":"
							, staticMapping.gidMapping);
					}
					else
					{
						updated = updateMapInternal(uidNameMap, "user", getId2NameCmdLinux(id, false), ":"
							, staticMapping.uidMapping);
					}
				}
				else
				{
					// Mac
					if (isGrp)
					{
						updated = updateMapInternal(gidNameMap, "group", getId2NameCmdMac(id, true), "\\s+"
							, staticMapping.gidMapping);
					}
					else
					{
						updated = updateMapInternal(uidNameMap, "user", getId2NameCmdMac(id, false), "\\s+"
							, staticMapping.uidMapping);
					}
				}
				if (updated)
				{
					lastUpdateTime = org.apache.hadoop.util.Time.monotonicNow();
				}
			}
		}

		[System.Serializable]
		internal sealed class PassThroughMap<K> : System.Collections.Generic.Dictionary<K
			, K>
		{
			public PassThroughMap()
				: this(new System.Collections.Generic.Dictionary<K, K>())
			{
			}

			public PassThroughMap(System.Collections.Generic.IDictionary<K, K> mapping)
				: base()
			{
				foreach (System.Collections.Generic.KeyValuePair<K, K> entry in mapping)
				{
					base.put;
				}
			}

			public override K get(object key)
			{
				if (base.Contains(key))
				{
					return base.get;
				}
				else
				{
					return (K)key;
				}
			}
		}

		internal sealed class StaticMapping
		{
			internal readonly System.Collections.Generic.IDictionary<int, int> uidMapping;

			internal readonly System.Collections.Generic.IDictionary<int, int> gidMapping;

			public StaticMapping(System.Collections.Generic.IDictionary<int, int> uidMapping, 
				System.Collections.Generic.IDictionary<int, int> gidMapping)
			{
				this.uidMapping = new org.apache.hadoop.security.ShellBasedIdMapping.PassThroughMap
					<int>(uidMapping);
				this.gidMapping = new org.apache.hadoop.security.ShellBasedIdMapping.PassThroughMap
					<int>(gidMapping);
			}

			public void clear()
			{
				uidMapping.clear();
				gidMapping.clear();
			}

			public bool isNonEmpty()
			{
				return uidMapping.Count > 0 || gidMapping.Count > 0;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static org.apache.hadoop.security.ShellBasedIdMapping.StaticMapping parseStaticMap
			(java.io.File staticMapFile)
		{
			System.Collections.Generic.IDictionary<int, int> uidMapping = new System.Collections.Generic.Dictionary
				<int, int>();
			System.Collections.Generic.IDictionary<int, int> gidMapping = new System.Collections.Generic.Dictionary
				<int, int>();
			java.io.BufferedReader @in = new java.io.BufferedReader(new java.io.InputStreamReader
				(new java.io.FileInputStream(staticMapFile), org.apache.commons.io.Charsets.UTF_8
				));
			try
			{
				string line = null;
				while ((line = @in.readLine()) != null)
				{
					// Skip entirely empty and comment lines.
					if (EMPTY_LINE.matcher(line).matches() || COMMENT_LINE.matcher(line).matches())
					{
						continue;
					}
					java.util.regex.Matcher lineMatcher = MAPPING_LINE.matcher(line);
					if (!lineMatcher.matches())
					{
						LOG.warn("Could not parse line '" + line + "'. Lines should be of " + "the form '[uid|gid] [remote id] [local id]'. Blank lines and "
							 + "everything following a '#' on a line will be ignored.");
						continue;
					}
					// We know the line is fine to parse without error checking like this
					// since it matched the regex above.
					string firstComponent = lineMatcher.group(1);
					int remoteId = parseId(lineMatcher.group(2));
					int localId = parseId(lineMatcher.group(3));
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
				@in.close();
			}
			return new org.apache.hadoop.security.ShellBasedIdMapping.StaticMapping(uidMapping
				, gidMapping);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int getUid(string user)
		{
			lock (this)
			{
				checkAndUpdateMaps();
				int id = uidNameMap.inverse()[user];
				if (id == null)
				{
					updateMapIncr(user, false);
					id = uidNameMap.inverse()[user];
					if (id == null)
					{
						throw new System.IO.IOException("User just deleted?:" + user);
					}
				}
				return id;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int getGid(string group)
		{
			lock (this)
			{
				checkAndUpdateMaps();
				int id = gidNameMap.inverse()[group];
				if (id == null)
				{
					updateMapIncr(group, true);
					id = gidNameMap.inverse()[group];
					if (id == null)
					{
						throw new System.IO.IOException("No such group:" + group);
					}
				}
				return id;
			}
		}

		public virtual string getUserName(int uid, string unknown)
		{
			lock (this)
			{
				checkAndUpdateMaps();
				string uname = uidNameMap[uid];
				if (uname == null)
				{
					try
					{
						updateMapIncr(uid, false);
					}
					catch (System.Exception)
					{
					}
					uname = uidNameMap[uid];
					if (uname == null)
					{
						LOG.warn("Can't find user name for uid " + uid + ". Use default user name " + unknown
							);
						uname = unknown;
					}
				}
				return uname;
			}
		}

		public virtual string getGroupName(int gid, string unknown)
		{
			lock (this)
			{
				checkAndUpdateMaps();
				string gname = gidNameMap[gid];
				if (gname == null)
				{
					try
					{
						updateMapIncr(gid, true);
					}
					catch (System.Exception)
					{
					}
					gname = gidNameMap[gid];
					if (gname == null)
					{
						LOG.warn("Can't find group name for gid " + gid + ". Use default group name " + unknown
							);
						gname = unknown;
					}
				}
				return gname;
			}
		}

		// When can't map user, return user name's string hashcode
		public virtual int getUidAllowingUnknown(string user)
		{
			checkAndUpdateMaps();
			int uid;
			try
			{
				uid = getUid(user);
			}
			catch (System.IO.IOException)
			{
				uid = user.GetHashCode();
				LOG.info("Can't map user " + user + ". Use its string hashcode:" + uid);
			}
			return uid;
		}

		// When can't map group, return group name's string hashcode
		public virtual int getGidAllowingUnknown(string group)
		{
			checkAndUpdateMaps();
			int gid;
			try
			{
				gid = getGid(group);
			}
			catch (System.IO.IOException)
			{
				gid = group.GetHashCode();
				LOG.info("Can't map group " + group + ". Use its string hashcode:" + gid);
			}
			return gid;
		}
	}
}
