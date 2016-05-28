using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>JSON Utilities</summary>
	public class JsonUtil
	{
		private static readonly object[] EmptyObjectArray = new object[] {  };

		private static readonly DatanodeInfo[] EmptyDatanodeInfoArray = new DatanodeInfo[
			] {  };

		/// <summary>Convert a token object to a Json string.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static string ToJsonString<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0
			> token)
			where _T0 : TokenIdentifier
		{
			return ToJsonString(typeof(Org.Apache.Hadoop.Security.Token.Token), ToJsonMap(token
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private static IDictionary<string, object> ToJsonMap<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			if (token == null)
			{
				return null;
			}
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["urlString"] = token.EncodeToUrlString();
			return m;
		}

		/// <summary>Convert a Json map to a Token.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> ToToken<_T0
			>(IDictionary<_T0> m)
		{
			if (m == null)
			{
				return null;
			}
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>();
			token.DecodeFromUrlString((string)m["urlString"]);
			return token;
		}

		/// <summary>Convert a Json map to a Token of DelegationTokenIdentifier.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> ToDelegationToken
			<_T0>(IDictionary<_T0> json)
		{
			IDictionary<object, object> m = (IDictionary<object, object>)json[typeof(Org.Apache.Hadoop.Security.Token.Token
				).Name];
			return (Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>)ToToken
				(m);
		}

		/// <summary>Convert a Json map to a Token of BlockTokenIdentifier.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> ToBlockToken
			<_T0>(IDictionary<_T0> m)
		{
			return (Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>)ToToken(m);
		}

		/// <summary>Convert an exception object to a Json string.</summary>
		public static string ToJsonString(Exception e)
		{
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["exception"] = e.GetType().Name;
			m["message"] = e.Message;
			m["javaClassName"] = e.GetType().FullName;
			return ToJsonString(typeof(RemoteException), m);
		}

		/// <summary>Convert a Json map to a RemoteException.</summary>
		public static RemoteException ToRemoteException<_T0>(IDictionary<_T0> json)
		{
			IDictionary<object, object> m = (IDictionary<object, object>)json[typeof(RemoteException
				).Name];
			string message = (string)m["message"];
			string javaClassName = (string)m["javaClassName"];
			return new RemoteException(javaClassName, message);
		}

		private static string ToJsonString(Type clazz, object value)
		{
			return ToJsonString(clazz.Name, value);
		}

		/// <summary>Convert a key-value pair to a Json string.</summary>
		public static string ToJsonString(string key, object value)
		{
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m[key] = value;
			ObjectMapper mapper = new ObjectMapper();
			try
			{
				return mapper.WriteValueAsString(m);
			}
			catch (IOException)
			{
			}
			return null;
		}

		/// <summary>Convert a FsPermission object to a string.</summary>
		private static string ToString(FsPermission permission)
		{
			return string.Format("%o", permission.ToShort());
		}

		/// <summary>Convert a string to a FsPermission object.</summary>
		private static FsPermission ToFsPermission(string s, bool aclBit, bool encBit)
		{
			FsPermission perm = new FsPermission(short.ParseShort(s, 8));
			bool aBit = (aclBit != null) ? aclBit : false;
			bool eBit = (encBit != null) ? encBit : false;
			if (aBit || eBit)
			{
				return new FsPermissionExtension(perm, aBit, eBit);
			}
			else
			{
				return perm;
			}
		}

		[System.Serializable]
		internal sealed class PathType
		{
			public static readonly JsonUtil.PathType File = new JsonUtil.PathType();

			public static readonly JsonUtil.PathType Directory = new JsonUtil.PathType();

			public static readonly JsonUtil.PathType Symlink = new JsonUtil.PathType();

			internal static JsonUtil.PathType ValueOf(HdfsFileStatus status)
			{
				return status.IsDir() ? JsonUtil.PathType.Directory : status.IsSymlink() ? JsonUtil.PathType
					.Symlink : JsonUtil.PathType.File;
			}
		}

		/// <summary>Convert a HdfsFileStatus object to a Json string.</summary>
		public static string ToJsonString(HdfsFileStatus status, bool includeType)
		{
			if (status == null)
			{
				return null;
			}
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["pathSuffix"] = status.GetLocalName();
			m["type"] = JsonUtil.PathType.ValueOf(status);
			if (status.IsSymlink())
			{
				m["symlink"] = status.GetSymlink();
			}
			m["length"] = status.GetLen();
			m["owner"] = status.GetOwner();
			m["group"] = status.GetGroup();
			FsPermission perm = status.GetPermission();
			m["permission"] = ToString(perm);
			if (perm.GetAclBit())
			{
				m["aclBit"] = true;
			}
			if (perm.GetEncryptedBit())
			{
				m["encBit"] = true;
			}
			m["accessTime"] = status.GetAccessTime();
			m["modificationTime"] = status.GetModificationTime();
			m["blockSize"] = status.GetBlockSize();
			m["replication"] = status.GetReplication();
			m["fileId"] = status.GetFileId();
			m["childrenNum"] = status.GetChildrenNum();
			m["storagePolicy"] = status.GetStoragePolicy();
			ObjectMapper mapper = new ObjectMapper();
			try
			{
				return includeType ? ToJsonString(typeof(FileStatus), m) : mapper.WriteValueAsString
					(m);
			}
			catch (IOException)
			{
			}
			return null;
		}

		/// <summary>Convert a Json map to a HdfsFileStatus object.</summary>
		public static HdfsFileStatus ToFileStatus<_T0>(IDictionary<_T0> json, bool includesType
			)
		{
			if (json == null)
			{
				return null;
			}
			IDictionary<object, object> m = includesType ? (IDictionary<object, object>)json[
				typeof(FileStatus).Name] : json;
			string localName = (string)m["pathSuffix"];
			JsonUtil.PathType type = JsonUtil.PathType.ValueOf((string)m["type"]);
			byte[] symlink = type != JsonUtil.PathType.Symlink ? null : DFSUtil.String2Bytes(
				(string)m["symlink"]);
			long len = ((Number)m["length"]);
			string owner = (string)m["owner"];
			string group = (string)m["group"];
			FsPermission permission = ToFsPermission((string)m["permission"], (bool)m["aclBit"
				], (bool)m["encBit"]);
			long aTime = ((Number)m["accessTime"]);
			long mTime = ((Number)m["modificationTime"]);
			long blockSize = ((Number)m["blockSize"]);
			short replication = ((Number)m["replication"]);
			long fileId = m.Contains("fileId") ? ((Number)m["fileId"]) : INodeId.GrandfatherInodeId;
			int childrenNum = GetInt(m, "childrenNum", -1);
			byte storagePolicy = m.Contains("storagePolicy") ? unchecked((byte)((Number)m["storagePolicy"
				])) : BlockStoragePolicySuite.IdUnspecified;
			return new HdfsFileStatus(len, type == JsonUtil.PathType.Directory, replication, 
				blockSize, mTime, aTime, permission, owner, group, symlink, DFSUtil.String2Bytes
				(localName), fileId, childrenNum, null, storagePolicy);
		}

		/// <summary>Convert an ExtendedBlock to a Json map.</summary>
		private static IDictionary<string, object> ToJsonMap(ExtendedBlock extendedblock)
		{
			if (extendedblock == null)
			{
				return null;
			}
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["blockPoolId"] = extendedblock.GetBlockPoolId();
			m["blockId"] = extendedblock.GetBlockId();
			m["numBytes"] = extendedblock.GetNumBytes();
			m["generationStamp"] = extendedblock.GetGenerationStamp();
			return m;
		}

		/// <summary>Convert a Json map to an ExtendedBlock object.</summary>
		private static ExtendedBlock ToExtendedBlock<_T0>(IDictionary<_T0> m)
		{
			if (m == null)
			{
				return null;
			}
			string blockPoolId = (string)m["blockPoolId"];
			long blockId = ((Number)m["blockId"]);
			long numBytes = ((Number)m["numBytes"]);
			long generationStamp = ((Number)m["generationStamp"]);
			return new ExtendedBlock(blockPoolId, blockId, numBytes, generationStamp);
		}

		/// <summary>Convert a DatanodeInfo to a Json map.</summary>
		internal static IDictionary<string, object> ToJsonMap(DatanodeInfo datanodeinfo)
		{
			if (datanodeinfo == null)
			{
				return null;
			}
			// TODO: Fix storageID
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["ipAddr"] = datanodeinfo.GetIpAddr();
			// 'name' is equivalent to ipAddr:xferPort. Older clients (1.x, 0.23.x) 
			// expects this instead of the two fields.
			m["name"] = datanodeinfo.GetXferAddr();
			m["hostName"] = datanodeinfo.GetHostName();
			m["storageID"] = datanodeinfo.GetDatanodeUuid();
			m["xferPort"] = datanodeinfo.GetXferPort();
			m["infoPort"] = datanodeinfo.GetInfoPort();
			m["infoSecurePort"] = datanodeinfo.GetInfoSecurePort();
			m["ipcPort"] = datanodeinfo.GetIpcPort();
			m["capacity"] = datanodeinfo.GetCapacity();
			m["dfsUsed"] = datanodeinfo.GetDfsUsed();
			m["remaining"] = datanodeinfo.GetRemaining();
			m["blockPoolUsed"] = datanodeinfo.GetBlockPoolUsed();
			m["cacheCapacity"] = datanodeinfo.GetCacheCapacity();
			m["cacheUsed"] = datanodeinfo.GetCacheUsed();
			m["lastUpdate"] = datanodeinfo.GetLastUpdate();
			m["lastUpdateMonotonic"] = datanodeinfo.GetLastUpdateMonotonic();
			m["xceiverCount"] = datanodeinfo.GetXceiverCount();
			m["networkLocation"] = datanodeinfo.GetNetworkLocation();
			m["adminState"] = datanodeinfo.GetAdminState().ToString();
			return m;
		}

		private static int GetInt<_T0>(IDictionary<_T0> m, string key, int defaultValue)
		{
			object value = m[key];
			if (value == null)
			{
				return defaultValue;
			}
			return ((Number)value);
		}

		private static long GetLong<_T0>(IDictionary<_T0> m, string key, long defaultValue
			)
		{
			object value = m[key];
			if (value == null)
			{
				return defaultValue;
			}
			return ((Number)value);
		}

		private static string GetString<_T0>(IDictionary<_T0> m, string key, string defaultValue
			)
		{
			object value = m[key];
			if (value == null)
			{
				return defaultValue;
			}
			return (string)value;
		}

		internal static IList<object> GetList<_T0>(IDictionary<_T0> m, string key)
		{
			object list = m[key];
			if (list is IList<object>)
			{
				return (IList<object>)list;
			}
			else
			{
				return null;
			}
		}

		/// <summary>Convert a Json map to an DatanodeInfo object.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static DatanodeInfo ToDatanodeInfo<_T0>(IDictionary<_T0> m)
		{
			if (m == null)
			{
				return null;
			}
			// ipAddr and xferPort are the critical fields for accessing data.
			// If any one of the two is missing, an exception needs to be thrown.
			// Handle the case of old servers (1.x, 0.23.x) sending 'name' instead
			// of ipAddr and xferPort.
			int xferPort = GetInt(m, "xferPort", -1);
			object tmpValue = m["ipAddr"];
			string ipAddr = (tmpValue == null) ? null : (string)tmpValue;
			if (ipAddr == null)
			{
				tmpValue = m["name"];
				if (tmpValue != null)
				{
					string name = (string)tmpValue;
					int colonIdx = name.IndexOf(':');
					if (colonIdx > 0)
					{
						ipAddr = Sharpen.Runtime.Substring(name, 0, colonIdx);
						xferPort = System.Convert.ToInt32(Sharpen.Runtime.Substring(name, colonIdx + 1));
					}
					else
					{
						throw new IOException("Invalid value in server response: name=[" + name + "]");
					}
				}
				else
				{
					throw new IOException("Missing both 'ipAddr' and 'name' in server response.");
				}
			}
			// ipAddr is non-null & non-empty string at this point.
			// Check the validity of xferPort.
			if (xferPort == -1)
			{
				throw new IOException("Invalid or missing 'xferPort' in server response.");
			}
			// TODO: Fix storageID
			return new DatanodeInfo(ipAddr, (string)m["hostName"], (string)m["storageID"], xferPort
				, ((Number)m["infoPort"]), GetInt(m, "infoSecurePort", 0), ((Number)m["ipcPort"]
				), GetLong(m, "capacity", 0l), GetLong(m, "dfsUsed", 0l), GetLong(m, "remaining"
				, 0l), GetLong(m, "blockPoolUsed", 0l), GetLong(m, "cacheCapacity", 0l), GetLong
				(m, "cacheUsed", 0l), GetLong(m, "lastUpdate", 0l), GetLong(m, "lastUpdateMonotonic"
				, 0l), GetInt(m, "xceiverCount", 0), GetString(m, "networkLocation", string.Empty
				), DatanodeInfo.AdminStates.ValueOf(GetString(m, "adminState", "NORMAL")));
		}

		/// <summary>Convert a DatanodeInfo[] to a Json array.</summary>
		private static object[] ToJsonArray(DatanodeInfo[] array)
		{
			if (array == null)
			{
				return null;
			}
			else
			{
				if (array.Length == 0)
				{
					return EmptyObjectArray;
				}
				else
				{
					object[] a = new object[array.Length];
					for (int i = 0; i < array.Length; i++)
					{
						a[i] = ToJsonMap(array[i]);
					}
					return a;
				}
			}
		}

		/// <summary>Convert an Object[] to a DatanodeInfo[].</summary>
		/// <exception cref="System.IO.IOException"/>
		private static DatanodeInfo[] ToDatanodeInfoArray<_T0>(IList<_T0> objects)
		{
			if (objects == null)
			{
				return null;
			}
			else
			{
				if (objects.IsEmpty())
				{
					return EmptyDatanodeInfoArray;
				}
				else
				{
					DatanodeInfo[] array = new DatanodeInfo[objects.Count];
					int i = 0;
					foreach (object @object in objects)
					{
						array[i++] = ToDatanodeInfo((IDictionary<object, object>)@object);
					}
					return array;
				}
			}
		}

		/// <summary>Convert a LocatedBlock to a Json map.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static IDictionary<string, object> ToJsonMap(LocatedBlock locatedblock)
		{
			if (locatedblock == null)
			{
				return null;
			}
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["blockToken"] = ToJsonMap(locatedblock.GetBlockToken());
			m["isCorrupt"] = locatedblock.IsCorrupt();
			m["startOffset"] = locatedblock.GetStartOffset();
			m["block"] = ToJsonMap(locatedblock.GetBlock());
			m["locations"] = ToJsonArray(locatedblock.GetLocations());
			m["cachedLocations"] = ToJsonArray(locatedblock.GetCachedLocations());
			return m;
		}

		/// <summary>Convert a Json map to LocatedBlock.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static LocatedBlock ToLocatedBlock<_T0>(IDictionary<_T0> m)
		{
			if (m == null)
			{
				return null;
			}
			ExtendedBlock b = ToExtendedBlock((IDictionary<object, object>)m["block"]);
			DatanodeInfo[] locations = ToDatanodeInfoArray(GetList(m, "locations"));
			long startOffset = ((Number)m["startOffset"]);
			bool isCorrupt = (bool)m["isCorrupt"];
			DatanodeInfo[] cachedLocations = ToDatanodeInfoArray(GetList(m, "cachedLocations"
				));
			LocatedBlock locatedblock = new LocatedBlock(b, locations, null, null, startOffset
				, isCorrupt, cachedLocations);
			locatedblock.SetBlockToken(ToBlockToken((IDictionary<object, object>)m["blockToken"
				]));
			return locatedblock;
		}

		/// <summary>Convert a LocatedBlock[] to a Json array.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static object[] ToJsonArray(IList<LocatedBlock> array)
		{
			if (array == null)
			{
				return null;
			}
			else
			{
				if (array.Count == 0)
				{
					return EmptyObjectArray;
				}
				else
				{
					object[] a = new object[array.Count];
					for (int i = 0; i < array.Count; i++)
					{
						a[i] = ToJsonMap(array[i]);
					}
					return a;
				}
			}
		}

		/// <summary>Convert an List of Object to a List of LocatedBlock.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static IList<LocatedBlock> ToLocatedBlockList<_T0>(IList<_T0> objects)
		{
			if (objects == null)
			{
				return null;
			}
			else
			{
				if (objects.IsEmpty())
				{
					return Sharpen.Collections.EmptyList();
				}
				else
				{
					IList<LocatedBlock> list = new AList<LocatedBlock>(objects.Count);
					foreach (object @object in objects)
					{
						list.AddItem(ToLocatedBlock((IDictionary<object, object>)@object));
					}
					return list;
				}
			}
		}

		/// <summary>Convert LocatedBlocks to a Json string.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static string ToJsonString(LocatedBlocks locatedblocks)
		{
			if (locatedblocks == null)
			{
				return null;
			}
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["fileLength"] = locatedblocks.GetFileLength();
			m["isUnderConstruction"] = locatedblocks.IsUnderConstruction();
			m["locatedBlocks"] = ToJsonArray(locatedblocks.GetLocatedBlocks());
			m["lastLocatedBlock"] = ToJsonMap(locatedblocks.GetLastLocatedBlock());
			m["isLastBlockComplete"] = locatedblocks.IsLastBlockComplete();
			return ToJsonString(typeof(LocatedBlocks), m);
		}

		/// <summary>Convert a Json map to LocatedBlock.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static LocatedBlocks ToLocatedBlocks<_T0>(IDictionary<_T0> json)
		{
			if (json == null)
			{
				return null;
			}
			IDictionary<object, object> m = (IDictionary<object, object>)json[typeof(LocatedBlocks
				).Name];
			long fileLength = ((Number)m["fileLength"]);
			bool isUnderConstruction = (bool)m["isUnderConstruction"];
			IList<LocatedBlock> locatedBlocks = ToLocatedBlockList(GetList(m, "locatedBlocks"
				));
			LocatedBlock lastLocatedBlock = ToLocatedBlock((IDictionary<object, object>)m["lastLocatedBlock"
				]);
			bool isLastBlockComplete = (bool)m["isLastBlockComplete"];
			return new LocatedBlocks(fileLength, isUnderConstruction, locatedBlocks, lastLocatedBlock
				, isLastBlockComplete, null);
		}

		/// <summary>Convert a ContentSummary to a Json string.</summary>
		public static string ToJsonString(ContentSummary contentsummary)
		{
			if (contentsummary == null)
			{
				return null;
			}
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["length"] = contentsummary.GetLength();
			m["fileCount"] = contentsummary.GetFileCount();
			m["directoryCount"] = contentsummary.GetDirectoryCount();
			m["quota"] = contentsummary.GetQuota();
			m["spaceConsumed"] = contentsummary.GetSpaceConsumed();
			m["spaceQuota"] = contentsummary.GetSpaceQuota();
			return ToJsonString(typeof(ContentSummary), m);
		}

		/// <summary>Convert a Json map to a ContentSummary.</summary>
		public static ContentSummary ToContentSummary<_T0>(IDictionary<_T0> json)
		{
			if (json == null)
			{
				return null;
			}
			IDictionary<object, object> m = (IDictionary<object, object>)json[typeof(ContentSummary
				).Name];
			long length = ((Number)m["length"]);
			long fileCount = ((Number)m["fileCount"]);
			long directoryCount = ((Number)m["directoryCount"]);
			long quota = ((Number)m["quota"]);
			long spaceConsumed = ((Number)m["spaceConsumed"]);
			long spaceQuota = ((Number)m["spaceQuota"]);
			return new ContentSummary.Builder().Length(length).FileCount(fileCount).DirectoryCount
				(directoryCount).Quota(quota).SpaceConsumed(spaceConsumed).SpaceQuota(spaceQuota
				).Build();
		}

		/// <summary>Convert a MD5MD5CRC32FileChecksum to a Json string.</summary>
		public static string ToJsonString(MD5MD5CRC32FileChecksum checksum)
		{
			if (checksum == null)
			{
				return null;
			}
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["algorithm"] = checksum.GetAlgorithmName();
			m["length"] = checksum.GetLength();
			m["bytes"] = StringUtils.ByteToHexString(checksum.GetBytes());
			return ToJsonString(typeof(FileChecksum), m);
		}

		/// <summary>Convert a Json map to a MD5MD5CRC32FileChecksum.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static MD5MD5CRC32FileChecksum ToMD5MD5CRC32FileChecksum<_T0>(IDictionary<
			_T0> json)
		{
			if (json == null)
			{
				return null;
			}
			IDictionary<object, object> m = (IDictionary<object, object>)json[typeof(FileChecksum
				).Name];
			string algorithm = (string)m["algorithm"];
			int length = ((Number)m["length"]);
			byte[] bytes = StringUtils.HexStringToByte((string)m["bytes"]);
			DataInputStream @in = new DataInputStream(new ByteArrayInputStream(bytes));
			DataChecksum.Type crcType = MD5MD5CRC32FileChecksum.GetCrcTypeFromAlgorithmName(algorithm
				);
			MD5MD5CRC32FileChecksum checksum;
			switch (crcType)
			{
				case DataChecksum.Type.Crc32:
				{
					// Recreate what DFSClient would have returned.
					checksum = new MD5MD5CRC32GzipFileChecksum();
					break;
				}

				case DataChecksum.Type.Crc32c:
				{
					checksum = new MD5MD5CRC32CastagnoliFileChecksum();
					break;
				}

				default:
				{
					throw new IOException("Unknown algorithm: " + algorithm);
				}
			}
			checksum.ReadFields(@in);
			//check algorithm name
			if (!checksum.GetAlgorithmName().Equals(algorithm))
			{
				throw new IOException("Algorithm not matched. Expected " + algorithm + ", Received "
					 + checksum.GetAlgorithmName());
			}
			//check length
			if (length != checksum.GetLength())
			{
				throw new IOException("Length not matched: length=" + length + ", checksum.getLength()="
					 + checksum.GetLength());
			}
			return checksum;
		}

		/// <summary>Convert a AclStatus object to a Json string.</summary>
		public static string ToJsonString(AclStatus status)
		{
			if (status == null)
			{
				return null;
			}
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["owner"] = status.GetOwner();
			m["group"] = status.GetGroup();
			m["stickyBit"] = status.IsStickyBit();
			IList<string> stringEntries = new AList<string>();
			foreach (AclEntry entry in status.GetEntries())
			{
				stringEntries.AddItem(entry.ToString());
			}
			m["entries"] = stringEntries;
			FsPermission perm = status.GetPermission();
			if (perm != null)
			{
				m["permission"] = ToString(perm);
				if (perm.GetAclBit())
				{
					m["aclBit"] = true;
				}
				if (perm.GetEncryptedBit())
				{
					m["encBit"] = true;
				}
			}
			IDictionary<string, IDictionary<string, object>> finalMap = new SortedDictionary<
				string, IDictionary<string, object>>();
			finalMap[typeof(AclStatus).Name] = m;
			ObjectMapper mapper = new ObjectMapper();
			try
			{
				return mapper.WriteValueAsString(finalMap);
			}
			catch (IOException)
			{
			}
			return null;
		}

		/// <summary>Convert a Json map to a AclStatus object.</summary>
		public static AclStatus ToAclStatus<_T0>(IDictionary<_T0> json)
		{
			if (json == null)
			{
				return null;
			}
			IDictionary<object, object> m = (IDictionary<object, object>)json[typeof(AclStatus
				).Name];
			AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
			aclStatusBuilder.Owner((string)m["owner"]);
			aclStatusBuilder.Group((string)m["group"]);
			aclStatusBuilder.StickyBit((bool)m["stickyBit"]);
			string permString = (string)m["permission"];
			if (permString != null)
			{
				FsPermission permission = ToFsPermission(permString, (bool)m["aclBit"], (bool)m["encBit"
					]);
				aclStatusBuilder.SetPermission(permission);
			}
			IList<object> entries = (IList<object>)m["entries"];
			IList<AclEntry> aclEntryList = new AList<AclEntry>();
			foreach (object entry in entries)
			{
				AclEntry aclEntry = AclEntry.ParseAclEntry((string)entry, true);
				aclEntryList.AddItem(aclEntry);
			}
			aclStatusBuilder.AddEntries(aclEntryList);
			return aclStatusBuilder.Build();
		}

		/// <exception cref="System.IO.IOException"/>
		private static IDictionary<string, object> ToJsonMap(XAttr xAttr, XAttrCodec encoding
			)
		{
			if (xAttr == null)
			{
				return null;
			}
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m["name"] = XAttrHelper.GetPrefixName(xAttr);
			m["value"] = xAttr.GetValue() != null ? XAttrCodec.EncodeValue(xAttr.GetValue(), 
				encoding) : null;
			return m;
		}

		/// <exception cref="System.IO.IOException"/>
		private static object[] ToJsonArray(IList<XAttr> array, XAttrCodec encoding)
		{
			if (array == null)
			{
				return null;
			}
			else
			{
				if (array.Count == 0)
				{
					return EmptyObjectArray;
				}
				else
				{
					object[] a = new object[array.Count];
					for (int i = 0; i < array.Count; i++)
					{
						a[i] = ToJsonMap(array[i], encoding);
					}
					return a;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static string ToJsonString(IList<XAttr> xAttrs, XAttrCodec encoding)
		{
			IDictionary<string, object> finalMap = new SortedDictionary<string, object>();
			finalMap["XAttrs"] = ToJsonArray(xAttrs, encoding);
			ObjectMapper mapper = new ObjectMapper();
			return mapper.WriteValueAsString(finalMap);
		}

		/// <exception cref="System.IO.IOException"/>
		public static string ToJsonString(IList<XAttr> xAttrs)
		{
			IList<string> names = Lists.NewArrayListWithCapacity(xAttrs.Count);
			foreach (XAttr xAttr in xAttrs)
			{
				names.AddItem(XAttrHelper.GetPrefixName(xAttr));
			}
			ObjectMapper mapper = new ObjectMapper();
			string ret = mapper.WriteValueAsString(names);
			IDictionary<string, object> finalMap = new SortedDictionary<string, object>();
			finalMap["XAttrNames"] = ret;
			return mapper.WriteValueAsString(finalMap);
		}

		/// <exception cref="System.IO.IOException"/>
		public static byte[] GetXAttr<_T0>(IDictionary<_T0> json, string name)
		{
			if (json == null)
			{
				return null;
			}
			IDictionary<string, byte[]> xAttrs = ToXAttrs(json);
			if (xAttrs != null)
			{
				return xAttrs[name];
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public static IDictionary<string, byte[]> ToXAttrs<_T0>(IDictionary<_T0> json)
		{
			if (json == null)
			{
				return null;
			}
			return ToXAttrMap(GetList(json, "XAttrs"));
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<string> ToXAttrNames<_T0>(IDictionary<_T0> json)
		{
			if (json == null)
			{
				return null;
			}
			string namesInJson = (string)json["XAttrNames"];
			ObjectReader reader = new ObjectMapper().Reader(typeof(IList));
			IList<object> xattrs = reader.ReadValue(namesInJson);
			IList<string> names = Lists.NewArrayListWithCapacity(json.Keys.Count);
			foreach (object xattr in xattrs)
			{
				names.AddItem((string)xattr);
			}
			return names;
		}

		/// <exception cref="System.IO.IOException"/>
		private static IDictionary<string, byte[]> ToXAttrMap<_T0>(IList<_T0> objects)
		{
			if (objects == null)
			{
				return null;
			}
			else
			{
				if (objects.IsEmpty())
				{
					return Maps.NewHashMap();
				}
				else
				{
					IDictionary<string, byte[]> xAttrs = Maps.NewHashMap();
					foreach (object @object in objects)
					{
						IDictionary<object, object> m = (IDictionary<object, object>)@object;
						string name = (string)m["name"];
						string value = (string)m["value"];
						xAttrs[name] = DecodeXAttrValue(value);
					}
					return xAttrs;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static byte[] DecodeXAttrValue(string value)
		{
			if (value != null)
			{
				return XAttrCodec.DecodeValue(value);
			}
			else
			{
				return new byte[0];
			}
		}
	}
}
