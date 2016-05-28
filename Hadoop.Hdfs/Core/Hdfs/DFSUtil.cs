using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Primitives;
using Com.Google.Protobuf;
using Javax.Net;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class DFSUtil
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.DFSUtil
			).FullName);

		public static readonly byte[] EmptyBytes = new byte[] {  };

		/// <summary>Compare two byte arrays by lexicographical order.</summary>
		public static int CompareBytes(byte[] left, byte[] right)
		{
			if (left == null)
			{
				left = EmptyBytes;
			}
			if (right == null)
			{
				right = EmptyBytes;
			}
			return SignedBytes.LexicographicalComparator().Compare(left, right);
		}

		private DFSUtil()
		{
		}

		private sealed class _ThreadLocal_132 : ThreadLocal<Random>
		{
			public _ThreadLocal_132()
			{
			}

			/* Hidden constructor */
			protected override Random InitialValue()
			{
				return new Random();
			}
		}

		private static readonly ThreadLocal<Random> Random = new _ThreadLocal_132();

		private sealed class _ThreadLocal_139 : ThreadLocal<SecureRandom>
		{
			public _ThreadLocal_139()
			{
			}

			protected override SecureRandom InitialValue()
			{
				return new SecureRandom();
			}
		}

		private static readonly ThreadLocal<SecureRandom> SecureRandom = new _ThreadLocal_139
			();

		/// <returns>a pseudo random number generator.</returns>
		public static Random GetRandom()
		{
			return Random.Get();
		}

		/// <returns>a pseudo secure random number generator.</returns>
		public static SecureRandom GetSecureRandom()
		{
			return SecureRandom.Get();
		}

		/// <summary>Shuffle the elements in the given array.</summary>
		public static T[] Shuffle<T>(T[] array)
		{
			if (array != null && array.Length > 0)
			{
				Random random = GetRandom();
				for (int n = array.Length; n > 1; )
				{
					int randomIndex = random.Next(n);
					n--;
					if (n != randomIndex)
					{
						T tmp = array[randomIndex];
						array[randomIndex] = array[n];
						array[n] = tmp;
					}
				}
			}
			return array;
		}

		private sealed class _IComparer_179 : IComparer<DatanodeInfo>
		{
			public _IComparer_179()
			{
			}

			public int Compare(DatanodeInfo a, DatanodeInfo b)
			{
				return a.IsDecommissioned() == b.IsDecommissioned() ? 0 : a.IsDecommissioned() ? 
					1 : -1;
			}
		}

		/// <summary>Compartor for sorting DataNodeInfo[] based on decommissioned states.</summary>
		/// <remarks>
		/// Compartor for sorting DataNodeInfo[] based on decommissioned states.
		/// Decommissioned nodes are moved to the end of the array on sorting with
		/// this compartor.
		/// </remarks>
		public static readonly IComparer<DatanodeInfo> DecomComparator = new _IComparer_179
			();

		/// <summary>Comparator for sorting DataNodeInfo[] based on decommissioned/stale states.
		/// 	</summary>
		/// <remarks>
		/// Comparator for sorting DataNodeInfo[] based on decommissioned/stale states.
		/// Decommissioned/stale nodes are moved to the end of the array on sorting
		/// with this comparator.
		/// </remarks>
		public class DecomStaleComparator : IComparer<DatanodeInfo>
		{
			private readonly long staleInterval;

			/// <summary>Constructor of DecomStaleComparator</summary>
			/// <param name="interval">
			/// The time interval for marking datanodes as stale is passed from
			/// outside, since the interval may be changed dynamically
			/// </param>
			public DecomStaleComparator(long interval)
			{
				this.staleInterval = interval;
			}

			public virtual int Compare(DatanodeInfo a, DatanodeInfo b)
			{
				// Decommissioned nodes will still be moved to the end of the list
				if (a.IsDecommissioned())
				{
					return b.IsDecommissioned() ? 0 : 1;
				}
				else
				{
					if (b.IsDecommissioned())
					{
						return -1;
					}
				}
				// Stale nodes will be moved behind the normal nodes
				bool aStale = a.IsStale(staleInterval);
				bool bStale = b.IsStale(staleInterval);
				return aStale == bStale ? 0 : (aStale ? 1 : -1);
			}
		}

		private sealed class _AddressMatcher_226 : DFSUtil.AddressMatcher
		{
			public _AddressMatcher_226()
			{
			}

			public bool Match(IPEndPoint s)
			{
				return NetUtils.IsLocalAddress(s.Address);
			}
		}

		/// <summary>Address matcher for matching an address to local address</summary>
		internal static readonly DFSUtil.AddressMatcher LocalAddressMatcher = new _AddressMatcher_226
			();

		/// <summary>Whether the pathname is valid.</summary>
		/// <remarks>
		/// Whether the pathname is valid.  Currently prohibits relative paths,
		/// names which contain a ":" or "//", or other non-canonical paths.
		/// </remarks>
		public static bool IsValidName(string src)
		{
			// Path must be absolute.
			if (!src.StartsWith(Path.Separator))
			{
				return false;
			}
			// Check for ".." "." ":" "/"
			string[] components = StringUtils.Split(src, '/');
			for (int i = 0; i < components.Length; i++)
			{
				string element = components[i];
				if (element.Equals(".") || (element.IndexOf(":") >= 0) || (element.IndexOf("/") >=
					 0))
				{
					return false;
				}
				// ".." is allowed in path starting with /.reserved/.inodes
				if (element.Equals(".."))
				{
					if (components.Length > 4 && components[1].Equals(FSDirectory.DotReservedString) 
						&& components[2].Equals(FSDirectory.DotInodesString))
					{
						continue;
					}
					return false;
				}
				// The string may start or end with a /, but not have
				// "//" in the middle.
				if (element.IsEmpty() && i != components.Length - 1 && i != 0)
				{
					return false;
				}
			}
			return true;
		}

		/// <summary>Checks if a string is a valid path component.</summary>
		/// <remarks>
		/// Checks if a string is a valid path component. For instance, components
		/// cannot contain a ":" or "/", and cannot be equal to a reserved component
		/// like ".snapshot".
		/// <p>
		/// The primary use of this method is for validating paths when loading the
		/// FSImage. During normal NN operation, paths are sometimes allowed to
		/// contain reserved components.
		/// </remarks>
		/// <returns>If component is valid</returns>
		public static bool IsValidNameForComponent(string component)
		{
			if (component.Equals(".") || component.Equals("..") || component.IndexOf(":") >= 
				0 || component.IndexOf("/") >= 0)
			{
				return false;
			}
			return !IsReservedPathComponent(component);
		}

		/// <summary>Returns if the component is reserved.</summary>
		/// <remarks>
		/// Returns if the component is reserved.
		/// <p>
		/// Note that some components are only reserved under certain directories, e.g.
		/// "/.reserved" is reserved, while "/hadoop/.reserved" is not.
		/// </remarks>
		/// <returns>true, if the component is reserved</returns>
		public static bool IsReservedPathComponent(string component)
		{
			foreach (string reserved in HdfsConstants.ReservedPathComponents)
			{
				if (component.Equals(reserved))
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>Converts a byte array to a string using UTF8 encoding.</summary>
		public static string Bytes2String(byte[] bytes)
		{
			return Bytes2String(bytes, 0, bytes.Length);
		}

		/// <summary>
		/// Decode a specific range of bytes of the given byte array to a string
		/// using UTF8.
		/// </summary>
		/// <param name="bytes">The bytes to be decoded into characters</param>
		/// <param name="offset">The index of the first byte to decode</param>
		/// <param name="length">The number of bytes to decode</param>
		/// <returns>The decoded string</returns>
		public static string Bytes2String(byte[] bytes, int offset, int length)
		{
			try
			{
				return Sharpen.Runtime.GetStringForBytes(bytes, offset, length, "UTF8");
			}
			catch (UnsupportedEncodingException)
			{
				System.Diagnostics.Debug.Assert(false, "UTF8 encoding is not supported ");
			}
			return null;
		}

		/// <summary>Converts a string to a byte array using UTF8 encoding.</summary>
		public static byte[] String2Bytes(string str)
		{
			return Sharpen.Runtime.GetBytesForString(str, Charsets.Utf8);
		}

		/// <summary>Given a list of path components returns a path as a UTF8 String</summary>
		public static string ByteArray2PathString(byte[][] pathComponents, int offset, int
			 length)
		{
			if (pathComponents.Length == 0)
			{
				return string.Empty;
			}
			Preconditions.CheckArgument(offset >= 0 && offset < pathComponents.Length);
			Preconditions.CheckArgument(length >= 0 && offset + length <= pathComponents.Length
				);
			if (pathComponents.Length == 1 && (pathComponents[0] == null || pathComponents[0]
				.Length == 0))
			{
				return Path.Separator;
			}
			StringBuilder result = new StringBuilder();
			for (int i = offset; i < offset + length; i++)
			{
				result.Append(new string(pathComponents[i], Charsets.Utf8));
				if (i < pathComponents.Length - 1)
				{
					result.Append(Path.SeparatorChar);
				}
			}
			return result.ToString();
		}

		public static string ByteArray2PathString(byte[][] pathComponents)
		{
			return ByteArray2PathString(pathComponents, 0, pathComponents.Length);
		}

		/// <summary>Converts a list of path components into a path using Path.SEPARATOR.</summary>
		/// <param name="components">Path components</param>
		/// <returns>Combined path as a UTF-8 string</returns>
		public static string Strings2PathString(string[] components)
		{
			if (components.Length == 0)
			{
				return string.Empty;
			}
			if (components.Length == 1)
			{
				if (components[0] == null || components[0].IsEmpty())
				{
					return Path.Separator;
				}
			}
			return Joiner.On(Path.Separator).Join(components);
		}

		/// <summary>Given a list of path components returns a byte array</summary>
		public static byte[] ByteArray2bytes(byte[][] pathComponents)
		{
			if (pathComponents.Length == 0)
			{
				return EmptyBytes;
			}
			else
			{
				if (pathComponents.Length == 1 && (pathComponents[0] == null || pathComponents[0]
					.Length == 0))
				{
					return new byte[] { unchecked((byte)Path.SeparatorChar) };
				}
			}
			int length = 0;
			for (int i = 0; i < pathComponents.Length; i++)
			{
				length += pathComponents[i].Length;
				if (i < pathComponents.Length - 1)
				{
					length++;
				}
			}
			// for SEPARATOR
			byte[] path = new byte[length];
			int index = 0;
			for (int i_1 = 0; i_1 < pathComponents.Length; i_1++)
			{
				System.Array.Copy(pathComponents[i_1], 0, path, index, pathComponents[i_1].Length
					);
				index += pathComponents[i_1].Length;
				if (i_1 < pathComponents.Length - 1)
				{
					path[index] = unchecked((byte)Path.SeparatorChar);
					index++;
				}
			}
			return path;
		}

		/// <summary>Convert an object representing a path to a string.</summary>
		public static string Path2String(object path)
		{
			return path == null ? null : path is string ? (string)path : path is byte[][] ? ByteArray2PathString
				((byte[][])path) : path.ToString();
		}

		/// <summary>
		/// Splits the array of bytes into array of arrays of bytes
		/// on byte separator
		/// </summary>
		/// <param name="bytes">the array of bytes to split</param>
		/// <param name="separator">the delimiting byte</param>
		public static byte[][] Bytes2byteArray(byte[] bytes, byte separator)
		{
			return Bytes2byteArray(bytes, bytes.Length, separator);
		}

		/// <summary>
		/// Splits first len bytes in bytes to array of arrays of bytes
		/// on byte separator
		/// </summary>
		/// <param name="bytes">the byte array to split</param>
		/// <param name="len">the number of bytes to split</param>
		/// <param name="separator">the delimiting byte</param>
		public static byte[][] Bytes2byteArray(byte[] bytes, int len, byte separator)
		{
			System.Diagnostics.Debug.Assert(len <= bytes.Length);
			int splits = 0;
			if (len == 0)
			{
				return new byte[][] { null };
			}
			// Count the splits. Omit multiple separators and the last one
			for (int i = 0; i < len; i++)
			{
				if (bytes[i] == separator)
				{
					splits++;
				}
			}
			int last = len - 1;
			while (last > -1 && bytes[last--] == separator)
			{
				splits--;
			}
			if (splits == 0 && bytes[0] == separator)
			{
				return new byte[][] { null };
			}
			splits++;
			byte[][] result = new byte[splits][];
			int startIndex = 0;
			int nextIndex = 0;
			int index = 0;
			// Build the splits
			while (index < splits)
			{
				while (nextIndex < len && bytes[nextIndex] != separator)
				{
					nextIndex++;
				}
				result[index] = new byte[nextIndex - startIndex];
				System.Array.Copy(bytes, startIndex, result[index], 0, nextIndex - startIndex);
				index++;
				startIndex = nextIndex + 1;
				nextIndex = startIndex;
			}
			return result;
		}

		/// <summary>Convert a LocatedBlocks to BlockLocations[]</summary>
		/// <param name="blocks">a LocatedBlocks</param>
		/// <returns>an array of BlockLocations</returns>
		public static BlockLocation[] LocatedBlocks2Locations(LocatedBlocks blocks)
		{
			if (blocks == null)
			{
				return new BlockLocation[0];
			}
			return LocatedBlocks2Locations(blocks.GetLocatedBlocks());
		}

		/// <summary>Convert a List<LocatedBlock> to BlockLocation[]</summary>
		/// <param name="blocks">A List<LocatedBlock> to be converted</param>
		/// <returns>converted array of BlockLocation</returns>
		public static BlockLocation[] LocatedBlocks2Locations(IList<LocatedBlock> blocks)
		{
			if (blocks == null)
			{
				return new BlockLocation[0];
			}
			int nrBlocks = blocks.Count;
			BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
			if (nrBlocks == 0)
			{
				return blkLocations;
			}
			int idx = 0;
			foreach (LocatedBlock blk in blocks)
			{
				System.Diagnostics.Debug.Assert(idx < nrBlocks, "Incorrect index");
				DatanodeInfo[] locations = blk.GetLocations();
				string[] hosts = new string[locations.Length];
				string[] xferAddrs = new string[locations.Length];
				string[] racks = new string[locations.Length];
				for (int hCnt = 0; hCnt < locations.Length; hCnt++)
				{
					hosts[hCnt] = locations[hCnt].GetHostName();
					xferAddrs[hCnt] = locations[hCnt].GetXferAddr();
					NodeBase node = new NodeBase(xferAddrs[hCnt], locations[hCnt].GetNetworkLocation(
						));
					racks[hCnt] = node.ToString();
				}
				DatanodeInfo[] cachedLocations = blk.GetCachedLocations();
				string[] cachedHosts = new string[cachedLocations.Length];
				for (int i = 0; i < cachedLocations.Length; i++)
				{
					cachedHosts[i] = cachedLocations[i].GetHostName();
				}
				blkLocations[idx] = new BlockLocation(xferAddrs, hosts, cachedHosts, racks, blk.GetStartOffset
					(), blk.GetBlockSize(), blk.IsCorrupt());
				idx++;
			}
			return blkLocations;
		}

		/// <summary>Returns collection of nameservice Ids from the configuration.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>collection of nameservice Ids, or null if not specified</returns>
		public static ICollection<string> GetNameServiceIds(Configuration conf)
		{
			return conf.GetTrimmedStringCollection(DFSConfigKeys.DfsNameservices);
		}

		/// <returns>
		/// <code>coll</code> if it is non-null and non-empty. Otherwise,
		/// returns a list with a single null value.
		/// </returns>
		private static ICollection<string> EmptyAsSingletonNull(ICollection<string> coll)
		{
			if (coll == null || coll.IsEmpty())
			{
				return Sharpen.Collections.SingletonList(null);
			}
			else
			{
				return coll;
			}
		}

		/// <summary>Namenode HighAvailability related configuration.</summary>
		/// <remarks>
		/// Namenode HighAvailability related configuration.
		/// Returns collection of namenode Ids from the configuration. One logical id
		/// for each namenode in the in the HA setup.
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <param name="nsId">the nameservice ID to look at, or null for non-federated</param>
		/// <returns>collection of namenode Ids</returns>
		public static ICollection<string> GetNameNodeIds(Configuration conf, string nsId)
		{
			string key = AddSuffix(DFSConfigKeys.DfsHaNamenodesKeyPrefix, nsId);
			return conf.GetTrimmedStringCollection(key);
		}

		/// <summary>
		/// Given a list of keys in the order of preference, returns a value
		/// for the key in the given order from the configuration.
		/// </summary>
		/// <param name="defaultValue">default value to return, when key was not found</param>
		/// <param name="keySuffix">suffix to add to the key, if it is not null</param>
		/// <param name="conf">Configuration</param>
		/// <param name="keys">list of keys in the order of preference</param>
		/// <returns>value of the key or default if a key was not found in configuration</returns>
		private static string GetConfValue(string defaultValue, string keySuffix, Configuration
			 conf, params string[] keys)
		{
			string value = null;
			foreach (string key in keys)
			{
				key = AddSuffix(key, keySuffix);
				value = conf.Get(key);
				if (value != null)
				{
					break;
				}
			}
			if (value == null)
			{
				value = defaultValue;
			}
			return value;
		}

		/// <summary>Add non empty and non null suffix to a key</summary>
		private static string AddSuffix(string key, string suffix)
		{
			if (suffix == null || suffix.IsEmpty())
			{
				return key;
			}
			System.Diagnostics.Debug.Assert(!suffix.StartsWith("."), "suffix '" + suffix + "' should not already have '.' prepended."
				);
			return key + "." + suffix;
		}

		/// <summary>Concatenate list of suffix strings '.' separated</summary>
		private static string ConcatSuffixes(params string[] suffixes)
		{
			if (suffixes == null)
			{
				return null;
			}
			return Joiner.On(".").SkipNulls().Join(suffixes);
		}

		/// <summary>Return configuration key of format key.suffix1.suffix2...suffixN</summary>
		public static string AddKeySuffixes(string key, params string[] suffixes)
		{
			string keySuffix = ConcatSuffixes(suffixes);
			return AddSuffix(key, keySuffix);
		}

		/// <summary>Returns the configured address for all NameNodes in the cluster.</summary>
		/// <param name="conf">configuration</param>
		/// <param name="defaultAddress">default address to return in case key is not found.</param>
		/// <param name="keys">Set of keys to look for in the order of preference</param>
		/// <returns>a map(nameserviceId to map(namenodeId to InetSocketAddress))</returns>
		private static IDictionary<string, IDictionary<string, IPEndPoint>> GetAddresses(
			Configuration conf, string defaultAddress, params string[] keys)
		{
			ICollection<string> nameserviceIds = GetNameServiceIds(conf);
			return GetAddressesForNsIds(conf, nameserviceIds, defaultAddress, keys);
		}

		/// <summary>Returns the configured address for all NameNodes in the cluster.</summary>
		/// <param name="conf">configuration</param>
		/// <param name="nsIds"/>
		/// <param name="defaultAddress">default address to return in case key is not found.</param>
		/// <param name="keys">Set of keys to look for in the order of preference   @return a map(nameserviceId to map(namenodeId to InetSocketAddress))
		/// 	</param>
		private static IDictionary<string, IDictionary<string, IPEndPoint>> GetAddressesForNsIds
			(Configuration conf, ICollection<string> nsIds, string defaultAddress, params string
			[] keys)
		{
			// Look for configurations of the form <key>[.<nameserviceId>][.<namenodeId>]
			// across all of the configured nameservices and namenodes.
			IDictionary<string, IDictionary<string, IPEndPoint>> ret = Maps.NewLinkedHashMap(
				);
			foreach (string nsId in EmptyAsSingletonNull(nsIds))
			{
				IDictionary<string, IPEndPoint> isas = GetAddressesForNameserviceId(conf, nsId, defaultAddress
					, keys);
				if (!isas.IsEmpty())
				{
					ret[nsId] = isas;
				}
			}
			return ret;
		}

		/// <summary>Get all of the RPC addresses of the individual NNs in a given nameservice.
		/// 	</summary>
		/// <param name="conf">Configuration</param>
		/// <param name="nsId">the nameservice whose NNs addresses we want.</param>
		/// <param name="defaultValue">default address to return in case key is not found.</param>
		/// <returns>A map from nnId -&gt; RPC address of each NN in the nameservice.</returns>
		public static IDictionary<string, IPEndPoint> GetRpcAddressesForNameserviceId(Configuration
			 conf, string nsId, string defaultValue)
		{
			return GetAddressesForNameserviceId(conf, nsId, defaultValue, DFSConfigKeys.DfsNamenodeRpcAddressKey
				);
		}

		private static IDictionary<string, IPEndPoint> GetAddressesForNameserviceId(Configuration
			 conf, string nsId, string defaultValue, params string[] keys)
		{
			ICollection<string> nnIds = GetNameNodeIds(conf, nsId);
			IDictionary<string, IPEndPoint> ret = Maps.NewHashMap();
			foreach (string nnId in EmptyAsSingletonNull(nnIds))
			{
				string suffix = ConcatSuffixes(nsId, nnId);
				string address = GetConfValue(defaultValue, suffix, conf, keys);
				if (address != null)
				{
					IPEndPoint isa = NetUtils.CreateSocketAddr(address);
					if (isa.IsUnresolved())
					{
						Log.Warn("Namenode for " + nsId + " remains unresolved for ID " + nnId + ".  Check your hdfs-site.xml file to "
							 + "ensure namenodes are configured properly.");
					}
					ret[nnId] = isa;
				}
			}
			return ret;
		}

		/// <returns>a collection of all configured NN Kerberos principals.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static ICollection<string> GetAllNnPrincipals(Configuration conf)
		{
			ICollection<string> principals = new HashSet<string>();
			foreach (string nsId in DFSUtil.GetNameServiceIds(conf))
			{
				if (HAUtil.IsHAEnabled(conf, nsId))
				{
					foreach (string nnId in DFSUtil.GetNameNodeIds(conf, nsId))
					{
						Configuration confForNn = new Configuration(conf);
						NameNode.InitializeGenericKeys(confForNn, nsId, nnId);
						string principal = SecurityUtil.GetServerPrincipal(confForNn.Get(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
							), NameNode.GetAddress(confForNn).GetHostName());
						principals.AddItem(principal);
					}
				}
				else
				{
					Configuration confForNn = new Configuration(conf);
					NameNode.InitializeGenericKeys(confForNn, nsId, null);
					string principal = SecurityUtil.GetServerPrincipal(confForNn.Get(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
						), NameNode.GetAddress(confForNn).GetHostName());
					principals.AddItem(principal);
				}
			}
			return principals;
		}

		/// <summary>
		/// Returns list of InetSocketAddress corresponding to HA NN RPC addresses from
		/// the configuration.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <returns>list of InetSocketAddresses</returns>
		public static IDictionary<string, IDictionary<string, IPEndPoint>> GetHaNnRpcAddresses
			(Configuration conf)
		{
			return GetAddresses(conf, null, DFSConfigKeys.DfsNamenodeRpcAddressKey);
		}

		/// <summary>
		/// Returns list of InetSocketAddress corresponding to HA NN HTTP addresses from
		/// the configuration.
		/// </summary>
		/// <returns>list of InetSocketAddresses</returns>
		public static IDictionary<string, IDictionary<string, IPEndPoint>> GetHaNnWebHdfsAddresses
			(Configuration conf, string scheme)
		{
			if (WebHdfsFileSystem.Scheme.Equals(scheme))
			{
				return GetAddresses(conf, null, DFSConfigKeys.DfsNamenodeHttpAddressKey);
			}
			else
			{
				if (SWebHdfsFileSystem.Scheme.Equals(scheme))
				{
					return GetAddresses(conf, null, DFSConfigKeys.DfsNamenodeHttpsAddressKey);
				}
				else
				{
					throw new ArgumentException("Unsupported scheme: " + scheme);
				}
			}
		}

		/// <summary>
		/// Returns list of InetSocketAddress corresponding to  backup node rpc
		/// addresses from the configuration.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <returns>list of InetSocketAddresses</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		public static IDictionary<string, IDictionary<string, IPEndPoint>> GetBackupNodeAddresses
			(Configuration conf)
		{
			IDictionary<string, IDictionary<string, IPEndPoint>> addressList = GetAddresses(conf
				, null, DFSConfigKeys.DfsNamenodeBackupAddressKey);
			if (addressList.IsEmpty())
			{
				throw new IOException("Incorrect configuration: backup node address " + DFSConfigKeys
					.DfsNamenodeBackupAddressKey + " is not configured.");
			}
			return addressList;
		}

		/// <summary>
		/// Returns list of InetSocketAddresses of corresponding to secondary namenode
		/// http addresses from the configuration.
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <returns>list of InetSocketAddresses</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		public static IDictionary<string, IDictionary<string, IPEndPoint>> GetSecondaryNameNodeAddresses
			(Configuration conf)
		{
			IDictionary<string, IDictionary<string, IPEndPoint>> addressList = GetAddresses(conf
				, null, DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey);
			if (addressList.IsEmpty())
			{
				throw new IOException("Incorrect configuration: secondary namenode address " + DFSConfigKeys
					.DfsNamenodeSecondaryHttpAddressKey + " is not configured.");
			}
			return addressList;
		}

		/// <summary>
		/// Returns list of InetSocketAddresses corresponding to namenodes from the
		/// configuration.
		/// </summary>
		/// <remarks>
		/// Returns list of InetSocketAddresses corresponding to namenodes from the
		/// configuration.
		/// Returns namenode address specifically configured for datanodes (using
		/// service ports), if found. If not, regular RPC address configured for other
		/// clients is returned.
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <returns>list of InetSocketAddress</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		public static IDictionary<string, IDictionary<string, IPEndPoint>> GetNNServiceRpcAddresses
			(Configuration conf)
		{
			// Use default address as fall back
			string defaultAddress;
			try
			{
				defaultAddress = NetUtils.GetHostPortString(NameNode.GetAddress(conf));
			}
			catch (ArgumentException)
			{
				defaultAddress = null;
			}
			IDictionary<string, IDictionary<string, IPEndPoint>> addressList = GetAddresses(conf
				, defaultAddress, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, DFSConfigKeys.DfsNamenodeRpcAddressKey
				);
			if (addressList.IsEmpty())
			{
				throw new IOException("Incorrect configuration: namenode address " + DFSConfigKeys
					.DfsNamenodeServiceRpcAddressKey + " or " + DFSConfigKeys.DfsNamenodeRpcAddressKey
					 + " is not configured.");
			}
			return addressList;
		}

		/// <summary>
		/// Returns list of InetSocketAddresses corresponding to the namenode
		/// that manages this cluster.
		/// </summary>
		/// <remarks>
		/// Returns list of InetSocketAddresses corresponding to the namenode
		/// that manages this cluster. Note this is to be used by datanodes to get
		/// the list of namenode addresses to talk to.
		/// Returns namenode address specifically configured for datanodes (using
		/// service ports), if found. If not, regular RPC address configured for other
		/// clients is returned.
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <returns>list of InetSocketAddress</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		public static IDictionary<string, IDictionary<string, IPEndPoint>> GetNNServiceRpcAddressesForCluster
			(Configuration conf)
		{
			// Use default address as fall back
			string defaultAddress;
			try
			{
				defaultAddress = NetUtils.GetHostPortString(NameNode.GetAddress(conf));
			}
			catch (ArgumentException)
			{
				defaultAddress = null;
			}
			ICollection<string> parentNameServices = conf.GetTrimmedStringCollection(DFSConfigKeys
				.DfsInternalNameservicesKey);
			if (parentNameServices.IsEmpty())
			{
				parentNameServices = conf.GetTrimmedStringCollection(DFSConfigKeys.DfsNameservices
					);
			}
			else
			{
				// Ensure that the internal service is ineed in the list of all available
				// nameservices.
				ICollection<string> availableNameServices = Sets.NewHashSet(conf.GetTrimmedStringCollection
					(DFSConfigKeys.DfsNameservices));
				foreach (string nsId in parentNameServices)
				{
					if (!availableNameServices.Contains(nsId))
					{
						throw new IOException("Unknown nameservice: " + nsId);
					}
				}
			}
			IDictionary<string, IDictionary<string, IPEndPoint>> addressList = GetAddressesForNsIds
				(conf, parentNameServices, defaultAddress, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey
				, DFSConfigKeys.DfsNamenodeRpcAddressKey);
			if (addressList.IsEmpty())
			{
				throw new IOException("Incorrect configuration: namenode address " + DFSConfigKeys
					.DfsNamenodeServiceRpcAddressKey + " or " + DFSConfigKeys.DfsNamenodeRpcAddressKey
					 + " is not configured.");
			}
			return addressList;
		}

		/// <summary>
		/// Flatten the given map, as returned by other functions in this class,
		/// into a flat list of
		/// <see cref="ConfiguredNNAddress"/>
		/// instances.
		/// </summary>
		public static IList<DFSUtil.ConfiguredNNAddress> FlattenAddressMap(IDictionary<string
			, IDictionary<string, IPEndPoint>> map)
		{
			IList<DFSUtil.ConfiguredNNAddress> ret = Lists.NewArrayList();
			foreach (KeyValuePair<string, IDictionary<string, IPEndPoint>> entry in map)
			{
				string nsId = entry.Key;
				IDictionary<string, IPEndPoint> nnMap = entry.Value;
				foreach (KeyValuePair<string, IPEndPoint> e2 in nnMap)
				{
					string nnId = e2.Key;
					IPEndPoint addr = e2.Value;
					ret.AddItem(new DFSUtil.ConfiguredNNAddress(nsId, nnId, addr));
				}
			}
			return ret;
		}

		/// <summary>
		/// Format the given map, as returned by other functions in this class,
		/// into a string suitable for debugging display.
		/// </summary>
		/// <remarks>
		/// Format the given map, as returned by other functions in this class,
		/// into a string suitable for debugging display. The format of this string
		/// should not be considered an interface, and is liable to change.
		/// </remarks>
		public static string AddressMapToString(IDictionary<string, IDictionary<string, IPEndPoint
			>> map)
		{
			StringBuilder b = new StringBuilder();
			foreach (KeyValuePair<string, IDictionary<string, IPEndPoint>> entry in map)
			{
				string nsId = entry.Key;
				IDictionary<string, IPEndPoint> nnMap = entry.Value;
				b.Append("Nameservice <").Append(nsId).Append(">:").Append("\n");
				foreach (KeyValuePair<string, IPEndPoint> e2 in nnMap)
				{
					b.Append("  NN ID ").Append(e2.Key).Append(" => ").Append(e2.Value).Append("\n");
				}
			}
			return b.ToString();
		}

		public static string NnAddressesAsString(Configuration conf)
		{
			IDictionary<string, IDictionary<string, IPEndPoint>> addresses = GetHaNnRpcAddresses
				(conf);
			return AddressMapToString(addresses);
		}

		/// <summary>Represent one of the NameNodes configured in the cluster.</summary>
		public class ConfiguredNNAddress
		{
			private readonly string nameserviceId;

			private readonly string namenodeId;

			private readonly IPEndPoint addr;

			private ConfiguredNNAddress(string nameserviceId, string namenodeId, IPEndPoint addr
				)
			{
				this.nameserviceId = nameserviceId;
				this.namenodeId = namenodeId;
				this.addr = addr;
			}

			public virtual string GetNameserviceId()
			{
				return nameserviceId;
			}

			public virtual string GetNamenodeId()
			{
				return namenodeId;
			}

			public virtual IPEndPoint GetAddress()
			{
				return addr;
			}

			public override string ToString()
			{
				return "ConfiguredNNAddress[nsId=" + nameserviceId + ";" + "nnId=" + namenodeId +
					 ";addr=" + addr + "]";
			}
		}

		/// <summary>Get a URI for each configured nameservice.</summary>
		/// <remarks>
		/// Get a URI for each configured nameservice. If a nameservice is
		/// HA-enabled, then the logical URI of the nameservice is returned. If the
		/// nameservice is not HA-enabled, then a URI corresponding to an RPC address
		/// of the single NN for that nameservice is returned, preferring the service
		/// RPC address over the client RPC address.
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <returns>
		/// a collection of all configured NN URIs, preferring service
		/// addresses
		/// </returns>
		public static ICollection<URI> GetNsServiceRpcUris(Configuration conf)
		{
			return GetNameServiceUris(conf, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, DFSConfigKeys
				.DfsNamenodeRpcAddressKey);
		}

		/// <summary>Get a URI for each configured nameservice.</summary>
		/// <remarks>
		/// Get a URI for each configured nameservice. If a nameservice is
		/// HA-enabled, then the logical URI of the nameservice is returned. If the
		/// nameservice is not HA-enabled, then a URI corresponding to the address of
		/// the single NN for that nameservice is returned.
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <param name="keys">
		/// configuration keys to try in order to get the URI for non-HA
		/// nameservices
		/// </param>
		/// <returns>a collection of all configured NN URIs</returns>
		public static ICollection<URI> GetNameServiceUris(Configuration conf, params string
			[] keys)
		{
			ICollection<URI> ret = new HashSet<URI>();
			// We're passed multiple possible configuration keys for any given NN or HA
			// nameservice, and search the config in order of these keys. In order to
			// make sure that a later config lookup (e.g. fs.defaultFS) doesn't add a
			// URI for a config key for which we've already found a preferred entry, we
			// keep track of non-preferred keys here.
			ICollection<URI> nonPreferredUris = new HashSet<URI>();
			foreach (string nsId in GetNameServiceIds(conf))
			{
				if (HAUtil.IsHAEnabled(conf, nsId))
				{
					// Add the logical URI of the nameservice.
					try
					{
						ret.AddItem(new URI(HdfsConstants.HdfsUriScheme + "://" + nsId));
					}
					catch (URISyntaxException ue)
					{
						throw new ArgumentException(ue);
					}
				}
				else
				{
					// Add the URI corresponding to the address of the NN.
					bool uriFound = false;
					foreach (string key in keys)
					{
						string addr = conf.Get(ConcatSuffixes(key, nsId));
						if (addr != null)
						{
							URI uri = CreateUri(HdfsConstants.HdfsUriScheme, NetUtils.CreateSocketAddr(addr));
							if (!uriFound)
							{
								uriFound = true;
								ret.AddItem(uri);
							}
							else
							{
								nonPreferredUris.AddItem(uri);
							}
						}
					}
				}
			}
			// Add the generic configuration keys.
			bool uriFound_1 = false;
			foreach (string key_1 in keys)
			{
				string addr = conf.Get(key_1);
				if (addr != null)
				{
					URI uri = CreateUri("hdfs", NetUtils.CreateSocketAddr(addr));
					if (!uriFound_1)
					{
						uriFound_1 = true;
						ret.AddItem(uri);
					}
					else
					{
						nonPreferredUris.AddItem(uri);
					}
				}
			}
			// Add the default URI if it is an HDFS URI.
			URI defaultUri = FileSystem.GetDefaultUri(conf);
			// checks if defaultUri is ip:port format
			// and convert it to hostname:port format
			if (defaultUri != null && (defaultUri.GetPort() != -1))
			{
				defaultUri = CreateUri(defaultUri.GetScheme(), NetUtils.CreateSocketAddr(defaultUri
					.GetHost(), defaultUri.GetPort()));
			}
			if (defaultUri != null && HdfsConstants.HdfsUriScheme.Equals(defaultUri.GetScheme
				()) && !nonPreferredUris.Contains(defaultUri))
			{
				ret.AddItem(defaultUri);
			}
			return ret;
		}

		/// <summary>
		/// Given the InetSocketAddress this method returns the nameservice Id
		/// corresponding to the key with matching address, by doing a reverse
		/// lookup on the list of nameservices until it finds a match.
		/// </summary>
		/// <remarks>
		/// Given the InetSocketAddress this method returns the nameservice Id
		/// corresponding to the key with matching address, by doing a reverse
		/// lookup on the list of nameservices until it finds a match.
		/// Since the process of resolving URIs to Addresses is slightly expensive,
		/// this utility method should not be used in performance-critical routines.
		/// </remarks>
		/// <param name="conf">- configuration</param>
		/// <param name="address">
		/// - InetSocketAddress for configured communication with NN.
		/// Configured addresses are typically given as URIs, but we may have to
		/// compare against a URI typed in by a human, or the server name may be
		/// aliased, so we compare unambiguous InetSocketAddresses instead of just
		/// comparing URI substrings.
		/// </param>
		/// <param name="keys">
		/// - list of configured communication parameters that should
		/// be checked for matches.  For example, to compare against RPC addresses,
		/// provide the list DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
		/// DFS_NAMENODE_RPC_ADDRESS_KEY.  Use the generic parameter keys,
		/// not the NameServiceId-suffixed keys.
		/// </param>
		/// <returns>nameserviceId, or null if no match found</returns>
		public static string GetNameServiceIdFromAddress(Configuration conf, IPEndPoint address
			, params string[] keys)
		{
			// Configuration with a single namenode and no nameserviceId
			string[] ids = GetSuffixIDs(conf, address, keys);
			return (ids != null) ? ids[0] : null;
		}

		/// <summary>
		/// return server http or https address from the configuration for a
		/// given namenode rpc address.
		/// </summary>
		/// <param name="namenodeAddr">- namenode RPC address</param>
		/// <param name="conf">configuration</param>
		/// <param name="scheme">- the scheme (http / https)</param>
		/// <returns>server http or https address</returns>
		/// <exception cref="System.IO.IOException"></exception>
		public static URI GetInfoServer(IPEndPoint namenodeAddr, Configuration conf, string
			 scheme)
		{
			string[] suffixes = null;
			if (namenodeAddr != null)
			{
				// if non-default namenode, try reverse look up 
				// the nameServiceID if it is available
				suffixes = GetSuffixIDs(conf, namenodeAddr, DFSConfigKeys.DfsNamenodeServiceRpcAddressKey
					, DFSConfigKeys.DfsNamenodeRpcAddressKey);
			}
			string authority;
			if ("http".Equals(scheme))
			{
				authority = GetSuffixedConf(conf, DFSConfigKeys.DfsNamenodeHttpAddressKey, DFSConfigKeys
					.DfsNamenodeHttpAddressDefault, suffixes);
			}
			else
			{
				if ("https".Equals(scheme))
				{
					authority = GetSuffixedConf(conf, DFSConfigKeys.DfsNamenodeHttpsAddressKey, DFSConfigKeys
						.DfsNamenodeHttpsAddressDefault, suffixes);
				}
				else
				{
					throw new ArgumentException("Invalid scheme:" + scheme);
				}
			}
			if (namenodeAddr != null)
			{
				authority = SubstituteForWildcardAddress(authority, namenodeAddr.GetHostName());
			}
			return URI.Create(scheme + "://" + authority);
		}

		/// <summary>
		/// Lookup the HTTP / HTTPS address of the namenode, and replace its hostname
		/// with defaultHost when it found out that the address is a wildcard / local
		/// address.
		/// </summary>
		/// <param name="defaultHost">The default host name of the namenode.</param>
		/// <param name="conf">The configuration</param>
		/// <param name="scheme">HTTP or HTTPS</param>
		/// <exception cref="System.IO.IOException"/>
		public static URI GetInfoServerWithDefaultHost(string defaultHost, Configuration 
			conf, string scheme)
		{
			URI configuredAddr = GetInfoServer(null, conf, scheme);
			string authority = SubstituteForWildcardAddress(configuredAddr.GetAuthority(), defaultHost
				);
			return URI.Create(scheme + "://" + authority);
		}

		/// <summary>
		/// Determine whether HTTP or HTTPS should be used to connect to the remote
		/// server.
		/// </summary>
		/// <remarks>
		/// Determine whether HTTP or HTTPS should be used to connect to the remote
		/// server. Currently the client only connects to the server via HTTPS if the
		/// policy is set to HTTPS_ONLY.
		/// </remarks>
		/// <returns>the scheme (HTTP / HTTPS)</returns>
		public static string GetHttpClientScheme(Configuration conf)
		{
			HttpConfig.Policy policy = DFSUtil.GetHttpPolicy(conf);
			return policy == HttpConfig.Policy.HttpsOnly ? "https" : "http";
		}

		/// <summary>
		/// Substitute a default host in the case that an address has been configured
		/// with a wildcard.
		/// </summary>
		/// <remarks>
		/// Substitute a default host in the case that an address has been configured
		/// with a wildcard. This is used, for example, when determining the HTTP
		/// address of the NN -- if it's configured to bind to 0.0.0.0, we want to
		/// substitute the hostname from the filesystem URI rather than trying to
		/// connect to 0.0.0.0.
		/// </remarks>
		/// <param name="configuredAddress">the address found in the configuration</param>
		/// <param name="defaultHost">
		/// the host to substitute with, if configuredAddress
		/// is a local/wildcard address.
		/// </param>
		/// <returns>the substituted address</returns>
		/// <exception cref="System.IO.IOException">if it is a wildcard address and security is enabled
		/// 	</exception>
		[VisibleForTesting]
		internal static string SubstituteForWildcardAddress(string configuredAddress, string
			 defaultHost)
		{
			IPEndPoint sockAddr = NetUtils.CreateSocketAddr(configuredAddress);
			IPEndPoint defaultSockAddr = NetUtils.CreateSocketAddr(defaultHost + ":0");
			IPAddress addr = sockAddr.Address;
			if (addr != null && addr.IsAnyLocalAddress())
			{
				if (UserGroupInformation.IsSecurityEnabled() && defaultSockAddr.Address.IsAnyLocalAddress
					())
				{
					throw new IOException("Cannot use a wildcard address with security. " + "Must explicitly set bind address for Kerberos"
						);
				}
				return defaultHost + ":" + sockAddr.Port;
			}
			else
			{
				return configuredAddress;
			}
		}

		private static string GetSuffixedConf(Configuration conf, string key, string defaultVal
			, string[] suffixes)
		{
			string ret = conf.Get(DFSUtil.AddKeySuffixes(key, suffixes));
			if (ret != null)
			{
				return ret;
			}
			return conf.Get(key, defaultVal);
		}

		/// <summary>Sets the node specific setting into generic configuration key.</summary>
		/// <remarks>
		/// Sets the node specific setting into generic configuration key. Looks up
		/// value of "key.nameserviceId.namenodeId" and if found sets that value into
		/// generic key in the conf. If this is not found, falls back to
		/// "key.nameserviceId" and then the unmodified key.
		/// Note that this only modifies the runtime conf.
		/// </remarks>
		/// <param name="conf">
		/// Configuration object to lookup specific key and to set the value
		/// to the key passed. Note the conf object is modified.
		/// </param>
		/// <param name="nameserviceId">
		/// nameservice Id to construct the node specific key. Pass null if
		/// federation is not configuration.
		/// </param>
		/// <param name="nnId">
		/// namenode Id to construct the node specific key. Pass null if
		/// HA is not configured.
		/// </param>
		/// <param name="keys">The key for which node specific value is looked up</param>
		public static void SetGenericConf(Configuration conf, string nameserviceId, string
			 nnId, params string[] keys)
		{
			foreach (string key in keys)
			{
				string value = conf.Get(AddKeySuffixes(key, nameserviceId, nnId));
				if (value != null)
				{
					conf.Set(key, value);
					continue;
				}
				value = conf.Get(AddKeySuffixes(key, nameserviceId));
				if (value != null)
				{
					conf.Set(key, value);
				}
			}
		}

		/// <summary>Return used as percentage of capacity</summary>
		public static float GetPercentUsed(long used, long capacity)
		{
			return capacity <= 0 ? 100 : (used * 100.0f) / capacity;
		}

		/// <summary>Return remaining as percentage of capacity</summary>
		public static float GetPercentRemaining(long remaining, long capacity)
		{
			return capacity <= 0 ? 0 : (remaining * 100.0f) / capacity;
		}

		/// <summary>Convert percentage to a string.</summary>
		public static string Percent2String(double percentage)
		{
			return StringUtils.Format("%.2f%%", percentage);
		}

		/// <summary>Round bytes to GiB (gibibyte)</summary>
		/// <param name="bytes">number of bytes</param>
		/// <returns>number of GiB</returns>
		public static int RoundBytesToGB(long bytes)
		{
			return Math.Round((float)bytes / 1024 / 1024 / 1024);
		}

		/// <summary>
		/// Create a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientDatanodeProtocol"/>
		/// proxy
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static ClientDatanodeProtocol CreateClientDatanodeProtocolProxy(DatanodeID
			 datanodeid, Configuration conf, int socketTimeout, bool connectToDnViaHostname, 
			LocatedBlock locatedBlock)
		{
			return new ClientDatanodeProtocolTranslatorPB(datanodeid, conf, socketTimeout, connectToDnViaHostname
				, locatedBlock);
		}

		/// <summary>
		/// Create
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientDatanodeProtocol"/>
		/// proxy using kerberos ticket
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static ClientDatanodeProtocol CreateClientDatanodeProtocolProxy(DatanodeID
			 datanodeid, Configuration conf, int socketTimeout, bool connectToDnViaHostname)
		{
			return new ClientDatanodeProtocolTranslatorPB(datanodeid, conf, socketTimeout, connectToDnViaHostname
				);
		}

		/// <summary>
		/// Create a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientDatanodeProtocol"/>
		/// proxy
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static ClientDatanodeProtocol CreateClientDatanodeProtocolProxy(IPEndPoint
			 addr, UserGroupInformation ticket, Configuration conf, SocketFactory factory)
		{
			return new ClientDatanodeProtocolTranslatorPB(addr, ticket, conf, factory);
		}

		/// <summary>
		/// Get nameservice Id for the
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode"/>
		/// based on namenode RPC address
		/// matching the local node address.
		/// </summary>
		public static string GetNamenodeNameServiceId(Configuration conf)
		{
			return GetNameServiceId(conf, DFSConfigKeys.DfsNamenodeRpcAddressKey);
		}

		/// <summary>
		/// Get nameservice Id for the BackupNode based on backup node RPC address
		/// matching the local node address.
		/// </summary>
		public static string GetBackupNameServiceId(Configuration conf)
		{
			return GetNameServiceId(conf, DFSConfigKeys.DfsNamenodeBackupAddressKey);
		}

		/// <summary>
		/// Get nameservice Id for the secondary node based on secondary http address
		/// matching the local node address.
		/// </summary>
		public static string GetSecondaryNameServiceId(Configuration conf)
		{
			return GetNameServiceId(conf, DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey);
		}

		/// <summary>
		/// Get the nameservice Id by matching the
		/// <paramref name="addressKey"/>
		/// with the
		/// the address of the local node.
		/// If
		/// <see cref="DFSConfigKeys.DfsNameserviceId"/>
		/// is not specifically
		/// configured, and more than one nameservice Id is configured, this method
		/// determines the nameservice Id by matching the local node's address with the
		/// configured addresses. When a match is found, it returns the nameservice Id
		/// from the corresponding configuration key.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <param name="addressKey">configuration key to get the address.</param>
		/// <returns>nameservice Id on success, null if federation is not configured.</returns>
		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException">on error</exception>
		private static string GetNameServiceId(Configuration conf, string addressKey)
		{
			string nameserviceId = conf.Get(DFSConfigKeys.DfsNameserviceId);
			if (nameserviceId != null)
			{
				return nameserviceId;
			}
			ICollection<string> nsIds = GetNameServiceIds(conf);
			if (1 == nsIds.Count)
			{
				return Sharpen.Collections.ToArray(nsIds, new string[1])[0];
			}
			string nnId = conf.Get(DFSConfigKeys.DfsHaNamenodeIdKey);
			return GetSuffixIDs(conf, addressKey, null, nnId, LocalAddressMatcher)[0];
		}

		/// <summary>
		/// Returns nameservice Id and namenode Id when the local host matches the
		/// configuration parameter
		/// <paramref name="addressKey"/>
		/// .<nameservice Id>.<namenode Id>
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <param name="addressKey">configuration key corresponding to the address.</param>
		/// <param name="knownNsId">only look at configs for the given nameservice, if not-null
		/// 	</param>
		/// <param name="knownNNId">only look at configs for the given namenode, if not null</param>
		/// <param name="matcher">matching criteria for matching the address</param>
		/// <returns>
		/// Array with nameservice Id and namenode Id on success. First element
		/// in the array is nameservice Id and second element is namenode Id.
		/// Null value indicates that the configuration does not have the the
		/// Id.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException">on error</exception>
		internal static string[] GetSuffixIDs(Configuration conf, string addressKey, string
			 knownNsId, string knownNNId, DFSUtil.AddressMatcher matcher)
		{
			string nameserviceId = null;
			string namenodeId = null;
			int found = 0;
			ICollection<string> nsIds = GetNameServiceIds(conf);
			foreach (string nsId in EmptyAsSingletonNull(nsIds))
			{
				if (knownNsId != null && !knownNsId.Equals(nsId))
				{
					continue;
				}
				ICollection<string> nnIds = GetNameNodeIds(conf, nsId);
				foreach (string nnId in EmptyAsSingletonNull(nnIds))
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace(string.Format("addressKey: %s nsId: %s nnId: %s", addressKey, nsId, nnId
							));
					}
					if (knownNNId != null && !knownNNId.Equals(nnId))
					{
						continue;
					}
					string key = AddKeySuffixes(addressKey, nsId, nnId);
					string addr = conf.Get(key);
					if (addr == null)
					{
						continue;
					}
					IPEndPoint s = null;
					try
					{
						s = NetUtils.CreateSocketAddr(addr);
					}
					catch (Exception e)
					{
						Log.Warn("Exception in creating socket address " + addr, e);
						continue;
					}
					if (!s.IsUnresolved() && matcher.Match(s))
					{
						nameserviceId = nsId;
						namenodeId = nnId;
						found++;
					}
				}
			}
			if (found > 1)
			{
				// Only one address must match the local address
				string msg = "Configuration has multiple addresses that match " + "local node's address. Please configure the system with "
					 + DFSConfigKeys.DfsNameserviceId + " and " + DFSConfigKeys.DfsHaNamenodeIdKey;
				throw new HadoopIllegalArgumentException(msg);
			}
			return new string[] { nameserviceId, namenodeId };
		}

		/// <summary>
		/// For given set of
		/// <paramref name="keys"/>
		/// adds nameservice Id and or namenode Id
		/// and returns {nameserviceId, namenodeId} when address match is found.
		/// </summary>
		/// <seealso cref="GetSuffixIDs(Org.Apache.Hadoop.Conf.Configuration, string, string, string, AddressMatcher)
		/// 	"/>
		internal static string[] GetSuffixIDs(Configuration conf, IPEndPoint address, params 
			string[] keys)
		{
			DFSUtil.AddressMatcher matcher = new _AddressMatcher_1412(address);
			foreach (string key in keys)
			{
				string[] ids = GetSuffixIDs(conf, key, null, null, matcher);
				if (ids != null && (ids[0] != null || ids[1] != null))
				{
					return ids;
				}
			}
			return null;
		}

		private sealed class _AddressMatcher_1412 : DFSUtil.AddressMatcher
		{
			public _AddressMatcher_1412(IPEndPoint address)
			{
				this.address = address;
			}

			public bool Match(IPEndPoint s)
			{
				return address.Equals(s);
			}

			private readonly IPEndPoint address;
		}

		private interface AddressMatcher
		{
			bool Match(IPEndPoint s);
		}

		/// <summary>Create a URI from the scheme and address</summary>
		public static URI CreateUri(string scheme, IPEndPoint address)
		{
			try
			{
				return new URI(scheme, null, address.GetHostName(), address.Port, null, null, null
					);
			}
			catch (URISyntaxException ue)
			{
				throw new ArgumentException(ue);
			}
		}

		/// <summary>
		/// Add protobuf based protocol to the
		/// <see cref="Org.Apache.Hadoop.Ipc.RPC.Server"/>
		/// </summary>
		/// <param name="conf">configuration</param>
		/// <param name="protocol">Protocol interface</param>
		/// <param name="service">service that implements the protocol</param>
		/// <param name="server">RPC server to which the protocol & implementation is added to
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		public static void AddPBProtocol(Configuration conf, Type protocol, BlockingService
			 service, RPC.Server server)
		{
			RPC.SetProtocolEngine(conf, protocol, typeof(ProtobufRpcEngine));
			server.AddProtocol(RPC.RpcKind.RpcProtocolBuffer, protocol, service);
		}

		/// <summary>Map a logical namenode ID to its service address.</summary>
		/// <remarks>
		/// Map a logical namenode ID to its service address. Use the given
		/// nameservice if specified, or the configured one if none is given.
		/// </remarks>
		/// <param name="conf">Configuration</param>
		/// <param name="nsId">which nameservice nnId is a part of, optional</param>
		/// <param name="nnId">the namenode ID to get the service addr for</param>
		/// <returns>the service addr, null if it could not be determined</returns>
		public static string GetNamenodeServiceAddr(Configuration conf, string nsId, string
			 nnId)
		{
			if (nsId == null)
			{
				nsId = GetOnlyNameServiceIdOrNull(conf);
			}
			string serviceAddrKey = ConcatSuffixes(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey
				, nsId, nnId);
			string addrKey = ConcatSuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, nsId, nnId
				);
			string serviceRpcAddr = conf.Get(serviceAddrKey);
			if (serviceRpcAddr == null)
			{
				serviceRpcAddr = conf.Get(addrKey);
			}
			return serviceRpcAddr;
		}

		/// <summary>
		/// If the configuration refers to only a single nameservice, return the
		/// name of that nameservice.
		/// </summary>
		/// <remarks>
		/// If the configuration refers to only a single nameservice, return the
		/// name of that nameservice. If it refers to 0 or more than 1, return null.
		/// </remarks>
		public static string GetOnlyNameServiceIdOrNull(Configuration conf)
		{
			ICollection<string> nsIds = GetNameServiceIds(conf);
			if (1 == nsIds.Count)
			{
				return Sharpen.Collections.ToArray(nsIds, new string[1])[0];
			}
			else
			{
				// No nameservice ID was given and more than one is configured
				return null;
			}
		}

		public static readonly Options helpOptions = new Options();

		public static readonly Option helpOpt = new Option("h", "help", false, "get help information"
			);

		static DFSUtil()
		{
			helpOptions.AddOption(helpOpt);
		}

		/// <summary>Parse the arguments for commands</summary>
		/// <param name="args">the argument to be parsed</param>
		/// <param name="helpDescription">help information to be printed out</param>
		/// <param name="out">Printer</param>
		/// <param name="printGenericCommandUsage">
		/// whether to print the
		/// generic command usage defined in ToolRunner
		/// </param>
		/// <returns>true when the argument matches help option, false if not</returns>
		public static bool ParseHelpArgument(string[] args, string helpDescription, TextWriter
			 @out, bool printGenericCommandUsage)
		{
			if (args.Length == 1)
			{
				try
				{
					CommandLineParser parser = new PosixParser();
					CommandLine cmdLine = parser.Parse(helpOptions, args);
					if (cmdLine.HasOption(helpOpt.GetOpt()) || cmdLine.HasOption(helpOpt.GetLongOpt()
						))
					{
						// should print out the help information
						@out.WriteLine(helpDescription + "\n");
						if (printGenericCommandUsage)
						{
							ToolRunner.PrintGenericCommandUsage(@out);
						}
						return true;
					}
				}
				catch (ParseException)
				{
					return false;
				}
			}
			return false;
		}

		/// <summary>Get DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION from configuration.</summary>
		/// <param name="conf">Configuration</param>
		/// <returns>Value of DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION</returns>
		public static float GetInvalidateWorkPctPerIteration(Configuration conf)
		{
			float blocksInvalidateWorkPct = conf.GetFloat(DFSConfigKeys.DfsNamenodeInvalidateWorkPctPerIteration
				, DFSConfigKeys.DfsNamenodeInvalidateWorkPctPerIterationDefault);
			Preconditions.CheckArgument((blocksInvalidateWorkPct > 0 && blocksInvalidateWorkPct
				 <= 1.0f), DFSConfigKeys.DfsNamenodeInvalidateWorkPctPerIteration + " = '" + blocksInvalidateWorkPct
				 + "' is invalid. " + "It should be a positive, non-zero float value, not greater than 1.0f, "
				 + "to indicate a percentage.");
			return blocksInvalidateWorkPct;
		}

		/// <summary>
		/// Get DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION from
		/// configuration.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <returns>Value of DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION</returns>
		public static int GetReplWorkMultiplier(Configuration conf)
		{
			int blocksReplWorkMultiplier = conf.GetInt(DFSConfigKeys.DfsNamenodeReplicationWorkMultiplierPerIteration
				, DFSConfigKeys.DfsNamenodeReplicationWorkMultiplierPerIterationDefault);
			Preconditions.CheckArgument((blocksReplWorkMultiplier > 0), DFSConfigKeys.DfsNamenodeReplicationWorkMultiplierPerIteration
				 + " = '" + blocksReplWorkMultiplier + "' is invalid. " + "It should be a positive, non-zero integer value."
				);
			return blocksReplWorkMultiplier;
		}

		/// <summary>Get SPNEGO keytab Key from configuration</summary>
		/// <param name="conf">Configuration</param>
		/// <param name="defaultKey">default key to be used for config lookup</param>
		/// <returns>
		/// DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY if the key is not empty
		/// else return defaultKey
		/// </returns>
		public static string GetSpnegoKeytabKey(Configuration conf, string defaultKey)
		{
			string value = conf.Get(DFSConfigKeys.DfsWebAuthenticationKerberosKeytabKey);
			return (value == null || value.IsEmpty()) ? defaultKey : DFSConfigKeys.DfsWebAuthenticationKerberosKeytabKey;
		}

		/// <summary>Get http policy.</summary>
		/// <remarks>
		/// Get http policy. Http Policy is chosen as follows:
		/// <ol>
		/// <li>If hadoop.ssl.enabled is set, http endpoints are not started. Only
		/// https endpoints are started on configured https ports</li>
		/// <li>This configuration is overridden by dfs.https.enable configuration, if
		/// it is set to true. In that case, both http and https endpoints are stared.</li>
		/// <li>All the above configurations are overridden by dfs.http.policy
		/// configuration. With this configuration you can set http-only, https-only
		/// and http-and-https endpoints.</li>
		/// </ol>
		/// See hdfs-default.xml documentation for more details on each of the above
		/// configuration settings.
		/// </remarks>
		public static HttpConfig.Policy GetHttpPolicy(Configuration conf)
		{
			string policyStr = conf.Get(DFSConfigKeys.DfsHttpPolicyKey);
			if (policyStr == null)
			{
				bool https = conf.GetBoolean(DFSConfigKeys.DfsHttpsEnableKey, DFSConfigKeys.DfsHttpsEnableDefault
					);
				bool hadoopSsl = conf.GetBoolean(CommonConfigurationKeys.HadoopSslEnabledKey, CommonConfigurationKeys
					.HadoopSslEnabledDefault);
				if (hadoopSsl)
				{
					Log.Warn(CommonConfigurationKeys.HadoopSslEnabledKey + " is deprecated. Please use "
						 + DFSConfigKeys.DfsHttpPolicyKey + ".");
				}
				if (https)
				{
					Log.Warn(DFSConfigKeys.DfsHttpsEnableKey + " is deprecated. Please use " + DFSConfigKeys
						.DfsHttpPolicyKey + ".");
				}
				return (hadoopSsl || https) ? HttpConfig.Policy.HttpAndHttps : HttpConfig.Policy.
					HttpOnly;
			}
			HttpConfig.Policy policy = HttpConfig.Policy.FromString(policyStr);
			if (policy == null)
			{
				throw new HadoopIllegalArgumentException("Unregonized value '" + policyStr + "' for "
					 + DFSConfigKeys.DfsHttpPolicyKey);
			}
			conf.Set(DFSConfigKeys.DfsHttpPolicyKey, policy.ToString());
			return policy;
		}

		public static HttpServer2.Builder LoadSslConfToHttpServerBuilder(HttpServer2.Builder
			 builder, Configuration sslConf)
		{
			return builder.NeedsClientAuth(sslConf.GetBoolean(DFSConfigKeys.DfsClientHttpsNeedAuthKey
				, DFSConfigKeys.DfsClientHttpsNeedAuthDefault)).KeyPassword(GetPassword(sslConf, 
				DFSConfigKeys.DfsServerHttpsKeypasswordKey)).KeyStore(sslConf.Get("ssl.server.keystore.location"
				), GetPassword(sslConf, DFSConfigKeys.DfsServerHttpsKeystorePasswordKey), sslConf
				.Get("ssl.server.keystore.type", "jks")).TrustStore(sslConf.Get("ssl.server.truststore.location"
				), GetPassword(sslConf, DFSConfigKeys.DfsServerHttpsTruststorePasswordKey), sslConf
				.Get("ssl.server.truststore.type", "jks"));
		}

		/// <summary>Load HTTPS-related configuration.</summary>
		public static Configuration LoadSslConfiguration(Configuration conf)
		{
			Configuration sslConf = new Configuration(false);
			sslConf.AddResource(conf.Get(DFSConfigKeys.DfsServerHttpsKeystoreResourceKey, DFSConfigKeys
				.DfsServerHttpsKeystoreResourceDefault));
			bool requireClientAuth = conf.GetBoolean(DFSConfigKeys.DfsClientHttpsNeedAuthKey, 
				DFSConfigKeys.DfsClientHttpsNeedAuthDefault);
			sslConf.SetBoolean(DFSConfigKeys.DfsClientHttpsNeedAuthKey, requireClientAuth);
			return sslConf;
		}

		/// <summary>
		/// Return a HttpServer.Builder that the journalnode / namenode / secondary
		/// namenode can use to initialize their HTTP / HTTPS server.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static HttpServer2.Builder HttpServerTemplateForNNAndJN(Configuration conf
			, IPEndPoint httpAddr, IPEndPoint httpsAddr, string name, string spnegoUserNameKey
			, string spnegoKeytabFileKey)
		{
			HttpConfig.Policy policy = GetHttpPolicy(conf);
			HttpServer2.Builder builder = new HttpServer2.Builder().SetName(name).SetConf(conf
				).SetACL(new AccessControlList(conf.Get(DFSConfigKeys.DfsAdmin, " "))).SetSecurityEnabled
				(UserGroupInformation.IsSecurityEnabled()).SetUsernameConfKey(spnegoUserNameKey)
				.SetKeytabConfKey(GetSpnegoKeytabKey(conf, spnegoKeytabFileKey));
			// initialize the webserver for uploading/downloading files.
			if (UserGroupInformation.IsSecurityEnabled())
			{
				Log.Info("Starting web server as: " + SecurityUtil.GetServerPrincipal(conf.Get(spnegoUserNameKey
					), httpAddr.GetHostName()));
			}
			if (policy.IsHttpEnabled())
			{
				if (httpAddr.Port == 0)
				{
					builder.SetFindPort(true);
				}
				URI uri = URI.Create("http://" + NetUtils.GetHostPortString(httpAddr));
				builder.AddEndpoint(uri);
				Log.Info("Starting Web-server for " + name + " at: " + uri);
			}
			if (policy.IsHttpsEnabled() && httpsAddr != null)
			{
				Configuration sslConf = LoadSslConfiguration(conf);
				LoadSslConfToHttpServerBuilder(builder, sslConf);
				if (httpsAddr.Port == 0)
				{
					builder.SetFindPort(true);
				}
				URI uri = URI.Create("https://" + NetUtils.GetHostPortString(httpsAddr));
				builder.AddEndpoint(uri);
				Log.Info("Starting Web-server for " + name + " at: " + uri);
			}
			return builder;
		}

		/// <summary>
		/// Leverages the Configuration.getPassword method to attempt to get
		/// passwords from the CredentialProvider API before falling back to
		/// clear text in config - if falling back is allowed.
		/// </summary>
		/// <param name="conf">Configuration instance</param>
		/// <param name="alias">name of the credential to retreive</param>
		/// <returns>String credential value or null</returns>
		internal static string GetPassword(Configuration conf, string alias)
		{
			string password = null;
			try
			{
				char[] passchars = conf.GetPassword(alias);
				if (passchars != null)
				{
					password = new string(passchars);
				}
			}
			catch (IOException)
			{
				password = null;
			}
			return password;
		}

		/// <summary>Converts a Date into an ISO-8601 formatted datetime string.</summary>
		public static string DateToIso8601String(DateTime date)
		{
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Sharpen.Extensions.GetEnglishCulture()
				);
			return df.Format(date);
		}

		/// <summary>Converts a time duration in milliseconds into DDD:HH:MM:SS format.</summary>
		public static string DurationToString(long durationMs)
		{
			bool negative = false;
			if (durationMs < 0)
			{
				negative = true;
				durationMs = -durationMs;
			}
			// Chop off the milliseconds
			long durationSec = durationMs / 1000;
			int secondsPerMinute = 60;
			int secondsPerHour = 60 * 60;
			int secondsPerDay = 60 * 60 * 24;
			long days = durationSec / secondsPerDay;
			durationSec -= days * secondsPerDay;
			long hours = durationSec / secondsPerHour;
			durationSec -= hours * secondsPerHour;
			long minutes = durationSec / secondsPerMinute;
			durationSec -= minutes * secondsPerMinute;
			long seconds = durationSec;
			long milliseconds = durationMs % 1000;
			string format = "%03d:%02d:%02d:%02d.%03d";
			if (negative)
			{
				format = "-" + format;
			}
			return string.Format(format, days, hours, minutes, seconds, milliseconds);
		}

		/// <summary>Converts a relative time string into a duration in milliseconds.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static long ParseRelativeTime(string relTime)
		{
			if (relTime.Length < 2)
			{
				throw new IOException("Unable to parse relative time value of " + relTime + ": too short"
					);
			}
			string ttlString = Sharpen.Runtime.Substring(relTime, 0, relTime.Length - 1);
			long ttl;
			try
			{
				ttl = long.Parse(ttlString);
			}
			catch (FormatException)
			{
				throw new IOException("Unable to parse relative time value of " + relTime + ": " 
					+ ttlString + " is not a number");
			}
			if (relTime.EndsWith("s"))
			{
			}
			else
			{
				// pass
				if (relTime.EndsWith("m"))
				{
					ttl *= 60;
				}
				else
				{
					if (relTime.EndsWith("h"))
					{
						ttl *= 60 * 60;
					}
					else
					{
						if (relTime.EndsWith("d"))
						{
							ttl *= 60 * 60 * 24;
						}
						else
						{
							throw new IOException("Unable to parse relative time value of " + relTime + ": unknown time unit "
								 + relTime[relTime.Length - 1]);
						}
					}
				}
			}
			return ttl * 1000;
		}

		/// <summary>Assert that all objects in the collection are equal.</summary>
		/// <remarks>
		/// Assert that all objects in the collection are equal. Returns silently if
		/// so, throws an AssertionError if any object is not equal. All null values
		/// are considered equal.
		/// </remarks>
		/// <param name="objects">the collection of objects to check for equality.</param>
		/// <exception cref="System.Exception"/>
		public static void AssertAllResultsEqual<_T0>(ICollection<_T0> objects)
		{
			if (objects.Count == 0 || objects.Count == 1)
			{
				return;
			}
			object[] resultsArray = Sharpen.Collections.ToArray(objects);
			for (int i = 1; i < resultsArray.Length; i++)
			{
				object currElement = resultsArray[i];
				object lastElement = resultsArray[i - 1];
				if ((currElement == null && currElement != lastElement) || (currElement != null &&
					 !currElement.Equals(lastElement)))
				{
					throw new Exception("Not all elements match in results: " + Arrays.ToString(resultsArray
						));
				}
			}
		}

		/// <summary>Creates a new KeyProvider from the given Configuration.</summary>
		/// <param name="conf">Configuration</param>
		/// <returns>new KeyProvider, or null if no provider was found.</returns>
		/// <exception cref="System.IO.IOException">
		/// if the KeyProvider is improperly specified in
		/// the Configuration
		/// </exception>
		public static KeyProvider CreateKeyProvider(Configuration conf)
		{
			string providerUriStr = conf.GetTrimmed(DFSConfigKeys.DfsEncryptionKeyProviderUri
				, string.Empty);
			// No provider set in conf
			if (providerUriStr.IsEmpty())
			{
				return null;
			}
			URI providerUri;
			try
			{
				providerUri = new URI(providerUriStr);
			}
			catch (URISyntaxException e)
			{
				throw new IOException(e);
			}
			KeyProvider keyProvider = KeyProviderFactory.Get(providerUri, conf);
			if (keyProvider == null)
			{
				throw new IOException("Could not instantiate KeyProvider from " + DFSConfigKeys.DfsEncryptionKeyProviderUri
					 + " setting of '" + providerUriStr + "'");
			}
			if (keyProvider.IsTransient())
			{
				throw new IOException("KeyProvider " + keyProvider.ToString() + " was found but it is a transient provider."
					);
			}
			return keyProvider;
		}

		/// <summary>
		/// Creates a new KeyProviderCryptoExtension by wrapping the
		/// KeyProvider specified in the given Configuration.
		/// </summary>
		/// <param name="conf">Configuration</param>
		/// <returns>new KeyProviderCryptoExtension, or null if no provider was found.</returns>
		/// <exception cref="System.IO.IOException">
		/// if the KeyProvider is improperly specified in
		/// the Configuration
		/// </exception>
		public static KeyProviderCryptoExtension CreateKeyProviderCryptoExtension(Configuration
			 conf)
		{
			KeyProvider keyProvider = CreateKeyProvider(conf);
			if (keyProvider == null)
			{
				return null;
			}
			KeyProviderCryptoExtension cryptoProvider = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension
				(keyProvider);
			return cryptoProvider;
		}

		/// <summary>
		/// Probe for HDFS Encryption being enabled; this uses the value of
		/// the option
		/// <see cref="DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI"/>
		/// ,
		/// returning true if that property contains a non-empty, non-whitespace
		/// string.
		/// </summary>
		/// <param name="conf">configuration to probe</param>
		/// <returns>true if encryption is considered enabled.</returns>
		public static bool IsHDFSEncryptionEnabled(Configuration conf)
		{
			return !conf.GetTrimmed(DFSConfigKeys.DfsEncryptionKeyProviderUri, string.Empty).
				IsEmpty();
		}
	}
}
