using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Manage datanodes, include decommission and other activities.</summary>
	public class DatanodeManager
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeManager
			));

		private readonly Namesystem namesystem;

		private readonly BlockManager blockManager;

		private readonly DecommissionManager decomManager;

		private readonly HeartbeatManager heartbeatManager;

		private readonly FSClusterStats fsClusterStats;

		/// <summary>Stores the datanode -&gt; block map.</summary>
		/// <remarks>
		/// Stores the datanode -&gt; block map.
		/// <p>
		/// Done by storing a set of
		/// <see cref="DatanodeDescriptor"/>
		/// objects, sorted by
		/// storage id. In order to keep the storage map consistent it tracks
		/// all storages ever registered with the namenode.
		/// A descriptor corresponding to a specific storage id can be
		/// <ul>
		/// <li>added to the map if it is a new storage id;</li>
		/// <li>updated with a new datanode started as a replacement for the old one
		/// with the same storage id; and </li>
		/// <li>removed if and only if an existing datanode is restarted to serve a
		/// different storage id.</li>
		/// </ul> <br />
		/// <p>
		/// Mapping: StorageID -&gt; DatanodeDescriptor
		/// </remarks>
		private readonly NavigableMap<string, DatanodeDescriptor> datanodeMap = new SortedDictionary
			<string, DatanodeDescriptor>();

		/// <summary>Cluster network topology</summary>
		private readonly NetworkTopology networktopology;

		/// <summary>Host names to datanode descriptors mapping.</summary>
		private readonly Host2NodesMap host2DatanodeMap = new Host2NodesMap();

		private readonly DNSToSwitchMapping dnsToSwitchMapping;

		private readonly bool rejectUnresolvedTopologyDN;

		private readonly int defaultXferPort;

		private readonly int defaultInfoPort;

		private readonly int defaultInfoSecurePort;

		private readonly int defaultIpcPort;

		/// <summary>Read include/exclude files</summary>
		private readonly HostFileManager hostFileManager = new HostFileManager();

		/// <summary>The period to wait for datanode heartbeat.</summary>
		private long heartbeatExpireInterval;

		/// <summary>Ask Datanode only up to this many blocks to delete.</summary>
		internal readonly int blockInvalidateLimit;

		/// <summary>The interval for judging stale DataNodes for read/write</summary>
		private readonly long staleInterval;

		/// <summary>Whether or not to avoid using stale DataNodes for reading</summary>
		private readonly bool avoidStaleDataNodesForRead;

		/// <summary>Whether or not to avoid using stale DataNodes for writing.</summary>
		/// <remarks>
		/// Whether or not to avoid using stale DataNodes for writing.
		/// Note that, even if this is configured, the policy may be
		/// temporarily disabled when a high percentage of the nodes
		/// are marked as stale.
		/// </remarks>
		private readonly bool avoidStaleDataNodesForWrite;

		/// <summary>
		/// When the ratio of stale datanodes reaches this number, stop avoiding
		/// writing to stale datanodes, i.e., continue using stale nodes for writing.
		/// </summary>
		private readonly float ratioUseStaleDataNodesForWrite;

		/// <summary>The number of stale DataNodes</summary>
		private volatile int numStaleNodes;

		/// <summary>The number of stale storages</summary>
		private volatile int numStaleStorages;

		/// <summary>Number of blocks to check for each postponedMisreplicatedBlocks iteration
		/// 	</summary>
		private readonly long blocksPerPostponedMisreplicatedBlocksRescan;

		/// <summary>
		/// Whether or not this cluster has ever consisted of more than 1 rack,
		/// according to the NetworkTopology.
		/// </summary>
		private bool hasClusterEverBeenMultiRack = false;

		private readonly bool checkIpHostnameInRegistration;

		/// <summary>
		/// Whether we should tell datanodes what to cache in replies to
		/// heartbeat messages.
		/// </summary>
		private bool shouldSendCachingCommands = false;

		/// <summary>The number of datanodes for each software version.</summary>
		/// <remarks>
		/// The number of datanodes for each software version. This list should change
		/// during rolling upgrades.
		/// Software version -&gt; Number of datanodes with this version
		/// </remarks>
		private Dictionary<string, int> datanodesSoftwareVersions = new Dictionary<string
			, int>(4, 0.75f);

		/// <summary>
		/// The minimum time between resending caching directives to Datanodes,
		/// in milliseconds.
		/// </summary>
		/// <remarks>
		/// The minimum time between resending caching directives to Datanodes,
		/// in milliseconds.
		/// Note that when a rescan happens, we will send the new directives
		/// as soon as possible.  This timeout only applies to resending
		/// directives that we've already sent.
		/// </remarks>
		private readonly long timeBetweenResendingCachingDirectivesMs;

		/// <exception cref="System.IO.IOException"/>
		internal DatanodeManager(BlockManager blockManager, Namesystem namesystem, Configuration
			 conf)
		{
			this.namesystem = namesystem;
			this.blockManager = blockManager;
			this.heartbeatManager = new HeartbeatManager(namesystem, blockManager, conf);
			this.decomManager = new DecommissionManager(namesystem, blockManager, heartbeatManager
				);
			this.fsClusterStats = NewFSClusterStats();
			networktopology = NetworkTopology.GetInstance(conf);
			this.defaultXferPort = NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys.DfsDatanodeAddressKey
				, DFSConfigKeys.DfsDatanodeAddressDefault)).Port;
			this.defaultInfoPort = NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys.DfsDatanodeHttpAddressKey
				, DFSConfigKeys.DfsDatanodeHttpAddressDefault)).Port;
			this.defaultInfoSecurePort = NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys
				.DfsDatanodeHttpsAddressKey, DFSConfigKeys.DfsDatanodeHttpsAddressDefault)).Port;
			this.defaultIpcPort = NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys.DfsDatanodeIpcAddressKey
				, DFSConfigKeys.DfsDatanodeIpcAddressDefault)).Port;
			try
			{
				this.hostFileManager.Refresh(conf.Get(DFSConfigKeys.DfsHosts, string.Empty), conf
					.Get(DFSConfigKeys.DfsHostsExclude, string.Empty));
			}
			catch (IOException e)
			{
				Log.Error("error reading hosts files: ", e);
			}
			this.dnsToSwitchMapping = ReflectionUtils.NewInstance(conf.GetClass<DNSToSwitchMapping
				>(DFSConfigKeys.NetTopologyNodeSwitchMappingImplKey, typeof(ScriptBasedMapping))
				, conf);
			this.rejectUnresolvedTopologyDN = conf.GetBoolean(DFSConfigKeys.DfsRejectUnresolvedDnTopologyMappingKey
				, DFSConfigKeys.DfsRejectUnresolvedDnTopologyMappingDefault);
			// If the dns to switch mapping supports cache, resolve network
			// locations of those hosts in the include list and store the mapping
			// in the cache; so future calls to resolve will be fast.
			if (dnsToSwitchMapping is CachedDNSToSwitchMapping)
			{
				AList<string> locations = new AList<string>();
				foreach (IPEndPoint addr in hostFileManager.GetIncludes())
				{
					locations.AddItem(addr.Address.GetHostAddress());
				}
				dnsToSwitchMapping.Resolve(locations);
			}
			long heartbeatIntervalSeconds = conf.GetLong(DFSConfigKeys.DfsHeartbeatIntervalKey
				, DFSConfigKeys.DfsHeartbeatIntervalDefault);
			int heartbeatRecheckInterval = conf.GetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey
				, DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalDefault);
			// 5 minutes
			this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval + 10 * 1000 * heartbeatIntervalSeconds;
			int blockInvalidateLimit = Math.Max(20 * (int)(heartbeatIntervalSeconds), DFSConfigKeys
				.DfsBlockInvalidateLimitDefault);
			this.blockInvalidateLimit = conf.GetInt(DFSConfigKeys.DfsBlockInvalidateLimitKey, 
				blockInvalidateLimit);
			Log.Info(DFSConfigKeys.DfsBlockInvalidateLimitKey + "=" + this.blockInvalidateLimit
				);
			this.checkIpHostnameInRegistration = conf.GetBoolean(DFSConfigKeys.DfsNamenodeDatanodeRegistrationIpHostnameCheckKey
				, DFSConfigKeys.DfsNamenodeDatanodeRegistrationIpHostnameCheckDefault);
			Log.Info(DFSConfigKeys.DfsNamenodeDatanodeRegistrationIpHostnameCheckKey + "=" + 
				checkIpHostnameInRegistration);
			this.avoidStaleDataNodesForRead = conf.GetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForReadKey
				, DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForReadDefault);
			this.avoidStaleDataNodesForWrite = conf.GetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForWriteKey
				, DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForWriteDefault);
			this.staleInterval = GetStaleIntervalFromConf(conf, heartbeatExpireInterval);
			this.ratioUseStaleDataNodesForWrite = conf.GetFloat(DFSConfigKeys.DfsNamenodeUseStaleDatanodeForWriteRatioKey
				, DFSConfigKeys.DfsNamenodeUseStaleDatanodeForWriteRatioDefault);
			Preconditions.CheckArgument((ratioUseStaleDataNodesForWrite > 0 && ratioUseStaleDataNodesForWrite
				 <= 1.0f), DFSConfigKeys.DfsNamenodeUseStaleDatanodeForWriteRatioKey + " = '" + 
				ratioUseStaleDataNodesForWrite + "' is invalid. " + "It should be a positive non-zero float value, not greater than 1.0f."
				);
			this.timeBetweenResendingCachingDirectivesMs = conf.GetLong(DFSConfigKeys.DfsNamenodePathBasedCacheRetryIntervalMs
				, DFSConfigKeys.DfsNamenodePathBasedCacheRetryIntervalMsDefault);
			this.blocksPerPostponedMisreplicatedBlocksRescan = conf.GetLong(DFSConfigKeys.DfsNamenodeBlocksPerPostponedblocksRescanKey
				, DFSConfigKeys.DfsNamenodeBlocksPerPostponedblocksRescanKeyDefault);
		}

		private static long GetStaleIntervalFromConf(Configuration conf, long heartbeatExpireInterval
			)
		{
			long staleInterval = conf.GetLong(DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalKey
				, DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalDefault);
			Preconditions.CheckArgument(staleInterval > 0, DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalKey
				 + " = '" + staleInterval + "' is invalid. " + "It should be a positive non-zero value."
				);
			long heartbeatIntervalSeconds = conf.GetLong(DFSConfigKeys.DfsHeartbeatIntervalKey
				, DFSConfigKeys.DfsHeartbeatIntervalDefault);
			// The stale interval value cannot be smaller than 
			// 3 times of heartbeat interval 
			long minStaleInterval = conf.GetInt(DFSConfigKeys.DfsNamenodeStaleDatanodeMinimumIntervalKey
				, DFSConfigKeys.DfsNamenodeStaleDatanodeMinimumIntervalDefault) * heartbeatIntervalSeconds
				 * 1000;
			if (staleInterval < minStaleInterval)
			{
				Log.Warn("The given interval for marking stale datanode = " + staleInterval + ", which is less than "
					 + DFSConfigKeys.DfsNamenodeStaleDatanodeMinimumIntervalDefault + " heartbeat intervals. This may cause too frequent changes of "
					 + "stale states of DataNodes since a heartbeat msg may be missing " + "due to temporary short-term failures. Reset stale interval to "
					 + minStaleInterval + ".");
				staleInterval = minStaleInterval;
			}
			if (staleInterval > heartbeatExpireInterval)
			{
				Log.Warn("The given interval for marking stale datanode = " + staleInterval + ", which is larger than heartbeat expire interval "
					 + heartbeatExpireInterval + ".");
			}
			return staleInterval;
		}

		internal virtual void Activate(Configuration conf)
		{
			decomManager.Activate(conf);
			heartbeatManager.Activate(conf);
		}

		internal virtual void Close()
		{
			decomManager.Close();
			heartbeatManager.Close();
		}

		/// <returns>the network topology.</returns>
		public virtual NetworkTopology GetNetworkTopology()
		{
			return networktopology;
		}

		/// <returns>the heartbeat manager.</returns>
		internal virtual HeartbeatManager GetHeartbeatManager()
		{
			return heartbeatManager;
		}

		[VisibleForTesting]
		public virtual DecommissionManager GetDecomManager()
		{
			return decomManager;
		}

		internal virtual HostFileManager GetHostFileManager()
		{
			return hostFileManager;
		}

		[VisibleForTesting]
		public virtual void SetHeartbeatExpireInterval(long expiryMs)
		{
			this.heartbeatExpireInterval = expiryMs;
		}

		[VisibleForTesting]
		public virtual FSClusterStats GetFSClusterStats()
		{
			return fsClusterStats;
		}

		/// <returns>the datanode statistics.</returns>
		public virtual DatanodeStatistics GetDatanodeStatistics()
		{
			return heartbeatManager;
		}

		private bool IsInactive(DatanodeInfo datanode)
		{
			if (datanode.IsDecommissioned())
			{
				return true;
			}
			if (avoidStaleDataNodesForRead)
			{
				return datanode.IsStale(staleInterval);
			}
			return false;
		}

		/// <summary>Sort the located blocks by the distance to the target host.</summary>
		public virtual void SortLocatedBlocks(string targethost, IList<LocatedBlock> locatedblocks
			)
		{
			//sort the blocks
			// As it is possible for the separation of node manager and datanode, 
			// here we should get node but not datanode only .
			Node client = GetDatanodeByHost(targethost);
			if (client == null)
			{
				IList<string> hosts = new AList<string>(1);
				hosts.AddItem(targethost);
				string rName = dnsToSwitchMapping.Resolve(hosts)[0];
				if (rName != null)
				{
					client = new NodeBase(rName + NodeBase.PathSeparatorStr + targethost);
				}
			}
			IComparer<DatanodeInfo> comparator = avoidStaleDataNodesForRead ? new DFSUtil.DecomStaleComparator
				(staleInterval) : DFSUtil.DecomComparator;
			foreach (LocatedBlock b in locatedblocks)
			{
				DatanodeInfo[] di = b.GetLocations();
				// Move decommissioned/stale datanodes to the bottom
				Arrays.Sort(di, comparator);
				int lastActiveIndex = di.Length - 1;
				while (lastActiveIndex > 0 && IsInactive(di[lastActiveIndex]))
				{
					--lastActiveIndex;
				}
				int activeLen = lastActiveIndex + 1;
				networktopology.SortByDistance(client, b.GetLocations(), activeLen);
				// must update cache since we modified locations array
				b.UpdateCachedStorageInfo();
			}
		}

		internal virtual CyclicIteration<string, DatanodeDescriptor> GetDatanodeCyclicIteration
			(string firstkey)
		{
			return new CyclicIteration<string, DatanodeDescriptor>(datanodeMap, firstkey);
		}

		/// <returns>the datanode descriptor for the host.</returns>
		public virtual DatanodeDescriptor GetDatanodeByHost(string host)
		{
			return host2DatanodeMap.GetDatanodeByHost(host);
		}

		/// <returns>the datanode descriptor for the host.</returns>
		public virtual DatanodeDescriptor GetDatanodeByXferAddr(string host, int xferPort
			)
		{
			return host2DatanodeMap.GetDatanodeByXferAddr(host, xferPort);
		}

		/// <returns>the Host2NodesMap</returns>
		public virtual Host2NodesMap GetHost2DatanodeMap()
		{
			return this.host2DatanodeMap;
		}

		/// <summary>
		/// Given datanode address or host name, returns the DatanodeDescriptor for the
		/// same, or if it doesn't find the datanode, it looks for a machine local and
		/// then rack local datanode, if a rack local datanode is not possible either,
		/// it returns the DatanodeDescriptor of any random node in the cluster.
		/// </summary>
		/// <param name="address">hostaddress:transfer address</param>
		/// <returns>the best match for the given datanode</returns>
		internal virtual DatanodeDescriptor GetDatanodeDescriptor(string address)
		{
			DatanodeID dnId = ParseDNFromHostsEntry(address);
			string host = dnId.GetIpAddr();
			int xferPort = dnId.GetXferPort();
			DatanodeDescriptor node = GetDatanodeByXferAddr(host, xferPort);
			if (node == null)
			{
				node = GetDatanodeByHost(host);
			}
			if (node == null)
			{
				string networkLocation = ResolveNetworkLocationWithFallBackToDefaultLocation(dnId
					);
				// If the current cluster doesn't contain the node, fallback to
				// something machine local and then rack local.
				IList<Node> rackNodes = GetNetworkTopology().GetDatanodesInRack(networkLocation);
				if (rackNodes != null)
				{
					// Try something machine local.
					foreach (Node rackNode in rackNodes)
					{
						if (((DatanodeDescriptor)rackNode).GetIpAddr().Equals(host))
						{
							node = (DatanodeDescriptor)rackNode;
							break;
						}
					}
					// Try something rack local.
					if (node == null && !rackNodes.IsEmpty())
					{
						node = (DatanodeDescriptor)(rackNodes[DFSUtil.GetRandom().Next(rackNodes.Count)]);
					}
				}
				// If we can't even choose rack local, just choose any node in the
				// cluster.
				if (node == null)
				{
					node = (DatanodeDescriptor)GetNetworkTopology().ChooseRandom(NodeBase.Root);
				}
			}
			return node;
		}

		/// <summary>Get a datanode descriptor given corresponding DatanodeUUID</summary>
		internal virtual DatanodeDescriptor GetDatanode(string datanodeUuid)
		{
			if (datanodeUuid == null)
			{
				return null;
			}
			return datanodeMap[datanodeUuid];
		}

		/// <summary>Get data node by datanode ID.</summary>
		/// <param name="nodeID">datanode ID</param>
		/// <returns>DatanodeDescriptor or null if the node is not found.</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.UnregisteredNodeException"/>
		public virtual DatanodeDescriptor GetDatanode(DatanodeID nodeID)
		{
			DatanodeDescriptor node = GetDatanode(nodeID.GetDatanodeUuid());
			if (node == null)
			{
				return null;
			}
			if (!node.GetXferAddr().Equals(nodeID.GetXferAddr()))
			{
				UnregisteredNodeException e = new UnregisteredNodeException(nodeID, node);
				NameNode.stateChangeLog.Error("BLOCK* NameSystem.getDatanode: " + e.GetLocalizedMessage
					());
				throw e;
			}
			return node;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.UnregisteredNodeException"/>
		public virtual DatanodeStorageInfo[] GetDatanodeStorageInfos(DatanodeID[] datanodeID
			, string[] storageIDs)
		{
			if (datanodeID.Length == 0)
			{
				return null;
			}
			DatanodeStorageInfo[] storages = new DatanodeStorageInfo[datanodeID.Length];
			for (int i = 0; i < datanodeID.Length; i++)
			{
				DatanodeDescriptor dd = GetDatanode(datanodeID[i]);
				storages[i] = dd.GetStorageInfo(storageIDs[i]);
			}
			return storages;
		}

		/// <summary>Prints information about all datanodes.</summary>
		internal virtual void DatanodeDump(PrintWriter @out)
		{
			lock (datanodeMap)
			{
				@out.WriteLine("Metasave: Number of datanodes: " + datanodeMap.Count);
				for (IEnumerator<DatanodeDescriptor> it = datanodeMap.Values.GetEnumerator(); it.
					HasNext(); )
				{
					DatanodeDescriptor node = it.Next();
					@out.WriteLine(node.DumpDatanode());
				}
			}
		}

		/// <summary>Remove a datanode descriptor.</summary>
		/// <param name="nodeInfo">datanode descriptor.</param>
		private void RemoveDatanode(DatanodeDescriptor nodeInfo)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			heartbeatManager.RemoveDatanode(nodeInfo);
			blockManager.RemoveBlocksAssociatedTo(nodeInfo);
			networktopology.Remove(nodeInfo);
			DecrementVersionCount(nodeInfo.GetSoftwareVersion());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("remove datanode " + nodeInfo);
			}
			namesystem.CheckSafeMode();
		}

		/// <summary>Remove a datanode</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.UnregisteredNodeException"></exception>
		public virtual void RemoveDatanode(DatanodeID node)
		{
			namesystem.WriteLock();
			try
			{
				DatanodeDescriptor descriptor = GetDatanode(node);
				if (descriptor != null)
				{
					RemoveDatanode(descriptor);
				}
				else
				{
					NameNode.stateChangeLog.Warn("BLOCK* removeDatanode: " + node + " does not exist"
						);
				}
			}
			finally
			{
				namesystem.WriteUnlock();
			}
		}

		/// <summary>Remove a dead datanode.</summary>
		internal virtual void RemoveDeadDatanode(DatanodeID nodeID)
		{
			lock (datanodeMap)
			{
				DatanodeDescriptor d;
				try
				{
					d = GetDatanode(nodeID);
				}
				catch (IOException)
				{
					d = null;
				}
				if (d != null && IsDatanodeDead(d))
				{
					NameNode.stateChangeLog.Info("BLOCK* removeDeadDatanode: lost heartbeat from " + 
						d);
					RemoveDatanode(d);
				}
			}
		}

		/// <summary>Is the datanode dead?</summary>
		internal virtual bool IsDatanodeDead(DatanodeDescriptor node)
		{
			return (node.GetLastUpdateMonotonic() < (Time.MonotonicNow() - heartbeatExpireInterval
				));
		}

		/// <summary>Add a datanode.</summary>
		internal virtual void AddDatanode(DatanodeDescriptor node)
		{
			// To keep host2DatanodeMap consistent with datanodeMap,
			// remove  from host2DatanodeMap the datanodeDescriptor removed
			// from datanodeMap before adding node to host2DatanodeMap.
			lock (datanodeMap)
			{
				host2DatanodeMap.Remove(datanodeMap[node.GetDatanodeUuid()] = node);
			}
			networktopology.Add(node);
			// may throw InvalidTopologyException
			host2DatanodeMap.Add(node);
			CheckIfClusterIsNowMultiRack(node);
			if (Log.IsDebugEnabled())
			{
				Log.Debug(GetType().Name + ".addDatanode: " + "node " + node + " is added to datanodeMap."
					);
			}
		}

		/// <summary>Physically remove node from datanodeMap.</summary>
		private void WipeDatanode(DatanodeID node)
		{
			string key = node.GetDatanodeUuid();
			lock (datanodeMap)
			{
				host2DatanodeMap.Remove(Sharpen.Collections.Remove(datanodeMap, key));
			}
			// Also remove all block invalidation tasks under this node
			blockManager.RemoveFromInvalidates(new DatanodeInfo(node));
			if (Log.IsDebugEnabled())
			{
				Log.Debug(GetType().Name + ".wipeDatanode(" + node + "): storage " + key + " is removed from datanodeMap."
					);
			}
		}

		private void IncrementVersionCount(string version)
		{
			if (version == null)
			{
				return;
			}
			lock (datanodeMap)
			{
				int count = this.datanodesSoftwareVersions[version];
				count = count == null ? 1 : count + 1;
				this.datanodesSoftwareVersions[version] = count;
			}
		}

		private void DecrementVersionCount(string version)
		{
			if (version == null)
			{
				return;
			}
			lock (datanodeMap)
			{
				int count = this.datanodesSoftwareVersions[version];
				if (count != null)
				{
					if (count > 1)
					{
						this.datanodesSoftwareVersions[version] = count - 1;
					}
					else
					{
						Sharpen.Collections.Remove(this.datanodesSoftwareVersions, version);
					}
				}
			}
		}

		private bool ShouldCountVersion(DatanodeDescriptor node)
		{
			return node.GetSoftwareVersion() != null && node.isAlive && !IsDatanodeDead(node);
		}

		private void CountSoftwareVersions()
		{
			lock (datanodeMap)
			{
				Dictionary<string, int> versionCount = new Dictionary<string, int>();
				foreach (DatanodeDescriptor dn in datanodeMap.Values)
				{
					// Check isAlive too because right after removeDatanode(),
					// isDatanodeDead() is still true 
					if (ShouldCountVersion(dn))
					{
						int num = versionCount[dn.GetSoftwareVersion()];
						num = num == null ? 1 : num + 1;
						versionCount[dn.GetSoftwareVersion()] = num;
					}
				}
				this.datanodesSoftwareVersions = versionCount;
			}
		}

		public virtual Dictionary<string, int> GetDatanodesSoftwareVersions()
		{
			lock (datanodeMap)
			{
				return new Dictionary<string, int>(this.datanodesSoftwareVersions);
			}
		}

		/// <summary>Resolve a node's network location.</summary>
		/// <remarks>
		/// Resolve a node's network location. If the DNS to switch mapping fails
		/// then this method guarantees default rack location.
		/// </remarks>
		/// <param name="node">to resolve to network location</param>
		/// <returns>network location path</returns>
		private string ResolveNetworkLocationWithFallBackToDefaultLocation(DatanodeID node
			)
		{
			string networkLocation;
			try
			{
				networkLocation = ResolveNetworkLocation(node);
			}
			catch (UnresolvedTopologyException)
			{
				Log.Error("Unresolved topology mapping. Using " + NetworkTopology.DefaultRack + " for host "
					 + node.GetHostName());
				networkLocation = NetworkTopology.DefaultRack;
			}
			return networkLocation;
		}

		/// <summary>Resolve a node's network location.</summary>
		/// <remarks>
		/// Resolve a node's network location. If the DNS to switch mapping fails,
		/// then this method throws UnresolvedTopologyException.
		/// </remarks>
		/// <param name="node">to resolve to network location</param>
		/// <returns>network location path.</returns>
		/// <exception cref="UnresolvedTopologyException">
		/// if the DNS to switch mapping fails
		/// to resolve network location.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.UnresolvedTopologyException
		/// 	"/>
		private string ResolveNetworkLocation(DatanodeID node)
		{
			IList<string> names = new AList<string>(1);
			if (dnsToSwitchMapping is CachedDNSToSwitchMapping)
			{
				names.AddItem(node.GetIpAddr());
			}
			else
			{
				names.AddItem(node.GetHostName());
			}
			IList<string> rName = ResolveNetworkLocation(names);
			string networkLocation;
			if (rName == null)
			{
				Log.Error("The resolve call returned null!");
				throw new UnresolvedTopologyException("Unresolved topology mapping for host " + node
					.GetHostName());
			}
			else
			{
				networkLocation = rName[0];
			}
			return networkLocation;
		}

		/// <summary>Resolve network locations for specified hosts</summary>
		/// <param name="names"/>
		/// <returns>Network locations if available, Else returns null</returns>
		public virtual IList<string> ResolveNetworkLocation(IList<string> names)
		{
			// resolve its network location
			IList<string> rName = dnsToSwitchMapping.Resolve(names);
			return rName;
		}

		/// <summary>Resolve a node's dependencies in the network.</summary>
		/// <remarks>
		/// Resolve a node's dependencies in the network. If the DNS to switch
		/// mapping fails then this method returns empty list of dependencies
		/// </remarks>
		/// <param name="node">to get dependencies for</param>
		/// <returns>List of dependent host names</returns>
		private IList<string> GetNetworkDependenciesWithDefault(DatanodeInfo node)
		{
			IList<string> dependencies;
			try
			{
				dependencies = GetNetworkDependencies(node);
			}
			catch (UnresolvedTopologyException)
			{
				Log.Error("Unresolved dependency mapping for host " + node.GetHostName() + ". Continuing with an empty dependency list"
					);
				dependencies = Sharpen.Collections.EmptyList();
			}
			return dependencies;
		}

		/// <summary>Resolves a node's dependencies in the network.</summary>
		/// <remarks>
		/// Resolves a node's dependencies in the network. If the DNS to switch
		/// mapping fails to get dependencies, then this method throws
		/// UnresolvedTopologyException.
		/// </remarks>
		/// <param name="node">to get dependencies for</param>
		/// <returns>List of dependent host names</returns>
		/// <exception cref="UnresolvedTopologyException">if the DNS to switch mapping fails</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.UnresolvedTopologyException
		/// 	"/>
		private IList<string> GetNetworkDependencies(DatanodeInfo node)
		{
			IList<string> dependencies = Sharpen.Collections.EmptyList();
			if (dnsToSwitchMapping is DNSToSwitchMappingWithDependency)
			{
				//Get dependencies
				dependencies = ((DNSToSwitchMappingWithDependency)dnsToSwitchMapping).GetDependency
					(node.GetHostName());
				if (dependencies == null)
				{
					Log.Error("The dependency call returned null for host " + node.GetHostName());
					throw new UnresolvedTopologyException("The dependency call returned " + "null for host "
						 + node.GetHostName());
				}
			}
			return dependencies;
		}

		/// <summary>
		/// Remove an already decommissioned data node who is neither in include nor
		/// exclude hosts lists from the the list of live or dead nodes.
		/// </summary>
		/// <remarks>
		/// Remove an already decommissioned data node who is neither in include nor
		/// exclude hosts lists from the the list of live or dead nodes.  This is used
		/// to not display an already decommssioned data node to the operators.
		/// The operation procedure of making a already decommissioned data node not
		/// to be displayed is as following:
		/// <ol>
		/// <li>
		/// Host must have been in the include hosts list and the include hosts list
		/// must not be empty.
		/// </li>
		/// <li>
		/// Host is decommissioned by remaining in the include hosts list and added
		/// into the exclude hosts list. Name node is updated with the new
		/// information by issuing dfsadmin -refreshNodes command.
		/// </li>
		/// <li>
		/// Host is removed from both include hosts and exclude hosts lists.  Name
		/// node is updated with the new informationby issuing dfsamin -refreshNodes
		/// command.
		/// <li>
		/// </ol>
		/// </remarks>
		/// <param name="nodeList">, array list of live or dead nodes.</param>
		private void RemoveDecomNodeFromList(IList<DatanodeDescriptor> nodeList)
		{
			// If the include list is empty, any nodes are welcomed and it does not
			// make sense to exclude any nodes from the cluster. Therefore, no remove.
			if (!hostFileManager.HasIncludes())
			{
				return;
			}
			for (IEnumerator<DatanodeDescriptor> it = nodeList.GetEnumerator(); it.HasNext(); )
			{
				DatanodeDescriptor node = it.Next();
				if ((!hostFileManager.IsIncluded(node)) && (!hostFileManager.IsExcluded(node)) &&
					 node.IsDecommissioned())
				{
					// Include list is not empty, an existing datanode does not appear
					// in both include or exclude lists and it has been decommissioned.
					it.Remove();
				}
			}
		}

		/// <summary>Decommission the node if it is in the host exclude list.</summary>
		/// <param name="nodeReg">datanode</param>
		internal virtual void StartDecommissioningIfExcluded(DatanodeDescriptor nodeReg)
		{
			// If the registered node is in exclude list, then decommission it
			if (GetHostFileManager().IsExcluded(nodeReg))
			{
				decomManager.StartDecommission(nodeReg);
			}
		}

		/// <summary>Register the given datanode with the namenode.</summary>
		/// <remarks>
		/// Register the given datanode with the namenode. NB: the given
		/// registration is mutated and given back to the datanode.
		/// </remarks>
		/// <param name="nodeReg">the datanode registration</param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.DisallowedDatanodeException
		/// 	">
		/// if the registration request is
		/// denied because the datanode does not match includes/excludes
		/// </exception>
		/// <exception cref="UnresolvedTopologyException">
		/// if the registration request is
		/// denied because resolving datanode network location fails.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.UnresolvedTopologyException
		/// 	"/>
		public virtual void RegisterDatanode(DatanodeRegistration nodeReg)
		{
			IPAddress dnAddress = Org.Apache.Hadoop.Ipc.Server.GetRemoteIp();
			if (dnAddress != null)
			{
				// Mostly called inside an RPC, update ip and peer hostname
				string hostname = dnAddress.GetHostName();
				string ip = dnAddress.GetHostAddress();
				if (checkIpHostnameInRegistration && !IsNameResolved(dnAddress))
				{
					// Reject registration of unresolved datanode to prevent performance
					// impact of repetitive DNS lookups later.
					string message = "hostname cannot be resolved (ip=" + ip + ", hostname=" + hostname
						 + ")";
					Log.Warn("Unresolved datanode registration: " + message);
					throw new DisallowedDatanodeException(nodeReg, message);
				}
				// update node registration with the ip and hostname from rpc request
				nodeReg.SetIpAddr(ip);
				nodeReg.SetPeerHostName(hostname);
			}
			try
			{
				nodeReg.SetExportedKeys(blockManager.GetBlockKeys());
				// Checks if the node is not on the hosts list.  If it is not, then
				// it will be disallowed from registering. 
				if (!hostFileManager.IsIncluded(nodeReg))
				{
					throw new DisallowedDatanodeException(nodeReg);
				}
				NameNode.stateChangeLog.Info("BLOCK* registerDatanode: from " + nodeReg + " storage "
					 + nodeReg.GetDatanodeUuid());
				DatanodeDescriptor nodeS = GetDatanode(nodeReg.GetDatanodeUuid());
				DatanodeDescriptor nodeN = host2DatanodeMap.GetDatanodeByXferAddr(nodeReg.GetIpAddr
					(), nodeReg.GetXferPort());
				if (nodeN != null && nodeN != nodeS)
				{
					NameNode.Log.Info("BLOCK* registerDatanode: " + nodeN);
					// nodeN previously served a different data storage, 
					// which is not served by anybody anymore.
					RemoveDatanode(nodeN);
					// physically remove node from datanodeMap
					WipeDatanode(nodeN);
					nodeN = null;
				}
				if (nodeS != null)
				{
					if (nodeN == nodeS)
					{
						// The same datanode has been just restarted to serve the same data 
						// storage. We do not need to remove old data blocks, the delta will
						// be calculated on the next block report from the datanode
						if (NameNode.stateChangeLog.IsDebugEnabled())
						{
							NameNode.stateChangeLog.Debug("BLOCK* registerDatanode: " + "node restarted.");
						}
					}
					else
					{
						// nodeS is found
						/* The registering datanode is a replacement node for the existing
						data storage, which from now on will be served by a new node.
						If this message repeats, both nodes might have same storageID
						by (insanely rare) random chance. User needs to restart one of the
						nodes with its data cleared (or user can just remove the StorageID
						value in "VERSION" file under the data directory of the datanode,
						but this is might not work if VERSION file format has changed
						*/
						NameNode.stateChangeLog.Info("BLOCK* registerDatanode: " + nodeS + " is replaced by "
							 + nodeReg + " with the same storageID " + nodeReg.GetDatanodeUuid());
					}
					bool success = false;
					try
					{
						// update cluster map
						GetNetworkTopology().Remove(nodeS);
						if (ShouldCountVersion(nodeS))
						{
							DecrementVersionCount(nodeS.GetSoftwareVersion());
						}
						nodeS.UpdateRegInfo(nodeReg);
						nodeS.SetSoftwareVersion(nodeReg.GetSoftwareVersion());
						nodeS.SetDisallowed(false);
						// Node is in the include list
						// resolve network location
						if (this.rejectUnresolvedTopologyDN)
						{
							nodeS.SetNetworkLocation(ResolveNetworkLocation(nodeS));
							nodeS.SetDependentHostNames(GetNetworkDependencies(nodeS));
						}
						else
						{
							nodeS.SetNetworkLocation(ResolveNetworkLocationWithFallBackToDefaultLocation(nodeS
								));
							nodeS.SetDependentHostNames(GetNetworkDependenciesWithDefault(nodeS));
						}
						GetNetworkTopology().Add(nodeS);
						// also treat the registration message as a heartbeat
						heartbeatManager.Register(nodeS);
						IncrementVersionCount(nodeS.GetSoftwareVersion());
						StartDecommissioningIfExcluded(nodeS);
						success = true;
					}
					finally
					{
						if (!success)
						{
							RemoveDatanode(nodeS);
							WipeDatanode(nodeS);
							CountSoftwareVersions();
						}
					}
					return;
				}
				DatanodeDescriptor nodeDescr = new DatanodeDescriptor(nodeReg, NetworkTopology.DefaultRack
					);
				bool success_1 = false;
				try
				{
					// resolve network location
					if (this.rejectUnresolvedTopologyDN)
					{
						nodeDescr.SetNetworkLocation(ResolveNetworkLocation(nodeDescr));
						nodeDescr.SetDependentHostNames(GetNetworkDependencies(nodeDescr));
					}
					else
					{
						nodeDescr.SetNetworkLocation(ResolveNetworkLocationWithFallBackToDefaultLocation(
							nodeDescr));
						nodeDescr.SetDependentHostNames(GetNetworkDependenciesWithDefault(nodeDescr));
					}
					networktopology.Add(nodeDescr);
					nodeDescr.SetSoftwareVersion(nodeReg.GetSoftwareVersion());
					// register new datanode
					AddDatanode(nodeDescr);
					// also treat the registration message as a heartbeat
					// no need to update its timestamp
					// because its is done when the descriptor is created
					heartbeatManager.AddDatanode(nodeDescr);
					IncrementVersionCount(nodeReg.GetSoftwareVersion());
					StartDecommissioningIfExcluded(nodeDescr);
					success_1 = true;
				}
				finally
				{
					if (!success_1)
					{
						RemoveDatanode(nodeDescr);
						WipeDatanode(nodeDescr);
						CountSoftwareVersions();
					}
				}
			}
			catch (NetworkTopology.InvalidTopologyException e)
			{
				// If the network location is invalid, clear the cached mappings
				// so that we have a chance to re-add this DataNode with the
				// correct network location later.
				IList<string> invalidNodeNames = new AList<string>(3);
				// clear cache for nodes in IP or Hostname
				invalidNodeNames.AddItem(nodeReg.GetIpAddr());
				invalidNodeNames.AddItem(nodeReg.GetHostName());
				invalidNodeNames.AddItem(nodeReg.GetPeerHostName());
				dnsToSwitchMapping.ReloadCachedMappings(invalidNodeNames);
				throw;
			}
		}

		/// <summary>Rereads conf to get hosts and exclude list file names.</summary>
		/// <remarks>
		/// Rereads conf to get hosts and exclude list file names.
		/// Rereads the files to update the hosts and exclude lists.  It
		/// checks if any of the hosts have changed states:
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshNodes(Configuration conf)
		{
			RefreshHostsReader(conf);
			namesystem.WriteLock();
			try
			{
				RefreshDatanodes();
				CountSoftwareVersions();
			}
			finally
			{
				namesystem.WriteUnlock();
			}
		}

		/// <summary>Reread include/exclude files.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void RefreshHostsReader(Configuration conf)
		{
			// Reread the conf to get dfs.hosts and dfs.hosts.exclude filenames.
			// Update the file names and refresh internal includes and excludes list.
			if (conf == null)
			{
				conf = new HdfsConfiguration();
			}
			this.hostFileManager.Refresh(conf.Get(DFSConfigKeys.DfsHosts, string.Empty), conf
				.Get(DFSConfigKeys.DfsHostsExclude, string.Empty));
		}

		/// <summary>1.</summary>
		/// <remarks>
		/// 1. Added to hosts  --&gt; no further work needed here.
		/// 2. Removed from hosts --&gt; mark AdminState as decommissioned.
		/// 3. Added to exclude --&gt; start decommission.
		/// 4. Removed from exclude --&gt; stop decommission.
		/// </remarks>
		private void RefreshDatanodes()
		{
			foreach (DatanodeDescriptor node in datanodeMap.Values)
			{
				// Check if not include.
				if (!hostFileManager.IsIncluded(node))
				{
					node.SetDisallowed(true);
				}
				else
				{
					// case 2.
					if (hostFileManager.IsExcluded(node))
					{
						decomManager.StartDecommission(node);
					}
					else
					{
						// case 3.
						decomManager.StopDecommission(node);
					}
				}
			}
		}

		// case 4.
		/// <returns>the number of live datanodes.</returns>
		public virtual int GetNumLiveDataNodes()
		{
			int numLive = 0;
			lock (datanodeMap)
			{
				foreach (DatanodeDescriptor dn in datanodeMap.Values)
				{
					if (!IsDatanodeDead(dn))
					{
						numLive++;
					}
				}
			}
			return numLive;
		}

		/// <returns>the number of dead datanodes.</returns>
		public virtual int GetNumDeadDataNodes()
		{
			return GetDatanodeListForReport(HdfsConstants.DatanodeReportType.Dead).Count;
		}

		/// <returns>list of datanodes where decommissioning is in progress.</returns>
		public virtual IList<DatanodeDescriptor> GetDecommissioningNodes()
		{
			// There is no need to take namesystem reader lock as
			// getDatanodeListForReport will synchronize on datanodeMap
			// A decommissioning DN may be "alive" or "dead".
			return GetDatanodeListForReport(HdfsConstants.DatanodeReportType.Decommissioning);
		}

		/* Getter and Setter for stale DataNodes related attributes */
		/// <summary>Whether stale datanodes should be avoided as targets on the write path.</summary>
		/// <remarks>
		/// Whether stale datanodes should be avoided as targets on the write path.
		/// The result of this function may change if the number of stale datanodes
		/// eclipses a configurable threshold.
		/// </remarks>
		/// <returns>whether stale datanodes should be avoided on the write path</returns>
		public virtual bool ShouldAvoidStaleDataNodesForWrite()
		{
			// If # stale exceeds maximum staleness ratio, disable stale
			// datanode avoidance on the write path
			return avoidStaleDataNodesForWrite && (numStaleNodes <= heartbeatManager.GetLiveDatanodeCount
				() * ratioUseStaleDataNodesForWrite);
		}

		public virtual long GetBlocksPerPostponedMisreplicatedBlocksRescan()
		{
			return blocksPerPostponedMisreplicatedBlocksRescan;
		}

		/// <returns>The time interval used to mark DataNodes as stale.</returns>
		internal virtual long GetStaleInterval()
		{
			return staleInterval;
		}

		/// <summary>Set the number of current stale DataNodes.</summary>
		/// <remarks>
		/// Set the number of current stale DataNodes. The HeartbeatManager got this
		/// number based on DataNodes' heartbeats.
		/// </remarks>
		/// <param name="numStaleNodes">The number of stale DataNodes to be set.</param>
		internal virtual void SetNumStaleNodes(int numStaleNodes)
		{
			this.numStaleNodes = numStaleNodes;
		}

		/// <returns>
		/// Return the current number of stale DataNodes (detected by
		/// HeartbeatManager).
		/// </returns>
		public virtual int GetNumStaleNodes()
		{
			return this.numStaleNodes;
		}

		/// <summary>Get the number of content stale storages.</summary>
		public virtual int GetNumStaleStorages()
		{
			return numStaleStorages;
		}

		/// <summary>Set the number of content stale storages.</summary>
		/// <param name="numStaleStorages">The number of content stale storages.</param>
		internal virtual void SetNumStaleStorages(int numStaleStorages)
		{
			this.numStaleStorages = numStaleStorages;
		}

		/// <summary>Fetch live and dead datanodes.</summary>
		public virtual void FetchDatanodes(IList<DatanodeDescriptor> live, IList<DatanodeDescriptor
			> dead, bool removeDecommissionNode)
		{
			if (live == null && dead == null)
			{
				throw new HadoopIllegalArgumentException("Both live and dead lists are null");
			}
			// There is no need to take namesystem reader lock as
			// getDatanodeListForReport will synchronize on datanodeMap
			IList<DatanodeDescriptor> results = GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.All);
			foreach (DatanodeDescriptor node in results)
			{
				if (IsDatanodeDead(node))
				{
					if (dead != null)
					{
						dead.AddItem(node);
					}
				}
				else
				{
					if (live != null)
					{
						live.AddItem(node);
					}
				}
			}
			if (removeDecommissionNode)
			{
				if (live != null)
				{
					RemoveDecomNodeFromList(live);
				}
				if (dead != null)
				{
					RemoveDecomNodeFromList(dead);
				}
			}
		}

		/// <returns>
		/// true if this cluster has ever consisted of multiple racks, even if
		/// it is not now a multi-rack cluster.
		/// </returns>
		internal virtual bool HasClusterEverBeenMultiRack()
		{
			return hasClusterEverBeenMultiRack;
		}

		/// <summary>Check if the cluster now consists of multiple racks.</summary>
		/// <remarks>
		/// Check if the cluster now consists of multiple racks. If it does, and this
		/// is the first time it's consisted of multiple racks, then process blocks
		/// that may now be misreplicated.
		/// </remarks>
		/// <param name="node">DN which caused cluster to become multi-rack. Used for logging.
		/// 	</param>
		[VisibleForTesting]
		internal virtual void CheckIfClusterIsNowMultiRack(DatanodeDescriptor node)
		{
			if (!hasClusterEverBeenMultiRack && networktopology.GetNumOfRacks() > 1)
			{
				string message = "DN " + node + " joining cluster has expanded a formerly " + "single-rack cluster to be multi-rack. ";
				if (namesystem.IsPopulatingReplQueues())
				{
					message += "Re-checking all blocks for replication, since they should " + "now be replicated cross-rack";
					Log.Info(message);
				}
				else
				{
					message += "Not checking for mis-replicated blocks because this NN is " + "not yet processing repl queues.";
					Log.Debug(message);
				}
				hasClusterEverBeenMultiRack = true;
				if (namesystem.IsPopulatingReplQueues())
				{
					blockManager.ProcessMisReplicatedBlocks();
				}
			}
		}

		/// <summary>Parse a DatanodeID from a hosts file entry</summary>
		/// <param name="hostLine">of form [hostname|ip][:port]?</param>
		/// <returns>DatanodeID constructed from the given string</returns>
		private DatanodeID ParseDNFromHostsEntry(string hostLine)
		{
			DatanodeID dnId;
			string hostStr;
			int port;
			int idx = hostLine.IndexOf(':');
			if (-1 == idx)
			{
				hostStr = hostLine;
				port = DFSConfigKeys.DfsDatanodeDefaultPort;
			}
			else
			{
				hostStr = Sharpen.Runtime.Substring(hostLine, 0, idx);
				port = System.Convert.ToInt32(Sharpen.Runtime.Substring(hostLine, idx + 1));
			}
			if (InetAddresses.IsInetAddress(hostStr))
			{
				// The IP:port is sufficient for listing in a report
				dnId = new DatanodeID(hostStr, string.Empty, string.Empty, port, DFSConfigKeys.DfsDatanodeHttpDefaultPort
					, DFSConfigKeys.DfsDatanodeHttpsDefaultPort, DFSConfigKeys.DfsDatanodeIpcDefaultPort
					);
			}
			else
			{
				string ipAddr = string.Empty;
				try
				{
					ipAddr = Sharpen.Extensions.GetAddressByName(hostStr).GetHostAddress();
				}
				catch (UnknownHostException)
				{
					Log.Warn("Invalid hostname " + hostStr + " in hosts file");
				}
				dnId = new DatanodeID(ipAddr, hostStr, string.Empty, port, DFSConfigKeys.DfsDatanodeHttpDefaultPort
					, DFSConfigKeys.DfsDatanodeHttpsDefaultPort, DFSConfigKeys.DfsDatanodeIpcDefaultPort
					);
			}
			return dnId;
		}

		/// <summary>For generating datanode reports</summary>
		public virtual IList<DatanodeDescriptor> GetDatanodeListForReport(HdfsConstants.DatanodeReportType
			 type)
		{
			bool listLiveNodes = type == HdfsConstants.DatanodeReportType.All || type == HdfsConstants.DatanodeReportType
				.Live;
			bool listDeadNodes = type == HdfsConstants.DatanodeReportType.All || type == HdfsConstants.DatanodeReportType
				.Dead;
			bool listDecommissioningNodes = type == HdfsConstants.DatanodeReportType.All || type
				 == HdfsConstants.DatanodeReportType.Decommissioning;
			AList<DatanodeDescriptor> nodes;
			HostFileManager.HostSet foundNodes = new HostFileManager.HostSet();
			HostFileManager.HostSet includedNodes = hostFileManager.GetIncludes();
			HostFileManager.HostSet excludedNodes = hostFileManager.GetExcludes();
			lock (datanodeMap)
			{
				nodes = new AList<DatanodeDescriptor>(datanodeMap.Count);
				foreach (DatanodeDescriptor dn in datanodeMap.Values)
				{
					bool isDead = IsDatanodeDead(dn);
					bool isDecommissioning = dn.IsDecommissionInProgress();
					if (((listLiveNodes && !isDead) || (listDeadNodes && isDead) || (listDecommissioningNodes
						 && isDecommissioning)) && hostFileManager.IsIncluded(dn))
					{
						nodes.AddItem(dn);
					}
					foundNodes.Add(HostFileManager.ResolvedAddressFromDatanodeID(dn));
				}
			}
			if (listDeadNodes)
			{
				foreach (IPEndPoint addr in includedNodes)
				{
					if (foundNodes.MatchedBy(addr) || excludedNodes.Match(addr))
					{
						continue;
					}
					// The remaining nodes are ones that are referenced by the hosts
					// files but that we do not know about, ie that we have never
					// head from. Eg. an entry that is no longer part of the cluster
					// or a bogus entry was given in the hosts files
					//
					// If the host file entry specified the xferPort, we use that.
					// Otherwise, we guess that it is the default xfer port.
					// We can't ask the DataNode what it had configured, because it's
					// dead.
					DatanodeDescriptor dn = new DatanodeDescriptor(new DatanodeID(addr.Address.GetHostAddress
						(), addr.GetHostName(), string.Empty, addr.Port == 0 ? defaultXferPort : addr.Port
						, defaultInfoPort, defaultInfoSecurePort, defaultIpcPort));
					SetDatanodeDead(dn);
					nodes.AddItem(dn);
				}
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("getDatanodeListForReport with " + "includedNodes = " + hostFileManager
					.GetIncludes() + ", excludedNodes = " + hostFileManager.GetExcludes() + ", foundNodes = "
					 + foundNodes + ", nodes = " + nodes);
			}
			return nodes;
		}

		/// <summary>Checks if name resolution was successful for the given address.</summary>
		/// <remarks>
		/// Checks if name resolution was successful for the given address.  If IP
		/// address and host name are the same, then it means name resolution has
		/// failed.  As a special case, local addresses are also considered
		/// acceptable.  This is particularly important on Windows, where 127.0.0.1 does
		/// not resolve to "localhost".
		/// </remarks>
		/// <param name="address">InetAddress to check</param>
		/// <returns>boolean true if name resolution successful or address is local</returns>
		private static bool IsNameResolved(IPAddress address)
		{
			string hostname = address.GetHostName();
			string ip = address.GetHostAddress();
			return !hostname.Equals(ip) || NetUtils.IsLocalAddress(address);
		}

		private void SetDatanodeDead(DatanodeDescriptor node)
		{
			node.SetLastUpdate(0);
			node.SetLastUpdateMonotonic(0);
		}

		/// <summary>Handle heartbeat from datanodes.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual DatanodeCommand[] HandleHeartbeat(DatanodeRegistration nodeReg, StorageReport
			[] reports, string blockPoolId, long cacheCapacity, long cacheUsed, int xceiverCount
			, int maxTransfers, int failedVolumes, VolumeFailureSummary volumeFailureSummary
			)
		{
			lock (heartbeatManager)
			{
				lock (datanodeMap)
				{
					DatanodeDescriptor nodeinfo = null;
					try
					{
						nodeinfo = GetDatanode(nodeReg);
					}
					catch (UnregisteredNodeException)
					{
						return new DatanodeCommand[] { RegisterCommand.Register };
					}
					// Check if this datanode should actually be shutdown instead. 
					if (nodeinfo != null && nodeinfo.IsDisallowed())
					{
						SetDatanodeDead(nodeinfo);
						throw new DisallowedDatanodeException(nodeinfo);
					}
					if (nodeinfo == null || !nodeinfo.isAlive)
					{
						return new DatanodeCommand[] { RegisterCommand.Register };
					}
					heartbeatManager.UpdateHeartbeat(nodeinfo, reports, cacheCapacity, cacheUsed, xceiverCount
						, failedVolumes, volumeFailureSummary);
					// If we are in safemode, do not send back any recovery / replication
					// requests. Don't even drain the existing queue of work.
					if (namesystem.IsInSafeMode())
					{
						return new DatanodeCommand[0];
					}
					//check lease recovery
					BlockInfoContiguousUnderConstruction[] blocks = nodeinfo.GetLeaseRecoveryCommand(
						int.MaxValue);
					if (blocks != null)
					{
						BlockRecoveryCommand brCommand = new BlockRecoveryCommand(blocks.Length);
						foreach (BlockInfoContiguousUnderConstruction b in blocks)
						{
							DatanodeStorageInfo[] storages = b.GetExpectedStorageLocations();
							// Skip stale nodes during recovery - not heart beated for some time (30s by default).
							IList<DatanodeStorageInfo> recoveryLocations = new AList<DatanodeStorageInfo>(storages
								.Length);
							for (int i = 0; i < storages.Length; i++)
							{
								if (!storages[i].GetDatanodeDescriptor().IsStale(staleInterval))
								{
									recoveryLocations.AddItem(storages[i]);
								}
							}
							// If we are performing a truncate recovery than set recovery fields
							// to old block.
							bool truncateRecovery = b.GetTruncateBlock() != null;
							bool copyOnTruncateRecovery = truncateRecovery && b.GetTruncateBlock().GetBlockId
								() != b.GetBlockId();
							ExtendedBlock primaryBlock = (copyOnTruncateRecovery) ? new ExtendedBlock(blockPoolId
								, b.GetTruncateBlock()) : new ExtendedBlock(blockPoolId, b);
							// If we only get 1 replica after eliminating stale nodes, then choose all
							// replicas for recovery and let the primary data node handle failures.
							DatanodeInfo[] recoveryInfos;
							if (recoveryLocations.Count > 1)
							{
								if (recoveryLocations.Count != storages.Length)
								{
									Log.Info("Skipped stale nodes for recovery : " + (storages.Length - recoveryLocations
										.Count));
								}
								recoveryInfos = DatanodeStorageInfo.ToDatanodeInfos(recoveryLocations);
							}
							else
							{
								// If too many replicas are stale, then choose all replicas to participate
								// in block recovery.
								recoveryInfos = DatanodeStorageInfo.ToDatanodeInfos(storages);
							}
							if (truncateRecovery)
							{
								Block recoveryBlock = (copyOnTruncateRecovery) ? b : b.GetTruncateBlock();
								brCommand.Add(new BlockRecoveryCommand.RecoveringBlock(primaryBlock, recoveryInfos
									, recoveryBlock));
							}
							else
							{
								brCommand.Add(new BlockRecoveryCommand.RecoveringBlock(primaryBlock, recoveryInfos
									, b.GetBlockRecoveryId()));
							}
						}
						return new DatanodeCommand[] { brCommand };
					}
					IList<DatanodeCommand> cmds = new AList<DatanodeCommand>();
					//check pending replication
					IList<DatanodeDescriptor.BlockTargetPair> pendingList = nodeinfo.GetReplicationCommand
						(maxTransfers);
					if (pendingList != null)
					{
						cmds.AddItem(new BlockCommand(DatanodeProtocol.DnaTransfer, blockPoolId, pendingList
							));
					}
					//check block invalidation
					Block[] blks = nodeinfo.GetInvalidateBlocks(blockInvalidateLimit);
					if (blks != null)
					{
						cmds.AddItem(new BlockCommand(DatanodeProtocol.DnaInvalidate, blockPoolId, blks));
					}
					bool sendingCachingCommands = false;
					long nowMs = Time.MonotonicNow();
					if (shouldSendCachingCommands && ((nowMs - nodeinfo.GetLastCachingDirectiveSentTimeMs
						()) >= timeBetweenResendingCachingDirectivesMs))
					{
						DatanodeCommand pendingCacheCommand = GetCacheCommand(nodeinfo.GetPendingCached()
							, nodeinfo, DatanodeProtocol.DnaCache, blockPoolId);
						if (pendingCacheCommand != null)
						{
							cmds.AddItem(pendingCacheCommand);
							sendingCachingCommands = true;
						}
						DatanodeCommand pendingUncacheCommand = GetCacheCommand(nodeinfo.GetPendingUncached
							(), nodeinfo, DatanodeProtocol.DnaUncache, blockPoolId);
						if (pendingUncacheCommand != null)
						{
							cmds.AddItem(pendingUncacheCommand);
							sendingCachingCommands = true;
						}
						if (sendingCachingCommands)
						{
							nodeinfo.SetLastCachingDirectiveSentTimeMs(nowMs);
						}
					}
					blockManager.AddKeyUpdateCommand(cmds, nodeinfo);
					// check for balancer bandwidth update
					if (nodeinfo.GetBalancerBandwidth() > 0)
					{
						cmds.AddItem(new BalancerBandwidthCommand(nodeinfo.GetBalancerBandwidth()));
						// set back to 0 to indicate that datanode has been sent the new value
						nodeinfo.SetBalancerBandwidth(0);
					}
					if (!cmds.IsEmpty())
					{
						return Sharpen.Collections.ToArray(cmds, new DatanodeCommand[cmds.Count]);
					}
				}
			}
			return new DatanodeCommand[0];
		}

		/// <summary>Convert a CachedBlockList into a DatanodeCommand with a list of blocks.</summary>
		/// <param name="list">
		/// The
		/// <see cref="CachedBlocksList"/>
		/// .  This function
		/// clears the list.
		/// </param>
		/// <param name="datanode">The datanode.</param>
		/// <param name="action">The action to perform in the command.</param>
		/// <param name="poolId">The block pool id.</param>
		/// <returns>
		/// A DatanodeCommand to be sent back to the DN, or null if
		/// there is nothing to be done.
		/// </returns>
		private DatanodeCommand GetCacheCommand(DatanodeDescriptor.CachedBlocksList list, 
			DatanodeDescriptor datanode, int action, string poolId)
		{
			int length = list.Count;
			if (length == 0)
			{
				return null;
			}
			// Read the existing cache commands.
			long[] blockIds = new long[length];
			int i = 0;
			for (IEnumerator<CachedBlock> iter = list.GetEnumerator(); iter.HasNext(); )
			{
				CachedBlock cachedBlock = iter.Next();
				blockIds[i++] = cachedBlock.GetBlockId();
			}
			return new BlockIdCommand(action, poolId, blockIds);
		}

		/// <summary>
		/// Tell all datanodes to use a new, non-persistent bandwidth value for
		/// dfs.balance.bandwidthPerSec.
		/// </summary>
		/// <remarks>
		/// Tell all datanodes to use a new, non-persistent bandwidth value for
		/// dfs.balance.bandwidthPerSec.
		/// A system administrator can tune the balancer bandwidth parameter
		/// (dfs.datanode.balance.bandwidthPerSec) dynamically by calling
		/// "dfsadmin -setBalanacerBandwidth newbandwidth", at which point the
		/// following 'bandwidth' variable gets updated with the new value for each
		/// node. Once the heartbeat command is issued to update the value on the
		/// specified datanode, this value will be set back to 0.
		/// </remarks>
		/// <param name="bandwidth">Blanacer bandwidth in bytes per second for all datanodes.
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetBalancerBandwidth(long bandwidth)
		{
			lock (datanodeMap)
			{
				foreach (DatanodeDescriptor nodeInfo in datanodeMap.Values)
				{
					nodeInfo.SetBalancerBandwidth(bandwidth);
				}
			}
		}

		public virtual void MarkAllDatanodesStale()
		{
			Log.Info("Marking all datandoes as stale");
			lock (datanodeMap)
			{
				foreach (DatanodeDescriptor dn in datanodeMap.Values)
				{
					foreach (DatanodeStorageInfo storage in dn.GetStorageInfos())
					{
						storage.MarkStaleAfterFailover();
					}
				}
			}
		}

		/// <summary>
		/// Clear any actions that are queued up to be sent to the DNs
		/// on their next heartbeats.
		/// </summary>
		/// <remarks>
		/// Clear any actions that are queued up to be sent to the DNs
		/// on their next heartbeats. This includes block invalidations,
		/// recoveries, and replication requests.
		/// </remarks>
		public virtual void ClearPendingQueues()
		{
			lock (datanodeMap)
			{
				foreach (DatanodeDescriptor dn in datanodeMap.Values)
				{
					dn.ClearBlockQueues();
				}
			}
		}

		/// <summary>
		/// Reset the lastCachingDirectiveSentTimeMs field of all the DataNodes we
		/// know about.
		/// </summary>
		public virtual void ResetLastCachingDirectiveSentTime()
		{
			lock (datanodeMap)
			{
				foreach (DatanodeDescriptor dn in datanodeMap.Values)
				{
					dn.SetLastCachingDirectiveSentTimeMs(0L);
				}
			}
		}

		public override string ToString()
		{
			return GetType().Name + ": " + host2DatanodeMap;
		}

		public virtual void ClearPendingCachingCommands()
		{
			foreach (DatanodeDescriptor dn in datanodeMap.Values)
			{
				dn.GetPendingCached().Clear();
				dn.GetPendingUncached().Clear();
			}
		}

		public virtual void SetShouldSendCachingCommands(bool shouldSendCachingCommands)
		{
			this.shouldSendCachingCommands = shouldSendCachingCommands;
		}

		internal virtual FSClusterStats NewFSClusterStats()
		{
			return new _FSClusterStats_1588(this);
		}

		private sealed class _FSClusterStats_1588 : FSClusterStats
		{
			public _FSClusterStats_1588(DatanodeManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public int GetTotalLoad()
			{
				return this._enclosing.heartbeatManager.GetXceiverCount();
			}

			public bool IsAvoidingStaleDataNodesForWrite()
			{
				return this._enclosing.ShouldAvoidStaleDataNodesForWrite();
			}

			public int GetNumDatanodesInService()
			{
				return this._enclosing.heartbeatManager.GetNumDatanodesInService();
			}

			public double GetInServiceXceiverAverage()
			{
				double avgLoad = 0;
				int nodes = this.GetNumDatanodesInService();
				if (nodes != 0)
				{
					int xceivers = this._enclosing.heartbeatManager.GetInServiceXceiverCount();
					avgLoad = (double)xceivers / nodes;
				}
				return avgLoad;
			}

			private readonly DatanodeManager _enclosing;
		}
	}
}
