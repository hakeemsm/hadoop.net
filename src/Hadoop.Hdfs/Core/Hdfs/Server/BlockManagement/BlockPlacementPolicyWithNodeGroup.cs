using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// The class is responsible for choosing the desired number of targets
	/// for placing block replicas on environment with node-group layer.
	/// </summary>
	/// <remarks>
	/// The class is responsible for choosing the desired number of targets
	/// for placing block replicas on environment with node-group layer.
	/// The replica placement strategy is adjusted to:
	/// If the writer is on a datanode, the 1st replica is placed on the local
	/// node (or local node-group), otherwise a random datanode.
	/// The 2nd replica is placed on a datanode that is on a different rack with 1st
	/// replica node.
	/// The 3rd replica is placed on a datanode which is on a different node-group
	/// but the same rack as the second replica node.
	/// </remarks>
	public class BlockPlacementPolicyWithNodeGroup : BlockPlacementPolicyDefault
	{
		protected internal BlockPlacementPolicyWithNodeGroup(Configuration conf, FSClusterStats
			 stats, NetworkTopology clusterMap, DatanodeManager datanodeManager)
		{
			Initialize(conf, stats, clusterMap, host2datanodeMap);
		}

		protected internal BlockPlacementPolicyWithNodeGroup()
		{
		}

		protected internal override void Initialize(Configuration conf, FSClusterStats stats
			, NetworkTopology clusterMap, Host2NodesMap host2datanodeMap)
		{
			base.Initialize(conf, stats, clusterMap, host2datanodeMap);
		}

		/// <summary>choose local node of localMachine as the target.</summary>
		/// <remarks>
		/// choose local node of localMachine as the target.
		/// if localMachine is not available, choose a node on the same nodegroup or
		/// rack instead.
		/// </remarks>
		/// <returns>the chosen node</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException
		/// 	"/>
		protected internal override DatanodeStorageInfo ChooseLocalStorage(Node localMachine
			, ICollection<Node> excludedNodes, long blocksize, int maxNodesPerRack, IList<DatanodeStorageInfo
			> results, bool avoidStaleNodes, EnumMap<StorageType, int> storageTypes, bool fallbackToLocalRack
			)
		{
			// if no local machine, randomly choose one node
			if (localMachine == null)
			{
				return ChooseRandom(NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack, results
					, avoidStaleNodes, storageTypes);
			}
			// otherwise try local machine first
			if (localMachine is DatanodeDescriptor)
			{
				DatanodeDescriptor localDataNode = (DatanodeDescriptor)localMachine;
				if (excludedNodes.AddItem(localMachine))
				{
					// was not in the excluded list
					for (IEnumerator<KeyValuePair<StorageType, int>> iter = storageTypes.GetEnumerator
						(); iter.HasNext(); )
					{
						KeyValuePair<StorageType, int> entry = iter.Next();
						foreach (DatanodeStorageInfo localStorage in DFSUtil.Shuffle(localDataNode.GetStorageInfos
							()))
						{
							StorageType type = entry.Key;
							if (AddIfIsGoodTarget(localStorage, excludedNodes, blocksize, maxNodesPerRack, false
								, results, avoidStaleNodes, type) >= 0)
							{
								int num = entry.Value;
								if (num == 1)
								{
									iter.Remove();
								}
								else
								{
									entry.SetValue(num - 1);
								}
								return localStorage;
							}
						}
					}
				}
			}
			// try a node on local node group
			DatanodeStorageInfo chosenStorage = ChooseLocalNodeGroup((NetworkTopologyWithNodeGroup
				)clusterMap, localMachine, excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes
				, storageTypes);
			if (chosenStorage != null)
			{
				return chosenStorage;
			}
			if (!fallbackToLocalRack)
			{
				return null;
			}
			// try a node on local rack
			return ChooseLocalRack(localMachine, excludedNodes, blocksize, maxNodesPerRack, results
				, avoidStaleNodes, storageTypes);
		}

		/// <returns>the node of the second replica</returns>
		private static DatanodeDescriptor SecondNode(Node localMachine, IList<DatanodeStorageInfo
			> results)
		{
			// find the second replica
			foreach (DatanodeStorageInfo nextStorage in results)
			{
				DatanodeDescriptor nextNode = nextStorage.GetDatanodeDescriptor();
				if (nextNode != localMachine)
				{
					return nextNode;
				}
			}
			return null;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException
		/// 	"/>
		protected internal override DatanodeStorageInfo ChooseLocalRack(Node localMachine
			, ICollection<Node> excludedNodes, long blocksize, int maxNodesPerRack, IList<DatanodeStorageInfo
			> results, bool avoidStaleNodes, EnumMap<StorageType, int> storageTypes)
		{
			// no local machine, so choose a random machine
			if (localMachine == null)
			{
				return ChooseRandom(NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack, results
					, avoidStaleNodes, storageTypes);
			}
			// choose one from the local rack, but off-nodegroup
			try
			{
				string scope = NetworkTopology.GetFirstHalf(localMachine.GetNetworkLocation());
				return ChooseRandom(scope, excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes
					, storageTypes);
			}
			catch (BlockPlacementPolicy.NotEnoughReplicasException)
			{
				// find the second replica
				DatanodeDescriptor newLocal = SecondNode(localMachine, results);
				if (newLocal != null)
				{
					try
					{
						return ChooseRandom(clusterMap.GetRack(newLocal.GetNetworkLocation()), excludedNodes
							, blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
					}
					catch (BlockPlacementPolicy.NotEnoughReplicasException)
					{
						//otherwise randomly choose one from the network
						return ChooseRandom(NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack, results
							, avoidStaleNodes, storageTypes);
					}
				}
				else
				{
					//otherwise randomly choose one from the network
					return ChooseRandom(NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack, results
						, avoidStaleNodes, storageTypes);
				}
			}
		}

		/// <summary><inheritDoc/></summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException
		/// 	"/>
		protected internal override void ChooseRemoteRack(int numOfReplicas, DatanodeDescriptor
			 localMachine, ICollection<Node> excludedNodes, long blocksize, int maxReplicasPerRack
			, IList<DatanodeStorageInfo> results, bool avoidStaleNodes, EnumMap<StorageType, 
			int> storageTypes)
		{
			int oldNumOfReplicas = results.Count;
			string rackLocation = NetworkTopology.GetFirstHalf(localMachine.GetNetworkLocation
				());
			try
			{
				// randomly choose from remote racks
				ChooseRandom(numOfReplicas, "~" + rackLocation, excludedNodes, blocksize, maxReplicasPerRack
					, results, avoidStaleNodes, storageTypes);
			}
			catch (BlockPlacementPolicy.NotEnoughReplicasException)
			{
				// fall back to the local rack
				ChooseRandom(numOfReplicas - (results.Count - oldNumOfReplicas), rackLocation, excludedNodes
					, blocksize, maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
			}
		}

		/* choose one node from the nodegroup that <i>localMachine</i> is on.
		* if no such node is available, choose one node from the nodegroup where
		* a second replica is on.
		* if still no such node is available, choose a random node in the cluster.
		* @return the chosen node
		*/
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException
		/// 	"/>
		private DatanodeStorageInfo ChooseLocalNodeGroup(NetworkTopologyWithNodeGroup clusterMap
			, Node localMachine, ICollection<Node> excludedNodes, long blocksize, int maxNodesPerRack
			, IList<DatanodeStorageInfo> results, bool avoidStaleNodes, EnumMap<StorageType, 
			int> storageTypes)
		{
			// no local machine, so choose a random machine
			if (localMachine == null)
			{
				return ChooseRandom(NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack, results
					, avoidStaleNodes, storageTypes);
			}
			// choose one from the local node group
			try
			{
				return ChooseRandom(clusterMap.GetNodeGroup(localMachine.GetNetworkLocation()), excludedNodes
					, blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
			}
			catch (BlockPlacementPolicy.NotEnoughReplicasException)
			{
				DatanodeDescriptor newLocal = SecondNode(localMachine, results);
				if (newLocal != null)
				{
					try
					{
						return ChooseRandom(clusterMap.GetNodeGroup(newLocal.GetNetworkLocation()), excludedNodes
							, blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
					}
					catch (BlockPlacementPolicy.NotEnoughReplicasException)
					{
						//otherwise randomly choose one from the network
						return ChooseRandom(NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack, results
							, avoidStaleNodes, storageTypes);
					}
				}
				else
				{
					//otherwise randomly choose one from the network
					return ChooseRandom(NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack, results
						, avoidStaleNodes, storageTypes);
				}
			}
		}

		protected internal override string GetRack(DatanodeInfo cur)
		{
			string nodeGroupString = cur.GetNetworkLocation();
			return NetworkTopology.GetFirstHalf(nodeGroupString);
		}

		/// <summary>
		/// Find other nodes in the same nodegroup of <i>localMachine</i> and add them
		/// into <i>excludeNodes</i> as replica should not be duplicated for nodes
		/// within the same nodegroup
		/// </summary>
		/// <returns>number of new excluded nodes</returns>
		protected internal override int AddToExcludedNodes(DatanodeDescriptor chosenNode, 
			ICollection<Node> excludedNodes)
		{
			int countOfExcludedNodes = 0;
			string nodeGroupScope = chosenNode.GetNetworkLocation();
			IList<Node> leafNodes = clusterMap.GetLeaves(nodeGroupScope);
			foreach (Node leafNode in leafNodes)
			{
				if (excludedNodes.AddItem(leafNode))
				{
					// not a existing node in excludedNodes
					countOfExcludedNodes++;
				}
			}
			countOfExcludedNodes += AddDependentNodesToExcludedNodes(chosenNode, excludedNodes
				);
			return countOfExcludedNodes;
		}

		/// <summary>Add all nodes from a dependent nodes list to excludedNodes.</summary>
		/// <returns>number of new excluded nodes</returns>
		private int AddDependentNodesToExcludedNodes(DatanodeDescriptor chosenNode, ICollection
			<Node> excludedNodes)
		{
			if (this.host2datanodeMap == null)
			{
				return 0;
			}
			int countOfExcludedNodes = 0;
			foreach (string hostname in chosenNode.GetDependentHostNames())
			{
				DatanodeDescriptor node = this.host2datanodeMap.GetDataNodeByHostName(hostname);
				if (node != null)
				{
					if (excludedNodes.AddItem(node))
					{
						countOfExcludedNodes++;
					}
				}
				else
				{
					Log.Warn("Not able to find datanode " + hostname + " which has dependency with datanode "
						 + chosenNode.GetHostName());
				}
			}
			return countOfExcludedNodes;
		}

		/// <summary>Pick up replica node set for deleting replica as over-replicated.</summary>
		/// <remarks>
		/// Pick up replica node set for deleting replica as over-replicated.
		/// First set contains replica nodes on rack with more than one
		/// replica while second set contains remaining replica nodes.
		/// If first is not empty, divide first set into two subsets:
		/// moreThanOne contains nodes on nodegroup with more than one replica
		/// exactlyOne contains the remaining nodes in first set
		/// then pickup priSet if not empty.
		/// If first is empty, then pick second.
		/// </remarks>
		protected internal override ICollection<DatanodeStorageInfo> PickupReplicaSet(ICollection
			<DatanodeStorageInfo> first, ICollection<DatanodeStorageInfo> second)
		{
			// If no replica within same rack, return directly.
			if (first.IsEmpty())
			{
				return second;
			}
			// Split data nodes in the first set into two sets, 
			// moreThanOne contains nodes on nodegroup with more than one replica
			// exactlyOne contains the remaining nodes
			IDictionary<string, IList<DatanodeStorageInfo>> nodeGroupMap = new Dictionary<string
				, IList<DatanodeStorageInfo>>();
			foreach (DatanodeStorageInfo storage in first)
			{
				string nodeGroupName = NetworkTopology.GetLastHalf(storage.GetDatanodeDescriptor(
					).GetNetworkLocation());
				IList<DatanodeStorageInfo> storageList = nodeGroupMap[nodeGroupName];
				if (storageList == null)
				{
					storageList = new AList<DatanodeStorageInfo>();
					nodeGroupMap[nodeGroupName] = storageList;
				}
				storageList.AddItem(storage);
			}
			IList<DatanodeStorageInfo> moreThanOne = new AList<DatanodeStorageInfo>();
			IList<DatanodeStorageInfo> exactlyOne = new AList<DatanodeStorageInfo>();
			// split nodes into two sets
			foreach (IList<DatanodeStorageInfo> datanodeList in nodeGroupMap.Values)
			{
				if (datanodeList.Count == 1)
				{
					// exactlyOne contains nodes on nodegroup with exactly one replica
					exactlyOne.AddItem(datanodeList[0]);
				}
				else
				{
					// moreThanOne contains nodes on nodegroup with more than one replica
					Sharpen.Collections.AddAll(moreThanOne, datanodeList);
				}
			}
			return moreThanOne.IsEmpty() ? exactlyOne : moreThanOne;
		}
	}
}
