using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// The class is responsible for choosing the desired number of targets
	/// for placing block replicas.
	/// </summary>
	/// <remarks>
	/// The class is responsible for choosing the desired number of targets
	/// for placing block replicas.
	/// The replica placement strategy is that if the writer is on a datanode,
	/// the 1st replica is placed on the local machine,
	/// otherwise a random datanode. The 2nd replica is placed on a datanode
	/// that is on a different rack. The 3rd replica is placed on a datanode
	/// which is on a different node of the rack as the second replica.
	/// </remarks>
	public class BlockPlacementPolicyDefault : BlockPlacementPolicy
	{
		private static readonly string enableDebugLogging = "For more information, please enable DEBUG log level on "
			 + typeof(BlockPlacementPolicy).FullName;

		private sealed class _ThreadLocal_58 : ThreadLocal<StringBuilder>
		{
			public _ThreadLocal_58()
			{
			}

			protected override StringBuilder InitialValue()
			{
				return new StringBuilder();
			}
		}

		private static readonly ThreadLocal<StringBuilder> debugLoggingBuilder = new _ThreadLocal_58
			();

		protected internal bool considerLoad;

		private bool preferLocalNode = true;

		protected internal NetworkTopology clusterMap;

		protected internal Host2NodesMap host2datanodeMap;

		private FSClusterStats stats;

		protected internal long heartbeatInterval;

		private long staleInterval;

		/// <summary>A miss of that many heartbeats is tolerated for replica deletion policy.
		/// 	</summary>
		protected internal int tolerateHeartbeatMultiplier;

		protected internal BlockPlacementPolicyDefault()
		{
		}

		// interval for DataNode heartbeats
		// interval used to identify stale DataNodes
		protected internal override void Initialize(Configuration conf, FSClusterStats stats
			, NetworkTopology clusterMap, Host2NodesMap host2datanodeMap)
		{
			this.considerLoad = conf.GetBoolean(DFSConfigKeys.DfsNamenodeReplicationConsiderloadKey
				, true);
			this.stats = stats;
			this.clusterMap = clusterMap;
			this.host2datanodeMap = host2datanodeMap;
			this.heartbeatInterval = conf.GetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, DFSConfigKeys
				.DfsHeartbeatIntervalDefault) * 1000;
			this.tolerateHeartbeatMultiplier = conf.GetInt(DFSConfigKeys.DfsNamenodeTolerateHeartbeatMultiplierKey
				, DFSConfigKeys.DfsNamenodeTolerateHeartbeatMultiplierDefault);
			this.staleInterval = conf.GetLong(DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalKey
				, DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalDefault);
		}

		public override DatanodeStorageInfo[] ChooseTarget(string srcPath, int numOfReplicas
			, Node writer, IList<DatanodeStorageInfo> chosenNodes, bool returnChosenNodes, ICollection
			<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy)
		{
			return ChooseTarget(numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes
				, blocksize, storagePolicy);
		}

		internal override DatanodeStorageInfo[] ChooseTarget(string src, int numOfReplicas
			, Node writer, ICollection<Node> excludedNodes, long blocksize, IList<DatanodeDescriptor
			> favoredNodes, BlockStoragePolicy storagePolicy)
		{
			try
			{
				if (favoredNodes == null || favoredNodes.Count == 0)
				{
					// Favored nodes not specified, fall back to regular block placement.
					return ChooseTarget(src, numOfReplicas, writer, new AList<DatanodeStorageInfo>(numOfReplicas
						), false, excludedNodes, blocksize, storagePolicy);
				}
				ICollection<Node> favoriteAndExcludedNodes = excludedNodes == null ? new HashSet<
					Node>() : new HashSet<Node>(excludedNodes);
				IList<StorageType> requiredStorageTypes = storagePolicy.ChooseStorageTypes((short
					)numOfReplicas);
				EnumMap<StorageType, int> storageTypes = GetRequiredStorageTypes(requiredStorageTypes
					);
				// Choose favored nodes
				IList<DatanodeStorageInfo> results = new AList<DatanodeStorageInfo>();
				bool avoidStaleNodes = stats != null && stats.IsAvoidingStaleDataNodesForWrite();
				int[] maxNodesAndReplicas = GetMaxNodesPerRack(0, numOfReplicas);
				numOfReplicas = maxNodesAndReplicas[0];
				int maxNodesPerRack = maxNodesAndReplicas[1];
				for (int i = 0; i < favoredNodes.Count && results.Count < numOfReplicas; i++)
				{
					DatanodeDescriptor favoredNode = favoredNodes[i];
					// Choose a single node which is local to favoredNode.
					// 'results' is updated within chooseLocalNode
					DatanodeStorageInfo target = ChooseLocalStorage(favoredNode, favoriteAndExcludedNodes
						, blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes, false);
					if (target == null)
					{
						Log.Warn("Could not find a target for file " + src + " with favored node " + favoredNode
							);
						continue;
					}
					favoriteAndExcludedNodes.AddItem(target.GetDatanodeDescriptor());
				}
				if (results.Count < numOfReplicas)
				{
					// Not enough favored nodes, choose other nodes.
					numOfReplicas -= results.Count;
					DatanodeStorageInfo[] remainingTargets = ChooseTarget(src, numOfReplicas, writer, 
						results, false, favoriteAndExcludedNodes, blocksize, storagePolicy);
					for (int i_1 = 0; i_1 < remainingTargets.Length; i_1++)
					{
						results.AddItem(remainingTargets[i_1]);
					}
				}
				return GetPipeline(writer, Sharpen.Collections.ToArray(results, new DatanodeStorageInfo
					[results.Count]));
			}
			catch (BlockPlacementPolicy.NotEnoughReplicasException nr)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Failed to choose with favored nodes (=" + favoredNodes + "), disregard favored nodes hint and retry."
						, nr);
				}
				// Fall back to regular block placement disregarding favored nodes hint
				return ChooseTarget(src, numOfReplicas, writer, new AList<DatanodeStorageInfo>(numOfReplicas
					), false, excludedNodes, blocksize, storagePolicy);
			}
		}

		/// <summary>This is the implementation.</summary>
		private DatanodeStorageInfo[] ChooseTarget(int numOfReplicas, Node writer, IList<
			DatanodeStorageInfo> chosenStorage, bool returnChosenNodes, ICollection<Node> excludedNodes
			, long blocksize, BlockStoragePolicy storagePolicy)
		{
			if (numOfReplicas == 0 || clusterMap.GetNumOfLeaves() == 0)
			{
				return DatanodeStorageInfo.EmptyArray;
			}
			if (excludedNodes == null)
			{
				excludedNodes = new HashSet<Node>();
			}
			int[] result = GetMaxNodesPerRack(chosenStorage.Count, numOfReplicas);
			numOfReplicas = result[0];
			int maxNodesPerRack = result[1];
			IList<DatanodeStorageInfo> results = new AList<DatanodeStorageInfo>(chosenStorage
				);
			foreach (DatanodeStorageInfo storage in chosenStorage)
			{
				// add localMachine and related nodes to excludedNodes
				AddToExcludedNodes(storage.GetDatanodeDescriptor(), excludedNodes);
			}
			bool avoidStaleNodes = (stats != null && stats.IsAvoidingStaleDataNodesForWrite()
				);
			Node localNode = ChooseTarget(numOfReplicas, writer, excludedNodes, blocksize, maxNodesPerRack
				, results, avoidStaleNodes, storagePolicy, EnumSet.NoneOf<StorageType>(), results
				.IsEmpty());
			if (!returnChosenNodes)
			{
				results.RemoveAll(chosenStorage);
			}
			// sorting nodes to form a pipeline
			return GetPipeline((writer != null && writer is DatanodeDescriptor) ? writer : localNode
				, Sharpen.Collections.ToArray(results, new DatanodeStorageInfo[results.Count]));
		}

		/// <summary>Calculate the maximum number of replicas to allocate per rack.</summary>
		/// <remarks>
		/// Calculate the maximum number of replicas to allocate per rack. It also
		/// limits the total number of replicas to the total number of nodes in the
		/// cluster. Caller should adjust the replica count to the return value.
		/// </remarks>
		/// <param name="numOfChosen">The number of already chosen nodes.</param>
		/// <param name="numOfReplicas">The number of additional nodes to allocate.</param>
		/// <returns>
		/// integer array. Index 0: The number of nodes allowed to allocate
		/// in addition to already chosen nodes.
		/// Index 1: The maximum allowed number of nodes per rack. This
		/// is independent of the number of chosen nodes, as it is calculated
		/// using the target number of replicas.
		/// </returns>
		private int[] GetMaxNodesPerRack(int numOfChosen, int numOfReplicas)
		{
			int clusterSize = clusterMap.GetNumOfLeaves();
			int totalNumOfReplicas = numOfChosen + numOfReplicas;
			if (totalNumOfReplicas > clusterSize)
			{
				numOfReplicas -= (totalNumOfReplicas - clusterSize);
				totalNumOfReplicas = clusterSize;
			}
			// No calculation needed when there is only one rack or picking one node.
			int numOfRacks = clusterMap.GetNumOfRacks();
			if (numOfRacks == 1 || totalNumOfReplicas <= 1)
			{
				return new int[] { numOfReplicas, totalNumOfReplicas };
			}
			int maxNodesPerRack = (totalNumOfReplicas - 1) / numOfRacks + 2;
			// At this point, there are more than one racks and more than one replicas
			// to store. Avoid all replicas being in the same rack.
			//
			// maxNodesPerRack has the following properties at this stage.
			//   1) maxNodesPerRack >= 2
			//   2) (maxNodesPerRack-1) * numOfRacks > totalNumOfReplicas
			//          when numOfRacks > 1
			//
			// Thus, the following adjustment will still result in a value that forces
			// multi-rack allocation and gives enough number of total nodes.
			if (maxNodesPerRack == totalNumOfReplicas)
			{
				maxNodesPerRack--;
			}
			return new int[] { numOfReplicas, maxNodesPerRack };
		}

		private EnumMap<StorageType, int> GetRequiredStorageTypes(IList<StorageType> types
			)
		{
			EnumMap<StorageType, int> map = new EnumMap<StorageType, int>(typeof(StorageType)
				);
			foreach (StorageType type in types)
			{
				if (!map.Contains(type))
				{
					map[type] = 1;
				}
				else
				{
					int num = map[type];
					map[type] = num + 1;
				}
			}
			return map;
		}

		/// <summary>choose <i>numOfReplicas</i> from all data nodes</summary>
		/// <param name="numOfReplicas">additional number of replicas wanted</param>
		/// <param name="writer">the writer's machine, could be a non-DatanodeDescriptor node
		/// 	</param>
		/// <param name="excludedNodes">datanodes that should not be considered as targets</param>
		/// <param name="blocksize">size of the data to be written</param>
		/// <param name="maxNodesPerRack">max nodes allowed per rack</param>
		/// <param name="results">the target nodes already chosen</param>
		/// <param name="avoidStaleNodes">avoid stale nodes in replica choosing</param>
		/// <returns>local node of writer (not chosen node)</returns>
		private Node ChooseTarget(int numOfReplicas, Node writer, ICollection<Node> excludedNodes
			, long blocksize, int maxNodesPerRack, IList<DatanodeStorageInfo> results, bool 
			avoidStaleNodes, BlockStoragePolicy storagePolicy, EnumSet<StorageType> unavailableStorages
			, bool newBlock)
		{
			if (numOfReplicas == 0 || clusterMap.GetNumOfLeaves() == 0)
			{
				return (writer is DatanodeDescriptor) ? writer : null;
			}
			int numOfResults = results.Count;
			int totalReplicasExpected = numOfReplicas + numOfResults;
			if ((writer == null || !(writer is DatanodeDescriptor)) && !newBlock)
			{
				writer = results[0].GetDatanodeDescriptor();
			}
			// Keep a copy of original excludedNodes
			ICollection<Node> oldExcludedNodes = new HashSet<Node>(excludedNodes);
			// choose storage types; use fallbacks for unavailable storages
			IList<StorageType> requiredStorageTypes = storagePolicy.ChooseStorageTypes((short
				)totalReplicasExpected, DatanodeStorageInfo.ToStorageTypes(results), unavailableStorages
				, newBlock);
			EnumMap<StorageType, int> storageTypes = GetRequiredStorageTypes(requiredStorageTypes
				);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("storageTypes=" + storageTypes);
			}
			try
			{
				if ((numOfReplicas = requiredStorageTypes.Count) == 0)
				{
					throw new BlockPlacementPolicy.NotEnoughReplicasException("All required storage types are unavailable: "
						 + " unavailableStorages=" + unavailableStorages + ", storagePolicy=" + storagePolicy
						);
				}
				if (numOfResults == 0)
				{
					writer = ChooseLocalStorage(writer, excludedNodes, blocksize, maxNodesPerRack, results
						, avoidStaleNodes, storageTypes, true).GetDatanodeDescriptor();
					if (--numOfReplicas == 0)
					{
						return writer;
					}
				}
				DatanodeDescriptor dn0 = results[0].GetDatanodeDescriptor();
				if (numOfResults <= 1)
				{
					ChooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes
						, storageTypes);
					if (--numOfReplicas == 0)
					{
						return writer;
					}
				}
				if (numOfResults <= 2)
				{
					DatanodeDescriptor dn1 = results[1].GetDatanodeDescriptor();
					if (clusterMap.IsOnSameRack(dn0, dn1))
					{
						ChooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes
							, storageTypes);
					}
					else
					{
						if (newBlock)
						{
							ChooseLocalRack(dn1, excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes
								, storageTypes);
						}
						else
						{
							ChooseLocalRack(writer, excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes
								, storageTypes);
						}
					}
					if (--numOfReplicas == 0)
					{
						return writer;
					}
				}
				ChooseRandom(numOfReplicas, NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack
					, results, avoidStaleNodes, storageTypes);
			}
			catch (BlockPlacementPolicy.NotEnoughReplicasException e)
			{
				string message = "Failed to place enough replicas, still in need of " + (totalReplicasExpected
					 - results.Count) + " to reach " + totalReplicasExpected + " (unavailableStorages="
					 + unavailableStorages + ", storagePolicy=" + storagePolicy + ", newBlock=" + newBlock
					 + ")";
				if (Log.IsTraceEnabled())
				{
					Log.Trace(message, e);
				}
				else
				{
					Log.Warn(message + " " + e.Message);
				}
				if (avoidStaleNodes)
				{
					// Retry chooseTarget again, this time not avoiding stale nodes.
					// excludedNodes contains the initial excludedNodes and nodes that were
					// not chosen because they were stale, decommissioned, etc.
					// We need to additionally exclude the nodes that were added to the 
					// result list in the successful calls to choose*() above.
					foreach (DatanodeStorageInfo resultStorage in results)
					{
						AddToExcludedNodes(resultStorage.GetDatanodeDescriptor(), oldExcludedNodes);
					}
					// Set numOfReplicas, since it can get out of sync with the result list
					// if the NotEnoughReplicasException was thrown in chooseRandom().
					numOfReplicas = totalReplicasExpected - results.Count;
					return ChooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize, maxNodesPerRack
						, results, false, storagePolicy, unavailableStorages, newBlock);
				}
				bool retry = false;
				// simply add all the remaining types into unavailableStorages and give
				// another try. No best effort is guaranteed here.
				foreach (StorageType type in storageTypes.Keys)
				{
					if (!unavailableStorages.Contains(type))
					{
						unavailableStorages.AddItem(type);
						retry = true;
					}
				}
				if (retry)
				{
					foreach (DatanodeStorageInfo resultStorage in results)
					{
						AddToExcludedNodes(resultStorage.GetDatanodeDescriptor(), oldExcludedNodes);
					}
					numOfReplicas = totalReplicasExpected - results.Count;
					return ChooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize, maxNodesPerRack
						, results, false, storagePolicy, unavailableStorages, newBlock);
				}
			}
			return writer;
		}

		/// <summary>Choose <i>localMachine</i> as the target.</summary>
		/// <remarks>
		/// Choose <i>localMachine</i> as the target.
		/// if <i>localMachine</i> is not available,
		/// choose a node on the same rack
		/// </remarks>
		/// <returns>the chosen storage</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException
		/// 	"/>
		protected internal virtual DatanodeStorageInfo ChooseLocalStorage(Node localMachine
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
			if (preferLocalNode && localMachine is DatanodeDescriptor)
			{
				DatanodeDescriptor localDatanode = (DatanodeDescriptor)localMachine;
				// otherwise try local machine first
				if (excludedNodes.AddItem(localMachine))
				{
					// was not in the excluded list
					for (IEnumerator<KeyValuePair<StorageType, int>> iter = storageTypes.GetEnumerator
						(); iter.HasNext(); )
					{
						KeyValuePair<StorageType, int> entry = iter.Next();
						foreach (DatanodeStorageInfo localStorage in DFSUtil.Shuffle(localDatanode.GetStorageInfos
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
			if (!fallbackToLocalRack)
			{
				return null;
			}
			// try a node on local rack
			return ChooseLocalRack(localMachine, excludedNodes, blocksize, maxNodesPerRack, results
				, avoidStaleNodes, storageTypes);
		}

		/// <summary>
		/// Add <i>localMachine</i> and related nodes to <i>excludedNodes</i>
		/// for next replica choosing.
		/// </summary>
		/// <remarks>
		/// Add <i>localMachine</i> and related nodes to <i>excludedNodes</i>
		/// for next replica choosing. In sub class, we can add more nodes within
		/// the same failure domain of localMachine
		/// </remarks>
		/// <returns>number of new excluded nodes</returns>
		protected internal virtual int AddToExcludedNodes(DatanodeDescriptor localMachine
			, ICollection<Node> excludedNodes)
		{
			return excludedNodes.AddItem(localMachine) ? 1 : 0;
		}

		/// <summary>Choose one node from the rack that <i>localMachine</i> is on.</summary>
		/// <remarks>
		/// Choose one node from the rack that <i>localMachine</i> is on.
		/// if no such node is available, choose one node from the rack where
		/// a second replica is on.
		/// if still no such node is available, choose a random node
		/// in the cluster.
		/// </remarks>
		/// <returns>the chosen node</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException
		/// 	"/>
		protected internal virtual DatanodeStorageInfo ChooseLocalRack(Node localMachine, 
			ICollection<Node> excludedNodes, long blocksize, int maxNodesPerRack, IList<DatanodeStorageInfo
			> results, bool avoidStaleNodes, EnumMap<StorageType, int> storageTypes)
		{
			// no local machine, so choose a random machine
			if (localMachine == null)
			{
				return ChooseRandom(NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack, results
					, avoidStaleNodes, storageTypes);
			}
			string localRack = localMachine.GetNetworkLocation();
			try
			{
				// choose one from the local rack
				return ChooseRandom(localRack, excludedNodes, blocksize, maxNodesPerRack, results
					, avoidStaleNodes, storageTypes);
			}
			catch (BlockPlacementPolicy.NotEnoughReplicasException e)
			{
				// find the next replica and retry with its rack
				foreach (DatanodeStorageInfo resultStorage in results)
				{
					DatanodeDescriptor nextNode = resultStorage.GetDatanodeDescriptor();
					if (nextNode != localMachine)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Failed to choose from local rack (location = " + localRack + "), retry with the rack of the next replica (location = "
								 + nextNode.GetNetworkLocation() + ")", e);
						}
						return ChooseFromNextRack(nextNode, excludedNodes, blocksize, maxNodesPerRack, results
							, avoidStaleNodes, storageTypes);
					}
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Failed to choose from local rack (location = " + localRack + "); the second replica is not found, retry choosing ramdomly"
						, e);
				}
				//the second replica is not found, randomly choose one from the network
				return ChooseRandom(NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack, results
					, avoidStaleNodes, storageTypes);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException
		/// 	"/>
		private DatanodeStorageInfo ChooseFromNextRack(Node next, ICollection<Node> excludedNodes
			, long blocksize, int maxNodesPerRack, IList<DatanodeStorageInfo> results, bool 
			avoidStaleNodes, EnumMap<StorageType, int> storageTypes)
		{
			string nextRack = next.GetNetworkLocation();
			try
			{
				return ChooseRandom(nextRack, excludedNodes, blocksize, maxNodesPerRack, results, 
					avoidStaleNodes, storageTypes);
			}
			catch (BlockPlacementPolicy.NotEnoughReplicasException e)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Failed to choose from the next rack (location = " + nextRack + "), retry choosing ramdomly"
						, e);
				}
				//otherwise randomly choose one from the network
				return ChooseRandom(NodeBase.Root, excludedNodes, blocksize, maxNodesPerRack, results
					, avoidStaleNodes, storageTypes);
			}
		}

		/// <summary>
		/// Choose <i>numOfReplicas</i> nodes from the racks
		/// that <i>localMachine</i> is NOT on.
		/// </summary>
		/// <remarks>
		/// Choose <i>numOfReplicas</i> nodes from the racks
		/// that <i>localMachine</i> is NOT on.
		/// if not enough nodes are available, choose the remaining ones
		/// from the local rack
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException
		/// 	"/>
		protected internal virtual void ChooseRemoteRack(int numOfReplicas, DatanodeDescriptor
			 localMachine, ICollection<Node> excludedNodes, long blocksize, int maxReplicasPerRack
			, IList<DatanodeStorageInfo> results, bool avoidStaleNodes, EnumMap<StorageType, 
			int> storageTypes)
		{
			int oldNumOfReplicas = results.Count;
			// randomly choose one node from remote racks
			try
			{
				ChooseRandom(numOfReplicas, "~" + localMachine.GetNetworkLocation(), excludedNodes
					, blocksize, maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
			}
			catch (BlockPlacementPolicy.NotEnoughReplicasException e)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Failed to choose remote rack (location = ~" + localMachine.GetNetworkLocation
						() + "), fallback to local rack", e);
				}
				ChooseRandom(numOfReplicas - (results.Count - oldNumOfReplicas), localMachine.GetNetworkLocation
					(), excludedNodes, blocksize, maxReplicasPerRack, results, avoidStaleNodes, storageTypes
					);
			}
		}

		/// <summary>Randomly choose one target from the given <i>scope</i>.</summary>
		/// <returns>the chosen storage, if there is any.</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException
		/// 	"/>
		protected internal virtual DatanodeStorageInfo ChooseRandom(string scope, ICollection
			<Node> excludedNodes, long blocksize, int maxNodesPerRack, IList<DatanodeStorageInfo
			> results, bool avoidStaleNodes, EnumMap<StorageType, int> storageTypes)
		{
			return ChooseRandom(1, scope, excludedNodes, blocksize, maxNodesPerRack, results, 
				avoidStaleNodes, storageTypes);
		}

		/// <summary>Randomly choose <i>numOfReplicas</i> targets from the given <i>scope</i>.
		/// 	</summary>
		/// <returns>the first chosen node, if there is any.</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException
		/// 	"/>
		protected internal virtual DatanodeStorageInfo ChooseRandom(int numOfReplicas, string
			 scope, ICollection<Node> excludedNodes, long blocksize, int maxNodesPerRack, IList
			<DatanodeStorageInfo> results, bool avoidStaleNodes, EnumMap<StorageType, int> storageTypes
			)
		{
			int numOfAvailableNodes = clusterMap.CountNumOfAvailableNodes(scope, excludedNodes
				);
			StringBuilder builder = null;
			if (Log.IsDebugEnabled())
			{
				builder = debugLoggingBuilder.Get();
				builder.Length = 0;
				builder.Append("[");
			}
			bool badTarget = false;
			DatanodeStorageInfo firstChosen = null;
			while (numOfReplicas > 0 && numOfAvailableNodes > 0)
			{
				DatanodeDescriptor chosenNode = (DatanodeDescriptor)clusterMap.ChooseRandom(scope
					);
				if (excludedNodes.AddItem(chosenNode))
				{
					//was not in the excluded list
					if (Log.IsDebugEnabled())
					{
						builder.Append("\nNode ").Append(NodeBase.GetPath(chosenNode)).Append(" [");
					}
					numOfAvailableNodes--;
					DatanodeStorageInfo[] storages = DFSUtil.Shuffle(chosenNode.GetStorageInfos());
					int i = 0;
					bool search = true;
					for (IEnumerator<KeyValuePair<StorageType, int>> iter = storageTypes.GetEnumerator
						(); search && iter.HasNext(); )
					{
						KeyValuePair<StorageType, int> entry = iter.Next();
						for (i = 0; i < storages.Length; i++)
						{
							StorageType type = entry.Key;
							int newExcludedNodes = AddIfIsGoodTarget(storages[i], excludedNodes, blocksize, maxNodesPerRack
								, considerLoad, results, avoidStaleNodes, type);
							if (newExcludedNodes >= 0)
							{
								numOfReplicas--;
								if (firstChosen == null)
								{
									firstChosen = storages[i];
								}
								numOfAvailableNodes -= newExcludedNodes;
								int num = entry.Value;
								if (num == 1)
								{
									iter.Remove();
								}
								else
								{
									entry.SetValue(num - 1);
								}
								search = false;
								break;
							}
						}
					}
					if (Log.IsDebugEnabled())
					{
						builder.Append("\n]");
					}
					// If no candidate storage was found on this DN then set badTarget.
					badTarget = (i == storages.Length);
				}
			}
			if (numOfReplicas > 0)
			{
				string detail = enableDebugLogging;
				if (Log.IsDebugEnabled())
				{
					if (badTarget && builder != null)
					{
						detail = builder.ToString();
						builder.Length = 0;
					}
					else
					{
						detail = string.Empty;
					}
				}
				throw new BlockPlacementPolicy.NotEnoughReplicasException(detail);
			}
			return firstChosen;
		}

		/// <summary>
		/// If the given storage is a good target, add it to the result list and
		/// update the set of excluded nodes.
		/// </summary>
		/// <returns>
		/// -1 if the given is not a good target;
		/// otherwise, return the number of nodes added to excludedNodes set.
		/// </returns>
		internal virtual int AddIfIsGoodTarget(DatanodeStorageInfo storage, ICollection<Node
			> excludedNodes, long blockSize, int maxNodesPerRack, bool considerLoad, IList<DatanodeStorageInfo
			> results, bool avoidStaleNodes, StorageType storageType)
		{
			if (IsGoodTarget(storage, blockSize, maxNodesPerRack, considerLoad, results, avoidStaleNodes
				, storageType))
			{
				results.AddItem(storage);
				// add node and related nodes to excludedNode
				return AddToExcludedNodes(storage.GetDatanodeDescriptor(), excludedNodes);
			}
			else
			{
				return -1;
			}
		}

		private static void LogNodeIsNotChosen(DatanodeStorageInfo storage, string reason
			)
		{
			if (Log.IsDebugEnabled())
			{
				// build the error message for later use.
				debugLoggingBuilder.Get().Append("\n  Storage ").Append(storage).Append(" is not chosen since "
					).Append(reason).Append(".");
			}
		}

		/// <summary>Determine if a storage is a good target.</summary>
		/// <param name="storage">The target storage</param>
		/// <param name="blockSize">Size of block</param>
		/// <param name="maxTargetPerRack">
		/// Maximum number of targets per rack. The value of
		/// this parameter depends on the number of racks in
		/// the cluster and total number of replicas for a block
		/// </param>
		/// <param name="considerLoad">whether or not to consider load of the target node</param>
		/// <param name="results">
		/// A list containing currently chosen nodes. Used to check if
		/// too many nodes has been chosen in the target rack.
		/// </param>
		/// <param name="avoidStaleNodes">Whether or not to avoid choosing stale nodes</param>
		/// <returns>
		/// Return true if <i>node</i> has enough space,
		/// does not have too much load,
		/// and the rack does not have too many nodes.
		/// </returns>
		private bool IsGoodTarget(DatanodeStorageInfo storage, long blockSize, int maxTargetPerRack
			, bool considerLoad, IList<DatanodeStorageInfo> results, bool avoidStaleNodes, StorageType
			 requiredStorageType)
		{
			if (storage.GetStorageType() != requiredStorageType)
			{
				LogNodeIsNotChosen(storage, "storage types do not match," + " where the required storage type is "
					 + requiredStorageType);
				return false;
			}
			if (storage.GetState() == DatanodeStorage.State.ReadOnlyShared)
			{
				LogNodeIsNotChosen(storage, "storage is read-only");
				return false;
			}
			if (storage.GetState() == DatanodeStorage.State.Failed)
			{
				LogNodeIsNotChosen(storage, "storage has failed");
				return false;
			}
			DatanodeDescriptor node = storage.GetDatanodeDescriptor();
			// check if the node is (being) decommissioned
			if (node.IsDecommissionInProgress() || node.IsDecommissioned())
			{
				LogNodeIsNotChosen(storage, "the node is (being) decommissioned ");
				return false;
			}
			if (avoidStaleNodes)
			{
				if (node.IsStale(this.staleInterval))
				{
					LogNodeIsNotChosen(storage, "the node is stale ");
					return false;
				}
			}
			long requiredSize = blockSize * HdfsConstants.MinBlocksForWrite;
			long scheduledSize = blockSize * node.GetBlocksScheduled(storage.GetStorageType()
				);
			long remaining = node.GetRemaining(storage.GetStorageType(), requiredSize);
			if (requiredSize > remaining - scheduledSize)
			{
				LogNodeIsNotChosen(storage, "the node does not have enough " + storage.GetStorageType
					() + " space" + " (required=" + requiredSize + ", scheduled=" + scheduledSize + 
					", remaining=" + remaining + ")");
				return false;
			}
			// check the communication traffic of the target machine
			if (considerLoad)
			{
				double maxLoad = 2.0 * stats.GetInServiceXceiverAverage();
				int nodeLoad = node.GetXceiverCount();
				if (nodeLoad > maxLoad)
				{
					LogNodeIsNotChosen(storage, "the node is too busy (load: " + nodeLoad + " > " + maxLoad
						 + ") ");
					return false;
				}
			}
			// check if the target rack has chosen too many nodes
			string rackname = node.GetNetworkLocation();
			int counter = 1;
			foreach (DatanodeStorageInfo resultStorage in results)
			{
				if (rackname.Equals(resultStorage.GetDatanodeDescriptor().GetNetworkLocation()))
				{
					counter++;
				}
			}
			if (counter > maxTargetPerRack)
			{
				LogNodeIsNotChosen(storage, "the rack has too many chosen nodes ");
				return false;
			}
			return true;
		}

		/// <summary>Return a pipeline of nodes.</summary>
		/// <remarks>
		/// Return a pipeline of nodes.
		/// The pipeline is formed finding a shortest path that
		/// starts from the writer and traverses all <i>nodes</i>
		/// This is basically a traveling salesman problem.
		/// </remarks>
		private DatanodeStorageInfo[] GetPipeline(Node writer, DatanodeStorageInfo[] storages
			)
		{
			if (storages.Length == 0)
			{
				return storages;
			}
			lock (clusterMap)
			{
				int index = 0;
				if (writer == null || !clusterMap.Contains(writer))
				{
					writer = storages[0].GetDatanodeDescriptor();
				}
				for (; index < storages.Length; index++)
				{
					DatanodeStorageInfo shortestStorage = storages[index];
					int shortestDistance = clusterMap.GetDistance(writer, shortestStorage.GetDatanodeDescriptor
						());
					int shortestIndex = index;
					for (int i = index + 1; i < storages.Length; i++)
					{
						int currentDistance = clusterMap.GetDistance(writer, storages[i].GetDatanodeDescriptor
							());
						if (shortestDistance > currentDistance)
						{
							shortestDistance = currentDistance;
							shortestStorage = storages[i];
							shortestIndex = i;
						}
					}
					//switch position index & shortestIndex
					if (index != shortestIndex)
					{
						storages[shortestIndex] = storages[index];
						storages[index] = shortestStorage;
					}
					writer = shortestStorage.GetDatanodeDescriptor();
				}
			}
			return storages;
		}

		public override BlockPlacementStatus VerifyBlockPlacement(string srcPath, LocatedBlock
			 lBlk, int numberOfReplicas)
		{
			DatanodeInfo[] locs = lBlk.GetLocations();
			if (locs == null)
			{
				locs = DatanodeDescriptor.EmptyArray;
			}
			int numRacks = clusterMap.GetNumOfRacks();
			if (numRacks <= 1)
			{
				// only one rack
				return new BlockPlacementStatusDefault(Math.Min(numRacks, numberOfReplicas), numRacks
					);
			}
			int minRacks = Math.Min(2, numberOfReplicas);
			// 1. Check that all locations are different.
			// 2. Count locations on different racks.
			ICollection<string> racks = new TreeSet<string>();
			foreach (DatanodeInfo dn in locs)
			{
				racks.AddItem(dn.GetNetworkLocation());
			}
			return new BlockPlacementStatusDefault(racks.Count, minRacks);
		}

		public override DatanodeStorageInfo ChooseReplicaToDelete(BlockCollection bc, Block
			 block, short replicationFactor, ICollection<DatanodeStorageInfo> first, ICollection
			<DatanodeStorageInfo> second, IList<StorageType> excessTypes)
		{
			long oldestHeartbeat = Time.MonotonicNow() - heartbeatInterval * tolerateHeartbeatMultiplier;
			DatanodeStorageInfo oldestHeartbeatStorage = null;
			long minSpace = long.MaxValue;
			DatanodeStorageInfo minSpaceStorage = null;
			// Pick the node with the oldest heartbeat or with the least free space,
			// if all hearbeats are within the tolerable heartbeat interval
			foreach (DatanodeStorageInfo storage in PickupReplicaSet(first, second))
			{
				if (!excessTypes.Contains(storage.GetStorageType()))
				{
					continue;
				}
				DatanodeDescriptor node = storage.GetDatanodeDescriptor();
				long free = node.GetRemaining();
				long lastHeartbeat = node.GetLastUpdateMonotonic();
				if (lastHeartbeat < oldestHeartbeat)
				{
					oldestHeartbeat = lastHeartbeat;
					oldestHeartbeatStorage = storage;
				}
				if (minSpace > free)
				{
					minSpace = free;
					minSpaceStorage = storage;
				}
			}
			DatanodeStorageInfo storage_1;
			if (oldestHeartbeatStorage != null)
			{
				storage_1 = oldestHeartbeatStorage;
			}
			else
			{
				if (minSpaceStorage != null)
				{
					storage_1 = minSpaceStorage;
				}
				else
				{
					return null;
				}
			}
			excessTypes.Remove(storage_1.GetStorageType());
			return storage_1;
		}

		/// <summary>Pick up replica node set for deleting replica as over-replicated.</summary>
		/// <remarks>
		/// Pick up replica node set for deleting replica as over-replicated.
		/// First set contains replica nodes on rack with more than one
		/// replica while second set contains remaining replica nodes.
		/// So pick up first set if not empty. If first is empty, then pick second.
		/// </remarks>
		protected internal virtual ICollection<DatanodeStorageInfo> PickupReplicaSet(ICollection
			<DatanodeStorageInfo> first, ICollection<DatanodeStorageInfo> second)
		{
			return first.IsEmpty() ? second : first;
		}

		[VisibleForTesting]
		internal virtual void SetPreferLocalNode(bool prefer)
		{
			this.preferLocalNode = prefer;
		}
	}
}
