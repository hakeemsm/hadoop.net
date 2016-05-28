using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Manages datanode decommissioning.</summary>
	/// <remarks>
	/// Manages datanode decommissioning. A background monitor thread
	/// periodically checks the status of datanodes that are in-progress of
	/// decommissioning.
	/// <p/>
	/// A datanode can be decommissioned in a few situations:
	/// <ul>
	/// <li>If a DN is dead, it is decommissioned immediately.</li>
	/// <li>If a DN is alive, it is decommissioned after all of its blocks
	/// are sufficiently replicated. Merely under-replicated blocks do not
	/// block decommissioning as long as they are above a replication
	/// threshold.</li>
	/// </ul>
	/// In the second case, the datanode transitions to a
	/// decommission-in-progress state and is tracked by the monitor thread. The
	/// monitor periodically scans through the list of insufficiently replicated
	/// blocks on these datanodes to
	/// determine if they can be decommissioned. The monitor also prunes this list
	/// as blocks become replicated, so monitor scans will become more efficient
	/// over time.
	/// <p/>
	/// Decommission-in-progress nodes that become dead do not progress to
	/// decommissioned until they become live again. This prevents potential
	/// durability loss for singly-replicated blocks (see HDFS-6791).
	/// <p/>
	/// This class depends on the FSNamesystem lock for synchronization.
	/// </remarks>
	public class DecommissionManager
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DecommissionManager
			));

		private readonly Namesystem namesystem;

		private readonly BlockManager blockManager;

		private readonly HeartbeatManager hbManager;

		private readonly ScheduledExecutorService executor;

		/// <summary>
		/// Map containing the decommission-in-progress datanodes that are being
		/// tracked so they can be be marked as decommissioned.
		/// </summary>
		/// <remarks>
		/// Map containing the decommission-in-progress datanodes that are being
		/// tracked so they can be be marked as decommissioned.
		/// <p/>
		/// This holds a set of references to the under-replicated blocks on the DN at
		/// the time the DN is added to the map, i.e. the blocks that are preventing
		/// the node from being marked as decommissioned. During a monitor tick, this
		/// list is pruned as blocks becomes replicated.
		/// <p/>
		/// Note also that the reference to the list of under-replicated blocks
		/// will be null on initial add
		/// <p/>
		/// However, this map can become out-of-date since it is not updated by block
		/// reports or other events. Before being finally marking as decommissioned,
		/// another check is done with the actual block map.
		/// </remarks>
		private readonly SortedDictionary<DatanodeDescriptor, AbstractList<BlockInfoContiguous
			>> decomNodeBlocks;

		/// <summary>Tracking a node in decomNodeBlocks consumes additional memory.</summary>
		/// <remarks>
		/// Tracking a node in decomNodeBlocks consumes additional memory. To limit
		/// the impact on NN memory consumption, we limit the number of nodes in
		/// decomNodeBlocks. Additional nodes wait in pendingNodes.
		/// </remarks>
		private readonly Queue<DatanodeDescriptor> pendingNodes;

		private DecommissionManager.Monitor monitor = null;

		internal DecommissionManager(Namesystem namesystem, BlockManager blockManager, HeartbeatManager
			 hbManager)
		{
			this.namesystem = namesystem;
			this.blockManager = blockManager;
			this.hbManager = hbManager;
			executor = Executors.NewScheduledThreadPool(1, new ThreadFactoryBuilder().SetNameFormat
				("DecommissionMonitor-%d").SetDaemon(true).Build());
			decomNodeBlocks = new SortedDictionary<DatanodeDescriptor, AbstractList<BlockInfoContiguous
				>>();
			pendingNodes = new List<DatanodeDescriptor>();
		}

		/// <summary>Start the decommission monitor thread.</summary>
		/// <param name="conf"/>
		internal virtual void Activate(Configuration conf)
		{
			int intervalSecs = conf.GetInt(DFSConfigKeys.DfsNamenodeDecommissionIntervalKey, 
				DFSConfigKeys.DfsNamenodeDecommissionIntervalDefault);
			Preconditions.CheckArgument(intervalSecs >= 0, "Cannot set a negative " + "value for "
				 + DFSConfigKeys.DfsNamenodeDecommissionIntervalKey);
			// By default, the new configuration key overrides the deprecated one.
			// No # node limit is set.
			int blocksPerInterval = conf.GetInt(DFSConfigKeys.DfsNamenodeDecommissionBlocksPerIntervalKey
				, DFSConfigKeys.DfsNamenodeDecommissionBlocksPerIntervalDefault);
			int nodesPerInterval = int.MaxValue;
			// If the expected key isn't present and the deprecated one is, 
			// use the deprecated one into the new one. This overrides the 
			// default.
			//
			// Also print a deprecation warning.
			string deprecatedKey = "dfs.namenode.decommission.nodes.per.interval";
			string strNodes = conf.Get(deprecatedKey);
			if (strNodes != null)
			{
				nodesPerInterval = System.Convert.ToInt32(strNodes);
				blocksPerInterval = int.MaxValue;
				Log.Warn("Using deprecated configuration key {} value of {}.", deprecatedKey, nodesPerInterval
					);
				Log.Warn("Please update your configuration to use {} instead.", DFSConfigKeys.DfsNamenodeDecommissionBlocksPerIntervalKey
					);
			}
			Preconditions.CheckArgument(blocksPerInterval > 0, "Must set a positive value for "
				 + DFSConfigKeys.DfsNamenodeDecommissionBlocksPerIntervalKey);
			int maxConcurrentTrackedNodes = conf.GetInt(DFSConfigKeys.DfsNamenodeDecommissionMaxConcurrentTrackedNodes
				, DFSConfigKeys.DfsNamenodeDecommissionMaxConcurrentTrackedNodesDefault);
			Preconditions.CheckArgument(maxConcurrentTrackedNodes >= 0, "Cannot set a negative "
				 + "value for " + DFSConfigKeys.DfsNamenodeDecommissionMaxConcurrentTrackedNodes
				);
			monitor = new DecommissionManager.Monitor(this, blocksPerInterval, nodesPerInterval
				, maxConcurrentTrackedNodes);
			executor.ScheduleAtFixedRate(monitor, intervalSecs, intervalSecs, TimeUnit.Seconds
				);
			Log.Debug("Activating DecommissionManager with interval {} seconds, " + "{} max blocks per interval, {} max nodes per interval, "
				 + "{} max concurrently tracked nodes.", intervalSecs, blocksPerInterval, nodesPerInterval
				, maxConcurrentTrackedNodes);
		}

		/// <summary>Stop the decommission monitor thread, waiting briefly for it to terminate.
		/// 	</summary>
		internal virtual void Close()
		{
			executor.ShutdownNow();
			try
			{
				executor.AwaitTermination(3000, TimeUnit.Milliseconds);
			}
			catch (Exception)
			{
			}
		}

		/// <summary>Start decommissioning the specified datanode.</summary>
		/// <param name="node"/>
		[VisibleForTesting]
		public virtual void StartDecommission(DatanodeDescriptor node)
		{
			if (!node.IsDecommissionInProgress() && !node.IsDecommissioned())
			{
				// Update DN stats maintained by HeartbeatManager
				hbManager.StartDecommission(node);
				// hbManager.startDecommission will set dead node to decommissioned.
				if (node.IsDecommissionInProgress())
				{
					foreach (DatanodeStorageInfo storage in node.GetStorageInfos())
					{
						Log.Info("Starting decommission of {} {} with {} blocks", node, storage, storage.
							NumBlocks());
					}
					node.decommissioningStatus.SetStartTime(Time.MonotonicNow());
					pendingNodes.AddItem(node);
				}
			}
			else
			{
				Log.Trace("startDecommission: Node {} in {}, nothing to do." + node, node.GetAdminState
					());
			}
		}

		/// <summary>Stop decommissioning the specified datanode.</summary>
		/// <param name="node"/>
		[VisibleForTesting]
		public virtual void StopDecommission(DatanodeDescriptor node)
		{
			if (node.IsDecommissionInProgress() || node.IsDecommissioned())
			{
				// Update DN stats maintained by HeartbeatManager
				hbManager.StopDecommission(node);
				// Over-replicated blocks will be detected and processed when
				// the dead node comes back and send in its full block report.
				if (node.isAlive)
				{
					blockManager.ProcessOverReplicatedBlocksOnReCommission(node);
				}
				// Remove from tracking in DecommissionManager
				pendingNodes.Remove(node);
				Sharpen.Collections.Remove(decomNodeBlocks, node);
			}
			else
			{
				Log.Trace("stopDecommission: Node {} in {}, nothing to do." + node, node.GetAdminState
					());
			}
		}

		private void SetDecommissioned(DatanodeDescriptor dn)
		{
			dn.SetDecommissioned();
			Log.Info("Decommissioning complete for node {}", dn);
		}

		/// <summary>Checks whether a block is sufficiently replicated for decommissioning.</summary>
		/// <remarks>
		/// Checks whether a block is sufficiently replicated for decommissioning.
		/// Full-strength replication is not always necessary, hence "sufficient".
		/// </remarks>
		/// <returns>true if sufficient, else false.</returns>
		private bool IsSufficientlyReplicated(BlockInfoContiguous block, BlockCollection 
			bc, NumberReplicas numberReplicas)
		{
			int numExpected = bc.GetBlockReplication();
			int numLive = numberReplicas.LiveReplicas();
			if (!blockManager.IsNeededReplication(block, numExpected, numLive))
			{
				// Block doesn't need replication. Skip.
				Log.Trace("Block {} does not need replication.", block);
				return true;
			}
			// Block is under-replicated
			Log.Trace("Block {} numExpected={}, numLive={}", block, numExpected, numLive);
			if (numExpected > numLive)
			{
				if (bc.IsUnderConstruction() && block.Equals(bc.GetLastBlock()))
				{
					// Can decom a UC block as long as there will still be minReplicas
					if (numLive >= blockManager.minReplication)
					{
						Log.Trace("UC block {} sufficiently-replicated since numLive ({}) " + ">= minR ({})"
							, block, numLive, blockManager.minReplication);
						return true;
					}
					else
					{
						Log.Trace("UC block {} insufficiently-replicated since numLive " + "({}) < minR ({})"
							, block, numLive, blockManager.minReplication);
					}
				}
				else
				{
					// Can decom a non-UC as long as the default replication is met
					if (numLive >= blockManager.defaultReplication)
					{
						return true;
					}
				}
			}
			return false;
		}

		private static void LogBlockReplicationInfo(Block block, BlockCollection bc, DatanodeDescriptor
			 srcNode, NumberReplicas num, IEnumerable<DatanodeStorageInfo> storages)
		{
			int curReplicas = num.LiveReplicas();
			int curExpectedReplicas = bc.GetBlockReplication();
			StringBuilder nodeList = new StringBuilder();
			foreach (DatanodeStorageInfo storage in storages)
			{
				DatanodeDescriptor node = storage.GetDatanodeDescriptor();
				nodeList.Append(node);
				nodeList.Append(" ");
			}
			Log.Info("Block: " + block + ", Expected Replicas: " + curExpectedReplicas + ", live replicas: "
				 + curReplicas + ", corrupt replicas: " + num.CorruptReplicas() + ", decommissioned replicas: "
				 + num.DecommissionedReplicas() + ", excess replicas: " + num.ExcessReplicas() +
				 ", Is Open File: " + bc.IsUnderConstruction() + ", Datanodes having this block: "
				 + nodeList + ", Current Datanode: " + srcNode + ", Is current datanode decommissioning: "
				 + srcNode.IsDecommissionInProgress());
		}

		[VisibleForTesting]
		public virtual int GetNumPendingNodes()
		{
			return pendingNodes.Count;
		}

		[VisibleForTesting]
		public virtual int GetNumTrackedNodes()
		{
			return decomNodeBlocks.Count;
		}

		[VisibleForTesting]
		public virtual int GetNumNodesChecked()
		{
			return monitor.numNodesChecked;
		}

		/// <summary>Checks to see if DNs have finished decommissioning.</summary>
		/// <remarks>
		/// Checks to see if DNs have finished decommissioning.
		/// <p/>
		/// Since this is done while holding the namesystem lock,
		/// the amount of work per monitor tick is limited.
		/// </remarks>
		private class Monitor : Runnable
		{
			/// <summary>The maximum number of blocks to check per tick.</summary>
			private readonly int numBlocksPerCheck;

			/// <summary>The maximum number of nodes to check per tick.</summary>
			private readonly int numNodesPerCheck;

			/// <summary>The maximum number of nodes to track in decomNodeBlocks.</summary>
			/// <remarks>
			/// The maximum number of nodes to track in decomNodeBlocks. A value of 0
			/// means no limit.
			/// </remarks>
			private readonly int maxConcurrentTrackedNodes;

			/// <summary>The number of blocks that have been checked on this tick.</summary>
			private int numBlocksChecked = 0;

			/// <summary>The number of nodes that have been checked on this tick.</summary>
			/// <remarks>
			/// The number of nodes that have been checked on this tick. Used for
			/// testing.
			/// </remarks>
			private int numNodesChecked = 0;

			/// <summary>The last datanode in decomNodeBlocks that we've processed</summary>
			private DatanodeDescriptor iterkey = new DatanodeDescriptor(new DatanodeID(string.Empty
				, string.Empty, string.Empty, 0, 0, 0, 0));

			internal Monitor(DecommissionManager _enclosing, int numBlocksPerCheck, int numNodesPerCheck
				, int maxConcurrentTrackedNodes)
			{
				this._enclosing = _enclosing;
				this.numBlocksPerCheck = numBlocksPerCheck;
				this.numNodesPerCheck = numNodesPerCheck;
				this.maxConcurrentTrackedNodes = maxConcurrentTrackedNodes;
			}

			private bool ExceededNumBlocksPerCheck()
			{
				DecommissionManager.Log.Trace("Processed {} blocks so far this tick", this.numBlocksChecked
					);
				return this.numBlocksChecked >= this.numBlocksPerCheck;
			}

			[Obsolete]
			private bool ExceededNumNodesPerCheck()
			{
				DecommissionManager.Log.Trace("Processed {} nodes so far this tick", this.numNodesChecked
					);
				return this.numNodesChecked >= this.numNodesPerCheck;
			}

			public virtual void Run()
			{
				if (!this._enclosing.namesystem.IsRunning())
				{
					DecommissionManager.Log.Info("Namesystem is not running, skipping decommissioning checks"
						 + ".");
					return;
				}
				// Reset the checked count at beginning of each iteration
				this.numBlocksChecked = 0;
				this.numNodesChecked = 0;
				// Check decom progress
				this._enclosing.namesystem.WriteLock();
				try
				{
					this.ProcessPendingNodes();
					this.Check();
				}
				finally
				{
					this._enclosing.namesystem.WriteUnlock();
				}
				if (this.numBlocksChecked + this.numNodesChecked > 0)
				{
					DecommissionManager.Log.Info("Checked {} blocks and {} nodes this tick", this.numBlocksChecked
						, this.numNodesChecked);
				}
			}

			/// <summary>
			/// Pop datanodes off the pending list and into decomNodeBlocks,
			/// subject to the maxConcurrentTrackedNodes limit.
			/// </summary>
			private void ProcessPendingNodes()
			{
				while (!this._enclosing.pendingNodes.IsEmpty() && (this.maxConcurrentTrackedNodes
					 == 0 || this._enclosing.decomNodeBlocks.Count < this.maxConcurrentTrackedNodes)
					)
				{
					this._enclosing.decomNodeBlocks[this._enclosing.pendingNodes.Poll()] = null;
				}
			}

			private void Check()
			{
				IEnumerator<KeyValuePair<DatanodeDescriptor, AbstractList<BlockInfoContiguous>>> 
					it = new CyclicIteration<DatanodeDescriptor, AbstractList<BlockInfoContiguous>>(
					this._enclosing.decomNodeBlocks, this.iterkey).GetEnumerator();
				List<DatanodeDescriptor> toRemove = new List<DatanodeDescriptor>();
				while (it.HasNext() && !this.ExceededNumBlocksPerCheck() && !this.ExceededNumNodesPerCheck
					())
				{
					this.numNodesChecked++;
					KeyValuePair<DatanodeDescriptor, AbstractList<BlockInfoContiguous>> entry = it.Next
						();
					DatanodeDescriptor dn = entry.Key;
					AbstractList<BlockInfoContiguous> blocks = entry.Value;
					bool fullScan = false;
					if (blocks == null)
					{
						// This is a newly added datanode, run through its list to schedule 
						// under-replicated blocks for replication and collect the blocks 
						// that are insufficiently replicated for further tracking
						DecommissionManager.Log.Debug("Newly-added node {}, doing full scan to find " + "insufficiently-replicated blocks."
							, dn);
						blocks = this.HandleInsufficientlyReplicated(dn);
						this._enclosing.decomNodeBlocks[dn] = blocks;
						fullScan = true;
					}
					else
					{
						// This is a known datanode, check if its # of insufficiently 
						// replicated blocks has dropped to zero and if it can be decommed
						DecommissionManager.Log.Debug("Processing decommission-in-progress node {}", dn);
						this.PruneSufficientlyReplicated(dn, blocks);
					}
					if (blocks.Count == 0)
					{
						if (!fullScan)
						{
							// If we didn't just do a full scan, need to re-check with the 
							// full block map.
							//
							// We've replicated all the known insufficiently replicated 
							// blocks. Re-check with the full block map before finally 
							// marking the datanode as decommissioned 
							DecommissionManager.Log.Debug("Node {} has finished replicating current set of " 
								+ "blocks, checking with the full block map.", dn);
							blocks = this.HandleInsufficientlyReplicated(dn);
							this._enclosing.decomNodeBlocks[dn] = blocks;
						}
						// If the full scan is clean AND the node liveness is okay, 
						// we can finally mark as decommissioned.
						bool isHealthy = this._enclosing.blockManager.IsNodeHealthyForDecommission(dn);
						if (blocks.Count == 0 && isHealthy)
						{
							this._enclosing.SetDecommissioned(dn);
							toRemove.AddItem(dn);
							DecommissionManager.Log.Debug("Node {} is sufficiently replicated and healthy, " 
								+ "marked as decommissioned.", dn);
						}
						else
						{
							if (DecommissionManager.Log.IsDebugEnabled())
							{
								StringBuilder b = new StringBuilder("Node {} ");
								if (isHealthy)
								{
									b.Append("is ");
								}
								else
								{
									b.Append("isn't ");
								}
								b.Append("healthy and still needs to replicate {} more blocks," + " decommissioning is still in progress."
									);
								DecommissionManager.Log.Debug(b.ToString(), dn, blocks.Count);
							}
						}
					}
					else
					{
						DecommissionManager.Log.Debug("Node {} still has {} blocks to replicate " + "before it is a candidate to finish decommissioning."
							, dn, blocks.Count);
					}
					this.iterkey = dn;
				}
				// Remove the datanodes that are decommissioned
				foreach (DatanodeDescriptor dn_1 in toRemove)
				{
					Preconditions.CheckState(dn_1.IsDecommissioned(), "Removing a node that is not yet decommissioned!"
						);
					Sharpen.Collections.Remove(this._enclosing.decomNodeBlocks, dn_1);
				}
			}

			/// <summary>
			/// Removes sufficiently replicated blocks from the block list of a
			/// datanode.
			/// </summary>
			private void PruneSufficientlyReplicated(DatanodeDescriptor datanode, AbstractList
				<BlockInfoContiguous> blocks)
			{
				this.ProcessBlocksForDecomInternal(datanode, blocks.GetEnumerator(), null, true);
			}

			/// <summary>
			/// Returns a list of blocks on a datanode that are insufficiently
			/// replicated, i.e.
			/// </summary>
			/// <remarks>
			/// Returns a list of blocks on a datanode that are insufficiently
			/// replicated, i.e. are under-replicated enough to prevent decommission.
			/// <p/>
			/// As part of this, it also schedules replication work for
			/// any under-replicated blocks.
			/// </remarks>
			/// <param name="datanode"/>
			/// <returns>List of insufficiently replicated blocks</returns>
			private AbstractList<BlockInfoContiguous> HandleInsufficientlyReplicated(DatanodeDescriptor
				 datanode)
			{
				AbstractList<BlockInfoContiguous> insufficient = new ChunkedArrayList<BlockInfoContiguous
					>();
				this.ProcessBlocksForDecomInternal(datanode, datanode.GetBlockIterator(), insufficient
					, false);
				return insufficient;
			}

			/// <summary>
			/// Used while checking if decommission-in-progress datanodes can be marked
			/// as decommissioned.
			/// </summary>
			/// <remarks>
			/// Used while checking if decommission-in-progress datanodes can be marked
			/// as decommissioned. Combines shared logic of
			/// pruneSufficientlyReplicated and handleInsufficientlyReplicated.
			/// </remarks>
			/// <param name="datanode">Datanode</param>
			/// <param name="it">
			/// Iterator over the blocks on the
			/// datanode
			/// </param>
			/// <param name="insufficientlyReplicated">
			/// Return parameter. If it's not null,
			/// will contain the insufficiently
			/// replicated-blocks from the list.
			/// </param>
			/// <param name="pruneSufficientlyReplicated">
			/// whether to remove sufficiently
			/// replicated blocks from the iterator
			/// </param>
			/// <returns>
			/// true if there are under-replicated blocks in the provided block
			/// iterator, else false.
			/// </returns>
			private void ProcessBlocksForDecomInternal(DatanodeDescriptor datanode, IEnumerator
				<BlockInfoContiguous> it, IList<BlockInfoContiguous> insufficientlyReplicated, bool
				 pruneSufficientlyReplicated)
			{
				bool firstReplicationLog = true;
				int underReplicatedBlocks = 0;
				int decommissionOnlyReplicas = 0;
				int underReplicatedInOpenFiles = 0;
				while (it.HasNext())
				{
					this.numBlocksChecked++;
					BlockInfoContiguous block = it.Next();
					// Remove the block from the list if it's no longer in the block map,
					// e.g. the containing file has been deleted
					if (this._enclosing.blockManager.blocksMap.GetStoredBlock(block) == null)
					{
						DecommissionManager.Log.Trace("Removing unknown block {}", block);
						it.Remove();
						continue;
					}
					BlockCollection bc = this._enclosing.blockManager.blocksMap.GetBlockCollection(block
						);
					if (bc == null)
					{
						// Orphan block, will be invalidated eventually. Skip.
						continue;
					}
					NumberReplicas num = this._enclosing.blockManager.CountNodes(block);
					int liveReplicas = num.LiveReplicas();
					int curReplicas = liveReplicas;
					// Schedule under-replicated blocks for replication if not already
					// pending
					if (this._enclosing.blockManager.IsNeededReplication(block, bc.GetBlockReplication
						(), liveReplicas))
					{
						if (!this._enclosing.blockManager.neededReplications.Contains(block) && this._enclosing
							.blockManager.pendingReplications.GetNumReplicas(block) == 0 && this._enclosing.
							namesystem.IsPopulatingReplQueues())
						{
							// Process these blocks only when active NN is out of safe mode.
							this._enclosing.blockManager.neededReplications.Add(block, curReplicas, num.DecommissionedReplicas
								(), bc.GetBlockReplication());
						}
					}
					// Even if the block is under-replicated, 
					// it doesn't block decommission if it's sufficiently replicated 
					if (this._enclosing.IsSufficientlyReplicated(block, bc, num))
					{
						if (pruneSufficientlyReplicated)
						{
							it.Remove();
						}
						continue;
					}
					// We've found an insufficiently replicated block.
					if (insufficientlyReplicated != null)
					{
						insufficientlyReplicated.AddItem(block);
					}
					// Log if this is our first time through
					if (firstReplicationLog)
					{
						DecommissionManager.LogBlockReplicationInfo(block, bc, datanode, num, this._enclosing
							.blockManager.blocksMap.GetStorages(block));
						firstReplicationLog = false;
					}
					// Update various counts
					underReplicatedBlocks++;
					if (bc.IsUnderConstruction())
					{
						underReplicatedInOpenFiles++;
					}
					if ((curReplicas == 0) && (num.DecommissionedReplicas() > 0))
					{
						decommissionOnlyReplicas++;
					}
				}
				datanode.decommissioningStatus.Set(underReplicatedBlocks, decommissionOnlyReplicas
					, underReplicatedInOpenFiles);
			}

			private readonly DecommissionManager _enclosing;
		}

		/// <exception cref="Sharpen.ExecutionException"/>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		internal virtual void RunMonitor()
		{
			Future f = executor.Submit(monitor);
			f.Get();
		}
	}
}
