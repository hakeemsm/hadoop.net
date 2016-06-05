using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Manage the heartbeats received from datanodes.</summary>
	/// <remarks>
	/// Manage the heartbeats received from datanodes.
	/// The datanode list and statistics are synchronized
	/// by the heartbeat manager lock.
	/// </remarks>
	internal class HeartbeatManager : DatanodeStatistics
	{
		internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.HeartbeatManager
			));

		/// <summary>
		/// Stores a subset of the datanodeMap in DatanodeManager,
		/// containing nodes that are considered alive.
		/// </summary>
		/// <remarks>
		/// Stores a subset of the datanodeMap in DatanodeManager,
		/// containing nodes that are considered alive.
		/// The HeartbeatMonitor periodically checks for out-dated entries,
		/// and removes them from the list.
		/// It is synchronized by the heartbeat manager lock.
		/// </remarks>
		private readonly IList<DatanodeDescriptor> datanodes = new AList<DatanodeDescriptor
			>();

		/// <summary>Statistics, which are synchronized by the heartbeat manager lock.</summary>
		private readonly HeartbeatManager.Stats stats = new HeartbeatManager.Stats();

		/// <summary>The time period to check for expired datanodes</summary>
		private readonly long heartbeatRecheckInterval;

		/// <summary>Heartbeat monitor thread</summary>
		private readonly Daemon heartbeatThread;

		internal readonly Namesystem namesystem;

		internal readonly BlockManager blockManager;

		internal HeartbeatManager(Namesystem namesystem, BlockManager blockManager, Configuration
			 conf)
		{
			heartbeatThread = new Daemon(new HeartbeatManager.Monitor(this));
			this.namesystem = namesystem;
			this.blockManager = blockManager;
			bool avoidStaleDataNodesForWrite = conf.GetBoolean(DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForWriteKey
				, DFSConfigKeys.DfsNamenodeAvoidStaleDatanodeForWriteDefault);
			long recheckInterval = conf.GetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey
				, DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalDefault);
			// 5 min
			long staleInterval = conf.GetLong(DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalKey
				, DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalDefault);
			// 30s
			if (avoidStaleDataNodesForWrite && staleInterval < recheckInterval)
			{
				this.heartbeatRecheckInterval = staleInterval;
				Log.Info("Setting heartbeat recheck interval to " + staleInterval + " since " + DFSConfigKeys
					.DfsNamenodeStaleDatanodeIntervalKey + " is less than " + DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey
					);
			}
			else
			{
				this.heartbeatRecheckInterval = recheckInterval;
			}
		}

		internal virtual void Activate(Configuration conf)
		{
			heartbeatThread.Start();
		}

		internal virtual void Close()
		{
			heartbeatThread.Interrupt();
			try
			{
				// This will no effect if the thread hasn't yet been started.
				heartbeatThread.Join(3000);
			}
			catch (Exception)
			{
			}
		}

		internal virtual int GetLiveDatanodeCount()
		{
			lock (this)
			{
				return datanodes.Count;
			}
		}

		public virtual long GetCapacityTotal()
		{
			lock (this)
			{
				return stats.capacityTotal;
			}
		}

		public virtual long GetCapacityUsed()
		{
			lock (this)
			{
				return stats.capacityUsed;
			}
		}

		public virtual float GetCapacityUsedPercent()
		{
			lock (this)
			{
				return DFSUtil.GetPercentUsed(stats.capacityUsed, stats.capacityTotal);
			}
		}

		public virtual long GetCapacityRemaining()
		{
			lock (this)
			{
				return stats.capacityRemaining;
			}
		}

		public virtual float GetCapacityRemainingPercent()
		{
			lock (this)
			{
				return DFSUtil.GetPercentRemaining(stats.capacityRemaining, stats.capacityTotal);
			}
		}

		public virtual long GetBlockPoolUsed()
		{
			lock (this)
			{
				return stats.blockPoolUsed;
			}
		}

		public virtual float GetPercentBlockPoolUsed()
		{
			lock (this)
			{
				return DFSUtil.GetPercentUsed(stats.blockPoolUsed, stats.capacityTotal);
			}
		}

		public virtual long GetCapacityUsedNonDFS()
		{
			lock (this)
			{
				long nonDFSUsed = stats.capacityTotal - stats.capacityRemaining - stats.capacityUsed;
				return nonDFSUsed < 0L ? 0L : nonDFSUsed;
			}
		}

		public virtual int GetXceiverCount()
		{
			lock (this)
			{
				return stats.xceiverCount;
			}
		}

		public virtual int GetInServiceXceiverCount()
		{
			lock (this)
			{
				return stats.nodesInServiceXceiverCount;
			}
		}

		public virtual int GetNumDatanodesInService()
		{
			lock (this)
			{
				return stats.nodesInService;
			}
		}

		public virtual long GetCacheCapacity()
		{
			lock (this)
			{
				return stats.cacheCapacity;
			}
		}

		public virtual long GetCacheUsed()
		{
			lock (this)
			{
				return stats.cacheUsed;
			}
		}

		public virtual long[] GetStats()
		{
			lock (this)
			{
				return new long[] { GetCapacityTotal(), GetCapacityUsed(), GetCapacityRemaining()
					, -1L, -1L, -1L, -1L };
			}
		}

		public virtual int GetExpiredHeartbeats()
		{
			lock (this)
			{
				return stats.expiredHeartbeats;
			}
		}

		internal virtual void Register(DatanodeDescriptor d)
		{
			lock (this)
			{
				if (!d.isAlive)
				{
					AddDatanode(d);
					//update its timestamp
					d.UpdateHeartbeatState(StorageReport.EmptyArray, 0L, 0L, 0, 0, null);
				}
			}
		}

		internal virtual DatanodeDescriptor[] GetDatanodes()
		{
			lock (this)
			{
				return Sharpen.Collections.ToArray(datanodes, new DatanodeDescriptor[datanodes.Count
					]);
			}
		}

		internal virtual void AddDatanode(DatanodeDescriptor d)
		{
			lock (this)
			{
				// update in-service node count
				stats.Add(d);
				datanodes.AddItem(d);
				d.isAlive = true;
			}
		}

		internal virtual void RemoveDatanode(DatanodeDescriptor node)
		{
			lock (this)
			{
				if (node.isAlive)
				{
					stats.Subtract(node);
					datanodes.Remove(node);
					node.isAlive = false;
				}
			}
		}

		internal virtual void UpdateHeartbeat(DatanodeDescriptor node, StorageReport[] reports
			, long cacheCapacity, long cacheUsed, int xceiverCount, int failedVolumes, VolumeFailureSummary
			 volumeFailureSummary)
		{
			lock (this)
			{
				stats.Subtract(node);
				node.UpdateHeartbeat(reports, cacheCapacity, cacheUsed, xceiverCount, failedVolumes
					, volumeFailureSummary);
				stats.Add(node);
			}
		}

		internal virtual void StartDecommission(DatanodeDescriptor node)
		{
			lock (this)
			{
				if (!node.isAlive)
				{
					Log.Info("Dead node {} is decommissioned immediately.", node);
					node.SetDecommissioned();
				}
				else
				{
					stats.Subtract(node);
					node.StartDecommission();
					stats.Add(node);
				}
			}
		}

		internal virtual void StopDecommission(DatanodeDescriptor node)
		{
			lock (this)
			{
				Log.Info("Stopping decommissioning of {} node {}", node.isAlive ? "live" : "dead"
					, node);
				if (!node.isAlive)
				{
					node.StopDecommission();
				}
				else
				{
					stats.Subtract(node);
					node.StopDecommission();
					stats.Add(node);
				}
			}
		}

		/// <summary>
		/// Check if there are any expired heartbeats, and if so,
		/// whether any blocks have to be re-replicated.
		/// </summary>
		/// <remarks>
		/// Check if there are any expired heartbeats, and if so,
		/// whether any blocks have to be re-replicated.
		/// While removing dead datanodes, make sure that only one datanode is marked
		/// dead at a time within the synchronized section. Otherwise, a cascading
		/// effect causes more datanodes to be declared dead.
		/// Check if there are any failed storage and if so,
		/// Remove all the blocks on the storage. It also covers the following less
		/// common scenarios. After DatanodeStorage is marked FAILED, it is still
		/// possible to receive IBR for this storage.
		/// 1) DN could deliver IBR for failed storage due to its implementation.
		/// a) DN queues a pending IBR request.
		/// b) The storage of the block fails.
		/// c) DN first sends HB, NN will mark the storage FAILED.
		/// d) DN then sends the pending IBR request.
		/// 2) SBN processes block request from pendingDNMessages.
		/// It is possible to have messages in pendingDNMessages that refer
		/// to some failed storage.
		/// a) SBN receives a IBR and put it in pendingDNMessages.
		/// b) The storage of the block fails.
		/// c) Edit log replay get the IBR from pendingDNMessages.
		/// Alternatively, we can resolve these scenarios with the following approaches.
		/// A. Make sure DN don't deliver IBR for failed storage.
		/// B. Remove all blocks in PendingDataNodeMessages for the failed storage
		/// when we remove all blocks from BlocksMap for that storage.
		/// </remarks>
		internal virtual void HeartbeatCheck()
		{
			DatanodeManager dm = blockManager.GetDatanodeManager();
			// It's OK to check safe mode w/o taking the lock here, we re-check
			// for safe mode after taking the lock before removing a datanode.
			if (namesystem.IsInStartupSafeMode())
			{
				return;
			}
			bool allAlive = false;
			while (!allAlive)
			{
				// locate the first dead node.
				DatanodeID dead = null;
				// locate the first failed storage that isn't on a dead node.
				DatanodeStorageInfo failedStorage = null;
				// check the number of stale nodes
				int numOfStaleNodes = 0;
				int numOfStaleStorages = 0;
				lock (this)
				{
					foreach (DatanodeDescriptor d in datanodes)
					{
						if (dead == null && dm.IsDatanodeDead(d))
						{
							stats.IncrExpiredHeartbeats();
							dead = d;
						}
						if (d.IsStale(dm.GetStaleInterval()))
						{
							numOfStaleNodes++;
						}
						DatanodeStorageInfo[] storageInfos = d.GetStorageInfos();
						foreach (DatanodeStorageInfo storageInfo in storageInfos)
						{
							if (storageInfo.AreBlockContentsStale())
							{
								numOfStaleStorages++;
							}
							if (failedStorage == null && storageInfo.AreBlocksOnFailedStorage() && d != dead)
							{
								failedStorage = storageInfo;
							}
						}
					}
					// Set the number of stale nodes in the DatanodeManager
					dm.SetNumStaleNodes(numOfStaleNodes);
					dm.SetNumStaleStorages(numOfStaleStorages);
				}
				allAlive = dead == null && failedStorage == null;
				if (dead != null)
				{
					// acquire the fsnamesystem lock, and then remove the dead node.
					namesystem.WriteLock();
					try
					{
						if (namesystem.IsInStartupSafeMode())
						{
							return;
						}
						lock (this)
						{
							dm.RemoveDeadDatanode(dead);
						}
					}
					finally
					{
						namesystem.WriteUnlock();
					}
				}
				if (failedStorage != null)
				{
					// acquire the fsnamesystem lock, and remove blocks on the storage.
					namesystem.WriteLock();
					try
					{
						if (namesystem.IsInStartupSafeMode())
						{
							return;
						}
						lock (this)
						{
							blockManager.RemoveBlocksAssociatedTo(failedStorage);
						}
					}
					finally
					{
						namesystem.WriteUnlock();
					}
				}
			}
		}

		/// <summary>Periodically check heartbeat and update block key</summary>
		private class Monitor : Runnable
		{
			private long lastHeartbeatCheck;

			private long lastBlockKeyUpdate;

			public virtual void Run()
			{
				while (this._enclosing.namesystem.IsRunning())
				{
					try
					{
						long now = Time.MonotonicNow();
						if (this.lastHeartbeatCheck + this._enclosing.heartbeatRecheckInterval < now)
						{
							this._enclosing.HeartbeatCheck();
							this.lastHeartbeatCheck = now;
						}
						if (this._enclosing.blockManager.ShouldUpdateBlockKey(now - this.lastBlockKeyUpdate
							))
						{
							lock (this._enclosing)
							{
								foreach (DatanodeDescriptor d in this._enclosing.datanodes)
								{
									d.needKeyUpdate = true;
								}
							}
							this.lastBlockKeyUpdate = now;
						}
					}
					catch (Exception e)
					{
						HeartbeatManager.Log.Error("Exception while checking heartbeat", e);
					}
					try
					{
						Sharpen.Thread.Sleep(5000);
					}
					catch (Exception)
					{
					}
				}
			}

			internal Monitor(HeartbeatManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly HeartbeatManager _enclosing;
			// 5 seconds
		}

		/// <summary>Datanode statistics.</summary>
		/// <remarks>
		/// Datanode statistics.
		/// For decommissioning/decommissioned nodes, only used capacity is counted.
		/// </remarks>
		private class Stats
		{
			private long capacityTotal = 0L;

			private long capacityUsed = 0L;

			private long capacityRemaining = 0L;

			private long blockPoolUsed = 0L;

			private int xceiverCount = 0;

			private long cacheCapacity = 0L;

			private long cacheUsed = 0L;

			private int nodesInService = 0;

			private int nodesInServiceXceiverCount = 0;

			private int expiredHeartbeats = 0;

			private void Add(DatanodeDescriptor node)
			{
				capacityUsed += node.GetDfsUsed();
				blockPoolUsed += node.GetBlockPoolUsed();
				xceiverCount += node.GetXceiverCount();
				if (!(node.IsDecommissionInProgress() || node.IsDecommissioned()))
				{
					nodesInService++;
					nodesInServiceXceiverCount += node.GetXceiverCount();
					capacityTotal += node.GetCapacity();
					capacityRemaining += node.GetRemaining();
				}
				else
				{
					capacityTotal += node.GetDfsUsed();
				}
				cacheCapacity += node.GetCacheCapacity();
				cacheUsed += node.GetCacheUsed();
			}

			private void Subtract(DatanodeDescriptor node)
			{
				capacityUsed -= node.GetDfsUsed();
				blockPoolUsed -= node.GetBlockPoolUsed();
				xceiverCount -= node.GetXceiverCount();
				if (!(node.IsDecommissionInProgress() || node.IsDecommissioned()))
				{
					nodesInService--;
					nodesInServiceXceiverCount -= node.GetXceiverCount();
					capacityTotal -= node.GetCapacity();
					capacityRemaining -= node.GetRemaining();
				}
				else
				{
					capacityTotal -= node.GetDfsUsed();
				}
				cacheCapacity -= node.GetCacheCapacity();
				cacheUsed -= node.GetCacheUsed();
			}

			/// <summary>Increment expired heartbeat counter.</summary>
			private void IncrExpiredHeartbeats()
			{
				expiredHeartbeats++;
			}
		}
	}
}
