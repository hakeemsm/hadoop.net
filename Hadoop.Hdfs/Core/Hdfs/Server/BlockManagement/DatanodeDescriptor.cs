using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// This class extends the DatanodeInfo class with ephemeral information (eg
	/// health, capacity, what blocks are associated with the Datanode) that is
	/// private to the Namenode, ie this class is not exposed to clients.
	/// </summary>
	public class DatanodeDescriptor : DatanodeInfo
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeDescriptor
			));

		public static readonly Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeDescriptor
			[] EmptyArray = new Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.DatanodeDescriptor
			[] {  };

		public readonly DatanodeDescriptor.DecommissioningStatus decommissioningStatus;

		private long curBlockReportId = 0;

		private BitSet curBlockReportRpcsSeen = null;

		// Stores status of decommissioning.
		// If node is not decommissioning, do not use this object for anything.
		public virtual int UpdateBlockReportContext(BlockReportContext context)
		{
			if (curBlockReportId != context.GetReportId())
			{
				curBlockReportId = context.GetReportId();
				curBlockReportRpcsSeen = new BitSet(context.GetTotalRpcs());
			}
			curBlockReportRpcsSeen.Set(context.GetCurRpc());
			return curBlockReportRpcsSeen.Cardinality();
		}

		public virtual void ClearBlockReportContext()
		{
			curBlockReportId = 0;
			curBlockReportRpcsSeen = null;
		}

		/// <summary>Block and targets pair</summary>
		public class BlockTargetPair
		{
			public readonly Block block;

			public readonly DatanodeStorageInfo[] targets;

			internal BlockTargetPair(Block block, DatanodeStorageInfo[] targets)
			{
				this.block = block;
				this.targets = targets;
			}
		}

		/// <summary>A BlockTargetPair queue.</summary>
		private class BlockQueue<E>
		{
			private readonly Queue<E> blockq = new List<E>();

			/// <summary>Size of the queue</summary>
			internal virtual int Size()
			{
				lock (this)
				{
					return blockq.Count;
				}
			}

			/// <summary>Enqueue</summary>
			internal virtual bool Offer(E e)
			{
				lock (this)
				{
					return blockq.Offer(e);
				}
			}

			/// <summary>Dequeue</summary>
			internal virtual IList<E> Poll(int numBlocks)
			{
				lock (this)
				{
					if (numBlocks <= 0 || blockq.IsEmpty())
					{
						return null;
					}
					IList<E> results = new AList<E>();
					for (; !blockq.IsEmpty() && numBlocks > 0; numBlocks--)
					{
						results.AddItem(blockq.Poll());
					}
					return results;
				}
			}

			/// <summary>Returns <tt>true</tt> if the queue contains the specified element.</summary>
			internal virtual bool Contains(E e)
			{
				return blockq.Contains(e);
			}

			internal virtual void Clear()
			{
				lock (this)
				{
					blockq.Clear();
				}
			}
		}

		private readonly IDictionary<string, DatanodeStorageInfo> storageMap = new Dictionary
			<string, DatanodeStorageInfo>();

		/// <summary>A list of CachedBlock objects on this datanode.</summary>
		public class CachedBlocksList : IntrusiveCollection<CachedBlock>
		{
			public enum Type
			{
				PendingCached,
				Cached,
				PendingUncached
			}

			private readonly DatanodeDescriptor datanode;

			private readonly DatanodeDescriptor.CachedBlocksList.Type type;

			internal CachedBlocksList(DatanodeDescriptor datanode, DatanodeDescriptor.CachedBlocksList.Type
				 type)
			{
				this.datanode = datanode;
				this.type = type;
			}

			public virtual DatanodeDescriptor GetDatanode()
			{
				return datanode;
			}

			public virtual DatanodeDescriptor.CachedBlocksList.Type GetType()
			{
				return type;
			}
		}

		/// <summary>The blocks which we want to cache on this DataNode.</summary>
		private readonly DatanodeDescriptor.CachedBlocksList pendingCached;

		/// <summary>The blocks which we know are cached on this datanode.</summary>
		/// <remarks>
		/// The blocks which we know are cached on this datanode.
		/// This list is updated by periodic cache reports.
		/// </remarks>
		private readonly DatanodeDescriptor.CachedBlocksList cached;

		/// <summary>The blocks which we want to uncache on this DataNode.</summary>
		private readonly DatanodeDescriptor.CachedBlocksList pendingUncached;

		public virtual DatanodeDescriptor.CachedBlocksList GetPendingCached()
		{
			return pendingCached;
		}

		public virtual DatanodeDescriptor.CachedBlocksList GetCached()
		{
			return cached;
		}

		public virtual DatanodeDescriptor.CachedBlocksList GetPendingUncached()
		{
			return pendingUncached;
		}

		/// <summary>
		/// The time when the last batch of caching directives was sent, in
		/// monotonic milliseconds.
		/// </summary>
		private long lastCachingDirectiveSentTimeMs;

		public bool isAlive = false;

		public bool needKeyUpdate = false;

		private long bandwidth;

		/// <summary>A queue of blocks to be replicated by this datanode</summary>
		private readonly DatanodeDescriptor.BlockQueue<DatanodeDescriptor.BlockTargetPair
			> replicateBlocks = new DatanodeDescriptor.BlockQueue<DatanodeDescriptor.BlockTargetPair
			>();

		/// <summary>A queue of blocks to be recovered by this datanode</summary>
		private readonly DatanodeDescriptor.BlockQueue<BlockInfoContiguousUnderConstruction
			> recoverBlocks = new DatanodeDescriptor.BlockQueue<BlockInfoContiguousUnderConstruction
			>();

		/// <summary>A set of blocks to be invalidated by this datanode</summary>
		private readonly LightWeightHashSet<Block> invalidateBlocks = new LightWeightHashSet
			<Block>();

		private EnumCounters<StorageType> currApproxBlocksScheduled = new EnumCounters<StorageType
			>(typeof(StorageType));

		private EnumCounters<StorageType> prevApproxBlocksScheduled = new EnumCounters<StorageType
			>(typeof(StorageType));

		private long lastBlocksScheduledRollTime = 0;

		private const int BlocksScheduledRollInterval = 600 * 1000;

		private int volumeFailures = 0;

		private VolumeFailureSummary volumeFailureSummary = null;

		/// <summary>
		/// When set to true, the node is not in include list and is not allowed
		/// to communicate with the namenode
		/// </summary>
		private bool disallowed = false;

		private int PendingReplicationWithoutTargets = 0;

		private bool heartbeatedSinceRegistration = false;

		/// <summary>DatanodeDescriptor constructor</summary>
		/// <param name="nodeID">id of the data node</param>
		public DatanodeDescriptor(DatanodeID nodeID)
			: base(nodeID)
		{
			decommissioningStatus = new DatanodeDescriptor.DecommissioningStatus(this);
			pendingCached = new DatanodeDescriptor.CachedBlocksList(this, DatanodeDescriptor.CachedBlocksList.Type
				.PendingCached);
			cached = new DatanodeDescriptor.CachedBlocksList(this, DatanodeDescriptor.CachedBlocksList.Type
				.Cached);
			pendingUncached = new DatanodeDescriptor.CachedBlocksList(this, DatanodeDescriptor.CachedBlocksList.Type
				.PendingUncached);
			// isAlive == heartbeats.contains(this)
			// This is an optimization, because contains takes O(n) time on Arraylist
			// A system administrator can tune the balancer bandwidth parameter
			// (dfs.balance.bandwidthPerSec) dynamically by calling
			// "dfsadmin -setBalanacerBandwidth <newbandwidth>", at which point the
			// following 'bandwidth' variable gets updated with the new value for each
			// node. Once the heartbeat command is issued to update the value on the
			// specified datanode, this value will be set back to 0.
			/* Variables for maintaining number of blocks scheduled to be written to
			* this storage. This count is approximate and might be slightly bigger
			* in case of errors (e.g. datanode does not report if an error occurs
			* while writing the block).
			*/
			//10min
			// The number of replication work pending before targets are determined
			// HB processing can use it to tell if it is the first HB since DN restarted
			UpdateHeartbeatState(StorageReport.EmptyArray, 0L, 0L, 0, 0, null);
		}

		/// <summary>DatanodeDescriptor constructor</summary>
		/// <param name="nodeID">id of the data node</param>
		/// <param name="networkLocation">location of the data node in network</param>
		public DatanodeDescriptor(DatanodeID nodeID, string networkLocation)
			: base(nodeID, networkLocation)
		{
			decommissioningStatus = new DatanodeDescriptor.DecommissioningStatus(this);
			pendingCached = new DatanodeDescriptor.CachedBlocksList(this, DatanodeDescriptor.CachedBlocksList.Type
				.PendingCached);
			cached = new DatanodeDescriptor.CachedBlocksList(this, DatanodeDescriptor.CachedBlocksList.Type
				.Cached);
			pendingUncached = new DatanodeDescriptor.CachedBlocksList(this, DatanodeDescriptor.CachedBlocksList.Type
				.PendingUncached);
			UpdateHeartbeatState(StorageReport.EmptyArray, 0L, 0L, 0, 0, null);
		}

		[VisibleForTesting]
		public virtual DatanodeStorageInfo GetStorageInfo(string storageID)
		{
			lock (storageMap)
			{
				return storageMap[storageID];
			}
		}

		internal virtual DatanodeStorageInfo[] GetStorageInfos()
		{
			lock (storageMap)
			{
				ICollection<DatanodeStorageInfo> storages = storageMap.Values;
				return Sharpen.Collections.ToArray(storages, new DatanodeStorageInfo[storages.Count
					]);
			}
		}

		public virtual StorageReport[] GetStorageReports()
		{
			DatanodeStorageInfo[] infos = GetStorageInfos();
			StorageReport[] reports = new StorageReport[infos.Length];
			for (int i = 0; i < infos.Length; i++)
			{
				reports[i] = infos[i].ToStorageReport();
			}
			return reports;
		}

		internal virtual bool HasStaleStorages()
		{
			lock (storageMap)
			{
				foreach (DatanodeStorageInfo storage in storageMap.Values)
				{
					if (storage.AreBlockContentsStale())
					{
						return true;
					}
				}
				return false;
			}
		}

		private static readonly IList<DatanodeStorageInfo> EmptyStorageInfoList = ImmutableList
			.Of();

		internal virtual IList<DatanodeStorageInfo> RemoveZombieStorages()
		{
			IList<DatanodeStorageInfo> zombies = null;
			lock (storageMap)
			{
				IEnumerator<KeyValuePair<string, DatanodeStorageInfo>> iter = storageMap.GetEnumerator
					();
				while (iter.HasNext())
				{
					KeyValuePair<string, DatanodeStorageInfo> entry = iter.Next();
					DatanodeStorageInfo storageInfo = entry.Value;
					if (storageInfo.GetLastBlockReportId() != curBlockReportId)
					{
						Log.Info(storageInfo.GetStorageID() + " had lastBlockReportId 0x" + long.ToHexString
							(storageInfo.GetLastBlockReportId()) + ", but curBlockReportId = 0x" + long.ToHexString
							(curBlockReportId));
						iter.Remove();
						if (zombies == null)
						{
							zombies = new List<DatanodeStorageInfo>();
						}
						zombies.AddItem(storageInfo);
					}
					storageInfo.SetLastBlockReportId(0);
				}
			}
			return zombies == null ? EmptyStorageInfoList : zombies;
		}

		/// <summary>Remove block from the list of blocks belonging to the data-node.</summary>
		/// <remarks>
		/// Remove block from the list of blocks belonging to the data-node. Remove
		/// data-node from the block.
		/// </remarks>
		internal virtual bool RemoveBlock(BlockInfoContiguous b)
		{
			DatanodeStorageInfo s = b.FindStorageInfo(this);
			// if block exists on this datanode
			if (s != null)
			{
				return s.RemoveBlock(b);
			}
			return false;
		}

		/// <summary>Remove block from the list of blocks belonging to the data-node.</summary>
		/// <remarks>
		/// Remove block from the list of blocks belonging to the data-node. Remove
		/// data-node from the block.
		/// </remarks>
		internal virtual bool RemoveBlock(string storageID, BlockInfoContiguous b)
		{
			DatanodeStorageInfo s = GetStorageInfo(storageID);
			if (s != null)
			{
				return s.RemoveBlock(b);
			}
			return false;
		}

		public virtual void ResetBlocks()
		{
			SetCapacity(0);
			SetRemaining(0);
			SetBlockPoolUsed(0);
			SetDfsUsed(0);
			SetXceiverCount(0);
			this.invalidateBlocks.Clear();
			this.volumeFailures = 0;
			// pendingCached, cached, and pendingUncached are protected by the
			// FSN lock.
			this.pendingCached.Clear();
			this.cached.Clear();
			this.pendingUncached.Clear();
		}

		public virtual void ClearBlockQueues()
		{
			lock (invalidateBlocks)
			{
				this.invalidateBlocks.Clear();
				this.recoverBlocks.Clear();
				this.replicateBlocks.Clear();
			}
			// pendingCached, cached, and pendingUncached are protected by the
			// FSN lock.
			this.pendingCached.Clear();
			this.cached.Clear();
			this.pendingUncached.Clear();
		}

		public virtual int NumBlocks()
		{
			int blocks = 0;
			foreach (DatanodeStorageInfo entry in GetStorageInfos())
			{
				blocks += entry.NumBlocks();
			}
			return blocks;
		}

		/// <summary>Updates stats from datanode heartbeat.</summary>
		public virtual void UpdateHeartbeat(StorageReport[] reports, long cacheCapacity, 
			long cacheUsed, int xceiverCount, int volFailures, VolumeFailureSummary volumeFailureSummary
			)
		{
			UpdateHeartbeatState(reports, cacheCapacity, cacheUsed, xceiverCount, volFailures
				, volumeFailureSummary);
			heartbeatedSinceRegistration = true;
		}

		/// <summary>process datanode heartbeat or stats initialization.</summary>
		public virtual void UpdateHeartbeatState(StorageReport[] reports, long cacheCapacity
			, long cacheUsed, int xceiverCount, int volFailures, VolumeFailureSummary volumeFailureSummary
			)
		{
			long totalCapacity = 0;
			long totalRemaining = 0;
			long totalBlockPoolUsed = 0;
			long totalDfsUsed = 0;
			ICollection<DatanodeStorageInfo> failedStorageInfos = null;
			// Decide if we should check for any missing StorageReport and mark it as
			// failed. There are different scenarios.
			// 1. When DN is running, a storage failed. Given the current DN
			//    implementation doesn't add recovered storage back to its storage list
			//    until DN restart, we can assume volFailures won't decrease
			//    during the current DN registration session.
			//    When volumeFailures == this.volumeFailures, it implies there is no
			//    state change. No need to check for failed storage. This is an
			//    optimization.  Recent versions of the DataNode report a
			//    VolumeFailureSummary containing the date/time of the last volume
			//    failure.  If that's available, then we check that instead for greater
			//    accuracy.
			// 2. After DN restarts, volFailures might not increase and it is possible
			//    we still have new failed storage. For example, admins reduce
			//    available storages in configuration. Another corner case
			//    is the failed volumes might change after restart; a) there
			//    is one good storage A, one restored good storage B, so there is
			//    one element in storageReports and that is A. b) A failed. c) Before
			//    DN sends HB to NN to indicate A has failed, DN restarts. d) After DN
			//    restarts, storageReports has one element which is B.
			bool checkFailedStorages;
			if (volumeFailureSummary != null && this.volumeFailureSummary != null)
			{
				checkFailedStorages = volumeFailureSummary.GetLastVolumeFailureDate() > this.volumeFailureSummary
					.GetLastVolumeFailureDate();
			}
			else
			{
				checkFailedStorages = (volFailures > this.volumeFailures) || !heartbeatedSinceRegistration;
			}
			if (checkFailedStorages)
			{
				Log.Info("Number of failed storage changes from " + this.volumeFailures + " to " 
					+ volFailures);
				failedStorageInfos = new HashSet<DatanodeStorageInfo>(storageMap.Values);
			}
			SetCacheCapacity(cacheCapacity);
			SetCacheUsed(cacheUsed);
			SetXceiverCount(xceiverCount);
			SetLastUpdate(Time.Now());
			SetLastUpdateMonotonic(Time.MonotonicNow());
			this.volumeFailures = volFailures;
			this.volumeFailureSummary = volumeFailureSummary;
			foreach (StorageReport report in reports)
			{
				DatanodeStorageInfo storage = UpdateStorage(report.GetStorage());
				if (checkFailedStorages)
				{
					failedStorageInfos.Remove(storage);
				}
				storage.ReceivedHeartbeat(report);
				totalCapacity += report.GetCapacity();
				totalRemaining += report.GetRemaining();
				totalBlockPoolUsed += report.GetBlockPoolUsed();
				totalDfsUsed += report.GetDfsUsed();
			}
			RollBlocksScheduled(GetLastUpdateMonotonic());
			// Update total metrics for the node.
			SetCapacity(totalCapacity);
			SetRemaining(totalRemaining);
			SetBlockPoolUsed(totalBlockPoolUsed);
			SetDfsUsed(totalDfsUsed);
			if (checkFailedStorages)
			{
				UpdateFailedStorage(failedStorageInfos);
			}
			if (storageMap.Count != reports.Length)
			{
				PruneStorageMap(reports);
			}
		}

		/// <summary>Remove stale storages from storageMap.</summary>
		/// <remarks>
		/// Remove stale storages from storageMap. We must not remove any storages
		/// as long as they have associated block replicas.
		/// </remarks>
		private void PruneStorageMap(StorageReport[] reports)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Number of storages reported in heartbeat=" + reports.Length + "; Number of storages in storageMap="
					 + storageMap.Count);
			}
			Dictionary<string, DatanodeStorageInfo> excessStorages;
			lock (storageMap)
			{
				// Init excessStorages with all known storages.
				excessStorages = new Dictionary<string, DatanodeStorageInfo>(storageMap);
				// Remove storages that the DN reported in the heartbeat.
				foreach (StorageReport report in reports)
				{
					Sharpen.Collections.Remove(excessStorages, report.GetStorage().GetStorageID());
				}
				// For each remaining storage, remove it if there are no associated
				// blocks.
				foreach (DatanodeStorageInfo storageInfo in excessStorages.Values)
				{
					if (storageInfo.NumBlocks() == 0)
					{
						Sharpen.Collections.Remove(storageMap, storageInfo.GetStorageID());
						Log.Info("Removed storage " + storageInfo + " from DataNode" + this);
					}
					else
					{
						if (Log.IsDebugEnabled())
						{
							// This can occur until all block reports are received.
							Log.Debug("Deferring removal of stale storage " + storageInfo + " with " + storageInfo
								.NumBlocks() + " blocks");
						}
					}
				}
			}
		}

		private void UpdateFailedStorage(ICollection<DatanodeStorageInfo> failedStorageInfos
			)
		{
			foreach (DatanodeStorageInfo storageInfo in failedStorageInfos)
			{
				if (storageInfo.GetState() != DatanodeStorage.State.Failed)
				{
					Log.Info(storageInfo + " failed.");
					storageInfo.SetState(DatanodeStorage.State.Failed);
				}
			}
		}

		private class BlockIterator : IEnumerator<BlockInfoContiguous>
		{
			private int index = 0;

			private readonly IList<IEnumerator<BlockInfoContiguous>> iterators;

			private BlockIterator(params DatanodeStorageInfo[] storages)
			{
				IList<IEnumerator<BlockInfoContiguous>> iterators = new AList<IEnumerator<BlockInfoContiguous
					>>();
				foreach (DatanodeStorageInfo e in storages)
				{
					iterators.AddItem(e.GetBlockIterator());
				}
				this.iterators = Sharpen.Collections.UnmodifiableList(iterators);
			}

			public override bool HasNext()
			{
				Update();
				return !iterators.IsEmpty() && iterators[index].HasNext();
			}

			public override BlockInfoContiguous Next()
			{
				Update();
				return iterators[index].Next();
			}

			public override void Remove()
			{
				throw new NotSupportedException("Remove unsupported.");
			}

			private void Update()
			{
				while (index < iterators.Count - 1 && !iterators[index].HasNext())
				{
					index++;
				}
			}
		}

		internal virtual IEnumerator<BlockInfoContiguous> GetBlockIterator()
		{
			return new DatanodeDescriptor.BlockIterator(GetStorageInfos());
		}

		internal virtual IEnumerator<BlockInfoContiguous> GetBlockIterator(string storageID
			)
		{
			return new DatanodeDescriptor.BlockIterator(GetStorageInfo(storageID));
		}

		internal virtual void IncrementPendingReplicationWithoutTargets()
		{
			PendingReplicationWithoutTargets++;
		}

		internal virtual void DecrementPendingReplicationWithoutTargets()
		{
			PendingReplicationWithoutTargets--;
		}

		/// <summary>Store block replication work.</summary>
		internal virtual void AddBlockToBeReplicated(Block block, DatanodeStorageInfo[] targets
			)
		{
			System.Diagnostics.Debug.Assert((block != null && targets != null && targets.Length
				 > 0));
			replicateBlocks.Offer(new DatanodeDescriptor.BlockTargetPair(block, targets));
		}

		/// <summary>Store block recovery work.</summary>
		internal virtual void AddBlockToBeRecovered(BlockInfoContiguousUnderConstruction 
			block)
		{
			if (recoverBlocks.Contains(block))
			{
				// this prevents adding the same block twice to the recovery queue
				BlockManager.Log.Info(block + " is already in the recovery queue");
				return;
			}
			recoverBlocks.Offer(block);
		}

		/// <summary>Store block invalidation work.</summary>
		internal virtual void AddBlocksToBeInvalidated(IList<Block> blocklist)
		{
			System.Diagnostics.Debug.Assert((blocklist != null && blocklist.Count > 0));
			lock (invalidateBlocks)
			{
				foreach (Block blk in blocklist)
				{
					invalidateBlocks.AddItem(blk);
				}
			}
		}

		/// <summary>The number of work items that are pending to be replicated</summary>
		internal virtual int GetNumberOfBlocksToBeReplicated()
		{
			return PendingReplicationWithoutTargets + replicateBlocks.Size();
		}

		/// <summary>
		/// The number of block invalidation items that are pending to
		/// be sent to the datanode
		/// </summary>
		internal virtual int GetNumberOfBlocksToBeInvalidated()
		{
			lock (invalidateBlocks)
			{
				return invalidateBlocks.Count;
			}
		}

		public virtual IList<DatanodeDescriptor.BlockTargetPair> GetReplicationCommand(int
			 maxTransfers)
		{
			return replicateBlocks.Poll(maxTransfers);
		}

		public virtual BlockInfoContiguousUnderConstruction[] GetLeaseRecoveryCommand(int
			 maxTransfers)
		{
			IList<BlockInfoContiguousUnderConstruction> blocks = recoverBlocks.Poll(maxTransfers
				);
			if (blocks == null)
			{
				return null;
			}
			return Sharpen.Collections.ToArray(blocks, new BlockInfoContiguousUnderConstruction
				[blocks.Count]);
		}

		/// <summary>Remove the specified number of blocks to be invalidated</summary>
		public virtual Block[] GetInvalidateBlocks(int maxblocks)
		{
			lock (invalidateBlocks)
			{
				Block[] deleteList = invalidateBlocks.PollToArray(new Block[Math.Min(invalidateBlocks
					.Count, maxblocks)]);
				return deleteList.Length == 0 ? null : deleteList;
			}
		}

		/// <summary>Return the sum of remaining spaces of the specified type.</summary>
		/// <remarks>
		/// Return the sum of remaining spaces of the specified type. If the remaining
		/// space of a storage is less than minSize, it won't be counted toward the
		/// sum.
		/// </remarks>
		/// <param name="t">The storage type. If null, the type is ignored.</param>
		/// <param name="minSize">The minimum free space required.</param>
		/// <returns>the sum of remaining spaces that are bigger than minSize.</returns>
		public virtual long GetRemaining(StorageType t, long minSize)
		{
			long remaining = 0;
			foreach (DatanodeStorageInfo s in GetStorageInfos())
			{
				if (s.GetState() == DatanodeStorage.State.Normal && (t == null || s.GetStorageType
					() == t))
				{
					long r = s.GetRemaining();
					if (r >= minSize)
					{
						remaining += r;
					}
				}
			}
			return remaining;
		}

		/// <returns>
		/// Approximate number of blocks currently scheduled to be written
		/// to the given storage type of this datanode.
		/// </returns>
		public virtual int GetBlocksScheduled(StorageType t)
		{
			return (int)(currApproxBlocksScheduled.Get(t) + prevApproxBlocksScheduled.Get(t));
		}

		/// <returns>
		/// Approximate number of blocks currently scheduled to be written
		/// to this datanode.
		/// </returns>
		public virtual int GetBlocksScheduled()
		{
			return (int)(currApproxBlocksScheduled.Sum() + prevApproxBlocksScheduled.Sum());
		}

		/// <summary>Increment the number of blocks scheduled.</summary>
		internal virtual void IncrementBlocksScheduled(StorageType t)
		{
			currApproxBlocksScheduled.Add(t, 1);
		}

		/// <summary>Decrement the number of blocks scheduled.</summary>
		internal virtual void DecrementBlocksScheduled(StorageType t)
		{
			if (prevApproxBlocksScheduled.Get(t) > 0)
			{
				prevApproxBlocksScheduled.Subtract(t, 1);
			}
			else
			{
				if (currApproxBlocksScheduled.Get(t) > 0)
				{
					currApproxBlocksScheduled.Subtract(t, 1);
				}
			}
		}

		// its ok if both counters are zero.
		/// <summary>Adjusts curr and prev number of blocks scheduled every few minutes.</summary>
		private void RollBlocksScheduled(long now)
		{
			if (now - lastBlocksScheduledRollTime > BlocksScheduledRollInterval)
			{
				prevApproxBlocksScheduled.Set(currApproxBlocksScheduled);
				currApproxBlocksScheduled.Reset();
				lastBlocksScheduledRollTime = now;
			}
		}

		public override int GetHashCode()
		{
			// Super implementation is sufficient
			return base.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			// Sufficient to use super equality as datanodes are uniquely identified
			// by DatanodeID
			return (this == obj) || base.Equals(obj);
		}

		/// <summary>Decommissioning status</summary>
		public class DecommissioningStatus
		{
			private int underReplicatedBlocks;

			private int decommissionOnlyReplicas;

			private int underReplicatedInOpenFiles;

			private long startTime;

			internal virtual void Set(int underRep, int onlyRep, int underConstruction)
			{
				lock (this)
				{
					if (this._enclosing.IsDecommissionInProgress() == false)
					{
						return;
					}
					this.underReplicatedBlocks = underRep;
					this.decommissionOnlyReplicas = onlyRep;
					this.underReplicatedInOpenFiles = underConstruction;
				}
			}

			/// <returns>the number of under-replicated blocks</returns>
			public virtual int GetUnderReplicatedBlocks()
			{
				lock (this)
				{
					if (this._enclosing.IsDecommissionInProgress() == false)
					{
						return 0;
					}
					return this.underReplicatedBlocks;
				}
			}

			/// <returns>the number of decommission-only replicas</returns>
			public virtual int GetDecommissionOnlyReplicas()
			{
				lock (this)
				{
					if (this._enclosing.IsDecommissionInProgress() == false)
					{
						return 0;
					}
					return this.decommissionOnlyReplicas;
				}
			}

			/// <returns>the number of under-replicated blocks in open files</returns>
			public virtual int GetUnderReplicatedInOpenFiles()
			{
				lock (this)
				{
					if (this._enclosing.IsDecommissionInProgress() == false)
					{
						return 0;
					}
					return this.underReplicatedInOpenFiles;
				}
			}

			/// <summary>Set start time</summary>
			public virtual void SetStartTime(long time)
			{
				lock (this)
				{
					this.startTime = time;
				}
			}

			/// <returns>start time</returns>
			public virtual long GetStartTime()
			{
				lock (this)
				{
					if (this._enclosing.IsDecommissionInProgress() == false)
					{
						return 0;
					}
					return this.startTime;
				}
			}

			internal DecommissioningStatus(DatanodeDescriptor _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DatanodeDescriptor _enclosing;
		}

		// End of class DecommissioningStatus
		/// <summary>
		/// Set the flag to indicate if this datanode is disallowed from communicating
		/// with the namenode.
		/// </summary>
		public virtual void SetDisallowed(bool flag)
		{
			disallowed = flag;
		}

		/// <summary>Is the datanode disallowed from communicating with the namenode?</summary>
		public virtual bool IsDisallowed()
		{
			return disallowed;
		}

		/// <returns>number of failed volumes in the datanode.</returns>
		public virtual int GetVolumeFailures()
		{
			return volumeFailures;
		}

		/// <summary>Returns info about volume failures.</summary>
		/// <returns>info about volume failures, possibly null</returns>
		public virtual VolumeFailureSummary GetVolumeFailureSummary()
		{
			return volumeFailureSummary;
		}

		/// <param name="nodeReg">DatanodeID to update registration for.</param>
		public override void UpdateRegInfo(DatanodeID nodeReg)
		{
			base.UpdateRegInfo(nodeReg);
			// must re-process IBR after re-registration
			foreach (DatanodeStorageInfo storage in GetStorageInfos())
			{
				storage.SetBlockReportCount(0);
			}
			heartbeatedSinceRegistration = false;
		}

		/// <returns>balancer bandwidth in bytes per second for this datanode</returns>
		public virtual long GetBalancerBandwidth()
		{
			return this.bandwidth;
		}

		/// <param name="bandwidth">balancer bandwidth in bytes per second for this datanode</param>
		public virtual void SetBalancerBandwidth(long bandwidth)
		{
			this.bandwidth = bandwidth;
		}

		public override string DumpDatanode()
		{
			StringBuilder sb = new StringBuilder(base.DumpDatanode());
			int repl = replicateBlocks.Size();
			if (repl > 0)
			{
				sb.Append(" ").Append(repl).Append(" blocks to be replicated;");
			}
			int inval = invalidateBlocks.Count;
			if (inval > 0)
			{
				sb.Append(" ").Append(inval).Append(" blocks to be invalidated;");
			}
			int recover = recoverBlocks.Size();
			if (recover > 0)
			{
				sb.Append(" ").Append(recover).Append(" blocks to be recovered;");
			}
			return sb.ToString();
		}

		internal virtual DatanodeStorageInfo UpdateStorage(DatanodeStorage s)
		{
			lock (storageMap)
			{
				DatanodeStorageInfo storage = storageMap[s.GetStorageID()];
				if (storage == null)
				{
					Log.Info("Adding new storage ID " + s.GetStorageID() + " for DN " + GetXferAddr()
						);
					storage = new DatanodeStorageInfo(this, s);
					storageMap[s.GetStorageID()] = storage;
				}
				else
				{
					if (storage.GetState() != s.GetState() || storage.GetStorageType() != s.GetStorageType
						())
					{
						// For backwards compatibility, make sure that the type and
						// state are updated. Some reports from older datanodes do
						// not include these fields so we may have assumed defaults.
						storage.UpdateFromStorage(s);
						storageMap[storage.GetStorageID()] = storage;
					}
				}
				return storage;
			}
		}

		/// <returns>
		/// The time at which we last sent caching directives to this
		/// DataNode, in monotonic milliseconds.
		/// </returns>
		public virtual long GetLastCachingDirectiveSentTimeMs()
		{
			return this.lastCachingDirectiveSentTimeMs;
		}

		/// <param name="time">
		/// The time at which we last sent caching directives to this
		/// DataNode, in monotonic milliseconds.
		/// </param>
		public virtual void SetLastCachingDirectiveSentTimeMs(long time)
		{
			this.lastCachingDirectiveSentTimeMs = time;
		}

		/// <summary>checks whether atleast first block report has been received</summary>
		/// <returns/>
		public virtual bool CheckBlockReportReceived()
		{
			if (this.GetStorageInfos().Length == 0)
			{
				return false;
			}
			foreach (DatanodeStorageInfo storageInfo in this.GetStorageInfos())
			{
				if (storageInfo.GetBlockReportCount() == 0)
				{
					return false;
				}
			}
			return true;
		}
	}
}
