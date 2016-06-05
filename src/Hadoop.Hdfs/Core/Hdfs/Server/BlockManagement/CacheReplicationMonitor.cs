using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Scans the namesystem, scheduling blocks to be cached as appropriate.</summary>
	/// <remarks>
	/// Scans the namesystem, scheduling blocks to be cached as appropriate.
	/// The CacheReplicationMonitor does a full scan when the NameNode first
	/// starts up, and at configurable intervals afterwards.
	/// </remarks>
	public class CacheReplicationMonitor : Sharpen.Thread, IDisposable
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.CacheReplicationMonitor
			));

		private readonly FSNamesystem namesystem;

		private readonly BlockManager blockManager;

		private readonly CacheManager cacheManager;

		private readonly GSet<CachedBlock, CachedBlock> cachedBlocks;

		/// <summary>Pseudorandom number source</summary>
		private static readonly Random random = new Random();

		/// <summary>The interval at which we scan the namesystem for caching changes.</summary>
		private readonly long intervalMs;

		/// <summary>The CacheReplicationMonitor (CRM) lock.</summary>
		/// <remarks>
		/// The CacheReplicationMonitor (CRM) lock. Used to synchronize starting and
		/// waiting for rescan operations.
		/// </remarks>
		private readonly ReentrantLock Lock;

		/// <summary>Notifies the scan thread that an immediate rescan is needed.</summary>
		private readonly Condition doRescan;

		/// <summary>Notifies waiting threads that a rescan has finished.</summary>
		private readonly Condition scanFinished;

		/// <summary>The number of rescans completed.</summary>
		/// <remarks>
		/// The number of rescans completed. Used to wait for scans to finish.
		/// Protected by the CacheReplicationMonitor lock.
		/// </remarks>
		private long completedScanCount = 0;

		/// <summary>The scan we're currently performing, or -1 if no scan is in progress.</summary>
		/// <remarks>
		/// The scan we're currently performing, or -1 if no scan is in progress.
		/// Protected by the CacheReplicationMonitor lock.
		/// </remarks>
		private long curScanCount = -1;

		/// <summary>The number of rescans we need to complete.</summary>
		/// <remarks>The number of rescans we need to complete.  Protected by the CRM lock.</remarks>
		private long neededScanCount = 0;

		/// <summary>True if this monitor should terminate.</summary>
		/// <remarks>True if this monitor should terminate. Protected by the CRM lock.</remarks>
		private bool shutdown = false;

		/// <summary>Mark status of the current scan.</summary>
		private bool mark = false;

		/// <summary>Cache directives found in the previous scan.</summary>
		private int scannedDirectives;

		/// <summary>Blocks found in the previous scan.</summary>
		private long scannedBlocks;

		public CacheReplicationMonitor(FSNamesystem namesystem, CacheManager cacheManager
			, long intervalMs, ReentrantLock Lock)
		{
			this.namesystem = namesystem;
			this.blockManager = namesystem.GetBlockManager();
			this.cacheManager = cacheManager;
			this.cachedBlocks = cacheManager.GetCachedBlocks();
			this.intervalMs = intervalMs;
			this.Lock = Lock;
			this.doRescan = this.Lock.NewCondition();
			this.scanFinished = this.Lock.NewCondition();
		}

		public override void Run()
		{
			long startTimeMs = 0;
			Sharpen.Thread.CurrentThread().SetName("CacheReplicationMonitor(" + Runtime.IdentityHashCode
				(this) + ")");
			Log.Info("Starting CacheReplicationMonitor with interval " + intervalMs + " milliseconds"
				);
			try
			{
				long curTimeMs = Time.MonotonicNow();
				while (true)
				{
					Lock.Lock();
					try
					{
						while (true)
						{
							if (shutdown)
							{
								Log.Debug("Shutting down CacheReplicationMonitor");
								return;
							}
							if (completedScanCount < neededScanCount)
							{
								Log.Debug("Rescanning because of pending operations");
								break;
							}
							long delta = (startTimeMs + intervalMs) - curTimeMs;
							if (delta <= 0)
							{
								Log.Debug("Rescanning after {} milliseconds", (curTimeMs - startTimeMs));
								break;
							}
							doRescan.Await(delta, TimeUnit.Milliseconds);
							curTimeMs = Time.MonotonicNow();
						}
					}
					finally
					{
						Lock.Unlock();
					}
					startTimeMs = curTimeMs;
					mark = !mark;
					Rescan();
					curTimeMs = Time.MonotonicNow();
					// Update synchronization-related variables.
					Lock.Lock();
					try
					{
						completedScanCount = curScanCount;
						curScanCount = -1;
						scanFinished.SignalAll();
					}
					finally
					{
						Lock.Unlock();
					}
					Log.Debug("Scanned {} directive(s) and {} block(s) in {} millisecond(s).", scannedDirectives
						, scannedBlocks, (curTimeMs - startTimeMs));
				}
			}
			catch (Exception)
			{
				Log.Info("Shutting down CacheReplicationMonitor.");
				return;
			}
			catch (Exception t)
			{
				Log.Error("Thread exiting", t);
				ExitUtil.Terminate(1, t);
			}
		}

		/// <summary>Waits for a rescan to complete.</summary>
		/// <remarks>
		/// Waits for a rescan to complete. This doesn't guarantee consistency with
		/// pending operations, only relative recency, since it will not force a new
		/// rescan if a rescan is already underway.
		/// <p>
		/// Note that this call will release the FSN lock, so operations before and
		/// after are not atomic.
		/// </remarks>
		public virtual void WaitForRescanIfNeeded()
		{
			Preconditions.CheckArgument(!namesystem.HasWriteLock(), "Must not hold the FSN write lock when waiting for a rescan."
				);
			Preconditions.CheckArgument(Lock.IsHeldByCurrentThread(), "Must hold the CRM lock when waiting for a rescan."
				);
			if (neededScanCount <= completedScanCount)
			{
				return;
			}
			// If no scan is already ongoing, mark the CRM as dirty and kick
			if (curScanCount < 0)
			{
				doRescan.Signal();
			}
			// Wait until the scan finishes and the count advances
			while ((!shutdown) && (completedScanCount < neededScanCount))
			{
				try
				{
					scanFinished.Await();
				}
				catch (Exception e)
				{
					Log.Warn("Interrupted while waiting for CacheReplicationMonitor" + " rescan", e);
					break;
				}
			}
		}

		/// <summary>
		/// Indicates to the CacheReplicationMonitor that there have been CacheManager
		/// changes that require a rescan.
		/// </summary>
		public virtual void SetNeedsRescan()
		{
			Preconditions.CheckArgument(Lock.IsHeldByCurrentThread(), "Must hold the CRM lock when setting the needsRescan bit."
				);
			if (curScanCount >= 0)
			{
				// If there is a scan in progress, we need to wait for the scan after
				// that.
				neededScanCount = curScanCount + 1;
			}
			else
			{
				// If there is no scan in progress, we need to wait for the next scan.
				neededScanCount = completedScanCount + 1;
			}
		}

		/// <summary>Shut down the monitor thread.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			Preconditions.CheckArgument(namesystem.HasWriteLock());
			Lock.Lock();
			try
			{
				if (shutdown)
				{
					return;
				}
				// Since we hold both the FSN write lock and the CRM lock here,
				// we know that the CRM thread cannot be currently modifying
				// the cache manager state while we're closing it.
				// Since the CRM thread checks the value of 'shutdown' after waiting
				// for a lock, we know that the thread will not modify the cache
				// manager state after this point.
				shutdown = true;
				doRescan.SignalAll();
				scanFinished.SignalAll();
			}
			finally
			{
				Lock.Unlock();
			}
		}

		/// <exception cref="System.Exception"/>
		private void Rescan()
		{
			scannedDirectives = 0;
			scannedBlocks = 0;
			try
			{
				namesystem.WriteLock();
				try
				{
					Lock.Lock();
					if (shutdown)
					{
						throw new Exception("CacheReplicationMonitor was " + "shut down.");
					}
					curScanCount = completedScanCount + 1;
				}
				finally
				{
					Lock.Unlock();
				}
				ResetStatistics();
				RescanCacheDirectives();
				RescanCachedBlockMap();
				blockManager.GetDatanodeManager().ResetLastCachingDirectiveSentTime();
			}
			finally
			{
				namesystem.WriteUnlock();
			}
		}

		private void ResetStatistics()
		{
			foreach (CachePool pool in cacheManager.GetCachePools())
			{
				pool.ResetStatistics();
			}
			foreach (CacheDirective directive in cacheManager.GetCacheDirectives())
			{
				directive.ResetStatistics();
			}
		}

		/// <summary>Scan all CacheDirectives.</summary>
		/// <remarks>
		/// Scan all CacheDirectives.  Use the information to figure out
		/// what cache replication factor each block should have.
		/// </remarks>
		private void RescanCacheDirectives()
		{
			FSDirectory fsDir = namesystem.GetFSDirectory();
			long now = new DateTime().GetTime();
			foreach (CacheDirective directive in cacheManager.GetCacheDirectives())
			{
				scannedDirectives++;
				// Skip processing this entry if it has expired
				if (directive.GetExpiryTime() > 0 && directive.GetExpiryTime() <= now)
				{
					Log.Debug("Directive {}: the directive expired at {} (now = {})", directive.GetId
						(), directive.GetExpiryTime(), now);
					continue;
				}
				string path = directive.GetPath();
				INode node;
				try
				{
					node = fsDir.GetINode(path);
				}
				catch (UnresolvedLinkException)
				{
					// We don't cache through symlinks
					Log.Debug("Directive {}: got UnresolvedLinkException while resolving " + "path {}"
						, directive.GetId(), path);
					continue;
				}
				if (node == null)
				{
					Log.Debug("Directive {}: No inode found at {}", directive.GetId(), path);
				}
				else
				{
					if (node.IsDirectory())
					{
						INodeDirectory dir = node.AsDirectory();
						ReadOnlyList<INode> children = dir.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
							.CurrentStateId);
						foreach (INode child in children)
						{
							if (child.IsFile())
							{
								RescanFile(directive, child.AsFile());
							}
						}
					}
					else
					{
						if (node.IsFile())
						{
							RescanFile(directive, node.AsFile());
						}
						else
						{
							Log.Debug("Directive {}: ignoring non-directive, non-file inode {} ", directive.GetId
								(), node);
						}
					}
				}
			}
		}

		/// <summary>Apply a CacheDirective to a file.</summary>
		/// <param name="directive">The CacheDirective to apply.</param>
		/// <param name="file">The file.</param>
		private void RescanFile(CacheDirective directive, INodeFile file)
		{
			BlockInfoContiguous[] blockInfos = file.GetBlocks();
			// Increment the "needed" statistics
			directive.AddFilesNeeded(1);
			// We don't cache UC blocks, don't add them to the total here
			long neededTotal = file.ComputeFileSizeNotIncludingLastUcBlock() * directive.GetReplication
				();
			directive.AddBytesNeeded(neededTotal);
			// The pool's bytesNeeded is incremented as we scan. If the demand
			// thus far plus the demand of this file would exceed the pool's limit,
			// do not cache this file.
			CachePool pool = directive.GetPool();
			if (pool.GetBytesNeeded() > pool.GetLimit())
			{
				Log.Debug("Directive {}: not scanning file {} because " + "bytesNeeded for pool {} is {}, but the pool's limit is {}"
					, directive.GetId(), file.GetFullPathName(), pool.GetPoolName(), pool.GetBytesNeeded
					(), pool.GetLimit());
				return;
			}
			long cachedTotal = 0;
			foreach (BlockInfoContiguous blockInfo in blockInfos)
			{
				if (!blockInfo.GetBlockUCState().Equals(HdfsServerConstants.BlockUCState.Complete
					))
				{
					// We don't try to cache blocks that are under construction.
					Log.Trace("Directive {}: can't cache block {} because it is in state " + "{}, not COMPLETE."
						, directive.GetId(), blockInfo, blockInfo.GetBlockUCState());
					continue;
				}
				Block block = new Block(blockInfo.GetBlockId());
				CachedBlock ncblock = new CachedBlock(block.GetBlockId(), directive.GetReplication
					(), mark);
				CachedBlock ocblock = cachedBlocks.Get(ncblock);
				if (ocblock == null)
				{
					cachedBlocks.Put(ncblock);
					ocblock = ncblock;
				}
				else
				{
					// Update bytesUsed using the current replication levels.
					// Assumptions: we assume that all the blocks are the same length
					// on each datanode.  We can assume this because we're only caching
					// blocks in state COMPLETE.
					// Note that if two directives are caching the same block(s), they will
					// both get them added to their bytesCached.
					IList<DatanodeDescriptor> cachedOn = ocblock.GetDatanodes(DatanodeDescriptor.CachedBlocksList.Type
						.Cached);
					long cachedByBlock = Math.Min(cachedOn.Count, directive.GetReplication()) * blockInfo
						.GetNumBytes();
					cachedTotal += cachedByBlock;
					if ((mark != ocblock.GetMark()) || (ocblock.GetReplication() < directive.GetReplication
						()))
					{
						//
						// Overwrite the block's replication and mark in two cases:
						//
						// 1. If the mark on the CachedBlock is different from the mark for
						// this scan, that means the block hasn't been updated during this
						// scan, and we should overwrite whatever is there, since it is no
						// longer valid.
						//
						// 2. If the replication in the CachedBlock is less than what the
						// directive asks for, we want to increase the block's replication
						// field to what the directive asks for.
						//
						ocblock.SetReplicationAndMark(directive.GetReplication(), mark);
					}
				}
				Log.Trace("Directive {}: setting replication for block {} to {}", directive.GetId
					(), blockInfo, ocblock.GetReplication());
			}
			// Increment the "cached" statistics
			directive.AddBytesCached(cachedTotal);
			if (cachedTotal == neededTotal)
			{
				directive.AddFilesCached(1);
			}
			Log.Debug("Directive {}: caching {}: {}/{} bytes", directive.GetId(), file.GetFullPathName
				(), cachedTotal, neededTotal);
		}

		private string FindReasonForNotCaching(CachedBlock cblock, BlockInfoContiguous blockInfo
			)
		{
			if (blockInfo == null)
			{
				// Somehow, a cache report with the block arrived, but the block
				// reports from the DataNode haven't (yet?) described such a block.
				// Alternately, the NameNode might have invalidated the block, but the
				// DataNode hasn't caught up.  In any case, we want to tell the DN
				// to uncache this.
				return "not tracked by the BlockManager";
			}
			else
			{
				if (!blockInfo.IsComplete())
				{
					// When a cached block changes state from complete to some other state
					// on the DataNode (perhaps because of append), it will begin the
					// uncaching process.  However, the uncaching process is not
					// instantaneous, especially if clients have pinned the block.  So
					// there may be a period of time when incomplete blocks remain cached
					// on the DataNodes.
					return "not complete";
				}
				else
				{
					if (cblock.GetReplication() == 0)
					{
						// Since 0 is not a valid value for a cache directive's replication
						// field, seeing a replication of 0 on a CacheBlock means that it
						// has never been reached by any sweep.
						return "not needed by any directives";
					}
					else
					{
						if (cblock.GetMark() != mark)
						{
							// Although the block was needed in the past, we didn't reach it during
							// the current sweep.  Therefore, it doesn't need to be cached any more.
							// Need to set the replication to 0 so it doesn't flip back to cached
							// when the mark flips on the next scan
							cblock.SetReplicationAndMark((short)0, mark);
							return "no longer needed by any directives";
						}
					}
				}
			}
			return null;
		}

		/// <summary>Scan through the cached block map.</summary>
		/// <remarks>
		/// Scan through the cached block map.
		/// Any blocks which are under-replicated should be assigned new Datanodes.
		/// Blocks that are over-replicated should be removed from Datanodes.
		/// </remarks>
		private void RescanCachedBlockMap()
		{
			for (IEnumerator<CachedBlock> cbIter = cachedBlocks.GetEnumerator(); cbIter.HasNext
				(); )
			{
				scannedBlocks++;
				CachedBlock cblock = cbIter.Next();
				IList<DatanodeDescriptor> pendingCached = cblock.GetDatanodes(DatanodeDescriptor.CachedBlocksList.Type
					.PendingCached);
				IList<DatanodeDescriptor> cached = cblock.GetDatanodes(DatanodeDescriptor.CachedBlocksList.Type
					.Cached);
				IList<DatanodeDescriptor> pendingUncached = cblock.GetDatanodes(DatanodeDescriptor.CachedBlocksList.Type
					.PendingUncached);
				// Remove nodes from PENDING_UNCACHED if they were actually uncached.
				for (IEnumerator<DatanodeDescriptor> iter = pendingUncached.GetEnumerator(); iter
					.HasNext(); )
				{
					DatanodeDescriptor datanode = iter.Next();
					if (!cblock.IsInList(datanode.GetCached()))
					{
						Log.Trace("Block {}: removing from PENDING_UNCACHED for node {} " + "because the DataNode uncached it."
							, cblock.GetBlockId(), datanode.GetDatanodeUuid());
						datanode.GetPendingUncached().Remove(cblock);
						iter.Remove();
					}
				}
				BlockInfoContiguous blockInfo = blockManager.GetStoredBlock(new Block(cblock.GetBlockId
					()));
				string reason = FindReasonForNotCaching(cblock, blockInfo);
				int neededCached = 0;
				if (reason != null)
				{
					Log.Trace("Block {}: can't cache block because it is {}", cblock.GetBlockId(), reason
						);
				}
				else
				{
					neededCached = cblock.GetReplication();
				}
				int numCached = cached.Count;
				if (numCached >= neededCached)
				{
					// If we have enough replicas, drop all pending cached.
					for (IEnumerator<DatanodeDescriptor> iter_1 = pendingCached.GetEnumerator(); iter_1
						.HasNext(); )
					{
						DatanodeDescriptor datanode = iter_1.Next();
						datanode.GetPendingCached().Remove(cblock);
						iter_1.Remove();
						Log.Trace("Block {}: removing from PENDING_CACHED for node {}" + "because we already have {} cached replicas and we only"
							 + " need {}", cblock.GetBlockId(), datanode.GetDatanodeUuid(), numCached, neededCached
							);
					}
				}
				if (numCached < neededCached)
				{
					// If we don't have enough replicas, drop all pending uncached.
					for (IEnumerator<DatanodeDescriptor> iter_1 = pendingUncached.GetEnumerator(); iter_1
						.HasNext(); )
					{
						DatanodeDescriptor datanode = iter_1.Next();
						datanode.GetPendingUncached().Remove(cblock);
						iter_1.Remove();
						Log.Trace("Block {}: removing from PENDING_UNCACHED for node {} " + "because we only have {} cached replicas and we need "
							 + "{}", cblock.GetBlockId(), datanode.GetDatanodeUuid(), numCached, neededCached
							);
					}
				}
				int neededUncached = numCached - (pendingUncached.Count + neededCached);
				if (neededUncached > 0)
				{
					AddNewPendingUncached(neededUncached, cblock, cached, pendingUncached);
				}
				else
				{
					int additionalCachedNeeded = neededCached - (numCached + pendingCached.Count);
					if (additionalCachedNeeded > 0)
					{
						AddNewPendingCached(additionalCachedNeeded, cblock, cached, pendingCached);
					}
				}
				if ((neededCached == 0) && pendingUncached.IsEmpty() && pendingCached.IsEmpty())
				{
					// we have nothing more to do with this block.
					Log.Trace("Block {}: removing from cachedBlocks, since neededCached " + "== 0, and pendingUncached and pendingCached are empty."
						, cblock.GetBlockId());
					cbIter.Remove();
				}
			}
		}

		/// <summary>Add new entries to the PendingUncached list.</summary>
		/// <param name="neededUncached">The number of replicas that need to be uncached.</param>
		/// <param name="cachedBlock">The block which needs to be uncached.</param>
		/// <param name="cached">A list of DataNodes currently caching the block.</param>
		/// <param name="pendingUncached">
		/// A list of DataNodes that will soon uncache the
		/// block.
		/// </param>
		private void AddNewPendingUncached(int neededUncached, CachedBlock cachedBlock, IList
			<DatanodeDescriptor> cached, IList<DatanodeDescriptor> pendingUncached)
		{
			// Figure out which replicas can be uncached.
			List<DatanodeDescriptor> possibilities = new List<DatanodeDescriptor>();
			foreach (DatanodeDescriptor datanode in cached)
			{
				if (!pendingUncached.Contains(datanode))
				{
					possibilities.AddItem(datanode);
				}
			}
			while (neededUncached > 0)
			{
				if (possibilities.IsEmpty())
				{
					Log.Warn("Logic error: we're trying to uncache more replicas than " + "actually exist for "
						 + cachedBlock);
					return;
				}
				DatanodeDescriptor datanode_1 = possibilities.Remove(random.Next(possibilities.Count
					));
				pendingUncached.AddItem(datanode_1);
				bool added = datanode_1.GetPendingUncached().AddItem(cachedBlock);
				System.Diagnostics.Debug.Assert(added);
				neededUncached--;
			}
		}

		/// <summary>Add new entries to the PendingCached list.</summary>
		/// <param name="neededCached">The number of replicas that need to be cached.</param>
		/// <param name="cachedBlock">The block which needs to be cached.</param>
		/// <param name="cached">A list of DataNodes currently caching the block.</param>
		/// <param name="pendingCached">
		/// A list of DataNodes that will soon cache the
		/// block.
		/// </param>
		private void AddNewPendingCached(int neededCached, CachedBlock cachedBlock, IList
			<DatanodeDescriptor> cached, IList<DatanodeDescriptor> pendingCached)
		{
			// To figure out which replicas can be cached, we consult the
			// blocksMap.  We don't want to try to cache a corrupt replica, though.
			BlockInfoContiguous blockInfo = blockManager.GetStoredBlock(new Block(cachedBlock
				.GetBlockId()));
			if (blockInfo == null)
			{
				Log.Debug("Block {}: can't add new cached replicas," + " because there is no record of this block "
					 + "on the NameNode.", cachedBlock.GetBlockId());
				return;
			}
			if (!blockInfo.IsComplete())
			{
				Log.Debug("Block {}: can't cache this block, because it is not yet" + " complete."
					, cachedBlock.GetBlockId());
				return;
			}
			// Filter the list of replicas to only the valid targets
			IList<DatanodeDescriptor> possibilities = new List<DatanodeDescriptor>();
			int numReplicas = blockInfo.GetCapacity();
			ICollection<DatanodeDescriptor> corrupt = blockManager.GetCorruptReplicas(blockInfo
				);
			int outOfCapacity = 0;
			for (int i = 0; i < numReplicas; i++)
			{
				DatanodeDescriptor datanode = blockInfo.GetDatanode(i);
				if (datanode == null)
				{
					continue;
				}
				if (datanode.IsDecommissioned() || datanode.IsDecommissionInProgress())
				{
					continue;
				}
				if (corrupt != null && corrupt.Contains(datanode))
				{
					continue;
				}
				if (pendingCached.Contains(datanode) || cached.Contains(datanode))
				{
					continue;
				}
				long pendingBytes = 0;
				// Subtract pending cached blocks from effective capacity
				IEnumerator<CachedBlock> it = datanode.GetPendingCached().GetEnumerator();
				while (it.HasNext())
				{
					CachedBlock cBlock = it.Next();
					BlockInfoContiguous info = blockManager.GetStoredBlock(new Block(cBlock.GetBlockId
						()));
					if (info != null)
					{
						pendingBytes -= info.GetNumBytes();
					}
				}
				it = datanode.GetPendingUncached().GetEnumerator();
				// Add pending uncached blocks from effective capacity
				while (it.HasNext())
				{
					CachedBlock cBlock = it.Next();
					BlockInfoContiguous info = blockManager.GetStoredBlock(new Block(cBlock.GetBlockId
						()));
					if (info != null)
					{
						pendingBytes += info.GetNumBytes();
					}
				}
				long pendingCapacity = pendingBytes + datanode.GetCacheRemaining();
				if (pendingCapacity < blockInfo.GetNumBytes())
				{
					Log.Trace("Block {}: DataNode {} is not a valid possibility " + "because the block has size {}, but the DataNode only has {}"
						 + "bytes of cache remaining ({} pending bytes, {} already cached.", blockInfo.GetBlockId
						(), datanode.GetDatanodeUuid(), blockInfo.GetNumBytes(), pendingCapacity, pendingBytes
						, datanode.GetCacheRemaining());
					outOfCapacity++;
					continue;
				}
				possibilities.AddItem(datanode);
			}
			IList<DatanodeDescriptor> chosen = ChooseDatanodesForCaching(possibilities, neededCached
				, blockManager.GetDatanodeManager().GetStaleInterval());
			foreach (DatanodeDescriptor datanode_1 in chosen)
			{
				Log.Trace("Block {}: added to PENDING_CACHED on DataNode {}", blockInfo.GetBlockId
					(), datanode_1.GetDatanodeUuid());
				pendingCached.AddItem(datanode_1);
				bool added = datanode_1.GetPendingCached().AddItem(cachedBlock);
				System.Diagnostics.Debug.Assert(added);
			}
			// We were unable to satisfy the requested replication factor
			if (neededCached > chosen.Count)
			{
				Log.Debug("Block {}: we only have {} of {} cached replicas." + " {} DataNodes have insufficient cache capacity."
					, blockInfo.GetBlockId(), (cachedBlock.GetReplication() - neededCached + chosen.
					Count), cachedBlock.GetReplication(), outOfCapacity);
			}
		}

		/// <summary>Chooses datanode locations for caching from a list of valid possibilities.
		/// 	</summary>
		/// <remarks>
		/// Chooses datanode locations for caching from a list of valid possibilities.
		/// Non-stale nodes are chosen before stale nodes.
		/// </remarks>
		/// <param name="possibilities">List of candidate datanodes</param>
		/// <param name="neededCached">Number of replicas needed</param>
		/// <param name="staleInterval">Age of a stale datanode</param>
		/// <returns>A list of chosen datanodes</returns>
		private static IList<DatanodeDescriptor> ChooseDatanodesForCaching(IList<DatanodeDescriptor
			> possibilities, int neededCached, long staleInterval)
		{
			// Make a copy that we can modify
			IList<DatanodeDescriptor> targets = new AList<DatanodeDescriptor>(possibilities);
			// Selected targets
			IList<DatanodeDescriptor> chosen = new List<DatanodeDescriptor>();
			// Filter out stale datanodes
			IList<DatanodeDescriptor> stale = new List<DatanodeDescriptor>();
			IEnumerator<DatanodeDescriptor> it = targets.GetEnumerator();
			while (it.HasNext())
			{
				DatanodeDescriptor d = it.Next();
				if (d.IsStale(staleInterval))
				{
					it.Remove();
					stale.AddItem(d);
				}
			}
			// Select targets
			while (chosen.Count < neededCached)
			{
				// Try to use stale nodes if we're out of non-stale nodes, else we're done
				if (targets.IsEmpty())
				{
					if (!stale.IsEmpty())
					{
						targets = stale;
					}
					else
					{
						break;
					}
				}
				// Select a random target
				DatanodeDescriptor target = ChooseRandomDatanodeByRemainingCapacity(targets);
				chosen.AddItem(target);
				targets.Remove(target);
			}
			return chosen;
		}

		/// <summary>
		/// Choose a single datanode from the provided list of possible
		/// targets, weighted by the percentage of free space remaining on the node.
		/// </summary>
		/// <returns>The chosen datanode</returns>
		private static DatanodeDescriptor ChooseRandomDatanodeByRemainingCapacity(IList<DatanodeDescriptor
			> targets)
		{
			// Use a weighted probability to choose the target datanode
			float total = 0;
			foreach (DatanodeDescriptor d in targets)
			{
				total += d.GetCacheRemainingPercent();
			}
			// Give each datanode a portion of keyspace equal to its relative weight
			// [0, w1) selects d1, [w1, w2) selects d2, etc.
			SortedDictionary<int, DatanodeDescriptor> lottery = new SortedDictionary<int, DatanodeDescriptor
				>();
			int offset = 0;
			foreach (DatanodeDescriptor d_1 in targets)
			{
				// Since we're using floats, be paranoid about negative values
				int weight = Math.Max(1, (int)((d_1.GetCacheRemainingPercent() / total) * 1000000
					));
				offset += weight;
				lottery[offset] = d_1;
			}
			// Choose a number from [0, offset), which is the total amount of weight,
			// to select the winner
			DatanodeDescriptor winner = lottery.HigherEntry(random.Next(offset)).Value;
			return winner;
		}
	}
}
