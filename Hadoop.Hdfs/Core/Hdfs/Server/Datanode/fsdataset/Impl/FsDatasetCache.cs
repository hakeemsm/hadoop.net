using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Lang.Time;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>
	/// Manages caching for an FsDatasetImpl by using the mmap(2) and mlock(2)
	/// system calls to lock blocks into memory.
	/// </summary>
	/// <remarks>
	/// Manages caching for an FsDatasetImpl by using the mmap(2) and mlock(2)
	/// system calls to lock blocks into memory. Block checksums are verified upon
	/// entry into the cache.
	/// </remarks>
	public class FsDatasetCache
	{
		/// <summary>MappableBlocks that we know about.</summary>
		private sealed class Value
		{
			internal readonly FsDatasetCache.State state;

			internal readonly MappableBlock mappableBlock;

			internal Value(MappableBlock mappableBlock, FsDatasetCache.State state)
			{
				this.mappableBlock = mappableBlock;
				this.state = state;
			}
		}

		[System.Serializable]
		private sealed class State
		{
			/// <summary>The MappableBlock is in the process of being cached.</summary>
			public static readonly FsDatasetCache.State Caching = new FsDatasetCache.State();

			/// <summary>
			/// The MappableBlock was in the process of being cached, but it was
			/// cancelled.
			/// </summary>
			/// <remarks>
			/// The MappableBlock was in the process of being cached, but it was
			/// cancelled.  Only the FsDatasetCache#WorkerTask can remove cancelled
			/// MappableBlock objects.
			/// </remarks>
			public static readonly FsDatasetCache.State CachingCancelled = new FsDatasetCache.State
				();

			/// <summary>The MappableBlock is in the cache.</summary>
			public static readonly FsDatasetCache.State Cached = new FsDatasetCache.State();

			/// <summary>The MappableBlock is in the process of uncaching.</summary>
			public static readonly FsDatasetCache.State Uncaching = new FsDatasetCache.State(
				);

			/// <summary>
			/// Whether we should advertise this block as cached to the NameNode and
			/// clients.
			/// </summary>
			public bool ShouldAdvertise()
			{
				return (this == FsDatasetCache.State.Cached);
			}
		}

		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(FsDatasetCache
			));

		/// <summary>Stores MappableBlock objects and the states they're in.</summary>
		private readonly Dictionary<ExtendedBlockId, FsDatasetCache.Value> mappableBlockMap
			 = new Dictionary<ExtendedBlockId, FsDatasetCache.Value>();

		private readonly AtomicLong numBlocksCached = new AtomicLong(0);

		private readonly FsDatasetImpl dataset;

		private readonly ThreadPoolExecutor uncachingExecutor;

		private readonly ScheduledThreadPoolExecutor deferredUncachingExecutor;

		private readonly long revocationMs;

		private readonly long revocationPollingMs;

		/// <summary>The approximate amount of cache space in use.</summary>
		/// <remarks>
		/// The approximate amount of cache space in use.
		/// This number is an overestimate, counting bytes that will be used only
		/// if pending caching operations succeed.  It does not take into account
		/// pending uncaching operations.
		/// This overestimate is more useful to the NameNode than an underestimate,
		/// since we don't want the NameNode to assign us more replicas than
		/// we can cache, because of the current batch of operations.
		/// </remarks>
		private readonly FsDatasetCache.UsedBytesCount usedBytesCount;

		public class PageRounder
		{
			private readonly long osPageSize = NativeIO.POSIX.GetCacheManipulator().GetOperatingSystemPageSize
				();

			/// <summary>Round up a number to the operating system page size.</summary>
			public virtual long Round(long count)
			{
				long newCount = (count + (osPageSize - 1)) / osPageSize;
				return newCount * osPageSize;
			}
		}

		private class UsedBytesCount
		{
			private readonly AtomicLong usedBytes = new AtomicLong(0);

			private readonly FsDatasetCache.PageRounder rounder = new FsDatasetCache.PageRounder
				();

			/// <summary>Try to reserve more bytes.</summary>
			/// <param name="count">
			/// The number of bytes to add.  We will round this
			/// up to the page size.
			/// </param>
			/// <returns>
			/// The new number of usedBytes if we succeeded;
			/// -1 if we failed.
			/// </returns>
			internal virtual long Reserve(long count)
			{
				count = this.rounder.Round(count);
				while (true)
				{
					long cur = this.usedBytes.Get();
					long next = cur + count;
					if (next > this._enclosing.maxBytes)
					{
						return -1;
					}
					if (this.usedBytes.CompareAndSet(cur, next))
					{
						return next;
					}
				}
			}

			/// <summary>Release some bytes that we're using.</summary>
			/// <param name="count">
			/// The number of bytes to release.  We will round this
			/// up to the page size.
			/// </param>
			/// <returns>The new number of usedBytes.</returns>
			internal virtual long Release(long count)
			{
				count = this.rounder.Round(count);
				return this.usedBytes.AddAndGet(-count);
			}

			internal virtual long Get()
			{
				return this.usedBytes.Get();
			}

			internal UsedBytesCount(FsDatasetCache _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FsDatasetCache _enclosing;
		}

		/// <summary>The total cache capacity in bytes.</summary>
		private readonly long maxBytes;

		/// <summary>Number of cache commands that could not be completed successfully</summary>
		internal readonly AtomicLong numBlocksFailedToCache = new AtomicLong(0);

		/// <summary>Number of uncache commands that could not be completed successfully</summary>
		internal readonly AtomicLong numBlocksFailedToUncache = new AtomicLong(0);

		public FsDatasetCache(FsDatasetImpl dataset)
		{
			this.dataset = dataset;
			this.maxBytes = dataset.datanode.GetDnConf().GetMaxLockedMemory();
			ThreadFactory workerFactory = new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat
				("FsDatasetCache-%d-" + dataset.ToString()).Build();
			this.usedBytesCount = new FsDatasetCache.UsedBytesCount(this);
			this.uncachingExecutor = new ThreadPoolExecutor(0, 1, 60, TimeUnit.Seconds, new LinkedBlockingQueue
				<Runnable>(), workerFactory);
			this.uncachingExecutor.AllowCoreThreadTimeOut(true);
			this.deferredUncachingExecutor = new ScheduledThreadPoolExecutor(1, workerFactory
				);
			this.revocationMs = dataset.datanode.GetConf().GetLong(DFSConfigKeys.DfsDatanodeCacheRevocationTimeoutMs
				, DFSConfigKeys.DfsDatanodeCacheRevocationTimeoutMsDefault);
			long confRevocationPollingMs = dataset.datanode.GetConf().GetLong(DFSConfigKeys.DfsDatanodeCacheRevocationPollingMs
				, DFSConfigKeys.DfsDatanodeCacheRevocationPollingMsDefault);
			long minRevocationPollingMs = revocationMs / 2;
			if (minRevocationPollingMs < confRevocationPollingMs)
			{
				throw new RuntimeException("configured value " + confRevocationPollingMs + "for "
					 + DFSConfigKeys.DfsDatanodeCacheRevocationPollingMs + " is too high.  It must not be more than half of the "
					 + "value of " + DFSConfigKeys.DfsDatanodeCacheRevocationTimeoutMs + ".  Reconfigure this to "
					 + minRevocationPollingMs);
			}
			this.revocationPollingMs = confRevocationPollingMs;
		}

		/// <returns>
		/// List of cached blocks suitable for translation into a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.BlockListAsLongs"/>
		/// for a cache report.
		/// </returns>
		internal virtual IList<long> GetCachedBlocks(string bpid)
		{
			lock (this)
			{
				IList<long> blocks = new AList<long>();
				for (IEnumerator<KeyValuePair<ExtendedBlockId, FsDatasetCache.Value>> iter = mappableBlockMap
					.GetEnumerator(); iter.HasNext(); )
				{
					KeyValuePair<ExtendedBlockId, FsDatasetCache.Value> entry = iter.Next();
					if (entry.Key.GetBlockPoolId().Equals(bpid))
					{
						if (entry.Value.state.ShouldAdvertise())
						{
							blocks.AddItem(entry.Key.GetBlockId());
						}
					}
				}
				return blocks;
			}
		}

		/// <summary>Attempt to begin caching a block.</summary>
		internal virtual void CacheBlock(long blockId, string bpid, string blockFileName, 
			long length, long genstamp, Executor volumeExecutor)
		{
			lock (this)
			{
				ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
				FsDatasetCache.Value prevValue = mappableBlockMap[key];
				if (prevValue != null)
				{
					Log.Debug("Block with id {}, pool {} already exists in the " + "FsDatasetCache with state {}"
						, blockId, bpid, prevValue.state);
					numBlocksFailedToCache.IncrementAndGet();
					return;
				}
				mappableBlockMap[key] = new FsDatasetCache.Value(null, FsDatasetCache.State.Caching
					);
				volumeExecutor.Execute(new FsDatasetCache.CachingTask(this, key, blockFileName, length
					, genstamp));
				Log.Debug("Initiating caching for Block with id {}, pool {}", blockId, bpid);
			}
		}

		internal virtual void UncacheBlock(string bpid, long blockId)
		{
			lock (this)
			{
				ExtendedBlockId key = new ExtendedBlockId(blockId, bpid);
				FsDatasetCache.Value prevValue = mappableBlockMap[key];
				bool deferred = false;
				if (!dataset.datanode.GetShortCircuitRegistry().ProcessBlockMunlockRequest(key))
				{
					deferred = true;
				}
				if (prevValue == null)
				{
					Log.Debug("Block with id {}, pool {} does not need to be uncached, " + "because it is not currently in the mappableBlockMap."
						, blockId, bpid);
					numBlocksFailedToUncache.IncrementAndGet();
					return;
				}
				switch (prevValue.state)
				{
					case FsDatasetCache.State.Caching:
					{
						Log.Debug("Cancelling caching for block with id {}, pool {}.", blockId, bpid);
						mappableBlockMap[key] = new FsDatasetCache.Value(prevValue.mappableBlock, FsDatasetCache.State
							.CachingCancelled);
						break;
					}

					case FsDatasetCache.State.Cached:
					{
						mappableBlockMap[key] = new FsDatasetCache.Value(prevValue.mappableBlock, FsDatasetCache.State
							.Uncaching);
						if (deferred)
						{
							Log.Debug("{} is anchored, and can't be uncached now.  Scheduling it " + "for uncaching in {} "
								, key, DurationFormatUtils.FormatDurationHMS(revocationPollingMs));
							deferredUncachingExecutor.Schedule(new FsDatasetCache.UncachingTask(this, key, revocationMs
								), revocationPollingMs, TimeUnit.Milliseconds);
						}
						else
						{
							Log.Debug("{} has been scheduled for immediate uncaching.", key);
							uncachingExecutor.Execute(new FsDatasetCache.UncachingTask(this, key, 0));
						}
						break;
					}

					default:
					{
						Log.Debug("Block with id {}, pool {} does not need to be uncached, " + "because it is in state {}."
							, blockId, bpid, prevValue.state);
						numBlocksFailedToUncache.IncrementAndGet();
						break;
					}
				}
			}
		}

		/// <summary>Background worker that mmaps, mlocks, and checksums a block</summary>
		private class CachingTask : Runnable
		{
			private readonly ExtendedBlockId key;

			private readonly string blockFileName;

			private readonly long length;

			private readonly long genstamp;

			internal CachingTask(FsDatasetCache _enclosing, ExtendedBlockId key, string blockFileName
				, long length, long genstamp)
			{
				this._enclosing = _enclosing;
				this.key = key;
				this.blockFileName = blockFileName;
				this.length = length;
				this.genstamp = genstamp;
			}

			public virtual void Run()
			{
				bool success = false;
				FileInputStream blockIn = null;
				FileInputStream metaIn = null;
				MappableBlock mappableBlock = null;
				ExtendedBlock extBlk = new ExtendedBlock(this.key.GetBlockPoolId(), this.key.GetBlockId
					(), this.length, this.genstamp);
				long newUsedBytes = this._enclosing.usedBytesCount.Reserve(this.length);
				bool reservedBytes = false;
				try
				{
					if (newUsedBytes < 0)
					{
						FsDatasetCache.Log.Warn("Failed to cache " + this.key + ": could not reserve " + 
							this.length + " more bytes in the cache: " + DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey
							 + " of " + this._enclosing.maxBytes + " exceeded.");
						return;
					}
					reservedBytes = true;
					try
					{
						blockIn = (FileInputStream)this._enclosing.dataset.GetBlockInputStream(extBlk, 0);
						metaIn = DatanodeUtil.GetMetaDataInputStream(extBlk, this._enclosing.dataset);
					}
					catch (InvalidCastException e)
					{
						FsDatasetCache.Log.Warn("Failed to cache " + this.key + ": Underlying blocks are not backed by files."
							, e);
						return;
					}
					catch (FileNotFoundException)
					{
						FsDatasetCache.Log.Info("Failed to cache " + this.key + ": failed to find backing "
							 + "files.");
						return;
					}
					catch (IOException e)
					{
						FsDatasetCache.Log.Warn("Failed to cache " + this.key + ": failed to open file", 
							e);
						return;
					}
					try
					{
						mappableBlock = MappableBlock.Load(this.length, blockIn, metaIn, this.blockFileName
							);
					}
					catch (ChecksumException)
					{
						// Exception message is bogus since this wasn't caused by a file read
						FsDatasetCache.Log.Warn("Failed to cache " + this.key + ": checksum verification failed."
							);
						return;
					}
					catch (IOException e)
					{
						FsDatasetCache.Log.Warn("Failed to cache " + this.key, e);
						return;
					}
					lock (this._enclosing)
					{
						FsDatasetCache.Value value = this._enclosing.mappableBlockMap[this.key];
						Preconditions.CheckNotNull(value);
						Preconditions.CheckState(value.state == FsDatasetCache.State.Caching || value.state
							 == FsDatasetCache.State.CachingCancelled);
						if (value.state == FsDatasetCache.State.CachingCancelled)
						{
							Sharpen.Collections.Remove(this._enclosing.mappableBlockMap, this.key);
							FsDatasetCache.Log.Warn("Caching of " + this.key + " was cancelled.");
							return;
						}
						this._enclosing.mappableBlockMap[this.key] = new FsDatasetCache.Value(mappableBlock
							, FsDatasetCache.State.Cached);
					}
					FsDatasetCache.Log.Debug("Successfully cached {}.  We are now caching {} bytes in"
						 + " total.", this.key, newUsedBytes);
					this._enclosing.dataset.datanode.GetShortCircuitRegistry().ProcessBlockMlockEvent
						(this.key);
					this._enclosing.numBlocksCached.AddAndGet(1);
					this._enclosing.dataset.datanode.GetMetrics().IncrBlocksCached(1);
					success = true;
				}
				finally
				{
					IOUtils.CloseQuietly(blockIn);
					IOUtils.CloseQuietly(metaIn);
					if (!success)
					{
						if (reservedBytes)
						{
							this._enclosing.usedBytesCount.Release(this.length);
						}
						FsDatasetCache.Log.Debug("Caching of {} was aborted.  We are now caching only {} "
							 + "bytes in total.", this.key, this._enclosing.usedBytesCount.Get());
						if (mappableBlock != null)
						{
							mappableBlock.Close();
						}
						this._enclosing.numBlocksFailedToCache.IncrementAndGet();
						lock (this._enclosing)
						{
							Sharpen.Collections.Remove(this._enclosing.mappableBlockMap, this.key);
						}
					}
				}
			}

			private readonly FsDatasetCache _enclosing;
		}

		private class UncachingTask : Runnable
		{
			private readonly ExtendedBlockId key;

			private readonly long revocationTimeMs;

			internal UncachingTask(FsDatasetCache _enclosing, ExtendedBlockId key, long revocationDelayMs
				)
			{
				this._enclosing = _enclosing;
				this.key = key;
				if (revocationDelayMs == 0)
				{
					this.revocationTimeMs = 0;
				}
				else
				{
					this.revocationTimeMs = revocationDelayMs + Org.Apache.Hadoop.Util.Time.MonotonicNow
						();
				}
			}

			private bool ShouldDefer()
			{
				/* If revocationTimeMs == 0, this is an immediate uncache request.
				* No clients were anchored at the time we made the request. */
				if (this.revocationTimeMs == 0)
				{
					return false;
				}
				/* Let's check if any clients still have this block anchored. */
				bool anchored = !this._enclosing.dataset.datanode.GetShortCircuitRegistry().ProcessBlockMunlockRequest
					(this.key);
				if (!anchored)
				{
					FsDatasetCache.Log.Debug("Uncaching {} now that it is no longer in use " + "by any clients."
						, this.key);
					return false;
				}
				long delta = this.revocationTimeMs - Org.Apache.Hadoop.Util.Time.MonotonicNow();
				if (delta < 0)
				{
					FsDatasetCache.Log.Warn("Forcibly uncaching {} after {} " + "because client(s) {} refused to stop using it."
						, this.key, DurationFormatUtils.FormatDurationHMS(this.revocationTimeMs), this._enclosing
						.dataset.datanode.GetShortCircuitRegistry().GetClientNames(this.key));
					return false;
				}
				FsDatasetCache.Log.Info("Replica {} still can't be uncached because some " + "clients continue to use it.  Will wait for {}"
					, this.key, DurationFormatUtils.FormatDurationHMS(delta));
				return true;
			}

			public virtual void Run()
			{
				FsDatasetCache.Value value;
				if (this.ShouldDefer())
				{
					this._enclosing.deferredUncachingExecutor.Schedule(this, this._enclosing.revocationPollingMs
						, TimeUnit.Milliseconds);
					return;
				}
				lock (this._enclosing)
				{
					value = this._enclosing.mappableBlockMap[this.key];
				}
				Preconditions.CheckNotNull(value);
				Preconditions.CheckArgument(value.state == FsDatasetCache.State.Uncaching);
				IOUtils.CloseQuietly(value.mappableBlock);
				lock (this._enclosing)
				{
					Sharpen.Collections.Remove(this._enclosing.mappableBlockMap, this.key);
				}
				long newUsedBytes = this._enclosing.usedBytesCount.Release(value.mappableBlock.GetLength
					());
				this._enclosing.numBlocksCached.AddAndGet(-1);
				this._enclosing.dataset.datanode.GetMetrics().IncrBlocksUncached(1);
				if (this.revocationTimeMs != 0)
				{
					FsDatasetCache.Log.Debug("Uncaching of {} completed. usedBytes = {}", this.key, newUsedBytes
						);
				}
				else
				{
					FsDatasetCache.Log.Debug("Deferred uncaching of {} completed. usedBytes = {}", this
						.key, newUsedBytes);
				}
			}

			private readonly FsDatasetCache _enclosing;
		}

		// Stats related methods for FSDatasetMBean
		/// <summary>Get the approximate amount of cache space used.</summary>
		public virtual long GetCacheUsed()
		{
			return usedBytesCount.Get();
		}

		/// <summary>Get the maximum amount of bytes we can cache.</summary>
		/// <remarks>Get the maximum amount of bytes we can cache.  This is a constant.</remarks>
		public virtual long GetCacheCapacity()
		{
			return maxBytes;
		}

		public virtual long GetNumBlocksFailedToCache()
		{
			return numBlocksFailedToCache.Get();
		}

		public virtual long GetNumBlocksFailedToUncache()
		{
			return numBlocksFailedToUncache.Get();
		}

		public virtual long GetNumBlocksCached()
		{
			return numBlocksCached.Get();
		}

		public virtual bool IsCached(string bpid, long blockId)
		{
			lock (this)
			{
				ExtendedBlockId block = new ExtendedBlockId(blockId, bpid);
				FsDatasetCache.Value val = mappableBlockMap[block];
				return (val != null) && val.state.ShouldAdvertise();
			}
		}
	}
}
