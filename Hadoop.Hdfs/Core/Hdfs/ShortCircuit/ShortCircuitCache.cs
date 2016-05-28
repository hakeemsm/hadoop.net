using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Lang.Mutable;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	/// <summary>
	/// The ShortCircuitCache tracks things which the client needs to access
	/// HDFS block files via short-circuit.
	/// </summary>
	/// <remarks>
	/// The ShortCircuitCache tracks things which the client needs to access
	/// HDFS block files via short-circuit.
	/// These things include: memory-mapped regions, file descriptors, and shared
	/// memory areas for communicating with the DataNode.
	/// </remarks>
	public class ShortCircuitCache : IDisposable
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Shortcircuit.ShortCircuitCache
			));

		/// <summary>
		/// Expiry thread which makes sure that the file descriptors get closed
		/// after a while.
		/// </summary>
		private class CacheCleaner : Runnable, IDisposable
		{
			private ScheduledFuture<object> future;

			/// <summary>Run the CacheCleaner thread.</summary>
			/// <remarks>
			/// Run the CacheCleaner thread.
			/// Whenever a thread requests a ShortCircuitReplica object, we will make
			/// sure it gets one.  That ShortCircuitReplica object can then be re-used
			/// when another thread requests a ShortCircuitReplica object for the same
			/// block.  So in that sense, there is no maximum size to the cache.
			/// However, when a ShortCircuitReplica object is unreferenced by the
			/// thread(s) that are using it, it becomes evictable.  There are two
			/// separate eviction lists-- one for mmaped objects, and another for
			/// non-mmaped objects.  We do this in order to avoid having the regular
			/// files kick the mmaped files out of the cache too quickly.  Reusing
			/// an already-existing mmap gives a huge performance boost, since the
			/// page table entries don't have to be re-populated.  Both the mmap
			/// and non-mmap evictable lists have maximum sizes and maximum lifespans.
			/// </remarks>
			public virtual void Run()
			{
				this._enclosing.Lock.Lock();
				try
				{
					if (this._enclosing.closed)
					{
						return;
					}
					long curMs = Time.MonotonicNow();
					if (ShortCircuitCache.Log.IsDebugEnabled())
					{
						ShortCircuitCache.Log.Debug(this + ": cache cleaner running at " + curMs);
					}
					int numDemoted = this._enclosing.DemoteOldEvictableMmaped(curMs);
					int numPurged = 0;
					long evictionTimeNs = Sharpen.Extensions.ValueOf(0);
					while (true)
					{
						KeyValuePair<long, ShortCircuitReplica> entry = this._enclosing.evictable.CeilingEntry
							(evictionTimeNs);
						if (entry == null)
						{
							break;
						}
						evictionTimeNs = entry.Key;
						long evictionTimeMs = TimeUnit.Milliseconds.Convert(evictionTimeNs, TimeUnit.Nanoseconds
							);
						if (evictionTimeMs + this._enclosing.maxNonMmappedEvictableLifespanMs >= curMs)
						{
							break;
						}
						ShortCircuitReplica replica = entry.Value;
						if (ShortCircuitCache.Log.IsTraceEnabled())
						{
							ShortCircuitCache.Log.Trace("CacheCleaner: purging " + replica + ": " + StringUtils
								.GetStackTrace(Sharpen.Thread.CurrentThread()));
						}
						this._enclosing.Purge(replica);
						numPurged++;
					}
					if (ShortCircuitCache.Log.IsDebugEnabled())
					{
						ShortCircuitCache.Log.Debug(this + ": finishing cache cleaner run started at " + 
							curMs + ".  Demoted " + numDemoted + " mmapped replicas; " + "purged " + numPurged
							 + " replicas.");
					}
				}
				finally
				{
					this._enclosing.Lock.Unlock();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				if (this.future != null)
				{
					this.future.Cancel(false);
				}
			}

			public virtual void SetFuture<_T0>(ScheduledFuture<_T0> future)
			{
				this.future = future;
			}

			/// <summary>Get the rate at which this cleaner thread should be scheduled.</summary>
			/// <remarks>
			/// Get the rate at which this cleaner thread should be scheduled.
			/// We do this by taking the minimum expiration time and dividing by 4.
			/// </remarks>
			/// <returns>
			/// the rate in milliseconds at which this thread should be
			/// scheduled.
			/// </returns>
			public virtual long GetRateInMs()
			{
				long minLifespanMs = Math.Min(this._enclosing.maxNonMmappedEvictableLifespanMs, this
					._enclosing.maxEvictableMmapedLifespanMs);
				long sampleTimeMs = minLifespanMs / 4;
				return (sampleTimeMs < 1) ? 1 : sampleTimeMs;
			}

			internal CacheCleaner(ShortCircuitCache _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ShortCircuitCache _enclosing;
		}

		/// <summary>
		/// A task which asks the DataNode to release a short-circuit shared memory
		/// slot.
		/// </summary>
		/// <remarks>
		/// A task which asks the DataNode to release a short-circuit shared memory
		/// slot.  If successful, this will tell the DataNode to stop monitoring
		/// changes to the mlock status of the replica associated with the slot.
		/// It will also allow us (the client) to re-use this slot for another
		/// replica.  If we can't communicate with the DataNode for some reason,
		/// we tear down the shared memory segment to avoid being in an inconsistent
		/// state.
		/// </remarks>
		private class SlotReleaser : Runnable
		{
			/// <summary>The slot that we need to release.</summary>
			private readonly ShortCircuitShm.Slot slot;

			internal SlotReleaser(ShortCircuitCache _enclosing, ShortCircuitShm.Slot slot)
			{
				this._enclosing = _enclosing;
				this.slot = slot;
			}

			public virtual void Run()
			{
				if (ShortCircuitCache.Log.IsTraceEnabled())
				{
					ShortCircuitCache.Log.Trace(this._enclosing + ": about to release " + this.slot);
				}
				DfsClientShm shm = (DfsClientShm)this.slot.GetShm();
				DomainSocket shmSock = shm.GetPeer().GetDomainSocket();
				DomainSocket sock = null;
				DataOutputStream @out = null;
				string path = shmSock.GetPath();
				bool success = false;
				try
				{
					sock = DomainSocket.Connect(path);
					@out = new DataOutputStream(new BufferedOutputStream(sock.GetOutputStream()));
					new Sender(@out).ReleaseShortCircuitFds(this.slot.GetSlotId());
					DataInputStream @in = new DataInputStream(sock.GetInputStream());
					DataTransferProtos.ReleaseShortCircuitAccessResponseProto resp = DataTransferProtos.ReleaseShortCircuitAccessResponseProto
						.ParseFrom(PBHelper.VintPrefixed(@in));
					if (resp.GetStatus() != DataTransferProtos.Status.Success)
					{
						string error = resp.HasError() ? resp.GetError() : "(unknown)";
						throw new IOException(resp.GetStatus().ToString() + ": " + error);
					}
					if (ShortCircuitCache.Log.IsTraceEnabled())
					{
						ShortCircuitCache.Log.Trace(this._enclosing + ": released " + this.slot);
					}
					success = true;
				}
				catch (IOException e)
				{
					ShortCircuitCache.Log.Error(this._enclosing + ": failed to release " + "short-circuit shared memory slot "
						 + this.slot + " by sending " + "ReleaseShortCircuitAccessRequestProto to " + path
						 + ".  Closing shared memory segment.", e);
				}
				finally
				{
					if (success)
					{
						this._enclosing.shmManager.FreeSlot(this.slot);
					}
					else
					{
						shm.GetEndpointShmManager().Shutdown(shm);
					}
					IOUtils.Cleanup(ShortCircuitCache.Log, sock, @out);
				}
			}

			private readonly ShortCircuitCache _enclosing;
		}

		public interface ShortCircuitReplicaCreator
		{
			/// <summary>Attempt to create a ShortCircuitReplica object.</summary>
			/// <remarks>
			/// Attempt to create a ShortCircuitReplica object.
			/// This callback will be made without holding any locks.
			/// </remarks>
			/// <returns>a non-null ShortCircuitReplicaInfo object.</returns>
			ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo();
		}

		/// <summary>Lock protecting the cache.</summary>
		private readonly ReentrantLock Lock = new ReentrantLock();

		/// <summary>The executor service that runs the cacheCleaner.</summary>
		private readonly ScheduledThreadPoolExecutor cleanerExecutor = new ScheduledThreadPoolExecutor
			(1, new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat("ShortCircuitCache_Cleaner"
			).Build());

		/// <summary>The executor service that runs the cacheCleaner.</summary>
		private readonly ScheduledThreadPoolExecutor releaserExecutor = new ScheduledThreadPoolExecutor
			(1, new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat("ShortCircuitCache_SlotReleaser"
			).Build());

		/// <summary>A map containing all ShortCircuitReplicaInfo objects, organized by Key.</summary>
		/// <remarks>
		/// A map containing all ShortCircuitReplicaInfo objects, organized by Key.
		/// ShortCircuitReplicaInfo objects may contain a replica, or an InvalidToken
		/// exception.
		/// </remarks>
		private readonly Dictionary<ExtendedBlockId, Waitable<ShortCircuitReplicaInfo>> replicaInfoMap
			 = new Dictionary<ExtendedBlockId, Waitable<ShortCircuitReplicaInfo>>();

		/// <summary>The CacheCleaner.</summary>
		/// <remarks>
		/// The CacheCleaner.  We don't create this and schedule it until it becomes
		/// necessary.
		/// </remarks>
		private ShortCircuitCache.CacheCleaner cacheCleaner;

		/// <summary>Tree of evictable elements.</summary>
		/// <remarks>
		/// Tree of evictable elements.
		/// Maps (unique) insertion time in nanoseconds to the element.
		/// </remarks>
		private readonly SortedDictionary<long, ShortCircuitReplica> evictable = new SortedDictionary
			<long, ShortCircuitReplica>();

		/// <summary>
		/// Maximum total size of the cache, including both mmapped and
		/// no$-mmapped elements.
		/// </summary>
		private readonly int maxTotalSize;

		/// <summary>Non-mmaped elements older than this will be closed.</summary>
		private long maxNonMmappedEvictableLifespanMs;

		/// <summary>Tree of mmaped evictable elements.</summary>
		/// <remarks>
		/// Tree of mmaped evictable elements.
		/// Maps (unique) insertion time in nanoseconds to the element.
		/// </remarks>
		private readonly SortedDictionary<long, ShortCircuitReplica> evictableMmapped = new 
			SortedDictionary<long, ShortCircuitReplica>();

		/// <summary>Maximum number of mmaped evictable elements.</summary>
		private int maxEvictableMmapedSize;

		/// <summary>Mmaped elements older than this will be closed.</summary>
		private readonly long maxEvictableMmapedLifespanMs;

		/// <summary>
		/// The minimum number of milliseconds we'll wait after an unsuccessful
		/// mmap attempt before trying again.
		/// </summary>
		private readonly long mmapRetryTimeoutMs;

		/// <summary>
		/// How long we will keep replicas in the cache before declaring them
		/// to be stale.
		/// </summary>
		private readonly long staleThresholdMs;

		/// <summary>True if the ShortCircuitCache is closed.</summary>
		private bool closed = false;

		/// <summary>Number of existing mmaps associated with this cache.</summary>
		private int outstandingMmapCount = 0;

		/// <summary>Manages short-circuit shared memory segments for the client.</summary>
		private readonly DfsClientShmManager shmManager;

		/// <summary>
		/// Create a
		/// <see cref="ShortCircuitCache"/>
		/// object from a
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// </summary>
		public static ShortCircuitCache FromConf(Configuration conf)
		{
			return new ShortCircuitCache(conf.GetInt(DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheSizeKey
				, DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheSizeDefault), conf.GetLong(
				DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheExpiryMsKey, DFSConfigKeys.DfsClientReadShortcircuitStreamsCacheExpiryMsDefault
				), conf.GetInt(DFSConfigKeys.DfsClientMmapCacheSize, DFSConfigKeys.DfsClientMmapCacheSizeDefault
				), conf.GetLong(DFSConfigKeys.DfsClientMmapCacheTimeoutMs, DFSConfigKeys.DfsClientMmapCacheTimeoutMsDefault
				), conf.GetLong(DFSConfigKeys.DfsClientMmapRetryTimeoutMs, DFSConfigKeys.DfsClientMmapRetryTimeoutMsDefault
				), conf.GetLong(DFSConfigKeys.DfsClientShortCircuitReplicaStaleThresholdMs, DFSConfigKeys
				.DfsClientShortCircuitReplicaStaleThresholdMsDefault), conf.GetInt(DFSConfigKeys
				.DfsShortCircuitSharedMemoryWatcherInterruptCheckMs, DFSConfigKeys.DfsShortCircuitSharedMemoryWatcherInterruptCheckMsDefault
				));
		}

		public ShortCircuitCache(int maxTotalSize, long maxNonMmappedEvictableLifespanMs, 
			int maxEvictableMmapedSize, long maxEvictableMmapedLifespanMs, long mmapRetryTimeoutMs
			, long staleThresholdMs, int shmInterruptCheckMs)
		{
			Preconditions.CheckArgument(maxTotalSize >= 0);
			this.maxTotalSize = maxTotalSize;
			Preconditions.CheckArgument(maxNonMmappedEvictableLifespanMs >= 0);
			this.maxNonMmappedEvictableLifespanMs = maxNonMmappedEvictableLifespanMs;
			Preconditions.CheckArgument(maxEvictableMmapedSize >= 0);
			this.maxEvictableMmapedSize = maxEvictableMmapedSize;
			Preconditions.CheckArgument(maxEvictableMmapedLifespanMs >= 0);
			this.maxEvictableMmapedLifespanMs = maxEvictableMmapedLifespanMs;
			this.mmapRetryTimeoutMs = mmapRetryTimeoutMs;
			this.staleThresholdMs = staleThresholdMs;
			DfsClientShmManager shmManager = null;
			if ((shmInterruptCheckMs > 0) && (DomainSocketWatcher.GetLoadingFailureReason() ==
				 null))
			{
				try
				{
					shmManager = new DfsClientShmManager(shmInterruptCheckMs);
				}
				catch (IOException e)
				{
					Log.Error("failed to create ShortCircuitShmManager", e);
				}
			}
			this.shmManager = shmManager;
		}

		public virtual long GetStaleThresholdMs()
		{
			return staleThresholdMs;
		}

		/// <summary>
		/// Increment the reference count of a replica, and remove it from any free
		/// list it may be in.
		/// </summary>
		/// <remarks>
		/// Increment the reference count of a replica, and remove it from any free
		/// list it may be in.
		/// You must hold the cache lock while calling this function.
		/// </remarks>
		/// <param name="replica">The replica we're removing.</param>
		private void Ref(ShortCircuitReplica replica)
		{
			Lock.Lock();
			try
			{
				Preconditions.CheckArgument(replica.refCount > 0, "can't ref %s because its refCount reached %d"
					, replica, replica.refCount);
				long evictableTimeNs = replica.GetEvictableTimeNs();
				replica.refCount++;
				if (evictableTimeNs != null)
				{
					string removedFrom = RemoveEvictable(replica);
					if (Log.IsTraceEnabled())
					{
						Log.Trace(this + ": " + removedFrom + " no longer contains " + replica + ".  refCount "
							 + (replica.refCount - 1) + " -> " + replica.refCount + StringUtils.GetStackTrace
							(Sharpen.Thread.CurrentThread()));
					}
				}
				else
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace(this + ": replica  refCount " + (replica.refCount - 1) + " -> " + replica
							.refCount + StringUtils.GetStackTrace(Sharpen.Thread.CurrentThread()));
					}
				}
			}
			finally
			{
				Lock.Unlock();
			}
		}

		/// <summary>Unreference a replica.</summary>
		/// <remarks>
		/// Unreference a replica.
		/// You must hold the cache lock while calling this function.
		/// </remarks>
		/// <param name="replica">The replica being unreferenced.</param>
		internal virtual void Unref(ShortCircuitReplica replica)
		{
			Lock.Lock();
			try
			{
				// If the replica is stale or unusable, but we haven't purged it yet,
				// let's do that.  It would be a shame to evict a non-stale replica so
				// that we could put a stale or unusable one into the cache.
				if (!replica.purged)
				{
					string purgeReason = null;
					if (!replica.GetDataStream().GetChannel().IsOpen())
					{
						purgeReason = "purging replica because its data channel is closed.";
					}
					else
					{
						if (!replica.GetMetaStream().GetChannel().IsOpen())
						{
							purgeReason = "purging replica because its meta channel is closed.";
						}
						else
						{
							if (replica.IsStale())
							{
								purgeReason = "purging replica because it is stale.";
							}
						}
					}
					if (purgeReason != null)
					{
						Log.Debug(this + ": " + purgeReason);
						Purge(replica);
					}
				}
				string addedString = string.Empty;
				bool shouldTrimEvictionMaps = false;
				int newRefCount = --replica.refCount;
				if (newRefCount == 0)
				{
					// Close replica, since there are no remaining references to it.
					Preconditions.CheckArgument(replica.purged, "Replica %s reached a refCount of 0 without being purged"
						, replica);
					replica.Close();
				}
				else
				{
					if (newRefCount == 1)
					{
						Preconditions.CheckState(null == replica.GetEvictableTimeNs(), "Replica %s had a refCount higher than 1, "
							 + "but was still evictable (evictableTimeNs = %d)", replica, replica.GetEvictableTimeNs
							());
						if (!replica.purged)
						{
							// Add the replica to the end of an eviction list.
							// Eviction lists are sorted by time.
							if (replica.HasMmap())
							{
								InsertEvictable(Runtime.NanoTime(), replica, evictableMmapped);
								addedString = "added to evictableMmapped, ";
							}
							else
							{
								InsertEvictable(Runtime.NanoTime(), replica, evictable);
								addedString = "added to evictable, ";
							}
							shouldTrimEvictionMaps = true;
						}
					}
					else
					{
						Preconditions.CheckArgument(replica.refCount >= 0, "replica's refCount went negative (refCount = %d"
							 + " for %s)", replica.refCount, replica);
					}
				}
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": unref replica " + replica + ": " + addedString + " refCount "
						 + (newRefCount + 1) + " -> " + newRefCount + StringUtils.GetStackTrace(Sharpen.Thread
						.CurrentThread()));
				}
				if (shouldTrimEvictionMaps)
				{
					TrimEvictionMaps();
				}
			}
			finally
			{
				Lock.Unlock();
			}
		}

		/// <summary>Demote old evictable mmaps into the regular eviction map.</summary>
		/// <remarks>
		/// Demote old evictable mmaps into the regular eviction map.
		/// You must hold the cache lock while calling this function.
		/// </remarks>
		/// <param name="now">Current time in monotonic milliseconds.</param>
		/// <returns>Number of replicas demoted.</returns>
		private int DemoteOldEvictableMmaped(long now)
		{
			int numDemoted = 0;
			bool needMoreSpace = false;
			long evictionTimeNs = Sharpen.Extensions.ValueOf(0);
			while (true)
			{
				KeyValuePair<long, ShortCircuitReplica> entry = evictableMmapped.CeilingEntry(evictionTimeNs
					);
				if (entry == null)
				{
					break;
				}
				evictionTimeNs = entry.Key;
				long evictionTimeMs = TimeUnit.Milliseconds.Convert(evictionTimeNs, TimeUnit.Nanoseconds
					);
				if (evictionTimeMs + maxEvictableMmapedLifespanMs >= now)
				{
					if (evictableMmapped.Count < maxEvictableMmapedSize)
					{
						break;
					}
					needMoreSpace = true;
				}
				ShortCircuitReplica replica = entry.Value;
				if (Log.IsTraceEnabled())
				{
					string rationale = needMoreSpace ? "because we need more space" : "because it's too old";
					Log.Trace("demoteOldEvictable: demoting " + replica + ": " + rationale + ": " + StringUtils
						.GetStackTrace(Sharpen.Thread.CurrentThread()));
				}
				RemoveEvictable(replica, evictableMmapped);
				Munmap(replica);
				InsertEvictable(evictionTimeNs, replica, evictable);
				numDemoted++;
			}
			return numDemoted;
		}

		/// <summary>Trim the eviction lists.</summary>
		private void TrimEvictionMaps()
		{
			long now = Time.MonotonicNow();
			DemoteOldEvictableMmaped(now);
			while (true)
			{
				long evictableSize = evictable.Count;
				long evictableMmappedSize = evictableMmapped.Count;
				if (evictableSize + evictableMmappedSize <= maxTotalSize)
				{
					return;
				}
				ShortCircuitReplica replica;
				if (evictableSize == 0)
				{
					replica = evictableMmapped.FirstEntry().Value;
				}
				else
				{
					replica = evictable.FirstEntry().Value;
				}
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": trimEvictionMaps is purging " + replica + StringUtils.GetStackTrace
						(Sharpen.Thread.CurrentThread()));
				}
				Purge(replica);
			}
		}

		/// <summary>Munmap a replica, updating outstandingMmapCount.</summary>
		/// <param name="replica">The replica to munmap.</param>
		private void Munmap(ShortCircuitReplica replica)
		{
			replica.Munmap();
			outstandingMmapCount--;
		}

		/// <summary>Remove a replica from an evictable map.</summary>
		/// <param name="replica">The replica to remove.</param>
		/// <returns>The map it was removed from.</returns>
		private string RemoveEvictable(ShortCircuitReplica replica)
		{
			if (replica.HasMmap())
			{
				RemoveEvictable(replica, evictableMmapped);
				return "evictableMmapped";
			}
			else
			{
				RemoveEvictable(replica, evictable);
				return "evictable";
			}
		}

		/// <summary>Remove a replica from an evictable map.</summary>
		/// <param name="replica">The replica to remove.</param>
		/// <param name="map">The map to remove it from.</param>
		private void RemoveEvictable(ShortCircuitReplica replica, SortedDictionary<long, 
			ShortCircuitReplica> map)
		{
			long evictableTimeNs = replica.GetEvictableTimeNs();
			Preconditions.CheckNotNull(evictableTimeNs);
			ShortCircuitReplica removed = Sharpen.Collections.Remove(map, evictableTimeNs);
			Preconditions.CheckState(removed == replica, "failed to make %s unevictable", replica
				);
			replica.SetEvictableTimeNs(null);
		}

		/// <summary>Insert a replica into an evictable map.</summary>
		/// <remarks>
		/// Insert a replica into an evictable map.
		/// If an element already exists with this eviction time, we add a nanosecond
		/// to it until we find an unused key.
		/// </remarks>
		/// <param name="evictionTimeNs">The eviction time in absolute nanoseconds.</param>
		/// <param name="replica">The replica to insert.</param>
		/// <param name="map">The map to insert it into.</param>
		private void InsertEvictable(long evictionTimeNs, ShortCircuitReplica replica, SortedDictionary
			<long, ShortCircuitReplica> map)
		{
			while (map.Contains(evictionTimeNs))
			{
				evictionTimeNs++;
			}
			Preconditions.CheckState(null == replica.GetEvictableTimeNs());
			replica.SetEvictableTimeNs(evictionTimeNs);
			map[evictionTimeNs] = replica;
		}

		/// <summary>Purge a replica from the cache.</summary>
		/// <remarks>
		/// Purge a replica from the cache.
		/// This doesn't necessarily close the replica, since there may be
		/// outstanding references to it.  However, it does mean the cache won't
		/// hand it out to anyone after this.
		/// You must hold the cache lock while calling this function.
		/// </remarks>
		/// <param name="replica">The replica being removed.</param>
		private void Purge(ShortCircuitReplica replica)
		{
			bool removedFromInfoMap = false;
			string evictionMapName = null;
			Preconditions.CheckArgument(!replica.purged);
			replica.purged = true;
			Waitable<ShortCircuitReplicaInfo> val = replicaInfoMap[replica.key];
			if (val != null)
			{
				ShortCircuitReplicaInfo info = val.GetVal();
				if ((info != null) && (info.GetReplica() == replica))
				{
					Sharpen.Collections.Remove(replicaInfoMap, replica.key);
					removedFromInfoMap = true;
				}
			}
			long evictableTimeNs = replica.GetEvictableTimeNs();
			if (evictableTimeNs != null)
			{
				evictionMapName = RemoveEvictable(replica);
			}
			if (Log.IsTraceEnabled())
			{
				StringBuilder builder = new StringBuilder();
				builder.Append(this).Append(": ").Append(": purged ").Append(replica).Append(" from the cache."
					);
				if (removedFromInfoMap)
				{
					builder.Append("  Removed from the replicaInfoMap.");
				}
				if (evictionMapName != null)
				{
					builder.Append("  Removed from ").Append(evictionMapName);
				}
				Log.Trace(builder.ToString());
			}
			Unref(replica);
		}

		/// <summary>Fetch or create a replica.</summary>
		/// <remarks>
		/// Fetch or create a replica.
		/// You must hold the cache lock while calling this function.
		/// </remarks>
		/// <param name="key">Key to use for lookup.</param>
		/// <param name="creator">
		/// Replica creator callback.  Will be called without
		/// the cache lock being held.
		/// </param>
		/// <returns>
		/// Null if no replica could be found or created.
		/// The replica, otherwise.
		/// </returns>
		public virtual ShortCircuitReplicaInfo FetchOrCreate(ExtendedBlockId key, ShortCircuitCache.ShortCircuitReplicaCreator
			 creator)
		{
			Waitable<ShortCircuitReplicaInfo> newWaitable = null;
			Lock.Lock();
			try
			{
				ShortCircuitReplicaInfo info = null;
				do
				{
					if (closed)
					{
						if (Log.IsTraceEnabled())
						{
							Log.Trace(this + ": can't fetchOrCreate " + key + " because the cache is closed."
								);
						}
						return null;
					}
					Waitable<ShortCircuitReplicaInfo> waitable = replicaInfoMap[key];
					if (waitable != null)
					{
						try
						{
							info = Fetch(key, waitable);
						}
						catch (RetriableException e)
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug(this + ": retrying " + e.Message);
							}
							continue;
						}
					}
				}
				while (false);
				if (info != null)
				{
					return info;
				}
				// We need to load the replica ourselves.
				newWaitable = new Waitable<ShortCircuitReplicaInfo>(Lock.NewCondition());
				replicaInfoMap[key] = newWaitable;
			}
			finally
			{
				Lock.Unlock();
			}
			return Create(key, creator, newWaitable);
		}

		/// <summary>Fetch an existing ReplicaInfo object.</summary>
		/// <param name="key">The key that we're using.</param>
		/// <param name="waitable">The waitable object to wait on.</param>
		/// <returns>
		/// The existing ReplicaInfo object, or null if there is
		/// none.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Ipc.RetriableException">If the caller needs to retry.
		/// 	</exception>
		private ShortCircuitReplicaInfo Fetch(ExtendedBlockId key, Waitable<ShortCircuitReplicaInfo
			> waitable)
		{
			// Another thread is already in the process of loading this
			// ShortCircuitReplica.  So we simply wait for it to complete.
			ShortCircuitReplicaInfo info;
			try
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": found waitable for " + key);
				}
				info = waitable.Await();
			}
			catch (Exception)
			{
				Log.Info(this + ": interrupted while waiting for " + key);
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new RetriableException("interrupted");
			}
			if (info.GetInvalidTokenException() != null)
			{
				Log.Info(this + ": could not get " + key + " due to InvalidToken " + "exception."
					, info.GetInvalidTokenException());
				return info;
			}
			ShortCircuitReplica replica = info.GetReplica();
			if (replica == null)
			{
				Log.Warn(this + ": failed to get " + key);
				return info;
			}
			if (replica.purged)
			{
				// Ignore replicas that have already been purged from the cache.
				throw new RetriableException("Ignoring purged replica " + replica + ".  Retrying."
					);
			}
			// Check if the replica is stale before using it.
			// If it is, purge it and retry.
			if (replica.IsStale())
			{
				Log.Info(this + ": got stale replica " + replica + ".  Removing " + "this replica from the replicaInfoMap and retrying."
					);
				// Remove the cache's reference to the replica.  This may or may not
				// trigger a close.
				Purge(replica);
				throw new RetriableException("ignoring stale replica " + replica);
			}
			Ref(replica);
			return info;
		}

		private ShortCircuitReplicaInfo Create(ExtendedBlockId key, ShortCircuitCache.ShortCircuitReplicaCreator
			 creator, Waitable<ShortCircuitReplicaInfo> newWaitable)
		{
			// Handle loading a new replica.
			ShortCircuitReplicaInfo info = null;
			try
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": loading " + key);
				}
				info = creator.CreateShortCircuitReplicaInfo();
			}
			catch (RuntimeException e)
			{
				Log.Warn(this + ": failed to load " + key, e);
			}
			if (info == null)
			{
				info = new ShortCircuitReplicaInfo();
			}
			Lock.Lock();
			try
			{
				if (info.GetReplica() != null)
				{
					// On success, make sure the cache cleaner thread is running.
					if (Log.IsTraceEnabled())
					{
						Log.Trace(this + ": successfully loaded " + info.GetReplica());
					}
					StartCacheCleanerThreadIfNeeded();
				}
				else
				{
					// Note: new ShortCircuitReplicas start with a refCount of 2,
					// indicating that both this cache and whoever requested the 
					// creation of the replica hold a reference.  So we don't need
					// to increment the reference count here.
					// On failure, remove the waitable from the replicaInfoMap.
					Waitable<ShortCircuitReplicaInfo> waitableInMap = replicaInfoMap[key];
					if (waitableInMap == newWaitable)
					{
						Sharpen.Collections.Remove(replicaInfoMap, key);
					}
					if (info.GetInvalidTokenException() != null)
					{
						Log.Info(this + ": could not load " + key + " due to InvalidToken " + "exception."
							, info.GetInvalidTokenException());
					}
					else
					{
						Log.Warn(this + ": failed to load " + key);
					}
				}
				newWaitable.Provide(info);
			}
			finally
			{
				Lock.Unlock();
			}
			return info;
		}

		private void StartCacheCleanerThreadIfNeeded()
		{
			if (cacheCleaner == null)
			{
				cacheCleaner = new ShortCircuitCache.CacheCleaner(this);
				long rateMs = cacheCleaner.GetRateInMs();
				ScheduledFuture<object> future = cleanerExecutor.ScheduleAtFixedRate(cacheCleaner
					, rateMs, rateMs, TimeUnit.Milliseconds);
				cacheCleaner.SetFuture(future);
				if (Log.IsDebugEnabled())
				{
					Log.Debug(this + ": starting cache cleaner thread which will run " + "every " + rateMs
						 + " ms");
				}
			}
		}

		internal virtual ClientMmap GetOrCreateClientMmap(ShortCircuitReplica replica, bool
			 anchored)
		{
			Condition newCond;
			Lock.Lock();
			try
			{
				while (replica.mmapData != null)
				{
					if (replica.mmapData is MappedByteBuffer)
					{
						Ref(replica);
						MappedByteBuffer mmap = (MappedByteBuffer)replica.mmapData;
						return new ClientMmap(replica, mmap, anchored);
					}
					else
					{
						if (replica.mmapData is long)
						{
							long lastAttemptTimeMs = (long)replica.mmapData;
							long delta = Time.MonotonicNow() - lastAttemptTimeMs;
							if (delta < mmapRetryTimeoutMs)
							{
								if (Log.IsTraceEnabled())
								{
									Log.Trace(this + ": can't create client mmap for " + replica + " because we failed to "
										 + "create one just " + delta + "ms ago.");
								}
								return null;
							}
							if (Log.IsTraceEnabled())
							{
								Log.Trace(this + ": retrying client mmap for " + replica + ", " + delta + " ms after the previous failure."
									);
							}
						}
						else
						{
							if (replica.mmapData is Condition)
							{
								Condition cond = (Condition)replica.mmapData;
								cond.AwaitUninterruptibly();
							}
							else
							{
								Preconditions.CheckState(false, "invalid mmapData type %s", replica.mmapData.GetType
									().FullName);
							}
						}
					}
				}
				newCond = Lock.NewCondition();
				replica.mmapData = newCond;
			}
			finally
			{
				Lock.Unlock();
			}
			MappedByteBuffer map = replica.LoadMmapInternal();
			Lock.Lock();
			try
			{
				if (map == null)
				{
					replica.mmapData = Sharpen.Extensions.ValueOf(Time.MonotonicNow());
					newCond.SignalAll();
					return null;
				}
				else
				{
					outstandingMmapCount++;
					replica.mmapData = map;
					Ref(replica);
					newCond.SignalAll();
					return new ClientMmap(replica, map, anchored);
				}
			}
			finally
			{
				Lock.Unlock();
			}
		}

		/// <summary>Close the cache and free all associated resources.</summary>
		public virtual void Close()
		{
			try
			{
				Lock.Lock();
				if (closed)
				{
					return;
				}
				closed = true;
				Log.Info(this + ": closing");
				maxNonMmappedEvictableLifespanMs = 0;
				maxEvictableMmapedSize = 0;
				// Close and join cacheCleaner thread.
				IOUtils.Cleanup(Log, cacheCleaner);
				// Purge all replicas.
				while (true)
				{
					KeyValuePair<long, ShortCircuitReplica> entry = evictable.FirstEntry();
					if (entry == null)
					{
						break;
					}
					Purge(entry.Value);
				}
				while (true)
				{
					KeyValuePair<long, ShortCircuitReplica> entry = evictableMmapped.FirstEntry();
					if (entry == null)
					{
						break;
					}
					Purge(entry.Value);
				}
			}
			finally
			{
				Lock.Unlock();
			}
			IOUtils.Cleanup(Log, shmManager);
		}

		public interface CacheVisitor
		{
			// ONLY for testing
			void Visit(int numOutstandingMmaps, IDictionary<ExtendedBlockId, ShortCircuitReplica
				> replicas, IDictionary<ExtendedBlockId, SecretManager.InvalidToken> failedLoads
				, IDictionary<long, ShortCircuitReplica> evictable, IDictionary<long, ShortCircuitReplica
				> evictableMmapped);
		}

		[VisibleForTesting]
		public virtual void Accept(ShortCircuitCache.CacheVisitor visitor)
		{
			// ONLY for testing
			Lock.Lock();
			try
			{
				IDictionary<ExtendedBlockId, ShortCircuitReplica> replicas = new Dictionary<ExtendedBlockId
					, ShortCircuitReplica>();
				IDictionary<ExtendedBlockId, SecretManager.InvalidToken> failedLoads = new Dictionary
					<ExtendedBlockId, SecretManager.InvalidToken>();
				foreach (KeyValuePair<ExtendedBlockId, Waitable<ShortCircuitReplicaInfo>> entry in 
					replicaInfoMap)
				{
					Waitable<ShortCircuitReplicaInfo> waitable = entry.Value;
					if (waitable.HasVal())
					{
						if (waitable.GetVal().GetReplica() != null)
						{
							replicas[entry.Key] = waitable.GetVal().GetReplica();
						}
						else
						{
							// The exception may be null here, indicating a failed load that
							// isn't the result of an invalid block token.
							failedLoads[entry.Key] = waitable.GetVal().GetInvalidTokenException();
						}
					}
				}
				if (Log.IsDebugEnabled())
				{
					StringBuilder builder = new StringBuilder();
					builder.Append("visiting ").Append(visitor.GetType().FullName).Append("with outstandingMmapCount="
						).Append(outstandingMmapCount).Append(", replicas=");
					string prefix = string.Empty;
					foreach (KeyValuePair<ExtendedBlockId, ShortCircuitReplica> entry_1 in replicas)
					{
						builder.Append(prefix).Append(entry_1.Value);
						prefix = ",";
					}
					prefix = string.Empty;
					builder.Append(", failedLoads=");
					foreach (KeyValuePair<ExtendedBlockId, SecretManager.InvalidToken> entry_2 in failedLoads)
					{
						builder.Append(prefix).Append(entry_2.Value);
						prefix = ",";
					}
					prefix = string.Empty;
					builder.Append(", evictable=");
					foreach (KeyValuePair<long, ShortCircuitReplica> entry_3 in evictable)
					{
						builder.Append(prefix).Append(entry_3.Key).Append(":").Append(entry_3.Value);
						prefix = ",";
					}
					prefix = string.Empty;
					builder.Append(", evictableMmapped=");
					foreach (KeyValuePair<long, ShortCircuitReplica> entry_4 in evictableMmapped)
					{
						builder.Append(prefix).Append(entry_4.Key).Append(":").Append(entry_4.Value);
						prefix = ",";
					}
					Log.Debug(builder.ToString());
				}
				visitor.Visit(outstandingMmapCount, replicas, failedLoads, evictable, evictableMmapped
					);
			}
			finally
			{
				Lock.Unlock();
			}
		}

		public override string ToString()
		{
			return "ShortCircuitCache(0x" + Sharpen.Extensions.ToHexString(Runtime.IdentityHashCode
				(this)) + ")";
		}

		/// <summary>Allocate a new shared memory slot.</summary>
		/// <param name="datanode">The datanode to allocate a shm slot with.</param>
		/// <param name="peer">A peer connected to the datanode.</param>
		/// <param name="usedPeer">Will be set to true if we use up the provided peer.</param>
		/// <param name="blockId">
		/// The block id and block pool id of the block we're
		/// allocating this slot for.
		/// </param>
		/// <param name="clientName">
		/// The name of the DFSClient allocating the shared
		/// memory.
		/// </param>
		/// <returns>
		/// Null if short-circuit shared memory is disabled;
		/// a short-circuit memory slot otherwise.
		/// </returns>
		/// <exception cref="System.IO.IOException">
		/// An exception if there was an error talking to
		/// the datanode.
		/// </exception>
		public virtual ShortCircuitShm.Slot AllocShmSlot(DatanodeInfo datanode, DomainPeer
			 peer, MutableBoolean usedPeer, ExtendedBlockId blockId, string clientName)
		{
			if (shmManager != null)
			{
				return shmManager.AllocSlot(datanode, peer, usedPeer, blockId, clientName);
			}
			else
			{
				return null;
			}
		}

		/// <summary>Free a slot immediately.</summary>
		/// <remarks>
		/// Free a slot immediately.
		/// ONLY use this if the DataNode is not yet aware of the slot.
		/// </remarks>
		/// <param name="slot">The slot to free.</param>
		public virtual void FreeSlot(ShortCircuitShm.Slot slot)
		{
			Preconditions.CheckState(shmManager != null);
			slot.MakeInvalid();
			shmManager.FreeSlot(slot);
		}

		/// <summary>Schedule a shared memory slot to be released.</summary>
		/// <param name="slot">The slot to release.</param>
		public virtual void ScheduleSlotReleaser(ShortCircuitShm.Slot slot)
		{
			Preconditions.CheckState(shmManager != null);
			releaserExecutor.Execute(new ShortCircuitCache.SlotReleaser(this, slot));
		}

		[VisibleForTesting]
		public virtual DfsClientShmManager GetDfsClientShmManager()
		{
			return shmManager;
		}
	}
}
