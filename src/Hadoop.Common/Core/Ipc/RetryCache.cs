using System;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Ipc.Metrics;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// Maintains a cache of non-idempotent requests that have been successfully
	/// processed by the RPC server implementation, to handle the retries.
	/// </summary>
	/// <remarks>
	/// Maintains a cache of non-idempotent requests that have been successfully
	/// processed by the RPC server implementation, to handle the retries. A request
	/// is uniquely identified by the unique client ID + call ID of the RPC request.
	/// On receiving retried request, an entry will be found in the
	/// <see cref="RetryCache"/>
	/// and the previous response is sent back to the request.
	/// <p>
	/// To look an implementation using this cache, see HDFS FSNamesystem class.
	/// </remarks>
	public class RetryCache
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.RetryCache
			));

		private readonly RetryCacheMetrics retryCacheMetrics;

		/// <summary>CacheEntry is tracked using unique client ID and callId of the RPC request
		/// 	</summary>
		public class CacheEntry : LightWeightCache.Entry
		{
			/// <summary>Processing state of the requests</summary>
			private static byte Inprogress = 0;

			private static byte Success = 1;

			private static byte Failed = 2;

			private byte state = Inprogress;

			private readonly long clientIdMsb;

			private readonly long clientIdLsb;

			private readonly int callId;

			private readonly long expirationTime;

			private LightWeightGSet.LinkedElement next;

			internal CacheEntry(byte[] clientId, int callId, long expirationTime)
			{
				// Store uuid as two long for better memory utilization
				// Most signficant bytes
				// Least significant bytes
				// ClientId must be a UUID - that is 16 octets.
				Preconditions.CheckArgument(clientId.Length == ClientId.ByteLength, "Invalid clientId - length is "
					 + clientId.Length + " expected length " + ClientId.ByteLength);
				// Convert UUID bytes to two longs
				clientIdMsb = ClientId.GetMsb(clientId);
				clientIdLsb = ClientId.GetLsb(clientId);
				this.callId = callId;
				this.expirationTime = expirationTime;
			}

			internal CacheEntry(byte[] clientId, int callId, long expirationTime, bool success
				)
				: this(clientId, callId, expirationTime)
			{
				this.state = success ? Success : Failed;
			}

			private static int HashCode(long value)
			{
				return (int)(value ^ ((long)(((ulong)value) >> 32)));
			}

			public override int GetHashCode()
			{
				return (HashCode(clientIdMsb) * 31 + HashCode(clientIdLsb)) * 31 + callId;
			}

			public override bool Equals(object obj)
			{
				if (this == obj)
				{
					return true;
				}
				if (!(obj is RetryCache.CacheEntry))
				{
					return false;
				}
				RetryCache.CacheEntry other = (RetryCache.CacheEntry)obj;
				return callId == other.callId && clientIdMsb == other.clientIdMsb && clientIdLsb 
					== other.clientIdLsb;
			}

			public virtual void SetNext(LightWeightGSet.LinkedElement next)
			{
				this.next = next;
			}

			public virtual LightWeightGSet.LinkedElement GetNext()
			{
				return next;
			}

			internal virtual void Completed(bool success)
			{
				lock (this)
				{
					state = success ? Success : Failed;
					Runtime.NotifyAll(this);
				}
			}

			public virtual bool IsSuccess()
			{
				lock (this)
				{
					return state == Success;
				}
			}

			public virtual void SetExpirationTime(long timeNano)
			{
			}

			// expiration time does not change
			public virtual long GetExpirationTime()
			{
				return expirationTime;
			}

			public override string ToString()
			{
				return (new UUID(this.clientIdMsb, this.clientIdLsb)).ToString() + ":" + this.callId
					 + ":" + this.state;
			}
		}

		/// <summary>
		/// CacheEntry with payload that tracks the previous response or parts of
		/// previous response to be used for generating response for retried requests.
		/// </summary>
		public class CacheEntryWithPayload : RetryCache.CacheEntry
		{
			private object payload;

			internal CacheEntryWithPayload(byte[] clientId, int callId, object payload, long 
				expirationTime)
				: base(clientId, callId, expirationTime)
			{
				this.payload = payload;
			}

			internal CacheEntryWithPayload(byte[] clientId, int callId, object payload, long 
				expirationTime, bool success)
				: base(clientId, callId, expirationTime, success)
			{
				this.payload = payload;
			}

			/// <summary>Override equals to avoid findbugs warnings</summary>
			public override bool Equals(object obj)
			{
				return base.Equals(obj);
			}

			/// <summary>Override hashcode to avoid findbugs warnings</summary>
			public override int GetHashCode()
			{
				return base.GetHashCode();
			}

			public virtual object GetPayload()
			{
				return payload;
			}
		}

		private readonly LightWeightGSet<RetryCache.CacheEntry, RetryCache.CacheEntry> set;

		private readonly long expirationTime;

		private string cacheName;

		private readonly ReentrantLock Lock = new ReentrantLock();

		/// <summary>Constructor</summary>
		/// <param name="cacheName">name to identify the cache by</param>
		/// <param name="percentage">percentage of total java heap space used by this cache</param>
		/// <param name="expirationTime">time for an entry to expire in nanoseconds</param>
		public RetryCache(string cacheName, double percentage, long expirationTime)
		{
			int capacity = LightWeightGSet.ComputeCapacity(percentage, cacheName);
			capacity = capacity > 16 ? capacity : 16;
			this.set = new LightWeightCache<RetryCache.CacheEntry, RetryCache.CacheEntry>(capacity
				, capacity, expirationTime, 0);
			this.expirationTime = expirationTime;
			this.cacheName = cacheName;
			this.retryCacheMetrics = RetryCacheMetrics.Create(this);
		}

		private static bool SkipRetryCache()
		{
			// Do not track non RPC invocation or RPC requests with
			// invalid callId or clientId in retry cache
			return !Server.IsRpcInvocation() || Server.GetCallId() < 0 || Arrays.Equals(Server
				.GetClientId(), RpcConstants.DummyClientId);
		}

		public virtual void Lock()
		{
			this.Lock.Lock();
		}

		public virtual void Unlock()
		{
			this.Lock.Unlock();
		}

		private void IncrCacheClearedCounter()
		{
			retryCacheMetrics.IncrCacheCleared();
		}

		[VisibleForTesting]
		public virtual LightWeightGSet<RetryCache.CacheEntry, RetryCache.CacheEntry> GetCacheSet
			()
		{
			return set;
		}

		[VisibleForTesting]
		public virtual RetryCacheMetrics GetMetricsForTests()
		{
			return retryCacheMetrics;
		}

		/// <summary>This method returns cache name for metrics.</summary>
		public virtual string GetCacheName()
		{
			return cacheName;
		}

		/// <summary>
		/// This method handles the following conditions:
		/// <ul>
		/// <li>If retry is not to be processed, return null</li>
		/// <li>If there is no cache entry, add a new entry
		/// <paramref name="newEntry"/>
		/// and return
		/// it.</li>
		/// <li>If there is an existing entry, wait for its completion. If the
		/// completion state is
		/// <see cref="CacheEntry.Failed"/>
		/// , the expectation is that the
		/// thread that waited for completion, retries the request. the
		/// <see cref="CacheEntry"/>
		/// state is set to
		/// <see cref="CacheEntry.Inprogress"/>
		/// again.
		/// <li>If the completion state is
		/// <see cref="CacheEntry.Success"/>
		/// , the entry is
		/// returned so that the thread that waits for it can can return previous
		/// response.</li>
		/// <ul>
		/// </summary>
		/// <returns>
		/// 
		/// <see cref="CacheEntry"/>
		/// .
		/// </returns>
		private RetryCache.CacheEntry WaitForCompletion(RetryCache.CacheEntry newEntry)
		{
			RetryCache.CacheEntry mapEntry = null;
			Lock.Lock();
			try
			{
				mapEntry = set.Get(newEntry);
				// If an entry in the cache does not exist, add a new one
				if (mapEntry == null)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("Adding Rpc request clientId " + newEntry.clientIdMsb + newEntry.clientIdLsb
							 + " callId " + newEntry.callId + " to retryCache");
					}
					set.Put(newEntry);
					retryCacheMetrics.IncrCacheUpdated();
					return newEntry;
				}
				else
				{
					retryCacheMetrics.IncrCacheHit();
				}
			}
			finally
			{
				Lock.Unlock();
			}
			// Entry already exists in cache. Wait for completion and return its state
			Preconditions.CheckNotNull(mapEntry, "Entry from the cache should not be null");
			// Wait for in progress request to complete
			lock (mapEntry)
			{
				while (mapEntry.state == RetryCache.CacheEntry.Inprogress)
				{
					try
					{
						Runtime.Wait(mapEntry);
					}
					catch (Exception)
					{
						// Restore the interrupted status
						Thread.CurrentThread().Interrupt();
					}
				}
				// Previous request has failed, the expectation is is that it will be
				// retried again.
				if (mapEntry.state != RetryCache.CacheEntry.Success)
				{
					mapEntry.state = RetryCache.CacheEntry.Inprogress;
				}
			}
			return mapEntry;
		}

		/// <summary>Add a new cache entry into the retry cache.</summary>
		/// <remarks>
		/// Add a new cache entry into the retry cache. The cache entry consists of
		/// clientId and callId extracted from editlog.
		/// </remarks>
		public virtual void AddCacheEntry(byte[] clientId, int callId)
		{
			RetryCache.CacheEntry newEntry = new RetryCache.CacheEntry(clientId, callId, Runtime
				.NanoTime() + expirationTime, true);
			Lock.Lock();
			try
			{
				set.Put(newEntry);
			}
			finally
			{
				Lock.Unlock();
			}
			retryCacheMetrics.IncrCacheUpdated();
		}

		public virtual void AddCacheEntryWithPayload(byte[] clientId, int callId, object 
			payload)
		{
			// since the entry is loaded from editlog, we can assume it succeeded.    
			RetryCache.CacheEntry newEntry = new RetryCache.CacheEntryWithPayload(clientId, callId
				, payload, Runtime.NanoTime() + expirationTime, true);
			Lock.Lock();
			try
			{
				set.Put(newEntry);
			}
			finally
			{
				Lock.Unlock();
			}
			retryCacheMetrics.IncrCacheUpdated();
		}

		private static RetryCache.CacheEntry NewEntry(long expirationTime)
		{
			return new RetryCache.CacheEntry(Server.GetClientId(), Server.GetCallId(), Runtime
				.NanoTime() + expirationTime);
		}

		private static RetryCache.CacheEntryWithPayload NewEntry(object payload, long expirationTime
			)
		{
			return new RetryCache.CacheEntryWithPayload(Server.GetClientId(), Server.GetCallId
				(), payload, Runtime.NanoTime() + expirationTime);
		}

		/// <summary>Static method that provides null check for retryCache</summary>
		public static RetryCache.CacheEntry WaitForCompletion(RetryCache cache)
		{
			if (SkipRetryCache())
			{
				return null;
			}
			return cache != null ? cache.WaitForCompletion(NewEntry(cache.expirationTime)) : 
				null;
		}

		/// <summary>Static method that provides null check for retryCache</summary>
		public static RetryCache.CacheEntryWithPayload WaitForCompletion(RetryCache cache
			, object payload)
		{
			if (SkipRetryCache())
			{
				return null;
			}
			return (RetryCache.CacheEntryWithPayload)(cache != null ? cache.WaitForCompletion
				(NewEntry(payload, cache.expirationTime)) : null);
		}

		public static void SetState(RetryCache.CacheEntry e, bool success)
		{
			if (e == null)
			{
				return;
			}
			e.Completed(success);
		}

		public static void SetState(RetryCache.CacheEntryWithPayload e, bool success, object
			 payload)
		{
			if (e == null)
			{
				return;
			}
			e.payload = payload;
			e.Completed(success);
		}

		public static void Clear(RetryCache cache)
		{
			if (cache != null)
			{
				cache.set.Clear();
				cache.IncrCacheClearedCounter();
			}
		}
	}
}
