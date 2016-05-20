using Sharpen;

namespace org.apache.hadoop.ipc
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
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.RetryCache)
			));

		private readonly org.apache.hadoop.ipc.metrics.RetryCacheMetrics retryCacheMetrics;

		/// <summary>CacheEntry is tracked using unique client ID and callId of the RPC request
		/// 	</summary>
		public class CacheEntry : org.apache.hadoop.util.LightWeightCache.Entry
		{
			/// <summary>Processing state of the requests</summary>
			private static byte INPROGRESS = 0;

			private static byte SUCCESS = 1;

			private static byte FAILED = 2;

			private byte state = INPROGRESS;

			private readonly long clientIdMsb;

			private readonly long clientIdLsb;

			private readonly int callId;

			private readonly long expirationTime;

			private org.apache.hadoop.util.LightWeightGSet.LinkedElement next;

			internal CacheEntry(byte[] clientId, int callId, long expirationTime)
			{
				// Store uuid as two long for better memory utilization
				// Most signficant bytes
				// Least significant bytes
				// ClientId must be a UUID - that is 16 octets.
				com.google.common.@base.Preconditions.checkArgument(clientId.Length == org.apache.hadoop.ipc.ClientId
					.BYTE_LENGTH, "Invalid clientId - length is " + clientId.Length + " expected length "
					 + org.apache.hadoop.ipc.ClientId.BYTE_LENGTH);
				// Convert UUID bytes to two longs
				clientIdMsb = org.apache.hadoop.ipc.ClientId.getMsb(clientId);
				clientIdLsb = org.apache.hadoop.ipc.ClientId.getLsb(clientId);
				this.callId = callId;
				this.expirationTime = expirationTime;
			}

			internal CacheEntry(byte[] clientId, int callId, long expirationTime, bool success
				)
				: this(clientId, callId, expirationTime)
			{
				this.state = success ? SUCCESS : FAILED;
			}

			private static int hashCode(long value)
			{
				return (int)(value ^ ((long)(((ulong)value) >> 32)));
			}

			public override int GetHashCode()
			{
				return (hashCode(clientIdMsb) * 31 + hashCode(clientIdLsb)) * 31 + callId;
			}

			public override bool Equals(object obj)
			{
				if (this == obj)
				{
					return true;
				}
				if (!(obj is org.apache.hadoop.ipc.RetryCache.CacheEntry))
				{
					return false;
				}
				org.apache.hadoop.ipc.RetryCache.CacheEntry other = (org.apache.hadoop.ipc.RetryCache.CacheEntry
					)obj;
				return callId == other.callId && clientIdMsb == other.clientIdMsb && clientIdLsb 
					== other.clientIdLsb;
			}

			public virtual void setNext(org.apache.hadoop.util.LightWeightGSet.LinkedElement 
				next)
			{
				this.next = next;
			}

			public virtual org.apache.hadoop.util.LightWeightGSet.LinkedElement getNext()
			{
				return next;
			}

			internal virtual void completed(bool success)
			{
				lock (this)
				{
					state = success ? SUCCESS : FAILED;
					Sharpen.Runtime.notifyAll(this);
				}
			}

			public virtual bool isSuccess()
			{
				lock (this)
				{
					return state == SUCCESS;
				}
			}

			public virtual void setExpirationTime(long timeNano)
			{
			}

			// expiration time does not change
			public virtual long getExpirationTime()
			{
				return expirationTime;
			}

			public override string ToString()
			{
				return (new java.util.UUID(this.clientIdMsb, this.clientIdLsb)).ToString() + ":" 
					+ this.callId + ":" + this.state;
			}
		}

		/// <summary>
		/// CacheEntry with payload that tracks the previous response or parts of
		/// previous response to be used for generating response for retried requests.
		/// </summary>
		public class CacheEntryWithPayload : org.apache.hadoop.ipc.RetryCache.CacheEntry
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

			public virtual object getPayload()
			{
				return payload;
			}
		}

		private readonly org.apache.hadoop.util.LightWeightGSet<org.apache.hadoop.ipc.RetryCache.CacheEntry
			, org.apache.hadoop.ipc.RetryCache.CacheEntry> set;

		private readonly long expirationTime;

		private string cacheName;

		private readonly java.util.concurrent.locks.ReentrantLock Lock = new java.util.concurrent.locks.ReentrantLock
			();

		/// <summary>Constructor</summary>
		/// <param name="cacheName">name to identify the cache by</param>
		/// <param name="percentage">percentage of total java heap space used by this cache</param>
		/// <param name="expirationTime">time for an entry to expire in nanoseconds</param>
		public RetryCache(string cacheName, double percentage, long expirationTime)
		{
			int capacity = org.apache.hadoop.util.LightWeightGSet.computeCapacity(percentage, 
				cacheName);
			capacity = capacity > 16 ? capacity : 16;
			this.set = new org.apache.hadoop.util.LightWeightCache<org.apache.hadoop.ipc.RetryCache.CacheEntry
				, org.apache.hadoop.ipc.RetryCache.CacheEntry>(capacity, capacity, expirationTime
				, 0);
			this.expirationTime = expirationTime;
			this.cacheName = cacheName;
			this.retryCacheMetrics = org.apache.hadoop.ipc.metrics.RetryCacheMetrics.create(this
				);
		}

		private static bool skipRetryCache()
		{
			// Do not track non RPC invocation or RPC requests with
			// invalid callId or clientId in retry cache
			return !org.apache.hadoop.ipc.Server.isRpcInvocation() || org.apache.hadoop.ipc.Server
				.getCallId() < 0 || java.util.Arrays.equals(org.apache.hadoop.ipc.Server.getClientId
				(), org.apache.hadoop.ipc.RpcConstants.DUMMY_CLIENT_ID);
		}

		public virtual void Lock()
		{
			this.Lock.Lock();
		}

		public virtual void unlock()
		{
			this.Lock.unlock();
		}

		private void incrCacheClearedCounter()
		{
			retryCacheMetrics.incrCacheCleared();
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.util.LightWeightGSet<org.apache.hadoop.ipc.RetryCache.CacheEntry
			, org.apache.hadoop.ipc.RetryCache.CacheEntry> getCacheSet()
		{
			return set;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.ipc.metrics.RetryCacheMetrics getMetricsForTests
			()
		{
			return retryCacheMetrics;
		}

		/// <summary>This method returns cache name for metrics.</summary>
		public virtual string getCacheName()
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
		/// <see cref="CacheEntry.FAILED"/>
		/// , the expectation is that the
		/// thread that waited for completion, retries the request. the
		/// <see cref="CacheEntry"/>
		/// state is set to
		/// <see cref="CacheEntry.INPROGRESS"/>
		/// again.
		/// <li>If the completion state is
		/// <see cref="CacheEntry.SUCCESS"/>
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
		private org.apache.hadoop.ipc.RetryCache.CacheEntry waitForCompletion(org.apache.hadoop.ipc.RetryCache.CacheEntry
			 newEntry)
		{
			org.apache.hadoop.ipc.RetryCache.CacheEntry mapEntry = null;
			Lock.Lock();
			try
			{
				mapEntry = set.get(newEntry);
				// If an entry in the cache does not exist, add a new one
				if (mapEntry == null)
				{
					if (LOG.isTraceEnabled())
					{
						LOG.trace("Adding Rpc request clientId " + newEntry.clientIdMsb + newEntry.clientIdLsb
							 + " callId " + newEntry.callId + " to retryCache");
					}
					set.put(newEntry);
					retryCacheMetrics.incrCacheUpdated();
					return newEntry;
				}
				else
				{
					retryCacheMetrics.incrCacheHit();
				}
			}
			finally
			{
				Lock.unlock();
			}
			// Entry already exists in cache. Wait for completion and return its state
			com.google.common.@base.Preconditions.checkNotNull(mapEntry, "Entry from the cache should not be null"
				);
			// Wait for in progress request to complete
			lock (mapEntry)
			{
				while (mapEntry.state == org.apache.hadoop.ipc.RetryCache.CacheEntry.INPROGRESS)
				{
					try
					{
						Sharpen.Runtime.wait(mapEntry);
					}
					catch (System.Exception)
					{
						// Restore the interrupted status
						java.lang.Thread.currentThread().interrupt();
					}
				}
				// Previous request has failed, the expectation is is that it will be
				// retried again.
				if (mapEntry.state != org.apache.hadoop.ipc.RetryCache.CacheEntry.SUCCESS)
				{
					mapEntry.state = org.apache.hadoop.ipc.RetryCache.CacheEntry.INPROGRESS;
				}
			}
			return mapEntry;
		}

		/// <summary>Add a new cache entry into the retry cache.</summary>
		/// <remarks>
		/// Add a new cache entry into the retry cache. The cache entry consists of
		/// clientId and callId extracted from editlog.
		/// </remarks>
		public virtual void addCacheEntry(byte[] clientId, int callId)
		{
			org.apache.hadoop.ipc.RetryCache.CacheEntry newEntry = new org.apache.hadoop.ipc.RetryCache.CacheEntry
				(clientId, callId, Sharpen.Runtime.nanoTime() + expirationTime, true);
			Lock.Lock();
			try
			{
				set.put(newEntry);
			}
			finally
			{
				Lock.unlock();
			}
			retryCacheMetrics.incrCacheUpdated();
		}

		public virtual void addCacheEntryWithPayload(byte[] clientId, int callId, object 
			payload)
		{
			// since the entry is loaded from editlog, we can assume it succeeded.    
			org.apache.hadoop.ipc.RetryCache.CacheEntry newEntry = new org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload
				(clientId, callId, payload, Sharpen.Runtime.nanoTime() + expirationTime, true);
			Lock.Lock();
			try
			{
				set.put(newEntry);
			}
			finally
			{
				Lock.unlock();
			}
			retryCacheMetrics.incrCacheUpdated();
		}

		private static org.apache.hadoop.ipc.RetryCache.CacheEntry newEntry(long expirationTime
			)
		{
			return new org.apache.hadoop.ipc.RetryCache.CacheEntry(org.apache.hadoop.ipc.Server
				.getClientId(), org.apache.hadoop.ipc.Server.getCallId(), Sharpen.Runtime.nanoTime
				() + expirationTime);
		}

		private static org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload newEntry(object
			 payload, long expirationTime)
		{
			return new org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload(org.apache.hadoop.ipc.Server
				.getClientId(), org.apache.hadoop.ipc.Server.getCallId(), payload, Sharpen.Runtime
				.nanoTime() + expirationTime);
		}

		/// <summary>Static method that provides null check for retryCache</summary>
		public static org.apache.hadoop.ipc.RetryCache.CacheEntry waitForCompletion(org.apache.hadoop.ipc.RetryCache
			 cache)
		{
			if (skipRetryCache())
			{
				return null;
			}
			return cache != null ? cache.waitForCompletion(newEntry(cache.expirationTime)) : 
				null;
		}

		/// <summary>Static method that provides null check for retryCache</summary>
		public static org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload waitForCompletion
			(org.apache.hadoop.ipc.RetryCache cache, object payload)
		{
			if (skipRetryCache())
			{
				return null;
			}
			return (org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload)(cache != null ? cache
				.waitForCompletion(newEntry(payload, cache.expirationTime)) : null);
		}

		public static void setState(org.apache.hadoop.ipc.RetryCache.CacheEntry e, bool success
			)
		{
			if (e == null)
			{
				return;
			}
			e.completed(success);
		}

		public static void setState(org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload
			 e, bool success, object payload)
		{
			if (e == null)
			{
				return;
			}
			e.payload = payload;
			e.completed(success);
		}

		public static void clear(org.apache.hadoop.ipc.RetryCache cache)
		{
			if (cache != null)
			{
				cache.set.clear();
				cache.incrCacheClearedCounter();
			}
		}
	}
}
