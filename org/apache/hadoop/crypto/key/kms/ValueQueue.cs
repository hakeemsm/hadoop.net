using Sharpen;

namespace org.apache.hadoop.crypto.key.kms
{
	/// <summary>A Utility class that maintains a Queue of entries for a given key.</summary>
	/// <remarks>
	/// A Utility class that maintains a Queue of entries for a given key. It tries
	/// to ensure that there is are always at-least <code>numValues</code> entries
	/// available for the client to consume for a particular key.
	/// It also uses an underlying Cache to evict queues for keys that have not been
	/// accessed for a configurable period of time.
	/// Implementing classes are required to implement the
	/// <code>QueueRefiller</code> interface that exposes a method to refill the
	/// queue, when empty
	/// </remarks>
	public class ValueQueue<E>
	{
		/// <summary>QueueRefiller interface a client must implement to use this class</summary>
		public interface QueueRefiller<E>
		{
			/// <summary>
			/// Method that has to be implemented by implementing classes to fill the
			/// Queue.
			/// </summary>
			/// <param name="keyName">Key name</param>
			/// <param name="keyQueue">Queue that needs to be filled</param>
			/// <param name="numValues">number of Values to be added to the queue.</param>
			/// <exception cref="System.IO.IOException"/>
			void fillQueueForKey(string keyName, java.util.Queue<E> keyQueue, int numValues);
		}

		private static readonly string REFILL_THREAD = Sharpen.Runtime.getClassForType(typeof(
			org.apache.hadoop.crypto.key.kms.ValueQueue)).getName() + "_thread";

		private readonly com.google.common.cache.LoadingCache<string, java.util.concurrent.LinkedBlockingQueue
			<E>> keyQueues;

		private readonly java.util.concurrent.ThreadPoolExecutor executor;

		private readonly org.apache.hadoop.crypto.key.kms.ValueQueue.UniqueKeyBlockingQueue
			 queue = new org.apache.hadoop.crypto.key.kms.ValueQueue.UniqueKeyBlockingQueue(
			);

		private readonly org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller<E> refiller;

		private readonly org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy
			 policy;

		private readonly int numValues;

		private readonly float lowWatermark;

		private volatile bool executorThreadsStarted = false;

		/// <summary>A <code>Runnable</code> which takes a string name.</summary>
		private abstract class NamedRunnable : java.lang.Runnable
		{
			internal readonly string name;

			private NamedRunnable(string keyName)
			{
				this.name = keyName;
			}

			public abstract void run();
		}

		/// <summary>
		/// This backing blocking queue used in conjunction with the
		/// <code>ThreadPoolExecutor</code> used by the <code>ValueQueue</code>.
		/// </summary>
		/// <remarks>
		/// This backing blocking queue used in conjunction with the
		/// <code>ThreadPoolExecutor</code> used by the <code>ValueQueue</code>. This
		/// Queue accepts a task only if the task is not currently in the process
		/// of being run by a thread which is implied by the presence of the key
		/// in the <code>keysInProgress</code> set.
		/// NOTE: Only methods that ware explicitly called by the
		/// <code>ThreadPoolExecutor</code> need to be over-ridden.
		/// </remarks>
		[System.Serializable]
		private class UniqueKeyBlockingQueue : java.util.concurrent.LinkedBlockingQueue<java.lang.Runnable
			>
		{
			private const long serialVersionUID = -2152747693695890371L;

			private java.util.HashSet<string> keysInProgress = new java.util.HashSet<string>(
				);

			/// <exception cref="System.Exception"/>
			public override void put(java.lang.Runnable e)
			{
				lock (this)
				{
					if (keysInProgress.add(((org.apache.hadoop.crypto.key.kms.ValueQueue.NamedRunnable
						)e).name))
					{
						base.put(e);
					}
				}
			}

			/// <exception cref="System.Exception"/>
			public override java.lang.Runnable take()
			{
				java.lang.Runnable k = base.take();
				if (k != null)
				{
					keysInProgress.remove(((org.apache.hadoop.crypto.key.kms.ValueQueue.NamedRunnable
						)k).name);
				}
				return k;
			}

			/// <exception cref="System.Exception"/>
			public override java.lang.Runnable poll(long timeout, java.util.concurrent.TimeUnit
				 unit)
			{
				java.lang.Runnable k = base.poll(timeout, unit);
				if (k != null)
				{
					keysInProgress.remove(((org.apache.hadoop.crypto.key.kms.ValueQueue.NamedRunnable
						)k).name);
				}
				return k;
			}
		}

		/// <summary>
		/// Policy to decide how many values to return to client when client asks for
		/// "n" values and Queue is empty.
		/// </summary>
		/// <remarks>
		/// Policy to decide how many values to return to client when client asks for
		/// "n" values and Queue is empty.
		/// This decides how many values to return when client calls "getAtMost"
		/// </remarks>
		public enum SyncGenerationPolicy
		{
			ATLEAST_ONE,
			LOW_WATERMARK,
			ALL
		}

		/// <summary>Constructor takes the following tunable configuration parameters</summary>
		/// <param name="numValues">
		/// The number of values cached in the Queue for a
		/// particular key.
		/// </param>
		/// <param name="lowWatermark">
		/// The ratio of (number of current entries/numValues)
		/// below which the <code>fillQueueForKey()</code> funciton will be
		/// invoked to fill the Queue.
		/// </param>
		/// <param name="expiry">
		/// Expiry time after which the Key and associated Queue are
		/// evicted from the cache.
		/// </param>
		/// <param name="numFillerThreads">Number of threads to use for the filler thread</param>
		/// <param name="policy">
		/// The SyncGenerationPolicy to use when client
		/// calls "getAtMost"
		/// </param>
		/// <param name="refiller">implementation of the QueueRefiller</param>
		public ValueQueue(int numValues, float lowWatermark, long expiry, int numFillerThreads
			, org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy policy, org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller
			<E> refiller)
		{
			// Return atleast 1 value
			// Return min(n, lowWatermark * numValues) values
			// Return n values
			com.google.common.@base.Preconditions.checkArgument(numValues > 0, "\"numValues\" must be > 0"
				);
			com.google.common.@base.Preconditions.checkArgument(((lowWatermark > 0) && (lowWatermark
				 <= 1)), "\"lowWatermark\" must be > 0 and <= 1");
			com.google.common.@base.Preconditions.checkArgument(expiry > 0, "\"expiry\" must be > 0"
				);
			com.google.common.@base.Preconditions.checkArgument(numFillerThreads > 0, "\"numFillerThreads\" must be > 0"
				);
			com.google.common.@base.Preconditions.checkNotNull(policy, "\"policy\" must not be null"
				);
			this.refiller = refiller;
			this.policy = policy;
			this.numValues = numValues;
			this.lowWatermark = lowWatermark;
			keyQueues = com.google.common.cache.CacheBuilder.newBuilder().expireAfterAccess(expiry
				, java.util.concurrent.TimeUnit.MILLISECONDS).build(new _CacheLoader_175(refiller
				, lowWatermark, numValues));
			executor = new java.util.concurrent.ThreadPoolExecutor(numFillerThreads, numFillerThreads
				, 0L, java.util.concurrent.TimeUnit.MILLISECONDS, queue, new com.google.common.util.concurrent.ThreadFactoryBuilder
				().setDaemon(true).setNameFormat(REFILL_THREAD).build());
		}

		private sealed class _CacheLoader_175 : com.google.common.cache.CacheLoader<string
			, java.util.concurrent.LinkedBlockingQueue<E>>
		{
			public _CacheLoader_175(org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller
				<E> refiller, float lowWatermark, int numValues)
			{
				this.refiller = refiller;
				this.lowWatermark = lowWatermark;
				this.numValues = numValues;
			}

			/// <exception cref="System.Exception"/>
			public override java.util.concurrent.LinkedBlockingQueue<E> load(string keyName)
			{
				java.util.concurrent.LinkedBlockingQueue<E> keyQueue = new java.util.concurrent.LinkedBlockingQueue
					<E>();
				refiller.fillQueueForKey(keyName, keyQueue, (int)(lowWatermark * numValues));
				return keyQueue;
			}

			private readonly org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller<E> refiller;

			private readonly float lowWatermark;

			private readonly int numValues;
		}

		public ValueQueue(int numValues, float lowWaterMark, long expiry, int numFillerThreads
			, org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller<E> fetcher)
			: this(numValues, lowWaterMark, expiry, numFillerThreads, org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy
				.ALL, fetcher)
		{
		}

		/// <summary>
		/// Initializes the Value Queues for the provided keys by calling the
		/// fill Method with "numInitValues" values
		/// </summary>
		/// <param name="keyNames">Array of key Names</param>
		/// <exception cref="java.util.concurrent.ExecutionException"/>
		public virtual void initializeQueuesForKeys(params string[] keyNames)
		{
			foreach (string keyName in keyNames)
			{
				keyQueues.get(keyName);
			}
		}

		/// <summary>
		/// This removes the value currently at the head of the Queue for the
		/// provided key.
		/// </summary>
		/// <remarks>
		/// This removes the value currently at the head of the Queue for the
		/// provided key. Will immediately fire the Queue filler function if key
		/// does not exist.
		/// If Queue exists but all values are drained, It will ask the generator
		/// function to add 1 value to Queue and then drain it.
		/// </remarks>
		/// <param name="keyName">String key name</param>
		/// <returns>E the next value in the Queue</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.util.concurrent.ExecutionException"/>
		public virtual E getNext(string keyName)
		{
			return getAtMost(keyName, 1)[0];
		}

		/// <summary>Drains the Queue for the provided key.</summary>
		/// <param name="keyName">the key to drain the Queue for</param>
		public virtual void drain(string keyName)
		{
			try
			{
				keyQueues.get(keyName).clear();
			}
			catch (java.util.concurrent.ExecutionException)
			{
			}
		}

		//NOP
		/// <summary>Get size of the Queue for keyName</summary>
		/// <param name="keyName">the key name</param>
		/// <returns>int queue size</returns>
		/// <exception cref="java.util.concurrent.ExecutionException"/>
		public virtual int getSize(string keyName)
		{
			return keyQueues.get(keyName).Count;
		}

		/// <summary>
		/// This removes the "num" values currently at the head of the Queue for the
		/// provided key.
		/// </summary>
		/// <remarks>
		/// This removes the "num" values currently at the head of the Queue for the
		/// provided key. Will immediately fire the Queue filler function if key
		/// does not exist
		/// How many values are actually returned is governed by the
		/// <code>SyncGenerationPolicy</code> specified by the user.
		/// </remarks>
		/// <param name="keyName">String key name</param>
		/// <param name="num">Minimum number of values to return.</param>
		/// <returns>List<E> values returned</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.util.concurrent.ExecutionException"/>
		public virtual System.Collections.Generic.IList<E> getAtMost(string keyName, int 
			num)
		{
			java.util.concurrent.LinkedBlockingQueue<E> keyQueue = keyQueues.get(keyName);
			// Using poll to avoid race condition..
			System.Collections.Generic.LinkedList<E> ekvs = new System.Collections.Generic.LinkedList
				<E>();
			try
			{
				for (int i = 0; i < num; i++)
				{
					E val = keyQueue.poll();
					// If queue is empty now, Based on the provided SyncGenerationPolicy,
					// figure out how many new values need to be generated synchronously
					if (val == null)
					{
						// Synchronous call to get remaining values
						int numToFill = 0;
						switch (policy)
						{
							case org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy.ATLEAST_ONE
								:
							{
								numToFill = (ekvs.Count < 1) ? 1 : 0;
								break;
							}

							case org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy.LOW_WATERMARK
								:
							{
								numToFill = System.Math.min(num, (int)(lowWatermark * numValues)) - ekvs.Count;
								break;
							}

							case org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy.ALL:
							{
								numToFill = num - ekvs.Count;
								break;
							}
						}
						// Synchronous fill if not enough values found
						if (numToFill > 0)
						{
							refiller.fillQueueForKey(keyName, ekvs, numToFill);
						}
						// Asynch task to fill > lowWatermark
						if (i <= (int)(lowWatermark * numValues))
						{
							submitRefillTask(keyName, keyQueue);
						}
						return ekvs;
					}
					ekvs.add(val);
				}
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException("Exeption while contacting value generator ", e);
			}
			return ekvs;
		}

		/// <exception cref="System.Exception"/>
		private void submitRefillTask(string keyName, java.util.Queue<E> keyQueue)
		{
			if (!executorThreadsStarted)
			{
				lock (this)
				{
					if (!executorThreadsStarted)
					{
						// To ensure all requests are first queued, make coreThreads =
						// maxThreads
						// and pre-start all the Core Threads.
						executor.prestartAllCoreThreads();
						executorThreadsStarted = true;
					}
				}
			}
			// The submit/execute method of the ThreadPoolExecutor is bypassed and
			// the Runnable is directly put in the backing BlockingQueue so that we
			// can control exactly how the runnable is inserted into the queue.
			queue.put(new _NamedRunnable_324(this, keyQueue, keyName));
		}

		private sealed class _NamedRunnable_324 : org.apache.hadoop.crypto.key.kms.ValueQueue.NamedRunnable
		{
			public _NamedRunnable_324(ValueQueue<E> _enclosing, java.util.Queue<E> keyQueue, 
				string baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.keyQueue = keyQueue;
			}

			public override void run()
			{
				int cacheSize = this._enclosing.numValues;
				int threshold = (int)(this._enclosing.lowWatermark * (float)cacheSize);
				// Need to ensure that only one refill task per key is executed
				try
				{
					if (keyQueue.Count < threshold)
					{
						this._enclosing.refiller.fillQueueForKey(this.name, keyQueue, cacheSize - keyQueue
							.Count);
					}
				}
				catch (System.Exception e)
				{
					throw new System.Exception(e);
				}
			}

			private readonly ValueQueue<E> _enclosing;

			private readonly java.util.Queue<E> keyQueue;
		}

		/// <summary>Cleanly shutdown</summary>
		public virtual void shutdown()
		{
			executor.shutdownNow();
		}
	}
}
