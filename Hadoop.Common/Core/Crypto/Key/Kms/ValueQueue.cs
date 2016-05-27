using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Cache;
using Com.Google.Common.Util.Concurrent;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms
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
			void FillQueueForKey(string keyName, Queue<E> keyQueue, int numValues);
		}

		private static readonly string RefillThread = typeof(ValueQueue).FullName + "_thread";

		private readonly LoadingCache<string, LinkedBlockingQueue<E>> keyQueues;

		private readonly ThreadPoolExecutor executor;

		private readonly ValueQueue.UniqueKeyBlockingQueue queue = new ValueQueue.UniqueKeyBlockingQueue
			();

		private readonly ValueQueue.QueueRefiller<E> refiller;

		private readonly ValueQueue.SyncGenerationPolicy policy;

		private readonly int numValues;

		private readonly float lowWatermark;

		private volatile bool executorThreadsStarted = false;

		/// <summary>A <code>Runnable</code> which takes a string name.</summary>
		private abstract class NamedRunnable : Runnable
		{
			internal readonly string name;

			private NamedRunnable(string keyName)
			{
				this.name = keyName;
			}

			public abstract void Run();
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
		private class UniqueKeyBlockingQueue : LinkedBlockingQueue<Runnable>
		{
			private const long serialVersionUID = -2152747693695890371L;

			private HashSet<string> keysInProgress = new HashSet<string>();

			/// <exception cref="System.Exception"/>
			public override void Put(Runnable e)
			{
				lock (this)
				{
					if (keysInProgress.AddItem(((ValueQueue.NamedRunnable)e).name))
					{
						base.Put(e);
					}
				}
			}

			/// <exception cref="System.Exception"/>
			public override Runnable Take()
			{
				Runnable k = base.Take();
				if (k != null)
				{
					keysInProgress.Remove(((ValueQueue.NamedRunnable)k).name);
				}
				return k;
			}

			/// <exception cref="System.Exception"/>
			public override Runnable Poll(long timeout, TimeUnit unit)
			{
				Runnable k = base.Poll(timeout, unit);
				if (k != null)
				{
					keysInProgress.Remove(((ValueQueue.NamedRunnable)k).name);
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
			AtleastOne,
			LowWatermark,
			All
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
			, ValueQueue.SyncGenerationPolicy policy, ValueQueue.QueueRefiller<E> refiller)
		{
			// Return atleast 1 value
			// Return min(n, lowWatermark * numValues) values
			// Return n values
			Preconditions.CheckArgument(numValues > 0, "\"numValues\" must be > 0");
			Preconditions.CheckArgument(((lowWatermark > 0) && (lowWatermark <= 1)), "\"lowWatermark\" must be > 0 and <= 1"
				);
			Preconditions.CheckArgument(expiry > 0, "\"expiry\" must be > 0");
			Preconditions.CheckArgument(numFillerThreads > 0, "\"numFillerThreads\" must be > 0"
				);
			Preconditions.CheckNotNull(policy, "\"policy\" must not be null");
			this.refiller = refiller;
			this.policy = policy;
			this.numValues = numValues;
			this.lowWatermark = lowWatermark;
			keyQueues = CacheBuilder.NewBuilder().ExpireAfterAccess(expiry, TimeUnit.Milliseconds
				).Build(new _CacheLoader_175(refiller, lowWatermark, numValues));
			executor = new ThreadPoolExecutor(numFillerThreads, numFillerThreads, 0L, TimeUnit
				.Milliseconds, queue, new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat(RefillThread
				).Build());
		}

		private sealed class _CacheLoader_175 : CacheLoader<string, LinkedBlockingQueue<E
			>>
		{
			public _CacheLoader_175(ValueQueue.QueueRefiller<E> refiller, float lowWatermark, 
				int numValues)
			{
				this.refiller = refiller;
				this.lowWatermark = lowWatermark;
				this.numValues = numValues;
			}

			/// <exception cref="System.Exception"/>
			public override LinkedBlockingQueue<E> Load(string keyName)
			{
				LinkedBlockingQueue<E> keyQueue = new LinkedBlockingQueue<E>();
				refiller.FillQueueForKey(keyName, keyQueue, (int)(lowWatermark * numValues));
				return keyQueue;
			}

			private readonly ValueQueue.QueueRefiller<E> refiller;

			private readonly float lowWatermark;

			private readonly int numValues;
		}

		public ValueQueue(int numValues, float lowWaterMark, long expiry, int numFillerThreads
			, ValueQueue.QueueRefiller<E> fetcher)
			: this(numValues, lowWaterMark, expiry, numFillerThreads, ValueQueue.SyncGenerationPolicy
				.All, fetcher)
		{
		}

		/// <summary>
		/// Initializes the Value Queues for the provided keys by calling the
		/// fill Method with "numInitValues" values
		/// </summary>
		/// <param name="keyNames">Array of key Names</param>
		/// <exception cref="Sharpen.ExecutionException"/>
		public virtual void InitializeQueuesForKeys(params string[] keyNames)
		{
			foreach (string keyName in keyNames)
			{
				keyQueues.Get(keyName);
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
		/// <exception cref="Sharpen.ExecutionException"/>
		public virtual E GetNext(string keyName)
		{
			return GetAtMost(keyName, 1)[0];
		}

		/// <summary>Drains the Queue for the provided key.</summary>
		/// <param name="keyName">the key to drain the Queue for</param>
		public virtual void Drain(string keyName)
		{
			try
			{
				keyQueues.Get(keyName).Clear();
			}
			catch (ExecutionException)
			{
			}
		}

		//NOP
		/// <summary>Get size of the Queue for keyName</summary>
		/// <param name="keyName">the key name</param>
		/// <returns>int queue size</returns>
		/// <exception cref="Sharpen.ExecutionException"/>
		public virtual int GetSize(string keyName)
		{
			return keyQueues.Get(keyName).Count;
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
		/// <exception cref="Sharpen.ExecutionException"/>
		public virtual IList<E> GetAtMost(string keyName, int num)
		{
			LinkedBlockingQueue<E> keyQueue = keyQueues.Get(keyName);
			// Using poll to avoid race condition..
			List<E> ekvs = new List<E>();
			try
			{
				for (int i = 0; i < num; i++)
				{
					E val = keyQueue.Poll();
					// If queue is empty now, Based on the provided SyncGenerationPolicy,
					// figure out how many new values need to be generated synchronously
					if (val == null)
					{
						// Synchronous call to get remaining values
						int numToFill = 0;
						switch (policy)
						{
							case ValueQueue.SyncGenerationPolicy.AtleastOne:
							{
								numToFill = (ekvs.Count < 1) ? 1 : 0;
								break;
							}

							case ValueQueue.SyncGenerationPolicy.LowWatermark:
							{
								numToFill = Math.Min(num, (int)(lowWatermark * numValues)) - ekvs.Count;
								break;
							}

							case ValueQueue.SyncGenerationPolicy.All:
							{
								numToFill = num - ekvs.Count;
								break;
							}
						}
						// Synchronous fill if not enough values found
						if (numToFill > 0)
						{
							refiller.FillQueueForKey(keyName, ekvs, numToFill);
						}
						// Asynch task to fill > lowWatermark
						if (i <= (int)(lowWatermark * numValues))
						{
							SubmitRefillTask(keyName, keyQueue);
						}
						return ekvs;
					}
					ekvs.AddItem(val);
				}
			}
			catch (Exception e)
			{
				throw new IOException("Exeption while contacting value generator ", e);
			}
			return ekvs;
		}

		/// <exception cref="System.Exception"/>
		private void SubmitRefillTask(string keyName, Queue<E> keyQueue)
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
						executor.PrestartAllCoreThreads();
						executorThreadsStarted = true;
					}
				}
			}
			// The submit/execute method of the ThreadPoolExecutor is bypassed and
			// the Runnable is directly put in the backing BlockingQueue so that we
			// can control exactly how the runnable is inserted into the queue.
			queue.Put(new _NamedRunnable_324(this, keyQueue, keyName));
		}

		private sealed class _NamedRunnable_324 : ValueQueue.NamedRunnable
		{
			public _NamedRunnable_324(ValueQueue<E> _enclosing, Queue<E> keyQueue, string baseArg1
				)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.keyQueue = keyQueue;
			}

			public override void Run()
			{
				int cacheSize = this._enclosing.numValues;
				int threshold = (int)(this._enclosing.lowWatermark * (float)cacheSize);
				// Need to ensure that only one refill task per key is executed
				try
				{
					if (keyQueue.Count < threshold)
					{
						this._enclosing.refiller.FillQueueForKey(this.name, keyQueue, cacheSize - keyQueue
							.Count);
					}
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}

			private readonly ValueQueue<E> _enclosing;

			private readonly Queue<E> keyQueue;
		}

		/// <summary>Cleanly shutdown</summary>
		public virtual void Shutdown()
		{
			executor.ShutdownNow();
		}
	}
}
