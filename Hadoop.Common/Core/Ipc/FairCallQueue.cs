using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Metrics2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>A queue with multiple levels for each priority.</summary>
	public class FairCallQueue<E> : AbstractQueue<E>, BlockingQueue<E>
		where E : Schedulable
	{
		public const int IpcCallqueuePriorityLevelsDefault = 4;

		public const string IpcCallqueuePriorityLevelsKey = "faircallqueue.priority-levels";

		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.FairCallQueue
			));

		private readonly AList<BlockingQueue<E>> queues;

		private readonly ReentrantLock takeLock = new ReentrantLock();

		private readonly Condition notEmpty = takeLock.NewCondition();

		// Configuration Keys
		/* The queues */
		/* Read locks */
		private void SignalNotEmpty()
		{
			takeLock.Lock();
			try
			{
				notEmpty.Signal();
			}
			finally
			{
				takeLock.Unlock();
			}
		}

		private RpcScheduler scheduler;

		private RpcMultiplexer multiplexer;

		private readonly AList<AtomicLong> overflowedCalls;

		/// <summary>Create a FairCallQueue.</summary>
		/// <param name="capacity">the maximum size of each sub-queue</param>
		/// <param name="ns">the prefix to use for configuration</param>
		/// <param name="conf">
		/// the configuration to read from
		/// Notes: the FairCallQueue has no fixed capacity. Rather, it has a minimum
		/// capacity of `capacity` and a maximum capacity of `capacity * number_queues`
		/// </param>
		public FairCallQueue(int capacity, string ns, Configuration conf)
		{
			/* Scheduler picks which queue to place in */
			/* Multiplexer picks which queue to draw from */
			/* Statistic tracking */
			int numQueues = ParseNumQueues(ns, conf);
			Log.Info("FairCallQueue is in use with " + numQueues + " queues.");
			this.queues = new AList<BlockingQueue<E>>(numQueues);
			this.overflowedCalls = new AList<AtomicLong>(numQueues);
			for (int i = 0; i < numQueues; i++)
			{
				this.queues.AddItem(new LinkedBlockingQueue<E>(capacity));
				this.overflowedCalls.AddItem(new AtomicLong(0));
			}
			this.scheduler = new DecayRpcScheduler(numQueues, ns, conf);
			this.multiplexer = new WeightedRoundRobinMultiplexer(numQueues, ns, conf);
			// Make this the active source of metrics
			FairCallQueue.MetricsProxy mp = FairCallQueue.MetricsProxy.GetInstance(ns);
			mp.SetDelegate(this);
		}

		/// <summary>Read the number of queues from the configuration.</summary>
		/// <remarks>
		/// Read the number of queues from the configuration.
		/// This will affect the FairCallQueue's overall capacity.
		/// </remarks>
		/// <exception cref="System.ArgumentException">on invalid queue count</exception>
		private static int ParseNumQueues(string ns, Configuration conf)
		{
			int retval = conf.GetInt(ns + "." + IpcCallqueuePriorityLevelsKey, IpcCallqueuePriorityLevelsDefault
				);
			if (retval < 1)
			{
				throw new ArgumentException("numQueues must be at least 1");
			}
			return retval;
		}

		/// <summary>
		/// Returns the first non-empty queue with equal or lesser priority
		/// than <i>startIdx</i>.
		/// </summary>
		/// <remarks>
		/// Returns the first non-empty queue with equal or lesser priority
		/// than <i>startIdx</i>. Wraps around, searching a maximum of N
		/// queues, where N is this.queues.size().
		/// </remarks>
		/// <param name="startIdx">the queue number to start searching at</param>
		/// <returns>
		/// the first non-empty queue with less priority, or null if
		/// everything was empty
		/// </returns>
		private BlockingQueue<E> GetFirstNonEmptyQueue(int startIdx)
		{
			int numQueues = this.queues.Count;
			for (int i = 0; i < numQueues; i++)
			{
				int idx = (i + startIdx) % numQueues;
				// offset and wrap around
				BlockingQueue<E> queue = this.queues[idx];
				if (queue.Count != 0)
				{
					return queue;
				}
			}
			// All queues were empty
			return null;
		}

		/* AbstractQueue and BlockingQueue methods */
		/// <summary>
		/// Put and offer follow the same pattern:
		/// 1.
		/// </summary>
		/// <remarks>
		/// Put and offer follow the same pattern:
		/// 1. Get a priorityLevel from the scheduler
		/// 2. Get the nth sub-queue matching this priorityLevel
		/// 3. delegate the call to this sub-queue.
		/// But differ in how they handle overflow:
		/// - Put will move on to the next queue until it lands on the last queue
		/// - Offer does not attempt other queues on overflow
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void Put(E e)
		{
			int priorityLevel = scheduler.GetPriorityLevel(e);
			int numLevels = this.queues.Count;
			while (true)
			{
				BlockingQueue<E> q = this.queues[priorityLevel];
				bool res = q.Offer(e);
				if (!res)
				{
					// Update stats
					this.overflowedCalls[priorityLevel].GetAndIncrement();
					// If we failed to insert, try again on the next level
					priorityLevel++;
					if (priorityLevel == numLevels)
					{
						// That was the last one, we will block on put in the last queue
						// Delete this line to drop the call
						this.queues[priorityLevel - 1].Put(e);
						break;
					}
				}
				else
				{
					break;
				}
			}
			SignalNotEmpty();
		}

		/// <exception cref="System.Exception"/>
		public virtual bool Offer(E e, long timeout, TimeUnit unit)
		{
			int priorityLevel = scheduler.GetPriorityLevel(e);
			BlockingQueue<E> q = this.queues[priorityLevel];
			bool ret = q.Offer(e, timeout, unit);
			SignalNotEmpty();
			return ret;
		}

		public override bool Offer(E e)
		{
			int priorityLevel = scheduler.GetPriorityLevel(e);
			BlockingQueue<E> q = this.queues[priorityLevel];
			bool ret = q.Offer(e);
			SignalNotEmpty();
			return ret;
		}

		/// <exception cref="System.Exception"/>
		public virtual E Take()
		{
			int startIdx = this.multiplexer.GetAndAdvanceCurrentIndex();
			takeLock.LockInterruptibly();
			try
			{
				// Wait while queue is empty
				for (; ; )
				{
					BlockingQueue<E> q = this.GetFirstNonEmptyQueue(startIdx);
					if (q != null)
					{
						// Got queue, so return if we can poll out an object
						E e = q.Poll();
						if (e != null)
						{
							return e;
						}
					}
					notEmpty.Await();
				}
			}
			finally
			{
				takeLock.Unlock();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual E Poll(long timeout, TimeUnit unit)
		{
			int startIdx = this.multiplexer.GetAndAdvanceCurrentIndex();
			long nanos = unit.ToNanos(timeout);
			takeLock.LockInterruptibly();
			try
			{
				for (; ; )
				{
					BlockingQueue<E> q = this.GetFirstNonEmptyQueue(startIdx);
					if (q != null)
					{
						E e = q.Poll();
						if (e != null)
						{
							// Escape condition: there might be something available
							return e;
						}
					}
					if (nanos <= 0)
					{
						// Wait has elapsed
						return null;
					}
					try
					{
						// Now wait on the condition for a bit. If we get
						// spuriously awoken we'll re-loop
						nanos = notEmpty.AwaitNanos(nanos);
					}
					catch (Exception ie)
					{
						notEmpty.Signal();
						// propagate to a non-interrupted thread
						throw;
					}
				}
			}
			finally
			{
				takeLock.Unlock();
			}
		}

		/// <summary>
		/// poll() provides no strict consistency: it is possible for poll to return
		/// null even though an element is in the queue.
		/// </summary>
		public override E Poll()
		{
			int startIdx = this.multiplexer.GetAndAdvanceCurrentIndex();
			BlockingQueue<E> q = this.GetFirstNonEmptyQueue(startIdx);
			if (q == null)
			{
				return null;
			}
			// everything is empty
			// Delegate to the sub-queue's poll, which could still return null
			return q.Poll();
		}

		/// <summary>Peek, like poll, provides no strict consistency.</summary>
		public override E Peek()
		{
			BlockingQueue<E> q = this.GetFirstNonEmptyQueue(0);
			if (q == null)
			{
				return null;
			}
			else
			{
				return q.Peek();
			}
		}

		/// <summary>
		/// Size returns the sum of all sub-queue sizes, so it may be greater than
		/// capacity.
		/// </summary>
		/// <remarks>
		/// Size returns the sum of all sub-queue sizes, so it may be greater than
		/// capacity.
		/// Note: size provides no strict consistency, and should not be used to
		/// control queue IO.
		/// </remarks>
		public override int Count
		{
			get
			{
				int size = 0;
				foreach (BlockingQueue q in this.queues)
				{
					size += q.Count;
				}
				return size;
			}
		}

		/// <summary>Iterator is not implemented, as it is not needed.</summary>
		public override IEnumerator<E> GetEnumerator()
		{
			throw new NotImplementedException();
		}

		/// <summary>drainTo defers to each sub-queue.</summary>
		/// <remarks>
		/// drainTo defers to each sub-queue. Note that draining from a FairCallQueue
		/// to another FairCallQueue will likely fail, since the incoming calls
		/// may be scheduled differently in the new FairCallQueue. Nonetheless this
		/// method is provided for completeness.
		/// </remarks>
		public virtual int DrainTo<_T0>(ICollection<_T0> c, int maxElements)
		{
			int sum = 0;
			foreach (BlockingQueue<E> q in this.queues)
			{
				sum += q.DrainTo(c, maxElements);
			}
			return sum;
		}

		public virtual int DrainTo<_T0>(ICollection<_T0> c)
		{
			int sum = 0;
			foreach (BlockingQueue<E> q in this.queues)
			{
				sum += q.DrainTo(c);
			}
			return sum;
		}

		/// <summary>Returns maximum remaining capacity.</summary>
		/// <remarks>
		/// Returns maximum remaining capacity. This does not reflect how much you can
		/// ideally fit in this FairCallQueue, as that would depend on the scheduler's
		/// decisions.
		/// </remarks>
		public virtual int RemainingCapacity()
		{
			int sum = 0;
			foreach (BlockingQueue q in this.queues)
			{
				sum += q.RemainingCapacity();
			}
			return sum;
		}

		/// <summary>
		/// MetricsProxy is a singleton because we may init multiple
		/// FairCallQueues, but the metrics system cannot unregister beans cleanly.
		/// </summary>
		private sealed class MetricsProxy : FairCallQueueMXBean
		{
			private static readonly Dictionary<string, FairCallQueue.MetricsProxy> Instances = 
				new Dictionary<string, FairCallQueue.MetricsProxy>();

			private WeakReference<FairCallQueue> delegate_;

			private int revisionNumber = 0;

			private MetricsProxy(string @namespace)
			{
				// One singleton per namespace
				// Weakref for delegate, so we don't retain it forever if it can be GC'd
				// Keep track of how many objects we registered
				MBeans.Register(@namespace, "FairCallQueue", this);
			}

			public static FairCallQueue.MetricsProxy GetInstance(string @namespace)
			{
				lock (typeof(MetricsProxy))
				{
					FairCallQueue.MetricsProxy mp = Instances[@namespace];
					if (mp == null)
					{
						// We must create one
						mp = new FairCallQueue.MetricsProxy(@namespace);
						Instances[@namespace] = mp;
					}
					return mp;
				}
			}

			public void SetDelegate(FairCallQueue obj)
			{
				this.delegate_ = new WeakReference<FairCallQueue>(obj);
				this.revisionNumber++;
			}

			public int[] GetQueueSizes()
			{
				FairCallQueue obj = this.delegate_.Get();
				if (obj == null)
				{
					return new int[] {  };
				}
				return obj.GetQueueSizes();
			}

			public long[] GetOverflowedCalls()
			{
				FairCallQueue obj = this.delegate_.Get();
				if (obj == null)
				{
					return new long[] {  };
				}
				return obj.GetOverflowedCalls();
			}

			public int GetRevision()
			{
				return revisionNumber;
			}
		}

		// FairCallQueueMXBean
		public virtual int[] GetQueueSizes()
		{
			int numQueues = queues.Count;
			int[] sizes = new int[numQueues];
			for (int i = 0; i < numQueues; i++)
			{
				sizes[i] = queues[i].Count;
			}
			return sizes;
		}

		public virtual long[] GetOverflowedCalls()
		{
			int numQueues = queues.Count;
			long[] calls = new long[numQueues];
			for (int i = 0; i < numQueues; i++)
			{
				calls[i] = overflowedCalls[i].Get();
			}
			return calls;
		}

		// For testing
		[VisibleForTesting]
		public virtual void SetScheduler(RpcScheduler newScheduler)
		{
			this.scheduler = newScheduler;
		}

		[VisibleForTesting]
		public virtual void SetMultiplexer(RpcMultiplexer newMux)
		{
			this.multiplexer = newMux;
		}
	}
}
