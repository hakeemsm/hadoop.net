using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>A queue with multiple levels for each priority.</summary>
	public class FairCallQueue<E> : java.util.AbstractQueue<E>, java.util.concurrent.BlockingQueue
		<E>
		where E : org.apache.hadoop.ipc.Schedulable
	{
		public const int IPC_CALLQUEUE_PRIORITY_LEVELS_DEFAULT = 4;

		public const string IPC_CALLQUEUE_PRIORITY_LEVELS_KEY = "faircallqueue.priority-levels";

		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.FairCallQueue
			)));

		private readonly System.Collections.Generic.List<java.util.concurrent.BlockingQueue
			<E>> queues;

		private readonly java.util.concurrent.locks.ReentrantLock takeLock = new java.util.concurrent.locks.ReentrantLock
			();

		private readonly java.util.concurrent.locks.Condition notEmpty = takeLock.newCondition
			();

		// Configuration Keys
		/* The queues */
		/* Read locks */
		private void signalNotEmpty()
		{
			takeLock.Lock();
			try
			{
				notEmpty.signal();
			}
			finally
			{
				takeLock.unlock();
			}
		}

		private org.apache.hadoop.ipc.RpcScheduler scheduler;

		private org.apache.hadoop.ipc.RpcMultiplexer multiplexer;

		private readonly System.Collections.Generic.List<java.util.concurrent.atomic.AtomicLong
			> overflowedCalls;

		/// <summary>Create a FairCallQueue.</summary>
		/// <param name="capacity">the maximum size of each sub-queue</param>
		/// <param name="ns">the prefix to use for configuration</param>
		/// <param name="conf">
		/// the configuration to read from
		/// Notes: the FairCallQueue has no fixed capacity. Rather, it has a minimum
		/// capacity of `capacity` and a maximum capacity of `capacity * number_queues`
		/// </param>
		public FairCallQueue(int capacity, string ns, org.apache.hadoop.conf.Configuration
			 conf)
		{
			/* Scheduler picks which queue to place in */
			/* Multiplexer picks which queue to draw from */
			/* Statistic tracking */
			int numQueues = parseNumQueues(ns, conf);
			LOG.info("FairCallQueue is in use with " + numQueues + " queues.");
			this.queues = new System.Collections.Generic.List<java.util.concurrent.BlockingQueue
				<E>>(numQueues);
			this.overflowedCalls = new System.Collections.Generic.List<java.util.concurrent.atomic.AtomicLong
				>(numQueues);
			for (int i = 0; i < numQueues; i++)
			{
				this.queues.add(new java.util.concurrent.LinkedBlockingQueue<E>(capacity));
				this.overflowedCalls.add(new java.util.concurrent.atomic.AtomicLong(0));
			}
			this.scheduler = new org.apache.hadoop.ipc.DecayRpcScheduler(numQueues, ns, conf);
			this.multiplexer = new org.apache.hadoop.ipc.WeightedRoundRobinMultiplexer(numQueues
				, ns, conf);
			// Make this the active source of metrics
			org.apache.hadoop.ipc.FairCallQueue.MetricsProxy mp = org.apache.hadoop.ipc.FairCallQueue.MetricsProxy
				.getInstance(ns);
			mp.setDelegate(this);
		}

		/// <summary>Read the number of queues from the configuration.</summary>
		/// <remarks>
		/// Read the number of queues from the configuration.
		/// This will affect the FairCallQueue's overall capacity.
		/// </remarks>
		/// <exception cref="System.ArgumentException">on invalid queue count</exception>
		private static int parseNumQueues(string ns, org.apache.hadoop.conf.Configuration
			 conf)
		{
			int retval = conf.getInt(ns + "." + IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, IPC_CALLQUEUE_PRIORITY_LEVELS_DEFAULT
				);
			if (retval < 1)
			{
				throw new System.ArgumentException("numQueues must be at least 1");
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
		private java.util.concurrent.BlockingQueue<E> getFirstNonEmptyQueue(int startIdx)
		{
			int numQueues = this.queues.Count;
			for (int i = 0; i < numQueues; i++)
			{
				int idx = (i + startIdx) % numQueues;
				// offset and wrap around
				java.util.concurrent.BlockingQueue<E> queue = this.queues[idx];
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
		public virtual void put(E e)
		{
			int priorityLevel = scheduler.getPriorityLevel(e);
			int numLevels = this.queues.Count;
			while (true)
			{
				java.util.concurrent.BlockingQueue<E> q = this.queues[priorityLevel];
				bool res = q.offer(e);
				if (!res)
				{
					// Update stats
					this.overflowedCalls[priorityLevel].getAndIncrement();
					// If we failed to insert, try again on the next level
					priorityLevel++;
					if (priorityLevel == numLevels)
					{
						// That was the last one, we will block on put in the last queue
						// Delete this line to drop the call
						this.queues[priorityLevel - 1].put(e);
						break;
					}
				}
				else
				{
					break;
				}
			}
			signalNotEmpty();
		}

		/// <exception cref="System.Exception"/>
		public virtual bool offer(E e, long timeout, java.util.concurrent.TimeUnit unit)
		{
			int priorityLevel = scheduler.getPriorityLevel(e);
			java.util.concurrent.BlockingQueue<E> q = this.queues[priorityLevel];
			bool ret = q.offer(e, timeout, unit);
			signalNotEmpty();
			return ret;
		}

		public override bool offer(E e)
		{
			int priorityLevel = scheduler.getPriorityLevel(e);
			java.util.concurrent.BlockingQueue<E> q = this.queues[priorityLevel];
			bool ret = q.offer(e);
			signalNotEmpty();
			return ret;
		}

		/// <exception cref="System.Exception"/>
		public virtual E take()
		{
			int startIdx = this.multiplexer.getAndAdvanceCurrentIndex();
			takeLock.lockInterruptibly();
			try
			{
				// Wait while queue is empty
				for (; ; )
				{
					java.util.concurrent.BlockingQueue<E> q = this.getFirstNonEmptyQueue(startIdx);
					if (q != null)
					{
						// Got queue, so return if we can poll out an object
						E e = q.poll();
						if (e != null)
						{
							return e;
						}
					}
					notEmpty.await();
				}
			}
			finally
			{
				takeLock.unlock();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual E poll(long timeout, java.util.concurrent.TimeUnit unit)
		{
			int startIdx = this.multiplexer.getAndAdvanceCurrentIndex();
			long nanos = unit.toNanos(timeout);
			takeLock.lockInterruptibly();
			try
			{
				for (; ; )
				{
					java.util.concurrent.BlockingQueue<E> q = this.getFirstNonEmptyQueue(startIdx);
					if (q != null)
					{
						E e = q.poll();
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
						nanos = notEmpty.awaitNanos(nanos);
					}
					catch (System.Exception ie)
					{
						notEmpty.signal();
						// propagate to a non-interrupted thread
						throw;
					}
				}
			}
			finally
			{
				takeLock.unlock();
			}
		}

		/// <summary>
		/// poll() provides no strict consistency: it is possible for poll to return
		/// null even though an element is in the queue.
		/// </summary>
		public override E poll()
		{
			int startIdx = this.multiplexer.getAndAdvanceCurrentIndex();
			java.util.concurrent.BlockingQueue<E> q = this.getFirstNonEmptyQueue(startIdx);
			if (q == null)
			{
				return null;
			}
			// everything is empty
			// Delegate to the sub-queue's poll, which could still return null
			return q.poll();
		}

		/// <summary>Peek, like poll, provides no strict consistency.</summary>
		public override E peek()
		{
			java.util.concurrent.BlockingQueue<E> q = this.getFirstNonEmptyQueue(0);
			if (q == null)
			{
				return null;
			}
			else
			{
				return q.peek();
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
				foreach (java.util.concurrent.BlockingQueue q in this.queues)
				{
					size += q.Count;
				}
				return size;
			}
		}

		/// <summary>Iterator is not implemented, as it is not needed.</summary>
		public override System.Collections.Generic.IEnumerator<E> GetEnumerator()
		{
			throw new org.apache.commons.lang.NotImplementedException();
		}

		/// <summary>drainTo defers to each sub-queue.</summary>
		/// <remarks>
		/// drainTo defers to each sub-queue. Note that draining from a FairCallQueue
		/// to another FairCallQueue will likely fail, since the incoming calls
		/// may be scheduled differently in the new FairCallQueue. Nonetheless this
		/// method is provided for completeness.
		/// </remarks>
		public virtual int drainTo<_T0>(System.Collections.Generic.ICollection<_T0> c, int
			 maxElements)
		{
			int sum = 0;
			foreach (java.util.concurrent.BlockingQueue<E> q in this.queues)
			{
				sum += q.drainTo(c, maxElements);
			}
			return sum;
		}

		public virtual int drainTo<_T0>(System.Collections.Generic.ICollection<_T0> c)
		{
			int sum = 0;
			foreach (java.util.concurrent.BlockingQueue<E> q in this.queues)
			{
				sum += q.drainTo(c);
			}
			return sum;
		}

		/// <summary>Returns maximum remaining capacity.</summary>
		/// <remarks>
		/// Returns maximum remaining capacity. This does not reflect how much you can
		/// ideally fit in this FairCallQueue, as that would depend on the scheduler's
		/// decisions.
		/// </remarks>
		public virtual int remainingCapacity()
		{
			int sum = 0;
			foreach (java.util.concurrent.BlockingQueue q in this.queues)
			{
				sum += q.remainingCapacity();
			}
			return sum;
		}

		/// <summary>
		/// MetricsProxy is a singleton because we may init multiple
		/// FairCallQueues, but the metrics system cannot unregister beans cleanly.
		/// </summary>
		private sealed class MetricsProxy : org.apache.hadoop.ipc.FairCallQueueMXBean
		{
			private static readonly System.Collections.Generic.Dictionary<string, org.apache.hadoop.ipc.FairCallQueue.MetricsProxy
				> INSTANCES = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.ipc.FairCallQueue.MetricsProxy
				>();

			private java.lang.@ref.WeakReference<org.apache.hadoop.ipc.FairCallQueue> delegate_;

			private int revisionNumber = 0;

			private MetricsProxy(string @namespace)
			{
				// One singleton per namespace
				// Weakref for delegate, so we don't retain it forever if it can be GC'd
				// Keep track of how many objects we registered
				org.apache.hadoop.metrics2.util.MBeans.register(@namespace, "FairCallQueue", this
					);
			}

			public static org.apache.hadoop.ipc.FairCallQueue.MetricsProxy getInstance(string
				 @namespace)
			{
				lock (typeof(MetricsProxy))
				{
					org.apache.hadoop.ipc.FairCallQueue.MetricsProxy mp = INSTANCES[@namespace];
					if (mp == null)
					{
						// We must create one
						mp = new org.apache.hadoop.ipc.FairCallQueue.MetricsProxy(@namespace);
						INSTANCES[@namespace] = mp;
					}
					return mp;
				}
			}

			public void setDelegate(org.apache.hadoop.ipc.FairCallQueue obj)
			{
				this.delegate_ = new java.lang.@ref.WeakReference<org.apache.hadoop.ipc.FairCallQueue
					>(obj);
				this.revisionNumber++;
			}

			public int[] getQueueSizes()
			{
				org.apache.hadoop.ipc.FairCallQueue obj = this.delegate_.get();
				if (obj == null)
				{
					return new int[] {  };
				}
				return obj.getQueueSizes();
			}

			public long[] getOverflowedCalls()
			{
				org.apache.hadoop.ipc.FairCallQueue obj = this.delegate_.get();
				if (obj == null)
				{
					return new long[] {  };
				}
				return obj.getOverflowedCalls();
			}

			public int getRevision()
			{
				return revisionNumber;
			}
		}

		// FairCallQueueMXBean
		public virtual int[] getQueueSizes()
		{
			int numQueues = queues.Count;
			int[] sizes = new int[numQueues];
			for (int i = 0; i < numQueues; i++)
			{
				sizes[i] = queues[i].Count;
			}
			return sizes;
		}

		public virtual long[] getOverflowedCalls()
		{
			int numQueues = queues.Count;
			long[] calls = new long[numQueues];
			for (int i = 0; i < numQueues; i++)
			{
				calls[i] = overflowedCalls[i].get();
			}
			return calls;
		}

		// For testing
		[com.google.common.annotations.VisibleForTesting]
		public virtual void setScheduler(org.apache.hadoop.ipc.RpcScheduler newScheduler)
		{
			this.scheduler = newScheduler;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual void setMultiplexer(org.apache.hadoop.ipc.RpcMultiplexer newMux)
		{
			this.multiplexer = newMux;
		}
	}
}
