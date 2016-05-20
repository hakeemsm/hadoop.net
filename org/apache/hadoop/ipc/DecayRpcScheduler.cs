using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// The decay RPC scheduler counts incoming requests in a map, then
	/// decays the counts at a fixed time interval.
	/// </summary>
	/// <remarks>
	/// The decay RPC scheduler counts incoming requests in a map, then
	/// decays the counts at a fixed time interval. The scheduler is optimized
	/// for large periods (on the order of seconds), as it offloads work to the
	/// decay sweep.
	/// </remarks>
	public class DecayRpcScheduler : org.apache.hadoop.ipc.RpcScheduler, org.apache.hadoop.ipc.DecayRpcSchedulerMXBean
	{
		/// <summary>Period controls how many milliseconds between each decay sweep.</summary>
		public const string IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY = "faircallqueue.decay-scheduler.period-ms";

		public const long IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_DEFAULT = 5000L;

		/// <summary>Decay factor controls how much each count is suppressed by on each sweep.
		/// 	</summary>
		/// <remarks>
		/// Decay factor controls how much each count is suppressed by on each sweep.
		/// Valid numbers are &gt; 0 and &lt; 1. Decay factor works in tandem with period
		/// to control how long the scheduler remembers an identity.
		/// </remarks>
		public const string IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_KEY = "faircallqueue.decay-scheduler.decay-factor";

		public const double IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_DEFAULT = 0.5;

		/// <summary>
		/// Thresholds are specified as integer percentages, and specify which usage
		/// range each queue will be allocated to.
		/// </summary>
		/// <remarks>
		/// Thresholds are specified as integer percentages, and specify which usage
		/// range each queue will be allocated to. For instance, specifying the list
		/// 10, 40, 80
		/// implies 4 queues, with
		/// - q3 from 80% up
		/// - q2 from 40 up to 80
		/// - q1 from 10 up to 40
		/// - q0 otherwise.
		/// </remarks>
		public const string IPC_CALLQUEUE_DECAYSCHEDULER_THRESHOLDS_KEY = "faircallqueue.decay-scheduler.thresholds";

		public const string DECAYSCHEDULER_UNKNOWN_IDENTITY = "IdentityProvider.Unknown";

		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.DecayRpcScheduler
			)));

		private readonly java.util.concurrent.ConcurrentHashMap<object, java.util.concurrent.atomic.AtomicLong
			> callCounts = new java.util.concurrent.ConcurrentHashMap<object, java.util.concurrent.atomic.AtomicLong
			>();

		private readonly java.util.concurrent.atomic.AtomicLong totalCalls = new java.util.concurrent.atomic.AtomicLong
			();

		private readonly java.util.concurrent.atomic.AtomicReference<System.Collections.Generic.IDictionary
			<object, int>> scheduleCacheRef = new java.util.concurrent.atomic.AtomicReference
			<System.Collections.Generic.IDictionary<object, int>>();

		private readonly long decayPeriodMillis;

		private readonly double decayFactor;

		private readonly int numQueues;

		private readonly double[] thresholds;

		private readonly org.apache.hadoop.ipc.IdentityProvider identityProvider;

		/// <summary>
		/// This TimerTask will call decayCurrentCounts until
		/// the scheduler has been garbage collected.
		/// </summary>
		public class DecayTask : java.util.TimerTask
		{
			private java.lang.@ref.WeakReference<org.apache.hadoop.ipc.DecayRpcScheduler> schedulerRef;

			private java.util.Timer timer;

			public DecayTask(org.apache.hadoop.ipc.DecayRpcScheduler scheduler, java.util.Timer
				 timer)
			{
				// Specifies the identity to use when the IdentityProvider cannot handle
				// a schedulable.
				// Track the number of calls for each schedulable identity
				// Should be the sum of all AtomicLongs in callCounts
				// Pre-computed scheduling decisions during the decay sweep are
				// atomically swapped in as a read-only map
				// Tune the behavior of the scheduler
				// How long between each tick
				// nextCount = currentCount / decayFactor
				// affects scheduling decisions, from 0 to numQueues - 1
				this.schedulerRef = new java.lang.@ref.WeakReference<org.apache.hadoop.ipc.DecayRpcScheduler
					>(scheduler);
				this.timer = timer;
			}

			public override void run()
			{
				org.apache.hadoop.ipc.DecayRpcScheduler sched = schedulerRef.get();
				if (sched != null)
				{
					sched.decayCurrentCounts();
				}
				else
				{
					// Our scheduler was garbage collected since it is no longer in use,
					// so we should terminate the timer as well
					timer.cancel();
					timer.purge();
				}
			}
		}

		/// <summary>Create a decay scheduler.</summary>
		/// <param name="numQueues">number of queues to schedule for</param>
		/// <param name="ns">
		/// config prefix, so that we can configure multiple schedulers
		/// in a single instance.
		/// </param>
		/// <param name="conf">configuration to use.</param>
		public DecayRpcScheduler(int numQueues, string ns, org.apache.hadoop.conf.Configuration
			 conf)
		{
			if (numQueues < 1)
			{
				throw new System.ArgumentException("number of queues must be > 0");
			}
			this.numQueues = numQueues;
			this.decayFactor = parseDecayFactor(ns, conf);
			this.decayPeriodMillis = parseDecayPeriodMillis(ns, conf);
			this.identityProvider = this.parseIdentityProvider(ns, conf);
			this.thresholds = parseThresholds(ns, conf, numQueues);
			// Setup delay timer
			java.util.Timer timer = new java.util.Timer();
			org.apache.hadoop.ipc.DecayRpcScheduler.DecayTask task = new org.apache.hadoop.ipc.DecayRpcScheduler.DecayTask
				(this, timer);
			timer.scheduleAtFixedRate(task, 0, this.decayPeriodMillis);
			org.apache.hadoop.ipc.DecayRpcScheduler.MetricsProxy prox = org.apache.hadoop.ipc.DecayRpcScheduler.MetricsProxy
				.getInstance(ns);
			prox.setDelegate(this);
		}

		// Load configs
		private org.apache.hadoop.ipc.IdentityProvider parseIdentityProvider(string ns, org.apache.hadoop.conf.Configuration
			 conf)
		{
			System.Collections.Generic.IList<org.apache.hadoop.ipc.IdentityProvider> providers
				 = conf.getInstances<org.apache.hadoop.ipc.IdentityProvider>(ns + "." + org.apache.hadoop.fs.CommonConfigurationKeys
				.IPC_CALLQUEUE_IDENTITY_PROVIDER_KEY);
			if (providers.Count < 1)
			{
				LOG.info("IdentityProvider not specified, " + "defaulting to UserIdentityProvider"
					);
				return new org.apache.hadoop.ipc.UserIdentityProvider();
			}
			return providers[0];
		}

		// use the first
		private static double parseDecayFactor(string ns, org.apache.hadoop.conf.Configuration
			 conf)
		{
			double factor = conf.getDouble(ns + "." + IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_KEY
				, IPC_CALLQUEUE_DECAYSCHEDULER_FACTOR_DEFAULT);
			if (factor <= 0 || factor >= 1)
			{
				throw new System.ArgumentException("Decay Factor " + "must be between 0 and 1");
			}
			return factor;
		}

		private static long parseDecayPeriodMillis(string ns, org.apache.hadoop.conf.Configuration
			 conf)
		{
			long period = conf.getLong(ns + "." + IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_KEY, IPC_CALLQUEUE_DECAYSCHEDULER_PERIOD_DEFAULT
				);
			if (period <= 0)
			{
				throw new System.ArgumentException("Period millis must be >= 0");
			}
			return period;
		}

		private static double[] parseThresholds(string ns, org.apache.hadoop.conf.Configuration
			 conf, int numQueues)
		{
			int[] percentages = conf.getInts(ns + "." + IPC_CALLQUEUE_DECAYSCHEDULER_THRESHOLDS_KEY
				);
			if (percentages.Length == 0)
			{
				return getDefaultThresholds(numQueues);
			}
			else
			{
				if (percentages.Length != numQueues - 1)
				{
					throw new System.ArgumentException("Number of thresholds should be " + (numQueues
						 - 1) + ". Was: " + percentages.Length);
				}
			}
			// Convert integer percentages to decimals
			double[] decimals = new double[percentages.Length];
			for (int i = 0; i < percentages.Length; i++)
			{
				decimals[i] = percentages[i] / 100.0;
			}
			return decimals;
		}

		/// <summary>Generate default thresholds if user did not specify.</summary>
		/// <remarks>
		/// Generate default thresholds if user did not specify. Strategy is
		/// to halve each time, since queue usage tends to be exponential.
		/// So if numQueues is 4, we would generate: double[]{0.125, 0.25, 0.5}
		/// which specifies the boundaries between each queue's usage.
		/// </remarks>
		/// <param name="numQueues">number of queues to compute for</param>
		/// <returns>array of boundaries of length numQueues - 1</returns>
		private static double[] getDefaultThresholds(int numQueues)
		{
			double[] ret = new double[numQueues - 1];
			double div = System.Math.pow(2, numQueues - 1);
			for (int i = 0; i < ret.Length; i++)
			{
				ret[i] = System.Math.pow(2, i) / div;
			}
			return ret;
		}

		/// <summary>Decay the stored counts for each user and clean as necessary.</summary>
		/// <remarks>
		/// Decay the stored counts for each user and clean as necessary.
		/// This method should be called periodically in order to keep
		/// counts current.
		/// </remarks>
		private void decayCurrentCounts()
		{
			long total = 0;
			System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<object
				, java.util.concurrent.atomic.AtomicLong>> it = callCounts.GetEnumerator();
			while (it.MoveNext())
			{
				System.Collections.Generic.KeyValuePair<object, java.util.concurrent.atomic.AtomicLong
					> entry = it.Current;
				java.util.concurrent.atomic.AtomicLong count = entry.Value;
				// Compute the next value by reducing it by the decayFactor
				long currentValue = count.get();
				long nextValue = (long)(currentValue * decayFactor);
				total += nextValue;
				count.set(nextValue);
				if (nextValue == 0)
				{
					// We will clean up unused keys here. An interesting optimization might
					// be to have an upper bound on keyspace in callCounts and only
					// clean once we pass it.
					it.remove();
				}
			}
			// Update the total so that we remain in sync
			totalCalls.set(total);
			// Now refresh the cache of scheduling decisions
			recomputeScheduleCache();
		}

		/// <summary>Update the scheduleCache to match current conditions in callCounts.</summary>
		private void recomputeScheduleCache()
		{
			System.Collections.Generic.IDictionary<object, int> nextCache = new System.Collections.Generic.Dictionary
				<object, int>();
			foreach (System.Collections.Generic.KeyValuePair<object, java.util.concurrent.atomic.AtomicLong
				> entry in callCounts)
			{
				object id = entry.Key;
				java.util.concurrent.atomic.AtomicLong value = entry.Value;
				long snapshot = value.get();
				int computedLevel = computePriorityLevel(snapshot);
				nextCache[id] = computedLevel;
			}
			// Swap in to activate
			scheduleCacheRef.set(java.util.Collections.unmodifiableMap(nextCache));
		}

		/// <summary>Get the number of occurrences and increment atomically.</summary>
		/// <param name="identity">the identity of the user to increment</param>
		/// <returns>the value before incrementation</returns>
		/// <exception cref="System.Exception"/>
		private long getAndIncrement(object identity)
		{
			// We will increment the count, or create it if no such count exists
			java.util.concurrent.atomic.AtomicLong count = this.callCounts[identity];
			if (count == null)
			{
				// Create the count since no such count exists.
				count = new java.util.concurrent.atomic.AtomicLong(0);
				// Put it in, or get the AtomicInteger that was put in by another thread
				java.util.concurrent.atomic.AtomicLong otherCount = callCounts.putIfAbsent(identity
					, count);
				if (otherCount != null)
				{
					count = otherCount;
				}
			}
			// Update the total
			totalCalls.getAndIncrement();
			// At this point value is guaranteed to be not null. It may however have
			// been clobbered from callCounts. Nonetheless, we return what
			// we have.
			return count.getAndIncrement();
		}

		/// <summary>Given the number of occurrences, compute a scheduling decision.</summary>
		/// <param name="occurrences">how many occurrences</param>
		/// <returns>scheduling decision from 0 to numQueues - 1</returns>
		private int computePriorityLevel(long occurrences)
		{
			long totalCallSnapshot = totalCalls.get();
			double proportion = 0;
			if (totalCallSnapshot > 0)
			{
				proportion = (double)occurrences / totalCallSnapshot;
			}
			// Start with low priority queues, since they will be most common
			for (int i = (numQueues - 1); i > 0; i--)
			{
				if (proportion >= this.thresholds[i - 1])
				{
					return i;
				}
			}
			// We've found our queue number
			// If we get this far, we're at queue 0
			return 0;
		}

		/// <summary>
		/// Returns the priority level for a given identity by first trying the cache,
		/// then computing it.
		/// </summary>
		/// <param name="identity">an object responding to toString and hashCode</param>
		/// <returns>integer scheduling decision from 0 to numQueues - 1</returns>
		private int cachedOrComputedPriorityLevel(object identity)
		{
			try
			{
				long occurrences = this.getAndIncrement(identity);
				// Try the cache
				System.Collections.Generic.IDictionary<object, int> scheduleCache = scheduleCacheRef
					.get();
				if (scheduleCache != null)
				{
					int priority = scheduleCache[identity];
					if (priority != null)
					{
						return priority;
					}
				}
				// Cache was no good, compute it
				return computePriorityLevel(occurrences);
			}
			catch (System.Exception)
			{
				LOG.warn("Caught InterruptedException, returning low priority queue");
				return numQueues - 1;
			}
		}

		/// <summary>Compute the appropriate priority for a schedulable based on past requests.
		/// 	</summary>
		/// <param name="obj">the schedulable obj to query and remember</param>
		/// <returns>the queue index which we recommend scheduling in</returns>
		public virtual int getPriorityLevel(org.apache.hadoop.ipc.Schedulable obj)
		{
			// First get the identity
			string identity = this.identityProvider.makeIdentity(obj);
			if (identity == null)
			{
				// Identity provider did not handle this
				identity = DECAYSCHEDULER_UNKNOWN_IDENTITY;
			}
			return cachedOrComputedPriorityLevel(identity);
		}

		// For testing
		[com.google.common.annotations.VisibleForTesting]
		public virtual double getDecayFactor()
		{
			return decayFactor;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual long getDecayPeriodMillis()
		{
			return decayPeriodMillis;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual double[] getThresholds()
		{
			return thresholds;
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual void forceDecay()
		{
			decayCurrentCounts();
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual System.Collections.Generic.IDictionary<object, long> getCallCountSnapshot
			()
		{
			System.Collections.Generic.Dictionary<object, long> snapshot = new System.Collections.Generic.Dictionary
				<object, long>();
			foreach (System.Collections.Generic.KeyValuePair<object, java.util.concurrent.atomic.AtomicLong
				> entry in callCounts)
			{
				snapshot[entry.Key] = entry.Value.get();
			}
			return java.util.Collections.unmodifiableMap(snapshot);
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual long getTotalCallSnapshot()
		{
			return totalCalls.get();
		}

		/// <summary>
		/// MetricsProxy is a singleton because we may init multiple schedulers and we
		/// want to clean up resources when a new scheduler replaces the old one.
		/// </summary>
		private sealed class MetricsProxy : org.apache.hadoop.ipc.DecayRpcSchedulerMXBean
		{
			private static readonly System.Collections.Generic.Dictionary<string, org.apache.hadoop.ipc.DecayRpcScheduler.MetricsProxy
				> INSTANCES = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.ipc.DecayRpcScheduler.MetricsProxy
				>();

			private java.lang.@ref.WeakReference<org.apache.hadoop.ipc.DecayRpcScheduler> delegate_;

			private MetricsProxy(string @namespace)
			{
				// One singleton per namespace
				// Weakref for delegate, so we don't retain it forever if it can be GC'd
				org.apache.hadoop.metrics2.util.MBeans.register(@namespace, "DecayRpcScheduler", 
					this);
			}

			public static org.apache.hadoop.ipc.DecayRpcScheduler.MetricsProxy getInstance(string
				 @namespace)
			{
				lock (typeof(MetricsProxy))
				{
					org.apache.hadoop.ipc.DecayRpcScheduler.MetricsProxy mp = INSTANCES[@namespace];
					if (mp == null)
					{
						// We must create one
						mp = new org.apache.hadoop.ipc.DecayRpcScheduler.MetricsProxy(@namespace);
						INSTANCES[@namespace] = mp;
					}
					return mp;
				}
			}

			public void setDelegate(org.apache.hadoop.ipc.DecayRpcScheduler obj)
			{
				this.delegate_ = new java.lang.@ref.WeakReference<org.apache.hadoop.ipc.DecayRpcScheduler
					>(obj);
			}

			public string getSchedulingDecisionSummary()
			{
				org.apache.hadoop.ipc.DecayRpcScheduler scheduler = delegate_.get();
				if (scheduler == null)
				{
					return "No Active Scheduler";
				}
				else
				{
					return scheduler.getSchedulingDecisionSummary();
				}
			}

			public string getCallVolumeSummary()
			{
				org.apache.hadoop.ipc.DecayRpcScheduler scheduler = delegate_.get();
				if (scheduler == null)
				{
					return "No Active Scheduler";
				}
				else
				{
					return scheduler.getCallVolumeSummary();
				}
			}

			public int getUniqueIdentityCount()
			{
				org.apache.hadoop.ipc.DecayRpcScheduler scheduler = delegate_.get();
				if (scheduler == null)
				{
					return -1;
				}
				else
				{
					return scheduler.getUniqueIdentityCount();
				}
			}

			public long getTotalCallVolume()
			{
				org.apache.hadoop.ipc.DecayRpcScheduler scheduler = delegate_.get();
				if (scheduler == null)
				{
					return -1;
				}
				else
				{
					return scheduler.getTotalCallVolume();
				}
			}
		}

		public virtual int getUniqueIdentityCount()
		{
			return callCounts.Count;
		}

		public virtual long getTotalCallVolume()
		{
			return totalCalls.get();
		}

		public virtual string getSchedulingDecisionSummary()
		{
			System.Collections.Generic.IDictionary<object, int> decisions = scheduleCacheRef.
				get();
			if (decisions == null)
			{
				return "{}";
			}
			else
			{
				try
				{
					org.codehaus.jackson.map.ObjectMapper om = new org.codehaus.jackson.map.ObjectMapper
						();
					return om.writeValueAsString(decisions);
				}
				catch (System.Exception e)
				{
					return "Error: " + e.Message;
				}
			}
		}

		public virtual string getCallVolumeSummary()
		{
			try
			{
				org.codehaus.jackson.map.ObjectMapper om = new org.codehaus.jackson.map.ObjectMapper
					();
				return om.writeValueAsString(callCounts);
			}
			catch (System.Exception e)
			{
				return "Error: " + e.Message;
			}
		}
	}
}
