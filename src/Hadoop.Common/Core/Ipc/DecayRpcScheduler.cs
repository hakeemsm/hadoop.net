using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Codehaus.Jackson.Map;


namespace Org.Apache.Hadoop.Ipc
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
	public class DecayRpcScheduler : RpcScheduler, DecayRpcSchedulerMXBean
	{
		/// <summary>Period controls how many milliseconds between each decay sweep.</summary>
		public const string IpcCallqueueDecayschedulerPeriodKey = "faircallqueue.decay-scheduler.period-ms";

		public const long IpcCallqueueDecayschedulerPeriodDefault = 5000L;

		/// <summary>Decay factor controls how much each count is suppressed by on each sweep.
		/// 	</summary>
		/// <remarks>
		/// Decay factor controls how much each count is suppressed by on each sweep.
		/// Valid numbers are &gt; 0 and &lt; 1. Decay factor works in tandem with period
		/// to control how long the scheduler remembers an identity.
		/// </remarks>
		public const string IpcCallqueueDecayschedulerFactorKey = "faircallqueue.decay-scheduler.decay-factor";

		public const double IpcCallqueueDecayschedulerFactorDefault = 0.5;

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
		public const string IpcCallqueueDecayschedulerThresholdsKey = "faircallqueue.decay-scheduler.thresholds";

		public const string DecayschedulerUnknownIdentity = "IdentityProvider.Unknown";

		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.DecayRpcScheduler
			));

		private readonly ConcurrentHashMap<object, AtomicLong> callCounts = new ConcurrentHashMap
			<object, AtomicLong>();

		private readonly AtomicLong totalCalls = new AtomicLong();

		private readonly AtomicReference<IDictionary<object, int>> scheduleCacheRef = new 
			AtomicReference<IDictionary<object, int>>();

		private readonly long decayPeriodMillis;

		private readonly double decayFactor;

		private readonly int numQueues;

		private readonly double[] thresholds;

		private readonly IdentityProvider identityProvider;

		/// <summary>
		/// This TimerTask will call decayCurrentCounts until
		/// the scheduler has been garbage collected.
		/// </summary>
		public class DecayTask : TimerTask
		{
			private WeakReference<DecayRpcScheduler> schedulerRef;

			private Timer timer;

			public DecayTask(DecayRpcScheduler scheduler, Timer timer)
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
				this.schedulerRef = new WeakReference<DecayRpcScheduler>(scheduler);
				this.timer = timer;
			}

			public override void Run()
			{
				DecayRpcScheduler sched = schedulerRef.Get();
				if (sched != null)
				{
					sched.DecayCurrentCounts();
				}
				else
				{
					// Our scheduler was garbage collected since it is no longer in use,
					// so we should terminate the timer as well
					timer.Cancel();
					timer.Purge();
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
		public DecayRpcScheduler(int numQueues, string ns, Configuration conf)
		{
			if (numQueues < 1)
			{
				throw new ArgumentException("number of queues must be > 0");
			}
			this.numQueues = numQueues;
			this.decayFactor = ParseDecayFactor(ns, conf);
			this.decayPeriodMillis = ParseDecayPeriodMillis(ns, conf);
			this.identityProvider = this.ParseIdentityProvider(ns, conf);
			this.thresholds = ParseThresholds(ns, conf, numQueues);
			// Setup delay timer
			Timer timer = new Timer();
			DecayRpcScheduler.DecayTask task = new DecayRpcScheduler.DecayTask(this, timer);
			timer.ScheduleAtFixedRate(task, 0, this.decayPeriodMillis);
			DecayRpcScheduler.MetricsProxy prox = DecayRpcScheduler.MetricsProxy.GetInstance(
				ns);
			prox.SetDelegate(this);
		}

		// Load configs
		private IdentityProvider ParseIdentityProvider(string ns, Configuration conf)
		{
			IList<IdentityProvider> providers = conf.GetInstances<IdentityProvider>(ns + "." 
				+ CommonConfigurationKeys.IpcCallqueueIdentityProviderKey);
			if (providers.Count < 1)
			{
				Log.Info("IdentityProvider not specified, " + "defaulting to UserIdentityProvider"
					);
				return new UserIdentityProvider();
			}
			return providers[0];
		}

		// use the first
		private static double ParseDecayFactor(string ns, Configuration conf)
		{
			double factor = conf.GetDouble(ns + "." + IpcCallqueueDecayschedulerFactorKey, IpcCallqueueDecayschedulerFactorDefault
				);
			if (factor <= 0 || factor >= 1)
			{
				throw new ArgumentException("Decay Factor " + "must be between 0 and 1");
			}
			return factor;
		}

		private static long ParseDecayPeriodMillis(string ns, Configuration conf)
		{
			long period = conf.GetLong(ns + "." + IpcCallqueueDecayschedulerPeriodKey, IpcCallqueueDecayschedulerPeriodDefault
				);
			if (period <= 0)
			{
				throw new ArgumentException("Period millis must be >= 0");
			}
			return period;
		}

		private static double[] ParseThresholds(string ns, Configuration conf, int numQueues
			)
		{
			int[] percentages = conf.GetInts(ns + "." + IpcCallqueueDecayschedulerThresholdsKey
				);
			if (percentages.Length == 0)
			{
				return GetDefaultThresholds(numQueues);
			}
			else
			{
				if (percentages.Length != numQueues - 1)
				{
					throw new ArgumentException("Number of thresholds should be " + (numQueues - 1) +
						 ". Was: " + percentages.Length);
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
		private static double[] GetDefaultThresholds(int numQueues)
		{
			double[] ret = new double[numQueues - 1];
			double div = Math.Pow(2, numQueues - 1);
			for (int i = 0; i < ret.Length; i++)
			{
				ret[i] = Math.Pow(2, i) / div;
			}
			return ret;
		}

		/// <summary>Decay the stored counts for each user and clean as necessary.</summary>
		/// <remarks>
		/// Decay the stored counts for each user and clean as necessary.
		/// This method should be called periodically in order to keep
		/// counts current.
		/// </remarks>
		private void DecayCurrentCounts()
		{
			long total = 0;
			IEnumerator<KeyValuePair<object, AtomicLong>> it = callCounts.GetEnumerator();
			while (it.HasNext())
			{
				KeyValuePair<object, AtomicLong> entry = it.Next();
				AtomicLong count = entry.Value;
				// Compute the next value by reducing it by the decayFactor
				long currentValue = count.Get();
				long nextValue = (long)(currentValue * decayFactor);
				total += nextValue;
				count.Set(nextValue);
				if (nextValue == 0)
				{
					// We will clean up unused keys here. An interesting optimization might
					// be to have an upper bound on keyspace in callCounts and only
					// clean once we pass it.
					it.Remove();
				}
			}
			// Update the total so that we remain in sync
			totalCalls.Set(total);
			// Now refresh the cache of scheduling decisions
			RecomputeScheduleCache();
		}

		/// <summary>Update the scheduleCache to match current conditions in callCounts.</summary>
		private void RecomputeScheduleCache()
		{
			IDictionary<object, int> nextCache = new Dictionary<object, int>();
			foreach (KeyValuePair<object, AtomicLong> entry in callCounts)
			{
				object id = entry.Key;
				AtomicLong value = entry.Value;
				long snapshot = value.Get();
				int computedLevel = ComputePriorityLevel(snapshot);
				nextCache[id] = computedLevel;
			}
			// Swap in to activate
			scheduleCacheRef.Set(Collections.UnmodifiableMap(nextCache));
		}

		/// <summary>Get the number of occurrences and increment atomically.</summary>
		/// <param name="identity">the identity of the user to increment</param>
		/// <returns>the value before incrementation</returns>
		/// <exception cref="System.Exception"/>
		private long GetAndIncrement(object identity)
		{
			// We will increment the count, or create it if no such count exists
			AtomicLong count = this.callCounts[identity];
			if (count == null)
			{
				// Create the count since no such count exists.
				count = new AtomicLong(0);
				// Put it in, or get the AtomicInteger that was put in by another thread
				AtomicLong otherCount = callCounts.PutIfAbsent(identity, count);
				if (otherCount != null)
				{
					count = otherCount;
				}
			}
			// Update the total
			totalCalls.GetAndIncrement();
			// At this point value is guaranteed to be not null. It may however have
			// been clobbered from callCounts. Nonetheless, we return what
			// we have.
			return count.GetAndIncrement();
		}

		/// <summary>Given the number of occurrences, compute a scheduling decision.</summary>
		/// <param name="occurrences">how many occurrences</param>
		/// <returns>scheduling decision from 0 to numQueues - 1</returns>
		private int ComputePriorityLevel(long occurrences)
		{
			long totalCallSnapshot = totalCalls.Get();
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
		private int CachedOrComputedPriorityLevel(object identity)
		{
			try
			{
				long occurrences = this.GetAndIncrement(identity);
				// Try the cache
				IDictionary<object, int> scheduleCache = scheduleCacheRef.Get();
				if (scheduleCache != null)
				{
					int priority = scheduleCache[identity];
					if (priority != null)
					{
						return priority;
					}
				}
				// Cache was no good, compute it
				return ComputePriorityLevel(occurrences);
			}
			catch (Exception)
			{
				Log.Warn("Caught InterruptedException, returning low priority queue");
				return numQueues - 1;
			}
		}

		/// <summary>Compute the appropriate priority for a schedulable based on past requests.
		/// 	</summary>
		/// <param name="obj">the schedulable obj to query and remember</param>
		/// <returns>the queue index which we recommend scheduling in</returns>
		public virtual int GetPriorityLevel(Schedulable obj)
		{
			// First get the identity
			string identity = this.identityProvider.MakeIdentity(obj);
			if (identity == null)
			{
				// Identity provider did not handle this
				identity = DecayschedulerUnknownIdentity;
			}
			return CachedOrComputedPriorityLevel(identity);
		}

		// For testing
		[VisibleForTesting]
		public virtual double GetDecayFactor()
		{
			return decayFactor;
		}

		[VisibleForTesting]
		public virtual long GetDecayPeriodMillis()
		{
			return decayPeriodMillis;
		}

		[VisibleForTesting]
		public virtual double[] GetThresholds()
		{
			return thresholds;
		}

		[VisibleForTesting]
		public virtual void ForceDecay()
		{
			DecayCurrentCounts();
		}

		[VisibleForTesting]
		public virtual IDictionary<object, long> GetCallCountSnapshot()
		{
			Dictionary<object, long> snapshot = new Dictionary<object, long>();
			foreach (KeyValuePair<object, AtomicLong> entry in callCounts)
			{
				snapshot[entry.Key] = entry.Value.Get();
			}
			return Collections.UnmodifiableMap(snapshot);
		}

		[VisibleForTesting]
		public virtual long GetTotalCallSnapshot()
		{
			return totalCalls.Get();
		}

		/// <summary>
		/// MetricsProxy is a singleton because we may init multiple schedulers and we
		/// want to clean up resources when a new scheduler replaces the old one.
		/// </summary>
		private sealed class MetricsProxy : DecayRpcSchedulerMXBean
		{
			private static readonly Dictionary<string, DecayRpcScheduler.MetricsProxy> Instances
				 = new Dictionary<string, DecayRpcScheduler.MetricsProxy>();

			private WeakReference<DecayRpcScheduler> delegate_;

			private MetricsProxy(string @namespace)
			{
				// One singleton per namespace
				// Weakref for delegate, so we don't retain it forever if it can be GC'd
				MBeans.Register(@namespace, "DecayRpcScheduler", this);
			}

			public static DecayRpcScheduler.MetricsProxy GetInstance(string @namespace)
			{
				lock (typeof(MetricsProxy))
				{
					DecayRpcScheduler.MetricsProxy mp = Instances[@namespace];
					if (mp == null)
					{
						// We must create one
						mp = new DecayRpcScheduler.MetricsProxy(@namespace);
						Instances[@namespace] = mp;
					}
					return mp;
				}
			}

			public void SetDelegate(DecayRpcScheduler obj)
			{
				this.delegate_ = new WeakReference<DecayRpcScheduler>(obj);
			}

			public string GetSchedulingDecisionSummary()
			{
				DecayRpcScheduler scheduler = delegate_.Get();
				if (scheduler == null)
				{
					return "No Active Scheduler";
				}
				else
				{
					return scheduler.GetSchedulingDecisionSummary();
				}
			}

			public string GetCallVolumeSummary()
			{
				DecayRpcScheduler scheduler = delegate_.Get();
				if (scheduler == null)
				{
					return "No Active Scheduler";
				}
				else
				{
					return scheduler.GetCallVolumeSummary();
				}
			}

			public int GetUniqueIdentityCount()
			{
				DecayRpcScheduler scheduler = delegate_.Get();
				if (scheduler == null)
				{
					return -1;
				}
				else
				{
					return scheduler.GetUniqueIdentityCount();
				}
			}

			public long GetTotalCallVolume()
			{
				DecayRpcScheduler scheduler = delegate_.Get();
				if (scheduler == null)
				{
					return -1;
				}
				else
				{
					return scheduler.GetTotalCallVolume();
				}
			}
		}

		public virtual int GetUniqueIdentityCount()
		{
			return callCounts.Count;
		}

		public virtual long GetTotalCallVolume()
		{
			return totalCalls.Get();
		}

		public virtual string GetSchedulingDecisionSummary()
		{
			IDictionary<object, int> decisions = scheduleCacheRef.Get();
			if (decisions == null)
			{
				return "{}";
			}
			else
			{
				try
				{
					ObjectMapper om = new ObjectMapper();
					return om.WriteValueAsString(decisions);
				}
				catch (Exception e)
				{
					return "Error: " + e.Message;
				}
			}
		}

		public virtual string GetCallVolumeSummary()
		{
			try
			{
				ObjectMapper om = new ObjectMapper();
				return om.WriteValueAsString(callCounts);
			}
			catch (Exception e)
			{
				return "Error: " + e.Message;
			}
		}
	}
}
