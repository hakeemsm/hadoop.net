using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public class QueueMetrics : MetricsSource
	{
		internal MutableCounterInt appsSubmitted;

		internal MutableGaugeInt appsRunning;

		internal MutableGaugeInt appsPending;

		internal MutableCounterInt appsCompleted;

		internal MutableCounterInt appsKilled;

		internal MutableCounterInt appsFailed;

		internal MutableGaugeInt allocatedMB;

		internal MutableGaugeInt allocatedVCores;

		internal MutableGaugeInt allocatedContainers;

		internal MutableCounterLong aggregateContainersAllocated;

		internal MutableCounterLong aggregateContainersReleased;

		internal MutableGaugeInt availableMB;

		internal MutableGaugeInt availableVCores;

		internal MutableGaugeInt pendingMB;

		internal MutableGaugeInt pendingVCores;

		internal MutableGaugeInt pendingContainers;

		internal MutableGaugeInt reservedMB;

		internal MutableGaugeInt reservedVCores;

		internal MutableGaugeInt reservedContainers;

		internal MutableGaugeInt activeUsers;

		internal MutableGaugeInt activeApplications;

		private readonly MutableGaugeInt[] runningTime;

		private TimeBucketMetrics<ApplicationId> runBuckets;

		internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
			));

		internal static readonly MetricsInfo RecordInfo = Interns.Info("QueueMetrics", "Metrics for the resource scheduler"
			);

		protected internal static readonly MetricsInfo QueueInfo = Interns.Info("Queue", 
			"Metrics by queue");

		internal static readonly MetricsInfo UserInfo = Interns.Info("User", "Metrics by user"
			);

		internal static readonly Splitter QSplitter = Splitter.On('.').OmitEmptyStrings()
			.TrimResults();

		internal readonly MetricsRegistry registry;

		internal readonly string queueName;

		internal readonly Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
			 parent;

		internal readonly MetricsSystem metricsSystem;

		private readonly IDictionary<string, Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
			> users;

		private readonly Configuration conf;

		protected internal QueueMetrics(MetricsSystem ms, string queueName, Queue parent, 
			bool enableUserMetrics, Configuration conf)
		{
			registry = new MetricsRegistry(RecordInfo);
			this.queueName = queueName;
			this.parent = parent != null ? parent.GetMetrics() : null;
			this.users = enableUserMetrics ? new Dictionary<string, Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
				>() : null;
			metricsSystem = ms;
			this.conf = conf;
			runningTime = BuildBuckets(conf);
		}

		protected internal virtual Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
			 Tag(MetricsInfo info, string value)
		{
			registry.Tag(info, value);
			return this;
		}

		protected internal static StringBuilder SourceName(string queueName)
		{
			StringBuilder sb = new StringBuilder(RecordInfo.Name());
			int i = 0;
			foreach (string node in QSplitter.Split(queueName))
			{
				sb.Append(",q").Append(i++).Append('=').Append(node);
			}
			return sb;
		}

		public static Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
			 ForQueue(string queueName, Queue parent, bool enableUserMetrics, Configuration 
			conf)
		{
			lock (typeof(QueueMetrics))
			{
				return ForQueue(DefaultMetricsSystem.Instance(), queueName, parent, enableUserMetrics
					, conf);
			}
		}

		/// <summary>Helper method to clear cache.</summary>
		[InterfaceAudience.Private]
		public static void ClearQueueMetrics()
		{
			lock (typeof(QueueMetrics))
			{
				queueMetrics.Clear();
			}
		}

		/// <summary>Simple metrics cache to help prevent re-registrations.</summary>
		protected internal static readonly IDictionary<string, Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
			> queueMetrics = new Dictionary<string, Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
			>();

		public static Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
			 ForQueue(MetricsSystem ms, string queueName, Queue parent, bool enableUserMetrics
			, Configuration conf)
		{
			lock (typeof(QueueMetrics))
			{
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics metrics = queueMetrics
					[queueName];
				if (metrics == null)
				{
					metrics = new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
						(ms, queueName, parent, enableUserMetrics, conf).Tag(QueueInfo, queueName);
					// Register with the MetricsSystems
					if (ms != null)
					{
						metrics = ms.Register(SourceName(queueName).ToString(), "Metrics for queue: " + queueName
							, metrics);
					}
					queueMetrics[queueName] = metrics;
				}
				return metrics;
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
			 GetUserMetrics(string userName)
		{
			lock (this)
			{
				if (users == null)
				{
					return null;
				}
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics metrics = users
					[userName];
				if (metrics == null)
				{
					metrics = new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics
						(metricsSystem, queueName, null, false, conf);
					users[userName] = metrics;
					metricsSystem.Register(SourceName(queueName).Append(",user=").Append(userName).ToString
						(), "Metrics for user '" + userName + "' in queue '" + queueName + "'", metrics.
						Tag(QueueInfo, queueName).Tag(UserInfo, userName));
				}
				return metrics;
			}
		}

		private AList<int> ParseInts(string value)
		{
			AList<int> result = new AList<int>();
			foreach (string s in value.Split(","))
			{
				result.AddItem(System.Convert.ToInt32(s.Trim()));
			}
			return result;
		}

		private MutableGaugeInt[] BuildBuckets(Configuration conf)
		{
			AList<int> buckets = ParseInts(conf.Get(YarnConfiguration.RmMetricsRuntimeBuckets
				, YarnConfiguration.DefaultRmMetricsRuntimeBuckets));
			MutableGaugeInt[] result = new MutableGaugeInt[buckets.Count + 1];
			result[0] = registry.NewGauge("running_0", string.Empty, 0);
			long[] cuts = new long[buckets.Count];
			for (int i = 0; i < buckets.Count; ++i)
			{
				result[i + 1] = registry.NewGauge("running_" + buckets[i], string.Empty, 0);
				cuts[i] = buckets[i] * 1000L * 60;
			}
			// covert from min to ms
			this.runBuckets = new TimeBucketMetrics<ApplicationId>(cuts);
			return result;
		}

		private void UpdateRunningTime()
		{
			int[] counts = runBuckets.GetBucketCounts(Runtime.CurrentTimeMillis());
			for (int i = 0; i < counts.Length; ++i)
			{
				runningTime[i].Set(counts[i]);
			}
		}

		public virtual void GetMetrics(MetricsCollector collector, bool all)
		{
			UpdateRunningTime();
			registry.Snapshot(collector.AddRecord(registry.Info()), all);
		}

		public virtual void SubmitApp(string user)
		{
			appsSubmitted.Incr();
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.SubmitApp(user);
			}
			if (parent != null)
			{
				parent.SubmitApp(user);
			}
		}

		public virtual void SubmitAppAttempt(string user)
		{
			appsPending.Incr();
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.SubmitAppAttempt(user);
			}
			if (parent != null)
			{
				parent.SubmitAppAttempt(user);
			}
		}

		public virtual void RunAppAttempt(ApplicationId appId, string user)
		{
			runBuckets.Add(appId, Runtime.CurrentTimeMillis());
			appsRunning.Incr();
			appsPending.Decr();
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.RunAppAttempt(appId, user);
			}
			if (parent != null)
			{
				parent.RunAppAttempt(appId, user);
			}
		}

		public virtual void FinishAppAttempt(ApplicationId appId, bool isPending, string 
			user)
		{
			runBuckets.Remove(appId);
			if (isPending)
			{
				appsPending.Decr();
			}
			else
			{
				appsRunning.Decr();
			}
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.FinishAppAttempt(appId, isPending, user);
			}
			if (parent != null)
			{
				parent.FinishAppAttempt(appId, isPending, user);
			}
		}

		public virtual void FinishApp(string user, RMAppState rmAppFinalState)
		{
			switch (rmAppFinalState)
			{
				case RMAppState.Killed:
				{
					appsKilled.Incr();
					break;
				}

				case RMAppState.Failed:
				{
					appsFailed.Incr();
					break;
				}

				default:
				{
					appsCompleted.Incr();
					break;
				}
			}
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.FinishApp(user, rmAppFinalState);
			}
			if (parent != null)
			{
				parent.FinishApp(user, rmAppFinalState);
			}
		}

		public virtual void MoveAppFrom(AppSchedulingInfo app)
		{
			if (app.IsPending())
			{
				appsPending.Decr();
			}
			else
			{
				appsRunning.Decr();
			}
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(app.GetUser());
			if (userMetrics != null)
			{
				userMetrics.MoveAppFrom(app);
			}
			if (parent != null)
			{
				parent.MoveAppFrom(app);
			}
		}

		public virtual void MoveAppTo(AppSchedulingInfo app)
		{
			if (app.IsPending())
			{
				appsPending.Incr();
			}
			else
			{
				appsRunning.Incr();
			}
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(app.GetUser());
			if (userMetrics != null)
			{
				userMetrics.MoveAppTo(app);
			}
			if (parent != null)
			{
				parent.MoveAppTo(app);
			}
		}

		/// <summary>Set available resources.</summary>
		/// <remarks>
		/// Set available resources. To be called by scheduler periodically as
		/// resources become available.
		/// </remarks>
		/// <param name="limit">resource limit</param>
		public virtual void SetAvailableResourcesToQueue(Resource limit)
		{
			availableMB.Set(limit.GetMemory());
			availableVCores.Set(limit.GetVirtualCores());
		}

		/// <summary>Set available resources.</summary>
		/// <remarks>
		/// Set available resources. To be called by scheduler periodically as
		/// resources become available.
		/// </remarks>
		/// <param name="user"/>
		/// <param name="limit">resource limit</param>
		public virtual void SetAvailableResourcesToUser(string user, Resource limit)
		{
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.SetAvailableResourcesToQueue(limit);
			}
		}

		/// <summary>Increment pending resource metrics</summary>
		/// <param name="user"/>
		/// <param name="containers"/>
		/// <param name="res">
		/// the TOTAL delta of resources note this is different from
		/// the other APIs which use per container resource
		/// </param>
		public virtual void IncrPendingResources(string user, int containers, Resource res
			)
		{
			_incrPendingResources(containers, res);
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.IncrPendingResources(user, containers, res);
			}
			if (parent != null)
			{
				parent.IncrPendingResources(user, containers, res);
			}
		}

		private void _incrPendingResources(int containers, Resource res)
		{
			pendingContainers.Incr(containers);
			pendingMB.Incr(res.GetMemory() * containers);
			pendingVCores.Incr(res.GetVirtualCores() * containers);
		}

		public virtual void DecrPendingResources(string user, int containers, Resource res
			)
		{
			_decrPendingResources(containers, res);
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.DecrPendingResources(user, containers, res);
			}
			if (parent != null)
			{
				parent.DecrPendingResources(user, containers, res);
			}
		}

		private void _decrPendingResources(int containers, Resource res)
		{
			pendingContainers.Decr(containers);
			pendingMB.Decr(res.GetMemory() * containers);
			pendingVCores.Decr(res.GetVirtualCores() * containers);
		}

		public virtual void AllocateResources(string user, int containers, Resource res, 
			bool decrPending)
		{
			allocatedContainers.Incr(containers);
			aggregateContainersAllocated.Incr(containers);
			allocatedMB.Incr(res.GetMemory() * containers);
			allocatedVCores.Incr(res.GetVirtualCores() * containers);
			if (decrPending)
			{
				_decrPendingResources(containers, res);
			}
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.AllocateResources(user, containers, res, decrPending);
			}
			if (parent != null)
			{
				parent.AllocateResources(user, containers, res, decrPending);
			}
		}

		public virtual void ReleaseResources(string user, int containers, Resource res)
		{
			allocatedContainers.Decr(containers);
			aggregateContainersReleased.Incr(containers);
			allocatedMB.Decr(res.GetMemory() * containers);
			allocatedVCores.Decr(res.GetVirtualCores() * containers);
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.ReleaseResources(user, containers, res);
			}
			if (parent != null)
			{
				parent.ReleaseResources(user, containers, res);
			}
		}

		public virtual void ReserveResource(string user, Resource res)
		{
			reservedContainers.Incr();
			reservedMB.Incr(res.GetMemory());
			reservedVCores.Incr(res.GetVirtualCores());
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.ReserveResource(user, res);
			}
			if (parent != null)
			{
				parent.ReserveResource(user, res);
			}
		}

		public virtual void UnreserveResource(string user, Resource res)
		{
			reservedContainers.Decr();
			reservedMB.Decr(res.GetMemory());
			reservedVCores.Decr(res.GetVirtualCores());
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.UnreserveResource(user, res);
			}
			if (parent != null)
			{
				parent.UnreserveResource(user, res);
			}
		}

		public virtual void IncrActiveUsers()
		{
			activeUsers.Incr();
		}

		public virtual void DecrActiveUsers()
		{
			activeUsers.Decr();
		}

		public virtual void ActivateApp(string user)
		{
			activeApplications.Incr();
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.ActivateApp(user);
			}
			if (parent != null)
			{
				parent.ActivateApp(user);
			}
		}

		public virtual void DeactivateApp(string user)
		{
			activeApplications.Decr();
			Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.QueueMetrics userMetrics = 
				GetUserMetrics(user);
			if (userMetrics != null)
			{
				userMetrics.DeactivateApp(user);
			}
			if (parent != null)
			{
				parent.DeactivateApp(user);
			}
		}

		public virtual int GetAppsSubmitted()
		{
			return appsSubmitted.Value();
		}

		public virtual int GetAppsRunning()
		{
			return appsRunning.Value();
		}

		public virtual int GetAppsPending()
		{
			return appsPending.Value();
		}

		public virtual int GetAppsCompleted()
		{
			return appsCompleted.Value();
		}

		public virtual int GetAppsKilled()
		{
			return appsKilled.Value();
		}

		public virtual int GetAppsFailed()
		{
			return appsFailed.Value();
		}

		public virtual Resource GetAllocatedResources()
		{
			return BuilderUtils.NewResource(allocatedMB.Value(), allocatedVCores.Value());
		}

		public virtual int GetAllocatedMB()
		{
			return allocatedMB.Value();
		}

		public virtual int GetAllocatedVirtualCores()
		{
			return allocatedVCores.Value();
		}

		public virtual int GetAllocatedContainers()
		{
			return allocatedContainers.Value();
		}

		public virtual int GetAvailableMB()
		{
			return availableMB.Value();
		}

		public virtual int GetAvailableVirtualCores()
		{
			return availableVCores.Value();
		}

		public virtual int GetPendingMB()
		{
			return pendingMB.Value();
		}

		public virtual int GetPendingVirtualCores()
		{
			return pendingVCores.Value();
		}

		public virtual int GetPendingContainers()
		{
			return pendingContainers.Value();
		}

		public virtual int GetReservedMB()
		{
			return reservedMB.Value();
		}

		public virtual int GetReservedVirtualCores()
		{
			return reservedVCores.Value();
		}

		public virtual int GetReservedContainers()
		{
			return reservedContainers.Value();
		}

		public virtual int GetActiveUsers()
		{
			return activeUsers.Value();
		}

		public virtual int GetActiveApps()
		{
			return activeApplications.Value();
		}

		public virtual MetricsSystem GetMetricsSystem()
		{
			return metricsSystem;
		}
	}
}
