using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class AllocationConfiguration : ReservationSchedulerConfiguration
	{
		private static readonly AccessControlList EverybodyAcl = new AccessControlList("*"
			);

		private static readonly AccessControlList NobodyAcl = new AccessControlList(" ");

		private readonly IDictionary<string, Resource> minQueueResources;

		[VisibleForTesting]
		internal readonly IDictionary<string, Resource> maxQueueResources;

		private readonly IDictionary<string, ResourceWeights> queueWeights;

		[VisibleForTesting]
		internal readonly IDictionary<string, int> queueMaxApps;

		[VisibleForTesting]
		internal readonly IDictionary<string, int> userMaxApps;

		private readonly int userMaxAppsDefault;

		private readonly int queueMaxAppsDefault;

		internal readonly IDictionary<string, float> queueMaxAMShares;

		private readonly float queueMaxAMShareDefault;

		private readonly IDictionary<string, IDictionary<QueueACL, AccessControlList>> queueAcls;

		private readonly IDictionary<string, long> minSharePreemptionTimeouts;

		private readonly IDictionary<string, long> fairSharePreemptionTimeouts;

		private readonly IDictionary<string, float> fairSharePreemptionThresholds;

		private readonly ICollection<string> reservableQueues;

		private readonly IDictionary<string, SchedulingPolicy> schedulingPolicies;

		private readonly SchedulingPolicy defaultSchedulingPolicy;

		[VisibleForTesting]
		internal QueuePlacementPolicy placementPolicy;

		[VisibleForTesting]
		internal IDictionary<FSQueueType, ICollection<string>> configuredQueues;

		private ReservationQueueConfiguration globalReservationQueueConfig;

		public AllocationConfiguration(IDictionary<string, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			> minQueueResources, IDictionary<string, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			> maxQueueResources, IDictionary<string, int> queueMaxApps, IDictionary<string, 
			int> userMaxApps, IDictionary<string, ResourceWeights> queueWeights, IDictionary
			<string, float> queueMaxAMShares, int userMaxAppsDefault, int queueMaxAppsDefault
			, float queueMaxAMShareDefault, IDictionary<string, SchedulingPolicy> schedulingPolicies
			, SchedulingPolicy defaultSchedulingPolicy, IDictionary<string, long> minSharePreemptionTimeouts
			, IDictionary<string, long> fairSharePreemptionTimeouts, IDictionary<string, float
			> fairSharePreemptionThresholds, IDictionary<string, IDictionary<QueueACL, AccessControlList
			>> queueAcls, QueuePlacementPolicy placementPolicy, IDictionary<FSQueueType, ICollection
			<string>> configuredQueues, ReservationQueueConfiguration globalReservationQueueConfig
			, ICollection<string> reservableQueues)
		{
			// Minimum resource allocation for each queue
			// Maximum amount of resources per queue
			// Sharing weights for each queue
			// Max concurrent running applications for each queue and for each user; in addition,
			// for users that have no max specified, we use the userMaxJobsDefault.
			// Maximum resource share for each leaf queue that can be used to run AMs
			// ACL's for each queue. Only specifies non-default ACL's from configuration.
			// Min share preemption timeout for each queue in seconds. If a job in the queue
			// waits this long without receiving its guaranteed share, it is allowed to
			// preempt other jobs' tasks.
			// Fair share preemption timeout for each queue in seconds. If a job in the
			// queue waits this long without receiving its fair share threshold, it is
			// allowed to preempt other jobs' tasks.
			// The fair share preemption threshold for each queue. If a queue waits
			// fairSharePreemptionTimeout without receiving
			// fairshare * fairSharePreemptionThreshold resources, it is allowed to
			// preempt other queues' tasks.
			// Policy for mapping apps to queues
			//Configured queues in the alloc xml
			// Reservation system configuration
			this.minQueueResources = minQueueResources;
			this.maxQueueResources = maxQueueResources;
			this.queueMaxApps = queueMaxApps;
			this.userMaxApps = userMaxApps;
			this.queueMaxAMShares = queueMaxAMShares;
			this.queueWeights = queueWeights;
			this.userMaxAppsDefault = userMaxAppsDefault;
			this.queueMaxAppsDefault = queueMaxAppsDefault;
			this.queueMaxAMShareDefault = queueMaxAMShareDefault;
			this.defaultSchedulingPolicy = defaultSchedulingPolicy;
			this.schedulingPolicies = schedulingPolicies;
			this.minSharePreemptionTimeouts = minSharePreemptionTimeouts;
			this.fairSharePreemptionTimeouts = fairSharePreemptionTimeouts;
			this.fairSharePreemptionThresholds = fairSharePreemptionThresholds;
			this.queueAcls = queueAcls;
			this.reservableQueues = reservableQueues;
			this.globalReservationQueueConfig = globalReservationQueueConfig;
			this.placementPolicy = placementPolicy;
			this.configuredQueues = configuredQueues;
		}

		public AllocationConfiguration(Configuration conf)
		{
			minQueueResources = new Dictionary<string, Org.Apache.Hadoop.Yarn.Api.Records.Resource
				>();
			maxQueueResources = new Dictionary<string, Org.Apache.Hadoop.Yarn.Api.Records.Resource
				>();
			queueWeights = new Dictionary<string, ResourceWeights>();
			queueMaxApps = new Dictionary<string, int>();
			userMaxApps = new Dictionary<string, int>();
			queueMaxAMShares = new Dictionary<string, float>();
			userMaxAppsDefault = int.MaxValue;
			queueMaxAppsDefault = int.MaxValue;
			queueMaxAMShareDefault = 0.5f;
			queueAcls = new Dictionary<string, IDictionary<QueueACL, AccessControlList>>();
			minSharePreemptionTimeouts = new Dictionary<string, long>();
			fairSharePreemptionTimeouts = new Dictionary<string, long>();
			fairSharePreemptionThresholds = new Dictionary<string, float>();
			schedulingPolicies = new Dictionary<string, SchedulingPolicy>();
			defaultSchedulingPolicy = SchedulingPolicy.DefaultPolicy;
			reservableQueues = new HashSet<string>();
			configuredQueues = new Dictionary<FSQueueType, ICollection<string>>();
			foreach (FSQueueType queueType in FSQueueType.Values())
			{
				configuredQueues[queueType] = new HashSet<string>();
			}
			placementPolicy = QueuePlacementPolicy.FromConfiguration(conf, configuredQueues);
		}

		/// <summary>Get the ACLs associated with this queue.</summary>
		/// <remarks>
		/// Get the ACLs associated with this queue. If a given ACL is not explicitly
		/// configured, include the default value for that ACL.  The default for the
		/// root queue is everybody ("*") and the default for all other queues is
		/// nobody ("")
		/// </remarks>
		public virtual AccessControlList GetQueueAcl(string queue, QueueACL operation)
		{
			IDictionary<QueueACL, AccessControlList> queueAcls = this.queueAcls[queue];
			if (queueAcls != null)
			{
				AccessControlList operationAcl = queueAcls[operation];
				if (operationAcl != null)
				{
					return operationAcl;
				}
			}
			return (queue.Equals("root")) ? EverybodyAcl : NobodyAcl;
		}

		/// <summary>
		/// Get a queue's min share preemption timeout configured in the allocation
		/// file, in milliseconds.
		/// </summary>
		/// <remarks>
		/// Get a queue's min share preemption timeout configured in the allocation
		/// file, in milliseconds. Return -1 if not set.
		/// </remarks>
		public virtual long GetMinSharePreemptionTimeout(string queueName)
		{
			long minSharePreemptionTimeout = minSharePreemptionTimeouts[queueName];
			return (minSharePreemptionTimeout == null) ? -1 : minSharePreemptionTimeout;
		}

		/// <summary>
		/// Get a queue's fair share preemption timeout configured in the allocation
		/// file, in milliseconds.
		/// </summary>
		/// <remarks>
		/// Get a queue's fair share preemption timeout configured in the allocation
		/// file, in milliseconds. Return -1 if not set.
		/// </remarks>
		public virtual long GetFairSharePreemptionTimeout(string queueName)
		{
			long fairSharePreemptionTimeout = fairSharePreemptionTimeouts[queueName];
			return (fairSharePreemptionTimeout == null) ? -1 : fairSharePreemptionTimeout;
		}

		/// <summary>Get a queue's fair share preemption threshold in the allocation file.</summary>
		/// <remarks>
		/// Get a queue's fair share preemption threshold in the allocation file.
		/// Return -1f if not set.
		/// </remarks>
		public virtual float GetFairSharePreemptionThreshold(string queueName)
		{
			float fairSharePreemptionThreshold = fairSharePreemptionThresholds[queueName];
			return (fairSharePreemptionThreshold == null) ? -1f : fairSharePreemptionThreshold;
		}

		public virtual ResourceWeights GetQueueWeight(string queue)
		{
			ResourceWeights weight = queueWeights[queue];
			return (weight == null) ? ResourceWeights.Neutral : weight;
		}

		public virtual void SetQueueWeight(string queue, ResourceWeights weight)
		{
			queueWeights[queue] = weight;
		}

		public virtual int GetUserMaxApps(string user)
		{
			int maxApps = userMaxApps[user];
			return (maxApps == null) ? userMaxAppsDefault : maxApps;
		}

		public virtual int GetQueueMaxApps(string queue)
		{
			int maxApps = queueMaxApps[queue];
			return (maxApps == null) ? queueMaxAppsDefault : maxApps;
		}

		public virtual float GetQueueMaxAMShare(string queue)
		{
			float maxAMShare = queueMaxAMShares[queue];
			return (maxAMShare == null) ? queueMaxAMShareDefault : maxAMShare;
		}

		/// <summary>Get the minimum resource allocation for the given queue.</summary>
		/// <returns>the cap set on this queue, or 0 if not set.</returns>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinResources(string
			 queue)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource minQueueResource = minQueueResources[
				queue];
			return (minQueueResource == null) ? Resources.None() : minQueueResource;
		}

		/// <summary>Get the maximum resource allocation for the given queue.</summary>
		/// <returns>the cap set on this queue, or Integer.MAX_VALUE if not set.</returns>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaxResources(string
			 queueName)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxQueueResource = maxQueueResources[
				queueName];
			return (maxQueueResource == null) ? Resources.Unbounded() : maxQueueResource;
		}

		public virtual bool HasAccess(string queueName, QueueACL acl, UserGroupInformation
			 user)
		{
			int lastPeriodIndex = queueName.Length;
			while (lastPeriodIndex != -1)
			{
				string queue = Sharpen.Runtime.Substring(queueName, 0, lastPeriodIndex);
				if (GetQueueAcl(queue, acl).IsUserAllowed(user))
				{
					return true;
				}
				lastPeriodIndex = queueName.LastIndexOf('.', lastPeriodIndex - 1);
			}
			return false;
		}

		public virtual SchedulingPolicy GetSchedulingPolicy(string queueName)
		{
			SchedulingPolicy policy = schedulingPolicies[queueName];
			return (policy == null) ? defaultSchedulingPolicy : policy;
		}

		public virtual SchedulingPolicy GetDefaultSchedulingPolicy()
		{
			return defaultSchedulingPolicy;
		}

		public virtual IDictionary<FSQueueType, ICollection<string>> GetConfiguredQueues(
			)
		{
			return configuredQueues;
		}

		public virtual QueuePlacementPolicy GetPlacementPolicy()
		{
			return placementPolicy;
		}

		public override bool IsReservable(string queue)
		{
			return reservableQueues.Contains(queue);
		}

		public override long GetReservationWindow(string queue)
		{
			return globalReservationQueueConfig.GetReservationWindowMsec();
		}

		public override float GetAverageCapacity(string queue)
		{
			return globalReservationQueueConfig.GetAvgOverTimeMultiplier() * 100;
		}

		public override float GetInstantaneousMaxCapacity(string queue)
		{
			return globalReservationQueueConfig.GetMaxOverTimeMultiplier() * 100;
		}

		public override string GetReservationAdmissionPolicy(string queue)
		{
			return globalReservationQueueConfig.GetReservationAdmissionPolicy();
		}

		public override string GetReservationAgent(string queue)
		{
			return globalReservationQueueConfig.GetReservationAgent();
		}

		public override bool GetShowReservationAsQueues(string queue)
		{
			return globalReservationQueueConfig.ShouldShowReservationAsQueues();
		}

		public override string GetReplanner(string queue)
		{
			return globalReservationQueueConfig.GetPlanner();
		}

		public override bool GetMoveOnExpiry(string queue)
		{
			return globalReservationQueueConfig.ShouldMoveOnExpiry();
		}

		public override long GetEnforcementWindow(string queue)
		{
			return globalReservationQueueConfig.GetEnforcementWindowMsec();
		}

		[VisibleForTesting]
		public virtual void SetReservationWindow(long window)
		{
			globalReservationQueueConfig.SetReservationWindow(window);
		}

		[VisibleForTesting]
		public virtual void SetAverageCapacity(int avgCapacity)
		{
			globalReservationQueueConfig.SetAverageCapacity(avgCapacity);
		}
	}
}
