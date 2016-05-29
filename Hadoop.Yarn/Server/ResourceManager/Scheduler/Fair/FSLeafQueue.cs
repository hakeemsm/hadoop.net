using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class FSLeafQueue : FSQueue
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSLeafQueue
			).FullName);

		private readonly IList<FSAppAttempt> runnableApps = new AList<FSAppAttempt>();

		private readonly IList<FSAppAttempt> nonRunnableApps = new AList<FSAppAttempt>();

		private readonly ReadWriteLock rwl = new ReentrantReadWriteLock(true);

		private readonly Lock readLock = rwl.ReadLock();

		private readonly Lock writeLock = rwl.WriteLock();

		private Resource demand = Resources.CreateResource(0);

		private long lastTimeAtMinShare;

		private long lastTimeAtFairShareThreshold;

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource amResourceUsage;

		private readonly ActiveUsersManager activeUsersManager;

		public FSLeafQueue(string name, FairScheduler scheduler, FSParentQueue parent)
			: base(name, scheduler, parent)
		{
			// apps that are runnable
			// get a lock with fair distribution for app list updates
			// Variables used for preemption
			// Track the AM resource usage for this queue
			this.lastTimeAtMinShare = scheduler.GetClock().GetTime();
			this.lastTimeAtFairShareThreshold = scheduler.GetClock().GetTime();
			activeUsersManager = new ActiveUsersManager(GetMetrics());
			amResourceUsage = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 0);
		}

		public virtual void AddApp(FSAppAttempt app, bool runnable)
		{
			writeLock.Lock();
			try
			{
				if (runnable)
				{
					runnableApps.AddItem(app);
				}
				else
				{
					nonRunnableApps.AddItem(app);
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		// for testing
		internal virtual void AddAppSchedulable(FSAppAttempt appSched)
		{
			writeLock.Lock();
			try
			{
				runnableApps.AddItem(appSched);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <summary>Removes the given app from this queue.</summary>
		/// <returns>whether or not the app was runnable</returns>
		public virtual bool RemoveApp(FSAppAttempt app)
		{
			bool runnable = false;
			// Remove app from runnable/nonRunnable list while holding the write lock
			writeLock.Lock();
			try
			{
				runnable = runnableApps.Remove(app);
				if (!runnable)
				{
					// removeNonRunnableApp acquires the write lock again, which is fine
					if (!RemoveNonRunnableApp(app))
					{
						throw new InvalidOperationException("Given app to remove " + app + " does not exist in queue "
							 + this);
					}
				}
			}
			finally
			{
				writeLock.Unlock();
			}
			// Update AM resource usage if needed
			if (runnable && app.IsAmRunning() && app.GetAMResource() != null)
			{
				Resources.SubtractFrom(amResourceUsage, app.GetAMResource());
			}
			return runnable;
		}

		/// <summary>Removes the given app if it is non-runnable and belongs to this queue</summary>
		/// <returns>true if the app is removed, false otherwise</returns>
		public virtual bool RemoveNonRunnableApp(FSAppAttempt app)
		{
			writeLock.Lock();
			try
			{
				return nonRunnableApps.Remove(app);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public virtual bool IsRunnableApp(FSAppAttempt attempt)
		{
			readLock.Lock();
			try
			{
				return runnableApps.Contains(attempt);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual bool IsNonRunnableApp(FSAppAttempt attempt)
		{
			readLock.Lock();
			try
			{
				return nonRunnableApps.Contains(attempt);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual void ResetPreemptedResources()
		{
			readLock.Lock();
			try
			{
				foreach (FSAppAttempt attempt in runnableApps)
				{
					attempt.ResetPreemptedResources();
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual void ClearPreemptedResources()
		{
			readLock.Lock();
			try
			{
				foreach (FSAppAttempt attempt in runnableApps)
				{
					attempt.ClearPreemptedResources();
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual IList<FSAppAttempt> GetCopyOfNonRunnableAppSchedulables()
		{
			IList<FSAppAttempt> appsToReturn = new AList<FSAppAttempt>();
			readLock.Lock();
			try
			{
				Sharpen.Collections.AddAll(appsToReturn, nonRunnableApps);
			}
			finally
			{
				readLock.Unlock();
			}
			return appsToReturn;
		}

		public override void CollectSchedulerApplications(ICollection<ApplicationAttemptId
			> apps)
		{
			readLock.Lock();
			try
			{
				foreach (FSAppAttempt appSched in runnableApps)
				{
					apps.AddItem(appSched.GetApplicationAttemptId());
				}
				foreach (FSAppAttempt appSched_1 in nonRunnableApps)
				{
					apps.AddItem(appSched_1.GetApplicationAttemptId());
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public override void SetPolicy(SchedulingPolicy policy)
		{
			if (!SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy.DepthLeaf))
			{
				ThrowPolicyDoesnotApplyException(policy);
			}
			base.policy = policy;
		}

		public override void RecomputeShares()
		{
			readLock.Lock();
			try
			{
				policy.ComputeShares(runnableApps, GetFairShare());
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetDemand()
		{
			return demand;
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResourceUsage()
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource usage = Resources.CreateResource(0);
			readLock.Lock();
			try
			{
				foreach (FSAppAttempt app in runnableApps)
				{
					Resources.AddTo(usage, app.GetResourceUsage());
				}
				foreach (FSAppAttempt app_1 in nonRunnableApps)
				{
					Resources.AddTo(usage, app_1.GetResourceUsage());
				}
			}
			finally
			{
				readLock.Unlock();
			}
			return usage;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetAmResourceUsage()
		{
			return amResourceUsage;
		}

		public override void UpdateDemand()
		{
			// Compute demand by iterating through apps in the queue
			// Limit demand to maxResources
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxRes = scheduler.GetAllocationConfiguration
				().GetMaxResources(GetName());
			demand = Resources.CreateResource(0);
			readLock.Lock();
			try
			{
				foreach (FSAppAttempt sched in runnableApps)
				{
					if (Resources.Equals(demand, maxRes))
					{
						break;
					}
					UpdateDemandForApp(sched, maxRes);
				}
				foreach (FSAppAttempt sched_1 in nonRunnableApps)
				{
					if (Resources.Equals(demand, maxRes))
					{
						break;
					}
					UpdateDemandForApp(sched_1, maxRes);
				}
			}
			finally
			{
				readLock.Unlock();
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("The updated demand for " + GetName() + " is " + demand + "; the max is "
					 + maxRes);
			}
		}

		private void UpdateDemandForApp(FSAppAttempt sched, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maxRes)
		{
			sched.UpdateDemand();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource toAdd = sched.GetDemand();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Counting resource from " + sched.GetName() + " " + toAdd + "; Total resource consumption for "
					 + GetName() + " now " + demand);
			}
			demand = Resources.Add(demand, toAdd);
			demand = Resources.ComponentwiseMin(demand, maxRes);
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignContainer(FSSchedulerNode
			 node)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource assigned = Resources.None();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Node " + node.GetNodeName() + " offered to queue: " + GetName());
			}
			if (!AssignContainerPreCheck(node))
			{
				return assigned;
			}
			IComparer<Schedulable> comparator = policy.GetComparator();
			writeLock.Lock();
			try
			{
				runnableApps.Sort(comparator);
			}
			finally
			{
				writeLock.Unlock();
			}
			// Release write lock here for better performance and avoiding deadlocks.
			// runnableApps can be in unsorted state because of this section,
			// but we can accept it in practice since the probability is low.
			readLock.Lock();
			try
			{
				foreach (FSAppAttempt sched in runnableApps)
				{
					if (SchedulerAppUtils.IsBlacklisted(sched, node, Log))
					{
						continue;
					}
					assigned = sched.AssignContainer(node);
					if (!assigned.Equals(Resources.None()))
					{
						break;
					}
				}
			}
			finally
			{
				readLock.Unlock();
			}
			return assigned;
		}

		public override RMContainer PreemptContainer()
		{
			RMContainer toBePreempted = null;
			// If this queue is not over its fair share, reject
			if (!PreemptContainerPreCheck())
			{
				return toBePreempted;
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Queue " + GetName() + " is going to preempt a container " + "from its applications."
					);
			}
			// Choose the app that is most over fair share
			IComparer<Schedulable> comparator = policy.GetComparator();
			FSAppAttempt candidateSched = null;
			readLock.Lock();
			try
			{
				foreach (FSAppAttempt sched in runnableApps)
				{
					if (candidateSched == null || comparator.Compare(sched, candidateSched) > 0)
					{
						candidateSched = sched;
					}
				}
			}
			finally
			{
				readLock.Unlock();
			}
			// Preempt from the selected app
			if (candidateSched != null)
			{
				toBePreempted = candidateSched.PreemptContainer();
			}
			return toBePreempted;
		}

		public override IList<FSQueue> GetChildQueues()
		{
			return new AList<FSQueue>(1);
		}

		public override IList<QueueUserACLInfo> GetQueueUserAclInfo(UserGroupInformation 
			user)
		{
			QueueUserACLInfo userAclInfo = recordFactory.NewRecordInstance<QueueUserACLInfo>(
				);
			IList<QueueACL> operations = new AList<QueueACL>();
			foreach (QueueACL operation in QueueACL.Values())
			{
				if (HasAccess(operation, user))
				{
					operations.AddItem(operation);
				}
			}
			userAclInfo.SetQueueName(GetQueueName());
			userAclInfo.SetUserAcls(operations);
			return Sharpen.Collections.SingletonList(userAclInfo);
		}

		public virtual long GetLastTimeAtMinShare()
		{
			return lastTimeAtMinShare;
		}

		private void SetLastTimeAtMinShare(long lastTimeAtMinShare)
		{
			this.lastTimeAtMinShare = lastTimeAtMinShare;
		}

		public virtual long GetLastTimeAtFairShareThreshold()
		{
			return lastTimeAtFairShareThreshold;
		}

		private void SetLastTimeAtFairShareThreshold(long lastTimeAtFairShareThreshold)
		{
			this.lastTimeAtFairShareThreshold = lastTimeAtFairShareThreshold;
		}

		public override int GetNumRunnableApps()
		{
			readLock.Lock();
			try
			{
				return runnableApps.Count;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual int GetNumNonRunnableApps()
		{
			readLock.Lock();
			try
			{
				return nonRunnableApps.Count;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual int GetNumPendingApps()
		{
			int numPendingApps = 0;
			readLock.Lock();
			try
			{
				foreach (FSAppAttempt attempt in runnableApps)
				{
					if (attempt.IsPending())
					{
						numPendingApps++;
					}
				}
				numPendingApps += nonRunnableApps.Count;
			}
			finally
			{
				readLock.Unlock();
			}
			return numPendingApps;
		}

		/// <summary>
		/// TODO: Based on how frequently this is called, we might want to club
		/// counting pending and active apps in the same method.
		/// </summary>
		public virtual int GetNumActiveApps()
		{
			int numActiveApps = 0;
			readLock.Lock();
			try
			{
				foreach (FSAppAttempt attempt in runnableApps)
				{
					if (!attempt.IsPending())
					{
						numActiveApps++;
					}
				}
			}
			finally
			{
				readLock.Unlock();
			}
			return numActiveApps;
		}

		public override ActiveUsersManager GetActiveUsersManager()
		{
			return activeUsersManager;
		}

		/// <summary>
		/// Check whether this queue can run this application master under the
		/// maxAMShare limit
		/// </summary>
		/// <param name="amResource"/>
		/// <returns>true if this queue can run</returns>
		public virtual bool CanRunAppAM(Org.Apache.Hadoop.Yarn.Api.Records.Resource amResource
			)
		{
			float maxAMShare = scheduler.GetAllocationConfiguration().GetQueueMaxAMShare(GetName
				());
			if (Math.Abs(maxAMShare - -1.0f) < 0.0001)
			{
				return true;
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAMResource = Resources.Multiply(GetFairShare
				(), maxAMShare);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource ifRunAMResource = Resources.Add(amResourceUsage
				, amResource);
			return !policy.CheckIfAMResourceUsageOverLimit(ifRunAMResource, maxAMResource);
		}

		public virtual void AddAMResourceUsage(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 amResource)
		{
			if (amResource != null)
			{
				Resources.AddTo(amResourceUsage, amResource);
			}
		}

		public override void RecoverContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer
			)
		{
		}

		// TODO Auto-generated method stub
		/// <summary>Update the preemption fields for the queue, i.e.</summary>
		/// <remarks>
		/// Update the preemption fields for the queue, i.e. the times since last was
		/// at its guaranteed share and over its fair share threshold.
		/// </remarks>
		public virtual void UpdateStarvationStats()
		{
			long now = scheduler.GetClock().GetTime();
			if (!IsStarvedForMinShare())
			{
				SetLastTimeAtMinShare(now);
			}
			if (!IsStarvedForFairShare())
			{
				SetLastTimeAtFairShareThreshold(now);
			}
		}

		/// <summary>
		/// Allows setting weight for a dynamically created queue
		/// Currently only used for reservation based queues
		/// </summary>
		/// <param name="weight">queue weight</param>
		public virtual void SetWeights(float weight)
		{
			scheduler.GetAllocationConfiguration().SetQueueWeight(GetName(), new ResourceWeights
				(weight));
		}

		/// <summary>Helper method to check if the queue should preempt containers</summary>
		/// <returns>true if check passes (can preempt) or false otherwise</returns>
		private bool PreemptContainerPreCheck()
		{
			return parent.GetPolicy().CheckIfUsageOverFairShare(GetResourceUsage(), GetFairShare
				());
		}

		/// <summary>Is a queue being starved for its min share.</summary>
		[VisibleForTesting]
		internal virtual bool IsStarvedForMinShare()
		{
			return IsStarved(GetMinShare());
		}

		/// <summary>Is a queue being starved for its fair share threshold.</summary>
		[VisibleForTesting]
		internal virtual bool IsStarvedForFairShare()
		{
			return IsStarved(Resources.Multiply(GetFairShare(), GetFairSharePreemptionThreshold
				()));
		}

		private bool IsStarved(Org.Apache.Hadoop.Yarn.Api.Records.Resource share)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource desiredShare = Resources.Min(scheduler
				.GetResourceCalculator(), scheduler.GetClusterResource(), share, GetDemand());
			return Resources.LessThan(scheduler.GetResourceCalculator(), scheduler.GetClusterResource
				(), GetResourceUsage(), desiredShare);
		}
	}
}
