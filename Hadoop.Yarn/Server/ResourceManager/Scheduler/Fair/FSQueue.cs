using System.Collections.Generic;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public abstract class FSQueue : Queue, Schedulable
	{
		private Resource fairShare = Resources.CreateResource(0, 0);

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource steadyFairShare = Resources.CreateResource
			(0, 0);

		private readonly string name;

		protected internal readonly FairScheduler scheduler;

		private readonly FSQueueMetrics metrics;

		protected internal readonly FSParentQueue parent;

		protected internal readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		protected internal SchedulingPolicy policy = SchedulingPolicy.DefaultPolicy;

		private long fairSharePreemptionTimeout = long.MaxValue;

		private long minSharePreemptionTimeout = long.MaxValue;

		private float fairSharePreemptionThreshold = 0.5f;

		public FSQueue(string name, FairScheduler scheduler, FSParentQueue parent)
		{
			this.name = name;
			this.scheduler = scheduler;
			this.metrics = ((FSQueueMetrics)FSQueueMetrics.ForQueue(GetName(), parent, true, 
				scheduler.GetConf()));
			metrics.SetMinShare(GetMinShare());
			metrics.SetMaxShare(GetMaxShare());
			this.parent = parent;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual string GetQueueName()
		{
			return name;
		}

		public virtual SchedulingPolicy GetPolicy()
		{
			return policy;
		}

		public virtual FSParentQueue GetParent()
		{
			return parent;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		protected internal virtual void ThrowPolicyDoesnotApplyException(SchedulingPolicy
			 policy)
		{
			throw new AllocationConfigurationException("SchedulingPolicy " + policy + " does not apply to queue "
				 + GetName());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public abstract void SetPolicy(SchedulingPolicy policy);

		public virtual ResourceWeights GetWeights()
		{
			return scheduler.GetAllocationConfiguration().GetQueueWeight(GetName());
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinShare()
		{
			return scheduler.GetAllocationConfiguration().GetMinResources(GetName());
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaxShare()
		{
			return scheduler.GetAllocationConfiguration().GetMaxResources(GetName());
		}

		public virtual long GetStartTime()
		{
			return 0;
		}

		public virtual Priority GetPriority()
		{
			Priority p = recordFactory.NewRecordInstance<Priority>();
			p.SetPriority(1);
			return p;
		}

		public virtual QueueInfo GetQueueInfo(bool includeChildQueues, bool recursive)
		{
			QueueInfo queueInfo = recordFactory.NewRecordInstance<QueueInfo>();
			queueInfo.SetQueueName(GetQueueName());
			if (scheduler.GetClusterResource().GetMemory() == 0)
			{
				queueInfo.SetCapacity(0.0f);
			}
			else
			{
				queueInfo.SetCapacity((float)GetFairShare().GetMemory() / scheduler.GetClusterResource
					().GetMemory());
			}
			if (GetFairShare().GetMemory() == 0)
			{
				queueInfo.SetCurrentCapacity(0.0f);
			}
			else
			{
				queueInfo.SetCurrentCapacity((float)GetResourceUsage().GetMemory() / GetFairShare
					().GetMemory());
			}
			AList<QueueInfo> childQueueInfos = new AList<QueueInfo>();
			if (includeChildQueues)
			{
				ICollection<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSQueue>
					 childQueues = GetChildQueues();
				foreach (Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSQueue child
					 in childQueues)
				{
					childQueueInfos.AddItem(child.GetQueueInfo(recursive, recursive));
				}
			}
			queueInfo.SetChildQueues(childQueueInfos);
			queueInfo.SetQueueState(QueueState.Running);
			return queueInfo;
		}

		public virtual FSQueueMetrics GetMetrics()
		{
			return metrics;
		}

		/// <summary>Get the fair share assigned to this Schedulable.</summary>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetFairShare()
		{
			return fairShare;
		}

		public virtual void SetFairShare(Org.Apache.Hadoop.Yarn.Api.Records.Resource fairShare
			)
		{
			this.fairShare = fairShare;
			metrics.SetFairShare(fairShare);
		}

		/// <summary>Get the steady fair share assigned to this Schedulable.</summary>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetSteadyFairShare()
		{
			return steadyFairShare;
		}

		public virtual void SetSteadyFairShare(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 steadyFairShare)
		{
			this.steadyFairShare = steadyFairShare;
			metrics.SetSteadyFairShare(steadyFairShare);
		}

		public virtual bool HasAccess(QueueACL acl, UserGroupInformation user)
		{
			return scheduler.GetAllocationConfiguration().HasAccess(name, acl, user);
		}

		public virtual long GetFairSharePreemptionTimeout()
		{
			return fairSharePreemptionTimeout;
		}

		public virtual void SetFairSharePreemptionTimeout(long fairSharePreemptionTimeout
			)
		{
			this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
		}

		public virtual long GetMinSharePreemptionTimeout()
		{
			return minSharePreemptionTimeout;
		}

		public virtual void SetMinSharePreemptionTimeout(long minSharePreemptionTimeout)
		{
			this.minSharePreemptionTimeout = minSharePreemptionTimeout;
		}

		public virtual float GetFairSharePreemptionThreshold()
		{
			return fairSharePreemptionThreshold;
		}

		public virtual void SetFairSharePreemptionThreshold(float fairSharePreemptionThreshold
			)
		{
			this.fairSharePreemptionThreshold = fairSharePreemptionThreshold;
		}

		/// <summary>
		/// Recomputes the shares for all child queues and applications based on this
		/// queue's current share
		/// </summary>
		public abstract void RecomputeShares();

		/// <summary>Update the min/fair share preemption timeouts and threshold for this queue.
		/// 	</summary>
		public virtual void UpdatePreemptionVariables()
		{
			// For min share timeout
			minSharePreemptionTimeout = scheduler.GetAllocationConfiguration().GetMinSharePreemptionTimeout
				(GetName());
			if (minSharePreemptionTimeout == -1 && parent != null)
			{
				minSharePreemptionTimeout = parent.GetMinSharePreemptionTimeout();
			}
			// For fair share timeout
			fairSharePreemptionTimeout = scheduler.GetAllocationConfiguration().GetFairSharePreemptionTimeout
				(GetName());
			if (fairSharePreemptionTimeout == -1 && parent != null)
			{
				fairSharePreemptionTimeout = parent.GetFairSharePreemptionTimeout();
			}
			// For fair share preemption threshold
			fairSharePreemptionThreshold = scheduler.GetAllocationConfiguration().GetFairSharePreemptionThreshold
				(GetName());
			if (fairSharePreemptionThreshold < 0 && parent != null)
			{
				fairSharePreemptionThreshold = parent.GetFairSharePreemptionThreshold();
			}
		}

		/// <summary>Gets the children of this queue, if any.</summary>
		public abstract IList<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSQueue
			> GetChildQueues();

		/// <summary>Adds all applications in the queue and its subqueues to the given collection.
		/// 	</summary>
		/// <param name="apps">the collection to add the applications to</param>
		public abstract void CollectSchedulerApplications(ICollection<ApplicationAttemptId
			> apps);

		/// <summary>Return the number of apps for which containers can be allocated.</summary>
		/// <remarks>
		/// Return the number of apps for which containers can be allocated.
		/// Includes apps in subqueues.
		/// </remarks>
		public abstract int GetNumRunnableApps();

		/// <summary>Helper method to check if the queue should attempt assigning resources</summary>
		/// <returns>true if check passes (can assign) or false otherwise</returns>
		protected internal virtual bool AssignContainerPreCheck(FSSchedulerNode node)
		{
			if (!Resources.FitsIn(GetResourceUsage(), scheduler.GetAllocationConfiguration().
				GetMaxResources(GetName())) || node.GetReservedContainer() != null)
			{
				return false;
			}
			return true;
		}

		/// <summary>Returns true if queue has at least one app running.</summary>
		public virtual bool IsActive()
		{
			return GetNumRunnableApps() > 0;
		}

		/// <summary>Convenient toString implementation for debugging.</summary>
		public override string ToString()
		{
			return string.Format("[%s, demand=%s, running=%s, share=%s, w=%s]", GetName(), GetDemand
				(), GetResourceUsage(), fairShare, GetWeights());
		}

		public virtual ICollection<string> GetAccessibleNodeLabels()
		{
			// TODO, add implementation for FS
			return null;
		}

		public virtual string GetDefaultNodeLabelExpression()
		{
			// TODO, add implementation for FS
			return null;
		}

		public abstract ActiveUsersManager GetActiveUsersManager();

		public abstract IList<QueueUserACLInfo> GetQueueUserAclInfo(UserGroupInformation 
			arg1);

		public abstract void RecoverContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 arg1, SchedulerApplicationAttempt arg2, RMContainer arg3);

		public abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignContainer(FSSchedulerNode
			 arg1);

		public abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource GetDemand();

		public abstract Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResourceUsage();

		public abstract RMContainer PreemptContainer();

		public abstract void UpdateDemand();
	}
}
