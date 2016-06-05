using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class FSParentQueue : FSQueue
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSParentQueue
			).FullName);

		private readonly IList<FSQueue> childQueues = new AList<FSQueue>();

		private Resource demand = Resources.CreateResource(0);

		private int runnableApps;

		public FSParentQueue(string name, FairScheduler scheduler, Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSParentQueue
			 parent)
			: base(name, scheduler, parent)
		{
		}

		public virtual void AddChildQueue(FSQueue child)
		{
			childQueues.AddItem(child);
		}

		public override void RecomputeShares()
		{
			policy.ComputeShares(childQueues, GetFairShare());
			foreach (FSQueue childQueue in childQueues)
			{
				childQueue.GetMetrics().SetFairShare(childQueue.GetFairShare());
				childQueue.RecomputeShares();
			}
		}

		public virtual void RecomputeSteadyShares()
		{
			policy.ComputeSteadyShares(childQueues, GetSteadyFairShare());
			foreach (FSQueue childQueue in childQueues)
			{
				childQueue.GetMetrics().SetSteadyFairShare(childQueue.GetSteadyFairShare());
				if (childQueue is Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSParentQueue)
				{
					((Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSParentQueue)childQueue
						).RecomputeSteadyShares();
				}
			}
		}

		public override void UpdatePreemptionVariables()
		{
			base.UpdatePreemptionVariables();
			// For child queues
			foreach (FSQueue childQueue in childQueues)
			{
				childQueue.UpdatePreemptionVariables();
			}
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetDemand()
		{
			return demand;
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResourceUsage()
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource usage = Resources.CreateResource(0);
			foreach (FSQueue child in childQueues)
			{
				Resources.AddTo(usage, child.GetResourceUsage());
			}
			return usage;
		}

		public override void UpdateDemand()
		{
			// Compute demand by iterating through apps in the queue
			// Limit demand to maxResources
			Org.Apache.Hadoop.Yarn.Api.Records.Resource maxRes = scheduler.GetAllocationConfiguration
				().GetMaxResources(GetName());
			demand = Resources.CreateResource(0);
			foreach (FSQueue childQueue in childQueues)
			{
				childQueue.UpdateDemand();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource toAdd = childQueue.GetDemand();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Counting resource from " + childQueue.GetName() + " " + toAdd + "; Total resource consumption for "
						 + GetName() + " now " + demand);
				}
				demand = Resources.Add(demand, toAdd);
				demand = Resources.ComponentwiseMin(demand, maxRes);
				if (Resources.Equals(demand, maxRes))
				{
					break;
				}
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("The updated demand for " + GetName() + " is " + demand + "; the max is "
					 + maxRes);
			}
		}

		private QueueUserACLInfo GetUserAclInfo(UserGroupInformation user)
		{
			lock (this)
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
				return userAclInfo;
			}
		}

		public override IList<QueueUserACLInfo> GetQueueUserAclInfo(UserGroupInformation 
			user)
		{
			lock (this)
			{
				IList<QueueUserACLInfo> userAcls = new AList<QueueUserACLInfo>();
				// Add queue acls
				userAcls.AddItem(GetUserAclInfo(user));
				// Add children queue acls
				foreach (FSQueue child in childQueues)
				{
					Sharpen.Collections.AddAll(userAcls, child.GetQueueUserAclInfo(user));
				}
				return userAcls;
			}
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignContainer(FSSchedulerNode
			 node)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource assigned = Resources.None();
			// If this queue is over its limit, reject
			if (!AssignContainerPreCheck(node))
			{
				return assigned;
			}
			childQueues.Sort(policy.GetComparator());
			foreach (FSQueue child in childQueues)
			{
				assigned = child.AssignContainer(node);
				if (!Resources.Equals(assigned, Resources.None()))
				{
					break;
				}
			}
			return assigned;
		}

		public override RMContainer PreemptContainer()
		{
			RMContainer toBePreempted = null;
			// Find the childQueue which is most over fair share
			FSQueue candidateQueue = null;
			IComparer<Schedulable> comparator = policy.GetComparator();
			foreach (FSQueue queue in childQueues)
			{
				if (candidateQueue == null || comparator.Compare(queue, candidateQueue) > 0)
				{
					candidateQueue = queue;
				}
			}
			// Let the selected queue choose which of its container to preempt
			if (candidateQueue != null)
			{
				toBePreempted = candidateQueue.PreemptContainer();
			}
			return toBePreempted;
		}

		public override IList<FSQueue> GetChildQueues()
		{
			return childQueues;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public override void SetPolicy(SchedulingPolicy policy)
		{
			bool allowed = SchedulingPolicy.IsApplicableTo(policy, (parent == null) ? SchedulingPolicy
				.DepthRoot : SchedulingPolicy.DepthIntermediate);
			if (!allowed)
			{
				ThrowPolicyDoesnotApplyException(policy);
			}
			base.policy = policy;
		}

		public virtual void IncrementRunnableApps()
		{
			runnableApps++;
		}

		public virtual void DecrementRunnableApps()
		{
			runnableApps--;
		}

		public override int GetNumRunnableApps()
		{
			return runnableApps;
		}

		public override void CollectSchedulerApplications(ICollection<ApplicationAttemptId
			> apps)
		{
			foreach (FSQueue childQueue in childQueues)
			{
				childQueue.CollectSchedulerApplications(apps);
			}
		}

		public override ActiveUsersManager GetActiveUsersManager()
		{
			// Should never be called since all applications are submitted to LeafQueues
			return null;
		}

		public override void RecoverContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer
			)
		{
		}
		// TODO Auto-generated method stub
	}
}
