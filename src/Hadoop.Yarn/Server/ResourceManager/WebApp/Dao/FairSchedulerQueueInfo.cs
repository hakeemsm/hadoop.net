using System.Collections.Generic;
using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class FairSchedulerQueueInfo
	{
		private int maxApps;

		[XmlTransient]
		private float fractionMemUsed;

		[XmlTransient]
		private float fractionMemSteadyFairShare;

		[XmlTransient]
		private float fractionMemFairShare;

		[XmlTransient]
		private float fractionMemMinShare;

		[XmlTransient]
		private float fractionMemMaxShare;

		private ResourceInfo minResources;

		private ResourceInfo maxResources;

		private ResourceInfo usedResources;

		private ResourceInfo steadyFairResources;

		private ResourceInfo fairResources;

		private ResourceInfo clusterResources;

		private string queueName;

		private string schedulingPolicy;

		private ICollection<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao.FairSchedulerQueueInfo
			> childQueues;

		public FairSchedulerQueueInfo()
		{
		}

		public FairSchedulerQueueInfo(FSQueue queue, FairScheduler scheduler)
		{
			AllocationConfiguration allocConf = scheduler.GetAllocationConfiguration();
			queueName = queue.GetName();
			schedulingPolicy = queue.GetPolicy().GetName();
			clusterResources = new ResourceInfo(scheduler.GetClusterResource());
			usedResources = new ResourceInfo(queue.GetResourceUsage());
			fractionMemUsed = (float)usedResources.GetMemory() / clusterResources.GetMemory();
			steadyFairResources = new ResourceInfo(queue.GetSteadyFairShare());
			fairResources = new ResourceInfo(queue.GetFairShare());
			minResources = new ResourceInfo(queue.GetMinShare());
			maxResources = new ResourceInfo(queue.GetMaxShare());
			maxResources = new ResourceInfo(Resources.ComponentwiseMin(queue.GetMaxShare(), scheduler
				.GetClusterResource()));
			fractionMemSteadyFairShare = (float)steadyFairResources.GetMemory() / clusterResources
				.GetMemory();
			fractionMemFairShare = (float)fairResources.GetMemory() / clusterResources.GetMemory
				();
			fractionMemMinShare = (float)minResources.GetMemory() / clusterResources.GetMemory
				();
			fractionMemMaxShare = (float)maxResources.GetMemory() / clusterResources.GetMemory
				();
			maxApps = allocConf.GetQueueMaxApps(queueName);
			childQueues = new AList<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao.FairSchedulerQueueInfo
				>();
			if (allocConf.IsReservable(queueName) && !allocConf.GetShowReservationAsQueues(queueName
				))
			{
				return;
			}
			ICollection<FSQueue> children = queue.GetChildQueues();
			foreach (FSQueue child in children)
			{
				if (child is FSLeafQueue)
				{
					childQueues.AddItem(new FairSchedulerLeafQueueInfo((FSLeafQueue)child, scheduler)
						);
				}
				else
				{
					childQueues.AddItem(new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao.FairSchedulerQueueInfo
						(child, scheduler));
				}
			}
		}

		/// <summary>Returns the steady fair share as a fraction of the entire cluster capacity.
		/// 	</summary>
		public virtual float GetSteadyFairShareMemoryFraction()
		{
			return fractionMemSteadyFairShare;
		}

		/// <summary>Returns the fair share as a fraction of the entire cluster capacity.</summary>
		public virtual float GetFairShareMemoryFraction()
		{
			return fractionMemFairShare;
		}

		/// <summary>Returns the steady fair share of this queue in megabytes.</summary>
		public virtual ResourceInfo GetSteadyFairShare()
		{
			return steadyFairResources;
		}

		/// <summary>Returns the fair share of this queue in megabytes</summary>
		public virtual ResourceInfo GetFairShare()
		{
			return fairResources;
		}

		public virtual ResourceInfo GetMinResources()
		{
			return minResources;
		}

		public virtual ResourceInfo GetMaxResources()
		{
			return maxResources;
		}

		public virtual int GetMaxApplications()
		{
			return maxApps;
		}

		public virtual string GetQueueName()
		{
			return queueName;
		}

		public virtual ResourceInfo GetUsedResources()
		{
			return usedResources;
		}

		/// <summary>
		/// Returns the queue's min share in as a fraction of the entire
		/// cluster capacity.
		/// </summary>
		public virtual float GetMinShareMemoryFraction()
		{
			return fractionMemMinShare;
		}

		/// <summary>
		/// Returns the memory used by this queue as a fraction of the entire
		/// cluster capacity.
		/// </summary>
		public virtual float GetUsedMemoryFraction()
		{
			return fractionMemUsed;
		}

		/// <summary>
		/// Returns the capacity of this queue as a fraction of the entire cluster
		/// capacity.
		/// </summary>
		public virtual float GetMaxResourcesFraction()
		{
			return fractionMemMaxShare;
		}

		/// <summary>Returns the name of the scheduling policy used by this queue.</summary>
		public virtual string GetSchedulingPolicy()
		{
			return schedulingPolicy;
		}

		public virtual ICollection<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao.FairSchedulerQueueInfo
			> GetChildQueues()
		{
			return childQueues;
		}
	}
}
