using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class CapacityHeadroomProvider
	{
		internal LeafQueue.User user;

		internal LeafQueue queue;

		internal FiCaSchedulerApp application;

		internal Resource required;

		internal LeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo;

		public CapacityHeadroomProvider(LeafQueue.User user, LeafQueue queue, FiCaSchedulerApp
			 application, Resource required, LeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo
			)
		{
			this.user = user;
			this.queue = queue;
			this.application = application;
			this.required = required;
			this.queueResourceLimitsInfo = queueResourceLimitsInfo;
		}

		public virtual Resource GetHeadroom()
		{
			Resource queueCurrentLimit;
			Resource clusterResource;
			lock (queueResourceLimitsInfo)
			{
				queueCurrentLimit = queueResourceLimitsInfo.GetQueueCurrentLimit();
				clusterResource = queueResourceLimitsInfo.GetClusterResource();
			}
			Resource headroom = queue.GetHeadroom(user, queueCurrentLimit, clusterResource, application
				, required);
			// Corner case to deal with applications being slightly over-limit
			if (headroom.GetMemory() < 0)
			{
				headroom.SetMemory(0);
			}
			return headroom;
		}
	}
}
