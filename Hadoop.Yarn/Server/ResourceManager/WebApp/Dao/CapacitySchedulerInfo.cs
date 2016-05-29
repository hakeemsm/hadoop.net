using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class CapacitySchedulerInfo : SchedulerInfo
	{
		protected internal float capacity;

		protected internal float usedCapacity;

		protected internal float maxCapacity;

		protected internal string queueName;

		protected internal CapacitySchedulerQueueInfoList queues;

		[XmlTransient]
		internal const float Epsilon = 1e-8f;

		public CapacitySchedulerInfo()
		{
		}

		public CapacitySchedulerInfo(CSQueue parent)
		{
			// JAXB needs this
			this.queueName = parent.GetQueueName();
			this.usedCapacity = parent.GetUsedCapacity() * 100;
			this.capacity = parent.GetCapacity() * 100;
			float max = parent.GetMaximumCapacity();
			if (max < Epsilon || max > 1f)
			{
				max = 1f;
			}
			this.maxCapacity = max * 100;
			queues = GetQueues(parent);
		}

		public virtual float GetCapacity()
		{
			return this.capacity;
		}

		public virtual float GetUsedCapacity()
		{
			return this.usedCapacity;
		}

		public virtual float GetMaxCapacity()
		{
			return this.maxCapacity;
		}

		public virtual string GetQueueName()
		{
			return this.queueName;
		}

		public virtual CapacitySchedulerQueueInfoList GetQueues()
		{
			return this.queues;
		}

		protected internal virtual CapacitySchedulerQueueInfoList GetQueues(CSQueue parent
			)
		{
			CSQueue parentQueue = parent;
			CapacitySchedulerQueueInfoList queuesInfo = new CapacitySchedulerQueueInfoList();
			foreach (CSQueue queue in parentQueue.GetChildQueues())
			{
				CapacitySchedulerQueueInfo info;
				if (queue is LeafQueue)
				{
					info = new CapacitySchedulerLeafQueueInfo((LeafQueue)queue);
				}
				else
				{
					info = new CapacitySchedulerQueueInfo(queue);
					info.queues = GetQueues(queue);
				}
				queuesInfo.AddToQueueInfoList(info);
			}
			return queuesInfo;
		}
	}
}
