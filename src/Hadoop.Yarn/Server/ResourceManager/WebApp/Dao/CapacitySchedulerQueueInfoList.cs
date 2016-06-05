using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class CapacitySchedulerQueueInfoList
	{
		protected internal AList<CapacitySchedulerQueueInfo> queue;

		public CapacitySchedulerQueueInfoList()
		{
			queue = new AList<CapacitySchedulerQueueInfo>();
		}

		public virtual AList<CapacitySchedulerQueueInfo> GetQueueInfoList()
		{
			return this.queue;
		}

		public virtual bool AddToQueueInfoList(CapacitySchedulerQueueInfo e)
		{
			return this.queue.AddItem(e);
		}

		public virtual CapacitySchedulerQueueInfo GetQueueInfo(int i)
		{
			return this.queue[i];
		}
	}
}
