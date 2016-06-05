using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class SchedulerInfo
	{
		protected internal string schedulerName;

		protected internal ResourceInfo minAllocResource;

		protected internal ResourceInfo maxAllocResource;

		protected internal EnumSet<YarnServiceProtos.SchedulerResourceTypes> schedulingResourceTypes;

		public SchedulerInfo()
		{
		}

		public SchedulerInfo(ResourceManager rm)
		{
			// JAXB needs this
			ResourceScheduler rs = rm.GetResourceScheduler();
			if (rs is CapacityScheduler)
			{
				this.schedulerName = "Capacity Scheduler";
			}
			else
			{
				if (rs is FairScheduler)
				{
					this.schedulerName = "Fair Scheduler";
				}
				else
				{
					if (rs is FifoScheduler)
					{
						this.schedulerName = "Fifo Scheduler";
					}
				}
			}
			this.minAllocResource = new ResourceInfo(rs.GetMinimumResourceCapability());
			this.maxAllocResource = new ResourceInfo(rs.GetMaximumResourceCapability());
			this.schedulingResourceTypes = rs.GetSchedulingResourceTypes();
		}

		public virtual string GetSchedulerType()
		{
			return this.schedulerName;
		}

		public virtual ResourceInfo GetMinAllocation()
		{
			return this.minAllocResource;
		}

		public virtual ResourceInfo GetMaxAllocation()
		{
			return this.maxAllocResource;
		}

		public virtual string GetSchedulerResourceTypes()
		{
			return this.schedulingResourceTypes.ToString();
		}
	}
}
