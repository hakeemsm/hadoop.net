using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class SchedulerTypeInfo
	{
		protected internal SchedulerInfo schedulerInfo;

		public SchedulerTypeInfo()
		{
		}

		public SchedulerTypeInfo(SchedulerInfo scheduler)
		{
			// JAXB needs this
			this.schedulerInfo = scheduler;
		}
	}
}
