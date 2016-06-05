using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class FairSchedulerLeafQueueInfo : FairSchedulerQueueInfo
	{
		private int numPendingApps;

		private int numActiveApps;

		public FairSchedulerLeafQueueInfo()
		{
		}

		public FairSchedulerLeafQueueInfo(FSLeafQueue queue, FairScheduler scheduler)
			: base(queue, scheduler)
		{
			numPendingApps = queue.GetNumPendingApps();
			numActiveApps = queue.GetNumActiveApps();
		}

		public virtual int GetNumActiveApplications()
		{
			return numActiveApps;
		}

		public virtual int GetNumPendingApplications()
		{
			return numPendingApps;
		}
	}
}
