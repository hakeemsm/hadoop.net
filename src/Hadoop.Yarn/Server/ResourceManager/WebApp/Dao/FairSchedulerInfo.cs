using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class FairSchedulerInfo : SchedulerInfo
	{
		public const int InvalidFairShare = -1;

		private FairSchedulerQueueInfo rootQueue;

		[XmlTransient]
		private FairScheduler scheduler;

		public FairSchedulerInfo()
		{
		}

		public FairSchedulerInfo(FairScheduler fs)
		{
			// JAXB needs this
			scheduler = fs;
			rootQueue = new FairSchedulerQueueInfo(scheduler.GetQueueManager().GetRootQueue()
				, scheduler);
		}

		/// <summary>Get the fair share assigned to the appAttemptId.</summary>
		/// <param name="appAttemptId"/>
		/// <returns>
		/// The fair share assigned to the appAttemptId,
		/// <code>FairSchedulerInfo#INVALID_FAIR_SHARE</code> if the scheduler does
		/// not know about this application attempt.
		/// </returns>
		public virtual int GetAppFairShare(ApplicationAttemptId appAttemptId)
		{
			FSAppAttempt fsAppAttempt = scheduler.GetSchedulerApp(appAttemptId);
			return fsAppAttempt == null ? InvalidFairShare : fsAppAttempt.GetFairShare().GetMemory
				();
		}

		public virtual FairSchedulerQueueInfo GetRootQueueInfo()
		{
			return rootQueue;
		}
	}
}
