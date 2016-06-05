using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class AppAttemptRemovedSchedulerEvent : SchedulerEvent
	{
		private readonly ApplicationAttemptId applicationAttemptId;

		private readonly RMAppAttemptState finalAttemptState;

		private readonly bool keepContainersAcrossAppAttempts;

		public AppAttemptRemovedSchedulerEvent(ApplicationAttemptId applicationAttemptId, 
			RMAppAttemptState finalAttemptState, bool keepContainers)
			: base(SchedulerEventType.AppAttemptRemoved)
		{
			this.applicationAttemptId = applicationAttemptId;
			this.finalAttemptState = finalAttemptState;
			this.keepContainersAcrossAppAttempts = keepContainers;
		}

		public virtual ApplicationAttemptId GetApplicationAttemptID()
		{
			return this.applicationAttemptId;
		}

		public virtual RMAppAttemptState GetFinalAttemptState()
		{
			return this.finalAttemptState;
		}

		public virtual bool GetKeepContainersAcrossAppAttempts()
		{
			return this.keepContainersAcrossAppAttempts;
		}
	}
}
