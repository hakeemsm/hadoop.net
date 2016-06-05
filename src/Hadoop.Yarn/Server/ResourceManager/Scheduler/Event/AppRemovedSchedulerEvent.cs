using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class AppRemovedSchedulerEvent : SchedulerEvent
	{
		private readonly ApplicationId applicationId;

		private readonly RMAppState finalState;

		public AppRemovedSchedulerEvent(ApplicationId applicationId, RMAppState finalState
			)
			: base(SchedulerEventType.AppRemoved)
		{
			this.applicationId = applicationId;
			this.finalState = finalState;
		}

		public virtual ApplicationId GetApplicationID()
		{
			return this.applicationId;
		}

		public virtual RMAppState GetFinalState()
		{
			return this.finalState;
		}
	}
}
