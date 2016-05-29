using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class AppAttemptAddedSchedulerEvent : SchedulerEvent
	{
		private readonly ApplicationAttemptId applicationAttemptId;

		private readonly bool transferStateFromPreviousAttempt;

		private readonly bool isAttemptRecovering;

		public AppAttemptAddedSchedulerEvent(ApplicationAttemptId applicationAttemptId, bool
			 transferStateFromPreviousAttempt)
			: this(applicationAttemptId, transferStateFromPreviousAttempt, false)
		{
		}

		public AppAttemptAddedSchedulerEvent(ApplicationAttemptId applicationAttemptId, bool
			 transferStateFromPreviousAttempt, bool isAttemptRecovering)
			: base(SchedulerEventType.AppAttemptAdded)
		{
			this.applicationAttemptId = applicationAttemptId;
			this.transferStateFromPreviousAttempt = transferStateFromPreviousAttempt;
			this.isAttemptRecovering = isAttemptRecovering;
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return applicationAttemptId;
		}

		public virtual bool GetTransferStateFromPreviousAttempt()
		{
			return transferStateFromPreviousAttempt;
		}

		public virtual bool GetIsAttemptRecovering()
		{
			return isAttemptRecovering;
		}
	}
}
