using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs
{
	public class WritingApplicationAttemptStartEvent : WritingApplicationHistoryEvent
	{
		private ApplicationAttemptId appAttemptId;

		private ApplicationAttemptStartData appAttemptStart;

		public WritingApplicationAttemptStartEvent(ApplicationAttemptId appAttemptId, ApplicationAttemptStartData
			 appAttemptStart)
			: base(WritingHistoryEventType.AppAttemptStart)
		{
			this.appAttemptId = appAttemptId;
			this.appAttemptStart = appAttemptStart;
		}

		public override int GetHashCode()
		{
			return appAttemptId.GetApplicationId().GetHashCode();
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return appAttemptId;
		}

		public virtual ApplicationAttemptStartData GetApplicationAttemptStartData()
		{
			return appAttemptStart;
		}
	}
}
