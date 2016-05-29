using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs
{
	public class WritingApplicationAttemptFinishEvent : WritingApplicationHistoryEvent
	{
		private ApplicationAttemptId appAttemptId;

		private ApplicationAttemptFinishData appAttemptFinish;

		public WritingApplicationAttemptFinishEvent(ApplicationAttemptId appAttemptId, ApplicationAttemptFinishData
			 appAttemptFinish)
			: base(WritingHistoryEventType.AppAttemptFinish)
		{
			this.appAttemptId = appAttemptId;
			this.appAttemptFinish = appAttemptFinish;
		}

		public override int GetHashCode()
		{
			return appAttemptId.GetApplicationId().GetHashCode();
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return appAttemptId;
		}

		public virtual ApplicationAttemptFinishData GetApplicationAttemptFinishData()
		{
			return appAttemptFinish;
		}
	}
}
