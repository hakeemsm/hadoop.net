using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs
{
	public class WritingApplicationStartEvent : WritingApplicationHistoryEvent
	{
		private ApplicationId appId;

		private ApplicationStartData appStart;

		public WritingApplicationStartEvent(ApplicationId appId, ApplicationStartData appStart
			)
			: base(WritingHistoryEventType.AppStart)
		{
			this.appId = appId;
			this.appStart = appStart;
		}

		public override int GetHashCode()
		{
			return appId.GetHashCode();
		}

		public virtual ApplicationId GetApplicationId()
		{
			return appId;
		}

		public virtual ApplicationStartData GetApplicationStartData()
		{
			return appStart;
		}
	}
}
