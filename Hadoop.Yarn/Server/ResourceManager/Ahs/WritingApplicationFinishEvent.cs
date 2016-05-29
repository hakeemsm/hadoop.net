using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs
{
	public class WritingApplicationFinishEvent : WritingApplicationHistoryEvent
	{
		private ApplicationId appId;

		private ApplicationFinishData appFinish;

		public WritingApplicationFinishEvent(ApplicationId appId, ApplicationFinishData appFinish
			)
			: base(WritingHistoryEventType.AppFinish)
		{
			this.appId = appId;
			this.appFinish = appFinish;
		}

		public override int GetHashCode()
		{
			return appId.GetHashCode();
		}

		public virtual ApplicationId GetApplicationId()
		{
			return appId;
		}

		public virtual ApplicationFinishData GetApplicationFinishData()
		{
			return appFinish;
		}
	}
}
