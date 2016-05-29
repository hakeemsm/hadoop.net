using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class RMAppManagerEvent : AbstractEvent<RMAppManagerEventType>
	{
		private readonly ApplicationId appId;

		public RMAppManagerEvent(ApplicationId appId, RMAppManagerEventType type)
			: base(type)
		{
			this.appId = appId;
		}

		public virtual ApplicationId GetApplicationId()
		{
			return this.appId;
		}
	}
}
