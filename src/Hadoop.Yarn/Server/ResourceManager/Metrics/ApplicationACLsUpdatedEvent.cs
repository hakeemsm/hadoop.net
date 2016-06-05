using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	public class ApplicationACLsUpdatedEvent : SystemMetricsEvent
	{
		private ApplicationId appId;

		private string viewAppACLs;

		public ApplicationACLsUpdatedEvent(ApplicationId appId, string viewAppACLs, long 
			updatedTime)
			: base(SystemMetricsEventType.AppAclsUpdated, updatedTime)
		{
			this.appId = appId;
			this.viewAppACLs = viewAppACLs;
		}

		public virtual ApplicationId GetApplicationId()
		{
			return appId;
		}

		public virtual string GetViewAppACLs()
		{
			return viewAppACLs;
		}
	}
}
