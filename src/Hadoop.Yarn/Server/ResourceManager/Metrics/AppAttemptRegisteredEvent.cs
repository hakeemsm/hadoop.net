using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	public class AppAttemptRegisteredEvent : SystemMetricsEvent
	{
		private ApplicationAttemptId appAttemptId;

		private string host;

		private int rpcPort;

		private string trackingUrl;

		private string originalTrackingUrl;

		private ContainerId masterContainerId;

		public AppAttemptRegisteredEvent(ApplicationAttemptId appAttemptId, string host, 
			int rpcPort, string trackingUrl, string originalTrackingUrl, ContainerId masterContainerId
			, long registeredTime)
			: base(SystemMetricsEventType.AppAttemptRegistered, registeredTime)
		{
			this.appAttemptId = appAttemptId;
			this.host = host;
			this.rpcPort = rpcPort;
			// This is the tracking URL after the application attempt is registered
			this.trackingUrl = trackingUrl;
			this.originalTrackingUrl = originalTrackingUrl;
			this.masterContainerId = masterContainerId;
		}

		public override int GetHashCode()
		{
			return appAttemptId.GetApplicationId().GetHashCode();
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return appAttemptId;
		}

		public virtual string GetHost()
		{
			return host;
		}

		public virtual int GetRpcPort()
		{
			return rpcPort;
		}

		public virtual string GetTrackingUrl()
		{
			return trackingUrl;
		}

		public virtual string GetOriginalTrackingURL()
		{
			return originalTrackingUrl;
		}

		public virtual ContainerId GetMasterContainerId()
		{
			return masterContainerId;
		}
	}
}
