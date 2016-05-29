using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	public class AppAttemptFinishedEvent : SystemMetricsEvent
	{
		private ApplicationAttemptId appAttemptId;

		private string trackingUrl;

		private string originalTrackingUrl;

		private string diagnosticsInfo;

		private FinalApplicationStatus appStatus;

		private YarnApplicationAttemptState state;

		public AppAttemptFinishedEvent(ApplicationAttemptId appAttemptId, string trackingUrl
			, string originalTrackingUrl, string diagnosticsInfo, FinalApplicationStatus appStatus
			, YarnApplicationAttemptState state, long finishedTime)
			: base(SystemMetricsEventType.AppAttemptFinished, finishedTime)
		{
			this.appAttemptId = appAttemptId;
			// This is the tracking URL after the application attempt is finished
			this.trackingUrl = trackingUrl;
			this.originalTrackingUrl = originalTrackingUrl;
			this.diagnosticsInfo = diagnosticsInfo;
			this.appStatus = appStatus;
			this.state = state;
		}

		public override int GetHashCode()
		{
			return appAttemptId.GetApplicationId().GetHashCode();
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return appAttemptId;
		}

		public virtual string GetTrackingUrl()
		{
			return trackingUrl;
		}

		public virtual string GetOriginalTrackingURL()
		{
			return originalTrackingUrl;
		}

		public virtual string GetDiagnosticsInfo()
		{
			return diagnosticsInfo;
		}

		public virtual FinalApplicationStatus GetFinalApplicationStatus()
		{
			return appStatus;
		}

		public virtual YarnApplicationAttemptState GetYarnApplicationAttemptState()
		{
			return state;
		}
	}
}
