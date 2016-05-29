using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	public class ApplicationFinishedEvent : SystemMetricsEvent
	{
		private ApplicationId appId;

		private string diagnosticsInfo;

		private FinalApplicationStatus appStatus;

		private YarnApplicationState state;

		private ApplicationAttemptId latestAppAttemptId;

		private RMAppMetrics appMetrics;

		public ApplicationFinishedEvent(ApplicationId appId, string diagnosticsInfo, FinalApplicationStatus
			 appStatus, YarnApplicationState state, ApplicationAttemptId latestAppAttemptId, 
			long finishedTime, RMAppMetrics appMetrics)
			: base(SystemMetricsEventType.AppFinished, finishedTime)
		{
			this.appId = appId;
			this.diagnosticsInfo = diagnosticsInfo;
			this.appStatus = appStatus;
			this.latestAppAttemptId = latestAppAttemptId;
			this.state = state;
			this.appMetrics = appMetrics;
		}

		public override int GetHashCode()
		{
			return appId.GetHashCode();
		}

		public virtual ApplicationId GetApplicationId()
		{
			return appId;
		}

		public virtual string GetDiagnosticsInfo()
		{
			return diagnosticsInfo;
		}

		public virtual FinalApplicationStatus GetFinalApplicationStatus()
		{
			return appStatus;
		}

		public virtual YarnApplicationState GetYarnApplicationState()
		{
			return state;
		}

		public virtual ApplicationAttemptId GetLatestApplicationAttemptId()
		{
			return latestAppAttemptId;
		}

		public virtual RMAppMetrics GetAppMetrics()
		{
			return appMetrics;
		}
	}
}
