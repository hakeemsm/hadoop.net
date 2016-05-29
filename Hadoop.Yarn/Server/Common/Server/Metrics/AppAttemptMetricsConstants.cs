using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Metrics
{
	public class AppAttemptMetricsConstants
	{
		public const string EntityType = "YARN_APPLICATION_ATTEMPT";

		public const string RegisteredEventType = "YARN_APPLICATION_ATTEMPT_REGISTERED";

		public const string FinishedEventType = "YARN_APPLICATION_ATTEMPT_FINISHED";

		public const string ParentPrimaryFilter = "YARN_APPLICATION_ATTEMPT_PARENT";

		public const string TrackingUrlEventInfo = "YARN_APPLICATION_ATTEMPT_TRACKING_URL";

		public const string OriginalTrackingUrlEventInfo = "YARN_APPLICATION_ATTEMPT_ORIGINAL_TRACKING_URL";

		public const string HostEventInfo = "YARN_APPLICATION_ATTEMPT_HOST";

		public const string RpcPortEventInfo = "YARN_APPLICATION_ATTEMPT_RPC_PORT";

		public const string MasterContainerEventInfo = "YARN_APPLICATION_ATTEMPT_MASTER_CONTAINER";

		public const string DiagnosticsInfoEventInfo = "YARN_APPLICATION_ATTEMPT_DIAGNOSTICS_INFO";

		public const string FinalStatusEventInfo = "YARN_APPLICATION_ATTEMPT_FINAL_STATUS";

		public const string StateEventInfo = "YARN_APPLICATION_ATTEMPT_STATE";
	}
}
