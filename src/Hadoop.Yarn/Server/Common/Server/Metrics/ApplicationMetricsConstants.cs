using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Metrics
{
	public class ApplicationMetricsConstants
	{
		public const string EntityType = "YARN_APPLICATION";

		public const string CreatedEventType = "YARN_APPLICATION_CREATED";

		public const string FinishedEventType = "YARN_APPLICATION_FINISHED";

		public const string AclsUpdatedEventType = "YARN_APPLICATION_ACLS_UPDATED";

		public const string NameEntityInfo = "YARN_APPLICATION_NAME";

		public const string TypeEntityInfo = "YARN_APPLICATION_TYPE";

		public const string UserEntityInfo = "YARN_APPLICATION_USER";

		public const string QueueEntityInfo = "YARN_APPLICATION_QUEUE";

		public const string SubmittedTimeEntityInfo = "YARN_APPLICATION_SUBMITTED_TIME";

		public const string AppViewAclsEntityInfo = "YARN_APPLICATION_VIEW_ACLS";

		public const string DiagnosticsInfoEventInfo = "YARN_APPLICATION_DIAGNOSTICS_INFO";

		public const string FinalStatusEventInfo = "YARN_APPLICATION_FINAL_STATUS";

		public const string StateEventInfo = "YARN_APPLICATION_STATE";

		public const string AppCpuMetrics = "YARN_APPLICATION_CPU_METRIC";

		public const string AppMemMetrics = "YARN_APPLICATION_MEM_METRIC";

		public const string LatestAppAttemptEventInfo = "YARN_APPLICATION_LATEST_APP_ATTEMPT";
	}
}
