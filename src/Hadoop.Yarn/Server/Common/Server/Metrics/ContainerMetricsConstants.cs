using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Metrics
{
	public class ContainerMetricsConstants
	{
		public const string EntityType = "YARN_CONTAINER";

		public const string CreatedEventType = "YARN_CONTAINER_CREATED";

		public const string FinishedEventType = "YARN_CONTAINER_FINISHED";

		public const string ParentPrimariyFilter = "YARN_CONTAINER_PARENT";

		public const string AllocatedMemoryEntityInfo = "YARN_CONTAINER_ALLOCATED_MEMORY";

		public const string AllocatedVcoreEntityInfo = "YARN_CONTAINER_ALLOCATED_VCORE";

		public const string AllocatedHostEntityInfo = "YARN_CONTAINER_ALLOCATED_HOST";

		public const string AllocatedPortEntityInfo = "YARN_CONTAINER_ALLOCATED_PORT";

		public const string AllocatedPriorityEntityInfo = "YARN_CONTAINER_ALLOCATED_PRIORITY";

		public const string DiagnosticsInfoEventInfo = "YARN_CONTAINER_DIAGNOSTICS_INFO";

		public const string ExitStatusEventInfo = "YARN_CONTAINER_EXIT_STATUS";

		public const string StateEventInfo = "YARN_CONTAINER_STATE";

		public const string AllocatedHostHttpAddressEntityInfo = "YARN_CONTAINER_ALLOCATED_HOST_HTTP_ADDRESS";
	}
}
