using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	public enum SystemMetricsEventType
	{
		AppCreated,
		AppFinished,
		AppAclsUpdated,
		AppAttemptRegistered,
		AppAttemptFinished,
		ContainerCreated,
		ContainerFinished
	}
}
