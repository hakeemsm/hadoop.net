using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Logaggregation
{
	public enum ContainerLogsRetentionPolicy
	{
		ApplicationMasterOnly,
		AmAndFailedContainersOnly,
		AllContainers
	}
}
