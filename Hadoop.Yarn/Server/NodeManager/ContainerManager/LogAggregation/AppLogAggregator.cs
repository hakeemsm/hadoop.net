using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Logaggregation
{
	public interface AppLogAggregator : Runnable
	{
		void StartContainerLogAggregation(ContainerId containerId, bool wasContainerSuccessful
			);

		void AbortLogAggregation();

		void FinishLogAggregation();

		void DisableLogAggregation();
	}
}
