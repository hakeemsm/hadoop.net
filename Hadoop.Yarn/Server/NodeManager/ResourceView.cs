using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public interface ResourceView
	{
		long GetVmemAllocatedForContainers();

		bool IsVmemCheckEnabled();

		long GetPmemAllocatedForContainers();

		bool IsPmemCheckEnabled();

		long GetVCoresAllocatedForContainers();
	}
}
