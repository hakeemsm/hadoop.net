using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Tracing
{
	public interface TraceAdminProtocolPB : TraceAdminPB.TraceAdminService.BlockingInterface
		, VersionedProtocol
	{
	}
}
