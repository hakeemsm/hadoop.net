using Sharpen;

namespace org.apache.hadoop.tracing
{
	public interface TraceAdminProtocolPB : org.apache.hadoop.tracing.TraceAdminPB.TraceAdminService.BlockingInterface
		, org.apache.hadoop.ipc.VersionedProtocol
	{
	}
}
