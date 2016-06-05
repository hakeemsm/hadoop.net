using Org.Apache.Hadoop.Ipc;


namespace Org.Apache.Hadoop.Tracing
{
	public interface TraceAdminProtocolPB : TraceAdminPB.TraceAdminService.BlockingInterface
		, VersionedProtocol
	{
	}
}
