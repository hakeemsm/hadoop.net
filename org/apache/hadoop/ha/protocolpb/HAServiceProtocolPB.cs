using Sharpen;

namespace org.apache.hadoop.ha.protocolPB
{
	public interface HAServiceProtocolPB : org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceProtocolService.BlockingInterface
		, org.apache.hadoop.ipc.VersionedProtocol
	{
	}
}
