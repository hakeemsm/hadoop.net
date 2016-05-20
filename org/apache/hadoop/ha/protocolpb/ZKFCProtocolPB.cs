using Sharpen;

namespace org.apache.hadoop.ha.protocolPB
{
	public interface ZKFCProtocolPB : org.apache.hadoop.ha.proto.ZKFCProtocolProtos.ZKFCProtocolService.BlockingInterface
		, org.apache.hadoop.ipc.VersionedProtocol
	{
	}
}
