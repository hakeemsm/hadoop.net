using Org.Apache.Hadoop.HA.Proto;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.HA.ProtocolPB
{
	public interface ZKFCProtocolPB : ZKFCProtocolProtos.ZKFCProtocolService.BlockingInterface
		, VersionedProtocol
	{
	}
}
