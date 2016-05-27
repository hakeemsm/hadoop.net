using Org.Apache.Hadoop.Ipc.Protobuf;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// Protocol to get versions and signatures for supported protocols from the
	/// server.
	/// </summary>
	/// <remarks>
	/// Protocol to get versions and signatures for supported protocols from the
	/// server.
	/// Note: This extends the protocolbuffer service based interface to
	/// add annotations.
	/// </remarks>
	public interface ProtocolMetaInfoPB : ProtocolInfoProtos.ProtocolInfoService.BlockingInterface
	{
	}
}
