using Sharpen;

namespace org.apache.hadoop.ipc
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
	public interface ProtocolMetaInfoPB : org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolInfoService.BlockingInterface
	{
	}
}
