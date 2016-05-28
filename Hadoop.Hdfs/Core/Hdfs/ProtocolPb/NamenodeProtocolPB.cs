using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>Protocol that a secondary NameNode uses to communicate with the NameNode.
	/// 	</summary>
	/// <remarks>
	/// Protocol that a secondary NameNode uses to communicate with the NameNode.
	/// It's used to get part of the name node state
	/// Note: This extends the protocolbuffer service based interface to
	/// add annotations required for security.
	/// </remarks>
	public interface NamenodeProtocolPB : NamenodeProtocolProtos.NamenodeProtocolService.BlockingInterface
	{
	}
}
