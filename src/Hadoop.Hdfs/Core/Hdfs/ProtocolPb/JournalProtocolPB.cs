using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>Protocol used to journal edits to a remote node.</summary>
	/// <remarks>
	/// Protocol used to journal edits to a remote node. Currently,
	/// this is used to publish edits from the NameNode to a BackupNode.
	/// Note: This extends the protocolbuffer service based interface to
	/// add annotations required for security.
	/// </remarks>
	public interface JournalProtocolPB : JournalProtocolProtos.JournalProtocolService.BlockingInterface
	{
	}
}
