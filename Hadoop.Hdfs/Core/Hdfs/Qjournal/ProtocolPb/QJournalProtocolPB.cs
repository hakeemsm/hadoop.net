using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.ProtocolPB
{
	/// <summary>
	/// Protocol used to journal edits to a JournalNode participating
	/// in the quorum journal.
	/// </summary>
	/// <remarks>
	/// Protocol used to journal edits to a JournalNode participating
	/// in the quorum journal.
	/// Note: This extends the protocolbuffer service based interface to
	/// add annotations required for security.
	/// </remarks>
	public interface QJournalProtocolPB : QJournalProtocolProtos.QJournalProtocolService.BlockingInterface
	{
	}
}
