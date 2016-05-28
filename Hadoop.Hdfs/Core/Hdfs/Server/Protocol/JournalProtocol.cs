using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Protocol used to journal edits to a remote node.</summary>
	/// <remarks>
	/// Protocol used to journal edits to a remote node. Currently,
	/// this is used to publish edits from the NameNode to a BackupNode.
	/// </remarks>
	public abstract class JournalProtocol
	{
		/// <summary>
		/// This class is used by both the Namenode (client) and BackupNode (server)
		/// to insulate from the protocol serialization.
		/// </summary>
		/// <remarks>
		/// This class is used by both the Namenode (client) and BackupNode (server)
		/// to insulate from the protocol serialization.
		/// If you are adding/changing DN's interface then you need to
		/// change both this class and ALSO related protocol buffer
		/// wire protocol definition in JournalProtocol.proto.
		/// For more details on protocol buffer wire protocol, please see
		/// .../org/apache/hadoop/hdfs/protocolPB/overview.html
		/// </remarks>
		public const long versionID = 1L;

		/// <summary>Journal edit records.</summary>
		/// <remarks>
		/// Journal edit records.
		/// This message is sent by the active name-node to the backup node
		/// via
		/// <c>EditLogBackupOutputStream</c>
		/// in order to synchronize meta-data
		/// changes with the backup namespace image.
		/// </remarks>
		/// <param name="journalInfo">journal information</param>
		/// <param name="epoch">marks beginning a new journal writer</param>
		/// <param name="firstTxnId">the first transaction of this batch</param>
		/// <param name="numTxns">number of transactions</param>
		/// <param name="records">byte array containing serialized journal records</param>
		/// <exception cref="FencedException">if the resource has been fenced</exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Journal(JournalInfo journalInfo, long epoch, long firstTxnId
			, int numTxns, byte[] records);

		/// <summary>
		/// Notify the BackupNode that the NameNode has rolled its edit logs
		/// and is now writing a new log segment.
		/// </summary>
		/// <param name="journalInfo">journal information</param>
		/// <param name="epoch">marks beginning a new journal writer</param>
		/// <param name="txid">the first txid in the new log</param>
		/// <exception cref="FencedException">if the resource has been fenced</exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StartLogSegment(JournalInfo journalInfo, long epoch, long txid
			);

		/// <summary>Request to fence any other journal writers.</summary>
		/// <remarks>
		/// Request to fence any other journal writers.
		/// Older writers with at previous epoch will be fenced and can no longer
		/// perform journal operations.
		/// </remarks>
		/// <param name="journalInfo">journal information</param>
		/// <param name="epoch">marks beginning a new journal writer</param>
		/// <param name="fencerInfo">info about fencer for debugging purposes</param>
		/// <exception cref="FencedException">if the resource has been fenced</exception>
		/// <exception cref="System.IO.IOException"/>
		public abstract FenceResponse Fence(JournalInfo journalInfo, long epoch, string fencerInfo
			);
	}

	public static class JournalProtocolConstants
	{
	}
}
