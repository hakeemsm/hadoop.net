using System;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO.Retry;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Protocol
{
	/// <summary>
	/// Protocol used to communicate between
	/// <see cref="Org.Apache.Hadoop.Hdfs.Qjournal.Client.QuorumJournalManager"/>
	/// and each
	/// <see cref="Org.Apache.Hadoop.Hdfs.Qjournal.Server.JournalNode"/>
	/// .
	/// This is responsible for sending edits as well as coordinating
	/// recovery of the nodes.
	/// </summary>
	public abstract class QJournalProtocol
	{
		public const long versionID = 1L;

		/// <returns>
		/// true if the given journal has been formatted and
		/// contains valid data.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool IsFormatted(string journalId);

		/// <summary>
		/// Get the current state of the journal, including the most recent
		/// epoch number and the HTTP port.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract QJournalProtocolProtos.GetJournalStateResponseProto GetJournalState
			(string journalId);

		/// <summary>Format the underlying storage for the given namespace.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Format(string journalId, NamespaceInfo nsInfo);

		/// <summary>Begin a new epoch.</summary>
		/// <remarks>Begin a new epoch. See the HDFS-3077 design doc for details.</remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract QJournalProtocolProtos.NewEpochResponseProto NewEpoch(string journalId
			, NamespaceInfo nsInfo, long epoch);

		/// <summary>Journal edit records.</summary>
		/// <remarks>
		/// Journal edit records.
		/// This message is sent by the active name-node to the JournalNodes
		/// to write edits to their local logs.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Journal(RequestInfo reqInfo, long segmentTxId, long firstTxnId
			, int numTxns, byte[] records);

		/// <summary>Heartbeat.</summary>
		/// <remarks>
		/// Heartbeat.
		/// This is a no-op on the server, except that it verifies that the
		/// caller is in fact still the active writer, and provides up-to-date
		/// information on the most recently committed txid.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Heartbeat(RequestInfo reqInfo);

		/// <summary>Start writing to a new log segment on the JournalNode.</summary>
		/// <remarks>
		/// Start writing to a new log segment on the JournalNode.
		/// Before calling this, one should finalize the previous segment
		/// using
		/// <see cref="FinalizeLogSegment(RequestInfo, long, long)"/>
		/// .
		/// </remarks>
		/// <param name="txid">the first txid in the new log</param>
		/// <param name="layoutVersion">the LayoutVersion of the new log</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StartLogSegment(RequestInfo reqInfo, long txid, int layoutVersion
			);

		/// <summary>Finalize the given log segment on the JournalNode.</summary>
		/// <remarks>
		/// Finalize the given log segment on the JournalNode. The segment
		/// is expected to be in-progress and starting at the given startTxId.
		/// </remarks>
		/// <param name="startTxId">the starting transaction ID of the log</param>
		/// <param name="endTxId">the expected last transaction in the given log</param>
		/// <exception cref="System.IO.IOException">if no such segment exists</exception>
		public abstract void FinalizeLogSegment(RequestInfo reqInfo, long startTxId, long
			 endTxId);

		/// <exception cref="System.IO.IOException"></exception>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.LogsPurgeable.PurgeLogsOlderThan(long)
		/// 	"/>
		public abstract void PurgeLogsOlderThan(RequestInfo requestInfo, long minTxIdToKeep
			);

		/// <param name="jid">the journal from which to enumerate edits</param>
		/// <param name="sinceTxId">the first transaction which the client cares about</param>
		/// <param name="inProgressOk">
		/// whether or not to check the in-progress edit log
		/// segment
		/// </param>
		/// <returns>a list of edit log segments since the given transaction ID.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract QJournalProtocolProtos.GetEditLogManifestResponseProto GetEditLogManifest
			(string jid, long sinceTxId, bool inProgressOk);

		/// <summary>Begin the recovery process for a given segment.</summary>
		/// <remarks>
		/// Begin the recovery process for a given segment. See the HDFS-3077
		/// design document for details.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract QJournalProtocolProtos.PrepareRecoveryResponseProto PrepareRecovery
			(RequestInfo reqInfo, long segmentTxId);

		/// <summary>Accept a proposed recovery for the given transaction ID.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void AcceptRecovery(RequestInfo reqInfo, QJournalProtocolProtos.SegmentStateProto
			 stateToAccept, Uri fromUrl);

		/// <exception cref="System.IO.IOException"/>
		public abstract void DoPreUpgrade(string journalId);

		/// <exception cref="System.IO.IOException"/>
		public abstract void DoUpgrade(string journalId, StorageInfo sInfo);

		/// <exception cref="System.IO.IOException"/>
		public abstract void DoFinalize(string journalId);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool CanRollBack(string journalId, StorageInfo storage, StorageInfo
			 prevStorage, int targetLayoutVersion);

		/// <exception cref="System.IO.IOException"/>
		public abstract void DoRollback(string journalId);

		/// <exception cref="System.IO.IOException"/>
		public abstract long GetJournalCTime(string journalId);

		/// <summary>
		/// Discard journal segments whose first TxId is greater than or equal to the
		/// given txid.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Idempotent]
		public abstract void DiscardSegments(string journalId, long startTxId);
	}

	public static class QJournalProtocolConstants
	{
	}
}
