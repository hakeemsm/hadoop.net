using System;
using System.Net;
using System.Text;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>Interface for a remote log which is only communicated with asynchronously.
	/// 	</summary>
	/// <remarks>
	/// Interface for a remote log which is only communicated with asynchronously.
	/// This is essentially a wrapper around
	/// <see cref="Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.QJournalProtocol"/>
	/// with the key
	/// differences being:
	/// <ul>
	/// <li>All methods return
	/// <see cref="Com.Google.Common.Util.Concurrent.ListenableFuture{V}"/>
	/// s instead of synchronous
	/// objects.</li>
	/// <li>The
	/// <see cref="Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.RequestInfo"/>
	/// objects are created by the underlying
	/// implementation.</li>
	/// </ul>
	/// </remarks>
	internal abstract class AsyncLogger
	{
		public interface Factory
		{
			AsyncLogger CreateLogger(Configuration conf, NamespaceInfo nsInfo, string journalId
				, IPEndPoint addr);
		}

		/// <summary>Send a batch of edits to the logger.</summary>
		/// <param name="segmentTxId">the first txid in the current segment</param>
		/// <param name="firstTxnId">the first txid of the edits.</param>
		/// <param name="numTxns">the number of transactions in the batch</param>
		/// <param name="data">the actual data to be sent</param>
		public abstract ListenableFuture<Void> SendEdits(long segmentTxId, long firstTxnId
			, int numTxns, byte[] data);

		/// <summary>Begin writing a new log segment.</summary>
		/// <param name="txid">the first txid to be written to the new log</param>
		/// <param name="layoutVersion">the LayoutVersion of the log</param>
		public abstract ListenableFuture<Void> StartLogSegment(long txid, int layoutVersion
			);

		/// <summary>Finalize a log segment.</summary>
		/// <param name="startTxId">the first txid that was written to the segment</param>
		/// <param name="endTxId">the last txid that was written to the segment</param>
		public abstract ListenableFuture<Void> FinalizeLogSegment(long startTxId, long endTxId
			);

		/// <summary>Allow the remote node to purge edit logs earlier than this.</summary>
		/// <param name="minTxIdToKeep">the min txid which must be retained</param>
		public abstract ListenableFuture<Void> PurgeLogsOlderThan(long minTxIdToKeep);

		/// <summary>Format the log directory.</summary>
		/// <param name="nsInfo">the namespace info to format with</param>
		public abstract ListenableFuture<Void> Format(NamespaceInfo nsInfo);

		/// <returns>whether or not the remote node has any valid data.</returns>
		public abstract ListenableFuture<bool> IsFormatted();

		/// <returns>the state of the last epoch on the target node.</returns>
		public abstract ListenableFuture<QJournalProtocolProtos.GetJournalStateResponseProto
			> GetJournalState();

		/// <summary>Begin a new epoch on the target node.</summary>
		public abstract ListenableFuture<QJournalProtocolProtos.NewEpochResponseProto> NewEpoch
			(long epoch);

		/// <summary>Fetch the list of edit logs available on the remote node.</summary>
		public abstract ListenableFuture<RemoteEditLogManifest> GetEditLogManifest(long fromTxnId
			, bool inProgressOk);

		/// <summary>Prepare recovery.</summary>
		/// <remarks>Prepare recovery. See the HDFS-3077 design document for details.</remarks>
		public abstract ListenableFuture<QJournalProtocolProtos.PrepareRecoveryResponseProto
			> PrepareRecovery(long segmentTxId);

		/// <summary>Accept a recovery proposal.</summary>
		/// <remarks>Accept a recovery proposal. See the HDFS-3077 design document for details.
		/// 	</remarks>
		public abstract ListenableFuture<Void> AcceptRecovery(QJournalProtocolProtos.SegmentStateProto
			 log, Uri fromUrl);

		/// <summary>Set the epoch number used for all future calls.</summary>
		public abstract void SetEpoch(long e);

		/// <summary>
		/// Let the logger know the highest committed txid across all loggers in the
		/// set.
		/// </summary>
		/// <remarks>
		/// Let the logger know the highest committed txid across all loggers in the
		/// set. This txid may be higher than the last committed txid for <em>this</em>
		/// logger. See HDFS-3863 for details.
		/// </remarks>
		public abstract void SetCommittedTxId(long txid);

		/// <summary>Build an HTTP URL to fetch the log segment with the given startTxId.</summary>
		public abstract Uri BuildURLToFetchLogs(long segmentTxId);

		/// <summary>Tear down any resources, connections, etc.</summary>
		/// <remarks>
		/// Tear down any resources, connections, etc. The proxy may not be used
		/// after this point, and any in-flight RPCs may throw an exception.
		/// </remarks>
		public abstract void Close();

		/// <summary>
		/// Append an HTML-formatted report for this logger's status to the provided
		/// StringBuilder.
		/// </summary>
		/// <remarks>
		/// Append an HTML-formatted report for this logger's status to the provided
		/// StringBuilder. This is displayed on the NN web UI.
		/// </remarks>
		public abstract void AppendReport(StringBuilder sb);

		public abstract ListenableFuture<Void> DoPreUpgrade();

		public abstract ListenableFuture<Void> DoUpgrade(StorageInfo sInfo);

		public abstract ListenableFuture<Void> DoFinalize();

		public abstract ListenableFuture<bool> CanRollBack(StorageInfo storage, StorageInfo
			 prevStorage, int targetLayoutVersion);

		public abstract ListenableFuture<Void> DoRollback();

		public abstract ListenableFuture<long> GetJournalCTime();

		public abstract ListenableFuture<Void> DiscardSegments(long startTxId);
	}

	internal static class AsyncLoggerConstants
	{
	}
}
