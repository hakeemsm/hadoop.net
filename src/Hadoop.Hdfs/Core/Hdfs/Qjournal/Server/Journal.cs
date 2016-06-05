using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Commons.Lang.Math;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	/// <summary>A JournalNode can manage journals for several clusters at once.</summary>
	/// <remarks>
	/// A JournalNode can manage journals for several clusters at once.
	/// Each such journal is entirely independent despite being hosted by
	/// the same JVM.
	/// </remarks>
	public class Journal : IDisposable
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Qjournal.Server.Journal
			));

		private EditLogOutputStream curSegment;

		private long curSegmentTxId = HdfsConstants.InvalidTxid;

		private long nextTxId = HdfsConstants.InvalidTxid;

		private long highestWrittenTxId = 0;

		private readonly string journalId;

		private readonly JNStorage storage;

		/// <summary>
		/// When a new writer comes along, it asks each node to promise
		/// to ignore requests from any previous writer, as identified
		/// by epoch number.
		/// </summary>
		/// <remarks>
		/// When a new writer comes along, it asks each node to promise
		/// to ignore requests from any previous writer, as identified
		/// by epoch number. In order to make such a promise, the epoch
		/// number of that writer is stored persistently on disk.
		/// </remarks>
		private PersistentLongFile lastPromisedEpoch;

		/// <summary>
		/// Each IPC that comes from a given client contains a serial number
		/// which only increases from the client's perspective.
		/// </summary>
		/// <remarks>
		/// Each IPC that comes from a given client contains a serial number
		/// which only increases from the client's perspective. Whenever
		/// we switch epochs, we reset this back to -1. Whenever an IPC
		/// comes from a client, we ensure that it is strictly higher
		/// than any previous IPC. This guards against any bugs in the IPC
		/// layer that would re-order IPCs or cause a stale retry from an old
		/// request to resurface and confuse things.
		/// </remarks>
		private long currentEpochIpcSerial = -1;

		/// <summary>The epoch number of the last writer to actually write a transaction.</summary>
		/// <remarks>
		/// The epoch number of the last writer to actually write a transaction.
		/// This is used to differentiate log segments after a crash at the very
		/// beginning of a segment. See the the 'testNewerVersionOfSegmentWins'
		/// test case.
		/// </remarks>
		private PersistentLongFile lastWriterEpoch;

		/// <summary>Lower-bound on the last committed transaction ID.</summary>
		/// <remarks>
		/// Lower-bound on the last committed transaction ID. This is not
		/// depended upon for correctness, but acts as a sanity check
		/// during the recovery procedures, and as a visibility mark
		/// for clients reading in-progress logs.
		/// </remarks>
		private BestEffortLongFile committedTxnId;

		public const string LastPromisedFilename = "last-promised-epoch";

		public const string LastWriterEpoch = "last-writer-epoch";

		private const string CommittedTxidFilename = "committed-txid";

		private readonly FileJournalManager fjm;

		private readonly JournalMetrics metrics;

		/// <summary>Time threshold for sync calls, beyond which a warning should be logged to the console.
		/// 	</summary>
		private const int WarnSyncMillisThreshold = 1000;

		/// <exception cref="System.IO.IOException"/>
		internal Journal(Configuration conf, FilePath logDir, string journalId, HdfsServerConstants.StartupOption
			 startOpt, StorageErrorReporter errorReporter)
		{
			// Current writing state
			storage = new JNStorage(conf, logDir, startOpt, errorReporter);
			this.journalId = journalId;
			RefreshCachedData();
			this.fjm = storage.GetJournalManager();
			this.metrics = JournalMetrics.Create(this);
			FileJournalManager.EditLogFile latest = ScanStorageForLatestEdits();
			if (latest != null)
			{
				highestWrittenTxId = latest.GetLastTxId();
			}
		}

		/// <summary>Reload any data that may have been cached.</summary>
		/// <remarks>
		/// Reload any data that may have been cached. This is necessary
		/// when we first load the Journal, but also after any formatting
		/// operation, since the cached data is no longer relevant.
		/// </remarks>
		private void RefreshCachedData()
		{
			lock (this)
			{
				IOUtils.CloseStream(committedTxnId);
				FilePath currentDir = storage.GetSingularStorageDir().GetCurrentDir();
				this.lastPromisedEpoch = new PersistentLongFile(new FilePath(currentDir, LastPromisedFilename
					), 0);
				this.lastWriterEpoch = new PersistentLongFile(new FilePath(currentDir, LastWriterEpoch
					), 0);
				this.committedTxnId = new BestEffortLongFile(new FilePath(currentDir, CommittedTxidFilename
					), HdfsConstants.InvalidTxid);
			}
		}

		/// <summary>
		/// Scan the local storage directory, and return the segment containing
		/// the highest transaction.
		/// </summary>
		/// <returns>
		/// the EditLogFile with the highest transactions, or null
		/// if no files exist.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private FileJournalManager.EditLogFile ScanStorageForLatestEdits()
		{
			lock (this)
			{
				if (!fjm.GetStorageDirectory().GetCurrentDir().Exists())
				{
					return null;
				}
				Log.Info("Scanning storage " + fjm);
				IList<FileJournalManager.EditLogFile> files = fjm.GetLogFiles(0);
				while (!files.IsEmpty())
				{
					FileJournalManager.EditLogFile latestLog = files.Remove(files.Count - 1);
					latestLog.ScanLog();
					Log.Info("Latest log is " + latestLog);
					if (latestLog.GetLastTxId() == HdfsConstants.InvalidTxid)
					{
						// the log contains no transactions
						Log.Warn("Latest log " + latestLog + " has no transactions. " + "moving it aside and looking for previous log"
							);
						latestLog.MoveAsideEmptyFile();
					}
					else
					{
						return latestLog;
					}
				}
				Log.Info("No files in " + fjm);
				return null;
			}
		}

		/// <summary>Format the local storage with the given namespace.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Format(NamespaceInfo nsInfo)
		{
			Preconditions.CheckState(nsInfo.GetNamespaceID() != 0, "can't format with uninitialized namespace info: %s"
				, nsInfo);
			Log.Info("Formatting " + this + " with namespace info: " + nsInfo);
			storage.Format(nsInfo);
			RefreshCachedData();
		}

		/// <summary>Unlock and release resources.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			// Closeable
			storage.Close();
			IOUtils.CloseStream(committedTxnId);
			IOUtils.CloseStream(curSegment);
		}

		internal virtual JNStorage GetStorage()
		{
			return storage;
		}

		internal virtual string GetJournalId()
		{
			return journalId;
		}

		/// <returns>
		/// the last epoch which this node has promised not to accept
		/// any lower epoch, or 0 if no promises have been made.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual long GetLastPromisedEpoch()
		{
			lock (this)
			{
				CheckFormatted();
				return lastPromisedEpoch.Get();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetLastWriterEpoch()
		{
			lock (this)
			{
				CheckFormatted();
				return lastWriterEpoch.Get();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long GetCommittedTxnIdForTests()
		{
			lock (this)
			{
				return committedTxnId.Get();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long GetCurrentLagTxns()
		{
			lock (this)
			{
				long committed = committedTxnId.Get();
				if (committed == 0)
				{
					return 0;
				}
				return Math.Max(committed - highestWrittenTxId, 0L);
			}
		}

		internal virtual long GetHighestWrittenTxId()
		{
			lock (this)
			{
				return highestWrittenTxId;
			}
		}

		[VisibleForTesting]
		internal virtual JournalMetrics GetMetricsForTests()
		{
			return metrics;
		}

		/// <summary>Try to create a new epoch for this journal.</summary>
		/// <param name="nsInfo">
		/// the namespace, which is verified for consistency or used to
		/// format, if the Journal has not yet been written to.
		/// </param>
		/// <param name="epoch">the epoch to start</param>
		/// <returns>the status information necessary to begin recovery</returns>
		/// <exception cref="System.IO.IOException">
		/// if the node has already made a promise to another
		/// writer with a higher epoch number, if the namespace is inconsistent,
		/// or if a disk error occurs.
		/// </exception>
		internal virtual QJournalProtocolProtos.NewEpochResponseProto NewEpoch(NamespaceInfo
			 nsInfo, long epoch)
		{
			lock (this)
			{
				CheckFormatted();
				storage.CheckConsistentNamespace(nsInfo);
				// Check that the new epoch being proposed is in fact newer than
				// any other that we've promised. 
				if (epoch <= GetLastPromisedEpoch())
				{
					throw new IOException("Proposed epoch " + epoch + " <= last promise " + GetLastPromisedEpoch
						());
				}
				UpdateLastPromisedEpoch(epoch);
				AbortCurSegment();
				QJournalProtocolProtos.NewEpochResponseProto.Builder builder = QJournalProtocolProtos.NewEpochResponseProto
					.NewBuilder();
				FileJournalManager.EditLogFile latestFile = ScanStorageForLatestEdits();
				if (latestFile != null)
				{
					builder.SetLastSegmentTxId(latestFile.GetFirstTxId());
				}
				return ((QJournalProtocolProtos.NewEpochResponseProto)builder.Build());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void UpdateLastPromisedEpoch(long newEpoch)
		{
			Log.Info("Updating lastPromisedEpoch from " + lastPromisedEpoch.Get() + " to " + 
				newEpoch + " for client " + Org.Apache.Hadoop.Ipc.Server.GetRemoteIp());
			lastPromisedEpoch.Set(newEpoch);
			// Since we have a new writer, reset the IPC serial - it will start
			// counting again from 0 for this writer.
			currentEpochIpcSerial = -1;
		}

		/// <exception cref="System.IO.IOException"/>
		private void AbortCurSegment()
		{
			if (curSegment == null)
			{
				return;
			}
			curSegment.Abort();
			curSegment = null;
			curSegmentTxId = HdfsConstants.InvalidTxid;
		}

		/// <summary>Write a batch of edits to the journal.</summary>
		/// <remarks>
		/// Write a batch of edits to the journal.
		/// <seealso>QJournalProtocol#journal(RequestInfo, long, long, int, byte[])</seealso>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Journal(RequestInfo reqInfo, long segmentTxId, long firstTxnId
			, int numTxns, byte[] records)
		{
			lock (this)
			{
				CheckFormatted();
				CheckWriteRequest(reqInfo);
				CheckSync(curSegment != null, "Can't write, no segment open");
				if (curSegmentTxId != segmentTxId)
				{
					// Sanity check: it is possible that the writer will fail IPCs
					// on both the finalize() and then the start() of the next segment.
					// This could cause us to continue writing to an old segment
					// instead of rolling to a new one, which breaks one of the
					// invariants in the design. If it happens, abort the segment
					// and throw an exception.
					JournalOutOfSyncException e = new JournalOutOfSyncException("Writer out of sync: it thinks it is writing segment "
						 + segmentTxId + " but current segment is " + curSegmentTxId);
					AbortCurSegment();
					throw e;
				}
				CheckSync(nextTxId == firstTxnId, "Can't write txid " + firstTxnId + " expecting nextTxId="
					 + nextTxId);
				long lastTxnId = firstTxnId + numTxns - 1;
				if (Log.IsTraceEnabled())
				{
					Log.Trace("Writing txid " + firstTxnId + "-" + lastTxnId);
				}
				// If the edit has already been marked as committed, we know
				// it has been fsynced on a quorum of other nodes, and we are
				// "catching up" with the rest. Hence we do not need to fsync.
				bool isLagging = lastTxnId <= committedTxnId.Get();
				bool shouldFsync = !isLagging;
				curSegment.WriteRaw(records, 0, records.Length);
				curSegment.SetReadyToFlush();
				StopWatch sw = new StopWatch();
				sw.Start();
				curSegment.Flush(shouldFsync);
				sw.Stop();
				long nanoSeconds = sw.Now();
				metrics.AddSync(TimeUnit.Microseconds.Convert(nanoSeconds, TimeUnit.Nanoseconds));
				long milliSeconds = TimeUnit.Milliseconds.Convert(nanoSeconds, TimeUnit.Nanoseconds
					);
				if (milliSeconds > WarnSyncMillisThreshold)
				{
					Log.Warn("Sync of transaction range " + firstTxnId + "-" + lastTxnId + " took " +
						 milliSeconds + "ms");
				}
				if (isLagging)
				{
					// This batch of edits has already been committed on a quorum of other
					// nodes. So, we are in "catch up" mode. This gets its own metric.
					metrics.batchesWrittenWhileLagging.Incr(1);
				}
				metrics.batchesWritten.Incr(1);
				metrics.bytesWritten.Incr(records.Length);
				metrics.txnsWritten.Incr(numTxns);
				highestWrittenTxId = lastTxnId;
				nextTxId = lastTxnId + 1;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Heartbeat(RequestInfo reqInfo)
		{
			CheckRequest(reqInfo);
		}

		/// <summary>Ensure that the given request is coming from the correct writer and in-order.
		/// 	</summary>
		/// <param name="reqInfo">the request info</param>
		/// <exception cref="System.IO.IOException">if the request is invalid.</exception>
		private void CheckRequest(RequestInfo reqInfo)
		{
			lock (this)
			{
				// Invariant 25 from ZAB paper
				if (reqInfo.GetEpoch() < lastPromisedEpoch.Get())
				{
					throw new IOException("IPC's epoch " + reqInfo.GetEpoch() + " is less than the last promised epoch "
						 + lastPromisedEpoch.Get());
				}
				else
				{
					if (reqInfo.GetEpoch() > lastPromisedEpoch.Get())
					{
						// A newer client has arrived. Fence any previous writers by updating
						// the promise.
						UpdateLastPromisedEpoch(reqInfo.GetEpoch());
					}
				}
				// Ensure that the IPCs are arriving in-order as expected.
				CheckSync(reqInfo.GetIpcSerialNumber() > currentEpochIpcSerial, "IPC serial %s from client %s was not higher than prior highest "
					 + "IPC serial %s", reqInfo.GetIpcSerialNumber(), Org.Apache.Hadoop.Ipc.Server.GetRemoteIp
					(), currentEpochIpcSerial);
				currentEpochIpcSerial = reqInfo.GetIpcSerialNumber();
				if (reqInfo.HasCommittedTxId())
				{
					Preconditions.CheckArgument(reqInfo.GetCommittedTxId() >= committedTxnId.Get(), "Client trying to move committed txid backward from "
						 + committedTxnId.Get() + " to " + reqInfo.GetCommittedTxId());
					committedTxnId.Set(reqInfo.GetCommittedTxId());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckWriteRequest(RequestInfo reqInfo)
		{
			lock (this)
			{
				CheckRequest(reqInfo);
				if (reqInfo.GetEpoch() != lastWriterEpoch.Get())
				{
					throw new IOException("IPC's epoch " + reqInfo.GetEpoch() + " is not the current writer epoch  "
						 + lastWriterEpoch.Get());
				}
			}
		}

		public virtual bool IsFormatted()
		{
			lock (this)
			{
				return storage.IsFormatted();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.JournalNotFormattedException
		/// 	"/>
		private void CheckFormatted()
		{
			if (!IsFormatted())
			{
				throw new JournalNotFormattedException("Journal " + storage.GetSingularStorageDir
					() + " not formatted");
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.JournalOutOfSyncException
		/// 	">
		/// if the given expression is not true.
		/// The message of the exception is formatted using the 'msg' and
		/// 'formatArgs' parameters.
		/// </exception>
		private void CheckSync(bool expression, string msg, params object[] formatArgs)
		{
			if (!expression)
			{
				throw new JournalOutOfSyncException(string.Format(msg, formatArgs));
			}
		}

		/// <exception cref="System.Exception">
		/// if the given expression is not true.
		/// The message of the exception is formatted using the 'msg' and
		/// 'formatArgs' parameters.
		/// This should be used in preference to Java's built-in assert in
		/// non-performance-critical paths, where a failure of this invariant
		/// might cause the protocol to lose data.
		/// </exception>
		private void AlwaysAssert(bool expression, string msg, params object[] formatArgs
			)
		{
			if (!expression)
			{
				throw new Exception(string.Format(msg, formatArgs));
			}
		}

		/// <summary>Start a new segment at the given txid.</summary>
		/// <remarks>
		/// Start a new segment at the given txid. The previous segment
		/// must have already been finalized.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartLogSegment(RequestInfo reqInfo, long txid, int layoutVersion
			)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(fjm != null);
				CheckFormatted();
				CheckRequest(reqInfo);
				if (curSegment != null)
				{
					Log.Warn("Client is requesting a new log segment " + txid + " though we are already writing "
						 + curSegment + ". " + "Aborting the current segment in order to begin the new one."
						);
					// The writer may have lost a connection to us and is now
					// re-connecting after the connection came back.
					// We should abort our own old segment.
					AbortCurSegment();
				}
				// Paranoid sanity check: we should never overwrite a finalized log file.
				// Additionally, if it's in-progress, it should have at most 1 transaction.
				// This can happen if the writer crashes exactly at the start of a segment.
				FileJournalManager.EditLogFile existing = fjm.GetLogFile(txid);
				if (existing != null)
				{
					if (!existing.IsInProgress())
					{
						throw new InvalidOperationException("Already have a finalized segment " + existing
							 + " beginning at " + txid);
					}
					// If it's in-progress, it should only contain one transaction,
					// because the "startLogSegment" transaction is written alone at the
					// start of each segment. 
					existing.ScanLog();
					if (existing.GetLastTxId() != existing.GetFirstTxId())
					{
						throw new InvalidOperationException("The log file " + existing + " seems to contain valid transactions"
							);
					}
				}
				long curLastWriterEpoch = lastWriterEpoch.Get();
				if (curLastWriterEpoch != reqInfo.GetEpoch())
				{
					Log.Info("Updating lastWriterEpoch from " + curLastWriterEpoch + " to " + reqInfo
						.GetEpoch() + " for client " + Org.Apache.Hadoop.Ipc.Server.GetRemoteIp());
					lastWriterEpoch.Set(reqInfo.GetEpoch());
				}
				// The fact that we are starting a segment at this txid indicates
				// that any previous recovery for this same segment was aborted.
				// Otherwise, no writer would have started writing. So, we can
				// remove the record of the older segment here.
				PurgePaxosDecision(txid);
				curSegment = fjm.StartLogSegment(txid, layoutVersion);
				curSegmentTxId = txid;
				nextTxId = txid;
			}
		}

		/// <summary>Finalize the log segment at the given transaction ID.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void FinalizeLogSegment(RequestInfo reqInfo, long startTxId, long 
			endTxId)
		{
			lock (this)
			{
				CheckFormatted();
				CheckRequest(reqInfo);
				bool needsValidation = true;
				// Finalizing the log that the writer was just writing.
				if (startTxId == curSegmentTxId)
				{
					if (curSegment != null)
					{
						curSegment.Close();
						curSegment = null;
						curSegmentTxId = HdfsConstants.InvalidTxid;
					}
					CheckSync(nextTxId == endTxId + 1, "Trying to finalize in-progress log segment %s to end at "
						 + "txid %s but only written up to txid %s", startTxId, endTxId, nextTxId - 1);
					// No need to validate the edit log if the client is finalizing
					// the log segment that it was just writing to.
					needsValidation = false;
				}
				FileJournalManager.EditLogFile elf = fjm.GetLogFile(startTxId);
				if (elf == null)
				{
					throw new JournalOutOfSyncException("No log file to finalize at " + "transaction ID "
						 + startTxId);
				}
				if (elf.IsInProgress())
				{
					if (needsValidation)
					{
						Log.Info("Validating log segment " + elf.GetFile() + " about to be " + "finalized"
							);
						elf.ScanLog();
						CheckSync(elf.GetLastTxId() == endTxId, "Trying to finalize in-progress log segment %s to end at "
							 + "txid %s but log %s on disk only contains up to txid %s", startTxId, endTxId, 
							elf.GetFile(), elf.GetLastTxId());
					}
					fjm.FinalizeLogSegment(startTxId, endTxId);
				}
				else
				{
					Preconditions.CheckArgument(endTxId == elf.GetLastTxId(), "Trying to re-finalize already finalized log "
						 + elf + " with different endTxId " + endTxId);
				}
				// Once logs are finalized, a different length will never be decided.
				// During recovery, we treat a finalized segment the same as an accepted
				// recovery. Thus, we no longer need to keep track of the previously-
				// accepted decision. The existence of the finalized log segment is enough.
				PurgePaxosDecision(elf.GetFirstTxId());
			}
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.LogsPurgeable.PurgeLogsOlderThan(long)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void PurgeLogsOlderThan(RequestInfo reqInfo, long minTxIdToKeep)
		{
			lock (this)
			{
				CheckFormatted();
				CheckRequest(reqInfo);
				storage.PurgeDataOlderThan(minTxIdToKeep);
			}
		}

		/// <summary>
		/// Remove the previously-recorded 'accepted recovery' information
		/// for a given log segment, once it is no longer necessary.
		/// </summary>
		/// <param name="segmentTxId">the transaction ID to purge</param>
		/// <exception cref="System.IO.IOException">if the file could not be deleted</exception>
		private void PurgePaxosDecision(long segmentTxId)
		{
			FilePath paxosFile = storage.GetPaxosFile(segmentTxId);
			if (paxosFile.Exists())
			{
				if (!paxosFile.Delete())
				{
					throw new IOException("Unable to delete paxos file " + paxosFile);
				}
			}
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.QJournalProtocol.GetEditLogManifest(string, long, bool)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteEditLogManifest GetEditLogManifest(long sinceTxId, bool inProgressOk
			)
		{
			// No need to checkRequest() here - anyone may ask for the list
			// of segments.
			CheckFormatted();
			IList<RemoteEditLog> logs = fjm.GetRemoteEditLogs(sinceTxId, inProgressOk);
			if (inProgressOk)
			{
				RemoteEditLog log = null;
				for (IEnumerator<RemoteEditLog> iter = logs.GetEnumerator(); iter.HasNext(); )
				{
					log = iter.Next();
					if (log.IsInProgress())
					{
						iter.Remove();
						break;
					}
				}
				if (log != null && log.IsInProgress())
				{
					logs.AddItem(new RemoteEditLog(log.GetStartTxId(), GetHighestWrittenTxId(), true)
						);
				}
			}
			return new RemoteEditLogManifest(logs);
		}

		/// <returns>
		/// the current state of the given segment, or null if the
		/// segment does not exist.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual QJournalProtocolProtos.SegmentStateProto GetSegmentInfo(long segmentTxId
			)
		{
			FileJournalManager.EditLogFile elf = fjm.GetLogFile(segmentTxId);
			if (elf == null)
			{
				return null;
			}
			if (elf.IsInProgress())
			{
				elf.ScanLog();
			}
			if (elf.GetLastTxId() == HdfsConstants.InvalidTxid)
			{
				Log.Info("Edit log file " + elf + " appears to be empty. " + "Moving it aside..."
					);
				elf.MoveAsideEmptyFile();
				return null;
			}
			QJournalProtocolProtos.SegmentStateProto ret = ((QJournalProtocolProtos.SegmentStateProto
				)QJournalProtocolProtos.SegmentStateProto.NewBuilder().SetStartTxId(segmentTxId)
				.SetEndTxId(elf.GetLastTxId()).SetIsInProgress(elf.IsInProgress()).Build());
			Log.Info("getSegmentInfo(" + segmentTxId + "): " + elf + " -> " + TextFormat.ShortDebugString
				(ret));
			return ret;
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.QJournalProtocol.PrepareRecovery(Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.RequestInfo, long)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual QJournalProtocolProtos.PrepareRecoveryResponseProto PrepareRecovery
			(RequestInfo reqInfo, long segmentTxId)
		{
			lock (this)
			{
				CheckFormatted();
				CheckRequest(reqInfo);
				AbortCurSegment();
				QJournalProtocolProtos.PrepareRecoveryResponseProto.Builder builder = QJournalProtocolProtos.PrepareRecoveryResponseProto
					.NewBuilder();
				QJournalProtocolProtos.PersistedRecoveryPaxosData previouslyAccepted = GetPersistedPaxosData
					(segmentTxId);
				CompleteHalfDoneAcceptRecovery(previouslyAccepted);
				QJournalProtocolProtos.SegmentStateProto segInfo = GetSegmentInfo(segmentTxId);
				bool hasFinalizedSegment = segInfo != null && !segInfo.GetIsInProgress();
				if (previouslyAccepted != null && !hasFinalizedSegment)
				{
					QJournalProtocolProtos.SegmentStateProto acceptedState = previouslyAccepted.GetSegmentState
						();
					System.Diagnostics.Debug.Assert(acceptedState.GetEndTxId() == segInfo.GetEndTxId(
						), "prev accepted: " + TextFormat.ShortDebugString(previouslyAccepted) + "\n" + 
						"on disk:       " + TextFormat.ShortDebugString(segInfo));
					builder.SetAcceptedInEpoch(previouslyAccepted.GetAcceptedInEpoch()).SetSegmentState
						(previouslyAccepted.GetSegmentState());
				}
				else
				{
					if (segInfo != null)
					{
						builder.SetSegmentState(segInfo);
					}
				}
				builder.SetLastWriterEpoch(lastWriterEpoch.Get());
				if (committedTxnId.Get() != HdfsConstants.InvalidTxid)
				{
					builder.SetLastCommittedTxId(committedTxnId.Get());
				}
				QJournalProtocolProtos.PrepareRecoveryResponseProto resp = ((QJournalProtocolProtos.PrepareRecoveryResponseProto
					)builder.Build());
				Log.Info("Prepared recovery for segment " + segmentTxId + ": " + TextFormat.ShortDebugString
					(resp));
				return resp;
			}
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.QJournalProtocol.AcceptRecovery(Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.RequestInfo, Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.QJournalProtocolProtos.SegmentStateProto, System.Uri)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AcceptRecovery(RequestInfo reqInfo, QJournalProtocolProtos.SegmentStateProto
			 segment, Uri fromUrl)
		{
			lock (this)
			{
				CheckFormatted();
				CheckRequest(reqInfo);
				AbortCurSegment();
				long segmentTxId = segment.GetStartTxId();
				// Basic sanity checks that the segment is well-formed and contains
				// at least one transaction.
				Preconditions.CheckArgument(segment.GetEndTxId() > 0 && segment.GetEndTxId() >= segmentTxId
					, "bad recovery state for segment %s: %s", segmentTxId, TextFormat.ShortDebugString
					(segment));
				QJournalProtocolProtos.PersistedRecoveryPaxosData oldData = GetPersistedPaxosData
					(segmentTxId);
				QJournalProtocolProtos.PersistedRecoveryPaxosData newData = ((QJournalProtocolProtos.PersistedRecoveryPaxosData
					)QJournalProtocolProtos.PersistedRecoveryPaxosData.NewBuilder().SetAcceptedInEpoch
					(reqInfo.GetEpoch()).SetSegmentState(segment).Build());
				// If we previously acted on acceptRecovery() from a higher-numbered writer,
				// this call is out of sync. We should never actually trigger this, since the
				// checkRequest() call above should filter non-increasing epoch numbers.
				if (oldData != null)
				{
					AlwaysAssert(oldData.GetAcceptedInEpoch() <= reqInfo.GetEpoch(), "Bad paxos transition, out-of-order epochs.\nOld: %s\nNew: %s\n"
						, oldData, newData);
				}
				FilePath syncedFile = null;
				QJournalProtocolProtos.SegmentStateProto currentSegment = GetSegmentInfo(segmentTxId
					);
				if (currentSegment == null || currentSegment.GetEndTxId() != segment.GetEndTxId())
				{
					if (currentSegment == null)
					{
						Log.Info("Synchronizing log " + TextFormat.ShortDebugString(segment) + ": no current segment in place"
							);
						// Update the highest txid for lag metrics
						highestWrittenTxId = Math.Max(segment.GetEndTxId(), highestWrittenTxId);
					}
					else
					{
						Log.Info("Synchronizing log " + TextFormat.ShortDebugString(segment) + ": old segment "
							 + TextFormat.ShortDebugString(currentSegment) + " is not the right length");
						// Paranoid sanity check: if the new log is shorter than the log we
						// currently have, we should not end up discarding any transactions
						// which are already Committed.
						if (TxnRange(currentSegment).ContainsLong(committedTxnId.Get()) && !TxnRange(segment
							).ContainsLong(committedTxnId.Get()))
						{
							throw new Exception("Cannot replace segment " + TextFormat.ShortDebugString(currentSegment
								) + " with new segment " + TextFormat.ShortDebugString(segment) + ": would discard already-committed txn "
								 + committedTxnId.Get());
						}
						// Another paranoid check: we should not be asked to synchronize a log
						// on top of a finalized segment.
						AlwaysAssert(currentSegment.GetIsInProgress(), "Should never be asked to synchronize a different log on top of an "
							 + "already-finalized segment");
						// If we're shortening the log, update our highest txid
						// used for lag metrics.
						if (TxnRange(currentSegment).ContainsLong(highestWrittenTxId))
						{
							highestWrittenTxId = segment.GetEndTxId();
						}
					}
					syncedFile = SyncLog(reqInfo, segment, fromUrl);
				}
				else
				{
					Log.Info("Skipping download of log " + TextFormat.ShortDebugString(segment) + ": already have up-to-date logs"
						);
				}
				// This is one of the few places in the protocol where we have a single
				// RPC that results in two distinct actions:
				//
				// - 1) Downloads the new log segment data (above)
				// - 2) Records the new Paxos data about the synchronized segment (below)
				//
				// These need to be treated as a transaction from the perspective
				// of any external process. We do this by treating the persistPaxosData()
				// success as the "commit" of an atomic transaction. If we fail before
				// this point, the downloaded edit log will only exist at a temporary
				// path, and thus not change any externally visible state. If we fail
				// after this point, then any future prepareRecovery() call will see
				// the Paxos data, and by calling completeHalfDoneAcceptRecovery() will
				// roll forward the rename of the referenced log file.
				//
				// See also: HDFS-3955
				//
				// The fault points here are exercised by the randomized fault injection
				// test case to ensure that this atomic "transaction" operates correctly.
				JournalFaultInjector.Get().BeforePersistPaxosData();
				PersistPaxosData(segmentTxId, newData);
				JournalFaultInjector.Get().AfterPersistPaxosData();
				if (syncedFile != null)
				{
					FileUtil.ReplaceFile(syncedFile, storage.GetInProgressEditLog(segmentTxId));
				}
				Log.Info("Accepted recovery for segment " + segmentTxId + ": " + TextFormat.ShortDebugString
					(newData));
			}
		}

		private LongRange TxnRange(QJournalProtocolProtos.SegmentStateProto seg)
		{
			Preconditions.CheckArgument(seg.HasEndTxId(), "invalid segment: %s", seg);
			return new LongRange(seg.GetStartTxId(), seg.GetEndTxId());
		}

		/// <summary>Synchronize a log segment from another JournalNode.</summary>
		/// <remarks>
		/// Synchronize a log segment from another JournalNode. The log is
		/// downloaded from the provided URL into a temporary location on disk,
		/// which is named based on the current request's epoch.
		/// </remarks>
		/// <returns>the temporary location of the downloaded file</returns>
		/// <exception cref="System.IO.IOException"/>
		private FilePath SyncLog(RequestInfo reqInfo, QJournalProtocolProtos.SegmentStateProto
			 segment, Uri url)
		{
			FilePath tmpFile = storage.GetSyncLogTemporaryFile(segment.GetStartTxId(), reqInfo
				.GetEpoch());
			IList<FilePath> localPaths = ImmutableList.Of(tmpFile);
			Log.Info("Synchronizing log " + TextFormat.ShortDebugString(segment) + " from " +
				 url);
			SecurityUtil.DoAsLoginUser(new _PrivilegedExceptionAction_880(this, url, localPaths
				, tmpFile));
			// We may have lost our ticket since last checkpoint, log in again, just in case
			return tmpFile;
		}

		private sealed class _PrivilegedExceptionAction_880 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_880(Journal _enclosing, Uri url, IList<FilePath
				> localPaths, FilePath tmpFile)
			{
				this._enclosing = _enclosing;
				this.url = url;
				this.localPaths = localPaths;
				this.tmpFile = tmpFile;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				if (UserGroupInformation.IsSecurityEnabled())
				{
					UserGroupInformation.GetCurrentUser().CheckTGTAndReloginFromKeytab();
				}
				bool success = false;
				try
				{
					TransferFsImage.DoGetUrl(url, localPaths, this._enclosing.storage, true);
					System.Diagnostics.Debug.Assert(tmpFile.Exists());
					success = true;
				}
				finally
				{
					if (!success)
					{
						if (!tmpFile.Delete())
						{
							Org.Apache.Hadoop.Hdfs.Qjournal.Server.Journal.Log.Warn("Failed to delete temporary file "
								 + tmpFile);
						}
					}
				}
				return null;
			}

			private readonly Journal _enclosing;

			private readonly Uri url;

			private readonly IList<FilePath> localPaths;

			private readonly FilePath tmpFile;
		}

		/// <summary>
		/// In the case the node crashes in between downloading a log segment
		/// and persisting the associated paxos recovery data, the log segment
		/// will be left in its temporary location on disk.
		/// </summary>
		/// <remarks>
		/// In the case the node crashes in between downloading a log segment
		/// and persisting the associated paxos recovery data, the log segment
		/// will be left in its temporary location on disk. Given the paxos data,
		/// we can check if this was indeed the case, and &quot;roll forward&quot;
		/// the atomic operation.
		/// See the inline comments in
		/// <see cref="AcceptRecovery(Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.RequestInfo, Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.QJournalProtocolProtos.SegmentStateProto, System.Uri)
		/// 	"/>
		/// for more
		/// details.
		/// </remarks>
		/// <exception cref="System.IO.IOException">
		/// if the temporary file is unable to be renamed into
		/// place
		/// </exception>
		private void CompleteHalfDoneAcceptRecovery(QJournalProtocolProtos.PersistedRecoveryPaxosData
			 paxosData)
		{
			if (paxosData == null)
			{
				return;
			}
			long segmentId = paxosData.GetSegmentState().GetStartTxId();
			long epoch = paxosData.GetAcceptedInEpoch();
			FilePath tmp = storage.GetSyncLogTemporaryFile(segmentId, epoch);
			if (tmp.Exists())
			{
				FilePath dst = storage.GetInProgressEditLog(segmentId);
				Log.Info("Rolling forward previously half-completed synchronization: " + tmp + " -> "
					 + dst);
				FileUtil.ReplaceFile(tmp, dst);
			}
		}

		/// <summary>Retrieve the persisted data for recovering the given segment from disk.</summary>
		/// <exception cref="System.IO.IOException"/>
		private QJournalProtocolProtos.PersistedRecoveryPaxosData GetPersistedPaxosData(long
			 segmentTxId)
		{
			FilePath f = storage.GetPaxosFile(segmentTxId);
			if (!f.Exists())
			{
				// Default instance has no fields filled in (they're optional)
				return null;
			}
			InputStream @in = new FileInputStream(f);
			try
			{
				QJournalProtocolProtos.PersistedRecoveryPaxosData ret = QJournalProtocolProtos.PersistedRecoveryPaxosData
					.ParseDelimitedFrom(@in);
				Preconditions.CheckState(ret != null && ret.GetSegmentState().GetStartTxId() == segmentTxId
					, "Bad persisted data for segment %s: %s", segmentTxId, ret);
				return ret;
			}
			finally
			{
				IOUtils.CloseStream(@in);
			}
		}

		/// <summary>Persist data for recovering the given segment from disk.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void PersistPaxosData(long segmentTxId, QJournalProtocolProtos.PersistedRecoveryPaxosData
			 newData)
		{
			FilePath f = storage.GetPaxosFile(segmentTxId);
			bool success = false;
			AtomicFileOutputStream fos = new AtomicFileOutputStream(f);
			try
			{
				newData.WriteDelimitedTo(fos);
				fos.Write('\n');
				// Write human-readable data after the protobuf. This is only
				// to assist in debugging -- it's not parsed at all.
				OutputStreamWriter writer = new OutputStreamWriter(fos, Charsets.Utf8);
				writer.Write(newData.ToString());
				writer.Write('\n');
				writer.Flush();
				fos.Flush();
				success = true;
			}
			finally
			{
				if (success)
				{
					IOUtils.CloseStream(fos);
				}
				else
				{
					fos.Abort();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void DiscardSegments(long startTxId)
		{
			lock (this)
			{
				storage.GetJournalManager().DiscardSegments(startTxId);
				// we delete all the segments after the startTxId. let's reset committedTxnId 
				committedTxnId.Set(startTxId - 1);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoPreUpgrade()
		{
			lock (this)
			{
				// Do not hold file lock on committedTxnId, because the containing
				// directory will be renamed.  It will be reopened lazily on next access.
				IOUtils.Cleanup(Log, committedTxnId);
				storage.GetJournalManager().DoPreUpgrade();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoUpgrade(StorageInfo sInfo)
		{
			lock (this)
			{
				long oldCTime = storage.GetCTime();
				storage.cTime = sInfo.cTime;
				int oldLV = storage.GetLayoutVersion();
				storage.layoutVersion = sInfo.layoutVersion;
				Log.Info("Starting upgrade of edits directory: " + ".\n   old LV = " + oldLV + "; old CTime = "
					 + oldCTime + ".\n   new LV = " + storage.GetLayoutVersion() + "; new CTime = " 
					+ storage.GetCTime());
				storage.GetJournalManager().DoUpgrade(storage);
				storage.CreatePaxosDir();
				// Copy over the contents of the epoch data files to the new dir.
				FilePath currentDir = storage.GetSingularStorageDir().GetCurrentDir();
				FilePath previousDir = storage.GetSingularStorageDir().GetPreviousDir();
				PersistentLongFile prevLastPromisedEpoch = new PersistentLongFile(new FilePath(previousDir
					, LastPromisedFilename), 0);
				PersistentLongFile prevLastWriterEpoch = new PersistentLongFile(new FilePath(previousDir
					, LastWriterEpoch), 0);
				BestEffortLongFile prevCommittedTxnId = new BestEffortLongFile(new FilePath(previousDir
					, CommittedTxidFilename), HdfsConstants.InvalidTxid);
				lastPromisedEpoch = new PersistentLongFile(new FilePath(currentDir, LastPromisedFilename
					), 0);
				lastWriterEpoch = new PersistentLongFile(new FilePath(currentDir, LastWriterEpoch
					), 0);
				committedTxnId = new BestEffortLongFile(new FilePath(currentDir, CommittedTxidFilename
					), HdfsConstants.InvalidTxid);
				try
				{
					lastPromisedEpoch.Set(prevLastPromisedEpoch.Get());
					lastWriterEpoch.Set(prevLastWriterEpoch.Get());
					committedTxnId.Set(prevCommittedTxnId.Get());
				}
				finally
				{
					IOUtils.Cleanup(Log, prevCommittedTxnId);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoFinalize()
		{
			lock (this)
			{
				Log.Info("Finalizing upgrade for journal " + storage.GetRoot() + "." + (storage.GetLayoutVersion
					() == 0 ? string.Empty : "\n   cur LV = " + storage.GetLayoutVersion() + "; cur CTime = "
					 + storage.GetCTime()));
				storage.GetJournalManager().DoFinalize();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool CanRollBack(StorageInfo storage, StorageInfo prevStorage, int
			 targetLayoutVersion)
		{
			return this.storage.GetJournalManager().CanRollBack(storage, prevStorage, targetLayoutVersion
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoRollback()
		{
			lock (this)
			{
				// Do not hold file lock on committedTxnId, because the containing
				// directory will be renamed.  It will be reopened lazily on next access.
				IOUtils.Cleanup(Log, committedTxnId);
				storage.GetJournalManager().DoRollback();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetJournalCTime()
		{
			return storage.GetJournalManager().GetJournalCTime();
		}
	}
}
