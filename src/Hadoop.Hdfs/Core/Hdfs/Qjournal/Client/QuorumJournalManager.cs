using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>
	/// A JournalManager that writes to a set of remote JournalNodes,
	/// requiring a quorum of nodes to ack each write.
	/// </summary>
	public class QuorumJournalManager : JournalManager
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Qjournal.Client.QuorumJournalManager
			));

		private readonly int startSegmentTimeoutMs;

		private readonly int prepareRecoveryTimeoutMs;

		private readonly int acceptRecoveryTimeoutMs;

		private readonly int finalizeSegmentTimeoutMs;

		private readonly int selectInputStreamsTimeoutMs;

		private readonly int getJournalStateTimeoutMs;

		private readonly int newEpochTimeoutMs;

		private readonly int writeTxnsTimeoutMs;

		private const int FormatTimeoutMs = 60000;

		private const int HasdataTimeoutMs = 60000;

		private const int CanRollBackTimeoutMs = 60000;

		private const int FinalizeTimeoutMs = 60000;

		private const int PreUpgradeTimeoutMs = 60000;

		private const int RollBackTimeoutMs = 60000;

		private const int UpgradeTimeoutMs = 60000;

		private const int GetJournalCtimeTimeoutMs = 60000;

		private const int DiscardSegmentsTimeoutMs = 60000;

		private readonly Configuration conf;

		private readonly URI uri;

		private readonly NamespaceInfo nsInfo;

		private bool isActiveWriter;

		private readonly AsyncLoggerSet loggers;

		private int outputBufferCapacity = 512 * 1024;

		private readonly URLConnectionFactory connectionFactory;

		/// <exception cref="System.IO.IOException"/>
		public QuorumJournalManager(Configuration conf, URI uri, NamespaceInfo nsInfo)
			: this(conf, uri, nsInfo, IPCLoggerChannel.Factory)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal QuorumJournalManager(Configuration conf, URI uri, NamespaceInfo nsInfo, 
			AsyncLogger.Factory loggerFactory)
		{
			// Timeouts for which the QJM will wait for each of the following actions.
			// Since these don't occur during normal operation, we can
			// use rather lengthy timeouts, and don't need to make them
			// configurable.
			Preconditions.CheckArgument(conf != null, "must be configured");
			this.conf = conf;
			this.uri = uri;
			this.nsInfo = nsInfo;
			this.loggers = new AsyncLoggerSet(CreateLoggers(loggerFactory));
			this.connectionFactory = URLConnectionFactory.NewDefaultURLConnectionFactory(conf
				);
			// Configure timeouts.
			this.startSegmentTimeoutMs = conf.GetInt(DFSConfigKeys.DfsQjournalStartSegmentTimeoutKey
				, DFSConfigKeys.DfsQjournalStartSegmentTimeoutDefault);
			this.prepareRecoveryTimeoutMs = conf.GetInt(DFSConfigKeys.DfsQjournalPrepareRecoveryTimeoutKey
				, DFSConfigKeys.DfsQjournalPrepareRecoveryTimeoutDefault);
			this.acceptRecoveryTimeoutMs = conf.GetInt(DFSConfigKeys.DfsQjournalAcceptRecoveryTimeoutKey
				, DFSConfigKeys.DfsQjournalAcceptRecoveryTimeoutDefault);
			this.finalizeSegmentTimeoutMs = conf.GetInt(DFSConfigKeys.DfsQjournalFinalizeSegmentTimeoutKey
				, DFSConfigKeys.DfsQjournalFinalizeSegmentTimeoutDefault);
			this.selectInputStreamsTimeoutMs = conf.GetInt(DFSConfigKeys.DfsQjournalSelectInputStreamsTimeoutKey
				, DFSConfigKeys.DfsQjournalSelectInputStreamsTimeoutDefault);
			this.getJournalStateTimeoutMs = conf.GetInt(DFSConfigKeys.DfsQjournalGetJournalStateTimeoutKey
				, DFSConfigKeys.DfsQjournalGetJournalStateTimeoutDefault);
			this.newEpochTimeoutMs = conf.GetInt(DFSConfigKeys.DfsQjournalNewEpochTimeoutKey, 
				DFSConfigKeys.DfsQjournalNewEpochTimeoutDefault);
			this.writeTxnsTimeoutMs = conf.GetInt(DFSConfigKeys.DfsQjournalWriteTxnsTimeoutKey
				, DFSConfigKeys.DfsQjournalWriteTxnsTimeoutDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<AsyncLogger> CreateLoggers(AsyncLogger.Factory factory
			)
		{
			return CreateLoggers(conf, uri, nsInfo, factory);
		}

		internal static string ParseJournalId(URI uri)
		{
			string path = uri.GetPath();
			Preconditions.CheckArgument(path != null && !path.IsEmpty(), "Bad URI '%s': must identify journal in path component"
				, uri);
			string journalId = Sharpen.Runtime.Substring(path, 1);
			CheckJournalId(journalId);
			return journalId;
		}

		public static void CheckJournalId(string jid)
		{
			Preconditions.CheckArgument(jid != null && !jid.IsEmpty() && !jid.Contains("/") &&
				 !jid.StartsWith("."), "bad journal id: " + jid);
		}

		/// <summary>
		/// Fence any previous writers, and obtain a unique epoch number
		/// for write-access to the journal nodes.
		/// </summary>
		/// <returns>the new, unique epoch number</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual IDictionary<AsyncLogger, QJournalProtocolProtos.NewEpochResponseProto
			> CreateNewUniqueEpoch()
		{
			Preconditions.CheckState(!loggers.IsEpochEstablished(), "epoch already created");
			IDictionary<AsyncLogger, QJournalProtocolProtos.GetJournalStateResponseProto> lastPromises
				 = loggers.WaitForWriteQuorum(loggers.GetJournalState(), getJournalStateTimeoutMs
				, "getJournalState()");
			long maxPromised = long.MinValue;
			foreach (QJournalProtocolProtos.GetJournalStateResponseProto resp in lastPromises
				.Values)
			{
				maxPromised = Math.Max(maxPromised, resp.GetLastPromisedEpoch());
			}
			System.Diagnostics.Debug.Assert(maxPromised >= 0);
			long myEpoch = maxPromised + 1;
			IDictionary<AsyncLogger, QJournalProtocolProtos.NewEpochResponseProto> resps = loggers
				.WaitForWriteQuorum(loggers.NewEpoch(nsInfo, myEpoch), newEpochTimeoutMs, "newEpoch("
				 + myEpoch + ")");
			loggers.SetEpoch(myEpoch);
			return resps;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Format(NamespaceInfo nsInfo)
		{
			QuorumCall<AsyncLogger, Void> call = loggers.Format(nsInfo);
			try
			{
				call.WaitFor(loggers.Size(), loggers.Size(), 0, FormatTimeoutMs, "format");
			}
			catch (Exception)
			{
				throw new IOException("Interrupted waiting for format() response");
			}
			catch (TimeoutException)
			{
				throw new IOException("Timed out waiting for format() response");
			}
			if (call.CountExceptions() > 0)
			{
				call.RethrowException("Could not format one or more JournalNodes");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool HasSomeData()
		{
			QuorumCall<AsyncLogger, bool> call = loggers.IsFormatted();
			try
			{
				call.WaitFor(loggers.Size(), 0, 0, HasdataTimeoutMs, "hasSomeData");
			}
			catch (Exception)
			{
				throw new IOException("Interrupted while determining if JNs have data");
			}
			catch (TimeoutException)
			{
				throw new IOException("Timed out waiting for response from loggers");
			}
			if (call.CountExceptions() > 0)
			{
				call.RethrowException("Unable to check if JNs are ready for formatting");
			}
			// If any of the loggers returned with a non-empty manifest, then
			// we should prompt for format.
			foreach (bool hasData in call.GetResults().Values)
			{
				if (hasData)
				{
					return true;
				}
			}
			// Otherwise, none were formatted, we can safely format.
			return false;
		}

		/// <summary>Run recovery/synchronization for a specific segment.</summary>
		/// <remarks>
		/// Run recovery/synchronization for a specific segment.
		/// Postconditions:
		/// <ul>
		/// <li>This segment will be finalized on a majority
		/// of nodes.</li>
		/// <li>All nodes which contain the finalized segment will
		/// agree on the length.</li>
		/// </ul>
		/// </remarks>
		/// <param name="segmentTxId">the starting txid of the segment</param>
		/// <exception cref="System.IO.IOException"/>
		private void RecoverUnclosedSegment(long segmentTxId)
		{
			Preconditions.CheckArgument(segmentTxId > 0);
			Log.Info("Beginning recovery of unclosed segment starting at txid " + segmentTxId
				);
			// Step 1. Prepare recovery
			QuorumCall<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto> prepare
				 = loggers.PrepareRecovery(segmentTxId);
			IDictionary<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto> prepareResponses
				 = loggers.WaitForWriteQuorum(prepare, prepareRecoveryTimeoutMs, "prepareRecovery("
				 + segmentTxId + ")");
			Log.Info("Recovery prepare phase complete. Responses:\n" + QuorumCall.MapToString
				(prepareResponses));
			// Determine the logger who either:
			// a) Has already accepted a previous proposal that's higher than any
			//    other
			//
			//  OR, if no such logger exists:
			//
			// b) Has the longest log starting at this transaction ID
			// TODO: we should collect any "ties" and pass the URL for all of them
			// when syncing, so we can tolerate failure during recovery better.
			KeyValuePair<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto> bestEntry
				 = Sharpen.Collections.Max(prepareResponses, SegmentRecoveryComparator.Instance);
			AsyncLogger bestLogger = bestEntry.Key;
			QJournalProtocolProtos.PrepareRecoveryResponseProto bestResponse = bestEntry.Value;
			// Log the above decision, check invariants.
			if (bestResponse.HasAcceptedInEpoch())
			{
				Log.Info("Using already-accepted recovery for segment " + "starting at txid " + segmentTxId
					 + ": " + bestEntry);
			}
			else
			{
				if (bestResponse.HasSegmentState())
				{
					Log.Info("Using longest log: " + bestEntry);
				}
				else
				{
					// None of the responses to prepareRecovery() had a segment at the given
					// txid. This can happen for example in the following situation:
					// - 3 JNs: JN1, JN2, JN3
					// - writer starts segment 101 on JN1, then crashes before
					//   writing to JN2 and JN3
					// - during newEpoch(), we saw the segment on JN1 and decide to
					//   recover segment 101
					// - before prepare(), JN1 crashes, and we only talk to JN2 and JN3,
					//   neither of which has any entry for this log.
					// In this case, it is allowed to do nothing for recovery, since the
					// segment wasn't started on a quorum of nodes.
					// Sanity check: we should only get here if none of the responses had
					// a log. This should be a postcondition of the recovery comparator,
					// but a bug in the comparator might cause us to get here.
					foreach (QJournalProtocolProtos.PrepareRecoveryResponseProto resp in prepareResponses
						.Values)
					{
						System.Diagnostics.Debug.Assert(!resp.HasSegmentState(), "One of the loggers had a response, but no best logger "
							 + "was found.");
					}
					Log.Info("None of the responders had a log to recover: " + QuorumCall.MapToString
						(prepareResponses));
					return;
				}
			}
			QJournalProtocolProtos.SegmentStateProto logToSync = bestResponse.GetSegmentState
				();
			System.Diagnostics.Debug.Assert(segmentTxId == logToSync.GetStartTxId());
			// Sanity check: none of the loggers should be aware of a higher
			// txid than the txid we intend to truncate to
			foreach (KeyValuePair<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto
				> e in prepareResponses)
			{
				AsyncLogger logger = e.Key;
				QJournalProtocolProtos.PrepareRecoveryResponseProto resp = e.Value;
				if (resp.HasLastCommittedTxId() && resp.GetLastCommittedTxId() > logToSync.GetEndTxId
					())
				{
					throw new Exception("Decided to synchronize log to " + logToSync + " but logger "
						 + logger + " had seen txid " + resp.GetLastCommittedTxId() + " committed");
				}
			}
			Uri syncFromUrl = bestLogger.BuildURLToFetchLogs(segmentTxId);
			QuorumCall<AsyncLogger, Void> accept = loggers.AcceptRecovery(logToSync, syncFromUrl
				);
			loggers.WaitForWriteQuorum(accept, acceptRecoveryTimeoutMs, "acceptRecovery(" + TextFormat
				.ShortDebugString(logToSync) + ")");
			// If one of the loggers above missed the synchronization step above, but
			// we send a finalize() here, that's OK. It validates the log before
			// finalizing. Hence, even if it is not "in sync", it won't incorrectly
			// finalize.
			QuorumCall<AsyncLogger, Void> finalize = loggers.FinalizeLogSegment(logToSync.GetStartTxId
				(), logToSync.GetEndTxId());
			loggers.WaitForWriteQuorum(finalize, finalizeSegmentTimeoutMs, string.Format("finalizeLogSegment(%s-%s)"
				, logToSync.GetStartTxId(), logToSync.GetEndTxId()));
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IList<AsyncLogger> CreateLoggers(Configuration conf, URI uri, NamespaceInfo
			 nsInfo, AsyncLogger.Factory factory)
		{
			IList<AsyncLogger> ret = Lists.NewArrayList();
			IList<IPEndPoint> addrs = GetLoggerAddresses(uri);
			string jid = ParseJournalId(uri);
			foreach (IPEndPoint addr in addrs)
			{
				ret.AddItem(factory.CreateLogger(conf, nsInfo, jid, addr));
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		private static IList<IPEndPoint> GetLoggerAddresses(URI uri)
		{
			string authority = uri.GetAuthority();
			Preconditions.CheckArgument(authority != null && !authority.IsEmpty(), "URI has no authority: "
				 + uri);
			string[] parts = StringUtils.Split(authority, ';');
			for (int i = 0; i < parts.Length; i++)
			{
				parts[i] = parts[i].Trim();
			}
			if (parts.Length % 2 == 0)
			{
				Log.Warn("Quorum journal URI '" + uri + "' has an even number " + "of Journal Nodes specified. This is not recommended!"
					);
			}
			IList<IPEndPoint> addrs = Lists.NewArrayList();
			foreach (string addr in parts)
			{
				addrs.AddItem(NetUtils.CreateSocketAddr(addr, DFSConfigKeys.DfsJournalnodeRpcPortDefault
					));
			}
			return addrs;
		}

		/// <exception cref="System.IO.IOException"/>
		public override EditLogOutputStream StartLogSegment(long txId, int layoutVersion)
		{
			Preconditions.CheckState(isActiveWriter, "must recover segments before starting a new one"
				);
			QuorumCall<AsyncLogger, Void> q = loggers.StartLogSegment(txId, layoutVersion);
			loggers.WaitForWriteQuorum(q, startSegmentTimeoutMs, "startLogSegment(" + txId + 
				")");
			return new QuorumOutputStream(loggers, txId, outputBufferCapacity, writeTxnsTimeoutMs
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void FinalizeLogSegment(long firstTxId, long lastTxId)
		{
			QuorumCall<AsyncLogger, Void> q = loggers.FinalizeLogSegment(firstTxId, lastTxId);
			loggers.WaitForWriteQuorum(q, finalizeSegmentTimeoutMs, string.Format("finalizeLogSegment(%s-%s)"
				, firstTxId, lastTxId));
		}

		public override void SetOutputBufferCapacity(int size)
		{
			outputBufferCapacity = size;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void PurgeLogsOlderThan(long minTxIdToKeep)
		{
			// This purges asynchronously -- there's no need to wait for a quorum
			// here, because it's always OK to fail.
			Log.Info("Purging remote journals older than txid " + minTxIdToKeep);
			loggers.PurgeLogsOlderThan(minTxIdToKeep);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RecoverUnfinalizedSegments()
		{
			Preconditions.CheckState(!isActiveWriter, "already active writer");
			Log.Info("Starting recovery process for unclosed journal segments...");
			IDictionary<AsyncLogger, QJournalProtocolProtos.NewEpochResponseProto> resps = CreateNewUniqueEpoch
				();
			Log.Info("Successfully started new epoch " + loggers.GetEpoch());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("newEpoch(" + loggers.GetEpoch() + ") responses:\n" + QuorumCall.MapToString
					(resps));
			}
			long mostRecentSegmentTxId = long.MinValue;
			foreach (QJournalProtocolProtos.NewEpochResponseProto r in resps.Values)
			{
				if (r.HasLastSegmentTxId())
				{
					mostRecentSegmentTxId = Math.Max(mostRecentSegmentTxId, r.GetLastSegmentTxId());
				}
			}
			// On a completely fresh system, none of the journals have any
			// segments, so there's nothing to recover.
			if (mostRecentSegmentTxId != long.MinValue)
			{
				RecoverUnclosedSegment(mostRecentSegmentTxId);
			}
			isActiveWriter = true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			loggers.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SelectInputStreams(ICollection<EditLogInputStream> streams, long
			 fromTxnId, bool inProgressOk)
		{
			QuorumCall<AsyncLogger, RemoteEditLogManifest> q = loggers.GetEditLogManifest(fromTxnId
				, inProgressOk);
			IDictionary<AsyncLogger, RemoteEditLogManifest> resps = loggers.WaitForWriteQuorum
				(q, selectInputStreamsTimeoutMs, "selectInputStreams");
			Log.Debug("selectInputStream manifests:\n" + Joiner.On("\n").WithKeyValueSeparator
				(": ").Join(resps));
			PriorityQueue<EditLogInputStream> allStreams = new PriorityQueue<EditLogInputStream
				>(64, JournalSet.EditLogInputStreamComparator);
			foreach (KeyValuePair<AsyncLogger, RemoteEditLogManifest> e in resps)
			{
				AsyncLogger logger = e.Key;
				RemoteEditLogManifest manifest = e.Value;
				foreach (RemoteEditLog remoteLog in manifest.GetLogs())
				{
					Uri url = logger.BuildURLToFetchLogs(remoteLog.GetStartTxId());
					EditLogInputStream elis = EditLogFileInputStream.FromUrl(connectionFactory, url, 
						remoteLog.GetStartTxId(), remoteLog.GetEndTxId(), remoteLog.IsInProgress());
					allStreams.AddItem(elis);
				}
			}
			JournalSet.ChainAndMakeRedundantStreams(streams, allStreams, fromTxnId);
		}

		public override string ToString()
		{
			return "QJM to " + loggers;
		}

		[VisibleForTesting]
		internal virtual AsyncLoggerSet GetLoggerSetForTests()
		{
			return loggers;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoPreUpgrade()
		{
			QuorumCall<AsyncLogger, Void> call = loggers.DoPreUpgrade();
			try
			{
				call.WaitFor(loggers.Size(), loggers.Size(), 0, PreUpgradeTimeoutMs, "doPreUpgrade"
					);
				if (call.CountExceptions() > 0)
				{
					call.RethrowException("Could not do pre-upgrade of one or more JournalNodes");
				}
			}
			catch (Exception)
			{
				throw new IOException("Interrupted waiting for doPreUpgrade() response");
			}
			catch (TimeoutException)
			{
				throw new IOException("Timed out waiting for doPreUpgrade() response");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoUpgrade(Storage storage)
		{
			QuorumCall<AsyncLogger, Void> call = loggers.DoUpgrade(storage);
			try
			{
				call.WaitFor(loggers.Size(), loggers.Size(), 0, UpgradeTimeoutMs, "doUpgrade");
				if (call.CountExceptions() > 0)
				{
					call.RethrowException("Could not perform upgrade of one or more JournalNodes");
				}
			}
			catch (Exception)
			{
				throw new IOException("Interrupted waiting for doUpgrade() response");
			}
			catch (TimeoutException)
			{
				throw new IOException("Timed out waiting for doUpgrade() response");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoFinalize()
		{
			QuorumCall<AsyncLogger, Void> call = loggers.DoFinalize();
			try
			{
				call.WaitFor(loggers.Size(), loggers.Size(), 0, FinalizeTimeoutMs, "doFinalize");
				if (call.CountExceptions() > 0)
				{
					call.RethrowException("Could not finalize one or more JournalNodes");
				}
			}
			catch (Exception)
			{
				throw new IOException("Interrupted waiting for doFinalize() response");
			}
			catch (TimeoutException)
			{
				throw new IOException("Timed out waiting for doFinalize() response");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool CanRollBack(StorageInfo storage, StorageInfo prevStorage, int
			 targetLayoutVersion)
		{
			QuorumCall<AsyncLogger, bool> call = loggers.CanRollBack(storage, prevStorage, targetLayoutVersion
				);
			try
			{
				call.WaitFor(loggers.Size(), loggers.Size(), 0, CanRollBackTimeoutMs, "lockSharedStorage"
					);
				if (call.CountExceptions() > 0)
				{
					call.RethrowException("Could not check if roll back possible for" + " one or more JournalNodes"
						);
				}
				// Either they all return the same thing or this call fails, so we can
				// just return the first result.
				try
				{
					DFSUtil.AssertAllResultsEqual(call.GetResults().Values);
				}
				catch (Exception ae)
				{
					throw new IOException("Results differed for canRollBack", ae);
				}
				foreach (bool result in call.GetResults().Values)
				{
					return result;
				}
			}
			catch (Exception)
			{
				throw new IOException("Interrupted waiting for lockSharedStorage() " + "response"
					);
			}
			catch (TimeoutException)
			{
				throw new IOException("Timed out waiting for lockSharedStorage() " + "response");
			}
			throw new Exception("Unreachable code.");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoRollback()
		{
			QuorumCall<AsyncLogger, Void> call = loggers.DoRollback();
			try
			{
				call.WaitFor(loggers.Size(), loggers.Size(), 0, RollBackTimeoutMs, "doRollback");
				if (call.CountExceptions() > 0)
				{
					call.RethrowException("Could not perform rollback of one or more JournalNodes");
				}
			}
			catch (Exception)
			{
				throw new IOException("Interrupted waiting for doFinalize() response");
			}
			catch (TimeoutException)
			{
				throw new IOException("Timed out waiting for doFinalize() response");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetJournalCTime()
		{
			QuorumCall<AsyncLogger, long> call = loggers.GetJournalCTime();
			try
			{
				call.WaitFor(loggers.Size(), loggers.Size(), 0, GetJournalCtimeTimeoutMs, "getJournalCTime"
					);
				if (call.CountExceptions() > 0)
				{
					call.RethrowException("Could not journal CTime for one " + "more JournalNodes");
				}
				// Either they all return the same thing or this call fails, so we can
				// just return the first result.
				try
				{
					DFSUtil.AssertAllResultsEqual(call.GetResults().Values);
				}
				catch (Exception ae)
				{
					throw new IOException("Results differed for getJournalCTime", ae);
				}
				foreach (long result in call.GetResults().Values)
				{
					return result;
				}
			}
			catch (Exception)
			{
				throw new IOException("Interrupted waiting for getJournalCTime() " + "response");
			}
			catch (TimeoutException)
			{
				throw new IOException("Timed out waiting for getJournalCTime() " + "response");
			}
			throw new Exception("Unreachable code.");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DiscardSegments(long startTxId)
		{
			QuorumCall<AsyncLogger, Void> call = loggers.DiscardSegments(startTxId);
			try
			{
				call.WaitFor(loggers.Size(), loggers.Size(), 0, DiscardSegmentsTimeoutMs, "discardSegments"
					);
				if (call.CountExceptions() > 0)
				{
					call.RethrowException("Could not perform discardSegments of one or more JournalNodes"
						);
				}
			}
			catch (Exception)
			{
				throw new IOException("Interrupted waiting for discardSegments() response");
			}
			catch (TimeoutException)
			{
				throw new IOException("Timed out waiting for discardSegments() response");
			}
		}
	}
}
