using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>
	/// Wrapper around a set of Loggers, taking care of fanning out
	/// calls to the underlying loggers and constructing corresponding
	/// <see cref="QuorumCall{KEY, RESULT}"/>
	/// instances.
	/// </summary>
	internal class AsyncLoggerSet
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Qjournal.Client.AsyncLoggerSet
			));

		private readonly IList<AsyncLogger> loggers;

		private const long InvalidEpoch = -1;

		private long myEpoch = InvalidEpoch;

		public AsyncLoggerSet(IList<AsyncLogger> loggers)
		{
			this.loggers = ImmutableList.CopyOf(loggers);
		}

		internal virtual void SetEpoch(long e)
		{
			Preconditions.CheckState(!IsEpochEstablished(), "Epoch already established: epoch=%s"
				, myEpoch);
			myEpoch = e;
			foreach (AsyncLogger l in loggers)
			{
				l.SetEpoch(e);
			}
		}

		/// <summary>Set the highest successfully committed txid seen by the writer.</summary>
		/// <remarks>
		/// Set the highest successfully committed txid seen by the writer.
		/// This should be called after a successful write to a quorum, and is used
		/// for extra sanity checks against the protocol. See HDFS-3863.
		/// </remarks>
		public virtual void SetCommittedTxId(long txid)
		{
			foreach (AsyncLogger logger in loggers)
			{
				logger.SetCommittedTxId(txid);
			}
		}

		/// <returns>true if an epoch has been established.</returns>
		internal virtual bool IsEpochEstablished()
		{
			return myEpoch != InvalidEpoch;
		}

		/// <returns>
		/// the epoch number for this writer. This may only be called after
		/// a successful call to
		/// <see cref="#createNewUniqueEpoch(NamespaceInfo)"/>
		/// .
		/// </returns>
		internal virtual long GetEpoch()
		{
			Preconditions.CheckState(myEpoch != InvalidEpoch, "No epoch created yet");
			return myEpoch;
		}

		/// <summary>Close all of the underlying loggers.</summary>
		internal virtual void Close()
		{
			foreach (AsyncLogger logger in loggers)
			{
				logger.Close();
			}
		}

		internal virtual void PurgeLogsOlderThan(long minTxIdToKeep)
		{
			foreach (AsyncLogger logger in loggers)
			{
				logger.PurgeLogsOlderThan(minTxIdToKeep);
			}
		}

		/// <summary>Wait for a quorum of loggers to respond to the given call.</summary>
		/// <remarks>
		/// Wait for a quorum of loggers to respond to the given call. If a quorum
		/// can't be achieved, throws a QuorumException.
		/// </remarks>
		/// <param name="q">the quorum call</param>
		/// <param name="timeoutMs">the number of millis to wait</param>
		/// <param name="operationName">textual description of the operation, for logging</param>
		/// <returns>a map of successful results</returns>
		/// <exception cref="QuorumException">if a quorum doesn't respond with success</exception>
		/// <exception cref="System.IO.IOException">if the thread is interrupted or times out
		/// 	</exception>
		internal virtual IDictionary<AsyncLogger, V> WaitForWriteQuorum<V>(QuorumCall<AsyncLogger
			, V> q, int timeoutMs, string operationName)
		{
			int majority = GetMajoritySize();
			try
			{
				q.WaitFor(loggers.Count, majority, majority, timeoutMs, operationName);
			}
			catch (Exception)
			{
				// either all respond 
				// or we get a majority successes
				// or we get a majority failures,
				Sharpen.Thread.CurrentThread().Interrupt();
				throw new IOException("Interrupted waiting " + timeoutMs + "ms for a " + "quorum of nodes to respond."
					);
			}
			catch (TimeoutException)
			{
				throw new IOException("Timed out waiting " + timeoutMs + "ms for a " + "quorum of nodes to respond."
					);
			}
			if (q.CountSuccesses() < majority)
			{
				q.RethrowException("Got too many exceptions to achieve quorum size " + GetMajorityString
					());
			}
			return q.GetResults();
		}

		/// <returns>the number of nodes which are required to obtain a quorum.</returns>
		internal virtual int GetMajoritySize()
		{
			return loggers.Count / 2 + 1;
		}

		/// <returns>a textual description of the majority size (eg "2/3" or "3/5")</returns>
		internal virtual string GetMajorityString()
		{
			return GetMajoritySize() + "/" + loggers.Count;
		}

		/// <returns>the number of loggers behind this set</returns>
		internal virtual int Size()
		{
			return loggers.Count;
		}

		public override string ToString()
		{
			return "[" + Joiner.On(", ").Join(loggers) + "]";
		}

		/// <summary>
		/// Append an HTML-formatted status readout on the current
		/// state of the underlying loggers.
		/// </summary>
		/// <param name="sb">the StringBuilder to append to</param>
		internal virtual void AppendReport(StringBuilder sb)
		{
			for (int i = 0; i < len; ++i)
			{
				AsyncLogger l = loggers[i];
				if (i != 0)
				{
					sb.Append(", ");
				}
				sb.Append(l).Append(" (");
				l.AppendReport(sb);
				sb.Append(")");
			}
		}

		/// <returns>
		/// the (mutable) list of loggers, for use in tests to
		/// set up spies
		/// </returns>
		[VisibleForTesting]
		internal virtual IList<AsyncLogger> GetLoggersForTests()
		{
			return loggers;
		}

		///////////////////////////////////////////////////////////////////////////
		// The rest of this file is simply boilerplate wrappers which fan-out the
		// various IPC calls to the underlying AsyncLoggers and wrap the result
		// in a QuorumCall.
		///////////////////////////////////////////////////////////////////////////
		public virtual QuorumCall<AsyncLogger, QJournalProtocolProtos.GetJournalStateResponseProto
			> GetJournalState()
		{
			IDictionary<AsyncLogger, ListenableFuture<QJournalProtocolProtos.GetJournalStateResponseProto
				>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				calls[logger] = logger.GetJournalState();
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, bool> IsFormatted()
		{
			IDictionary<AsyncLogger, ListenableFuture<bool>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				calls[logger] = logger.IsFormatted();
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, QJournalProtocolProtos.NewEpochResponseProto
			> NewEpoch(NamespaceInfo nsInfo, long epoch)
		{
			IDictionary<AsyncLogger, ListenableFuture<QJournalProtocolProtos.NewEpochResponseProto
				>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				calls[logger] = logger.NewEpoch(epoch);
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, Void> StartLogSegment(long txid, int layoutVersion
			)
		{
			IDictionary<AsyncLogger, ListenableFuture<Void>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				calls[logger] = logger.StartLogSegment(txid, layoutVersion);
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, Void> FinalizeLogSegment(long firstTxId, long
			 lastTxId)
		{
			IDictionary<AsyncLogger, ListenableFuture<Void>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				calls[logger] = logger.FinalizeLogSegment(firstTxId, lastTxId);
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, Void> SendEdits(long segmentTxId, long firstTxnId
			, int numTxns, byte[] data)
		{
			IDictionary<AsyncLogger, ListenableFuture<Void>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<Void> future = logger.SendEdits(segmentTxId, firstTxnId, numTxns
					, data);
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, RemoteEditLogManifest> GetEditLogManifest(
			long fromTxnId, bool inProgressOk)
		{
			IDictionary<AsyncLogger, ListenableFuture<RemoteEditLogManifest>> calls = Maps.NewHashMap
				();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<RemoteEditLogManifest> future = logger.GetEditLogManifest(fromTxnId
					, inProgressOk);
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		internal virtual QuorumCall<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto
			> PrepareRecovery(long segmentTxId)
		{
			IDictionary<AsyncLogger, ListenableFuture<QJournalProtocolProtos.PrepareRecoveryResponseProto
				>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<QJournalProtocolProtos.PrepareRecoveryResponseProto> future = logger
					.PrepareRecovery(segmentTxId);
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		internal virtual QuorumCall<AsyncLogger, Void> AcceptRecovery(QJournalProtocolProtos.SegmentStateProto
			 log, Uri fromURL)
		{
			IDictionary<AsyncLogger, ListenableFuture<Void>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<Void> future = logger.AcceptRecovery(log, fromURL);
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		internal virtual QuorumCall<AsyncLogger, Void> Format(NamespaceInfo nsInfo)
		{
			IDictionary<AsyncLogger, ListenableFuture<Void>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<Void> future = logger.Format(nsInfo);
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, Void> DiscardSegments(long startTxId)
		{
			IDictionary<AsyncLogger, ListenableFuture<Void>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<Void> future = logger.DiscardSegments(startTxId);
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		internal virtual QuorumCall<AsyncLogger, Void> DoPreUpgrade()
		{
			IDictionary<AsyncLogger, ListenableFuture<Void>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<Void> future = logger.DoPreUpgrade();
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, Void> DoUpgrade(StorageInfo sInfo)
		{
			IDictionary<AsyncLogger, ListenableFuture<Void>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<Void> future = logger.DoUpgrade(sInfo);
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, Void> DoFinalize()
		{
			IDictionary<AsyncLogger, ListenableFuture<Void>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<Void> future = logger.DoFinalize();
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, bool> CanRollBack(StorageInfo storage, StorageInfo
			 prevStorage, int targetLayoutVersion)
		{
			IDictionary<AsyncLogger, ListenableFuture<bool>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<bool> future = logger.CanRollBack(storage, prevStorage, targetLayoutVersion
					);
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, Void> DoRollback()
		{
			IDictionary<AsyncLogger, ListenableFuture<Void>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<Void> future = logger.DoRollback();
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}

		public virtual QuorumCall<AsyncLogger, long> GetJournalCTime()
		{
			IDictionary<AsyncLogger, ListenableFuture<long>> calls = Maps.NewHashMap();
			foreach (AsyncLogger logger in loggers)
			{
				ListenableFuture<long> future = logger.GetJournalCTime();
				calls[logger] = future;
			}
			return QuorumCall.Create(calls);
		}
	}
}
