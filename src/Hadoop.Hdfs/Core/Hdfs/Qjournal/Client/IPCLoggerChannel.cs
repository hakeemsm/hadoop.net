using System;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Net;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Qjournal.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Qjournal.Server;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>Channel to a remote JournalNode using Hadoop IPC.</summary>
	/// <remarks>
	/// Channel to a remote JournalNode using Hadoop IPC.
	/// All of the calls are run on a separate thread, and return
	/// <see cref="Com.Google.Common.Util.Concurrent.ListenableFuture{V}"/>
	/// instances to wait for their result.
	/// This allows calls to be bound together using the
	/// <see cref="QuorumCall{KEY, RESULT}"/>
	/// class.
	/// </remarks>
	public class IPCLoggerChannel : AsyncLogger
	{
		private readonly Configuration conf;

		protected internal readonly IPEndPoint addr;

		private QJournalProtocol proxy;

		/// <summary>
		/// Executes tasks submitted to it serially, on a single thread, in FIFO order
		/// (generally used for write tasks that should not be reordered).
		/// </summary>
		private readonly ListeningExecutorService singleThreadExecutor;

		/// <summary>
		/// Executes tasks submitted to it in parallel with each other and with those
		/// submitted to singleThreadExecutor (generally used for read tasks that can
		/// be safely reordered and interleaved with writes).
		/// </summary>
		private readonly ListeningExecutorService parallelExecutor;

		private long ipcSerial = 0;

		private long epoch = -1;

		private long committedTxId = HdfsConstants.InvalidTxid;

		private readonly string journalId;

		private readonly NamespaceInfo nsInfo;

		private Uri httpServerURL;

		private readonly IPCLoggerChannelMetrics metrics;

		/// <summary>The number of bytes of edits data still in the queue.</summary>
		private int queuedEditsSizeBytes = 0;

		/// <summary>The highest txid that has been successfully logged on the remote JN.</summary>
		private long highestAckedTxId = 0;

		/// <summary>
		/// Nanotime of the last time we successfully journaled some edits
		/// to the remote node.
		/// </summary>
		private long lastAckNanos = 0;

		/// <summary>Nanotime of the last time that committedTxId was update.</summary>
		/// <remarks>
		/// Nanotime of the last time that committedTxId was update. Used
		/// to calculate the lag in terms of time, rather than just a number
		/// of txns.
		/// </remarks>
		private long lastCommitNanos = 0;

		/// <summary>The maximum number of bytes that can be pending in the queue.</summary>
		/// <remarks>
		/// The maximum number of bytes that can be pending in the queue.
		/// This keeps the writer from hitting OOME if one of the loggers
		/// starts responding really slowly. Eventually, the queue
		/// overflows and it starts to treat the logger as having errored.
		/// </remarks>
		private readonly int queueSizeLimitBytes;

		/// <summary>
		/// If this logger misses some edits, or restarts in the middle of
		/// a segment, the writer won't be able to write any more edits until
		/// the beginning of the next segment.
		/// </summary>
		/// <remarks>
		/// If this logger misses some edits, or restarts in the middle of
		/// a segment, the writer won't be able to write any more edits until
		/// the beginning of the next segment. Upon detecting this situation,
		/// the writer sets this flag to true to avoid sending useless RPCs.
		/// </remarks>
		private bool outOfSync = false;

		/// <summary>Stopwatch which starts counting on each heartbeat that is sent</summary>
		private readonly StopWatch lastHeartbeatStopwatch = new StopWatch();

		private const long HeartbeatIntervalMillis = 1000;

		private const long WarnJournalMillisThreshold = 1000;

		private sealed class _Factory_152 : AsyncLogger.Factory
		{
			public _Factory_152()
			{
			}

			public AsyncLogger CreateLogger(Configuration conf, NamespaceInfo nsInfo, string 
				journalId, IPEndPoint addr)
			{
				return new Org.Apache.Hadoop.Hdfs.Qjournal.Client.IPCLoggerChannel(conf, nsInfo, 
					journalId, addr);
			}
		}

		internal static readonly AsyncLogger.Factory Factory = new _Factory_152();

		public IPCLoggerChannel(Configuration conf, NamespaceInfo nsInfo, string journalId
			, IPEndPoint addr)
		{
			this.conf = conf;
			this.nsInfo = nsInfo;
			this.journalId = journalId;
			this.addr = addr;
			this.queueSizeLimitBytes = 1024 * 1024 * conf.GetInt(DFSConfigKeys.DfsQjournalQueueSizeLimitKey
				, DFSConfigKeys.DfsQjournalQueueSizeLimitDefault);
			singleThreadExecutor = MoreExecutors.ListeningDecorator(CreateSingleThreadExecutor
				());
			parallelExecutor = MoreExecutors.ListeningDecorator(CreateParallelExecutor());
			metrics = IPCLoggerChannelMetrics.Create(this);
		}

		public override void SetEpoch(long epoch)
		{
			lock (this)
			{
				this.epoch = epoch;
			}
		}

		public override void SetCommittedTxId(long txid)
		{
			lock (this)
			{
				Preconditions.CheckArgument(txid >= committedTxId, "Trying to move committed txid backwards in client "
					 + "old: %s new: %s", committedTxId, txid);
				this.committedTxId = txid;
				this.lastCommitNanos = Runtime.NanoTime();
			}
		}

		public override void Close()
		{
			// No more tasks may be submitted after this point.
			singleThreadExecutor.Shutdown();
			parallelExecutor.Shutdown();
			if (proxy != null)
			{
				// TODO: this can hang for quite some time if the client
				// is currently in the middle of a call to a downed JN.
				// We should instead do this asynchronously, and just stop
				// making any more calls after this point (eg clear the queue)
				RPC.StopProxy(proxy);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual QJournalProtocol GetProxy()
		{
			if (proxy != null)
			{
				return proxy;
			}
			proxy = CreateProxy();
			return proxy;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual QJournalProtocol CreateProxy()
		{
			Configuration confCopy = new Configuration(conf);
			// Need to set NODELAY or else batches larger than MTU can trigger 
			// 40ms nagling delays.
			confCopy.SetBoolean(CommonConfigurationKeysPublic.IpcClientTcpnodelayKey, true);
			RPC.SetProtocolEngine(confCopy, typeof(QJournalProtocolPB), typeof(ProtobufRpcEngine
				));
			return SecurityUtil.DoAsLoginUser(new _PrivilegedExceptionAction_227(this, confCopy
				));
		}

		private sealed class _PrivilegedExceptionAction_227 : PrivilegedExceptionAction<QJournalProtocol
			>
		{
			public _PrivilegedExceptionAction_227(IPCLoggerChannel _enclosing, Configuration 
				confCopy)
			{
				this._enclosing = _enclosing;
				this.confCopy = confCopy;
			}

			/// <exception cref="System.IO.IOException"/>
			public QJournalProtocol Run()
			{
				RPC.SetProtocolEngine(confCopy, typeof(QJournalProtocolPB), typeof(ProtobufRpcEngine
					));
				QJournalProtocolPB pbproxy = RPC.GetProxy<QJournalProtocolPB>(RPC.GetProtocolVersion
					(typeof(QJournalProtocolPB)), this._enclosing.addr, confCopy);
				return new QJournalProtocolTranslatorPB(pbproxy);
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly Configuration confCopy;
		}

		/// <summary>Separated out for easy overriding in tests.</summary>
		[VisibleForTesting]
		protected internal virtual ExecutorService CreateSingleThreadExecutor()
		{
			return Executors.NewSingleThreadExecutor(new ThreadFactoryBuilder().SetDaemon(true
				).SetNameFormat("Logger channel (from single-thread executor) to " + addr).SetUncaughtExceptionHandler
				(UncaughtExceptionHandlers.SystemExit()).Build());
		}

		/// <summary>Separated out for easy overriding in tests.</summary>
		[VisibleForTesting]
		protected internal virtual ExecutorService CreateParallelExecutor()
		{
			return Executors.NewCachedThreadPool(new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat
				("Logger channel (from parallel executor) to " + addr).SetUncaughtExceptionHandler
				(UncaughtExceptionHandlers.SystemExit()).Build());
		}

		public override Uri BuildURLToFetchLogs(long segmentTxId)
		{
			Preconditions.CheckArgument(segmentTxId > 0, "Invalid segment: %s", segmentTxId);
			Preconditions.CheckState(HasHttpServerEndPoint(), "No HTTP/HTTPS endpoint");
			try
			{
				string path = GetJournalEditServlet.BuildPath(journalId, segmentTxId, nsInfo);
				return new Uri(httpServerURL, path);
			}
			catch (UriFormatException e)
			{
				// should never get here.
				throw new RuntimeException(e);
			}
		}

		private RequestInfo CreateReqInfo()
		{
			lock (this)
			{
				Preconditions.CheckState(epoch > 0, "bad epoch: " + epoch);
				return new RequestInfo(journalId, epoch, ipcSerial++, committedTxId);
			}
		}

		[VisibleForTesting]
		internal virtual long GetNextIpcSerial()
		{
			lock (this)
			{
				return ipcSerial;
			}
		}

		public virtual int GetQueuedEditsSize()
		{
			lock (this)
			{
				return queuedEditsSizeBytes;
			}
		}

		public virtual IPEndPoint GetRemoteAddress()
		{
			return addr;
		}

		/// <returns>
		/// true if the server has gotten out of sync from the client,
		/// and thus a log roll is required for this logger to successfully start
		/// logging more edits.
		/// </returns>
		public virtual bool IsOutOfSync()
		{
			lock (this)
			{
				return outOfSync;
			}
		}

		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		internal virtual void WaitForAllPendingCalls()
		{
			try
			{
				singleThreadExecutor.Submit(new _Runnable_317()).Get();
			}
			catch (ExecutionException e)
			{
				// This can't happen!
				throw new Exception(e);
			}
		}

		private sealed class _Runnable_317 : Runnable
		{
			public _Runnable_317()
			{
			}

			public void Run()
			{
			}
		}

		public override ListenableFuture<bool> IsFormatted()
		{
			return singleThreadExecutor.Submit(new _Callable_330(this));
		}

		private sealed class _Callable_330 : Callable<bool>
		{
			public _Callable_330(IPCLoggerChannel _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public bool Call()
			{
				return this._enclosing.GetProxy().IsFormatted(this._enclosing.journalId);
			}

			private readonly IPCLoggerChannel _enclosing;
		}

		public override ListenableFuture<QJournalProtocolProtos.GetJournalStateResponseProto
			> GetJournalState()
		{
			return singleThreadExecutor.Submit(new _Callable_340(this));
		}

		private sealed class _Callable_340 : Callable<QJournalProtocolProtos.GetJournalStateResponseProto
			>
		{
			public _Callable_340(IPCLoggerChannel _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public QJournalProtocolProtos.GetJournalStateResponseProto Call()
			{
				QJournalProtocolProtos.GetJournalStateResponseProto ret = this._enclosing.GetProxy
					().GetJournalState(this._enclosing.journalId);
				this._enclosing.ConstructHttpServerURI(ret);
				return ret;
			}

			private readonly IPCLoggerChannel _enclosing;
		}

		public override ListenableFuture<QJournalProtocolProtos.NewEpochResponseProto> NewEpoch
			(long epoch)
		{
			return singleThreadExecutor.Submit(new _Callable_354(this, epoch));
		}

		private sealed class _Callable_354 : Callable<QJournalProtocolProtos.NewEpochResponseProto
			>
		{
			public _Callable_354(IPCLoggerChannel _enclosing, long epoch)
			{
				this._enclosing = _enclosing;
				this.epoch = epoch;
			}

			/// <exception cref="System.IO.IOException"/>
			public QJournalProtocolProtos.NewEpochResponseProto Call()
			{
				return this._enclosing.GetProxy().NewEpoch(this._enclosing.journalId, this._enclosing
					.nsInfo, epoch);
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly long epoch;
		}

		public override ListenableFuture<Void> SendEdits(long segmentTxId, long firstTxnId
			, int numTxns, byte[] data)
		{
			try
			{
				ReserveQueueSpace(data.Length);
			}
			catch (LoggerTooFarBehindException e)
			{
				return Futures.ImmediateFailedFuture(e);
			}
			// When this batch is acked, we use its submission time in order
			// to calculate how far we are lagging.
			long submitNanos = Runtime.NanoTime();
			ListenableFuture<Void> ret = null;
			try
			{
				ret = singleThreadExecutor.Submit(new _Callable_378(this, segmentTxId, firstTxnId
					, numTxns, data, submitNanos));
			}
			finally
			{
				if (ret == null)
				{
					// it didn't successfully get submitted,
					// so adjust the queue size back down.
					UnreserveQueueSpace(data.Length);
				}
				else
				{
					// It was submitted to the queue, so adjust the length
					// once the call completes, regardless of whether it
					// succeeds or fails.
					Futures.AddCallback(ret, new _FutureCallback_428(this, data));
				}
			}
			return ret;
		}

		private sealed class _Callable_378 : Callable<Void>
		{
			public _Callable_378(IPCLoggerChannel _enclosing, long segmentTxId, long firstTxnId
				, int numTxns, byte[] data, long submitNanos)
			{
				this._enclosing = _enclosing;
				this.segmentTxId = segmentTxId;
				this.firstTxnId = firstTxnId;
				this.numTxns = numTxns;
				this.data = data;
				this.submitNanos = submitNanos;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				this._enclosing.ThrowIfOutOfSync();
				long rpcSendTimeNanos = Runtime.NanoTime();
				try
				{
					this._enclosing.GetProxy().Journal(this._enclosing.CreateReqInfo(), segmentTxId, 
						firstTxnId, numTxns, data);
				}
				catch (IOException e)
				{
					QuorumJournalManager.Log.Warn("Remote journal " + this._enclosing + " failed to "
						 + "write txns " + firstTxnId + "-" + (firstTxnId + numTxns - 1) + ". Will try to write to this JN again after the next "
						 + "log roll.", e);
					lock (this._enclosing)
					{
						this._enclosing.outOfSync = true;
					}
					throw;
				}
				finally
				{
					long now = Runtime.NanoTime();
					long rpcTime = TimeUnit.Microseconds.Convert(now - rpcSendTimeNanos, TimeUnit.Nanoseconds
						);
					long endToEndTime = TimeUnit.Microseconds.Convert(now - submitNanos, TimeUnit.Nanoseconds
						);
					this._enclosing.metrics.AddWriteEndToEndLatency(endToEndTime);
					this._enclosing.metrics.AddWriteRpcLatency(rpcTime);
					if (rpcTime / 1000 > Org.Apache.Hadoop.Hdfs.Qjournal.Client.IPCLoggerChannel.WarnJournalMillisThreshold)
					{
						QuorumJournalManager.Log.Warn("Took " + (rpcTime / 1000) + "ms to send a batch of "
							 + numTxns + " edits (" + data.Length + " bytes) to " + "remote journal " + this
							._enclosing);
					}
				}
				lock (this._enclosing)
				{
					this._enclosing.highestAckedTxId = firstTxnId + numTxns - 1;
					this._enclosing.lastAckNanos = submitNanos;
				}
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly long segmentTxId;

			private readonly long firstTxnId;

			private readonly int numTxns;

			private readonly byte[] data;

			private readonly long submitNanos;
		}

		private sealed class _FutureCallback_428 : FutureCallback<Void>
		{
			public _FutureCallback_428(IPCLoggerChannel _enclosing, byte[] data)
			{
				this._enclosing = _enclosing;
				this.data = data;
			}

			public void OnFailure(Exception t)
			{
				this._enclosing.UnreserveQueueSpace(data.Length);
			}

			public void OnSuccess(Void t)
			{
				this._enclosing.UnreserveQueueSpace(data.Length);
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly byte[] data;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Qjournal.Protocol.JournalOutOfSyncException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		private void ThrowIfOutOfSync()
		{
			if (IsOutOfSync())
			{
				// Even if we're out of sync, it's useful to send an RPC
				// to the remote node in order to update its lag metrics, etc.
				HeartbeatIfNecessary();
				throw new JournalOutOfSyncException("Journal disabled until next roll");
			}
		}

		/// <summary>
		/// When we've entered an out-of-sync state, it's still useful to periodically
		/// send an empty RPC to the server, such that it has the up to date
		/// committedTxId.
		/// </summary>
		/// <remarks>
		/// When we've entered an out-of-sync state, it's still useful to periodically
		/// send an empty RPC to the server, such that it has the up to date
		/// committedTxId. This acts as a sanity check during recovery, and also allows
		/// that node's metrics to be up-to-date about its lag.
		/// In the future, this method may also be used in order to check that the
		/// current node is still the current writer, even if no edits are being
		/// written.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void HeartbeatIfNecessary()
		{
			if (lastHeartbeatStopwatch.Now(TimeUnit.Milliseconds) > HeartbeatIntervalMillis ||
				 !lastHeartbeatStopwatch.IsRunning())
			{
				try
				{
					GetProxy().Heartbeat(CreateReqInfo());
				}
				finally
				{
					// Don't send heartbeats more often than the configured interval,
					// even if they fail.
					lastHeartbeatStopwatch.Reset().Start();
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Qjournal.Client.LoggerTooFarBehindException
		/// 	"/>
		private void ReserveQueueSpace(int size)
		{
			lock (this)
			{
				Preconditions.CheckArgument(size >= 0);
				if (queuedEditsSizeBytes + size > queueSizeLimitBytes && queuedEditsSizeBytes > 0)
				{
					throw new LoggerTooFarBehindException();
				}
				queuedEditsSizeBytes += size;
			}
		}

		private void UnreserveQueueSpace(int size)
		{
			lock (this)
			{
				Preconditions.CheckArgument(size >= 0);
				queuedEditsSizeBytes -= size;
			}
		}

		public override ListenableFuture<Void> Format(NamespaceInfo nsInfo)
		{
			return singleThreadExecutor.Submit(new _Callable_495(this, nsInfo));
		}

		private sealed class _Callable_495 : Callable<Void>
		{
			public _Callable_495(IPCLoggerChannel _enclosing, NamespaceInfo nsInfo)
			{
				this._enclosing = _enclosing;
				this.nsInfo = nsInfo;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				this._enclosing.GetProxy().Format(this._enclosing.journalId, nsInfo);
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly NamespaceInfo nsInfo;
		}

		public override ListenableFuture<Void> StartLogSegment(long txid, int layoutVersion
			)
		{
			return singleThreadExecutor.Submit(new _Callable_507(this, txid, layoutVersion));
		}

		private sealed class _Callable_507 : Callable<Void>
		{
			public _Callable_507(IPCLoggerChannel _enclosing, long txid, int layoutVersion)
			{
				this._enclosing = _enclosing;
				this.txid = txid;
				this.layoutVersion = layoutVersion;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				this._enclosing.GetProxy().StartLogSegment(this._enclosing.CreateReqInfo(), txid, 
					layoutVersion);
				lock (this._enclosing)
				{
					if (this._enclosing.outOfSync)
					{
						this._enclosing.outOfSync = false;
						QuorumJournalManager.Log.Info("Restarting previously-stopped writes to " + this._enclosing
							 + " in segment starting at txid " + txid);
					}
				}
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly long txid;

			private readonly int layoutVersion;
		}

		public override ListenableFuture<Void> FinalizeLogSegment(long startTxId, long endTxId
			)
		{
			return singleThreadExecutor.Submit(new _Callable_528(this, startTxId, endTxId));
		}

		private sealed class _Callable_528 : Callable<Void>
		{
			public _Callable_528(IPCLoggerChannel _enclosing, long startTxId, long endTxId)
			{
				this._enclosing = _enclosing;
				this.startTxId = startTxId;
				this.endTxId = endTxId;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				this._enclosing.ThrowIfOutOfSync();
				this._enclosing.GetProxy().FinalizeLogSegment(this._enclosing.CreateReqInfo(), startTxId
					, endTxId);
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly long startTxId;

			private readonly long endTxId;
		}

		public override ListenableFuture<Void> PurgeLogsOlderThan(long minTxIdToKeep)
		{
			return singleThreadExecutor.Submit(new _Callable_541(this, minTxIdToKeep));
		}

		private sealed class _Callable_541 : Callable<Void>
		{
			public _Callable_541(IPCLoggerChannel _enclosing, long minTxIdToKeep)
			{
				this._enclosing = _enclosing;
				this.minTxIdToKeep = minTxIdToKeep;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				this._enclosing.GetProxy().PurgeLogsOlderThan(this._enclosing.CreateReqInfo(), minTxIdToKeep
					);
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly long minTxIdToKeep;
		}

		public override ListenableFuture<RemoteEditLogManifest> GetEditLogManifest(long fromTxnId
			, bool inProgressOk)
		{
			return parallelExecutor.Submit(new _Callable_553(this, fromTxnId, inProgressOk));
		}

		private sealed class _Callable_553 : Callable<RemoteEditLogManifest>
		{
			public _Callable_553(IPCLoggerChannel _enclosing, long fromTxnId, bool inProgressOk
				)
			{
				this._enclosing = _enclosing;
				this.fromTxnId = fromTxnId;
				this.inProgressOk = inProgressOk;
			}

			/// <exception cref="System.IO.IOException"/>
			public RemoteEditLogManifest Call()
			{
				QJournalProtocolProtos.GetEditLogManifestResponseProto ret = this._enclosing.GetProxy
					().GetEditLogManifest(this._enclosing.journalId, fromTxnId, inProgressOk);
				// Update the http port, since we need this to build URLs to any of the
				// returned logs.
				this._enclosing.ConstructHttpServerURI(ret);
				return PBHelper.Convert(ret.GetManifest());
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly long fromTxnId;

			private readonly bool inProgressOk;
		}

		public override ListenableFuture<QJournalProtocolProtos.PrepareRecoveryResponseProto
			> PrepareRecovery(long segmentTxId)
		{
			return singleThreadExecutor.Submit(new _Callable_569(this, segmentTxId));
		}

		private sealed class _Callable_569 : Callable<QJournalProtocolProtos.PrepareRecoveryResponseProto
			>
		{
			public _Callable_569(IPCLoggerChannel _enclosing, long segmentTxId)
			{
				this._enclosing = _enclosing;
				this.segmentTxId = segmentTxId;
			}

			/// <exception cref="System.IO.IOException"/>
			public QJournalProtocolProtos.PrepareRecoveryResponseProto Call()
			{
				if (!this._enclosing.HasHttpServerEndPoint())
				{
					// force an RPC call so we know what the HTTP port should be if it
					// haven't done so.
					QJournalProtocolProtos.GetJournalStateResponseProto ret = this._enclosing.GetProxy
						().GetJournalState(this._enclosing.journalId);
					this._enclosing.ConstructHttpServerURI(ret);
				}
				return this._enclosing.GetProxy().PrepareRecovery(this._enclosing.CreateReqInfo()
					, segmentTxId);
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly long segmentTxId;
		}

		public override ListenableFuture<Void> AcceptRecovery(QJournalProtocolProtos.SegmentStateProto
			 log, Uri url)
		{
			return singleThreadExecutor.Submit(new _Callable_587(this, log, url));
		}

		private sealed class _Callable_587 : Callable<Void>
		{
			public _Callable_587(IPCLoggerChannel _enclosing, QJournalProtocolProtos.SegmentStateProto
				 log, Uri url)
			{
				this._enclosing = _enclosing;
				this.log = log;
				this.url = url;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				this._enclosing.GetProxy().AcceptRecovery(this._enclosing.CreateReqInfo(), log, url
					);
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly QJournalProtocolProtos.SegmentStateProto log;

			private readonly Uri url;
		}

		public override ListenableFuture<Void> DiscardSegments(long startTxId)
		{
			return singleThreadExecutor.Submit(new _Callable_598(this, startTxId));
		}

		private sealed class _Callable_598 : Callable<Void>
		{
			public _Callable_598(IPCLoggerChannel _enclosing, long startTxId)
			{
				this._enclosing = _enclosing;
				this.startTxId = startTxId;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				this._enclosing.GetProxy().DiscardSegments(this._enclosing.journalId, startTxId);
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly long startTxId;
		}

		public override ListenableFuture<Void> DoPreUpgrade()
		{
			return singleThreadExecutor.Submit(new _Callable_609(this));
		}

		private sealed class _Callable_609 : Callable<Void>
		{
			public _Callable_609(IPCLoggerChannel _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				this._enclosing.GetProxy().DoPreUpgrade(this._enclosing.journalId);
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;
		}

		public override ListenableFuture<Void> DoUpgrade(StorageInfo sInfo)
		{
			return singleThreadExecutor.Submit(new _Callable_620(this, sInfo));
		}

		private sealed class _Callable_620 : Callable<Void>
		{
			public _Callable_620(IPCLoggerChannel _enclosing, StorageInfo sInfo)
			{
				this._enclosing = _enclosing;
				this.sInfo = sInfo;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				this._enclosing.GetProxy().DoUpgrade(this._enclosing.journalId, sInfo);
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly StorageInfo sInfo;
		}

		public override ListenableFuture<Void> DoFinalize()
		{
			return singleThreadExecutor.Submit(new _Callable_631(this));
		}

		private sealed class _Callable_631 : Callable<Void>
		{
			public _Callable_631(IPCLoggerChannel _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				this._enclosing.GetProxy().DoFinalize(this._enclosing.journalId);
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;
		}

		public override ListenableFuture<bool> CanRollBack(StorageInfo storage, StorageInfo
			 prevStorage, int targetLayoutVersion)
		{
			return singleThreadExecutor.Submit(new _Callable_643(this, storage, prevStorage, 
				targetLayoutVersion));
		}

		private sealed class _Callable_643 : Callable<bool>
		{
			public _Callable_643(IPCLoggerChannel _enclosing, StorageInfo storage, StorageInfo
				 prevStorage, int targetLayoutVersion)
			{
				this._enclosing = _enclosing;
				this.storage = storage;
				this.prevStorage = prevStorage;
				this.targetLayoutVersion = targetLayoutVersion;
			}

			/// <exception cref="System.IO.IOException"/>
			public bool Call()
			{
				return this._enclosing.GetProxy().CanRollBack(this._enclosing.journalId, storage, 
					prevStorage, targetLayoutVersion);
			}

			private readonly IPCLoggerChannel _enclosing;

			private readonly StorageInfo storage;

			private readonly StorageInfo prevStorage;

			private readonly int targetLayoutVersion;
		}

		public override ListenableFuture<Void> DoRollback()
		{
			return singleThreadExecutor.Submit(new _Callable_654(this));
		}

		private sealed class _Callable_654 : Callable<Void>
		{
			public _Callable_654(IPCLoggerChannel _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				this._enclosing.GetProxy().DoRollback(this._enclosing.journalId);
				return null;
			}

			private readonly IPCLoggerChannel _enclosing;
		}

		public override ListenableFuture<long> GetJournalCTime()
		{
			return singleThreadExecutor.Submit(new _Callable_665(this));
		}

		private sealed class _Callable_665 : Callable<long>
		{
			public _Callable_665(IPCLoggerChannel _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public long Call()
			{
				return this._enclosing.GetProxy().GetJournalCTime(this._enclosing.journalId);
			}

			private readonly IPCLoggerChannel _enclosing;
		}

		public override string ToString()
		{
			return InetAddresses.ToAddrString(addr.Address) + ':' + addr.Port;
		}

		public override void AppendReport(StringBuilder sb)
		{
			lock (this)
			{
				sb.Append("Written txid ").Append(highestAckedTxId);
				long behind = GetLagTxns();
				if (behind > 0)
				{
					if (lastAckNanos != 0)
					{
						long lagMillis = GetLagTimeMillis();
						sb.Append(" (" + behind + " txns/" + lagMillis + "ms behind)");
					}
					else
					{
						sb.Append(" (never written");
					}
				}
				if (outOfSync)
				{
					sb.Append(" (will try to re-sync on next segment)");
				}
			}
		}

		public virtual long GetLagTxns()
		{
			lock (this)
			{
				return Math.Max(committedTxId - highestAckedTxId, 0);
			}
		}

		public virtual long GetLagTimeMillis()
		{
			lock (this)
			{
				return TimeUnit.Milliseconds.Convert(Math.Max(lastCommitNanos - lastAckNanos, 0), 
					TimeUnit.Nanoseconds);
			}
		}

		private void ConstructHttpServerURI(QJournalProtocolProtos.GetEditLogManifestResponseProto
			 ret)
		{
			if (ret.HasFromURL())
			{
				URI uri = URI.Create(ret.GetFromURL());
				httpServerURL = GetHttpServerURI(uri.GetScheme(), uri.GetPort());
			}
			else
			{
				httpServerURL = GetHttpServerURI("http", ret.GetHttpPort());
			}
		}

		private void ConstructHttpServerURI(QJournalProtocolProtos.GetJournalStateResponseProto
			 ret)
		{
			if (ret.HasFromURL())
			{
				URI uri = URI.Create(ret.GetFromURL());
				httpServerURL = GetHttpServerURI(uri.GetScheme(), uri.GetPort());
			}
			else
			{
				httpServerURL = GetHttpServerURI("http", ret.GetHttpPort());
			}
		}

		/// <summary>Construct the http server based on the response.</summary>
		/// <remarks>
		/// Construct the http server based on the response.
		/// The fromURL field in the response specifies the endpoint of the http
		/// server. However, the address might not be accurate since the server can
		/// bind to multiple interfaces. Here the client plugs in the address specified
		/// in the configuration and generates the URI.
		/// </remarks>
		private Uri GetHttpServerURI(string scheme, int port)
		{
			try
			{
				return new Uri(scheme, addr.GetHostName(), port, string.Empty);
			}
			catch (UriFormatException e)
			{
				// Unreachable
				throw new RuntimeException(e);
			}
		}

		private bool HasHttpServerEndPoint()
		{
			return httpServerURL != null;
		}
	}
}
