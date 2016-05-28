using System;
using System.Net;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Qjournal.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	internal class JournalNodeRpcServer : QJournalProtocol
	{
		private const int HandlerCount = 5;

		private readonly JournalNode jn;

		private RPC.Server server;

		/// <exception cref="System.IO.IOException"/>
		internal JournalNodeRpcServer(Configuration conf, JournalNode jn)
		{
			this.jn = jn;
			Configuration confCopy = new Configuration(conf);
			// Ensure that nagling doesn't kick in, which could cause latency issues.
			confCopy.SetBoolean(CommonConfigurationKeysPublic.IpcServerTcpnodelayKey, true);
			IPEndPoint addr = GetAddress(confCopy);
			RPC.SetProtocolEngine(confCopy, typeof(QJournalProtocolPB), typeof(ProtobufRpcEngine
				));
			QJournalProtocolServerSideTranslatorPB translator = new QJournalProtocolServerSideTranslatorPB
				(this);
			BlockingService service = QJournalProtocolProtos.QJournalProtocolService.NewReflectiveBlockingService
				(translator);
			this.server = new RPC.Builder(confCopy).SetProtocol(typeof(QJournalProtocolPB)).SetInstance
				(service).SetBindAddress(addr.GetHostName()).SetPort(addr.Port).SetNumHandlers(HandlerCount
				).SetVerbose(false).Build();
			// set service-level authorization security policy
			if (confCopy.GetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, false
				))
			{
				server.RefreshServiceAcl(confCopy, new HDFSPolicyProvider());
			}
		}

		internal virtual void Start()
		{
			this.server.Start();
		}

		public virtual IPEndPoint GetAddress()
		{
			return server.GetListenerAddress();
		}

		/// <exception cref="System.Exception"/>
		internal virtual void Join()
		{
			this.server.Join();
		}

		internal virtual void Stop()
		{
			this.server.Stop();
		}

		internal static IPEndPoint GetAddress(Configuration conf)
		{
			string addr = conf.Get(DFSConfigKeys.DfsJournalnodeRpcAddressKey, DFSConfigKeys.DfsJournalnodeRpcAddressDefault
				);
			return NetUtils.CreateSocketAddr(addr, 0, DFSConfigKeys.DfsJournalnodeRpcAddressKey
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsFormatted(string journalId)
		{
			return jn.GetOrCreateJournal(journalId).IsFormatted();
		}

		/// <exception cref="System.IO.IOException"/>
		public override QJournalProtocolProtos.GetJournalStateResponseProto GetJournalState
			(string journalId)
		{
			long epoch = jn.GetOrCreateJournal(journalId).GetLastPromisedEpoch();
			return ((QJournalProtocolProtos.GetJournalStateResponseProto)QJournalProtocolProtos.GetJournalStateResponseProto
				.NewBuilder().SetLastPromisedEpoch(epoch).SetHttpPort(jn.GetBoundHttpAddress().Port
				).SetFromURL(jn.GetHttpServerURI()).Build());
		}

		/// <exception cref="System.IO.IOException"/>
		public override QJournalProtocolProtos.NewEpochResponseProto NewEpoch(string journalId
			, NamespaceInfo nsInfo, long epoch)
		{
			return jn.GetOrCreateJournal(journalId).NewEpoch(nsInfo, epoch);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Format(string journalId, NamespaceInfo nsInfo)
		{
			jn.GetOrCreateJournal(journalId).Format(nsInfo);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Journal(RequestInfo reqInfo, long segmentTxId, long firstTxnId
			, int numTxns, byte[] records)
		{
			jn.GetOrCreateJournal(reqInfo.GetJournalId()).Journal(reqInfo, segmentTxId, firstTxnId
				, numTxns, records);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Heartbeat(RequestInfo reqInfo)
		{
			jn.GetOrCreateJournal(reqInfo.GetJournalId()).Heartbeat(reqInfo);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StartLogSegment(RequestInfo reqInfo, long txid, int layoutVersion
			)
		{
			jn.GetOrCreateJournal(reqInfo.GetJournalId()).StartLogSegment(reqInfo, txid, layoutVersion
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void FinalizeLogSegment(RequestInfo reqInfo, long startTxId, long
			 endTxId)
		{
			jn.GetOrCreateJournal(reqInfo.GetJournalId()).FinalizeLogSegment(reqInfo, startTxId
				, endTxId);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void PurgeLogsOlderThan(RequestInfo reqInfo, long minTxIdToKeep)
		{
			jn.GetOrCreateJournal(reqInfo.GetJournalId()).PurgeLogsOlderThan(reqInfo, minTxIdToKeep
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override QJournalProtocolProtos.GetEditLogManifestResponseProto GetEditLogManifest
			(string jid, long sinceTxId, bool inProgressOk)
		{
			RemoteEditLogManifest manifest = jn.GetOrCreateJournal(jid).GetEditLogManifest(sinceTxId
				, inProgressOk);
			return ((QJournalProtocolProtos.GetEditLogManifestResponseProto)QJournalProtocolProtos.GetEditLogManifestResponseProto
				.NewBuilder().SetManifest(PBHelper.Convert(manifest)).SetHttpPort(jn.GetBoundHttpAddress
				().Port).SetFromURL(jn.GetHttpServerURI()).Build());
		}

		/// <exception cref="System.IO.IOException"/>
		public override QJournalProtocolProtos.PrepareRecoveryResponseProto PrepareRecovery
			(RequestInfo reqInfo, long segmentTxId)
		{
			return jn.GetOrCreateJournal(reqInfo.GetJournalId()).PrepareRecovery(reqInfo, segmentTxId
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void AcceptRecovery(RequestInfo reqInfo, QJournalProtocolProtos.SegmentStateProto
			 log, Uri fromUrl)
		{
			jn.GetOrCreateJournal(reqInfo.GetJournalId()).AcceptRecovery(reqInfo, log, fromUrl
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoPreUpgrade(string journalId)
		{
			jn.DoPreUpgrade(journalId);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoUpgrade(string journalId, StorageInfo sInfo)
		{
			jn.DoUpgrade(journalId, sInfo);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoFinalize(string journalId)
		{
			jn.DoFinalize(journalId);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool CanRollBack(string journalId, StorageInfo storage, StorageInfo
			 prevStorage, int targetLayoutVersion)
		{
			return jn.CanRollBack(journalId, storage, prevStorage, targetLayoutVersion);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoRollback(string journalId)
		{
			jn.DoRollback(journalId);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetJournalCTime(string journalId)
		{
			return jn.GetJournalCTime(journalId);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DiscardSegments(string journalId, long startTxId)
		{
			jn.DiscardSegments(journalId, startTxId);
		}
	}
}
