using System;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.ProtocolPB
{
	/// <summary>
	/// This class is the client side translator to translate the requests made on
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalProtocol"/>
	/// interfaces to the RPC server implementing
	/// <see cref="Org.Apache.Hadoop.Hdfs.ProtocolPB.JournalProtocolPB"/>
	/// .
	/// </summary>
	public class QJournalProtocolTranslatorPB : ProtocolMetaInterface, QJournalProtocol
		, IDisposable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private readonly QJournalProtocolPB rpcProxy;

		public QJournalProtocolTranslatorPB(QJournalProtocolPB rpcProxy)
		{
			this.rpcProxy = rpcProxy;
		}

		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsFormatted(string journalId)
		{
			try
			{
				QJournalProtocolProtos.IsFormattedRequestProto req = ((QJournalProtocolProtos.IsFormattedRequestProto
					)QJournalProtocolProtos.IsFormattedRequestProto.NewBuilder().SetJid(ConvertJournalId
					(journalId)).Build());
				QJournalProtocolProtos.IsFormattedResponseProto resp = rpcProxy.IsFormatted(NullController
					, req);
				return resp.GetIsFormatted();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual QJournalProtocolProtos.GetJournalStateResponseProto GetJournalState
			(string jid)
		{
			try
			{
				QJournalProtocolProtos.GetJournalStateRequestProto req = ((QJournalProtocolProtos.GetJournalStateRequestProto
					)QJournalProtocolProtos.GetJournalStateRequestProto.NewBuilder().SetJid(ConvertJournalId
					(jid)).Build());
				return rpcProxy.GetJournalState(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		private QJournalProtocolProtos.JournalIdProto ConvertJournalId(string jid)
		{
			return ((QJournalProtocolProtos.JournalIdProto)QJournalProtocolProtos.JournalIdProto
				.NewBuilder().SetIdentifier(jid).Build());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Format(string jid, NamespaceInfo nsInfo)
		{
			try
			{
				QJournalProtocolProtos.FormatRequestProto req = ((QJournalProtocolProtos.FormatRequestProto
					)QJournalProtocolProtos.FormatRequestProto.NewBuilder().SetJid(ConvertJournalId(
					jid)).SetNsInfo(PBHelper.Convert(nsInfo)).Build());
				rpcProxy.Format(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual QJournalProtocolProtos.NewEpochResponseProto NewEpoch(string jid, 
			NamespaceInfo nsInfo, long epoch)
		{
			try
			{
				QJournalProtocolProtos.NewEpochRequestProto req = ((QJournalProtocolProtos.NewEpochRequestProto
					)QJournalProtocolProtos.NewEpochRequestProto.NewBuilder().SetJid(ConvertJournalId
					(jid)).SetNsInfo(PBHelper.Convert(nsInfo)).SetEpoch(epoch).Build());
				return rpcProxy.NewEpoch(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Journal(RequestInfo reqInfo, long segmentTxId, long firstTxnId
			, int numTxns, byte[] records)
		{
			QJournalProtocolProtos.JournalRequestProto req = ((QJournalProtocolProtos.JournalRequestProto
				)QJournalProtocolProtos.JournalRequestProto.NewBuilder().SetReqInfo(Convert(reqInfo
				)).SetSegmentTxnId(segmentTxId).SetFirstTxnId(firstTxnId).SetNumTxns(numTxns).SetRecords
				(PBHelper.GetByteString(records)).Build());
			try
			{
				rpcProxy.Journal(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Heartbeat(RequestInfo reqInfo)
		{
			try
			{
				rpcProxy.Heartbeat(NullController, ((QJournalProtocolProtos.HeartbeatRequestProto
					)QJournalProtocolProtos.HeartbeatRequestProto.NewBuilder().SetReqInfo(Convert(reqInfo
					)).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		private QJournalProtocolProtos.RequestInfoProto Convert(RequestInfo reqInfo)
		{
			QJournalProtocolProtos.RequestInfoProto.Builder builder = QJournalProtocolProtos.RequestInfoProto
				.NewBuilder().SetJournalId(ConvertJournalId(reqInfo.GetJournalId())).SetEpoch(reqInfo
				.GetEpoch()).SetIpcSerialNumber(reqInfo.GetIpcSerialNumber());
			if (reqInfo.HasCommittedTxId())
			{
				builder.SetCommittedTxId(reqInfo.GetCommittedTxId());
			}
			return ((QJournalProtocolProtos.RequestInfoProto)builder.Build());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartLogSegment(RequestInfo reqInfo, long txid, int layoutVersion
			)
		{
			QJournalProtocolProtos.StartLogSegmentRequestProto req = ((QJournalProtocolProtos.StartLogSegmentRequestProto
				)QJournalProtocolProtos.StartLogSegmentRequestProto.NewBuilder().SetReqInfo(Convert
				(reqInfo)).SetTxid(txid).SetLayoutVersion(layoutVersion).Build());
			try
			{
				rpcProxy.StartLogSegment(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void FinalizeLogSegment(RequestInfo reqInfo, long startTxId, long 
			endTxId)
		{
			QJournalProtocolProtos.FinalizeLogSegmentRequestProto req = ((QJournalProtocolProtos.FinalizeLogSegmentRequestProto
				)QJournalProtocolProtos.FinalizeLogSegmentRequestProto.NewBuilder().SetReqInfo(Convert
				(reqInfo)).SetStartTxId(startTxId).SetEndTxId(endTxId).Build());
			try
			{
				rpcProxy.FinalizeLogSegment(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void PurgeLogsOlderThan(RequestInfo reqInfo, long minTxIdToKeep)
		{
			QJournalProtocolProtos.PurgeLogsRequestProto req = ((QJournalProtocolProtos.PurgeLogsRequestProto
				)QJournalProtocolProtos.PurgeLogsRequestProto.NewBuilder().SetReqInfo(Convert(reqInfo
				)).SetMinTxIdToKeep(minTxIdToKeep).Build());
			try
			{
				rpcProxy.PurgeLogs(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual QJournalProtocolProtos.GetEditLogManifestResponseProto GetEditLogManifest
			(string jid, long sinceTxId, bool inProgressOk)
		{
			try
			{
				return rpcProxy.GetEditLogManifest(NullController, ((QJournalProtocolProtos.GetEditLogManifestRequestProto
					)QJournalProtocolProtos.GetEditLogManifestRequestProto.NewBuilder().SetJid(ConvertJournalId
					(jid)).SetSinceTxId(sinceTxId).SetInProgressOk(inProgressOk).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual QJournalProtocolProtos.PrepareRecoveryResponseProto PrepareRecovery
			(RequestInfo reqInfo, long segmentTxId)
		{
			try
			{
				return rpcProxy.PrepareRecovery(NullController, ((QJournalProtocolProtos.PrepareRecoveryRequestProto
					)QJournalProtocolProtos.PrepareRecoveryRequestProto.NewBuilder().SetReqInfo(Convert
					(reqInfo)).SetSegmentTxId(segmentTxId).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AcceptRecovery(RequestInfo reqInfo, QJournalProtocolProtos.SegmentStateProto
			 stateToAccept, Uri fromUrl)
		{
			try
			{
				rpcProxy.AcceptRecovery(NullController, ((QJournalProtocolProtos.AcceptRecoveryRequestProto
					)QJournalProtocolProtos.AcceptRecoveryRequestProto.NewBuilder().SetReqInfo(Convert
					(reqInfo)).SetStateToAccept(stateToAccept).SetFromURL(fromUrl.ToExternalForm()).
					Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(QJournalProtocolPB), RPC.RpcKind
				.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(QJournalProtocolPB)), methodName
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoPreUpgrade(string jid)
		{
			try
			{
				rpcProxy.DoPreUpgrade(NullController, ((QJournalProtocolProtos.DoPreUpgradeRequestProto
					)QJournalProtocolProtos.DoPreUpgradeRequestProto.NewBuilder().SetJid(ConvertJournalId
					(jid)).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoUpgrade(string journalId, StorageInfo sInfo)
		{
			try
			{
				rpcProxy.DoUpgrade(NullController, ((QJournalProtocolProtos.DoUpgradeRequestProto
					)QJournalProtocolProtos.DoUpgradeRequestProto.NewBuilder().SetJid(ConvertJournalId
					(journalId)).SetSInfo(PBHelper.Convert(sInfo)).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoFinalize(string jid)
		{
			try
			{
				rpcProxy.DoFinalize(NullController, ((QJournalProtocolProtos.DoFinalizeRequestProto
					)QJournalProtocolProtos.DoFinalizeRequestProto.NewBuilder().SetJid(ConvertJournalId
					(jid)).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool CanRollBack(string journalId, StorageInfo storage, StorageInfo
			 prevStorage, int targetLayoutVersion)
		{
			try
			{
				QJournalProtocolProtos.CanRollBackResponseProto response = rpcProxy.CanRollBack(NullController
					, ((QJournalProtocolProtos.CanRollBackRequestProto)QJournalProtocolProtos.CanRollBackRequestProto
					.NewBuilder().SetJid(ConvertJournalId(journalId)).SetStorage(PBHelper.Convert(storage
					)).SetPrevStorage(PBHelper.Convert(prevStorage)).SetTargetLayoutVersion(targetLayoutVersion
					).Build()));
				return response.GetCanRollBack();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DoRollback(string journalId)
		{
			try
			{
				rpcProxy.DoRollback(NullController, ((QJournalProtocolProtos.DoRollbackRequestProto
					)QJournalProtocolProtos.DoRollbackRequestProto.NewBuilder().SetJid(ConvertJournalId
					(journalId)).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetJournalCTime(string journalId)
		{
			try
			{
				QJournalProtocolProtos.GetJournalCTimeResponseProto response = rpcProxy.GetJournalCTime
					(NullController, ((QJournalProtocolProtos.GetJournalCTimeRequestProto)QJournalProtocolProtos.GetJournalCTimeRequestProto
					.NewBuilder().SetJid(ConvertJournalId(journalId)).Build()));
				return response.GetResultCTime();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DiscardSegments(string journalId, long startTxId)
		{
			try
			{
				rpcProxy.DiscardSegments(NullController, ((QJournalProtocolProtos.DiscardSegmentsRequestProto
					)QJournalProtocolProtos.DiscardSegmentsRequestProto.NewBuilder().SetJid(ConvertJournalId
					(journalId)).SetStartTxId(startTxId).Build()));
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}
	}
}
