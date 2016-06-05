using System;
using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.ProtocolPB
{
	/// <summary>
	/// Implementation for protobuf service that forwards requests
	/// received on
	/// <see cref="Org.Apache.Hadoop.Hdfs.ProtocolPB.JournalProtocolPB"/>
	/// to the
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalProtocol"/>
	/// server implementation.
	/// </summary>
	public class QJournalProtocolServerSideTranslatorPB : QJournalProtocolPB
	{
		/// <summary>Server side implementation to delegate the requests to</summary>
		private readonly QJournalProtocol impl;

		private static readonly QJournalProtocolProtos.JournalResponseProto VoidJournalResponse
			 = ((QJournalProtocolProtos.JournalResponseProto)QJournalProtocolProtos.JournalResponseProto
			.NewBuilder().Build());

		private static readonly QJournalProtocolProtos.StartLogSegmentResponseProto VoidStartLogSegmentResponse
			 = ((QJournalProtocolProtos.StartLogSegmentResponseProto)QJournalProtocolProtos.StartLogSegmentResponseProto
			.NewBuilder().Build());

		public QJournalProtocolServerSideTranslatorPB(QJournalProtocol impl)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.IsFormattedResponseProto IsFormatted(RpcController
			 controller, QJournalProtocolProtos.IsFormattedRequestProto request)
		{
			try
			{
				bool ret = impl.IsFormatted(Convert(request.GetJid()));
				return ((QJournalProtocolProtos.IsFormattedResponseProto)QJournalProtocolProtos.IsFormattedResponseProto
					.NewBuilder().SetIsFormatted(ret).Build());
			}
			catch (IOException ioe)
			{
				throw new ServiceException(ioe);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.GetJournalStateResponseProto GetJournalState
			(RpcController controller, QJournalProtocolProtos.GetJournalStateRequestProto request
			)
		{
			try
			{
				return impl.GetJournalState(Convert(request.GetJid()));
			}
			catch (IOException ioe)
			{
				throw new ServiceException(ioe);
			}
		}

		private string Convert(QJournalProtocolProtos.JournalIdProto jid)
		{
			return jid.GetIdentifier();
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.NewEpochResponseProto NewEpoch(RpcController
			 controller, QJournalProtocolProtos.NewEpochRequestProto request)
		{
			try
			{
				return impl.NewEpoch(request.GetJid().GetIdentifier(), PBHelper.Convert(request.GetNsInfo
					()), request.GetEpoch());
			}
			catch (IOException ioe)
			{
				throw new ServiceException(ioe);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.FormatResponseProto Format(RpcController controller
			, QJournalProtocolProtos.FormatRequestProto request)
		{
			try
			{
				impl.Format(request.GetJid().GetIdentifier(), PBHelper.Convert(request.GetNsInfo(
					)));
				return QJournalProtocolProtos.FormatResponseProto.GetDefaultInstance();
			}
			catch (IOException ioe)
			{
				throw new ServiceException(ioe);
			}
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalProtocol.Journal(Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalInfo, long, long, int, byte[])
		/// 	"></seealso>
		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.JournalResponseProto Journal(RpcController 
			unused, QJournalProtocolProtos.JournalRequestProto req)
		{
			try
			{
				impl.Journal(Convert(req.GetReqInfo()), req.GetSegmentTxnId(), req.GetFirstTxnId(
					), req.GetNumTxns(), req.GetRecords().ToByteArray());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidJournalResponse;
		}

		/// <seealso cref="JournalProtocol#heartbeat"></seealso>
		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.HeartbeatResponseProto Heartbeat(RpcController
			 controller, QJournalProtocolProtos.HeartbeatRequestProto req)
		{
			try
			{
				impl.Heartbeat(Convert(req.GetReqInfo()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return QJournalProtocolProtos.HeartbeatResponseProto.GetDefaultInstance();
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalProtocol.StartLogSegment(Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalInfo, long, long)
		/// 	"></seealso>
		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.StartLogSegmentResponseProto StartLogSegment
			(RpcController controller, QJournalProtocolProtos.StartLogSegmentRequestProto req
			)
		{
			try
			{
				int layoutVersion = req.HasLayoutVersion() ? req.GetLayoutVersion() : NameNodeLayoutVersion
					.CurrentLayoutVersion;
				impl.StartLogSegment(Convert(req.GetReqInfo()), req.GetTxid(), layoutVersion);
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidStartLogSegmentResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.FinalizeLogSegmentResponseProto FinalizeLogSegment
			(RpcController controller, QJournalProtocolProtos.FinalizeLogSegmentRequestProto
			 req)
		{
			try
			{
				impl.FinalizeLogSegment(Convert(req.GetReqInfo()), req.GetStartTxId(), req.GetEndTxId
					());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((QJournalProtocolProtos.FinalizeLogSegmentResponseProto)QJournalProtocolProtos.FinalizeLogSegmentResponseProto
				.NewBuilder().Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.PurgeLogsResponseProto PurgeLogs(RpcController
			 controller, QJournalProtocolProtos.PurgeLogsRequestProto req)
		{
			try
			{
				impl.PurgeLogsOlderThan(Convert(req.GetReqInfo()), req.GetMinTxIdToKeep());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return QJournalProtocolProtos.PurgeLogsResponseProto.GetDefaultInstance();
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.GetEditLogManifestResponseProto GetEditLogManifest
			(RpcController controller, QJournalProtocolProtos.GetEditLogManifestRequestProto
			 request)
		{
			try
			{
				return impl.GetEditLogManifest(request.GetJid().GetIdentifier(), request.GetSinceTxId
					(), request.GetInProgressOk());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.PrepareRecoveryResponseProto PrepareRecovery
			(RpcController controller, QJournalProtocolProtos.PrepareRecoveryRequestProto request
			)
		{
			try
			{
				return impl.PrepareRecovery(Convert(request.GetReqInfo()), request.GetSegmentTxId
					());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.AcceptRecoveryResponseProto AcceptRecovery(
			RpcController controller, QJournalProtocolProtos.AcceptRecoveryRequestProto request
			)
		{
			try
			{
				impl.AcceptRecovery(Convert(request.GetReqInfo()), request.GetStateToAccept(), new 
					Uri(request.GetFromURL()));
				return QJournalProtocolProtos.AcceptRecoveryResponseProto.GetDefaultInstance();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		private RequestInfo Convert(QJournalProtocolProtos.RequestInfoProto reqInfo)
		{
			return new RequestInfo(reqInfo.GetJournalId().GetIdentifier(), reqInfo.GetEpoch()
				, reqInfo.GetIpcSerialNumber(), reqInfo.HasCommittedTxId() ? reqInfo.GetCommittedTxId
				() : HdfsConstants.InvalidTxid);
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.DiscardSegmentsResponseProto DiscardSegments
			(RpcController controller, QJournalProtocolProtos.DiscardSegmentsRequestProto request
			)
		{
			try
			{
				impl.DiscardSegments(Convert(request.GetJid()), request.GetStartTxId());
				return QJournalProtocolProtos.DiscardSegmentsResponseProto.GetDefaultInstance();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.DoPreUpgradeResponseProto DoPreUpgrade(RpcController
			 controller, QJournalProtocolProtos.DoPreUpgradeRequestProto request)
		{
			try
			{
				impl.DoPreUpgrade(Convert(request.GetJid()));
				return QJournalProtocolProtos.DoPreUpgradeResponseProto.GetDefaultInstance();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.DoUpgradeResponseProto DoUpgrade(RpcController
			 controller, QJournalProtocolProtos.DoUpgradeRequestProto request)
		{
			StorageInfo si = PBHelper.Convert(request.GetSInfo(), HdfsServerConstants.NodeType
				.JournalNode);
			try
			{
				impl.DoUpgrade(Convert(request.GetJid()), si);
				return QJournalProtocolProtos.DoUpgradeResponseProto.GetDefaultInstance();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.DoFinalizeResponseProto DoFinalize(RpcController
			 controller, QJournalProtocolProtos.DoFinalizeRequestProto request)
		{
			try
			{
				impl.DoFinalize(Convert(request.GetJid()));
				return QJournalProtocolProtos.DoFinalizeResponseProto.GetDefaultInstance();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.CanRollBackResponseProto CanRollBack(RpcController
			 controller, QJournalProtocolProtos.CanRollBackRequestProto request)
		{
			try
			{
				StorageInfo si = PBHelper.Convert(request.GetStorage(), HdfsServerConstants.NodeType
					.JournalNode);
				bool result = impl.CanRollBack(Convert(request.GetJid()), si, PBHelper.Convert(request
					.GetPrevStorage(), HdfsServerConstants.NodeType.JournalNode), request.GetTargetLayoutVersion
					());
				return ((QJournalProtocolProtos.CanRollBackResponseProto)QJournalProtocolProtos.CanRollBackResponseProto
					.NewBuilder().SetCanRollBack(result).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.DoRollbackResponseProto DoRollback(RpcController
			 controller, QJournalProtocolProtos.DoRollbackRequestProto request)
		{
			try
			{
				impl.DoRollback(Convert(request.GetJid()));
				return QJournalProtocolProtos.DoRollbackResponseProto.GetDefaultInstance();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual QJournalProtocolProtos.GetJournalCTimeResponseProto GetJournalCTime
			(RpcController controller, QJournalProtocolProtos.GetJournalCTimeRequestProto request
			)
		{
			try
			{
				long resultCTime = impl.GetJournalCTime(Convert(request.GetJid()));
				return ((QJournalProtocolProtos.GetJournalCTimeResponseProto)QJournalProtocolProtos.GetJournalCTimeResponseProto
					.NewBuilder().SetResultCTime(resultCTime).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}
	}
}
