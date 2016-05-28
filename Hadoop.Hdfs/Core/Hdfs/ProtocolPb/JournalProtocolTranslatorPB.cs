using System;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// This class is the client side translator to translate the requests made on
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalProtocol"/>
	/// interfaces to the RPC server implementing
	/// <see cref="JournalProtocolPB"/>
	/// .
	/// </summary>
	public class JournalProtocolTranslatorPB : ProtocolMetaInterface, JournalProtocol
		, IDisposable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private readonly JournalProtocolPB rpcProxy;

		public JournalProtocolTranslatorPB(JournalProtocolPB rpcProxy)
		{
			this.rpcProxy = rpcProxy;
		}

		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Journal(JournalInfo journalInfo, long epoch, long firstTxnId, 
			int numTxns, byte[] records)
		{
			JournalProtocolProtos.JournalRequestProto req = ((JournalProtocolProtos.JournalRequestProto
				)JournalProtocolProtos.JournalRequestProto.NewBuilder().SetJournalInfo(PBHelper.
				Convert(journalInfo)).SetEpoch(epoch).SetFirstTxnId(firstTxnId).SetNumTxns(numTxns
				).SetRecords(PBHelper.GetByteString(records)).Build());
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
		public virtual void StartLogSegment(JournalInfo journalInfo, long epoch, long txid
			)
		{
			JournalProtocolProtos.StartLogSegmentRequestProto req = ((JournalProtocolProtos.StartLogSegmentRequestProto
				)JournalProtocolProtos.StartLogSegmentRequestProto.NewBuilder().SetJournalInfo(PBHelper
				.Convert(journalInfo)).SetEpoch(epoch).SetTxid(txid).Build());
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
		public virtual FenceResponse Fence(JournalInfo journalInfo, long epoch, string fencerInfo
			)
		{
			JournalProtocolProtos.FenceRequestProto req = ((JournalProtocolProtos.FenceRequestProto
				)JournalProtocolProtos.FenceRequestProto.NewBuilder().SetEpoch(epoch).SetJournalInfo
				(PBHelper.Convert(journalInfo)).Build());
			try
			{
				JournalProtocolProtos.FenceResponseProto resp = rpcProxy.Fence(NullController, req
					);
				return new FenceResponse(resp.GetPreviousEpoch(), resp.GetLastTransactionId(), resp
					.GetInSync());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(JournalProtocolPB), RPC.RpcKind
				.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(JournalProtocolPB)), methodName
				);
		}
	}
}
