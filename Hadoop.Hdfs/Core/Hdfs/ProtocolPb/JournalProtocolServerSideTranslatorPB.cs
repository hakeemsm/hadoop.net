using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// Implementation for protobuf service that forwards requests
	/// received on
	/// <see cref="JournalProtocolPB"/>
	/// to the
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalProtocol"/>
	/// server implementation.
	/// </summary>
	public class JournalProtocolServerSideTranslatorPB : JournalProtocolPB
	{
		/// <summary>Server side implementation to delegate the requests to</summary>
		private readonly JournalProtocol impl;

		private static readonly JournalProtocolProtos.JournalResponseProto VoidJournalResponse
			 = ((JournalProtocolProtos.JournalResponseProto)JournalProtocolProtos.JournalResponseProto
			.NewBuilder().Build());

		private static readonly JournalProtocolProtos.StartLogSegmentResponseProto VoidStartLogSegmentResponse
			 = ((JournalProtocolProtos.StartLogSegmentResponseProto)JournalProtocolProtos.StartLogSegmentResponseProto
			.NewBuilder().Build());

		public JournalProtocolServerSideTranslatorPB(JournalProtocol impl)
		{
			this.impl = impl;
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalProtocol.Journal(Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalInfo, long, long, int, byte[])
		/// 	"></seealso>
		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual JournalProtocolProtos.JournalResponseProto Journal(RpcController unused
			, JournalProtocolProtos.JournalRequestProto req)
		{
			try
			{
				impl.Journal(PBHelper.Convert(req.GetJournalInfo()), req.GetEpoch(), req.GetFirstTxnId
					(), req.GetNumTxns(), req.GetRecords().ToByteArray());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidJournalResponse;
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalProtocol.StartLogSegment(Org.Apache.Hadoop.Hdfs.Server.Protocol.JournalInfo, long, long)
		/// 	"></seealso>
		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual JournalProtocolProtos.StartLogSegmentResponseProto StartLogSegment
			(RpcController controller, JournalProtocolProtos.StartLogSegmentRequestProto req
			)
		{
			try
			{
				impl.StartLogSegment(PBHelper.Convert(req.GetJournalInfo()), req.GetEpoch(), req.
					GetTxid());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidStartLogSegmentResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual JournalProtocolProtos.FenceResponseProto Fence(RpcController controller
			, JournalProtocolProtos.FenceRequestProto req)
		{
			try
			{
				FenceResponse resp = impl.Fence(PBHelper.Convert(req.GetJournalInfo()), req.GetEpoch
					(), req.GetFencerInfo());
				return ((JournalProtocolProtos.FenceResponseProto)JournalProtocolProtos.FenceResponseProto
					.NewBuilder().SetInSync(resp.IsInSync()).SetLastTransactionId(resp.GetLastTransactionId
					()).SetPreviousEpoch(resp.GetPreviousEpoch()).Build());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
		}
	}
}
