using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>Sender</summary>
	public class Sender : DataTransferProtocol
	{
		private readonly DataOutputStream @out;

		/// <summary>Create a sender for DataTransferProtocol with a output stream.</summary>
		public Sender(DataOutputStream @out)
		{
			this.@out = @out;
		}

		/// <summary>Initialize a operation.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static void Op(DataOutput @out, OP op)
		{
			@out.WriteShort(DataTransferProtocol.DataTransferVersion);
			op.Write(@out);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Send(DataOutputStream @out, OP opcode, Message proto)
		{
			if (Log.IsTraceEnabled())
			{
				Log.Trace("Sending DataTransferOp " + proto.GetType().Name + ": " + proto);
			}
			Op(@out, opcode);
			proto.WriteDelimitedTo(@out);
			@out.Flush();
		}

		private static DataTransferProtos.CachingStrategyProto GetCachingStrategy(CachingStrategy
			 cachingStrategy)
		{
			DataTransferProtos.CachingStrategyProto.Builder builder = DataTransferProtos.CachingStrategyProto
				.NewBuilder();
			if (cachingStrategy.GetReadahead() != null)
			{
				builder.SetReadahead(cachingStrategy.GetReadahead());
			}
			if (cachingStrategy.GetDropBehind() != null)
			{
				builder.SetDropBehind(cachingStrategy.GetDropBehind());
			}
			return ((DataTransferProtos.CachingStrategyProto)builder.Build());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadBlock(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string clientName, long blockOffset, long length
			, bool sendChecksum, CachingStrategy cachingStrategy)
		{
			DataTransferProtos.OpReadBlockProto proto = ((DataTransferProtos.OpReadBlockProto
				)DataTransferProtos.OpReadBlockProto.NewBuilder().SetHeader(DataTransferProtoUtil
				.BuildClientHeader(blk, clientName, blockToken)).SetOffset(blockOffset).SetLen(length
				).SetSendChecksums(sendChecksum).SetCachingStrategy(GetCachingStrategy(cachingStrategy
				)).Build());
			Send(@out, OP.ReadBlock, proto);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void WriteBlock(ExtendedBlock blk, StorageType storageType, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string clientName, DatanodeInfo[] targets, StorageType
			[] targetStorageTypes, DatanodeInfo source, BlockConstructionStage stage, int pipelineSize
			, long minBytesRcvd, long maxBytesRcvd, long latestGenerationStamp, DataChecksum
			 requestedChecksum, CachingStrategy cachingStrategy, bool allowLazyPersist, bool
			 pinning, bool[] targetPinnings)
		{
			DataTransferProtos.ClientOperationHeaderProto header = DataTransferProtoUtil.BuildClientHeader
				(blk, clientName, blockToken);
			DataTransferProtos.ChecksumProto checksumProto = DataTransferProtoUtil.ToProto(requestedChecksum
				);
			DataTransferProtos.OpWriteBlockProto.Builder proto = DataTransferProtos.OpWriteBlockProto
				.NewBuilder().SetHeader(header).SetStorageType(PBHelper.ConvertStorageType(storageType
				)).AddAllTargets(PBHelper.Convert(targets, 1)).AddAllTargetStorageTypes(PBHelper
				.ConvertStorageTypes(targetStorageTypes, 1)).SetStage(DataTransferProtoUtil.ToProto
				(stage)).SetPipelineSize(pipelineSize).SetMinBytesRcvd(minBytesRcvd).SetMaxBytesRcvd
				(maxBytesRcvd).SetLatestGenerationStamp(latestGenerationStamp).SetRequestedChecksum
				(checksumProto).SetCachingStrategy(GetCachingStrategy(cachingStrategy)).SetAllowLazyPersist
				(allowLazyPersist).SetPinning(pinning).AddAllTargetPinnings(PBHelper.Convert(targetPinnings
				, 1));
			if (source != null)
			{
				proto.SetSource(PBHelper.ConvertDatanodeInfo(source));
			}
			Send(@out, OP.WriteBlock, ((DataTransferProtos.OpWriteBlockProto)proto.Build()));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TransferBlock(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string clientName, DatanodeInfo[] targets, StorageType
			[] targetStorageTypes)
		{
			DataTransferProtos.OpTransferBlockProto proto = ((DataTransferProtos.OpTransferBlockProto
				)DataTransferProtos.OpTransferBlockProto.NewBuilder().SetHeader(DataTransferProtoUtil
				.BuildClientHeader(blk, clientName, blockToken)).AddAllTargets(PBHelper.Convert(
				targets)).AddAllTargetStorageTypes(PBHelper.ConvertStorageTypes(targetStorageTypes
				)).Build());
			Send(@out, OP.TransferBlock, proto);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RequestShortCircuitFds(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, ShortCircuitShm.SlotId slotId, int maxVersion
			, bool supportsReceiptVerification)
		{
			DataTransferProtos.OpRequestShortCircuitAccessProto.Builder builder = DataTransferProtos.OpRequestShortCircuitAccessProto
				.NewBuilder().SetHeader(DataTransferProtoUtil.BuildBaseHeader(blk, blockToken)).
				SetMaxVersion(maxVersion);
			if (slotId != null)
			{
				builder.SetSlotId(PBHelper.Convert(slotId));
			}
			builder.SetSupportsReceiptVerification(supportsReceiptVerification);
			DataTransferProtos.OpRequestShortCircuitAccessProto proto = ((DataTransferProtos.OpRequestShortCircuitAccessProto
				)builder.Build());
			Send(@out, OP.RequestShortCircuitFds, proto);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReleaseShortCircuitFds(ShortCircuitShm.SlotId slotId)
		{
			DataTransferProtos.ReleaseShortCircuitAccessRequestProto.Builder builder = DataTransferProtos.ReleaseShortCircuitAccessRequestProto
				.NewBuilder().SetSlotId(PBHelper.Convert(slotId));
			if (Trace.IsTracing())
			{
				Span s = Trace.CurrentSpan();
				builder.SetTraceInfo(DataTransferProtos.DataTransferTraceInfoProto.NewBuilder().SetTraceId
					(s.GetTraceId()).SetParentId(s.GetSpanId()));
			}
			DataTransferProtos.ReleaseShortCircuitAccessRequestProto proto = ((DataTransferProtos.ReleaseShortCircuitAccessRequestProto
				)builder.Build());
			Send(@out, OP.ReleaseShortCircuitFds, proto);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RequestShortCircuitShm(string clientName)
		{
			DataTransferProtos.ShortCircuitShmRequestProto.Builder builder = DataTransferProtos.ShortCircuitShmRequestProto
				.NewBuilder().SetClientName(clientName);
			if (Trace.IsTracing())
			{
				Span s = Trace.CurrentSpan();
				builder.SetTraceInfo(DataTransferProtos.DataTransferTraceInfoProto.NewBuilder().SetTraceId
					(s.GetTraceId()).SetParentId(s.GetSpanId()));
			}
			DataTransferProtos.ShortCircuitShmRequestProto proto = ((DataTransferProtos.ShortCircuitShmRequestProto
				)builder.Build());
			Send(@out, OP.RequestShortCircuitShm, proto);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReplaceBlock(ExtendedBlock blk, StorageType storageType, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken, string delHint, DatanodeInfo source)
		{
			DataTransferProtos.OpReplaceBlockProto proto = ((DataTransferProtos.OpReplaceBlockProto
				)DataTransferProtos.OpReplaceBlockProto.NewBuilder().SetHeader(DataTransferProtoUtil
				.BuildBaseHeader(blk, blockToken)).SetStorageType(PBHelper.ConvertStorageType(storageType
				)).SetDelHint(delHint).SetSource(PBHelper.ConvertDatanodeInfo(source)).Build());
			Send(@out, OP.ReplaceBlock, proto);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CopyBlock(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken)
		{
			DataTransferProtos.OpCopyBlockProto proto = ((DataTransferProtos.OpCopyBlockProto
				)DataTransferProtos.OpCopyBlockProto.NewBuilder().SetHeader(DataTransferProtoUtil
				.BuildBaseHeader(blk, blockToken)).Build());
			Send(@out, OP.CopyBlock, proto);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void BlockChecksum(ExtendedBlock blk, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> blockToken)
		{
			DataTransferProtos.OpBlockChecksumProto proto = ((DataTransferProtos.OpBlockChecksumProto
				)DataTransferProtos.OpBlockChecksumProto.NewBuilder().SetHeader(DataTransferProtoUtil
				.BuildBaseHeader(blk, blockToken)).Build());
			Send(@out, OP.BlockChecksum, proto);
		}
	}
}
