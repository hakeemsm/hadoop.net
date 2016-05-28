using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>
	/// Static utilities for dealing with the protocol buffers used by the
	/// Data Transfer Protocol.
	/// </summary>
	public abstract class DataTransferProtoUtil
	{
		internal static BlockConstructionStage FromProto(DataTransferProtos.OpWriteBlockProto.BlockConstructionStage
			 stage)
		{
			return BlockConstructionStage.ValueOf(stage.ToString());
		}

		internal static DataTransferProtos.OpWriteBlockProto.BlockConstructionStage ToProto
			(BlockConstructionStage stage)
		{
			return DataTransferProtos.OpWriteBlockProto.BlockConstructionStage.ValueOf(stage.
				ToString());
		}

		public static DataTransferProtos.ChecksumProto ToProto(DataChecksum checksum)
		{
			HdfsProtos.ChecksumTypeProto type = PBHelper.Convert(checksum.GetChecksumType());
			// ChecksumType#valueOf never returns null
			return ((DataTransferProtos.ChecksumProto)DataTransferProtos.ChecksumProto.NewBuilder
				().SetBytesPerChecksum(checksum.GetBytesPerChecksum()).SetType(type).Build());
		}

		public static DataChecksum FromProto(DataTransferProtos.ChecksumProto proto)
		{
			if (proto == null)
			{
				return null;
			}
			int bytesPerChecksum = proto.GetBytesPerChecksum();
			DataChecksum.Type type = PBHelper.Convert(proto.GetType());
			return DataChecksum.NewDataChecksum(type, bytesPerChecksum);
		}

		internal static DataTransferProtos.ClientOperationHeaderProto BuildClientHeader(ExtendedBlock
			 blk, string client, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> blockToken)
		{
			DataTransferProtos.ClientOperationHeaderProto header = ((DataTransferProtos.ClientOperationHeaderProto
				)DataTransferProtos.ClientOperationHeaderProto.NewBuilder().SetBaseHeader(BuildBaseHeader
				(blk, blockToken)).SetClientName(client).Build());
			return header;
		}

		internal static DataTransferProtos.BaseHeaderProto BuildBaseHeader(ExtendedBlock 
			blk, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> blockToken)
		{
			DataTransferProtos.BaseHeaderProto.Builder builder = DataTransferProtos.BaseHeaderProto
				.NewBuilder().SetBlock(PBHelper.Convert(blk)).SetToken(PBHelper.Convert(blockToken
				));
			if (Trace.IsTracing())
			{
				Span s = Trace.CurrentSpan();
				builder.SetTraceInfo(DataTransferProtos.DataTransferTraceInfoProto.NewBuilder().SetTraceId
					(s.GetTraceId()).SetParentId(s.GetSpanId()));
			}
			return ((DataTransferProtos.BaseHeaderProto)builder.Build());
		}

		public static TraceInfo FromProto(DataTransferProtos.DataTransferTraceInfoProto proto
			)
		{
			if (proto == null)
			{
				return null;
			}
			if (!proto.HasTraceId())
			{
				return null;
			}
			return new TraceInfo(proto.GetTraceId(), proto.GetParentId());
		}

		public static TraceScope ContinueTraceSpan(DataTransferProtos.ClientOperationHeaderProto
			 header, string description)
		{
			return ContinueTraceSpan(header.GetBaseHeader(), description);
		}

		public static TraceScope ContinueTraceSpan(DataTransferProtos.BaseHeaderProto header
			, string description)
		{
			return ContinueTraceSpan(header.GetTraceInfo(), description);
		}

		public static TraceScope ContinueTraceSpan(DataTransferProtos.DataTransferTraceInfoProto
			 proto, string description)
		{
			TraceScope scope = null;
			TraceInfo info = FromProto(proto);
			if (info != null)
			{
				scope = Trace.StartSpan(description, info);
			}
			return scope;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckBlockOpStatus(DataTransferProtos.BlockOpResponseProto response
			, string logInfo)
		{
			if (response.GetStatus() != DataTransferProtos.Status.Success)
			{
				if (response.GetStatus() == DataTransferProtos.Status.ErrorAccessToken)
				{
					throw new InvalidBlockTokenException("Got access token error" + ", status message "
						 + response.GetMessage() + ", " + logInfo);
				}
				else
				{
					throw new IOException("Got error" + ", status message " + response.GetMessage() +
						 ", " + logInfo);
				}
			}
		}
	}
}
