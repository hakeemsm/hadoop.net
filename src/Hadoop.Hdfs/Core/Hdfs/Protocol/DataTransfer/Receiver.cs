using System.IO;
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
	/// <summary>Receiver</summary>
	public abstract class Receiver : DataTransferProtocol
	{
		protected internal DataInputStream @in;

		/// <summary>Initialize a receiver for DataTransferProtocol with a socket.</summary>
		protected internal virtual void Initialize(DataInputStream @in)
		{
			this.@in = @in;
		}

		/// <summary>Read an Op.</summary>
		/// <remarks>Read an Op.  It also checks protocol version.</remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal OP ReadOp()
		{
			short version = @in.ReadShort();
			if (version != DataTransferProtocol.DataTransferVersion)
			{
				throw new IOException("Version Mismatch (Expected: " + DataTransferProtocol.DataTransferVersion
					 + ", Received: " + version + " )");
			}
			return OP.Read(@in);
		}

		/// <summary>Process op by the corresponding method.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal void ProcessOp(OP op)
		{
			switch (op)
			{
				case OP.ReadBlock:
				{
					OpReadBlock();
					break;
				}

				case OP.WriteBlock:
				{
					OpWriteBlock(@in);
					break;
				}

				case OP.ReplaceBlock:
				{
					OpReplaceBlock(@in);
					break;
				}

				case OP.CopyBlock:
				{
					OpCopyBlock(@in);
					break;
				}

				case OP.BlockChecksum:
				{
					OpBlockChecksum(@in);
					break;
				}

				case OP.TransferBlock:
				{
					OpTransferBlock(@in);
					break;
				}

				case OP.RequestShortCircuitFds:
				{
					OpRequestShortCircuitFds(@in);
					break;
				}

				case OP.ReleaseShortCircuitFds:
				{
					OpReleaseShortCircuitFds(@in);
					break;
				}

				case OP.RequestShortCircuitShm:
				{
					OpRequestShortCircuitShm(@in);
					break;
				}

				default:
				{
					throw new IOException("Unknown op " + op + " in data stream");
				}
			}
		}

		private static CachingStrategy GetCachingStrategy(DataTransferProtos.CachingStrategyProto
			 strategy)
		{
			bool dropBehind = strategy.HasDropBehind() ? strategy.GetDropBehind() : null;
			long readahead = strategy.HasReadahead() ? strategy.GetReadahead() : null;
			return new CachingStrategy(dropBehind, readahead);
		}

		/// <summary>Receive OP_READ_BLOCK</summary>
		/// <exception cref="System.IO.IOException"/>
		private void OpReadBlock()
		{
			DataTransferProtos.OpReadBlockProto proto = DataTransferProtos.OpReadBlockProto.ParseFrom
				(PBHelper.VintPrefixed(@in));
			TraceScope traceScope = DataTransferProtoUtil.ContinueTraceSpan(proto.GetHeader()
				, proto.GetType().Name);
			try
			{
				ReadBlock(PBHelper.Convert(proto.GetHeader().GetBaseHeader().GetBlock()), PBHelper
					.Convert(proto.GetHeader().GetBaseHeader().GetToken()), proto.GetHeader().GetClientName
					(), proto.GetOffset(), proto.GetLen(), proto.GetSendChecksums(), (proto.HasCachingStrategy
					() ? GetCachingStrategy(proto.GetCachingStrategy()) : CachingStrategy.NewDefaultStrategy
					()));
			}
			finally
			{
				if (traceScope != null)
				{
					traceScope.Close();
				}
			}
		}

		/// <summary>Receive OP_WRITE_BLOCK</summary>
		/// <exception cref="System.IO.IOException"/>
		private void OpWriteBlock(DataInputStream @in)
		{
			DataTransferProtos.OpWriteBlockProto proto = DataTransferProtos.OpWriteBlockProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			DatanodeInfo[] targets = PBHelper.Convert(proto.GetTargetsList());
			TraceScope traceScope = DataTransferProtoUtil.ContinueTraceSpan(proto.GetHeader()
				, proto.GetType().Name);
			try
			{
				WriteBlock(PBHelper.Convert(proto.GetHeader().GetBaseHeader().GetBlock()), PBHelper
					.ConvertStorageType(proto.GetStorageType()), PBHelper.Convert(proto.GetHeader().
					GetBaseHeader().GetToken()), proto.GetHeader().GetClientName(), targets, PBHelper
					.ConvertStorageTypes(proto.GetTargetStorageTypesList(), targets.Length), PBHelper
					.Convert(proto.GetSource()), DataTransferProtoUtil.FromProto(proto.GetStage()), 
					proto.GetPipelineSize(), proto.GetMinBytesRcvd(), proto.GetMaxBytesRcvd(), proto
					.GetLatestGenerationStamp(), DataTransferProtoUtil.FromProto(proto.GetRequestedChecksum
					()), (proto.HasCachingStrategy() ? GetCachingStrategy(proto.GetCachingStrategy()
					) : CachingStrategy.NewDefaultStrategy()), (proto.HasAllowLazyPersist() ? proto.
					GetAllowLazyPersist() : false), (proto.HasPinning() ? proto.GetPinning() : false
					), (PBHelper.ConvertBooleanList(proto.GetTargetPinningsList())));
			}
			finally
			{
				if (traceScope != null)
				{
					traceScope.Close();
				}
			}
		}

		/// <summary>
		/// Receive
		/// <see cref="OP.TransferBlock"/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void OpTransferBlock(DataInputStream @in)
		{
			DataTransferProtos.OpTransferBlockProto proto = DataTransferProtos.OpTransferBlockProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			DatanodeInfo[] targets = PBHelper.Convert(proto.GetTargetsList());
			TraceScope traceScope = DataTransferProtoUtil.ContinueTraceSpan(proto.GetHeader()
				, proto.GetType().Name);
			try
			{
				TransferBlock(PBHelper.Convert(proto.GetHeader().GetBaseHeader().GetBlock()), PBHelper
					.Convert(proto.GetHeader().GetBaseHeader().GetToken()), proto.GetHeader().GetClientName
					(), targets, PBHelper.ConvertStorageTypes(proto.GetTargetStorageTypesList(), targets
					.Length));
			}
			finally
			{
				if (traceScope != null)
				{
					traceScope.Close();
				}
			}
		}

		/// <summary>
		/// Receive
		/// <see cref="OP.RequestShortCircuitFds"/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void OpRequestShortCircuitFds(DataInputStream @in)
		{
			DataTransferProtos.OpRequestShortCircuitAccessProto proto = DataTransferProtos.OpRequestShortCircuitAccessProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			ShortCircuitShm.SlotId slotId = (proto.HasSlotId()) ? PBHelper.Convert(proto.GetSlotId
				()) : null;
			TraceScope traceScope = DataTransferProtoUtil.ContinueTraceSpan(proto.GetHeader()
				, proto.GetType().Name);
			try
			{
				RequestShortCircuitFds(PBHelper.Convert(proto.GetHeader().GetBlock()), PBHelper.Convert
					(proto.GetHeader().GetToken()), slotId, proto.GetMaxVersion(), proto.GetSupportsReceiptVerification
					());
			}
			finally
			{
				if (traceScope != null)
				{
					traceScope.Close();
				}
			}
		}

		/// <summary>
		/// Receive
		/// <see cref="OP.ReleaseShortCircuitFds"/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void OpReleaseShortCircuitFds(DataInputStream @in)
		{
			DataTransferProtos.ReleaseShortCircuitAccessRequestProto proto = DataTransferProtos.ReleaseShortCircuitAccessRequestProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			TraceScope traceScope = DataTransferProtoUtil.ContinueTraceSpan(proto.GetTraceInfo
				(), proto.GetType().Name);
			try
			{
				ReleaseShortCircuitFds(PBHelper.Convert(proto.GetSlotId()));
			}
			finally
			{
				if (traceScope != null)
				{
					traceScope.Close();
				}
			}
		}

		/// <summary>
		/// Receive
		/// <see cref="OP.RequestShortCircuitShm"/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void OpRequestShortCircuitShm(DataInputStream @in)
		{
			DataTransferProtos.ShortCircuitShmRequestProto proto = DataTransferProtos.ShortCircuitShmRequestProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			TraceScope traceScope = DataTransferProtoUtil.ContinueTraceSpan(proto.GetTraceInfo
				(), proto.GetType().Name);
			try
			{
				RequestShortCircuitShm(proto.GetClientName());
			}
			finally
			{
				if (traceScope != null)
				{
					traceScope.Close();
				}
			}
		}

		/// <summary>Receive OP_REPLACE_BLOCK</summary>
		/// <exception cref="System.IO.IOException"/>
		private void OpReplaceBlock(DataInputStream @in)
		{
			DataTransferProtos.OpReplaceBlockProto proto = DataTransferProtos.OpReplaceBlockProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			TraceScope traceScope = DataTransferProtoUtil.ContinueTraceSpan(proto.GetHeader()
				, proto.GetType().Name);
			try
			{
				ReplaceBlock(PBHelper.Convert(proto.GetHeader().GetBlock()), PBHelper.ConvertStorageType
					(proto.GetStorageType()), PBHelper.Convert(proto.GetHeader().GetToken()), proto.
					GetDelHint(), PBHelper.Convert(proto.GetSource()));
			}
			finally
			{
				if (traceScope != null)
				{
					traceScope.Close();
				}
			}
		}

		/// <summary>Receive OP_COPY_BLOCK</summary>
		/// <exception cref="System.IO.IOException"/>
		private void OpCopyBlock(DataInputStream @in)
		{
			DataTransferProtos.OpCopyBlockProto proto = DataTransferProtos.OpCopyBlockProto.ParseFrom
				(PBHelper.VintPrefixed(@in));
			TraceScope traceScope = DataTransferProtoUtil.ContinueTraceSpan(proto.GetHeader()
				, proto.GetType().Name);
			try
			{
				CopyBlock(PBHelper.Convert(proto.GetHeader().GetBlock()), PBHelper.Convert(proto.
					GetHeader().GetToken()));
			}
			finally
			{
				if (traceScope != null)
				{
					traceScope.Close();
				}
			}
		}

		/// <summary>Receive OP_BLOCK_CHECKSUM</summary>
		/// <exception cref="System.IO.IOException"/>
		private void OpBlockChecksum(DataInputStream @in)
		{
			DataTransferProtos.OpBlockChecksumProto proto = DataTransferProtos.OpBlockChecksumProto
				.ParseFrom(PBHelper.VintPrefixed(@in));
			TraceScope traceScope = DataTransferProtoUtil.ContinueTraceSpan(proto.GetHeader()
				, proto.GetType().Name);
			try
			{
				BlockChecksum(PBHelper.Convert(proto.GetHeader().GetBlock()), PBHelper.Convert(proto
					.GetHeader().GetToken()));
			}
			finally
			{
				if (traceScope != null)
				{
					traceScope.Close();
				}
			}
		}

		public abstract void BlockChecksum(ExtendedBlock arg1, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> arg2);

		public abstract void CopyBlock(ExtendedBlock arg1, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> arg2);

		public abstract void ReadBlock(ExtendedBlock arg1, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> arg2, string arg3, long arg4, long arg5, bool arg6, CachingStrategy
			 arg7);

		public abstract void ReleaseShortCircuitFds(ShortCircuitShm.SlotId arg1);

		public abstract void ReplaceBlock(ExtendedBlock arg1, StorageType arg2, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> arg3, string arg4, DatanodeInfo arg5);

		public abstract void RequestShortCircuitFds(ExtendedBlock arg1, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> arg2, ShortCircuitShm.SlotId arg3, int arg4, bool arg5);

		public abstract void RequestShortCircuitShm(string arg1);

		public abstract void TransferBlock(ExtendedBlock arg1, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> arg2, string arg3, DatanodeInfo[] arg4, StorageType[] arg5
			);

		public abstract void WriteBlock(ExtendedBlock arg1, StorageType arg2, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> arg3, string arg4, DatanodeInfo[] arg5, StorageType[] arg6
			, DatanodeInfo arg7, BlockConstructionStage arg8, int arg9, long arg10, long arg11
			, long arg12, DataChecksum arg13, CachingStrategy arg14, bool arg15, bool arg16, 
			bool[] arg17);
	}
}
