using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// Implementation for protobuf service that forwards requests
	/// received on
	/// <see cref="NamenodeProtocolPB"/>
	/// to the
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.NamenodeProtocol"/>
	/// server implementation.
	/// </summary>
	public class NamenodeProtocolServerSideTranslatorPB : NamenodeProtocolPB
	{
		private readonly NamenodeProtocol impl;

		private static readonly NamenodeProtocolProtos.ErrorReportResponseProto VoidErrorReportResponse
			 = ((NamenodeProtocolProtos.ErrorReportResponseProto)NamenodeProtocolProtos.ErrorReportResponseProto
			.NewBuilder().Build());

		private static readonly NamenodeProtocolProtos.EndCheckpointResponseProto VoidEndCheckpointResponse
			 = ((NamenodeProtocolProtos.EndCheckpointResponseProto)NamenodeProtocolProtos.EndCheckpointResponseProto
			.NewBuilder().Build());

		public NamenodeProtocolServerSideTranslatorPB(NamenodeProtocol impl)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.GetBlocksResponseProto GetBlocks(RpcController
			 unused, NamenodeProtocolProtos.GetBlocksRequestProto request)
		{
			DatanodeInfo dnInfo = new DatanodeInfo(PBHelper.Convert(request.GetDatanode()));
			BlocksWithLocations blocks;
			try
			{
				blocks = impl.GetBlocks(dnInfo, request.GetSize());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((NamenodeProtocolProtos.GetBlocksResponseProto)NamenodeProtocolProtos.GetBlocksResponseProto
				.NewBuilder().SetBlocks(PBHelper.Convert(blocks)).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.GetBlockKeysResponseProto GetBlockKeys(RpcController
			 unused, NamenodeProtocolProtos.GetBlockKeysRequestProto request)
		{
			ExportedBlockKeys keys;
			try
			{
				keys = impl.GetBlockKeys();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			NamenodeProtocolProtos.GetBlockKeysResponseProto.Builder builder = NamenodeProtocolProtos.GetBlockKeysResponseProto
				.NewBuilder();
			if (keys != null)
			{
				builder.SetKeys(PBHelper.Convert(keys));
			}
			return ((NamenodeProtocolProtos.GetBlockKeysResponseProto)builder.Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.GetTransactionIdResponseProto GetTransactionId
			(RpcController unused, NamenodeProtocolProtos.GetTransactionIdRequestProto request
			)
		{
			long txid;
			try
			{
				txid = impl.GetTransactionID();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((NamenodeProtocolProtos.GetTransactionIdResponseProto)NamenodeProtocolProtos.GetTransactionIdResponseProto
				.NewBuilder().SetTxId(txid).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.GetMostRecentCheckpointTxIdResponseProto GetMostRecentCheckpointTxId
			(RpcController unused, NamenodeProtocolProtos.GetMostRecentCheckpointTxIdRequestProto
			 request)
		{
			long txid;
			try
			{
				txid = impl.GetMostRecentCheckpointTxId();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((NamenodeProtocolProtos.GetMostRecentCheckpointTxIdResponseProto)NamenodeProtocolProtos.GetMostRecentCheckpointTxIdResponseProto
				.NewBuilder().SetTxId(txid).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.RollEditLogResponseProto RollEditLog(RpcController
			 unused, NamenodeProtocolProtos.RollEditLogRequestProto request)
		{
			CheckpointSignature signature;
			try
			{
				signature = impl.RollEditLog();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((NamenodeProtocolProtos.RollEditLogResponseProto)NamenodeProtocolProtos.RollEditLogResponseProto
				.NewBuilder().SetSignature(PBHelper.Convert(signature)).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.ErrorReportResponseProto ErrorReport(RpcController
			 unused, NamenodeProtocolProtos.ErrorReportRequestProto request)
		{
			try
			{
				impl.ErrorReport(PBHelper.Convert(request.GetRegistration()), request.GetErrorCode
					(), request.GetMsg());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidErrorReportResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.RegisterResponseProto RegisterSubordinateNamenode
			(RpcController unused, NamenodeProtocolProtos.RegisterRequestProto request)
		{
			NamenodeRegistration reg;
			try
			{
				reg = impl.RegisterSubordinateNamenode(PBHelper.Convert(request.GetRegistration()
					));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((NamenodeProtocolProtos.RegisterResponseProto)NamenodeProtocolProtos.RegisterResponseProto
				.NewBuilder().SetRegistration(PBHelper.Convert(reg)).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.StartCheckpointResponseProto StartCheckpoint
			(RpcController unused, NamenodeProtocolProtos.StartCheckpointRequestProto request
			)
		{
			NamenodeCommand cmd;
			try
			{
				cmd = impl.StartCheckpoint(PBHelper.Convert(request.GetRegistration()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((NamenodeProtocolProtos.StartCheckpointResponseProto)NamenodeProtocolProtos.StartCheckpointResponseProto
				.NewBuilder().SetCommand(PBHelper.Convert(cmd)).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.EndCheckpointResponseProto EndCheckpoint(RpcController
			 unused, NamenodeProtocolProtos.EndCheckpointRequestProto request)
		{
			try
			{
				impl.EndCheckpoint(PBHelper.Convert(request.GetRegistration()), PBHelper.Convert(
					request.GetSignature()));
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidEndCheckpointResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.GetEditLogManifestResponseProto GetEditLogManifest
			(RpcController unused, NamenodeProtocolProtos.GetEditLogManifestRequestProto request
			)
		{
			RemoteEditLogManifest manifest;
			try
			{
				manifest = impl.GetEditLogManifest(request.GetSinceTxId());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((NamenodeProtocolProtos.GetEditLogManifestResponseProto)NamenodeProtocolProtos.GetEditLogManifestResponseProto
				.NewBuilder().SetManifest(PBHelper.Convert(manifest)).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual HdfsProtos.VersionResponseProto VersionRequest(RpcController controller
			, HdfsProtos.VersionRequestProto request)
		{
			NamespaceInfo info;
			try
			{
				info = impl.VersionRequest();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((HdfsProtos.VersionResponseProto)HdfsProtos.VersionResponseProto.NewBuilder
				().SetInfo(PBHelper.Convert(info)).Build());
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto IsUpgradeFinalized
			(RpcController controller, NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto
			 request)
		{
			bool isUpgradeFinalized;
			try
			{
				isUpgradeFinalized = impl.IsUpgradeFinalized();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return ((NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto)NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto
				.NewBuilder().SetIsUpgradeFinalized(isUpgradeFinalized).Build());
		}
	}
}
