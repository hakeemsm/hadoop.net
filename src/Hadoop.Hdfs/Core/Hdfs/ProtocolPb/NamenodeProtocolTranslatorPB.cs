using System;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// This class is the client side translator to translate the requests made on
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.NamenodeProtocol"/>
	/// interfaces to the RPC server implementing
	/// <see cref="NamenodeProtocolPB"/>
	/// .
	/// </summary>
	public class NamenodeProtocolTranslatorPB : NamenodeProtocol, ProtocolMetaInterface
		, IDisposable, ProtocolTranslator
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private static readonly NamenodeProtocolProtos.GetBlockKeysRequestProto VoidGetBlockkeysRequest
			 = ((NamenodeProtocolProtos.GetBlockKeysRequestProto)NamenodeProtocolProtos.GetBlockKeysRequestProto
			.NewBuilder().Build());

		private static readonly NamenodeProtocolProtos.GetTransactionIdRequestProto VoidGetTransactionidRequest
			 = ((NamenodeProtocolProtos.GetTransactionIdRequestProto)NamenodeProtocolProtos.GetTransactionIdRequestProto
			.NewBuilder().Build());

		private static readonly NamenodeProtocolProtos.RollEditLogRequestProto VoidRollEditlogRequest
			 = ((NamenodeProtocolProtos.RollEditLogRequestProto)NamenodeProtocolProtos.RollEditLogRequestProto
			.NewBuilder().Build());

		private static readonly HdfsProtos.VersionRequestProto VoidVersionRequest = ((HdfsProtos.VersionRequestProto
			)HdfsProtos.VersionRequestProto.NewBuilder().Build());

		private readonly NamenodeProtocolPB rpcProxy;

		public NamenodeProtocolTranslatorPB(NamenodeProtocolPB rpcProxy)
		{
			/*
			* Protobuf requests with no parameters instantiated only once
			*/
			this.rpcProxy = rpcProxy;
		}

		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		public virtual object GetUnderlyingProxyObject()
		{
			return rpcProxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BlocksWithLocations GetBlocks(DatanodeInfo datanode, long size)
		{
			NamenodeProtocolProtos.GetBlocksRequestProto req = ((NamenodeProtocolProtos.GetBlocksRequestProto
				)NamenodeProtocolProtos.GetBlocksRequestProto.NewBuilder().SetDatanode(PBHelper.
				Convert((DatanodeID)datanode)).SetSize(size).Build());
			try
			{
				return PBHelper.Convert(rpcProxy.GetBlocks(NullController, req).GetBlocks());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ExportedBlockKeys GetBlockKeys()
		{
			try
			{
				NamenodeProtocolProtos.GetBlockKeysResponseProto rsp = rpcProxy.GetBlockKeys(NullController
					, VoidGetBlockkeysRequest);
				return rsp.HasKeys() ? PBHelper.Convert(rsp.GetKeys()) : null;
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetTransactionID()
		{
			try
			{
				return rpcProxy.GetTransactionId(NullController, VoidGetTransactionidRequest).GetTxId
					();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetMostRecentCheckpointTxId()
		{
			try
			{
				return rpcProxy.GetMostRecentCheckpointTxId(NullController, NamenodeProtocolProtos.GetMostRecentCheckpointTxIdRequestProto
					.GetDefaultInstance()).GetTxId();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual CheckpointSignature RollEditLog()
		{
			try
			{
				return PBHelper.Convert(rpcProxy.RollEditLog(NullController, VoidRollEditlogRequest
					).GetSignature());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual NamespaceInfo VersionRequest()
		{
			try
			{
				return PBHelper.Convert(rpcProxy.VersionRequest(NullController, VoidVersionRequest
					).GetInfo());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ErrorReport(NamenodeRegistration registration, int errorCode, 
			string msg)
		{
			NamenodeProtocolProtos.ErrorReportRequestProto req = ((NamenodeProtocolProtos.ErrorReportRequestProto
				)NamenodeProtocolProtos.ErrorReportRequestProto.NewBuilder().SetErrorCode(errorCode
				).SetMsg(msg).SetRegistration(PBHelper.Convert(registration)).Build());
			try
			{
				rpcProxy.ErrorReport(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual NamenodeRegistration RegisterSubordinateNamenode(NamenodeRegistration
			 registration)
		{
			NamenodeProtocolProtos.RegisterRequestProto req = ((NamenodeProtocolProtos.RegisterRequestProto
				)NamenodeProtocolProtos.RegisterRequestProto.NewBuilder().SetRegistration(PBHelper
				.Convert(registration)).Build());
			try
			{
				return PBHelper.Convert(rpcProxy.RegisterSubordinateNamenode(NullController, req)
					.GetRegistration());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual NamenodeCommand StartCheckpoint(NamenodeRegistration registration)
		{
			NamenodeProtocolProtos.StartCheckpointRequestProto req = ((NamenodeProtocolProtos.StartCheckpointRequestProto
				)NamenodeProtocolProtos.StartCheckpointRequestProto.NewBuilder().SetRegistration
				(PBHelper.Convert(registration)).Build());
			HdfsProtos.NamenodeCommandProto cmd;
			try
			{
				cmd = rpcProxy.StartCheckpoint(NullController, req).GetCommand();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
			return PBHelper.Convert(cmd);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndCheckpoint(NamenodeRegistration registration, CheckpointSignature
			 sig)
		{
			NamenodeProtocolProtos.EndCheckpointRequestProto req = ((NamenodeProtocolProtos.EndCheckpointRequestProto
				)NamenodeProtocolProtos.EndCheckpointRequestProto.NewBuilder().SetRegistration(PBHelper
				.Convert(registration)).SetSignature(PBHelper.Convert(sig)).Build());
			try
			{
				rpcProxy.EndCheckpoint(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteEditLogManifest GetEditLogManifest(long sinceTxId)
		{
			NamenodeProtocolProtos.GetEditLogManifestRequestProto req = ((NamenodeProtocolProtos.GetEditLogManifestRequestProto
				)NamenodeProtocolProtos.GetEditLogManifestRequestProto.NewBuilder().SetSinceTxId
				(sinceTxId).Build());
			try
			{
				return PBHelper.Convert(rpcProxy.GetEditLogManifest(NullController, req).GetManifest
					());
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(NamenodeProtocolPB), RPC.RpcKind
				.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(NamenodeProtocolPB)), methodName
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsUpgradeFinalized()
		{
			NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto req = ((NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto
				)NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto.NewBuilder().Build());
			try
			{
				NamenodeProtocolProtos.IsUpgradeFinalizedResponseProto response = rpcProxy.IsUpgradeFinalized
					(NullController, req);
				return response.GetIsUpgradeFinalized();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}
	}
}
