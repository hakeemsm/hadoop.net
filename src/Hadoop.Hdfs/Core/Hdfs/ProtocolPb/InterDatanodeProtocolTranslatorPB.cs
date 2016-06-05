using System;
using System.IO;
using System.Net;
using Com.Google.Protobuf;
using Javax.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.ProtocolPB
{
	/// <summary>
	/// This class is the client side translator to translate the requests made on
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.InterDatanodeProtocol"/>
	/// interfaces to the RPC server implementing
	/// <see cref="InterDatanodeProtocolPB"/>
	/// .
	/// </summary>
	public class InterDatanodeProtocolTranslatorPB : ProtocolMetaInterface, InterDatanodeProtocol
		, IDisposable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private readonly InterDatanodeProtocolPB rpcProxy;

		/// <exception cref="System.IO.IOException"/>
		public InterDatanodeProtocolTranslatorPB(IPEndPoint addr, UserGroupInformation ugi
			, Configuration conf, SocketFactory factory, int socketTimeout)
		{
			RPC.SetProtocolEngine(conf, typeof(InterDatanodeProtocolPB), typeof(ProtobufRpcEngine
				));
			rpcProxy = RPC.GetProxy<InterDatanodeProtocolPB>(RPC.GetProtocolVersion(typeof(InterDatanodeProtocolPB
				)), addr, ugi, conf, factory, socketTimeout);
		}

		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ReplicaRecoveryInfo InitReplicaRecovery(BlockRecoveryCommand.RecoveringBlock
			 rBlock)
		{
			InterDatanodeProtocolProtos.InitReplicaRecoveryRequestProto req = ((InterDatanodeProtocolProtos.InitReplicaRecoveryRequestProto
				)InterDatanodeProtocolProtos.InitReplicaRecoveryRequestProto.NewBuilder().SetBlock
				(PBHelper.Convert(rBlock)).Build());
			InterDatanodeProtocolProtos.InitReplicaRecoveryResponseProto resp;
			try
			{
				resp = rpcProxy.InitReplicaRecovery(NullController, req);
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
			if (!resp.GetReplicaFound())
			{
				// No replica found on the remote node.
				return null;
			}
			else
			{
				if (!resp.HasBlock() || !resp.HasState())
				{
					throw new IOException("Replica was found but missing fields. " + "Req: " + req + 
						"\n" + "Resp: " + resp);
				}
			}
			HdfsProtos.BlockProto b = resp.GetBlock();
			return new ReplicaRecoveryInfo(b.GetBlockId(), b.GetNumBytes(), b.GetGenStamp(), 
				PBHelper.Convert(resp.GetState()));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string UpdateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId
			, long newBlockId, long newLength)
		{
			InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryRequestProto req = ((InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryRequestProto
				)InterDatanodeProtocolProtos.UpdateReplicaUnderRecoveryRequestProto.NewBuilder()
				.SetBlock(PBHelper.Convert(oldBlock)).SetNewLength(newLength).SetNewBlockId(newBlockId
				).SetRecoveryId(recoveryId).Build());
			try
			{
				return rpcProxy.UpdateReplicaUnderRecovery(NullController, req).GetStorageUuid();
			}
			catch (ServiceException e)
			{
				throw ProtobufHelper.GetRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(InterDatanodeProtocolPB), 
				RPC.RpcKind.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(InterDatanodeProtocolPB
				)), methodName);
		}
	}
}
