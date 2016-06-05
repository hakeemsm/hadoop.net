using System;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.Proto;


namespace Org.Apache.Hadoop.Ipc.ProtocolPB
{
	public class RefreshCallQueueProtocolClientSideTranslatorPB : ProtocolMetaInterface
		, RefreshCallQueueProtocol, IDisposable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private readonly RefreshCallQueueProtocolPB rpcProxy;

		private static readonly RefreshCallQueueProtocolProtos.RefreshCallQueueRequestProto
			 VoidRefreshCallQueueRequest = ((RefreshCallQueueProtocolProtos.RefreshCallQueueRequestProto
			)RefreshCallQueueProtocolProtos.RefreshCallQueueRequestProto.NewBuilder().Build(
			));

		public RefreshCallQueueProtocolClientSideTranslatorPB(RefreshCallQueueProtocolPB 
			rpcProxy)
		{
			this.rpcProxy = rpcProxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			RPC.StopProxy(rpcProxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshCallQueue()
		{
			try
			{
				rpcProxy.RefreshCallQueue(NullController, VoidRefreshCallQueueRequest);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(RefreshCallQueueProtocolPB
				), RPC.RpcKind.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(RefreshCallQueueProtocolPB
				)), methodName);
		}
	}
}
