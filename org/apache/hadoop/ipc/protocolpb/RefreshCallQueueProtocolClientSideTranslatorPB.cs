using Sharpen;

namespace org.apache.hadoop.ipc.protocolPB
{
	public class RefreshCallQueueProtocolClientSideTranslatorPB : org.apache.hadoop.ipc.ProtocolMetaInterface
		, org.apache.hadoop.ipc.RefreshCallQueueProtocol, java.io.Closeable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly com.google.protobuf.RpcController NULL_CONTROLLER = null;

		private readonly org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB rpcProxy;

		private static readonly org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueRequestProto
			 VOID_REFRESH_CALL_QUEUE_REQUEST = ((org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueRequestProto
			)org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueRequestProto
			.newBuilder().build());

		public RefreshCallQueueProtocolClientSideTranslatorPB(org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB
			 rpcProxy)
		{
			this.rpcProxy = rpcProxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			org.apache.hadoop.ipc.RPC.stopProxy(rpcProxy);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void refreshCallQueue()
		{
			try
			{
				rpcProxy.refreshCallQueue(NULL_CONTROLLER, VOID_REFRESH_CALL_QUEUE_REQUEST);
			}
			catch (com.google.protobuf.ServiceException se)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool isMethodSupported(string methodName)
		{
			return org.apache.hadoop.ipc.RpcClientUtil.isMethodSupported(rpcProxy, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB)), org.apache.hadoop.ipc.RPC.RpcKind
				.RPC_PROTOCOL_BUFFER, org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB))), methodName
				);
		}
	}
}
