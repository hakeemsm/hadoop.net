using Sharpen;

namespace org.apache.hadoop.ha.protocolPB
{
	public class ZKFCProtocolClientSideTranslatorPB : org.apache.hadoop.ha.ZKFCProtocol
		, java.io.Closeable, org.apache.hadoop.ipc.ProtocolTranslator
	{
		private static readonly com.google.protobuf.RpcController NULL_CONTROLLER = null;

		private readonly org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB rpcProxy;

		/// <exception cref="System.IO.IOException"/>
		public ZKFCProtocolClientSideTranslatorPB(java.net.InetSocketAddress addr, org.apache.hadoop.conf.Configuration
			 conf, javax.net.SocketFactory socketFactory, int timeout)
		{
			org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
			rpcProxy = org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB
				>(org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB))), addr, org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser(), conf, socketFactory, timeout);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		public virtual void cedeActive(int millisToCede)
		{
			try
			{
				org.apache.hadoop.ha.proto.ZKFCProtocolProtos.CedeActiveRequestProto req = ((org.apache.hadoop.ha.proto.ZKFCProtocolProtos.CedeActiveRequestProto
					)org.apache.hadoop.ha.proto.ZKFCProtocolProtos.CedeActiveRequestProto.newBuilder
					().setMillisToCede(millisToCede).build());
				rpcProxy.cedeActive(NULL_CONTROLLER, req);
			}
			catch (com.google.protobuf.ServiceException e)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
		public virtual void gracefulFailover()
		{
			try
			{
				rpcProxy.gracefulFailover(NULL_CONTROLLER, org.apache.hadoop.ha.proto.ZKFCProtocolProtos.GracefulFailoverRequestProto
					.getDefaultInstance());
			}
			catch (com.google.protobuf.ServiceException e)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(e);
			}
		}

		public virtual void close()
		{
			org.apache.hadoop.ipc.RPC.stopProxy(rpcProxy);
		}

		public virtual object getUnderlyingProxyObject()
		{
			return rpcProxy;
		}
	}
}
