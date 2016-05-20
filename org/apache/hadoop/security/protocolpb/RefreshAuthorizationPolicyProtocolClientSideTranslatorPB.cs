using Sharpen;

namespace org.apache.hadoop.security.protocolPB
{
	public class RefreshAuthorizationPolicyProtocolClientSideTranslatorPB : org.apache.hadoop.ipc.ProtocolMetaInterface
		, org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol, java.io.Closeable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly com.google.protobuf.RpcController NULL_CONTROLLER = null;

		private readonly org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB
			 rpcProxy;

		private static readonly org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto
			 VOID_REFRESH_SERVICE_ACL_REQUEST = ((org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto
			)org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto
			.newBuilder().build());

		public RefreshAuthorizationPolicyProtocolClientSideTranslatorPB(org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB
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
		public virtual void refreshServiceAcl()
		{
			try
			{
				rpcProxy.refreshServiceAcl(NULL_CONTROLLER, VOID_REFRESH_SERVICE_ACL_REQUEST);
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
				(typeof(org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB
				)), org.apache.hadoop.ipc.RPC.RpcKind.RPC_PROTOCOL_BUFFER, org.apache.hadoop.ipc.RPC
				.getProtocolVersion(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB
				))), methodName);
		}
	}
}
