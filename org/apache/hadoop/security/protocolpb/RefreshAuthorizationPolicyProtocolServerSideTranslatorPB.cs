using Sharpen;

namespace org.apache.hadoop.security.protocolPB
{
	public class RefreshAuthorizationPolicyProtocolServerSideTranslatorPB : org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB
	{
		private readonly org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol
			 impl;

		private static readonly org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclResponseProto
			 VOID_REFRESH_SERVICE_ACL_RESPONSE = ((org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclResponseProto
			)org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclResponseProto
			.newBuilder().build());

		public RefreshAuthorizationPolicyProtocolServerSideTranslatorPB(org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol
			 impl)
		{
			this.impl = impl;
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclResponseProto
			 refreshServiceAcl(com.google.protobuf.RpcController controller, org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto
			 request)
		{
			try
			{
				impl.refreshServiceAcl();
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
			return VOID_REFRESH_SERVICE_ACL_RESPONSE;
		}
	}
}
