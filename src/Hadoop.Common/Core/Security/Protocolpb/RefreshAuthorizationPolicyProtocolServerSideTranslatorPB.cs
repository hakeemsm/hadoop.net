using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Proto;


namespace Org.Apache.Hadoop.Security.ProtocolPB
{
	public class RefreshAuthorizationPolicyProtocolServerSideTranslatorPB : RefreshAuthorizationPolicyProtocolPB
	{
		private readonly RefreshAuthorizationPolicyProtocol impl;

		private static readonly RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclResponseProto
			 VoidRefreshServiceAclResponse = ((RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclResponseProto
			)RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclResponseProto.NewBuilder
			().Build());

		public RefreshAuthorizationPolicyProtocolServerSideTranslatorPB(RefreshAuthorizationPolicyProtocol
			 impl)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclResponseProto
			 RefreshServiceAcl(RpcController controller, RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto
			 request)
		{
			try
			{
				impl.RefreshServiceAcl();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRefreshServiceAclResponse;
		}
	}
}
