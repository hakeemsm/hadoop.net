using System;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Security.ProtocolPB
{
	public class RefreshAuthorizationPolicyProtocolClientSideTranslatorPB : ProtocolMetaInterface
		, RefreshAuthorizationPolicyProtocol, IDisposable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private readonly RefreshAuthorizationPolicyProtocolPB rpcProxy;

		private static readonly RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto
			 VoidRefreshServiceAclRequest = ((RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto
			)RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto.NewBuilder
			().Build());

		public RefreshAuthorizationPolicyProtocolClientSideTranslatorPB(RefreshAuthorizationPolicyProtocolPB
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
		public virtual void RefreshServiceAcl()
		{
			try
			{
				rpcProxy.RefreshServiceAcl(NullController, VoidRefreshServiceAclRequest);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(RefreshAuthorizationPolicyProtocolPB
				), RPC.RpcKind.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(RefreshAuthorizationPolicyProtocolPB
				)), methodName);
		}
	}
}
