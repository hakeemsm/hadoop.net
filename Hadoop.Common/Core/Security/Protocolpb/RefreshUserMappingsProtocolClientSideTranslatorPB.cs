using System;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Security.ProtocolPB
{
	public class RefreshUserMappingsProtocolClientSideTranslatorPB : ProtocolMetaInterface
		, RefreshUserMappingsProtocol, IDisposable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly RpcController NullController = null;

		private readonly RefreshUserMappingsProtocolPB rpcProxy;

		private static readonly RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto
			 VoidRefreshUserToGroupsMappingRequest = ((RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto
			)RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto.NewBuilder
			().Build());

		private static readonly RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto
			 VoidRefreshSuperuserGroupsConfigurationRequest = ((RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto
			)RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto
			.NewBuilder().Build());

		public RefreshUserMappingsProtocolClientSideTranslatorPB(RefreshUserMappingsProtocolPB
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
		public virtual void RefreshUserToGroupsMappings()
		{
			try
			{
				rpcProxy.RefreshUserToGroupsMappings(NullController, VoidRefreshUserToGroupsMappingRequest
					);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshSuperUserGroupsConfiguration()
		{
			try
			{
				rpcProxy.RefreshSuperUserGroupsConfiguration(NullController, VoidRefreshSuperuserGroupsConfigurationRequest
					);
			}
			catch (ServiceException se)
			{
				throw ProtobufHelper.GetRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool IsMethodSupported(string methodName)
		{
			return RpcClientUtil.IsMethodSupported(rpcProxy, typeof(RefreshUserMappingsProtocolPB
				), RPC.RpcKind.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(RefreshUserMappingsProtocolPB
				)), methodName);
		}
	}
}
