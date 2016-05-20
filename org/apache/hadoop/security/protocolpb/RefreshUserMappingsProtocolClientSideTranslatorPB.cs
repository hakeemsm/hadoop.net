using Sharpen;

namespace org.apache.hadoop.security.protocolPB
{
	public class RefreshUserMappingsProtocolClientSideTranslatorPB : org.apache.hadoop.ipc.ProtocolMetaInterface
		, org.apache.hadoop.security.RefreshUserMappingsProtocol, java.io.Closeable
	{
		/// <summary>RpcController is not used and hence is set to null</summary>
		private static readonly com.google.protobuf.RpcController NULL_CONTROLLER = null;

		private readonly org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB
			 rpcProxy;

		private static readonly org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto
			 VOID_REFRESH_USER_TO_GROUPS_MAPPING_REQUEST = ((org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto
			)org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto
			.newBuilder().build());

		private static readonly org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto
			 VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_REQUEST = ((org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto
			)org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto
			.newBuilder().build());

		public RefreshUserMappingsProtocolClientSideTranslatorPB(org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB
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
		public virtual void refreshUserToGroupsMappings()
		{
			try
			{
				rpcProxy.refreshUserToGroupsMappings(NULL_CONTROLLER, VOID_REFRESH_USER_TO_GROUPS_MAPPING_REQUEST
					);
			}
			catch (com.google.protobuf.ServiceException se)
			{
				throw org.apache.hadoop.ipc.ProtobufHelper.getRemoteException(se);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void refreshSuperUserGroupsConfiguration()
		{
			try
			{
				rpcProxy.refreshSuperUserGroupsConfiguration(NULL_CONTROLLER, VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_REQUEST
					);
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
				(typeof(org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB)), org.apache.hadoop.ipc.RPC.RpcKind
				.RPC_PROTOCOL_BUFFER, org.apache.hadoop.ipc.RPC.getProtocolVersion(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB))), 
				methodName);
		}
	}
}
