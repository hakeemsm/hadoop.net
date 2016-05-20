using Sharpen;

namespace org.apache.hadoop.security.protocolPB
{
	public class RefreshUserMappingsProtocolServerSideTranslatorPB : org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB
	{
		private readonly org.apache.hadoop.security.RefreshUserMappingsProtocol impl;

		private static readonly org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto
			 VOID_REFRESH_USER_GROUPS_MAPPING_RESPONSE = ((org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto
			)org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto
			.newBuilder().build());

		private static readonly org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto
			 VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_RESPONSE = ((org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto
			)org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto
			.newBuilder().build());

		public RefreshUserMappingsProtocolServerSideTranslatorPB(org.apache.hadoop.security.RefreshUserMappingsProtocol
			 impl)
		{
			this.impl = impl;
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto
			 refreshUserToGroupsMappings(com.google.protobuf.RpcController controller, org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto
			 request)
		{
			try
			{
				impl.refreshUserToGroupsMappings();
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
			return VOID_REFRESH_USER_GROUPS_MAPPING_RESPONSE;
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto
			 refreshSuperUserGroupsConfiguration(com.google.protobuf.RpcController controller
			, org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto
			 request)
		{
			try
			{
				impl.refreshSuperUserGroupsConfiguration();
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
			return VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_RESPONSE;
		}
	}
}
