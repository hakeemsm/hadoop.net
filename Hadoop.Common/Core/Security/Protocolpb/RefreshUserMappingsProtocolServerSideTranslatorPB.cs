using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Proto;


namespace Org.Apache.Hadoop.Security.ProtocolPB
{
	public class RefreshUserMappingsProtocolServerSideTranslatorPB : RefreshUserMappingsProtocolPB
	{
		private readonly RefreshUserMappingsProtocol impl;

		private static readonly RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto
			 VoidRefreshUserGroupsMappingResponse = ((RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto
			)RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto.NewBuilder
			().Build());

		private static readonly RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto
			 VoidRefreshSuperuserGroupsConfigurationResponse = ((RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto
			)RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto
			.NewBuilder().Build());

		public RefreshUserMappingsProtocolServerSideTranslatorPB(RefreshUserMappingsProtocol
			 impl)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto
			 RefreshUserToGroupsMappings(RpcController controller, RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto
			 request)
		{
			try
			{
				impl.RefreshUserToGroupsMappings();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRefreshUserGroupsMappingResponse;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto
			 RefreshSuperUserGroupsConfiguration(RpcController controller, RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto
			 request)
		{
			try
			{
				impl.RefreshSuperUserGroupsConfiguration();
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			return VoidRefreshSuperuserGroupsConfigurationResponse;
		}
	}
}
