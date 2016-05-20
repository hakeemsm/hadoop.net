using Sharpen;

namespace org.apache.hadoop.tools.protocolPB
{
	public class GetUserMappingsProtocolServerSideTranslatorPB : org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB
	{
		private readonly org.apache.hadoop.tools.GetUserMappingsProtocol impl;

		public GetUserMappingsProtocolServerSideTranslatorPB(org.apache.hadoop.tools.GetUserMappingsProtocol
			 impl)
		{
			this.impl = impl;
		}

		/// <exception cref="com.google.protobuf.ServiceException"/>
		public virtual org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto
			 getGroupsForUser(com.google.protobuf.RpcController controller, org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto
			 request)
		{
			string[] groups;
			try
			{
				groups = impl.getGroupsForUser(request.getUser());
			}
			catch (System.IO.IOException e)
			{
				throw new com.google.protobuf.ServiceException(e);
			}
			org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto.Builder
				 builder = org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto
				.newBuilder();
			foreach (string g in groups)
			{
				builder.addGroups(g);
			}
			return ((org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto
				)builder.build());
		}
	}
}
