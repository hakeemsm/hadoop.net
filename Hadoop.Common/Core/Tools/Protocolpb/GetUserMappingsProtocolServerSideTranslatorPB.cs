using System.IO;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Tools.Proto;


namespace Org.Apache.Hadoop.Tools.ProtocolPB
{
	public class GetUserMappingsProtocolServerSideTranslatorPB : GetUserMappingsProtocolPB
	{
		private readonly GetUserMappingsProtocol impl;

		public GetUserMappingsProtocolServerSideTranslatorPB(GetUserMappingsProtocol impl
			)
		{
			this.impl = impl;
		}

		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		public virtual GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto GetGroupsForUser
			(RpcController controller, GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto
			 request)
		{
			string[] groups;
			try
			{
				groups = impl.GetGroupsForUser(request.GetUser());
			}
			catch (IOException e)
			{
				throw new ServiceException(e);
			}
			GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto.Builder builder = GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto
				.NewBuilder();
			foreach (string g in groups)
			{
				builder.AddGroups(g);
			}
			return ((GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto)builder.Build
				());
		}
	}
}
