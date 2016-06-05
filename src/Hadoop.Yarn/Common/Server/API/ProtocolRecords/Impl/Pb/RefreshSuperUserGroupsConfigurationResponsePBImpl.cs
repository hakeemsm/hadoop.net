using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class RefreshSuperUserGroupsConfigurationResponsePBImpl : RefreshSuperUserGroupsConfigurationResponse
	{
		internal YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto
			 proto = YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto
			.GetDefaultInstance();

		internal YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto.Builder
			 builder = null;

		internal bool viaProto = false;

		public RefreshSuperUserGroupsConfigurationResponsePBImpl()
		{
			builder = YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto
				.NewBuilder();
		}

		public RefreshSuperUserGroupsConfigurationResponsePBImpl(YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto
			 GetProto()
		{
			proto = viaProto ? proto : ((YarnServerResourceManagerServiceProtos.RefreshSuperUserGroupsConfigurationResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override int GetHashCode()
		{
			return GetProto().GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other == null)
			{
				return false;
			}
			if (other.GetType().IsAssignableFrom(this.GetType()))
			{
				return this.GetProto().Equals(this.GetType().Cast(other).GetProto());
			}
			return false;
		}

		public override string ToString()
		{
			return TextFormat.ShortDebugString(GetProto());
		}
	}
}
