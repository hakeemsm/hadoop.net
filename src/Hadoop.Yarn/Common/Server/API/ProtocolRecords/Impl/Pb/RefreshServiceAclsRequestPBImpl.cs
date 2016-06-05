using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class RefreshServiceAclsRequestPBImpl : RefreshServiceAclsRequest
	{
		internal YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto proto
			 = YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto.GetDefaultInstance
			();

		internal YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto.Builder
			 builder = null;

		internal bool viaProto = false;

		public RefreshServiceAclsRequestPBImpl()
		{
			builder = YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto.NewBuilder
				();
		}

		public RefreshServiceAclsRequestPBImpl(YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto
			 GetProto()
		{
			proto = viaProto ? proto : ((YarnServerResourceManagerServiceProtos.RefreshServiceAclsRequestProto
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
