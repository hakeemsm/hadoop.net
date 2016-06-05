using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class RefreshQueuesRequestPBImpl : RefreshQueuesRequest
	{
		internal YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto proto = 
			YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto.GetDefaultInstance
			();

		internal YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto.Builder
			 builder = null;

		internal bool viaProto = false;

		public RefreshQueuesRequestPBImpl()
		{
			builder = YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto.NewBuilder
				();
		}

		public RefreshQueuesRequestPBImpl(YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto GetProto
			()
		{
			proto = viaProto ? proto : ((YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto
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
