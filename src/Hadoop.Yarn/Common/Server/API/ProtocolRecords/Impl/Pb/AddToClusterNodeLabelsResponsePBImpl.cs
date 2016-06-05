using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class AddToClusterNodeLabelsResponsePBImpl : AddToClusterNodeLabelsResponse
	{
		internal YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto
			 proto = YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto
			.GetDefaultInstance();

		internal YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto.Builder
			 builder = null;

		internal bool viaProto = false;

		public AddToClusterNodeLabelsResponsePBImpl()
		{
			builder = YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto
				.NewBuilder();
		}

		public AddToClusterNodeLabelsResponsePBImpl(YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto
			 GetProto()
		{
			proto = viaProto ? proto : ((YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsResponseProto
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
