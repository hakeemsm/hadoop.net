using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetClusterNodeLabelsRequestPBImpl : GetClusterNodeLabelsRequest
	{
		internal YarnServiceProtos.GetClusterNodeLabelsRequestProto proto = YarnServiceProtos.GetClusterNodeLabelsRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetClusterNodeLabelsRequestProto.Builder builder = null;

		internal bool viaProto = false;

		public GetClusterNodeLabelsRequestPBImpl()
		{
			builder = YarnServiceProtos.GetClusterNodeLabelsRequestProto.NewBuilder();
		}

		public GetClusterNodeLabelsRequestPBImpl(YarnServiceProtos.GetClusterNodeLabelsRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetClusterNodeLabelsRequestProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.GetClusterNodeLabelsRequestProto)builder
				.Build());
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
