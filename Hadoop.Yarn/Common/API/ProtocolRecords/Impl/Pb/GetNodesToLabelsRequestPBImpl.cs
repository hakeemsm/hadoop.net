using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetNodesToLabelsRequestPBImpl : GetNodesToLabelsRequest
	{
		internal YarnServiceProtos.GetNodesToLabelsRequestProto proto = YarnServiceProtos.GetNodesToLabelsRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetNodesToLabelsRequestProto.Builder builder = null;

		internal bool viaProto = false;

		public GetNodesToLabelsRequestPBImpl()
		{
			builder = YarnServiceProtos.GetNodesToLabelsRequestProto.NewBuilder();
		}

		public GetNodesToLabelsRequestPBImpl(YarnServiceProtos.GetNodesToLabelsRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetNodesToLabelsRequestProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.GetNodesToLabelsRequestProto)builder
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
