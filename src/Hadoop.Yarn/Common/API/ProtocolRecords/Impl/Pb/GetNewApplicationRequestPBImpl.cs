using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetNewApplicationRequestPBImpl : GetNewApplicationRequest
	{
		internal YarnServiceProtos.GetNewApplicationRequestProto proto = YarnServiceProtos.GetNewApplicationRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetNewApplicationRequestProto.Builder builder = null;

		internal bool viaProto = false;

		public GetNewApplicationRequestPBImpl()
		{
			builder = YarnServiceProtos.GetNewApplicationRequestProto.NewBuilder();
		}

		public GetNewApplicationRequestPBImpl(YarnServiceProtos.GetNewApplicationRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetNewApplicationRequestProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.GetNewApplicationRequestProto)builder
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
