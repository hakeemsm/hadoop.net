using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class SubmitApplicationResponsePBImpl : SubmitApplicationResponse
	{
		internal YarnServiceProtos.SubmitApplicationResponseProto proto = YarnServiceProtos.SubmitApplicationResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.SubmitApplicationResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public SubmitApplicationResponsePBImpl()
		{
			builder = YarnServiceProtos.SubmitApplicationResponseProto.NewBuilder();
		}

		public SubmitApplicationResponsePBImpl(YarnServiceProtos.SubmitApplicationResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.SubmitApplicationResponseProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.SubmitApplicationResponseProto)builder
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
