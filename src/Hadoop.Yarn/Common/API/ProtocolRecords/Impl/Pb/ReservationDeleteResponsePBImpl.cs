using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class ReservationDeleteResponsePBImpl : ReservationDeleteResponse
	{
		internal YarnServiceProtos.ReservationDeleteResponseProto proto = YarnServiceProtos.ReservationDeleteResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.ReservationDeleteResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public ReservationDeleteResponsePBImpl()
		{
			builder = YarnServiceProtos.ReservationDeleteResponseProto.NewBuilder();
		}

		public ReservationDeleteResponsePBImpl(YarnServiceProtos.ReservationDeleteResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.ReservationDeleteResponseProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.ReservationDeleteResponseProto)builder
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
