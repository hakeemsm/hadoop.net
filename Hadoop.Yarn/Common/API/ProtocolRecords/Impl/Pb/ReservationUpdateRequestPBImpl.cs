using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class ReservationUpdateRequestPBImpl : ReservationUpdateRequest
	{
		internal YarnServiceProtos.ReservationUpdateRequestProto proto = YarnServiceProtos.ReservationUpdateRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.ReservationUpdateRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private ReservationDefinition reservationDefinition;

		private ReservationId reservationId;

		public ReservationUpdateRequestPBImpl()
		{
			builder = YarnServiceProtos.ReservationUpdateRequestProto.NewBuilder();
		}

		public ReservationUpdateRequestPBImpl(YarnServiceProtos.ReservationUpdateRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.ReservationUpdateRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.ReservationUpdateRequestProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.reservationId != null)
			{
				builder.SetReservationId(ConvertToProtoFormat(this.reservationId));
			}
			if (this.reservationDefinition != null)
			{
				builder.SetReservationDefinition(ConvertToProtoFormat(reservationDefinition));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.ReservationUpdateRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.ReservationUpdateRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ReservationDefinition GetReservationDefinition()
		{
			YarnServiceProtos.ReservationUpdateRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (reservationDefinition != null)
			{
				return reservationDefinition;
			}
			if (!p.HasReservationDefinition())
			{
				return null;
			}
			reservationDefinition = ConvertFromProtoFormat(p.GetReservationDefinition());
			return reservationDefinition;
		}

		public override void SetReservationDefinition(ReservationDefinition reservationDefinition
			)
		{
			MaybeInitBuilder();
			if (reservationDefinition == null)
			{
				builder.ClearReservationDefinition();
			}
			this.reservationDefinition = reservationDefinition;
		}

		public override ReservationId GetReservationId()
		{
			YarnServiceProtos.ReservationUpdateRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (reservationId != null)
			{
				return reservationId;
			}
			if (!p.HasReservationId())
			{
				return null;
			}
			reservationId = ConvertFromProtoFormat(p.GetReservationId());
			return reservationId;
		}

		public override void SetReservationId(ReservationId reservationId)
		{
			MaybeInitBuilder();
			if (reservationId == null)
			{
				builder.ClearReservationId();
				return;
			}
			this.reservationId = reservationId;
		}

		private ReservationIdPBImpl ConvertFromProtoFormat(YarnProtos.ReservationIdProto 
			p)
		{
			return new ReservationIdPBImpl(p);
		}

		private YarnProtos.ReservationIdProto ConvertToProtoFormat(ReservationId t)
		{
			return ((ReservationIdPBImpl)t).GetProto();
		}

		private YarnProtos.ReservationDefinitionProto ConvertToProtoFormat(ReservationDefinition
			 r)
		{
			return ((ReservationDefinitionPBImpl)r).GetProto();
		}

		private ReservationDefinitionPBImpl ConvertFromProtoFormat(YarnProtos.ReservationDefinitionProto
			 r)
		{
			return new ReservationDefinitionPBImpl(r);
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
