using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class ReservationSubmissionResponsePBImpl : ReservationSubmissionResponse
	{
		internal YarnServiceProtos.ReservationSubmissionResponseProto proto = YarnServiceProtos.ReservationSubmissionResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.ReservationSubmissionResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private ReservationId reservationId;

		public ReservationSubmissionResponsePBImpl()
		{
			builder = YarnServiceProtos.ReservationSubmissionResponseProto.NewBuilder();
		}

		public ReservationSubmissionResponsePBImpl(YarnServiceProtos.ReservationSubmissionResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.ReservationSubmissionResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.ReservationSubmissionResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.reservationId != null)
			{
				builder.SetReservationId(ConvertToProtoFormat(this.reservationId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.ReservationSubmissionResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.ReservationSubmissionResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ReservationId GetReservationId()
		{
			YarnServiceProtos.ReservationSubmissionResponseProtoOrBuilder p = viaProto ? proto
				 : builder;
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
