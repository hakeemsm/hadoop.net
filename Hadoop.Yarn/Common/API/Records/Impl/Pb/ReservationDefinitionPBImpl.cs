using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ReservationDefinitionPBImpl : ReservationDefinition
	{
		internal YarnProtos.ReservationDefinitionProto proto = YarnProtos.ReservationDefinitionProto
			.GetDefaultInstance();

		internal YarnProtos.ReservationDefinitionProto.Builder builder = null;

		internal bool viaProto = false;

		private ReservationRequests reservationReqs;

		public ReservationDefinitionPBImpl()
		{
			builder = YarnProtos.ReservationDefinitionProto.NewBuilder();
		}

		public ReservationDefinitionPBImpl(YarnProtos.ReservationDefinitionProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ReservationDefinitionProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ReservationDefinitionProto)builder.Build(
				));
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.reservationReqs != null)
			{
				builder.SetReservationRequests(ConvertToProtoFormat(this.reservationReqs));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ReservationDefinitionProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ReservationDefinitionProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override long GetArrival()
		{
			YarnProtos.ReservationDefinitionProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasArrival())
			{
				return 0;
			}
			return (p.GetArrival());
		}

		public override void SetArrival(long earliestStartTime)
		{
			MaybeInitBuilder();
			if (earliestStartTime <= 0)
			{
				builder.ClearArrival();
				return;
			}
			builder.SetArrival(earliestStartTime);
		}

		public override long GetDeadline()
		{
			YarnProtos.ReservationDefinitionProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasDeadline())
			{
				return 0;
			}
			return (p.GetDeadline());
		}

		public override void SetDeadline(long latestEndTime)
		{
			MaybeInitBuilder();
			if (latestEndTime <= 0)
			{
				builder.ClearDeadline();
				return;
			}
			builder.SetDeadline(latestEndTime);
		}

		public override ReservationRequests GetReservationRequests()
		{
			YarnProtos.ReservationDefinitionProtoOrBuilder p = viaProto ? proto : builder;
			if (reservationReqs != null)
			{
				return reservationReqs;
			}
			if (!p.HasReservationRequests())
			{
				return null;
			}
			reservationReqs = ConvertFromProtoFormat(p.GetReservationRequests());
			return reservationReqs;
		}

		public override void SetReservationRequests(ReservationRequests reservationRequests
			)
		{
			if (reservationRequests == null)
			{
				builder.ClearReservationRequests();
				return;
			}
			this.reservationReqs = reservationRequests;
		}

		public override string GetReservationName()
		{
			YarnProtos.ReservationDefinitionProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasReservationName())
			{
				return null;
			}
			return (p.GetReservationName());
		}

		public override void SetReservationName(string name)
		{
			MaybeInitBuilder();
			if (name == null)
			{
				builder.ClearReservationName();
				return;
			}
			builder.SetReservationName(name);
		}

		private ReservationRequestsPBImpl ConvertFromProtoFormat(YarnProtos.ReservationRequestsProto
			 p)
		{
			return new ReservationRequestsPBImpl(p);
		}

		private YarnProtos.ReservationRequestsProto ConvertToProtoFormat(ReservationRequests
			 t)
		{
			return ((ReservationRequestsPBImpl)t).GetProto();
		}

		public override string ToString()
		{
			return "{Arrival: " + GetArrival() + ", Deadline: " + GetDeadline() + ", Reservation Name: "
				 + GetReservationName() + ", Resources: " + GetReservationRequests() + "}";
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
	}
}
