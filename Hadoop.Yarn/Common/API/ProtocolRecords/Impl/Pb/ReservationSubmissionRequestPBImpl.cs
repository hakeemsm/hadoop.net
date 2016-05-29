using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class ReservationSubmissionRequestPBImpl : ReservationSubmissionRequest
	{
		internal YarnServiceProtos.ReservationSubmissionRequestProto proto = YarnServiceProtos.ReservationSubmissionRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.ReservationSubmissionRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private ReservationDefinition reservationDefinition;

		public ReservationSubmissionRequestPBImpl()
		{
			builder = YarnServiceProtos.ReservationSubmissionRequestProto.NewBuilder();
		}

		public ReservationSubmissionRequestPBImpl(YarnServiceProtos.ReservationSubmissionRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.ReservationSubmissionRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.ReservationSubmissionRequestProto)
				builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
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
			proto = ((YarnServiceProtos.ReservationSubmissionRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.ReservationSubmissionRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ReservationDefinition GetReservationDefinition()
		{
			YarnServiceProtos.ReservationSubmissionRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
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

		public override string GetQueue()
		{
			YarnServiceProtos.ReservationSubmissionRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasQueue())
			{
				return null;
			}
			return (p.GetQueue());
		}

		public override void SetQueue(string planName)
		{
			MaybeInitBuilder();
			if (planName == null)
			{
				builder.ClearQueue();
				return;
			}
			builder.SetQueue(planName);
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
