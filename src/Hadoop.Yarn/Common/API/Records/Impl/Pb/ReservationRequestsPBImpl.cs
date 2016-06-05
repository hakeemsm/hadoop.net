using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ReservationRequestsPBImpl : ReservationRequests
	{
		internal YarnProtos.ReservationRequestsProto proto = YarnProtos.ReservationRequestsProto
			.GetDefaultInstance();

		internal YarnProtos.ReservationRequestsProto.Builder builder = null;

		internal bool viaProto = false;

		public IList<ReservationRequest> reservationRequests;

		public ReservationRequestsPBImpl()
		{
			builder = YarnProtos.ReservationRequestsProto.NewBuilder();
		}

		public ReservationRequestsPBImpl(YarnProtos.ReservationRequestsProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ReservationRequestsProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ReservationRequestsProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.reservationRequests != null)
			{
				AddReservationResourcesToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ReservationRequestsProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ReservationRequestsProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override IList<ReservationRequest> GetReservationResources()
		{
			InitReservationRequestsList();
			return reservationRequests;
		}

		public override void SetReservationResources(IList<ReservationRequest> resources)
		{
			if (resources == null)
			{
				builder.ClearReservationResources();
				return;
			}
			this.reservationRequests = resources;
		}

		public override ReservationRequestInterpreter GetInterpreter()
		{
			YarnProtos.ReservationRequestsProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasInterpreter())
			{
				return null;
			}
			return (ConvertFromProtoFormat(p.GetInterpreter()));
		}

		public override void SetInterpreter(ReservationRequestInterpreter interpreter)
		{
			MaybeInitBuilder();
			if (interpreter == null)
			{
				builder.ClearInterpreter();
				return;
			}
			builder.SetInterpreter(ConvertToProtoFormat(interpreter));
		}

		private void InitReservationRequestsList()
		{
			if (this.reservationRequests != null)
			{
				return;
			}
			YarnProtos.ReservationRequestsProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.ReservationRequestProto> resourceProtos = p.GetReservationResourcesList
				();
			reservationRequests = new AList<ReservationRequest>();
			foreach (YarnProtos.ReservationRequestProto r in resourceProtos)
			{
				reservationRequests.AddItem(ConvertFromProtoFormat(r));
			}
		}

		private void AddReservationResourcesToProto()
		{
			MaybeInitBuilder();
			builder.ClearReservationResources();
			if (reservationRequests == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ReservationRequestProto> iterable = new _IEnumerable_133(this
				);
			builder.AddAllReservationResources(iterable);
		}

		private sealed class _IEnumerable_133 : IEnumerable<YarnProtos.ReservationRequestProto
			>
		{
			public _IEnumerable_133(ReservationRequestsPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ReservationRequestProto> GetEnumerator()
			{
				return new _IEnumerator_136(this);
			}

			private sealed class _IEnumerator_136 : IEnumerator<YarnProtos.ReservationRequestProto
				>
			{
				public _IEnumerator_136(_IEnumerable_133 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.reservationRequests.GetEnumerator();
				}

				internal IEnumerator<ReservationRequest> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ReservationRequestProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_133 _enclosing;
			}

			private readonly ReservationRequestsPBImpl _enclosing;
		}

		private YarnProtos.ReservationRequestProto ConvertToProtoFormat(ReservationRequest
			 r)
		{
			return ((ReservationRequestPBImpl)r).GetProto();
		}

		private ReservationRequestPBImpl ConvertFromProtoFormat(YarnProtos.ReservationRequestProto
			 r)
		{
			return new ReservationRequestPBImpl(r);
		}

		private YarnProtos.ReservationRequestInterpreterProto ConvertToProtoFormat(ReservationRequestInterpreter
			 r)
		{
			return ProtoUtils.ConvertToProtoFormat(r);
		}

		private ReservationRequestInterpreter ConvertFromProtoFormat(YarnProtos.ReservationRequestInterpreterProto
			 r)
		{
			return ProtoUtils.ConvertFromProtoFormat(r);
		}

		public override string ToString()
		{
			return "{Reservation Resources: " + GetReservationResources() + ", Reservation Type: "
				 + GetInterpreter() + "}";
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
