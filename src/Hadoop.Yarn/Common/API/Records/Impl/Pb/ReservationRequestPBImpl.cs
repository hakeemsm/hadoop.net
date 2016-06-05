using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ReservationRequestPBImpl : ReservationRequest
	{
		internal YarnProtos.ReservationRequestProto proto = YarnProtos.ReservationRequestProto
			.GetDefaultInstance();

		internal YarnProtos.ReservationRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private Resource capability = null;

		public ReservationRequestPBImpl()
		{
			builder = YarnProtos.ReservationRequestProto.NewBuilder();
		}

		public ReservationRequestPBImpl(YarnProtos.ReservationRequestProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ReservationRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ReservationRequestProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.capability != null)
			{
				builder.SetCapability(ConvertToProtoFormat(this.capability));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ReservationRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ReservationRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override Resource GetCapability()
		{
			YarnProtos.ReservationRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.capability != null)
			{
				return this.capability;
			}
			if (!p.HasCapability())
			{
				return null;
			}
			this.capability = ConvertFromProtoFormat(p.GetCapability());
			return this.capability;
		}

		public override void SetCapability(Resource capability)
		{
			MaybeInitBuilder();
			if (capability == null)
			{
				builder.ClearCapability();
			}
			this.capability = capability;
		}

		public override int GetNumContainers()
		{
			YarnProtos.ReservationRequestProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetNumContainers());
		}

		public override void SetNumContainers(int numContainers)
		{
			MaybeInitBuilder();
			builder.SetNumContainers((numContainers));
		}

		public override int GetConcurrency()
		{
			YarnProtos.ReservationRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasConcurrency())
			{
				return 1;
			}
			return (p.GetConcurrency());
		}

		public override void SetConcurrency(int numContainers)
		{
			MaybeInitBuilder();
			builder.SetConcurrency(numContainers);
		}

		public override long GetDuration()
		{
			YarnProtos.ReservationRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasDuration())
			{
				return 0;
			}
			return (p.GetDuration());
		}

		public override void SetDuration(long duration)
		{
			MaybeInitBuilder();
			builder.SetDuration(duration);
		}

		private ResourcePBImpl ConvertFromProtoFormat(YarnProtos.ResourceProto p)
		{
			return new ResourcePBImpl(p);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource t)
		{
			return ((ResourcePBImpl)t).GetProto();
		}

		public override string ToString()
		{
			return "{Capability: " + GetCapability() + ", # Containers: " + GetNumContainers(
				) + ", Concurrency: " + GetConcurrency() + ", Lease Duration: " + GetDuration() 
				+ "}";
		}
	}
}
