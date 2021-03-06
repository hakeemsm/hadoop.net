using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ContainerResourceIncreaseRequestPBImpl : ContainerResourceIncreaseRequest
	{
		internal YarnProtos.ContainerResourceIncreaseRequestProto proto = YarnProtos.ContainerResourceIncreaseRequestProto
			.GetDefaultInstance();

		internal YarnProtos.ContainerResourceIncreaseRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private ContainerId existingContainerId = null;

		private Resource targetCapability = null;

		public ContainerResourceIncreaseRequestPBImpl()
		{
			builder = YarnProtos.ContainerResourceIncreaseRequestProto.NewBuilder();
		}

		public ContainerResourceIncreaseRequestPBImpl(YarnProtos.ContainerResourceIncreaseRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ContainerResourceIncreaseRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ContainerResourceIncreaseRequestProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		public override ContainerId GetContainerId()
		{
			YarnProtos.ContainerResourceIncreaseRequestProtoOrBuilder p = viaProto ? proto : 
				builder;
			if (this.existingContainerId != null)
			{
				return this.existingContainerId;
			}
			if (p.HasContainerId())
			{
				this.existingContainerId = ConvertFromProtoFormat(p.GetContainerId());
			}
			return this.existingContainerId;
		}

		public override void SetContainerId(ContainerId existingContainerId)
		{
			MaybeInitBuilder();
			if (existingContainerId == null)
			{
				builder.ClearContainerId();
			}
			this.existingContainerId = existingContainerId;
		}

		public override Resource GetCapability()
		{
			YarnProtos.ContainerResourceIncreaseRequestProtoOrBuilder p = viaProto ? proto : 
				builder;
			if (this.targetCapability != null)
			{
				return this.targetCapability;
			}
			if (p.HasCapability())
			{
				this.targetCapability = ConvertFromProtoFormat(p.GetCapability());
			}
			return this.targetCapability;
		}

		public override void SetCapability(Resource targetCapability)
		{
			MaybeInitBuilder();
			if (targetCapability == null)
			{
				builder.ClearCapability();
			}
			this.targetCapability = targetCapability;
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
		}

		private Resource ConvertFromProtoFormat(YarnProtos.ResourceProto p)
		{
			return new ResourcePBImpl(p);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource t)
		{
			return ((ResourcePBImpl)t).GetProto();
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ContainerResourceIncreaseRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ContainerResourceIncreaseRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void MergeLocalToBuilder()
		{
			if (this.existingContainerId != null)
			{
				builder.SetContainerId(ConvertToProtoFormat(this.existingContainerId));
			}
			if (this.targetCapability != null)
			{
				builder.SetCapability(ConvertToProtoFormat(this.targetCapability));
			}
		}
	}
}
