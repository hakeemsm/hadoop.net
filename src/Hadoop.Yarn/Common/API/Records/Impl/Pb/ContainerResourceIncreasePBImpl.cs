using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ContainerResourceIncreasePBImpl : ContainerResourceIncrease
	{
		internal YarnProtos.ContainerResourceIncreaseProto proto = YarnProtos.ContainerResourceIncreaseProto
			.GetDefaultInstance();

		internal YarnProtos.ContainerResourceIncreaseProto.Builder builder = null;

		internal bool viaProto = false;

		private ContainerId existingContainerId = null;

		private Resource targetCapability = null;

		private Token token = null;

		public ContainerResourceIncreasePBImpl()
		{
			builder = YarnProtos.ContainerResourceIncreaseProto.NewBuilder();
		}

		public ContainerResourceIncreasePBImpl(YarnProtos.ContainerResourceIncreaseProto 
			proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ContainerResourceIncreaseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ContainerResourceIncreaseProto)builder.Build
				());
			viaProto = true;
			return proto;
		}

		public override ContainerId GetContainerId()
		{
			YarnProtos.ContainerResourceIncreaseProtoOrBuilder p = viaProto ? proto : builder;
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
			YarnProtos.ContainerResourceIncreaseProtoOrBuilder p = viaProto ? proto : builder;
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

		public override Token GetContainerToken()
		{
			YarnProtos.ContainerResourceIncreaseProtoOrBuilder p = viaProto ? proto : builder;
			if (this.token != null)
			{
				return this.token;
			}
			if (p.HasContainerToken())
			{
				this.token = ConvertFromProtoFormat(p.GetContainerToken());
			}
			return this.token;
		}

		public override void SetContainerToken(Token token)
		{
			MaybeInitBuilder();
			if (token == null)
			{
				builder.ClearContainerToken();
			}
			this.token = token;
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

		private Token ConvertFromProtoFormat(SecurityProtos.TokenProto p)
		{
			return new TokenPBImpl(p);
		}

		private SecurityProtos.TokenProto ConvertToProtoFormat(Token t)
		{
			return ((TokenPBImpl)t).GetProto();
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ContainerResourceIncreaseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ContainerResourceIncreaseProto.NewBuilder(proto);
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
			if (this.token != null)
			{
				builder.SetContainerToken(ConvertToProtoFormat(this.token));
			}
		}
	}
}
