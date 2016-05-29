using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ResourceRequestPBImpl : ResourceRequest
	{
		internal YarnProtos.ResourceRequestProto proto = YarnProtos.ResourceRequestProto.
			GetDefaultInstance();

		internal YarnProtos.ResourceRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private Priority priority = null;

		private Resource capability = null;

		public ResourceRequestPBImpl()
		{
			builder = YarnProtos.ResourceRequestProto.NewBuilder();
		}

		public ResourceRequestPBImpl(YarnProtos.ResourceRequestProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ResourceRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ResourceRequestProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.priority != null)
			{
				builder.SetPriority(ConvertToProtoFormat(this.priority));
			}
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
			proto = ((YarnProtos.ResourceRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ResourceRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override Priority GetPriority()
		{
			YarnProtos.ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.priority != null)
			{
				return this.priority;
			}
			if (!p.HasPriority())
			{
				return null;
			}
			this.priority = ConvertFromProtoFormat(p.GetPriority());
			return this.priority;
		}

		public override void SetPriority(Priority priority)
		{
			MaybeInitBuilder();
			if (priority == null)
			{
				builder.ClearPriority();
			}
			this.priority = priority;
		}

		public override string GetResourceName()
		{
			YarnProtos.ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasResourceName())
			{
				return null;
			}
			return (p.GetResourceName());
		}

		public override void SetResourceName(string resourceName)
		{
			MaybeInitBuilder();
			if (resourceName == null)
			{
				builder.ClearResourceName();
				return;
			}
			builder.SetResourceName((resourceName));
		}

		public override Resource GetCapability()
		{
			YarnProtos.ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
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
			lock (this)
			{
				YarnProtos.ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetNumContainers());
			}
		}

		public override void SetNumContainers(int numContainers)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetNumContainers((numContainers));
			}
		}

		public override bool GetRelaxLocality()
		{
			YarnProtos.ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetRelaxLocality();
		}

		public override void SetRelaxLocality(bool relaxLocality)
		{
			MaybeInitBuilder();
			builder.SetRelaxLocality(relaxLocality);
		}

		private PriorityPBImpl ConvertFromProtoFormat(YarnProtos.PriorityProto p)
		{
			return new PriorityPBImpl(p);
		}

		private YarnProtos.PriorityProto ConvertToProtoFormat(Priority t)
		{
			return ((PriorityPBImpl)t).GetProto();
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
			return "{Priority: " + GetPriority() + ", Capability: " + GetCapability() + ", # Containers: "
				 + GetNumContainers() + ", Location: " + GetResourceName() + ", Relax Locality: "
				 + GetRelaxLocality() + "}";
		}

		public override string GetNodeLabelExpression()
		{
			YarnProtos.ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasNodeLabelExpression())
			{
				return null;
			}
			return (p.GetNodeLabelExpression().Trim());
		}

		public override void SetNodeLabelExpression(string nodeLabelExpression)
		{
			MaybeInitBuilder();
			if (nodeLabelExpression == null)
			{
				builder.ClearNodeLabelExpression();
				return;
			}
			builder.SetNodeLabelExpression(nodeLabelExpression);
		}
	}
}
