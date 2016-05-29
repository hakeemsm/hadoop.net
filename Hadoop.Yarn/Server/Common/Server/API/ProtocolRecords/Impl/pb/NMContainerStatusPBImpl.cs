using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class NMContainerStatusPBImpl : NMContainerStatus
	{
		internal YarnServerCommonServiceProtos.NMContainerStatusProto proto = YarnServerCommonServiceProtos.NMContainerStatusProto
			.GetDefaultInstance();

		internal YarnServerCommonServiceProtos.NMContainerStatusProto.Builder builder = null;

		internal bool viaProto = false;

		private ContainerId containerId = null;

		private Resource resource = null;

		private Priority priority = null;

		public NMContainerStatusPBImpl()
		{
			builder = YarnServerCommonServiceProtos.NMContainerStatusProto.NewBuilder();
		}

		public NMContainerStatusPBImpl(YarnServerCommonServiceProtos.NMContainerStatusProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerCommonServiceProtos.NMContainerStatusProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerCommonServiceProtos.NMContainerStatusProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override int GetHashCode()
		{
			return this.GetProto().GetHashCode();
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

		public override Resource GetAllocatedResource()
		{
			if (this.resource != null)
			{
				return this.resource;
			}
			YarnServerCommonServiceProtos.NMContainerStatusProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasResource())
			{
				return null;
			}
			this.resource = ConvertFromProtoFormat(p.GetResource());
			return this.resource;
		}

		public override ContainerId GetContainerId()
		{
			if (this.containerId != null)
			{
				return this.containerId;
			}
			YarnServerCommonServiceProtos.NMContainerStatusProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasContainerId())
			{
				return null;
			}
			this.containerId = ConvertFromProtoFormat(p.GetContainerId());
			return this.containerId;
		}

		public override string GetDiagnostics()
		{
			YarnServerCommonServiceProtos.NMContainerStatusProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasDiagnostics())
			{
				return null;
			}
			return (p.GetDiagnostics());
		}

		public override ContainerState GetContainerState()
		{
			YarnServerCommonServiceProtos.NMContainerStatusProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasContainerState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetContainerState());
		}

		public override void SetAllocatedResource(Resource resource)
		{
			MaybeInitBuilder();
			if (resource == null)
			{
				builder.ClearResource();
			}
			this.resource = resource;
		}

		public override void SetContainerId(ContainerId containerId)
		{
			MaybeInitBuilder();
			if (containerId == null)
			{
				builder.ClearContainerId();
			}
			this.containerId = containerId;
		}

		public override void SetDiagnostics(string diagnosticsInfo)
		{
			MaybeInitBuilder();
			if (diagnosticsInfo == null)
			{
				builder.ClearDiagnostics();
				return;
			}
			builder.SetDiagnostics(diagnosticsInfo);
		}

		public override void SetContainerState(ContainerState containerState)
		{
			MaybeInitBuilder();
			if (containerState == null)
			{
				builder.ClearContainerState();
				return;
			}
			builder.SetContainerState(ConvertToProtoFormat(containerState));
		}

		public override int GetContainerExitStatus()
		{
			YarnServerCommonServiceProtos.NMContainerStatusProtoOrBuilder p = viaProto ? proto
				 : builder;
			return p.GetContainerExitStatus();
		}

		public override void SetContainerExitStatus(int containerExitStatus)
		{
			MaybeInitBuilder();
			builder.SetContainerExitStatus(containerExitStatus);
		}

		public override Priority GetPriority()
		{
			YarnServerCommonServiceProtos.NMContainerStatusProtoOrBuilder p = viaProto ? proto
				 : builder;
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

		public override long GetCreationTime()
		{
			YarnServerCommonServiceProtos.NMContainerStatusProtoOrBuilder p = viaProto ? proto
				 : builder;
			return p.GetCreationTime();
		}

		public override void SetCreationTime(long creationTime)
		{
			MaybeInitBuilder();
			builder.SetCreationTime(creationTime);
		}

		private void MergeLocalToBuilder()
		{
			if (this.containerId != null && !((ContainerIdPBImpl)containerId).GetProto().Equals
				(builder.GetContainerId()))
			{
				builder.SetContainerId(ConvertToProtoFormat(this.containerId));
			}
			if (this.resource != null && !((ResourcePBImpl)this.resource).GetProto().Equals(builder
				.GetResource()))
			{
				builder.SetResource(ConvertToProtoFormat(this.resource));
			}
			if (this.priority != null)
			{
				builder.SetPriority(ConvertToProtoFormat(this.priority));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerCommonServiceProtos.NMContainerStatusProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerCommonServiceProtos.NMContainerStatusProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
		}

		private ResourcePBImpl ConvertFromProtoFormat(YarnProtos.ResourceProto p)
		{
			return new ResourcePBImpl(p);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource t)
		{
			return ((ResourcePBImpl)t).GetProto();
		}

		private YarnProtos.ContainerStateProto ConvertToProtoFormat(ContainerState containerState
			)
		{
			return ProtoUtils.ConvertToProtoFormat(containerState);
		}

		private ContainerState ConvertFromProtoFormat(YarnProtos.ContainerStateProto containerState
			)
		{
			return ProtoUtils.ConvertFromProtoFormat(containerState);
		}

		private PriorityPBImpl ConvertFromProtoFormat(YarnProtos.PriorityProto p)
		{
			return new PriorityPBImpl(p);
		}

		private YarnProtos.PriorityProto ConvertToProtoFormat(Priority t)
		{
			return ((PriorityPBImpl)t).GetProto();
		}
	}
}
