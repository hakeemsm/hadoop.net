using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ContainerReportPBImpl : ContainerReport
	{
		internal YarnProtos.ContainerReportProto proto = YarnProtos.ContainerReportProto.
			GetDefaultInstance();

		internal YarnProtos.ContainerReportProto.Builder builder = null;

		internal bool viaProto = false;

		private ContainerId containerId = null;

		private Resource resource = null;

		private NodeId nodeId = null;

		private Priority priority = null;

		public ContainerReportPBImpl()
		{
			builder = YarnProtos.ContainerReportProto.NewBuilder();
		}

		public ContainerReportPBImpl(YarnProtos.ContainerReportProto proto)
		{
			this.proto = proto;
			viaProto = true;
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
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasResource())
			{
				return null;
			}
			this.resource = ConvertFromProtoFormat(p.GetResource());
			return this.resource;
		}

		public override NodeId GetAssignedNode()
		{
			if (this.nodeId != null)
			{
				return this.nodeId;
			}
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasNodeId())
			{
				return null;
			}
			this.nodeId = ConvertFromProtoFormat(p.GetNodeId());
			return this.nodeId;
		}

		public override ContainerId GetContainerId()
		{
			if (this.containerId != null)
			{
				return this.containerId;
			}
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasContainerId())
			{
				return null;
			}
			this.containerId = ConvertFromProtoFormat(p.GetContainerId());
			return this.containerId;
		}

		public override string GetDiagnosticsInfo()
		{
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasDiagnosticsInfo())
			{
				return null;
			}
			return (p.GetDiagnosticsInfo());
		}

		public override ContainerState GetContainerState()
		{
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasContainerState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetContainerState());
		}

		public override long GetFinishTime()
		{
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetFinishTime();
		}

		public override string GetLogUrl()
		{
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasLogUrl())
			{
				return null;
			}
			return (p.GetLogUrl());
		}

		public override Priority GetPriority()
		{
			if (this.priority != null)
			{
				return this.priority;
			}
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasPriority())
			{
				return null;
			}
			this.priority = ConvertFromProtoFormat(p.GetPriority());
			return this.priority;
		}

		public override long GetCreationTime()
		{
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetCreationTime();
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

		public override void SetAssignedNode(NodeId nodeId)
		{
			MaybeInitBuilder();
			if (nodeId == null)
			{
				builder.ClearNodeId();
			}
			this.nodeId = nodeId;
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

		public override void SetDiagnosticsInfo(string diagnosticsInfo)
		{
			MaybeInitBuilder();
			if (diagnosticsInfo == null)
			{
				builder.ClearDiagnosticsInfo();
				return;
			}
			builder.SetDiagnosticsInfo(diagnosticsInfo);
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
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetContainerExitStatus();
		}

		public override void SetContainerExitStatus(int containerExitStatus)
		{
			MaybeInitBuilder();
			builder.SetContainerExitStatus(containerExitStatus);
		}

		public override void SetFinishTime(long finishTime)
		{
			MaybeInitBuilder();
			builder.SetFinishTime(finishTime);
		}

		public override void SetLogUrl(string logUrl)
		{
			MaybeInitBuilder();
			if (logUrl == null)
			{
				builder.ClearLogUrl();
				return;
			}
			builder.SetLogUrl(logUrl);
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

		public override void SetCreationTime(long creationTime)
		{
			MaybeInitBuilder();
			builder.SetCreationTime(creationTime);
		}

		public virtual YarnProtos.ContainerReportProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ContainerReportProto)builder.Build());
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

		private void MergeLocalToBuilder()
		{
			if (this.containerId != null && !((ContainerIdPBImpl)containerId).GetProto().Equals
				(builder.GetContainerId()))
			{
				builder.SetContainerId(ConvertToProtoFormat(this.containerId));
			}
			if (this.nodeId != null && !((NodeIdPBImpl)nodeId).GetProto().Equals(builder.GetNodeId
				()))
			{
				builder.SetNodeId(ConvertToProtoFormat(this.nodeId));
			}
			if (this.resource != null && !((ResourcePBImpl)this.resource).GetProto().Equals(builder
				.GetResource()))
			{
				builder.SetResource(ConvertToProtoFormat(this.resource));
			}
			if (this.priority != null && !((PriorityPBImpl)this.priority).GetProto().Equals(builder
				.GetPriority()))
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
			proto = ((YarnProtos.ContainerReportProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ContainerReportProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private NodeIdPBImpl ConvertFromProtoFormat(YarnProtos.NodeIdProto p)
		{
			return new NodeIdPBImpl(p);
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
		}

		private YarnProtos.NodeIdProto ConvertToProtoFormat(NodeId t)
		{
			return ((NodeIdPBImpl)t).GetProto();
		}

		private ResourcePBImpl ConvertFromProtoFormat(YarnProtos.ResourceProto p)
		{
			return new ResourcePBImpl(p);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource t)
		{
			return ((ResourcePBImpl)t).GetProto();
		}

		private PriorityPBImpl ConvertFromProtoFormat(YarnProtos.PriorityProto p)
		{
			return new PriorityPBImpl(p);
		}

		private YarnProtos.PriorityProto ConvertToProtoFormat(Priority p)
		{
			return ((PriorityPBImpl)p).GetProto();
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

		public override string GetNodeHttpAddress()
		{
			YarnProtos.ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasNodeHttpAddress())
			{
				return null;
			}
			return (p.GetNodeHttpAddress());
		}

		public override void SetNodeHttpAddress(string nodeHttpAddress)
		{
			MaybeInitBuilder();
			if (nodeHttpAddress == null)
			{
				builder.ClearNodeHttpAddress();
				return;
			}
			builder.SetNodeHttpAddress(nodeHttpAddress);
		}
	}
}
