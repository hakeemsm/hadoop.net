using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.Impl.PB
{
	public class ContainerStartDataPBImpl : ContainerStartData
	{
		internal ApplicationHistoryServerProtos.ContainerStartDataProto proto = ApplicationHistoryServerProtos.ContainerStartDataProto
			.GetDefaultInstance();

		internal ApplicationHistoryServerProtos.ContainerStartDataProto.Builder builder = 
			null;

		internal bool viaProto = false;

		private ContainerId containerId;

		private Resource resource;

		private NodeId nodeId;

		private Priority priority;

		public ContainerStartDataPBImpl()
		{
			builder = ApplicationHistoryServerProtos.ContainerStartDataProto.NewBuilder();
		}

		public ContainerStartDataPBImpl(ApplicationHistoryServerProtos.ContainerStartDataProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override ContainerId GetContainerId()
		{
			if (this.containerId != null)
			{
				return this.containerId;
			}
			ApplicationHistoryServerProtos.ContainerStartDataProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasContainerId())
			{
				return null;
			}
			this.containerId = ConvertFromProtoFormat(p.GetContainerId());
			return this.containerId;
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

		public override Resource GetAllocatedResource()
		{
			if (this.resource != null)
			{
				return this.resource;
			}
			ApplicationHistoryServerProtos.ContainerStartDataProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasAllocatedResource())
			{
				return null;
			}
			this.resource = ConvertFromProtoFormat(p.GetAllocatedResource());
			return this.resource;
		}

		public override void SetAllocatedResource(Resource resource)
		{
			MaybeInitBuilder();
			if (resource == null)
			{
				builder.ClearAllocatedResource();
			}
			this.resource = resource;
		}

		public override NodeId GetAssignedNode()
		{
			if (this.nodeId != null)
			{
				return this.nodeId;
			}
			ApplicationHistoryServerProtos.ContainerStartDataProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasAssignedNodeId())
			{
				return null;
			}
			this.nodeId = ConvertFromProtoFormat(p.GetAssignedNodeId());
			return this.nodeId;
		}

		public override void SetAssignedNode(NodeId nodeId)
		{
			MaybeInitBuilder();
			if (nodeId == null)
			{
				builder.ClearAssignedNodeId();
			}
			this.nodeId = nodeId;
		}

		public override Priority GetPriority()
		{
			if (this.priority != null)
			{
				return this.priority;
			}
			ApplicationHistoryServerProtos.ContainerStartDataProtoOrBuilder p = viaProto ? proto
				 : builder;
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

		public override long GetStartTime()
		{
			ApplicationHistoryServerProtos.ContainerStartDataProtoOrBuilder p = viaProto ? proto
				 : builder;
			return p.GetStartTime();
		}

		public override void SetStartTime(long startTime)
		{
			MaybeInitBuilder();
			builder.SetStartTime(startTime);
		}

		public virtual ApplicationHistoryServerProtos.ContainerStartDataProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((ApplicationHistoryServerProtos.ContainerStartDataProto
				)builder.Build());
			viaProto = true;
			return proto;
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

		private void MergeLocalToBuilder()
		{
			if (this.containerId != null && !((ContainerIdPBImpl)this.containerId).GetProto()
				.Equals(builder.GetContainerId()))
			{
				builder.SetContainerId(ConvertToProtoFormat(this.containerId));
			}
			if (this.resource != null && !((ResourcePBImpl)this.resource).GetProto().Equals(builder
				.GetAllocatedResource()))
			{
				builder.SetAllocatedResource(ConvertToProtoFormat(this.resource));
			}
			if (this.nodeId != null && !((NodeIdPBImpl)this.nodeId).GetProto().Equals(builder
				.GetAssignedNodeId()))
			{
				builder.SetAssignedNodeId(ConvertToProtoFormat(this.nodeId));
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
			proto = ((ApplicationHistoryServerProtos.ContainerStartDataProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = ApplicationHistoryServerProtos.ContainerStartDataProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId containerId)
		{
			return ((ContainerIdPBImpl)containerId).GetProto();
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto containerId
			)
		{
			return new ContainerIdPBImpl(containerId);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource resource)
		{
			return ((ResourcePBImpl)resource).GetProto();
		}

		private ResourcePBImpl ConvertFromProtoFormat(YarnProtos.ResourceProto resource)
		{
			return new ResourcePBImpl(resource);
		}

		private YarnProtos.NodeIdProto ConvertToProtoFormat(NodeId nodeId)
		{
			return ((NodeIdPBImpl)nodeId).GetProto();
		}

		private NodeIdPBImpl ConvertFromProtoFormat(YarnProtos.NodeIdProto nodeId)
		{
			return new NodeIdPBImpl(nodeId);
		}

		private YarnProtos.PriorityProto ConvertToProtoFormat(Priority priority)
		{
			return ((PriorityPBImpl)priority).GetProto();
		}

		private PriorityPBImpl ConvertFromProtoFormat(YarnProtos.PriorityProto priority)
		{
			return new PriorityPBImpl(priority);
		}
	}
}
