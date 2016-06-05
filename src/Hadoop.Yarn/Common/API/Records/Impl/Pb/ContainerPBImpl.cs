using System.Text;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ContainerPBImpl : Container
	{
		internal YarnProtos.ContainerProto proto = YarnProtos.ContainerProto.GetDefaultInstance
			();

		internal YarnProtos.ContainerProto.Builder builder = null;

		internal bool viaProto = false;

		private ContainerId containerId = null;

		private NodeId nodeId = null;

		private Resource resource = null;

		private Priority priority = null;

		private Token containerToken = null;

		public ContainerPBImpl()
		{
			builder = YarnProtos.ContainerProto.NewBuilder();
		}

		public ContainerPBImpl(YarnProtos.ContainerProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ContainerProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ContainerProto)builder.Build());
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

		private void MergeLocalToBuilder()
		{
			if (this.containerId != null && !((ContainerIdPBImpl)containerId).GetProto().Equals
				(builder.GetId()))
			{
				builder.SetId(ConvertToProtoFormat(this.containerId));
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
			if (this.containerToken != null && !((TokenPBImpl)this.containerToken).GetProto()
				.Equals(builder.GetContainerToken()))
			{
				builder.SetContainerToken(ConvertToProtoFormat(this.containerToken));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ContainerProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ContainerProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ContainerId GetId()
		{
			YarnProtos.ContainerProtoOrBuilder p = viaProto ? proto : builder;
			if (this.containerId != null)
			{
				return this.containerId;
			}
			if (!p.HasId())
			{
				return null;
			}
			this.containerId = ConvertFromProtoFormat(p.GetId());
			return this.containerId;
		}

		public override void SetNodeId(NodeId nodeId)
		{
			MaybeInitBuilder();
			if (nodeId == null)
			{
				builder.ClearNodeId();
			}
			this.nodeId = nodeId;
		}

		public override NodeId GetNodeId()
		{
			YarnProtos.ContainerProtoOrBuilder p = viaProto ? proto : builder;
			if (this.nodeId != null)
			{
				return this.nodeId;
			}
			if (!p.HasNodeId())
			{
				return null;
			}
			this.nodeId = ConvertFromProtoFormat(p.GetNodeId());
			return this.nodeId;
		}

		public override void SetId(ContainerId id)
		{
			MaybeInitBuilder();
			if (id == null)
			{
				builder.ClearId();
			}
			this.containerId = id;
		}

		public override string GetNodeHttpAddress()
		{
			YarnProtos.ContainerProtoOrBuilder p = viaProto ? proto : builder;
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

		public override Resource GetResource()
		{
			YarnProtos.ContainerProtoOrBuilder p = viaProto ? proto : builder;
			if (this.resource != null)
			{
				return this.resource;
			}
			if (!p.HasResource())
			{
				return null;
			}
			this.resource = ConvertFromProtoFormat(p.GetResource());
			return this.resource;
		}

		public override void SetResource(Resource resource)
		{
			MaybeInitBuilder();
			if (resource == null)
			{
				builder.ClearResource();
			}
			this.resource = resource;
		}

		public override Priority GetPriority()
		{
			YarnProtos.ContainerProtoOrBuilder p = viaProto ? proto : builder;
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

		public override Token GetContainerToken()
		{
			YarnProtos.ContainerProtoOrBuilder p = viaProto ? proto : builder;
			if (this.containerToken != null)
			{
				return this.containerToken;
			}
			if (!p.HasContainerToken())
			{
				return null;
			}
			this.containerToken = ConvertFromProtoFormat(p.GetContainerToken());
			return this.containerToken;
		}

		public override void SetContainerToken(Token containerToken)
		{
			MaybeInitBuilder();
			if (containerToken == null)
			{
				builder.ClearContainerToken();
			}
			this.containerToken = containerToken;
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

		private TokenPBImpl ConvertFromProtoFormat(SecurityProtos.TokenProto p)
		{
			return new TokenPBImpl(p);
		}

		private SecurityProtos.TokenProto ConvertToProtoFormat(Token t)
		{
			return ((TokenPBImpl)t).GetProto();
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("Container: [");
			sb.Append("ContainerId: ").Append(GetId()).Append(", ");
			sb.Append("NodeId: ").Append(GetNodeId()).Append(", ");
			sb.Append("NodeHttpAddress: ").Append(GetNodeHttpAddress()).Append(", ");
			sb.Append("Resource: ").Append(GetResource()).Append(", ");
			sb.Append("Priority: ").Append(GetPriority()).Append(", ");
			sb.Append("Token: ").Append(GetContainerToken()).Append(", ");
			sb.Append("]");
			return sb.ToString();
		}

		//TODO Comparator
		public override int CompareTo(Container other)
		{
			if (this.GetId().CompareTo(other.GetId()) == 0)
			{
				if (this.GetNodeId().CompareTo(other.GetNodeId()) == 0)
				{
					return this.GetResource().CompareTo(other.GetResource());
				}
				else
				{
					return this.GetNodeId().CompareTo(other.GetNodeId());
				}
			}
			else
			{
				return this.GetId().CompareTo(other.GetId());
			}
		}
	}
}
