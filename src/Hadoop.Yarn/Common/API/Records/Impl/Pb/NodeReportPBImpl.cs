using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class NodeReportPBImpl : NodeReport
	{
		private YarnProtos.NodeReportProto proto = YarnProtos.NodeReportProto.GetDefaultInstance
			();

		private YarnProtos.NodeReportProto.Builder builder = null;

		private bool viaProto = false;

		private NodeId nodeId;

		private Resource used;

		private Resource capability;

		internal ICollection<string> labels;

		public NodeReportPBImpl()
		{
			builder = YarnProtos.NodeReportProto.NewBuilder();
		}

		public NodeReportPBImpl(YarnProtos.NodeReportProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override Resource GetCapability()
		{
			if (this.capability != null)
			{
				return this.capability;
			}
			YarnProtos.NodeReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasCapability())
			{
				return null;
			}
			this.capability = ConvertFromProtoFormat(p.GetCapability());
			return this.capability;
		}

		public override string GetHealthReport()
		{
			YarnProtos.NodeReportProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetHealthReport();
		}

		public override void SetHealthReport(string healthReport)
		{
			MaybeInitBuilder();
			if (healthReport == null)
			{
				builder.ClearHealthReport();
				return;
			}
			builder.SetHealthReport(healthReport);
		}

		public override long GetLastHealthReportTime()
		{
			YarnProtos.NodeReportProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetLastHealthReportTime();
		}

		public override void SetLastHealthReportTime(long lastHealthReportTime)
		{
			MaybeInitBuilder();
			builder.SetLastHealthReportTime(lastHealthReportTime);
		}

		public override string GetHttpAddress()
		{
			YarnProtos.NodeReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasHttpAddress()) ? p.GetHttpAddress() : null;
		}

		public override int GetNumContainers()
		{
			YarnProtos.NodeReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasNumContainers()) ? p.GetNumContainers() : 0;
		}

		public override string GetRackName()
		{
			YarnProtos.NodeReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.HasRackName()) ? p.GetRackName() : null;
		}

		public override Resource GetUsed()
		{
			if (this.used != null)
			{
				return this.used;
			}
			YarnProtos.NodeReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasUsed())
			{
				return null;
			}
			this.used = ConvertFromProtoFormat(p.GetUsed());
			return this.used;
		}

		public override NodeId GetNodeId()
		{
			if (this.nodeId != null)
			{
				return this.nodeId;
			}
			YarnProtos.NodeReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasNodeId())
			{
				return null;
			}
			this.nodeId = ConvertFromProtoFormat(p.GetNodeId());
			return this.nodeId;
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

		public override NodeState GetNodeState()
		{
			YarnProtos.NodeReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasNodeState())
			{
				return null;
			}
			return ProtoUtils.ConvertFromProtoFormat(p.GetNodeState());
		}

		public override void SetNodeState(NodeState nodeState)
		{
			MaybeInitBuilder();
			if (nodeState == null)
			{
				builder.ClearNodeState();
				return;
			}
			builder.SetNodeState(ProtoUtils.ConvertToProtoFormat(nodeState));
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

		public override void SetHttpAddress(string httpAddress)
		{
			MaybeInitBuilder();
			if (httpAddress == null)
			{
				builder.ClearHttpAddress();
				return;
			}
			builder.SetHttpAddress(httpAddress);
		}

		public override void SetNumContainers(int numContainers)
		{
			MaybeInitBuilder();
			if (numContainers == 0)
			{
				builder.ClearNumContainers();
				return;
			}
			builder.SetNumContainers(numContainers);
		}

		public override void SetRackName(string rackName)
		{
			MaybeInitBuilder();
			if (rackName == null)
			{
				builder.ClearRackName();
				return;
			}
			builder.SetRackName(rackName);
		}

		public override void SetUsed(Resource used)
		{
			MaybeInitBuilder();
			if (used == null)
			{
				builder.ClearUsed();
			}
			this.used = used;
		}

		public virtual YarnProtos.NodeReportProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.NodeReportProto)builder.Build());
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
			if (this.nodeId != null && !((NodeIdPBImpl)this.nodeId).GetProto().Equals(builder
				.GetNodeId()))
			{
				builder.SetNodeId(ConvertToProtoFormat(this.nodeId));
			}
			if (this.used != null && !((ResourcePBImpl)this.used).GetProto().Equals(builder.GetUsed
				()))
			{
				builder.SetUsed(ConvertToProtoFormat(this.used));
			}
			if (this.capability != null && !((ResourcePBImpl)this.capability).GetProto().Equals
				(builder.GetCapability()))
			{
				builder.SetCapability(ConvertToProtoFormat(this.capability));
			}
			if (this.labels != null)
			{
				builder.ClearNodeLabels();
				builder.AddAllNodeLabels(this.labels);
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.NodeReportProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.NodeReportProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private NodeIdPBImpl ConvertFromProtoFormat(YarnProtos.NodeIdProto p)
		{
			return new NodeIdPBImpl(p);
		}

		private YarnProtos.NodeIdProto ConvertToProtoFormat(NodeId nodeId)
		{
			return ((NodeIdPBImpl)nodeId).GetProto();
		}

		private ResourcePBImpl ConvertFromProtoFormat(YarnProtos.ResourceProto p)
		{
			return new ResourcePBImpl(p);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource r)
		{
			return ((ResourcePBImpl)r).GetProto();
		}

		public override ICollection<string> GetNodeLabels()
		{
			InitNodeLabels();
			return this.labels;
		}

		public override void SetNodeLabels(ICollection<string> nodeLabels)
		{
			MaybeInitBuilder();
			builder.ClearNodeLabels();
			this.labels = nodeLabels;
		}

		private void InitNodeLabels()
		{
			if (this.labels != null)
			{
				return;
			}
			YarnProtos.NodeReportProtoOrBuilder p = viaProto ? proto : builder;
			this.labels = new HashSet<string>();
			Sharpen.Collections.AddAll(this.labels, p.GetNodeLabelsList());
		}
	}
}
