using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetLabelsToNodesRequestPBImpl : GetLabelsToNodesRequest
	{
		internal ICollection<string> nodeLabels = null;

		internal YarnServiceProtos.GetLabelsToNodesRequestProto proto = YarnServiceProtos.GetLabelsToNodesRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetLabelsToNodesRequestProto.Builder builder = null;

		internal bool viaProto = false;

		public GetLabelsToNodesRequestPBImpl()
		{
			builder = YarnServiceProtos.GetLabelsToNodesRequestProto.NewBuilder();
		}

		public GetLabelsToNodesRequestPBImpl(YarnServiceProtos.GetLabelsToNodesRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetLabelsToNodesRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetLabelsToNodesRequestProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetLabelsToNodesRequestProto)builder.Build());
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (nodeLabels != null && !nodeLabels.IsEmpty())
			{
				builder.ClearNodeLabels();
				builder.AddAllNodeLabels(nodeLabels);
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetLabelsToNodesRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void InitNodeLabels()
		{
			if (this.nodeLabels != null)
			{
				return;
			}
			YarnServiceProtos.GetLabelsToNodesRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<string> nodeLabelsList = p.GetNodeLabelsList();
			this.nodeLabels = new HashSet<string>();
			Sharpen.Collections.AddAll(this.nodeLabels, nodeLabelsList);
		}

		public override ICollection<string> GetNodeLabels()
		{
			InitNodeLabels();
			return this.nodeLabels;
		}

		public override void SetNodeLabels(ICollection<string> nodeLabels)
		{
			MaybeInitBuilder();
			if (nodeLabels == null)
			{
				builder.ClearNodeLabels();
			}
			this.nodeLabels = nodeLabels;
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
	}
}
