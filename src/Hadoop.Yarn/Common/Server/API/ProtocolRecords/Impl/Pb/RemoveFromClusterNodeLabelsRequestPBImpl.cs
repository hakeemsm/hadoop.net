using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class RemoveFromClusterNodeLabelsRequestPBImpl : RemoveFromClusterNodeLabelsRequest
	{
		internal ICollection<string> labels;

		internal YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
			 proto = YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
			.GetDefaultInstance();

		internal YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto.Builder
			 builder = null;

		internal bool viaProto = false;

		public RemoveFromClusterNodeLabelsRequestPBImpl()
		{
			this.builder = YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
				.NewBuilder();
		}

		public RemoveFromClusterNodeLabelsRequestPBImpl(YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
					.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void MergeLocalToBuilder()
		{
			if (this.labels != null && !this.labels.IsEmpty())
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
			proto = ((YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
				)builder.Build());
			viaProto = true;
		}

		public virtual YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
			 GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void InitNodeLabels()
		{
			if (this.labels != null)
			{
				return;
			}
			YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProtoOrBuilder
				 p = viaProto ? proto : builder;
			this.labels = new HashSet<string>();
			Sharpen.Collections.AddAll(this.labels, p.GetNodeLabelsList());
		}

		public override void SetNodeLabels(ICollection<string> labels)
		{
			MaybeInitBuilder();
			if (labels == null || labels.IsEmpty())
			{
				builder.ClearNodeLabels();
			}
			this.labels = labels;
		}

		public override ICollection<string> GetNodeLabels()
		{
			InitNodeLabels();
			return this.labels;
		}

		public override int GetHashCode()
		{
			System.Diagnostics.Debug.Assert(false, "hashCode not designed");
			return 0;
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
	}
}
