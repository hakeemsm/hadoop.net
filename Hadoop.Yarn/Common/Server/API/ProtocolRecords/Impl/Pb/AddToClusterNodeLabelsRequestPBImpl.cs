using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class AddToClusterNodeLabelsRequestPBImpl : AddToClusterNodeLabelsRequest
	{
		internal ICollection<string> labels;

		internal YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
			 proto = YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
			.GetDefaultInstance();

		internal YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto.Builder
			 builder = null;

		internal bool viaProto = false;

		public AddToClusterNodeLabelsRequestPBImpl()
		{
			this.builder = YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
				.NewBuilder();
		}

		public AddToClusterNodeLabelsRequestPBImpl(YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
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
			proto = ((YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
				)builder.Build());
			viaProto = true;
		}

		public virtual YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
			 GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void InitLabels()
		{
			if (this.labels != null)
			{
				return;
			}
			YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProtoOrBuilder
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
			InitLabels();
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
