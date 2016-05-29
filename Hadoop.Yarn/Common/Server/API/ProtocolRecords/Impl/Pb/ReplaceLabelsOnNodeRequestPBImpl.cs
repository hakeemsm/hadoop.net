using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class ReplaceLabelsOnNodeRequestPBImpl : ReplaceLabelsOnNodeRequest
	{
		internal YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto proto
			 = YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto.GetDefaultInstance
			();

		internal YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto.Builder
			 builder = null;

		internal bool viaProto = false;

		private IDictionary<NodeId, ICollection<string>> nodeIdToLabels;

		public ReplaceLabelsOnNodeRequestPBImpl()
		{
			this.builder = YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto
				.NewBuilder();
		}

		public ReplaceLabelsOnNodeRequestPBImpl(YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto
			 proto)
		{
			this.proto = proto;
			this.viaProto = true;
		}

		private void InitNodeToLabels()
		{
			if (this.nodeIdToLabels != null)
			{
				return;
			}
			YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProtoOrBuilder p
				 = viaProto ? proto : builder;
			IList<YarnProtos.NodeIdToLabelsProto> list = p.GetNodeToLabelsList();
			this.nodeIdToLabels = new Dictionary<NodeId, ICollection<string>>();
			foreach (YarnProtos.NodeIdToLabelsProto c in list)
			{
				this.nodeIdToLabels[new NodeIdPBImpl(c.GetNodeId())] = Sets.NewHashSet(c.GetNodeLabelsList
					());
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto.
					NewBuilder(proto);
			}
			viaProto = false;
		}

		private void AddNodeToLabelsToProto()
		{
			MaybeInitBuilder();
			builder.ClearNodeToLabels();
			if (nodeIdToLabels == null)
			{
				return;
			}
			IEnumerable<YarnProtos.NodeIdToLabelsProto> iterable = new _IEnumerable_84(this);
			builder.AddAllNodeToLabels(iterable);
		}

		private sealed class _IEnumerable_84 : IEnumerable<YarnProtos.NodeIdToLabelsProto
			>
		{
			public _IEnumerable_84(ReplaceLabelsOnNodeRequestPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.NodeIdToLabelsProto> GetEnumerator()
			{
				return new _IEnumerator_87(this);
			}

			private sealed class _IEnumerator_87 : IEnumerator<YarnProtos.NodeIdToLabelsProto
				>
			{
				public _IEnumerator_87(_IEnumerable_84 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.nodeIdToLabels.GetEnumerator();
				}

				internal IEnumerator<KeyValuePair<NodeId, ICollection<string>>> iter;

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				public override YarnProtos.NodeIdToLabelsProto Next()
				{
					KeyValuePair<NodeId, ICollection<string>> now = this.iter.Next();
					return ((YarnProtos.NodeIdToLabelsProto)YarnProtos.NodeIdToLabelsProto.NewBuilder
						().SetNodeId(this._enclosing._enclosing.ConvertToProtoFormat(now.Key)).ClearNodeLabels
						().AddAllNodeLabels(now.Value).Build());
				}

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				private readonly _IEnumerable_84 _enclosing;
			}

			private readonly ReplaceLabelsOnNodeRequestPBImpl _enclosing;
		}

		private void MergeLocalToBuilder()
		{
			if (this.nodeIdToLabels != null)
			{
				AddNodeToLabelsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto)
				builder.Build());
			viaProto = true;
		}

		public virtual YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto
			 GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override IDictionary<NodeId, ICollection<string>> GetNodeToLabels()
		{
			InitNodeToLabels();
			return this.nodeIdToLabels;
		}

		public override void SetNodeToLabels(IDictionary<NodeId, ICollection<string>> map
			)
		{
			InitNodeToLabels();
			nodeIdToLabels.Clear();
			nodeIdToLabels.PutAll(map);
		}

		private YarnProtos.NodeIdProto ConvertToProtoFormat(NodeId t)
		{
			return ((NodeIdPBImpl)t).GetProto();
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
