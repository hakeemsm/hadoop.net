using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetNodesToLabelsResponsePBImpl : GetNodesToLabelsResponse
	{
		internal YarnServiceProtos.GetNodesToLabelsResponseProto proto = YarnServiceProtos.GetNodesToLabelsResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetNodesToLabelsResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private IDictionary<NodeId, ICollection<string>> nodeToLabels;

		public GetNodesToLabelsResponsePBImpl()
		{
			this.builder = YarnServiceProtos.GetNodesToLabelsResponseProto.NewBuilder();
		}

		public GetNodesToLabelsResponsePBImpl(YarnServiceProtos.GetNodesToLabelsResponseProto
			 proto)
		{
			this.proto = proto;
			this.viaProto = true;
		}

		private void InitNodeToLabels()
		{
			if (this.nodeToLabels != null)
			{
				return;
			}
			YarnServiceProtos.GetNodesToLabelsResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.NodeIdToLabelsProto> list = p.GetNodeToLabelsList();
			this.nodeToLabels = new Dictionary<NodeId, ICollection<string>>();
			foreach (YarnProtos.NodeIdToLabelsProto c in list)
			{
				this.nodeToLabels[new NodeIdPBImpl(c.GetNodeId())] = Sets.NewHashSet(c.GetNodeLabelsList
					());
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetNodesToLabelsResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void AddNodeToLabelsToProto()
		{
			MaybeInitBuilder();
			builder.ClearNodeToLabels();
			if (nodeToLabels == null)
			{
				return;
			}
			IEnumerable<YarnProtos.NodeIdToLabelsProto> iterable = new _IEnumerable_84(this);
			builder.AddAllNodeToLabels(iterable);
		}

		private sealed class _IEnumerable_84 : IEnumerable<YarnProtos.NodeIdToLabelsProto
			>
		{
			public _IEnumerable_84(GetNodesToLabelsResponsePBImpl _enclosing)
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
					this.iter = this._enclosing._enclosing.nodeToLabels.GetEnumerator();
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
						().SetNodeId(this._enclosing._enclosing.ConvertToProtoFormat(now.Key)).AddAllNodeLabels
						(now.Value).Build());
				}

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				private readonly _IEnumerable_84 _enclosing;
			}

			private readonly GetNodesToLabelsResponsePBImpl _enclosing;
		}

		private void MergeLocalToBuilder()
		{
			if (this.nodeToLabels != null)
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
			proto = ((YarnServiceProtos.GetNodesToLabelsResponseProto)builder.Build());
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetNodesToLabelsResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetNodesToLabelsResponseProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		public override IDictionary<NodeId, ICollection<string>> GetNodeToLabels()
		{
			InitNodeToLabels();
			return this.nodeToLabels;
		}

		public override void SetNodeToLabels(IDictionary<NodeId, ICollection<string>> map
			)
		{
			InitNodeToLabels();
			nodeToLabels.Clear();
			nodeToLabels.PutAll(map);
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
