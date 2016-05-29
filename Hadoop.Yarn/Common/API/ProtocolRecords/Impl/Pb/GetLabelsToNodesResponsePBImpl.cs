using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetLabelsToNodesResponsePBImpl : GetLabelsToNodesResponse
	{
		internal YarnServiceProtos.GetLabelsToNodesResponseProto proto = YarnServiceProtos.GetLabelsToNodesResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetLabelsToNodesResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private IDictionary<string, ICollection<NodeId>> labelsToNodes;

		public GetLabelsToNodesResponsePBImpl()
		{
			this.builder = YarnServiceProtos.GetLabelsToNodesResponseProto.NewBuilder();
		}

		public GetLabelsToNodesResponsePBImpl(YarnServiceProtos.GetLabelsToNodesResponseProto
			 proto)
		{
			this.proto = proto;
			this.viaProto = true;
		}

		private void InitLabelsToNodes()
		{
			if (this.labelsToNodes != null)
			{
				return;
			}
			YarnServiceProtos.GetLabelsToNodesResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.LabelsToNodeIdsProto> list = p.GetLabelsToNodesList();
			this.labelsToNodes = new Dictionary<string, ICollection<NodeId>>();
			foreach (YarnProtos.LabelsToNodeIdsProto c in list)
			{
				ICollection<NodeId> setNodes = new HashSet<NodeId>();
				foreach (YarnProtos.NodeIdProto n in c.GetNodeIdList())
				{
					NodeId node = new NodeIdPBImpl(n);
					setNodes.AddItem(node);
				}
				if (!setNodes.IsEmpty())
				{
					this.labelsToNodes[c.GetNodeLabels()] = setNodes;
				}
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetLabelsToNodesResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void AddLabelsToNodesToProto()
		{
			MaybeInitBuilder();
			builder.ClearLabelsToNodes();
			if (labelsToNodes == null)
			{
				return;
			}
			IEnumerable<YarnProtos.LabelsToNodeIdsProto> iterable = new _IEnumerable_92(this);
			builder.AddAllLabelsToNodes(iterable);
		}

		private sealed class _IEnumerable_92 : IEnumerable<YarnProtos.LabelsToNodeIdsProto
			>
		{
			public _IEnumerable_92(GetLabelsToNodesResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.LabelsToNodeIdsProto> GetEnumerator()
			{
				return new _IEnumerator_95(this);
			}

			private sealed class _IEnumerator_95 : IEnumerator<YarnProtos.LabelsToNodeIdsProto
				>
			{
				public _IEnumerator_95(_IEnumerable_92 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.labelsToNodes.GetEnumerator();
				}

				internal IEnumerator<KeyValuePair<string, ICollection<NodeId>>> iter;

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				public override YarnProtos.LabelsToNodeIdsProto Next()
				{
					KeyValuePair<string, ICollection<NodeId>> now = this.iter.Next();
					ICollection<YarnProtos.NodeIdProto> nodeProtoSet = new HashSet<YarnProtos.NodeIdProto
						>();
					foreach (NodeId n in now.Value)
					{
						nodeProtoSet.AddItem(this._enclosing._enclosing.ConvertToProtoFormat(n));
					}
					return ((YarnProtos.LabelsToNodeIdsProto)YarnProtos.LabelsToNodeIdsProto.NewBuilder
						().SetNodeLabels(now.Key).AddAllNodeId(nodeProtoSet).Build());
				}

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				private readonly _IEnumerable_92 _enclosing;
			}

			private readonly GetLabelsToNodesResponsePBImpl _enclosing;
		}

		private void MergeLocalToBuilder()
		{
			if (this.labelsToNodes != null)
			{
				AddLabelsToNodesToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetLabelsToNodesResponseProto)builder.Build());
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetLabelsToNodesResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetLabelsToNodesResponseProto)builder
				.Build());
			viaProto = true;
			return proto;
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

		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public override void SetLabelsToNodes(IDictionary<string, ICollection<NodeId>> map
			)
		{
			InitLabelsToNodes();
			labelsToNodes.Clear();
			labelsToNodes.PutAll(map);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public override IDictionary<string, ICollection<NodeId>> GetLabelsToNodes()
		{
			InitLabelsToNodes();
			return this.labelsToNodes;
		}
	}
}
