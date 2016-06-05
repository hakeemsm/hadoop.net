using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class UpdateNodeResourceRequestPBImpl : UpdateNodeResourceRequest
	{
		internal YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto proto
			 = YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto.GetDefaultInstance
			();

		internal YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto.Builder
			 builder = null;

		internal bool viaProto = false;

		internal IDictionary<NodeId, ResourceOption> nodeResourceMap = null;

		public UpdateNodeResourceRequestPBImpl()
		{
			builder = YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto.NewBuilder
				();
		}

		public UpdateNodeResourceRequestPBImpl(YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override IDictionary<NodeId, ResourceOption> GetNodeResourceMap()
		{
			InitNodeResourceMap();
			return this.nodeResourceMap;
		}

		public override void SetNodeResourceMap(IDictionary<NodeId, ResourceOption> nodeResourceMap
			)
		{
			if (nodeResourceMap == null)
			{
				return;
			}
			InitNodeResourceMap();
			this.nodeResourceMap.Clear();
			this.nodeResourceMap.PutAll(nodeResourceMap);
		}

		public virtual YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto
			 GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.nodeResourceMap != null)
			{
				AddNodeResourceMap();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto)builder
				.Build());
			viaProto = true;
		}

		private void InitNodeResourceMap()
		{
			if (this.nodeResourceMap != null)
			{
				return;
			}
			YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProtoOrBuilder p = 
				viaProto ? proto : builder;
			IList<YarnProtos.NodeResourceMapProto> list = p.GetNodeResourceMapList();
			this.nodeResourceMap = new Dictionary<NodeId, ResourceOption>(list.Count);
			foreach (YarnProtos.NodeResourceMapProto nodeResourceProto in list)
			{
				this.nodeResourceMap[ConvertFromProtoFormat(nodeResourceProto.GetNodeId())] = ConvertFromProtoFormat
					(nodeResourceProto.GetResourceOption());
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto.NewBuilder
					(proto);
			}
			viaProto = false;
		}

		private YarnProtos.NodeIdProto ConvertToProtoFormat(NodeId nodeId)
		{
			return ((NodeIdPBImpl)nodeId).GetProto();
		}

		private NodeId ConvertFromProtoFormat(YarnProtos.NodeIdProto proto)
		{
			return new NodeIdPBImpl(proto);
		}

		private ResourceOptionPBImpl ConvertFromProtoFormat(YarnProtos.ResourceOptionProto
			 c)
		{
			return new ResourceOptionPBImpl(c);
		}

		private YarnProtos.ResourceOptionProto ConvertToProtoFormat(ResourceOption c)
		{
			return ((ResourceOptionPBImpl)c).GetProto();
		}

		private void AddNodeResourceMap()
		{
			MaybeInitBuilder();
			builder.ClearNodeResourceMap();
			if (nodeResourceMap == null)
			{
				return;
			}
			IEnumerable<YarnProtos.NodeResourceMapProto> values = new _IEnumerable_135(this);
			this.builder.AddAllNodeResourceMap(values);
		}

		private sealed class _IEnumerable_135 : IEnumerable<YarnProtos.NodeResourceMapProto
			>
		{
			public _IEnumerable_135(UpdateNodeResourceRequestPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.NodeResourceMapProto> GetEnumerator()
			{
				return new _IEnumerator_139(this);
			}

			private sealed class _IEnumerator_139 : IEnumerator<YarnProtos.NodeResourceMapProto
				>
			{
				public _IEnumerator_139(_IEnumerable_135 _enclosing)
				{
					this._enclosing = _enclosing;
					this.nodeIterator = this._enclosing._enclosing.nodeResourceMap.Keys.GetEnumerator
						();
				}

				internal IEnumerator<NodeId> nodeIterator;

				public override bool HasNext()
				{
					return this.nodeIterator.HasNext();
				}

				public override YarnProtos.NodeResourceMapProto Next()
				{
					NodeId nodeId = this.nodeIterator.Next();
					return ((YarnProtos.NodeResourceMapProto)YarnProtos.NodeResourceMapProto.NewBuilder
						().SetNodeId(this._enclosing._enclosing.ConvertToProtoFormat(nodeId)).SetResourceOption
						(this._enclosing._enclosing.ConvertToProtoFormat(this._enclosing._enclosing.nodeResourceMap
						[nodeId])).Build());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_135 _enclosing;
			}

			private readonly UpdateNodeResourceRequestPBImpl _enclosing;
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
	}
}
