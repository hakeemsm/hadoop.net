using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetClusterNodesRequestPBImpl : GetClusterNodesRequest
	{
		internal YarnServiceProtos.GetClusterNodesRequestProto proto = YarnServiceProtos.GetClusterNodesRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetClusterNodesRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private EnumSet<NodeState> states = null;

		public GetClusterNodesRequestPBImpl()
		{
			builder = YarnServiceProtos.GetClusterNodesRequestProto.NewBuilder();
		}

		public GetClusterNodesRequestPBImpl(YarnServiceProtos.GetClusterNodesRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetClusterNodesRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetClusterNodesRequestProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		public override EnumSet<NodeState> GetNodeStates()
		{
			InitNodeStates();
			return this.states;
		}

		public override void SetNodeStates(EnumSet<NodeState> states)
		{
			InitNodeStates();
			this.states.Clear();
			if (states == null)
			{
				return;
			}
			Sharpen.Collections.AddAll(this.states, states);
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetClusterNodesRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetClusterNodesRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private void MergeLocalToBuilder()
		{
			if (this.states != null)
			{
				MaybeInitBuilder();
				builder.ClearNodeStates();
				IEnumerable<YarnProtos.NodeStateProto> iterable = new _IEnumerable_98(this);
				builder.AddAllNodeStates(iterable);
			}
		}

		private sealed class _IEnumerable_98 : IEnumerable<YarnProtos.NodeStateProto>
		{
			public _IEnumerable_98(GetClusterNodesRequestPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.NodeStateProto> GetEnumerator()
			{
				return new _IEnumerator_101(this);
			}

			private sealed class _IEnumerator_101 : IEnumerator<YarnProtos.NodeStateProto>
			{
				public _IEnumerator_101()
				{
					this.iter = this._enclosing._enclosing.states.GetEnumerator();
				}

				internal IEnumerator<NodeState> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.NodeStateProto Next()
				{
					return ProtoUtils.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}
			}

			private readonly GetClusterNodesRequestPBImpl _enclosing;
		}

		private void InitNodeStates()
		{
			if (this.states != null)
			{
				return;
			}
			YarnServiceProtos.GetClusterNodesRequestProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.NodeStateProto> list = p.GetNodeStatesList();
			this.states = EnumSet.NoneOf<NodeState>();
			foreach (YarnProtos.NodeStateProto c in list)
			{
				this.states.AddItem(ProtoUtils.ConvertFromProtoFormat(c));
			}
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
