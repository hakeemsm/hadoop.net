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
	public class GetClusterNodesResponsePBImpl : GetClusterNodesResponse
	{
		internal YarnServiceProtos.GetClusterNodesResponseProto proto = YarnServiceProtos.GetClusterNodesResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetClusterNodesResponseProto.Builder builder = null;

		internal bool viaProto = false;

		internal IList<NodeReport> nodeManagerInfoList;

		public GetClusterNodesResponsePBImpl()
		{
			builder = YarnServiceProtos.GetClusterNodesResponseProto.NewBuilder();
		}

		public GetClusterNodesResponsePBImpl(YarnServiceProtos.GetClusterNodesResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override IList<NodeReport> GetNodeReports()
		{
			InitLocalNodeManagerInfosList();
			return this.nodeManagerInfoList;
		}

		public override void SetNodeReports(IList<NodeReport> nodeManagers)
		{
			if (nodeManagers == null)
			{
				builder.ClearNodeReports();
			}
			this.nodeManagerInfoList = nodeManagers;
		}

		public virtual YarnServiceProtos.GetClusterNodesResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetClusterNodesResponseProto)builder
				.Build());
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
			if (this.nodeManagerInfoList != null)
			{
				AddLocalNodeManagerInfosToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetClusterNodesResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetClusterNodesResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		// Once this is called. containerList will never be null - until a getProto
		// is called.
		private void InitLocalNodeManagerInfosList()
		{
			if (this.nodeManagerInfoList != null)
			{
				return;
			}
			YarnServiceProtos.GetClusterNodesResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.NodeReportProto> list = p.GetNodeReportsList();
			nodeManagerInfoList = new AList<NodeReport>();
			foreach (YarnProtos.NodeReportProto a in list)
			{
				nodeManagerInfoList.AddItem(ConvertFromProtoFormat(a));
			}
		}

		private void AddLocalNodeManagerInfosToProto()
		{
			MaybeInitBuilder();
			builder.ClearNodeReports();
			if (nodeManagerInfoList == null)
			{
				return;
			}
			IEnumerable<YarnProtos.NodeReportProto> iterable = new _IEnumerable_138(this);
			builder.AddAllNodeReports(iterable);
		}

		private sealed class _IEnumerable_138 : IEnumerable<YarnProtos.NodeReportProto>
		{
			public _IEnumerable_138(GetClusterNodesResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.NodeReportProto> GetEnumerator()
			{
				return new _IEnumerator_141(this);
			}

			private sealed class _IEnumerator_141 : IEnumerator<YarnProtos.NodeReportProto>
			{
				public _IEnumerator_141(_IEnumerable_138 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.nodeManagerInfoList.GetEnumerator();
				}

				internal IEnumerator<NodeReport> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.NodeReportProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_138 _enclosing;
			}

			private readonly GetClusterNodesResponsePBImpl _enclosing;
		}

		private NodeReportPBImpl ConvertFromProtoFormat(YarnProtos.NodeReportProto p)
		{
			return new NodeReportPBImpl(p);
		}

		private YarnProtos.NodeReportProto ConvertToProtoFormat(NodeReport t)
		{
			return ((NodeReportPBImpl)t).GetProto();
		}
	}
}
