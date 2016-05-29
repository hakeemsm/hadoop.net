using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB
{
	public class NodeStatusPBImpl : NodeStatus
	{
		internal YarnServerCommonProtos.NodeStatusProto proto = YarnServerCommonProtos.NodeStatusProto
			.GetDefaultInstance();

		internal YarnServerCommonProtos.NodeStatusProto.Builder builder = null;

		internal bool viaProto = false;

		private NodeId nodeId = null;

		private IList<ContainerStatus> containers = null;

		private NodeHealthStatus nodeHealthStatus = null;

		private IList<ApplicationId> keepAliveApplications = null;

		public NodeStatusPBImpl()
		{
			builder = YarnServerCommonProtos.NodeStatusProto.NewBuilder();
		}

		public NodeStatusPBImpl(YarnServerCommonProtos.NodeStatusProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerCommonProtos.NodeStatusProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((YarnServerCommonProtos.NodeStatusProto)builder.Build
					());
				viaProto = true;
				return proto;
			}
		}

		private void MergeLocalToBuilder()
		{
			lock (this)
			{
				if (this.nodeId != null)
				{
					builder.SetNodeId(ConvertToProtoFormat(this.nodeId));
				}
				if (this.containers != null)
				{
					AddContainersToProto();
				}
				if (this.nodeHealthStatus != null)
				{
					builder.SetNodeHealthStatus(ConvertToProtoFormat(this.nodeHealthStatus));
				}
				if (this.keepAliveApplications != null)
				{
					AddKeepAliveApplicationsToProto();
				}
			}
		}

		private void MergeLocalToProto()
		{
			lock (this)
			{
				if (viaProto)
				{
					MaybeInitBuilder();
				}
				MergeLocalToBuilder();
				proto = ((YarnServerCommonProtos.NodeStatusProto)builder.Build());
				viaProto = true;
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = YarnServerCommonProtos.NodeStatusProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		private void AddContainersToProto()
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.ClearContainersStatuses();
				if (containers == null)
				{
					return;
				}
				IEnumerable<YarnProtos.ContainerStatusProto> iterable = new _IEnumerable_104(this
					);
				builder.AddAllContainersStatuses(iterable);
			}
		}

		private sealed class _IEnumerable_104 : IEnumerable<YarnProtos.ContainerStatusProto
			>
		{
			public _IEnumerable_104(NodeStatusPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ContainerStatusProto> GetEnumerator()
			{
				return new _IEnumerator_107(this);
			}

			private sealed class _IEnumerator_107 : IEnumerator<YarnProtos.ContainerStatusProto
				>
			{
				public _IEnumerator_107(_IEnumerable_104 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.containers.GetEnumerator();
				}

				internal IEnumerator<ContainerStatus> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ContainerStatusProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_104 _enclosing;
			}

			private readonly NodeStatusPBImpl _enclosing;
		}

		private void AddKeepAliveApplicationsToProto()
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.ClearKeepAliveApplications();
				if (keepAliveApplications == null)
				{
					return;
				}
				IEnumerable<YarnProtos.ApplicationIdProto> iterable = new _IEnumerable_138(this);
				builder.AddAllKeepAliveApplications(iterable);
			}
		}

		private sealed class _IEnumerable_138 : IEnumerable<YarnProtos.ApplicationIdProto
			>
		{
			public _IEnumerable_138(NodeStatusPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ApplicationIdProto> GetEnumerator()
			{
				return new _IEnumerator_141(this);
			}

			private sealed class _IEnumerator_141 : IEnumerator<YarnProtos.ApplicationIdProto
				>
			{
				public _IEnumerator_141(_IEnumerable_138 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.keepAliveApplications.GetEnumerator();
				}

				internal IEnumerator<ApplicationId> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ApplicationIdProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_138 _enclosing;
			}

			private readonly NodeStatusPBImpl _enclosing;
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

		public override int GetResponseId()
		{
			lock (this)
			{
				YarnServerCommonProtos.NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
				return p.GetResponseId();
			}
		}

		public override void SetResponseId(int responseId)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetResponseId(responseId);
			}
		}

		public override NodeId GetNodeId()
		{
			lock (this)
			{
				YarnServerCommonProtos.NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
				if (this.nodeId != null)
				{
					return this.nodeId;
				}
				if (!p.HasNodeId())
				{
					return null;
				}
				this.nodeId = ConvertFromProtoFormat(p.GetNodeId());
				return this.nodeId;
			}
		}

		public override void SetNodeId(NodeId nodeId)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (nodeId == null)
				{
					builder.ClearNodeId();
				}
				this.nodeId = nodeId;
			}
		}

		public override IList<ContainerStatus> GetContainersStatuses()
		{
			lock (this)
			{
				InitContainers();
				return this.containers;
			}
		}

		public override void SetContainersStatuses(IList<ContainerStatus> containers)
		{
			lock (this)
			{
				if (containers == null)
				{
					builder.ClearContainersStatuses();
				}
				this.containers = containers;
			}
		}

		public override IList<ApplicationId> GetKeepAliveApplications()
		{
			lock (this)
			{
				InitKeepAliveApplications();
				return this.keepAliveApplications;
			}
		}

		public override void SetKeepAliveApplications(IList<ApplicationId> appIds)
		{
			lock (this)
			{
				if (appIds == null)
				{
					builder.ClearKeepAliveApplications();
				}
				this.keepAliveApplications = appIds;
			}
		}

		private void InitContainers()
		{
			lock (this)
			{
				if (this.containers != null)
				{
					return;
				}
				YarnServerCommonProtos.NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
				IList<YarnProtos.ContainerStatusProto> list = p.GetContainersStatusesList();
				this.containers = new AList<ContainerStatus>();
				foreach (YarnProtos.ContainerStatusProto c in list)
				{
					this.containers.AddItem(ConvertFromProtoFormat(c));
				}
			}
		}

		private void InitKeepAliveApplications()
		{
			lock (this)
			{
				if (this.keepAliveApplications != null)
				{
					return;
				}
				YarnServerCommonProtos.NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
				IList<YarnProtos.ApplicationIdProto> list = p.GetKeepAliveApplicationsList();
				this.keepAliveApplications = new AList<ApplicationId>();
				foreach (YarnProtos.ApplicationIdProto c in list)
				{
					this.keepAliveApplications.AddItem(ConvertFromProtoFormat(c));
				}
			}
		}

		public override NodeHealthStatus GetNodeHealthStatus()
		{
			lock (this)
			{
				YarnServerCommonProtos.NodeStatusProtoOrBuilder p = viaProto ? proto : builder;
				if (nodeHealthStatus != null)
				{
					return nodeHealthStatus;
				}
				if (!p.HasNodeHealthStatus())
				{
					return null;
				}
				nodeHealthStatus = ConvertFromProtoFormat(p.GetNodeHealthStatus());
				return nodeHealthStatus;
			}
		}

		public override void SetNodeHealthStatus(NodeHealthStatus healthStatus)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (healthStatus == null)
				{
					builder.ClearNodeHealthStatus();
				}
				this.nodeHealthStatus = healthStatus;
			}
		}

		private YarnProtos.NodeIdProto ConvertToProtoFormat(NodeId nodeId)
		{
			return ((NodeIdPBImpl)nodeId).GetProto();
		}

		private NodeId ConvertFromProtoFormat(YarnProtos.NodeIdProto proto)
		{
			return new NodeIdPBImpl(proto);
		}

		private YarnServerCommonProtos.NodeHealthStatusProto ConvertToProtoFormat(NodeHealthStatus
			 healthStatus)
		{
			return ((NodeHealthStatusPBImpl)healthStatus).GetProto();
		}

		private NodeHealthStatus ConvertFromProtoFormat(YarnServerCommonProtos.NodeHealthStatusProto
			 proto)
		{
			return new NodeHealthStatusPBImpl(proto);
		}

		private ContainerStatusPBImpl ConvertFromProtoFormat(YarnProtos.ContainerStatusProto
			 c)
		{
			return new ContainerStatusPBImpl(c);
		}

		private YarnProtos.ContainerStatusProto ConvertToProtoFormat(ContainerStatus c)
		{
			return ((ContainerStatusPBImpl)c).GetProto();
		}

		private ApplicationIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationIdProto 
			c)
		{
			return new ApplicationIdPBImpl(c);
		}

		private YarnProtos.ApplicationIdProto ConvertToProtoFormat(ApplicationId c)
		{
			return ((ApplicationIdPBImpl)c).GetProto();
		}
	}
}
