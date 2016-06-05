using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class RegisterNodeManagerRequestPBImpl : RegisterNodeManagerRequest
	{
		internal YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto proto = YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto
			.GetDefaultInstance();

		internal YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private Resource resource = null;

		private NodeId nodeId = null;

		private IList<NMContainerStatus> containerStatuses = null;

		private IList<ApplicationId> runningApplications = null;

		public RegisterNodeManagerRequestPBImpl()
		{
			builder = YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto.NewBuilder
				();
		}

		public RegisterNodeManagerRequestPBImpl(YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.containerStatuses != null)
			{
				AddNMContainerStatusesToProto();
			}
			if (this.runningApplications != null)
			{
				AddRunningApplicationsToProto();
			}
			if (this.resource != null)
			{
				builder.SetResource(ConvertToProtoFormat(this.resource));
			}
			if (this.nodeId != null)
			{
				builder.SetNodeId(ConvertToProtoFormat(this.nodeId));
			}
		}

		private void AddNMContainerStatusesToProto()
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.ClearContainerStatuses();
				IList<YarnServerCommonServiceProtos.NMContainerStatusProto> list = new AList<YarnServerCommonServiceProtos.NMContainerStatusProto
					>();
				foreach (NMContainerStatus status in this.containerStatuses)
				{
					list.AddItem(ConvertToProtoFormat(status));
				}
				builder.AddAllContainerStatuses(list);
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto)builder.Build
				());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto.NewBuilder
					(proto);
			}
			viaProto = false;
		}

		public override Resource GetResource()
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerRequestProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (this.resource != null)
			{
				return this.resource;
			}
			if (!p.HasResource())
			{
				return null;
			}
			this.resource = ConvertFromProtoFormat(p.GetResource());
			return this.resource;
		}

		public override void SetResource(Resource resource)
		{
			MaybeInitBuilder();
			if (resource == null)
			{
				builder.ClearResource();
			}
			this.resource = resource;
		}

		public override NodeId GetNodeId()
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerRequestProtoOrBuilder p = viaProto
				 ? proto : builder;
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

		public override void SetNodeId(NodeId nodeId)
		{
			MaybeInitBuilder();
			if (nodeId == null)
			{
				builder.ClearNodeId();
			}
			this.nodeId = nodeId;
		}

		public override int GetHttpPort()
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerRequestProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasHttpPort())
			{
				return 0;
			}
			return (p.GetHttpPort());
		}

		public override void SetHttpPort(int httpPort)
		{
			MaybeInitBuilder();
			builder.SetHttpPort(httpPort);
		}

		public override IList<ApplicationId> GetRunningApplications()
		{
			InitRunningApplications();
			return runningApplications;
		}

		private void InitRunningApplications()
		{
			if (this.runningApplications != null)
			{
				return;
			}
			YarnServerCommonServiceProtos.RegisterNodeManagerRequestProtoOrBuilder p = viaProto
				 ? proto : builder;
			IList<YarnProtos.ApplicationIdProto> list = p.GetRunningApplicationsList();
			this.runningApplications = new AList<ApplicationId>();
			foreach (YarnProtos.ApplicationIdProto c in list)
			{
				this.runningApplications.AddItem(ConvertFromProtoFormat(c));
			}
		}

		public override void SetRunningApplications(IList<ApplicationId> apps)
		{
			if (apps == null)
			{
				return;
			}
			InitRunningApplications();
			Sharpen.Collections.AddAll(this.runningApplications, apps);
		}

		private void AddRunningApplicationsToProto()
		{
			MaybeInitBuilder();
			builder.ClearRunningApplications();
			if (runningApplications == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ApplicationIdProto> it = new _IEnumerable_210(this);
			builder.AddAllRunningApplications(it);
		}

		private sealed class _IEnumerable_210 : IEnumerable<YarnProtos.ApplicationIdProto
			>
		{
			public _IEnumerable_210(RegisterNodeManagerRequestPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ApplicationIdProto> GetEnumerator()
			{
				return new _IEnumerator_214(this);
			}

			private sealed class _IEnumerator_214 : IEnumerator<YarnProtos.ApplicationIdProto
				>
			{
				public _IEnumerator_214(_IEnumerable_210 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.runningApplications.GetEnumerator();
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

				private readonly _IEnumerable_210 _enclosing;
			}

			private readonly RegisterNodeManagerRequestPBImpl _enclosing;
		}

		public override IList<NMContainerStatus> GetNMContainerStatuses()
		{
			InitContainerRecoveryReports();
			return containerStatuses;
		}

		private void InitContainerRecoveryReports()
		{
			if (this.containerStatuses != null)
			{
				return;
			}
			YarnServerCommonServiceProtos.RegisterNodeManagerRequestProtoOrBuilder p = viaProto
				 ? proto : builder;
			IList<YarnServerCommonServiceProtos.NMContainerStatusProto> list = p.GetContainerStatusesList
				();
			this.containerStatuses = new AList<NMContainerStatus>();
			foreach (YarnServerCommonServiceProtos.NMContainerStatusProto c in list)
			{
				this.containerStatuses.AddItem(ConvertFromProtoFormat(c));
			}
		}

		public override void SetContainerStatuses(IList<NMContainerStatus> containerReports
			)
		{
			if (containerReports == null)
			{
				return;
			}
			InitContainerRecoveryReports();
			Sharpen.Collections.AddAll(this.containerStatuses, containerReports);
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

		public override string GetNMVersion()
		{
			YarnServerCommonServiceProtos.RegisterNodeManagerRequestProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasNmVersion())
			{
				return string.Empty;
			}
			return (p.GetNmVersion());
		}

		public override void SetNMVersion(string version)
		{
			MaybeInitBuilder();
			builder.SetNmVersion(version);
		}

		private ApplicationIdPBImpl ConvertFromProtoFormat(YarnProtos.ApplicationIdProto 
			p)
		{
			return new ApplicationIdPBImpl(p);
		}

		private YarnProtos.ApplicationIdProto ConvertToProtoFormat(ApplicationId t)
		{
			return ((ApplicationIdPBImpl)t).GetProto();
		}

		private NodeIdPBImpl ConvertFromProtoFormat(YarnProtos.NodeIdProto p)
		{
			return new NodeIdPBImpl(p);
		}

		private YarnProtos.NodeIdProto ConvertToProtoFormat(NodeId t)
		{
			return ((NodeIdPBImpl)t).GetProto();
		}

		private ResourcePBImpl ConvertFromProtoFormat(YarnProtos.ResourceProto p)
		{
			return new ResourcePBImpl(p);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource t)
		{
			return ((ResourcePBImpl)t).GetProto();
		}

		private NMContainerStatusPBImpl ConvertFromProtoFormat(YarnServerCommonServiceProtos.NMContainerStatusProto
			 c)
		{
			return new NMContainerStatusPBImpl(c);
		}

		private YarnServerCommonServiceProtos.NMContainerStatusProto ConvertToProtoFormat
			(NMContainerStatus c)
		{
			return ((NMContainerStatusPBImpl)c).GetProto();
		}
	}
}
