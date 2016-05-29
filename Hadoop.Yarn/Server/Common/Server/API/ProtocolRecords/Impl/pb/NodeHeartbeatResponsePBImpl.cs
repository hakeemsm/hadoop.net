using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class NodeHeartbeatResponsePBImpl : ProtoBase<YarnServerCommonServiceProtos.NodeHeartbeatResponseProto
		>, NodeHeartbeatResponse
	{
		internal YarnServerCommonServiceProtos.NodeHeartbeatResponseProto proto = YarnServerCommonServiceProtos.NodeHeartbeatResponseProto
			.GetDefaultInstance();

		internal YarnServerCommonServiceProtos.NodeHeartbeatResponseProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private IList<ContainerId> containersToCleanup = null;

		private IList<ContainerId> containersToBeRemovedFromNM = null;

		private IList<ApplicationId> applicationsToCleanup = null;

		private IDictionary<ApplicationId, ByteBuffer> systemCredentials = null;

		private MasterKey containerTokenMasterKey = null;

		private MasterKey nmTokenMasterKey = null;

		public NodeHeartbeatResponsePBImpl()
		{
			builder = YarnServerCommonServiceProtos.NodeHeartbeatResponseProto.NewBuilder();
		}

		public NodeHeartbeatResponsePBImpl(YarnServerCommonServiceProtos.NodeHeartbeatResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override YarnServerCommonServiceProtos.NodeHeartbeatResponseProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerCommonServiceProtos.NodeHeartbeatResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.containersToCleanup != null)
			{
				AddContainersToCleanupToProto();
			}
			if (this.applicationsToCleanup != null)
			{
				AddApplicationsToCleanupToProto();
			}
			if (this.containersToBeRemovedFromNM != null)
			{
				AddContainersToBeRemovedFromNMToProto();
			}
			if (this.containerTokenMasterKey != null)
			{
				builder.SetContainerTokenMasterKey(ConvertToProtoFormat(this.containerTokenMasterKey
					));
			}
			if (this.nmTokenMasterKey != null)
			{
				builder.SetNmTokenMasterKey(ConvertToProtoFormat(this.nmTokenMasterKey));
			}
			if (this.systemCredentials != null)
			{
				AddSystemCredentialsToProto();
			}
		}

		private void AddSystemCredentialsToProto()
		{
			MaybeInitBuilder();
			builder.ClearSystemCredentialsForApps();
			foreach (KeyValuePair<ApplicationId, ByteBuffer> entry in systemCredentials)
			{
				builder.AddSystemCredentialsForApps(YarnServerCommonServiceProtos.SystemCredentialsForAppsProto
					.NewBuilder().SetAppId(ConvertToProtoFormat(entry.Key)).SetCredentialsForApp(ProtoUtils
					.ConvertToProtoFormat(entry.Value.Duplicate())));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerCommonServiceProtos.NodeHeartbeatResponseProto)builder.Build(
				));
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerCommonServiceProtos.NodeHeartbeatResponseProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		public virtual int GetResponseId()
		{
			YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			return (p.GetResponseId());
		}

		public virtual void SetResponseId(int responseId)
		{
			MaybeInitBuilder();
			builder.SetResponseId((responseId));
		}

		public virtual MasterKey GetContainerTokenMasterKey()
		{
			YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (this.containerTokenMasterKey != null)
			{
				return this.containerTokenMasterKey;
			}
			if (!p.HasContainerTokenMasterKey())
			{
				return null;
			}
			this.containerTokenMasterKey = ConvertFromProtoFormat(p.GetContainerTokenMasterKey
				());
			return this.containerTokenMasterKey;
		}

		public virtual void SetContainerTokenMasterKey(MasterKey masterKey)
		{
			MaybeInitBuilder();
			if (masterKey == null)
			{
				builder.ClearContainerTokenMasterKey();
			}
			this.containerTokenMasterKey = masterKey;
		}

		public virtual MasterKey GetNMTokenMasterKey()
		{
			YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (this.nmTokenMasterKey != null)
			{
				return this.nmTokenMasterKey;
			}
			if (!p.HasNmTokenMasterKey())
			{
				return null;
			}
			this.nmTokenMasterKey = ConvertFromProtoFormat(p.GetNmTokenMasterKey());
			return this.nmTokenMasterKey;
		}

		public virtual void SetNMTokenMasterKey(MasterKey masterKey)
		{
			MaybeInitBuilder();
			if (masterKey == null)
			{
				builder.ClearNmTokenMasterKey();
			}
			this.nmTokenMasterKey = masterKey;
		}

		public virtual NodeAction GetNodeAction()
		{
			YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasNodeAction())
			{
				return null;
			}
			return (ConvertFromProtoFormat(p.GetNodeAction()));
		}

		public virtual void SetNodeAction(NodeAction nodeAction)
		{
			MaybeInitBuilder();
			if (nodeAction == null)
			{
				builder.ClearNodeAction();
				return;
			}
			builder.SetNodeAction(ConvertToProtoFormat(nodeAction));
		}

		public virtual string GetDiagnosticsMessage()
		{
			YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasDiagnosticsMessage())
			{
				return null;
			}
			return p.GetDiagnosticsMessage();
		}

		public virtual void SetDiagnosticsMessage(string diagnosticsMessage)
		{
			MaybeInitBuilder();
			if (diagnosticsMessage == null)
			{
				builder.ClearDiagnosticsMessage();
				return;
			}
			builder.SetDiagnosticsMessage((diagnosticsMessage));
		}

		public virtual IList<ContainerId> GetContainersToCleanup()
		{
			InitContainersToCleanup();
			return this.containersToCleanup;
		}

		public virtual IList<ContainerId> GetContainersToBeRemovedFromNM()
		{
			InitContainersToBeRemovedFromNM();
			return this.containersToBeRemovedFromNM;
		}

		private void InitContainersToCleanup()
		{
			if (this.containersToCleanup != null)
			{
				return;
			}
			YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			IList<YarnProtos.ContainerIdProto> list = p.GetContainersToCleanupList();
			this.containersToCleanup = new AList<ContainerId>();
			foreach (YarnProtos.ContainerIdProto c in list)
			{
				this.containersToCleanup.AddItem(ConvertFromProtoFormat(c));
			}
		}

		private void InitContainersToBeRemovedFromNM()
		{
			if (this.containersToBeRemovedFromNM != null)
			{
				return;
			}
			YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			IList<YarnProtos.ContainerIdProto> list = p.GetContainersToBeRemovedFromNmList();
			this.containersToBeRemovedFromNM = new AList<ContainerId>();
			foreach (YarnProtos.ContainerIdProto c in list)
			{
				this.containersToBeRemovedFromNM.AddItem(ConvertFromProtoFormat(c));
			}
		}

		public virtual void AddAllContainersToCleanup(IList<ContainerId> containersToCleanup
			)
		{
			if (containersToCleanup == null)
			{
				return;
			}
			InitContainersToCleanup();
			Sharpen.Collections.AddAll(this.containersToCleanup, containersToCleanup);
		}

		public virtual void AddContainersToBeRemovedFromNM(IList<ContainerId> containers)
		{
			if (containers == null)
			{
				return;
			}
			InitContainersToBeRemovedFromNM();
			Sharpen.Collections.AddAll(this.containersToBeRemovedFromNM, containers);
		}

		private void AddContainersToCleanupToProto()
		{
			MaybeInitBuilder();
			builder.ClearContainersToCleanup();
			if (containersToCleanup == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ContainerIdProto> iterable = new _IEnumerable_283(this);
			builder.AddAllContainersToCleanup(iterable);
		}

		private sealed class _IEnumerable_283 : IEnumerable<YarnProtos.ContainerIdProto>
		{
			public _IEnumerable_283(NodeHeartbeatResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ContainerIdProto> GetEnumerator()
			{
				return new _IEnumerator_287(this);
			}

			private sealed class _IEnumerator_287 : IEnumerator<YarnProtos.ContainerIdProto>
			{
				public _IEnumerator_287(_IEnumerable_283 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.containersToCleanup.GetEnumerator();
				}

				internal IEnumerator<ContainerId> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ContainerIdProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_283 _enclosing;
			}

			private readonly NodeHeartbeatResponsePBImpl _enclosing;
		}

		private void AddContainersToBeRemovedFromNMToProto()
		{
			MaybeInitBuilder();
			builder.ClearContainersToBeRemovedFromNm();
			if (containersToBeRemovedFromNM == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ContainerIdProto> iterable = new _IEnumerable_318(this);
			builder.AddAllContainersToBeRemovedFromNm(iterable);
		}

		private sealed class _IEnumerable_318 : IEnumerable<YarnProtos.ContainerIdProto>
		{
			public _IEnumerable_318(NodeHeartbeatResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ContainerIdProto> GetEnumerator()
			{
				return new _IEnumerator_322(this);
			}

			private sealed class _IEnumerator_322 : IEnumerator<YarnProtos.ContainerIdProto>
			{
				public _IEnumerator_322(_IEnumerable_318 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.containersToBeRemovedFromNM.GetEnumerator(
						);
				}

				internal IEnumerator<ContainerId> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnProtos.ContainerIdProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_318 _enclosing;
			}

			private readonly NodeHeartbeatResponsePBImpl _enclosing;
		}

		public virtual IList<ApplicationId> GetApplicationsToCleanup()
		{
			InitApplicationsToCleanup();
			return this.applicationsToCleanup;
		}

		private void InitApplicationsToCleanup()
		{
			if (this.applicationsToCleanup != null)
			{
				return;
			}
			YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			IList<YarnProtos.ApplicationIdProto> list = p.GetApplicationsToCleanupList();
			this.applicationsToCleanup = new AList<ApplicationId>();
			foreach (YarnProtos.ApplicationIdProto c in list)
			{
				this.applicationsToCleanup.AddItem(ConvertFromProtoFormat(c));
			}
		}

		public virtual void AddAllApplicationsToCleanup(IList<ApplicationId> applicationsToCleanup
			)
		{
			if (applicationsToCleanup == null)
			{
				return;
			}
			InitApplicationsToCleanup();
			Sharpen.Collections.AddAll(this.applicationsToCleanup, applicationsToCleanup);
		}

		private void AddApplicationsToCleanupToProto()
		{
			MaybeInitBuilder();
			builder.ClearApplicationsToCleanup();
			if (applicationsToCleanup == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ApplicationIdProto> iterable = new _IEnumerable_381(this);
			builder.AddAllApplicationsToCleanup(iterable);
		}

		private sealed class _IEnumerable_381 : IEnumerable<YarnProtos.ApplicationIdProto
			>
		{
			public _IEnumerable_381(NodeHeartbeatResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ApplicationIdProto> GetEnumerator()
			{
				return new _IEnumerator_385(this);
			}

			private sealed class _IEnumerator_385 : IEnumerator<YarnProtos.ApplicationIdProto
				>
			{
				public _IEnumerator_385(_IEnumerable_381 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.applicationsToCleanup.GetEnumerator();
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

				private readonly _IEnumerable_381 _enclosing;
			}

			private readonly NodeHeartbeatResponsePBImpl _enclosing;
		}

		public virtual IDictionary<ApplicationId, ByteBuffer> GetSystemCredentialsForApps
			()
		{
			if (this.systemCredentials != null)
			{
				return this.systemCredentials;
			}
			InitSystemCredentials();
			return systemCredentials;
		}

		private void InitSystemCredentials()
		{
			YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			IList<YarnServerCommonServiceProtos.SystemCredentialsForAppsProto> list = p.GetSystemCredentialsForAppsList
				();
			this.systemCredentials = new Dictionary<ApplicationId, ByteBuffer>();
			foreach (YarnServerCommonServiceProtos.SystemCredentialsForAppsProto c in list)
			{
				ApplicationId appId = ConvertFromProtoFormat(c.GetAppId());
				ByteBuffer byteBuffer = ProtoUtils.ConvertFromProtoFormat(c.GetCredentialsForApp(
					));
				this.systemCredentials[appId] = byteBuffer;
			}
		}

		public virtual void SetSystemCredentialsForApps(IDictionary<ApplicationId, ByteBuffer
			> systemCredentials)
		{
			if (systemCredentials == null || systemCredentials.IsEmpty())
			{
				return;
			}
			MaybeInitBuilder();
			this.systemCredentials = new Dictionary<ApplicationId, ByteBuffer>();
			this.systemCredentials.PutAll(systemCredentials);
		}

		public virtual long GetNextHeartBeatInterval()
		{
			YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			return (p.GetNextHeartBeatInterval());
		}

		public virtual void SetNextHeartBeatInterval(long nextHeartBeatInterval)
		{
			MaybeInitBuilder();
			builder.SetNextHeartBeatInterval(nextHeartBeatInterval);
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
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

		private NodeAction ConvertFromProtoFormat(YarnServerCommonProtos.NodeActionProto 
			p)
		{
			return NodeAction.ValueOf(p.ToString());
		}

		private YarnServerCommonProtos.NodeActionProto ConvertToProtoFormat(NodeAction t)
		{
			return YarnServerCommonProtos.NodeActionProto.ValueOf(t.ToString());
		}

		private MasterKeyPBImpl ConvertFromProtoFormat(YarnServerCommonProtos.MasterKeyProto
			 p)
		{
			return new MasterKeyPBImpl(p);
		}

		private YarnServerCommonProtos.MasterKeyProto ConvertToProtoFormat(MasterKey t)
		{
			return ((MasterKeyPBImpl)t).GetProto();
		}
	}
}
