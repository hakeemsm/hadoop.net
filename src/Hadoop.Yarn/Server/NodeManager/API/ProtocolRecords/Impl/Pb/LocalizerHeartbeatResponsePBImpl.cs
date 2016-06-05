using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.Impl.PB
{
	public class LocalizerHeartbeatResponsePBImpl : ProtoBase<YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto
		>, LocalizerHeartbeatResponse
	{
		internal YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto proto
			 = YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto.GetDefaultInstance
			();

		internal YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto.Builder
			 builder = null;

		internal bool viaProto = false;

		private IList<ResourceLocalizationSpec> resourceSpecs;

		public LocalizerHeartbeatResponsePBImpl()
		{
			builder = YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto.NewBuilder
				();
		}

		public LocalizerHeartbeatResponsePBImpl(YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto
			 GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (resourceSpecs != null)
			{
				AddResourcesToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto)builder
				.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto.NewBuilder
					(proto);
			}
			viaProto = false;
		}

		public virtual LocalizerAction GetLocalizerAction()
		{
			YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasAction())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetAction());
		}

		public virtual IList<ResourceLocalizationSpec> GetResourceSpecs()
		{
			InitResources();
			return this.resourceSpecs;
		}

		public virtual void SetLocalizerAction(LocalizerAction action)
		{
			MaybeInitBuilder();
			if (action == null)
			{
				builder.ClearAction();
				return;
			}
			builder.SetAction(ConvertToProtoFormat(action));
		}

		public virtual void SetResourceSpecs(IList<ResourceLocalizationSpec> rsrcs)
		{
			MaybeInitBuilder();
			if (rsrcs == null)
			{
				builder.ClearResources();
				return;
			}
			this.resourceSpecs = rsrcs;
		}

		private void InitResources()
		{
			if (this.resourceSpecs != null)
			{
				return;
			}
			YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			IList<YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto> list = p.
				GetResourcesList();
			this.resourceSpecs = new AList<ResourceLocalizationSpec>();
			foreach (YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto c in list)
			{
				this.resourceSpecs.AddItem(ConvertFromProtoFormat(c));
			}
		}

		private void AddResourcesToProto()
		{
			MaybeInitBuilder();
			builder.ClearResources();
			if (this.resourceSpecs == null)
			{
				return;
			}
			IEnumerable<YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto> iterable
				 = new _IEnumerable_135(this);
			builder.AddAllResources(iterable);
		}

		private sealed class _IEnumerable_135 : IEnumerable<YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto
			>
		{
			public _IEnumerable_135(LocalizerHeartbeatResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto
				> GetEnumerator()
			{
				return new _IEnumerator_138(this);
			}

			private sealed class _IEnumerator_138 : IEnumerator<YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto
				>
			{
				public _IEnumerator_138()
				{
					this.iter = this._enclosing._enclosing.resourceSpecs.GetEnumerator();
				}

				internal IEnumerator<ResourceLocalizationSpec> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto 
					Next()
				{
					ResourceLocalizationSpec resource = this.iter.Next();
					return ((ResourceLocalizationSpecPBImpl)resource).GetProto();
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}
			}

			private readonly LocalizerHeartbeatResponsePBImpl _enclosing;
		}

		private ResourceLocalizationSpec ConvertFromProtoFormat(YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto
			 p)
		{
			return new ResourceLocalizationSpecPBImpl(p);
		}

		private YarnServerNodemanagerServiceProtos.LocalizerActionProto ConvertToProtoFormat
			(LocalizerAction a)
		{
			return YarnServerNodemanagerServiceProtos.LocalizerActionProto.ValueOf(a.ToString
				());
		}

		private LocalizerAction ConvertFromProtoFormat(YarnServerNodemanagerServiceProtos.LocalizerActionProto
			 a)
		{
			return LocalizerAction.ValueOf(a.ToString());
		}
	}
}
