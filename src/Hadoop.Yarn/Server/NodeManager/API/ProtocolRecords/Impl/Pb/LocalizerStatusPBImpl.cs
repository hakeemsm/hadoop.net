using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.Impl.PB
{
	public class LocalizerStatusPBImpl : ProtoBase<YarnServerNodemanagerServiceProtos.LocalizerStatusProto
		>, LocalizerStatus
	{
		internal YarnServerNodemanagerServiceProtos.LocalizerStatusProto proto = YarnServerNodemanagerServiceProtos.LocalizerStatusProto
			.GetDefaultInstance();

		internal YarnServerNodemanagerServiceProtos.LocalizerStatusProto.Builder builder = 
			null;

		internal bool viaProto = false;

		private IList<LocalResourceStatus> resources = null;

		public LocalizerStatusPBImpl()
		{
			builder = YarnServerNodemanagerServiceProtos.LocalizerStatusProto.NewBuilder();
		}

		public LocalizerStatusPBImpl(YarnServerNodemanagerServiceProtos.LocalizerStatusProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override YarnServerNodemanagerServiceProtos.LocalizerStatusProto GetProto(
			)
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerNodemanagerServiceProtos.LocalizerStatusProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.resources != null)
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
			proto = ((YarnServerNodemanagerServiceProtos.LocalizerStatusProto)builder.Build()
				);
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerNodemanagerServiceProtos.LocalizerStatusProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		public virtual string GetLocalizerId()
		{
			YarnServerNodemanagerServiceProtos.LocalizerStatusProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (!p.HasLocalizerId())
			{
				return null;
			}
			return (p.GetLocalizerId());
		}

		public virtual IList<LocalResourceStatus> GetResources()
		{
			InitResources();
			return this.resources;
		}

		public virtual void SetLocalizerId(string localizerId)
		{
			MaybeInitBuilder();
			if (localizerId == null)
			{
				builder.ClearLocalizerId();
				return;
			}
			builder.SetLocalizerId(localizerId);
		}

		private void InitResources()
		{
			if (this.resources != null)
			{
				return;
			}
			YarnServerNodemanagerServiceProtos.LocalizerStatusProtoOrBuilder p = viaProto ? proto
				 : builder;
			IList<YarnServerNodemanagerServiceProtos.LocalResourceStatusProto> list = p.GetResourcesList
				();
			this.resources = new AList<LocalResourceStatus>();
			foreach (YarnServerNodemanagerServiceProtos.LocalResourceStatusProto c in list)
			{
				this.resources.AddItem(ConvertFromProtoFormat(c));
			}
		}

		private void AddResourcesToProto()
		{
			MaybeInitBuilder();
			builder.ClearResources();
			if (this.resources == null)
			{
				return;
			}
			IEnumerable<YarnServerNodemanagerServiceProtos.LocalResourceStatusProto> iterable
				 = new _IEnumerable_122(this);
			builder.AddAllResources(iterable);
		}

		private sealed class _IEnumerable_122 : IEnumerable<YarnServerNodemanagerServiceProtos.LocalResourceStatusProto
			>
		{
			public _IEnumerable_122(LocalizerStatusPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnServerNodemanagerServiceProtos.LocalResourceStatusProto
				> GetEnumerator()
			{
				return new _IEnumerator_125(this);
			}

			private sealed class _IEnumerator_125 : IEnumerator<YarnServerNodemanagerServiceProtos.LocalResourceStatusProto
				>
			{
				public _IEnumerator_125(_IEnumerable_122 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.resources.GetEnumerator();
				}

				internal IEnumerator<LocalResourceStatus> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnServerNodemanagerServiceProtos.LocalResourceStatusProto Next(
					)
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_122 _enclosing;
			}

			private readonly LocalizerStatusPBImpl _enclosing;
		}

		public virtual void AddAllResources(IList<LocalResourceStatus> resources)
		{
			if (resources == null)
			{
				return;
			}
			InitResources();
			Sharpen.Collections.AddAll(this.resources, resources);
		}

		public virtual LocalResourceStatus GetResourceStatus(int index)
		{
			InitResources();
			return this.resources[index];
		}

		public virtual void AddResourceStatus(LocalResourceStatus resource)
		{
			InitResources();
			this.resources.AddItem(resource);
		}

		public virtual void RemoveResource(int index)
		{
			InitResources();
			this.resources.Remove(index);
		}

		public virtual void ClearResources()
		{
			InitResources();
			this.resources.Clear();
		}

		private LocalResourceStatus ConvertFromProtoFormat(YarnServerNodemanagerServiceProtos.LocalResourceStatusProto
			 p)
		{
			return new LocalResourceStatusPBImpl(p);
		}

		private YarnServerNodemanagerServiceProtos.LocalResourceStatusProto ConvertToProtoFormat
			(LocalResourceStatus s)
		{
			return ((LocalResourceStatusPBImpl)s).GetProto();
		}
	}
}
