using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Impl.PB
{
	public class ResourceLocalizationSpecPBImpl : ProtoBase<YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto
		>, ResourceLocalizationSpec
	{
		private YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto proto = 
			YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto.GetDefaultInstance
			();

		private YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto.Builder 
			builder = null;

		private bool viaProto;

		private LocalResource resource = null;

		private URL destinationDirectory = null;

		public ResourceLocalizationSpecPBImpl()
		{
			builder = YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto.NewBuilder
				();
		}

		public ResourceLocalizationSpecPBImpl(YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual LocalResource GetResource()
		{
			YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (resource != null)
			{
				return resource;
			}
			if (!p.HasResource())
			{
				return null;
			}
			resource = new LocalResourcePBImpl(p.GetResource());
			return resource;
		}

		public virtual void SetResource(LocalResource rsrc)
		{
			MaybeInitBuilder();
			resource = rsrc;
		}

		public virtual URL GetDestinationDirectory()
		{
			YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (destinationDirectory != null)
			{
				return destinationDirectory;
			}
			if (!p.HasDestinationDirectory())
			{
				return null;
			}
			destinationDirectory = new URLPBImpl(p.GetDestinationDirectory());
			return destinationDirectory;
		}

		public virtual void SetDestinationDirectory(URL destinationDirectory)
		{
			MaybeInitBuilder();
			this.destinationDirectory = destinationDirectory;
		}

		public override YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto 
			GetProto()
		{
			MergeLocalToBuilder();
			proto = viaProto ? proto : ((YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (builder == null || viaProto)
				{
					builder = YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto.NewBuilder
						(proto);
				}
				viaProto = false;
			}
		}

		private void MergeLocalToBuilder()
		{
			YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProtoOrBuilder l = viaProto
				 ? proto : builder;
			if (this.resource != null && !(l.GetResource().Equals(((LocalResourcePBImpl)resource
				).GetProto())))
			{
				MaybeInitBuilder();
				builder.SetResource(((LocalResourcePBImpl)resource).GetProto());
			}
			if (this.destinationDirectory != null && !(l.GetDestinationDirectory().Equals(((URLPBImpl
				)destinationDirectory).GetProto())))
			{
				MaybeInitBuilder();
				builder.SetDestinationDirectory(((URLPBImpl)destinationDirectory).GetProto());
			}
		}
	}
}
