using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.Impl.PB
{
	public class LocalResourceStatusPBImpl : ProtoBase<YarnServerNodemanagerServiceProtos.LocalResourceStatusProto
		>, LocalResourceStatus
	{
		internal YarnServerNodemanagerServiceProtos.LocalResourceStatusProto proto = YarnServerNodemanagerServiceProtos.LocalResourceStatusProto
			.GetDefaultInstance();

		internal YarnServerNodemanagerServiceProtos.LocalResourceStatusProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private LocalResource resource;

		private URL localPath;

		private SerializedException exception;

		public LocalResourceStatusPBImpl()
		{
			builder = YarnServerNodemanagerServiceProtos.LocalResourceStatusProto.NewBuilder(
				);
		}

		public LocalResourceStatusPBImpl(YarnServerNodemanagerServiceProtos.LocalResourceStatusProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override YarnServerNodemanagerServiceProtos.LocalResourceStatusProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerNodemanagerServiceProtos.LocalResourceStatusProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.resource != null && !((LocalResourcePBImpl)this.resource).GetProto().Equals
				(builder.GetResource()))
			{
				builder.SetResource(ConvertToProtoFormat(this.resource));
			}
			if (this.localPath != null && !((URLPBImpl)this.localPath).GetProto().Equals(builder
				.GetLocalPath()))
			{
				builder.SetLocalPath(ConvertToProtoFormat(this.localPath));
			}
			if (this.exception != null && !((SerializedExceptionPBImpl)this.exception).GetProto
				().Equals(builder.GetException()))
			{
				builder.SetException(ConvertToProtoFormat(this.exception));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServerNodemanagerServiceProtos.LocalResourceStatusProto)builder.Build
				());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerNodemanagerServiceProtos.LocalResourceStatusProto.NewBuilder(
					proto);
			}
			viaProto = false;
		}

		public virtual LocalResource GetResource()
		{
			YarnServerNodemanagerServiceProtos.LocalResourceStatusProtoOrBuilder p = viaProto
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

		public virtual ResourceStatusType GetStatus()
		{
			YarnServerNodemanagerServiceProtos.LocalResourceStatusProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (!p.HasStatus())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetStatus());
		}

		public virtual URL GetLocalPath()
		{
			YarnServerNodemanagerServiceProtos.LocalResourceStatusProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (this.localPath != null)
			{
				return this.localPath;
			}
			if (!p.HasLocalPath())
			{
				return null;
			}
			this.localPath = ConvertFromProtoFormat(p.GetLocalPath());
			return this.localPath;
		}

		public virtual long GetLocalSize()
		{
			YarnServerNodemanagerServiceProtos.LocalResourceStatusProtoOrBuilder p = viaProto
				 ? proto : builder;
			return (p.GetLocalSize());
		}

		public virtual SerializedException GetException()
		{
			YarnServerNodemanagerServiceProtos.LocalResourceStatusProtoOrBuilder p = viaProto
				 ? proto : builder;
			if (this.exception != null)
			{
				return this.exception;
			}
			if (!p.HasException())
			{
				return null;
			}
			this.exception = ConvertFromProtoFormat(p.GetException());
			return this.exception;
		}

		public virtual void SetResource(LocalResource resource)
		{
			MaybeInitBuilder();
			if (resource == null)
			{
				builder.ClearResource();
			}
			this.resource = resource;
		}

		public virtual void SetStatus(ResourceStatusType status)
		{
			MaybeInitBuilder();
			if (status == null)
			{
				builder.ClearStatus();
				return;
			}
			builder.SetStatus(ConvertToProtoFormat(status));
		}

		public virtual void SetLocalPath(URL localPath)
		{
			MaybeInitBuilder();
			if (localPath == null)
			{
				builder.ClearLocalPath();
			}
			this.localPath = localPath;
		}

		public virtual void SetLocalSize(long size)
		{
			MaybeInitBuilder();
			builder.SetLocalSize(size);
		}

		public virtual void SetException(SerializedException exception)
		{
			MaybeInitBuilder();
			if (exception == null)
			{
				builder.ClearException();
			}
			this.exception = exception;
		}

		private YarnProtos.LocalResourceProto ConvertToProtoFormat(LocalResource rsrc)
		{
			return ((LocalResourcePBImpl)rsrc).GetProto();
		}

		private LocalResourcePBImpl ConvertFromProtoFormat(YarnProtos.LocalResourceProto 
			rsrc)
		{
			return new LocalResourcePBImpl(rsrc);
		}

		private URLPBImpl ConvertFromProtoFormat(YarnProtos.URLProto p)
		{
			return new URLPBImpl(p);
		}

		private YarnProtos.URLProto ConvertToProtoFormat(URL t)
		{
			return ((URLPBImpl)t).GetProto();
		}

		private YarnServerNodemanagerServiceProtos.ResourceStatusTypeProto ConvertToProtoFormat
			(ResourceStatusType e)
		{
			return YarnServerNodemanagerServiceProtos.ResourceStatusTypeProto.ValueOf(e.ToString
				());
		}

		private ResourceStatusType ConvertFromProtoFormat(YarnServerNodemanagerServiceProtos.ResourceStatusTypeProto
			 e)
		{
			return ResourceStatusType.ValueOf(e.ToString());
		}

		private SerializedExceptionPBImpl ConvertFromProtoFormat(YarnProtos.SerializedExceptionProto
			 p)
		{
			return new SerializedExceptionPBImpl(p);
		}

		private YarnProtos.SerializedExceptionProto ConvertToProtoFormat(SerializedException
			 t)
		{
			return ((SerializedExceptionPBImpl)t).GetProto();
		}
	}
}
