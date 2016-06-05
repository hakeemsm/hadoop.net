using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class LocalResourcePBImpl : LocalResource
	{
		internal YarnProtos.LocalResourceProto proto = YarnProtos.LocalResourceProto.GetDefaultInstance
			();

		internal YarnProtos.LocalResourceProto.Builder builder = null;

		internal bool viaProto = false;

		private URL url = null;

		public LocalResourcePBImpl()
		{
			builder = YarnProtos.LocalResourceProto.NewBuilder();
		}

		public LocalResourcePBImpl(YarnProtos.LocalResourceProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.LocalResourceProto GetProto()
		{
			lock (this)
			{
				MergeLocalToBuilder();
				proto = viaProto ? proto : ((YarnProtos.LocalResourceProto)builder.Build());
				viaProto = true;
				return proto;
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

		private void MergeLocalToBuilder()
		{
			lock (this)
			{
				YarnProtos.LocalResourceProtoOrBuilder l = viaProto ? proto : builder;
				if (this.url != null && !(l.GetResource().Equals(((URLPBImpl)url).GetProto())))
				{
					MaybeInitBuilder();
					l = builder;
					builder.SetResource(ConvertToProtoFormat(this.url));
				}
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = YarnProtos.LocalResourceProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public override long GetSize()
		{
			lock (this)
			{
				YarnProtos.LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetSize());
			}
		}

		public override void SetSize(long size)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetSize((size));
			}
		}

		public override long GetTimestamp()
		{
			lock (this)
			{
				YarnProtos.LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetTimestamp());
			}
		}

		public override void SetTimestamp(long timestamp)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetTimestamp((timestamp));
			}
		}

		public override LocalResourceType GetType()
		{
			lock (this)
			{
				YarnProtos.LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasType())
				{
					return null;
				}
				return ConvertFromProtoFormat(p.GetType());
			}
		}

		public override void SetType(LocalResourceType type)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (type == null)
				{
					builder.ClearType();
					return;
				}
				builder.SetType(ConvertToProtoFormat(type));
			}
		}

		public override URL GetResource()
		{
			lock (this)
			{
				YarnProtos.LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
				if (this.url != null)
				{
					return this.url;
				}
				if (!p.HasResource())
				{
					return null;
				}
				this.url = ConvertFromProtoFormat(p.GetResource());
				return this.url;
			}
		}

		public override void SetResource(URL resource)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (resource == null)
				{
					builder.ClearResource();
				}
				this.url = resource;
			}
		}

		public override LocalResourceVisibility GetVisibility()
		{
			lock (this)
			{
				YarnProtos.LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasVisibility())
				{
					return null;
				}
				return ConvertFromProtoFormat(p.GetVisibility());
			}
		}

		public override void SetVisibility(LocalResourceVisibility visibility)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (visibility == null)
				{
					builder.ClearVisibility();
					return;
				}
				builder.SetVisibility(ConvertToProtoFormat(visibility));
			}
		}

		public override string GetPattern()
		{
			lock (this)
			{
				YarnProtos.LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasPattern())
				{
					return null;
				}
				return p.GetPattern();
			}
		}

		public override void SetPattern(string pattern)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (pattern == null)
				{
					builder.ClearPattern();
					return;
				}
				builder.SetPattern(pattern);
			}
		}

		public override bool GetShouldBeUploadedToSharedCache()
		{
			lock (this)
			{
				YarnProtos.LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasShouldBeUploadedToSharedCache())
				{
					return false;
				}
				return p.GetShouldBeUploadedToSharedCache();
			}
		}

		public override void SetShouldBeUploadedToSharedCache(bool shouldBeUploadedToSharedCache
			)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (!shouldBeUploadedToSharedCache)
				{
					builder.ClearShouldBeUploadedToSharedCache();
					return;
				}
				builder.SetShouldBeUploadedToSharedCache(shouldBeUploadedToSharedCache);
			}
		}

		private YarnProtos.LocalResourceTypeProto ConvertToProtoFormat(LocalResourceType 
			e)
		{
			return ProtoUtils.ConvertToProtoFormat(e);
		}

		private LocalResourceType ConvertFromProtoFormat(YarnProtos.LocalResourceTypeProto
			 e)
		{
			return ProtoUtils.ConvertFromProtoFormat(e);
		}

		private URLPBImpl ConvertFromProtoFormat(YarnProtos.URLProto p)
		{
			return new URLPBImpl(p);
		}

		private YarnProtos.URLProto ConvertToProtoFormat(URL t)
		{
			return ((URLPBImpl)t).GetProto();
		}

		private YarnProtos.LocalResourceVisibilityProto ConvertToProtoFormat(LocalResourceVisibility
			 e)
		{
			return ProtoUtils.ConvertToProtoFormat(e);
		}

		private LocalResourceVisibility ConvertFromProtoFormat(YarnProtos.LocalResourceVisibilityProto
			 e)
		{
			return ProtoUtils.ConvertFromProtoFormat(e);
		}
	}
}
