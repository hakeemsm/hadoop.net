using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class UseSharedCacheResourceRequestPBImpl : UseSharedCacheResourceRequest
	{
		internal YarnServiceProtos.UseSharedCacheResourceRequestProto proto = YarnServiceProtos.UseSharedCacheResourceRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.UseSharedCacheResourceRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private ApplicationId applicationId = null;

		public UseSharedCacheResourceRequestPBImpl()
		{
			builder = YarnServiceProtos.UseSharedCacheResourceRequestProto.NewBuilder();
		}

		public UseSharedCacheResourceRequestPBImpl(YarnServiceProtos.UseSharedCacheResourceRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.UseSharedCacheResourceRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.UseSharedCacheResourceRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override ApplicationId GetAppId()
		{
			YarnServiceProtos.UseSharedCacheResourceRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
			if (this.applicationId != null)
			{
				return this.applicationId;
			}
			if (!p.HasApplicationId())
			{
				return null;
			}
			this.applicationId = ConvertFromProtoFormat(p.GetApplicationId());
			return this.applicationId;
		}

		public override void SetAppId(ApplicationId id)
		{
			MaybeInitBuilder();
			if (id == null)
			{
				builder.ClearApplicationId();
			}
			this.applicationId = id;
		}

		public override string GetResourceKey()
		{
			YarnServiceProtos.UseSharedCacheResourceRequestProtoOrBuilder p = viaProto ? proto
				 : builder;
			return (p.HasResourceKey()) ? p.GetResourceKey() : null;
		}

		public override void SetResourceKey(string key)
		{
			MaybeInitBuilder();
			if (key == null)
			{
				builder.ClearResourceKey();
				return;
			}
			builder.SetResourceKey(key);
		}

		private void MergeLocalToBuilder()
		{
			if (applicationId != null)
			{
				builder.SetApplicationId(ConvertToProtoFormat(this.applicationId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.UseSharedCacheResourceRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.UseSharedCacheResourceRequestProto.NewBuilder(proto);
			}
			viaProto = false;
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
	}
}
