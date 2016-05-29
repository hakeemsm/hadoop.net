using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class ReleaseSharedCacheResourceRequestPBImpl : ReleaseSharedCacheResourceRequest
	{
		internal YarnServiceProtos.ReleaseSharedCacheResourceRequestProto proto = YarnServiceProtos.ReleaseSharedCacheResourceRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.ReleaseSharedCacheResourceRequestProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private ApplicationId applicationId = null;

		public ReleaseSharedCacheResourceRequestPBImpl()
		{
			builder = YarnServiceProtos.ReleaseSharedCacheResourceRequestProto.NewBuilder();
		}

		public ReleaseSharedCacheResourceRequestPBImpl(YarnServiceProtos.ReleaseSharedCacheResourceRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.ReleaseSharedCacheResourceRequestProto GetProto(
			)
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.ReleaseSharedCacheResourceRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override ApplicationId GetAppId()
		{
			YarnServiceProtos.ReleaseSharedCacheResourceRequestProtoOrBuilder p = viaProto ? 
				proto : builder;
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
			YarnServiceProtos.ReleaseSharedCacheResourceRequestProtoOrBuilder p = viaProto ? 
				proto : builder;
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
			proto = ((YarnServiceProtos.ReleaseSharedCacheResourceRequestProto)builder.Build(
				));
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.ReleaseSharedCacheResourceRequestProto.NewBuilder(proto
					);
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
