using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class UseSharedCacheResourceResponsePBImpl : UseSharedCacheResourceResponse
	{
		internal YarnServiceProtos.UseSharedCacheResourceResponseProto proto = YarnServiceProtos.UseSharedCacheResourceResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.UseSharedCacheResourceResponseProto.Builder builder = 
			null;

		internal bool viaProto = false;

		public UseSharedCacheResourceResponsePBImpl()
		{
			builder = YarnServiceProtos.UseSharedCacheResourceResponseProto.NewBuilder();
		}

		public UseSharedCacheResourceResponsePBImpl(YarnServiceProtos.UseSharedCacheResourceResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.UseSharedCacheResourceResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.UseSharedCacheResourceResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override string GetPath()
		{
			YarnServiceProtos.UseSharedCacheResourceResponseProtoOrBuilder p = viaProto ? proto
				 : builder;
			return (p.HasPath()) ? p.GetPath() : null;
		}

		public override void SetPath(string path)
		{
			MaybeInitBuilder();
			if (path == null)
			{
				builder.ClearPath();
				return;
			}
			builder.SetPath(path);
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			proto = ((YarnServiceProtos.UseSharedCacheResourceResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.UseSharedCacheResourceResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}
	}
}
