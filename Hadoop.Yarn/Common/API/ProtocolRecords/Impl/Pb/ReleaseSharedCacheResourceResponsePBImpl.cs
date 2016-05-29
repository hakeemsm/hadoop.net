using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class ReleaseSharedCacheResourceResponsePBImpl : ReleaseSharedCacheResourceResponse
	{
		internal YarnServiceProtos.ReleaseSharedCacheResourceResponseProto proto = YarnServiceProtos.ReleaseSharedCacheResourceResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.ReleaseSharedCacheResourceResponseProto.Builder builder
			 = null;

		internal bool viaProto = false;

		public ReleaseSharedCacheResourceResponsePBImpl()
		{
			builder = YarnServiceProtos.ReleaseSharedCacheResourceResponseProto.NewBuilder();
		}

		public ReleaseSharedCacheResourceResponsePBImpl(YarnServiceProtos.ReleaseSharedCacheResourceResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.ReleaseSharedCacheResourceResponseProto GetProto
			()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.ReleaseSharedCacheResourceResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.ReleaseSharedCacheResourceResponseProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}
	}
}
