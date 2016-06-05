using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class RunSharedCacheCleanerTaskRequestPBImpl : RunSharedCacheCleanerTaskRequest
	{
		internal YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto proto = YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto.Builder builder = 
			null;

		internal bool viaProto = false;

		public RunSharedCacheCleanerTaskRequestPBImpl()
		{
			builder = YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto.NewBuilder();
		}

		public RunSharedCacheCleanerTaskRequestPBImpl(YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}
	}
}
