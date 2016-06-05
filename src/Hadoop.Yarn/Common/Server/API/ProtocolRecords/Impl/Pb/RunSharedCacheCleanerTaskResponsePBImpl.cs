using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class RunSharedCacheCleanerTaskResponsePBImpl : RunSharedCacheCleanerTaskResponse
	{
		internal YarnServiceProtos.RunSharedCacheCleanerTaskResponseProto proto = YarnServiceProtos.RunSharedCacheCleanerTaskResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.RunSharedCacheCleanerTaskResponseProto.Builder builder
			 = null;

		internal bool viaProto = false;

		public RunSharedCacheCleanerTaskResponsePBImpl()
		{
			builder = YarnServiceProtos.RunSharedCacheCleanerTaskResponseProto.NewBuilder();
		}

		public RunSharedCacheCleanerTaskResponsePBImpl(YarnServiceProtos.RunSharedCacheCleanerTaskResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override bool GetAccepted()
		{
			YarnServiceProtos.RunSharedCacheCleanerTaskResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			return (p.HasAccepted()) ? p.GetAccepted() : false;
		}

		public override void SetAccepted(bool b)
		{
			MaybeInitBuilder();
			builder.SetAccepted(b);
		}

		public virtual YarnServiceProtos.RunSharedCacheCleanerTaskResponseProto GetProto(
			)
		{
			proto = viaProto ? proto : ((YarnServiceProtos.RunSharedCacheCleanerTaskResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.RunSharedCacheCleanerTaskResponseProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}
	}
}
