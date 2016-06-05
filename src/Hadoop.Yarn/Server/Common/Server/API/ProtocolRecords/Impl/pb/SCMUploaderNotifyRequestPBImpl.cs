using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class SCMUploaderNotifyRequestPBImpl : SCMUploaderNotifyRequest
	{
		internal YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto proto = YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto
			.GetDefaultInstance();

		internal YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto.Builder builder
			 = null;

		internal bool viaProto = false;

		public SCMUploaderNotifyRequestPBImpl()
		{
			builder = YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto.NewBuilder(
				);
		}

		public SCMUploaderNotifyRequestPBImpl(YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override string GetResourceKey()
		{
			YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProtoOrBuilder p = viaProto
				 ? proto : builder;
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

		public override string GetFileName()
		{
			YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProtoOrBuilder p = viaProto
				 ? proto : builder;
			return (p.HasFilename()) ? p.GetFilename() : null;
		}

		public override void SetFilename(string filename)
		{
			MaybeInitBuilder();
			if (filename == null)
			{
				builder.ClearFilename();
				return;
			}
			builder.SetFilename(filename);
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			proto = ((YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto)builder.Build
				());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto.NewBuilder(
					proto);
			}
			viaProto = false;
		}
	}
}
