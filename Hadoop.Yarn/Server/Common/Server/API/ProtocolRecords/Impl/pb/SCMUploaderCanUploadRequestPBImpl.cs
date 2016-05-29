using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class SCMUploaderCanUploadRequestPBImpl : SCMUploaderCanUploadRequest
	{
		internal YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto proto = YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto
			.GetDefaultInstance();

		internal YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto.Builder builder
			 = null;

		internal bool viaProto = false;

		public SCMUploaderCanUploadRequestPBImpl()
		{
			builder = YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto.NewBuilder
				();
		}

		public SCMUploaderCanUploadRequestPBImpl(YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override string GetResourceKey()
		{
			YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProtoOrBuilder p = viaProto
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

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			proto = ((YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto)builder.
				Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto.NewBuilder
					(proto);
			}
			viaProto = false;
		}
	}
}
