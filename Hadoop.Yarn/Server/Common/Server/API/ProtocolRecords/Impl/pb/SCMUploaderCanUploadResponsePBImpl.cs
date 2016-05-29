using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class SCMUploaderCanUploadResponsePBImpl : SCMUploaderCanUploadResponse
	{
		internal YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto proto = 
			YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto.GetDefaultInstance
			();

		internal YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto.Builder 
			builder = null;

		internal bool viaProto = false;

		public SCMUploaderCanUploadResponsePBImpl()
		{
			builder = YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto.NewBuilder
				();
		}

		public SCMUploaderCanUploadResponsePBImpl(YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override bool GetUploadable()
		{
			YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			// Default to true, when in doubt allow the upload
			return (p.HasUploadable()) ? p.GetUploadable() : true;
		}

		public override void SetUploadable(bool b)
		{
			MaybeInitBuilder();
			builder.SetUploadable(b);
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			proto = ((YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto)builder
				.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto.NewBuilder
					(proto);
			}
			viaProto = false;
		}
	}
}
