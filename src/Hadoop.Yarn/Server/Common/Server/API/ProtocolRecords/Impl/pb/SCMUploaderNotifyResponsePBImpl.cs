using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB
{
	public class SCMUploaderNotifyResponsePBImpl : SCMUploaderNotifyResponse
	{
		internal YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto proto = YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto
			.GetDefaultInstance();

		internal YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto.Builder builder
			 = null;

		internal bool viaProto = false;

		public SCMUploaderNotifyResponsePBImpl()
		{
			builder = YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto.NewBuilder
				();
		}

		public SCMUploaderNotifyResponsePBImpl(YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override bool GetAccepted()
		{
			YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			// Default to true, when in doubt just leave the file in the cache
			return (p.HasAccepted()) ? p.GetAccepted() : true;
		}

		public override void SetAccepted(bool b)
		{
			MaybeInitBuilder();
			builder.SetAccepted(b);
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			proto = ((YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto)builder.Build
				());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServerCommonServiceProtos.SCMUploaderNotifyResponseProto.NewBuilder
					(proto);
			}
			viaProto = false;
		}
	}
}
