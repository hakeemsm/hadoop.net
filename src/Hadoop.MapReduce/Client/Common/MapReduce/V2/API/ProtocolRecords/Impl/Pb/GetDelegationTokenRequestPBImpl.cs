using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetDelegationTokenRequestPBImpl : ProtoBase<SecurityProtos.GetDelegationTokenRequestProto
		>, GetDelegationTokenRequest
	{
		internal string renewer;

		internal SecurityProtos.GetDelegationTokenRequestProto proto = SecurityProtos.GetDelegationTokenRequestProto
			.GetDefaultInstance();

		internal SecurityProtos.GetDelegationTokenRequestProto.Builder builder = null;

		internal bool viaProto = false;

		public GetDelegationTokenRequestPBImpl()
		{
			builder = SecurityProtos.GetDelegationTokenRequestProto.NewBuilder();
		}

		public GetDelegationTokenRequestPBImpl(SecurityProtos.GetDelegationTokenRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual string GetRenewer()
		{
			SecurityProtos.GetDelegationTokenRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.renewer != null)
			{
				return this.renewer;
			}
			this.renewer = p.GetRenewer();
			return this.renewer;
		}

		public virtual void SetRenewer(string renewer)
		{
			MaybeInitBuilder();
			if (renewer == null)
			{
				builder.ClearRenewer();
			}
			this.renewer = renewer;
		}

		public override SecurityProtos.GetDelegationTokenRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((SecurityProtos.GetDelegationTokenRequestProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (renewer != null)
			{
				builder.SetRenewer(this.renewer);
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((SecurityProtos.GetDelegationTokenRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = SecurityProtos.GetDelegationTokenRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}
	}
}
