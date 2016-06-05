using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetDelegationTokenResponsePBImpl : ProtoBase<SecurityProtos.GetDelegationTokenResponseProto
		>, GetDelegationTokenResponse
	{
		internal Token mrToken;

		internal SecurityProtos.GetDelegationTokenResponseProto proto = SecurityProtos.GetDelegationTokenResponseProto
			.GetDefaultInstance();

		internal SecurityProtos.GetDelegationTokenResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public GetDelegationTokenResponsePBImpl()
		{
			builder = SecurityProtos.GetDelegationTokenResponseProto.NewBuilder();
		}

		public GetDelegationTokenResponsePBImpl(SecurityProtos.GetDelegationTokenResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual Token GetDelegationToken()
		{
			SecurityProtos.GetDelegationTokenResponseProtoOrBuilder p = viaProto ? proto : builder;
			if (this.mrToken != null)
			{
				return this.mrToken;
			}
			if (!p.HasToken())
			{
				return null;
			}
			this.mrToken = ConvertFromProtoFormat(p.GetToken());
			return this.mrToken;
		}

		public virtual void SetDelegationToken(Token mrToken)
		{
			MaybeInitBuilder();
			if (mrToken == null)
			{
				builder.GetToken();
			}
			this.mrToken = mrToken;
		}

		public override SecurityProtos.GetDelegationTokenResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((SecurityProtos.GetDelegationTokenResponseProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (mrToken != null)
			{
				builder.SetToken(ConvertToProtoFormat(this.mrToken));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((SecurityProtos.GetDelegationTokenResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = SecurityProtos.GetDelegationTokenResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private TokenPBImpl ConvertFromProtoFormat(SecurityProtos.TokenProto p)
		{
			return new TokenPBImpl(p);
		}

		private SecurityProtos.TokenProto ConvertToProtoFormat(Token t)
		{
			return ((TokenPBImpl)t).GetProto();
		}
	}
}
