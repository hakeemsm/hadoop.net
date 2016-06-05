using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetDelegationTokenResponsePBImpl : GetDelegationTokenResponse
	{
		internal Token appToken;

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

		public override Token GetRMDelegationToken()
		{
			SecurityProtos.GetDelegationTokenResponseProtoOrBuilder p = viaProto ? proto : builder;
			if (this.appToken != null)
			{
				return this.appToken;
			}
			if (!p.HasToken())
			{
				return null;
			}
			this.appToken = ConvertFromProtoFormat(p.GetToken());
			return this.appToken;
		}

		public override void SetRMDelegationToken(Token appToken)
		{
			MaybeInitBuilder();
			if (appToken == null)
			{
				builder.ClearToken();
			}
			this.appToken = appToken;
		}

		public virtual SecurityProtos.GetDelegationTokenResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((SecurityProtos.GetDelegationTokenResponseProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		public override int GetHashCode()
		{
			return GetProto().GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other == null)
			{
				return false;
			}
			if (other.GetType().IsAssignableFrom(this.GetType()))
			{
				return this.GetProto().Equals(this.GetType().Cast(other).GetProto());
			}
			return false;
		}

		public override string ToString()
		{
			return TextFormat.ShortDebugString(GetProto());
		}

		private void MergeLocalToBuilder()
		{
			if (appToken != null)
			{
				builder.SetToken(ConvertToProtoFormat(this.appToken));
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
