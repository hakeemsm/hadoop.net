using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class RenewDelegationTokenResponsePBImpl : RenewDelegationTokenResponse
	{
		internal SecurityProtos.RenewDelegationTokenResponseProto proto = SecurityProtos.RenewDelegationTokenResponseProto
			.GetDefaultInstance();

		internal SecurityProtos.RenewDelegationTokenResponseProto.Builder builder = null;

		internal bool viaProto = false;

		public RenewDelegationTokenResponsePBImpl()
		{
			this.builder = SecurityProtos.RenewDelegationTokenResponseProto.NewBuilder();
		}

		public RenewDelegationTokenResponsePBImpl(SecurityProtos.RenewDelegationTokenResponseProto
			 proto)
		{
			this.proto = proto;
			this.viaProto = true;
		}

		public virtual SecurityProtos.RenewDelegationTokenResponseProto GetProto()
		{
			proto = viaProto ? proto : ((SecurityProtos.RenewDelegationTokenResponseProto)builder
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

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = SecurityProtos.RenewDelegationTokenResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override long GetNextExpirationTime()
		{
			SecurityProtos.RenewDelegationTokenResponseProtoOrBuilder p = viaProto ? proto : 
				builder;
			return p.GetNewExpiryTime();
		}

		public override void SetNextExpirationTime(long expTime)
		{
			MaybeInitBuilder();
			builder.SetNewExpiryTime(expTime);
		}
	}
}
