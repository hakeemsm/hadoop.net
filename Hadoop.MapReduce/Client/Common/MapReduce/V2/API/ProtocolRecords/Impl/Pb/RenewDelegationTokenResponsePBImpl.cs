using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class RenewDelegationTokenResponsePBImpl : ProtoBase<SecurityProtos.RenewDelegationTokenResponseProto
		>, RenewDelegationTokenResponse
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

		public override SecurityProtos.RenewDelegationTokenResponseProto GetProto()
		{
			proto = viaProto ? proto : ((SecurityProtos.RenewDelegationTokenResponseProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = SecurityProtos.RenewDelegationTokenResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual long GetNextExpirationTime()
		{
			SecurityProtos.RenewDelegationTokenResponseProtoOrBuilder p = viaProto ? proto : 
				builder;
			return p.GetNewExpiryTime();
		}

		public virtual void SetNextExpirationTime(long expTime)
		{
			MaybeInitBuilder();
			builder.SetNewExpiryTime(expTime);
		}
	}
}
