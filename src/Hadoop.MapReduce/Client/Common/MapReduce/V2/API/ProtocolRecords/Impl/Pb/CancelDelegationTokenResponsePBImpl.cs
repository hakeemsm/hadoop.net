using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class CancelDelegationTokenResponsePBImpl : ProtoBase<SecurityProtos.CancelDelegationTokenResponseProto
		>, CancelDelegationTokenResponse
	{
		internal SecurityProtos.CancelDelegationTokenResponseProto proto = SecurityProtos.CancelDelegationTokenResponseProto
			.GetDefaultInstance();

		public CancelDelegationTokenResponsePBImpl()
		{
		}

		public CancelDelegationTokenResponsePBImpl(SecurityProtos.CancelDelegationTokenResponseProto
			 proto)
		{
			this.proto = proto;
		}

		public override SecurityProtos.CancelDelegationTokenResponseProto GetProto()
		{
			return proto;
		}
	}
}
