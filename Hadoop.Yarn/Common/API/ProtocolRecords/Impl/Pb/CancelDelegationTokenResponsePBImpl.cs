using Com.Google.Protobuf;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class CancelDelegationTokenResponsePBImpl : CancelDelegationTokenResponse
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

		public virtual SecurityProtos.CancelDelegationTokenResponseProto GetProto()
		{
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
	}
}
