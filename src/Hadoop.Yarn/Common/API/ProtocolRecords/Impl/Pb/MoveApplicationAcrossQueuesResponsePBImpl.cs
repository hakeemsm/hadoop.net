using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class MoveApplicationAcrossQueuesResponsePBImpl : MoveApplicationAcrossQueuesResponse
	{
		internal YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto proto = YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto.Builder builder
			 = null;

		internal bool viaProto = false;

		public MoveApplicationAcrossQueuesResponsePBImpl()
		{
			builder = YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto.NewBuilder();
		}

		public MoveApplicationAcrossQueuesResponsePBImpl(YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto GetProto
			()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto
				)builder.Build());
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
	}
}
