using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetQueueUserAclsInfoRequestPBImpl : GetQueueUserAclsInfoRequest
	{
		internal YarnServiceProtos.GetQueueUserAclsInfoRequestProto proto = YarnServiceProtos.GetQueueUserAclsInfoRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetQueueUserAclsInfoRequestProto.Builder builder = null;

		internal bool viaProto = false;

		public GetQueueUserAclsInfoRequestPBImpl()
		{
			builder = YarnServiceProtos.GetQueueUserAclsInfoRequestProto.NewBuilder();
		}

		public GetQueueUserAclsInfoRequestPBImpl(YarnServiceProtos.GetQueueUserAclsInfoRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetQueueUserAclsInfoRequestProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.GetQueueUserAclsInfoRequestProto)builder
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
	}
}
