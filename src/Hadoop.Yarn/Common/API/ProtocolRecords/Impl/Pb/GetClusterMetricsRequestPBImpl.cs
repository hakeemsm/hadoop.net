using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetClusterMetricsRequestPBImpl : GetClusterMetricsRequest
	{
		internal YarnServiceProtos.GetClusterMetricsRequestProto proto = YarnServiceProtos.GetClusterMetricsRequestProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetClusterMetricsRequestProto.Builder builder = null;

		internal bool viaProto = false;

		public GetClusterMetricsRequestPBImpl()
		{
			builder = YarnServiceProtos.GetClusterMetricsRequestProto.NewBuilder();
		}

		public GetClusterMetricsRequestPBImpl(YarnServiceProtos.GetClusterMetricsRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetClusterMetricsRequestProto GetProto()
		{
			proto = viaProto ? proto : ((YarnServiceProtos.GetClusterMetricsRequestProto)builder
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
