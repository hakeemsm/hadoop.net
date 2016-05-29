using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class YarnClusterMetricsPBImpl : YarnClusterMetrics
	{
		internal YarnProtos.YarnClusterMetricsProto proto = YarnProtos.YarnClusterMetricsProto
			.GetDefaultInstance();

		internal YarnProtos.YarnClusterMetricsProto.Builder builder = null;

		internal bool viaProto = false;

		public YarnClusterMetricsPBImpl()
		{
			builder = YarnProtos.YarnClusterMetricsProto.NewBuilder();
		}

		public YarnClusterMetricsPBImpl(YarnProtos.YarnClusterMetricsProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.YarnClusterMetricsProto GetProto()
		{
			proto = viaProto ? proto : ((YarnProtos.YarnClusterMetricsProto)builder.Build());
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
				builder = YarnProtos.YarnClusterMetricsProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override int GetNumNodeManagers()
		{
			YarnProtos.YarnClusterMetricsProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetNumNodeManagers());
		}

		public override void SetNumNodeManagers(int numNodeManagers)
		{
			MaybeInitBuilder();
			builder.SetNumNodeManagers((numNodeManagers));
		}
	}
}
