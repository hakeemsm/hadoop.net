using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetClusterMetricsResponsePBImpl : GetClusterMetricsResponse
	{
		internal YarnServiceProtos.GetClusterMetricsResponseProto proto = YarnServiceProtos.GetClusterMetricsResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetClusterMetricsResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private YarnClusterMetrics yarnClusterMetrics = null;

		public GetClusterMetricsResponsePBImpl()
		{
			builder = YarnServiceProtos.GetClusterMetricsResponseProto.NewBuilder();
		}

		public GetClusterMetricsResponsePBImpl(YarnServiceProtos.GetClusterMetricsResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetClusterMetricsResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetClusterMetricsResponseProto)builder
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
			if (this.yarnClusterMetrics != null)
			{
				builder.SetClusterMetrics(ConvertToProtoFormat(this.yarnClusterMetrics));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetClusterMetricsResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetClusterMetricsResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override YarnClusterMetrics GetClusterMetrics()
		{
			YarnServiceProtos.GetClusterMetricsResponseProtoOrBuilder p = viaProto ? proto : 
				builder;
			if (this.yarnClusterMetrics != null)
			{
				return this.yarnClusterMetrics;
			}
			if (!p.HasClusterMetrics())
			{
				return null;
			}
			this.yarnClusterMetrics = ConvertFromProtoFormat(p.GetClusterMetrics());
			return this.yarnClusterMetrics;
		}

		public override void SetClusterMetrics(YarnClusterMetrics clusterMetrics)
		{
			MaybeInitBuilder();
			if (clusterMetrics == null)
			{
				builder.ClearClusterMetrics();
			}
			this.yarnClusterMetrics = clusterMetrics;
		}

		private YarnClusterMetricsPBImpl ConvertFromProtoFormat(YarnProtos.YarnClusterMetricsProto
			 p)
		{
			return new YarnClusterMetricsPBImpl(p);
		}

		private YarnProtos.YarnClusterMetricsProto ConvertToProtoFormat(YarnClusterMetrics
			 t)
		{
			return ((YarnClusterMetricsPBImpl)t).GetProto();
		}
	}
}
