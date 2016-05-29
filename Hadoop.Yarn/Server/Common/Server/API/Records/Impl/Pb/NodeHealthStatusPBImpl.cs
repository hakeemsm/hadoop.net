using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB
{
	public class NodeHealthStatusPBImpl : NodeHealthStatus
	{
		private YarnServerCommonProtos.NodeHealthStatusProto.Builder builder;

		private bool viaProto = false;

		private YarnServerCommonProtos.NodeHealthStatusProto proto = YarnServerCommonProtos.NodeHealthStatusProto
			.GetDefaultInstance();

		public NodeHealthStatusPBImpl()
		{
			this.builder = YarnServerCommonProtos.NodeHealthStatusProto.NewBuilder();
		}

		public NodeHealthStatusPBImpl(YarnServerCommonProtos.NodeHealthStatusProto proto)
		{
			this.proto = proto;
			this.viaProto = true;
		}

		public virtual YarnServerCommonProtos.NodeHealthStatusProto GetProto()
		{
			MergeLocalToProto();
			this.proto = this.viaProto ? this.proto : ((YarnServerCommonProtos.NodeHealthStatusProto
				)this.builder.Build());
			this.viaProto = true;
			return this.proto;
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

		private void MergeLocalToProto()
		{
			if (this.viaProto)
			{
				MaybeInitBuilder();
			}
			this.proto = ((YarnServerCommonProtos.NodeHealthStatusProto)this.builder.Build());
			this.viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (this.viaProto || this.builder == null)
			{
				this.builder = YarnServerCommonProtos.NodeHealthStatusProto.NewBuilder(this.proto
					);
			}
			this.viaProto = false;
		}

		public override bool GetIsNodeHealthy()
		{
			YarnServerCommonProtos.NodeHealthStatusProtoOrBuilder p = this.viaProto ? this.proto
				 : this.builder;
			return p.GetIsNodeHealthy();
		}

		public override void SetIsNodeHealthy(bool isNodeHealthy)
		{
			MaybeInitBuilder();
			this.builder.SetIsNodeHealthy(isNodeHealthy);
		}

		public override string GetHealthReport()
		{
			YarnServerCommonProtos.NodeHealthStatusProtoOrBuilder p = this.viaProto ? this.proto
				 : this.builder;
			if (!p.HasHealthReport())
			{
				return null;
			}
			return (p.GetHealthReport());
		}

		public override void SetHealthReport(string healthReport)
		{
			MaybeInitBuilder();
			if (healthReport == null)
			{
				this.builder.ClearHealthReport();
				return;
			}
			this.builder.SetHealthReport((healthReport));
		}

		public override long GetLastHealthReportTime()
		{
			YarnServerCommonProtos.NodeHealthStatusProtoOrBuilder p = this.viaProto ? this.proto
				 : this.builder;
			return (p.GetLastHealthReportTime());
		}

		public override void SetLastHealthReportTime(long lastHealthReport)
		{
			MaybeInitBuilder();
			this.builder.SetLastHealthReportTime((lastHealthReport));
		}
	}
}
