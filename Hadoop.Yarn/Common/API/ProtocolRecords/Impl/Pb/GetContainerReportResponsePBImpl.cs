using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class GetContainerReportResponsePBImpl : GetContainerReportResponse
	{
		internal YarnServiceProtos.GetContainerReportResponseProto proto = YarnServiceProtos.GetContainerReportResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.GetContainerReportResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private ContainerReport containerReport = null;

		public GetContainerReportResponsePBImpl()
		{
			builder = YarnServiceProtos.GetContainerReportResponseProto.NewBuilder();
		}

		public GetContainerReportResponsePBImpl(YarnServiceProtos.GetContainerReportResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.GetContainerReportResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.GetContainerReportResponseProto)builder
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
			if (this.containerReport != null)
			{
				builder.SetContainerReport(ConvertToProtoFormat(this.containerReport));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.GetContainerReportResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.GetContainerReportResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override ContainerReport GetContainerReport()
		{
			if (this.containerReport != null)
			{
				return this.containerReport;
			}
			YarnServiceProtos.GetContainerReportResponseProtoOrBuilder p = viaProto ? proto : 
				builder;
			if (!p.HasContainerReport())
			{
				return null;
			}
			this.containerReport = ConvertFromProtoFormat(p.GetContainerReport());
			return this.containerReport;
		}

		public override void SetContainerReport(ContainerReport containerReport)
		{
			MaybeInitBuilder();
			if (containerReport == null)
			{
				builder.ClearContainerReport();
			}
			this.containerReport = containerReport;
		}

		private ContainerReportPBImpl ConvertFromProtoFormat(YarnProtos.ContainerReportProto
			 p)
		{
			return new ContainerReportPBImpl(p);
		}

		private YarnProtos.ContainerReportProto ConvertToProtoFormat(ContainerReport t)
		{
			return ((ContainerReportPBImpl)t).GetProto();
		}
	}
}
