using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetJobReportResponsePBImpl : ProtoBase<MRServiceProtos.GetJobReportResponseProto
		>, GetJobReportResponse
	{
		internal MRServiceProtos.GetJobReportResponseProto proto = MRServiceProtos.GetJobReportResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetJobReportResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private JobReport jobReport = null;

		public GetJobReportResponsePBImpl()
		{
			builder = MRServiceProtos.GetJobReportResponseProto.NewBuilder();
		}

		public GetJobReportResponsePBImpl(MRServiceProtos.GetJobReportResponseProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetJobReportResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetJobReportResponseProto)builder.Build
				());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.jobReport != null)
			{
				builder.SetJobReport(ConvertToProtoFormat(this.jobReport));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRServiceProtos.GetJobReportResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetJobReportResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual JobReport GetJobReport()
		{
			MRServiceProtos.GetJobReportResponseProtoOrBuilder p = viaProto ? proto : builder;
			if (this.jobReport != null)
			{
				return this.jobReport;
			}
			if (!p.HasJobReport())
			{
				return null;
			}
			this.jobReport = ConvertFromProtoFormat(p.GetJobReport());
			return this.jobReport;
		}

		public virtual void SetJobReport(JobReport jobReport)
		{
			MaybeInitBuilder();
			if (jobReport == null)
			{
				builder.ClearJobReport();
			}
			this.jobReport = jobReport;
		}

		private JobReportPBImpl ConvertFromProtoFormat(MRProtos.JobReportProto p)
		{
			return new JobReportPBImpl(p);
		}

		private MRProtos.JobReportProto ConvertToProtoFormat(JobReport t)
		{
			return ((JobReportPBImpl)t).GetProto();
		}
	}
}
