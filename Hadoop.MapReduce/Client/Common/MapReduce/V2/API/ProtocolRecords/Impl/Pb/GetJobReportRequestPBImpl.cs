using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetJobReportRequestPBImpl : ProtoBase<MRServiceProtos.GetJobReportRequestProto
		>, GetJobReportRequest
	{
		internal MRServiceProtos.GetJobReportRequestProto proto = MRServiceProtos.GetJobReportRequestProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetJobReportRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private JobId jobId = null;

		public GetJobReportRequestPBImpl()
		{
			builder = MRServiceProtos.GetJobReportRequestProto.NewBuilder();
		}

		public GetJobReportRequestPBImpl(MRServiceProtos.GetJobReportRequestProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetJobReportRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetJobReportRequestProto)builder.Build
				());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.jobId != null)
			{
				builder.SetJobId(ConvertToProtoFormat(this.jobId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRServiceProtos.GetJobReportRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetJobReportRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual JobId GetJobId()
		{
			MRServiceProtos.GetJobReportRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.jobId != null)
			{
				return this.jobId;
			}
			if (!p.HasJobId())
			{
				return null;
			}
			this.jobId = ConvertFromProtoFormat(p.GetJobId());
			return this.jobId;
		}

		public virtual void SetJobId(JobId jobId)
		{
			MaybeInitBuilder();
			if (jobId == null)
			{
				builder.ClearJobId();
			}
			this.jobId = jobId;
		}

		private JobIdPBImpl ConvertFromProtoFormat(MRProtos.JobIdProto p)
		{
			return new JobIdPBImpl(p);
		}

		private MRProtos.JobIdProto ConvertToProtoFormat(JobId t)
		{
			return ((JobIdPBImpl)t).GetProto();
		}
	}
}
