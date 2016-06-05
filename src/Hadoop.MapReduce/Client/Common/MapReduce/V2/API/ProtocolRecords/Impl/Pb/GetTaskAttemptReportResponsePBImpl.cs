using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetTaskAttemptReportResponsePBImpl : ProtoBase<MRServiceProtos.GetTaskAttemptReportResponseProto
		>, GetTaskAttemptReportResponse
	{
		internal MRServiceProtos.GetTaskAttemptReportResponseProto proto = MRServiceProtos.GetTaskAttemptReportResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetTaskAttemptReportResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private TaskAttemptReport taskAttemptReport = null;

		public GetTaskAttemptReportResponsePBImpl()
		{
			builder = MRServiceProtos.GetTaskAttemptReportResponseProto.NewBuilder();
		}

		public GetTaskAttemptReportResponsePBImpl(MRServiceProtos.GetTaskAttemptReportResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetTaskAttemptReportResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetTaskAttemptReportResponseProto)builder
				.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.taskAttemptReport != null)
			{
				builder.SetTaskAttemptReport(ConvertToProtoFormat(this.taskAttemptReport));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRServiceProtos.GetTaskAttemptReportResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetTaskAttemptReportResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual TaskAttemptReport GetTaskAttemptReport()
		{
			MRServiceProtos.GetTaskAttemptReportResponseProtoOrBuilder p = viaProto ? proto : 
				builder;
			if (this.taskAttemptReport != null)
			{
				return this.taskAttemptReport;
			}
			if (!p.HasTaskAttemptReport())
			{
				return null;
			}
			this.taskAttemptReport = ConvertFromProtoFormat(p.GetTaskAttemptReport());
			return this.taskAttemptReport;
		}

		public virtual void SetTaskAttemptReport(TaskAttemptReport taskAttemptReport)
		{
			MaybeInitBuilder();
			if (taskAttemptReport == null)
			{
				builder.ClearTaskAttemptReport();
			}
			this.taskAttemptReport = taskAttemptReport;
		}

		private TaskAttemptReportPBImpl ConvertFromProtoFormat(MRProtos.TaskAttemptReportProto
			 p)
		{
			return new TaskAttemptReportPBImpl(p);
		}

		private MRProtos.TaskAttemptReportProto ConvertToProtoFormat(TaskAttemptReport t)
		{
			return ((TaskAttemptReportPBImpl)t).GetProto();
		}
	}
}
