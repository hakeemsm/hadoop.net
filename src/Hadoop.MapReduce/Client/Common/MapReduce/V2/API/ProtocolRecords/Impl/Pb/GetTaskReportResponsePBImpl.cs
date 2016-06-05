using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetTaskReportResponsePBImpl : ProtoBase<MRServiceProtos.GetTaskReportResponseProto
		>, GetTaskReportResponse
	{
		internal MRServiceProtos.GetTaskReportResponseProto proto = MRServiceProtos.GetTaskReportResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetTaskReportResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private TaskReport taskReport = null;

		public GetTaskReportResponsePBImpl()
		{
			builder = MRServiceProtos.GetTaskReportResponseProto.NewBuilder();
		}

		public GetTaskReportResponsePBImpl(MRServiceProtos.GetTaskReportResponseProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetTaskReportResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetTaskReportResponseProto)builder.Build
				());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.taskReport != null)
			{
				builder.SetTaskReport(ConvertToProtoFormat(this.taskReport));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRServiceProtos.GetTaskReportResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetTaskReportResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual TaskReport GetTaskReport()
		{
			MRServiceProtos.GetTaskReportResponseProtoOrBuilder p = viaProto ? proto : builder;
			if (this.taskReport != null)
			{
				return this.taskReport;
			}
			if (!p.HasTaskReport())
			{
				return null;
			}
			this.taskReport = ConvertFromProtoFormat(p.GetTaskReport());
			return this.taskReport;
		}

		public virtual void SetTaskReport(TaskReport taskReport)
		{
			MaybeInitBuilder();
			if (taskReport == null)
			{
				builder.ClearTaskReport();
			}
			this.taskReport = taskReport;
		}

		private TaskReportPBImpl ConvertFromProtoFormat(MRProtos.TaskReportProto p)
		{
			return new TaskReportPBImpl(p);
		}

		private MRProtos.TaskReportProto ConvertToProtoFormat(TaskReport t)
		{
			return ((TaskReportPBImpl)t).GetProto();
		}
	}
}
