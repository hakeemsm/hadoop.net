using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetTaskReportsRequestPBImpl : ProtoBase<MRServiceProtos.GetTaskReportsRequestProto
		>, GetTaskReportsRequest
	{
		internal MRServiceProtos.GetTaskReportsRequestProto proto = MRServiceProtos.GetTaskReportsRequestProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetTaskReportsRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private JobId jobId = null;

		public GetTaskReportsRequestPBImpl()
		{
			builder = MRServiceProtos.GetTaskReportsRequestProto.NewBuilder();
		}

		public GetTaskReportsRequestPBImpl(MRServiceProtos.GetTaskReportsRequestProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetTaskReportsRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetTaskReportsRequestProto)builder.Build
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
			proto = ((MRServiceProtos.GetTaskReportsRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetTaskReportsRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual JobId GetJobId()
		{
			MRServiceProtos.GetTaskReportsRequestProtoOrBuilder p = viaProto ? proto : builder;
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

		public virtual TaskType GetTaskType()
		{
			MRServiceProtos.GetTaskReportsRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasTaskType())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetTaskType());
		}

		public virtual void SetTaskType(TaskType taskType)
		{
			MaybeInitBuilder();
			if (taskType == null)
			{
				builder.ClearTaskType();
				return;
			}
			builder.SetTaskType(ConvertToProtoFormat(taskType));
		}

		private JobIdPBImpl ConvertFromProtoFormat(MRProtos.JobIdProto p)
		{
			return new JobIdPBImpl(p);
		}

		private MRProtos.JobIdProto ConvertToProtoFormat(JobId t)
		{
			return ((JobIdPBImpl)t).GetProto();
		}

		private MRProtos.TaskTypeProto ConvertToProtoFormat(TaskType e)
		{
			return MRProtoUtils.ConvertToProtoFormat(e);
		}

		private TaskType ConvertFromProtoFormat(MRProtos.TaskTypeProto e)
		{
			return MRProtoUtils.ConvertFromProtoFormat(e);
		}
	}
}
