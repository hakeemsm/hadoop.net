using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class TaskIdPBImpl : TaskId
	{
		internal MRProtos.TaskIdProto proto = MRProtos.TaskIdProto.GetDefaultInstance();

		internal MRProtos.TaskIdProto.Builder builder = null;

		internal bool viaProto = false;

		private JobId jobId = null;

		public TaskIdPBImpl()
		{
			builder = MRProtos.TaskIdProto.NewBuilder(proto);
		}

		public TaskIdPBImpl(MRProtos.TaskIdProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual MRProtos.TaskIdProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((MRProtos.TaskIdProto)builder.Build());
				viaProto = true;
				return proto;
			}
		}

		private void MergeLocalToBuilder()
		{
			lock (this)
			{
				if (this.jobId != null && !((JobIdPBImpl)this.jobId).GetProto().Equals(builder.GetJobId
					()))
				{
					builder.SetJobId(ConvertToProtoFormat(this.jobId));
				}
			}
		}

		private void MergeLocalToProto()
		{
			lock (this)
			{
				if (viaProto)
				{
					MaybeInitBuilder();
				}
				MergeLocalToBuilder();
				proto = ((MRProtos.TaskIdProto)builder.Build());
				viaProto = true;
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = MRProtos.TaskIdProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public override int GetId()
		{
			lock (this)
			{
				MRProtos.TaskIdProtoOrBuilder p = viaProto ? proto : builder;
				return (p.GetId());
			}
		}

		public override void SetId(int id)
		{
			lock (this)
			{
				MaybeInitBuilder();
				builder.SetId((id));
			}
		}

		public override JobId GetJobId()
		{
			lock (this)
			{
				MRProtos.TaskIdProtoOrBuilder p = viaProto ? proto : builder;
				if (this.jobId != null)
				{
					return this.jobId;
				}
				if (!p.HasJobId())
				{
					return null;
				}
				jobId = ConvertFromProtoFormat(p.GetJobId());
				return jobId;
			}
		}

		public override void SetJobId(JobId jobId)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (jobId == null)
				{
					builder.ClearJobId();
				}
				this.jobId = jobId;
			}
		}

		public override TaskType GetTaskType()
		{
			lock (this)
			{
				MRProtos.TaskIdProtoOrBuilder p = viaProto ? proto : builder;
				if (!p.HasTaskType())
				{
					return null;
				}
				return ConvertFromProtoFormat(p.GetTaskType());
			}
		}

		public override void SetTaskType(TaskType taskType)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (taskType == null)
				{
					builder.ClearTaskType();
					return;
				}
				builder.SetTaskType(ConvertToProtoFormat(taskType));
			}
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
