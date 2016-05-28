using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class TaskAttemptIdPBImpl : TaskAttemptId
	{
		internal MRProtos.TaskAttemptIdProto proto = MRProtos.TaskAttemptIdProto.GetDefaultInstance
			();

		internal MRProtos.TaskAttemptIdProto.Builder builder = null;

		internal bool viaProto = false;

		private TaskId taskId = null;

		public TaskAttemptIdPBImpl()
		{
			builder = MRProtos.TaskAttemptIdProto.NewBuilder();
		}

		public TaskAttemptIdPBImpl(MRProtos.TaskAttemptIdProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual MRProtos.TaskAttemptIdProto GetProto()
		{
			lock (this)
			{
				MergeLocalToProto();
				proto = viaProto ? proto : ((MRProtos.TaskAttemptIdProto)builder.Build());
				viaProto = true;
				return proto;
			}
		}

		private void MergeLocalToBuilder()
		{
			lock (this)
			{
				if (this.taskId != null && !((TaskIdPBImpl)this.taskId).GetProto().Equals(builder
					.GetTaskId()))
				{
					builder.SetTaskId(ConvertToProtoFormat(this.taskId));
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
				proto = ((MRProtos.TaskAttemptIdProto)builder.Build());
				viaProto = true;
			}
		}

		private void MaybeInitBuilder()
		{
			lock (this)
			{
				if (viaProto || builder == null)
				{
					builder = MRProtos.TaskAttemptIdProto.NewBuilder(proto);
				}
				viaProto = false;
			}
		}

		public override int GetId()
		{
			lock (this)
			{
				MRProtos.TaskAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
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

		public override TaskId GetTaskId()
		{
			lock (this)
			{
				MRProtos.TaskAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
				if (this.taskId != null)
				{
					return this.taskId;
				}
				if (!p.HasTaskId())
				{
					return null;
				}
				taskId = ConvertFromProtoFormat(p.GetTaskId());
				return taskId;
			}
		}

		public override void SetTaskId(TaskId taskId)
		{
			lock (this)
			{
				MaybeInitBuilder();
				if (taskId == null)
				{
					builder.ClearTaskId();
				}
				this.taskId = taskId;
			}
		}

		private TaskIdPBImpl ConvertFromProtoFormat(MRProtos.TaskIdProto p)
		{
			return new TaskIdPBImpl(p);
		}

		private MRProtos.TaskIdProto ConvertToProtoFormat(TaskId t)
		{
			return ((TaskIdPBImpl)t).GetProto();
		}
	}
}
