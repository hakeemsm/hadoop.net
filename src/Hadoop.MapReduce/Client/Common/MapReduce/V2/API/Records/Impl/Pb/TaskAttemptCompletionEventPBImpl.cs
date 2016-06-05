using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class TaskAttemptCompletionEventPBImpl : ProtoBase<MRProtos.TaskAttemptCompletionEventProto
		>, TaskAttemptCompletionEvent
	{
		internal MRProtos.TaskAttemptCompletionEventProto proto = MRProtos.TaskAttemptCompletionEventProto
			.GetDefaultInstance();

		internal MRProtos.TaskAttemptCompletionEventProto.Builder builder = null;

		internal bool viaProto = false;

		private TaskAttemptId taskAttemptId = null;

		public TaskAttemptCompletionEventPBImpl()
		{
			builder = MRProtos.TaskAttemptCompletionEventProto.NewBuilder();
		}

		public TaskAttemptCompletionEventPBImpl(MRProtos.TaskAttemptCompletionEventProto 
			proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRProtos.TaskAttemptCompletionEventProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRProtos.TaskAttemptCompletionEventProto)builder.Build
				());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.taskAttemptId != null)
			{
				builder.SetAttemptId(ConvertToProtoFormat(this.taskAttemptId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRProtos.TaskAttemptCompletionEventProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRProtos.TaskAttemptCompletionEventProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual TaskAttemptId GetAttemptId()
		{
			MRProtos.TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
			if (this.taskAttemptId != null)
			{
				return this.taskAttemptId;
			}
			if (!p.HasAttemptId())
			{
				return null;
			}
			this.taskAttemptId = ConvertFromProtoFormat(p.GetAttemptId());
			return this.taskAttemptId;
		}

		public virtual void SetAttemptId(TaskAttemptId attemptId)
		{
			MaybeInitBuilder();
			if (attemptId == null)
			{
				builder.ClearAttemptId();
			}
			this.taskAttemptId = attemptId;
		}

		public virtual TaskAttemptCompletionEventStatus GetStatus()
		{
			MRProtos.TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasStatus())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetStatus());
		}

		public virtual void SetStatus(TaskAttemptCompletionEventStatus status)
		{
			MaybeInitBuilder();
			if (status == null)
			{
				builder.ClearStatus();
				return;
			}
			builder.SetStatus(ConvertToProtoFormat(status));
		}

		public virtual string GetMapOutputServerAddress()
		{
			MRProtos.TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasMapOutputServerAddress())
			{
				return null;
			}
			return (p.GetMapOutputServerAddress());
		}

		public virtual void SetMapOutputServerAddress(string mapOutputServerAddress)
		{
			MaybeInitBuilder();
			if (mapOutputServerAddress == null)
			{
				builder.ClearMapOutputServerAddress();
				return;
			}
			builder.SetMapOutputServerAddress((mapOutputServerAddress));
		}

		public virtual int GetAttemptRunTime()
		{
			MRProtos.TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetAttemptRunTime());
		}

		public virtual void SetAttemptRunTime(int attemptRunTime)
		{
			MaybeInitBuilder();
			builder.SetAttemptRunTime((attemptRunTime));
		}

		public virtual int GetEventId()
		{
			MRProtos.TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetEventId());
		}

		public virtual void SetEventId(int eventId)
		{
			MaybeInitBuilder();
			builder.SetEventId((eventId));
		}

		private TaskAttemptIdPBImpl ConvertFromProtoFormat(MRProtos.TaskAttemptIdProto p)
		{
			return new TaskAttemptIdPBImpl(p);
		}

		private MRProtos.TaskAttemptIdProto ConvertToProtoFormat(TaskAttemptId t)
		{
			return ((TaskAttemptIdPBImpl)t).GetProto();
		}

		private MRProtos.TaskAttemptCompletionEventStatusProto ConvertToProtoFormat(TaskAttemptCompletionEventStatus
			 e)
		{
			return MRProtoUtils.ConvertToProtoFormat(e);
		}

		private TaskAttemptCompletionEventStatus ConvertFromProtoFormat(MRProtos.TaskAttemptCompletionEventStatusProto
			 e)
		{
			return MRProtoUtils.ConvertFromProtoFormat(e);
		}
	}
}
