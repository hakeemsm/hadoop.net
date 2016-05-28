using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class KillTaskRequestPBImpl : ProtoBase<MRServiceProtos.KillTaskRequestProto
		>, KillTaskRequest
	{
		internal MRServiceProtos.KillTaskRequestProto proto = MRServiceProtos.KillTaskRequestProto
			.GetDefaultInstance();

		internal MRServiceProtos.KillTaskRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private TaskId taskId = null;

		public KillTaskRequestPBImpl()
		{
			builder = MRServiceProtos.KillTaskRequestProto.NewBuilder();
		}

		public KillTaskRequestPBImpl(MRServiceProtos.KillTaskRequestProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.KillTaskRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.KillTaskRequestProto)builder.Build()
				);
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.taskId != null)
			{
				builder.SetTaskId(ConvertToProtoFormat(this.taskId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRServiceProtos.KillTaskRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.KillTaskRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual TaskId GetTaskId()
		{
			MRServiceProtos.KillTaskRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.taskId != null)
			{
				return this.taskId;
			}
			if (!p.HasTaskId())
			{
				return null;
			}
			this.taskId = ConvertFromProtoFormat(p.GetTaskId());
			return this.taskId;
		}

		public virtual void SetTaskId(TaskId taskId)
		{
			MaybeInitBuilder();
			if (taskId == null)
			{
				builder.ClearTaskId();
			}
			this.taskId = taskId;
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
