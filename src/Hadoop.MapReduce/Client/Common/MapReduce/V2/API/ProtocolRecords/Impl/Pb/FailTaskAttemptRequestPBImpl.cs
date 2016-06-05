using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class FailTaskAttemptRequestPBImpl : ProtoBase<MRServiceProtos.FailTaskAttemptRequestProto
		>, FailTaskAttemptRequest
	{
		internal MRServiceProtos.FailTaskAttemptRequestProto proto = MRServiceProtos.FailTaskAttemptRequestProto
			.GetDefaultInstance();

		internal MRServiceProtos.FailTaskAttemptRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private TaskAttemptId taskAttemptId = null;

		public FailTaskAttemptRequestPBImpl()
		{
			builder = MRServiceProtos.FailTaskAttemptRequestProto.NewBuilder();
		}

		public FailTaskAttemptRequestPBImpl(MRServiceProtos.FailTaskAttemptRequestProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.FailTaskAttemptRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.FailTaskAttemptRequestProto)builder.
				Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.taskAttemptId != null)
			{
				builder.SetTaskAttemptId(ConvertToProtoFormat(this.taskAttemptId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRServiceProtos.FailTaskAttemptRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.FailTaskAttemptRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual TaskAttemptId GetTaskAttemptId()
		{
			MRServiceProtos.FailTaskAttemptRequestProtoOrBuilder p = viaProto ? proto : builder;
			if (this.taskAttemptId != null)
			{
				return this.taskAttemptId;
			}
			if (!p.HasTaskAttemptId())
			{
				return null;
			}
			this.taskAttemptId = ConvertFromProtoFormat(p.GetTaskAttemptId());
			return this.taskAttemptId;
		}

		public virtual void SetTaskAttemptId(TaskAttemptId taskAttemptId)
		{
			MaybeInitBuilder();
			if (taskAttemptId == null)
			{
				builder.ClearTaskAttemptId();
			}
			this.taskAttemptId = taskAttemptId;
		}

		private TaskAttemptIdPBImpl ConvertFromProtoFormat(MRProtos.TaskAttemptIdProto p)
		{
			return new TaskAttemptIdPBImpl(p);
		}

		private MRProtos.TaskAttemptIdProto ConvertToProtoFormat(TaskAttemptId t)
		{
			return ((TaskAttemptIdPBImpl)t).GetProto();
		}
	}
}
