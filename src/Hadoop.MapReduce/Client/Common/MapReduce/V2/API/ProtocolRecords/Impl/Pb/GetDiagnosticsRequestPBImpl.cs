using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetDiagnosticsRequestPBImpl : ProtoBase<MRServiceProtos.GetDiagnosticsRequestProto
		>, GetDiagnosticsRequest
	{
		internal MRServiceProtos.GetDiagnosticsRequestProto proto = MRServiceProtos.GetDiagnosticsRequestProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetDiagnosticsRequestProto.Builder builder = null;

		internal bool viaProto = false;

		private TaskAttemptId taskAttemptId = null;

		public GetDiagnosticsRequestPBImpl()
		{
			builder = MRServiceProtos.GetDiagnosticsRequestProto.NewBuilder();
		}

		public GetDiagnosticsRequestPBImpl(MRServiceProtos.GetDiagnosticsRequestProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetDiagnosticsRequestProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetDiagnosticsRequestProto)builder.Build
				());
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
			proto = ((MRServiceProtos.GetDiagnosticsRequestProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetDiagnosticsRequestProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual TaskAttemptId GetTaskAttemptId()
		{
			MRServiceProtos.GetDiagnosticsRequestProtoOrBuilder p = viaProto ? proto : builder;
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
