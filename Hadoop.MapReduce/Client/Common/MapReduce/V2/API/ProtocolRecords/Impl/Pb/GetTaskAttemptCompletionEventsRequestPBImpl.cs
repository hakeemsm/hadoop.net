using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetTaskAttemptCompletionEventsRequestPBImpl : ProtoBase<MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto
		>, GetTaskAttemptCompletionEventsRequest
	{
		internal MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto proto = MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private JobId jobId = null;

		public GetTaskAttemptCompletionEventsRequestPBImpl()
		{
			builder = MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto.NewBuilder();
		}

		public GetTaskAttemptCompletionEventsRequestPBImpl(MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto
				)builder.Build());
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
			proto = ((MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto)builder.Build
				());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		public virtual JobId GetJobId()
		{
			MRServiceProtos.GetTaskAttemptCompletionEventsRequestProtoOrBuilder p = viaProto ? 
				proto : builder;
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

		public virtual int GetFromEventId()
		{
			MRServiceProtos.GetTaskAttemptCompletionEventsRequestProtoOrBuilder p = viaProto ? 
				proto : builder;
			return (p.GetFromEventId());
		}

		public virtual void SetFromEventId(int fromEventId)
		{
			MaybeInitBuilder();
			builder.SetFromEventId((fromEventId));
		}

		public virtual int GetMaxEvents()
		{
			MRServiceProtos.GetTaskAttemptCompletionEventsRequestProtoOrBuilder p = viaProto ? 
				proto : builder;
			return (p.GetMaxEvents());
		}

		public virtual void SetMaxEvents(int maxEvents)
		{
			MaybeInitBuilder();
			builder.SetMaxEvents((maxEvents));
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
