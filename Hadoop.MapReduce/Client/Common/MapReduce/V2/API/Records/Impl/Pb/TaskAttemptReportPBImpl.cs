using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class TaskAttemptReportPBImpl : ProtoBase<MRProtos.TaskAttemptReportProto>
		, TaskAttemptReport
	{
		internal MRProtos.TaskAttemptReportProto proto = MRProtos.TaskAttemptReportProto.
			GetDefaultInstance();

		internal MRProtos.TaskAttemptReportProto.Builder builder = null;

		internal bool viaProto = false;

		private TaskAttemptId taskAttemptId = null;

		private Counters counters = null;

		private ContainerId containerId = null;

		public TaskAttemptReportPBImpl()
		{
			builder = MRProtos.TaskAttemptReportProto.NewBuilder();
		}

		public TaskAttemptReportPBImpl(MRProtos.TaskAttemptReportProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRProtos.TaskAttemptReportProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRProtos.TaskAttemptReportProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.taskAttemptId != null)
			{
				builder.SetTaskAttemptId(ConvertToProtoFormat(this.taskAttemptId));
			}
			if (this.counters != null)
			{
				builder.SetCounters(ConvertToProtoFormat(this.counters));
			}
			if (this.containerId != null)
			{
				builder.SetContainerId(ConvertToProtoFormat(this.containerId));
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRProtos.TaskAttemptReportProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRProtos.TaskAttemptReportProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual Counters GetCounters()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (this.counters != null)
			{
				return this.counters;
			}
			if (!p.HasCounters())
			{
				return null;
			}
			this.counters = ConvertFromProtoFormat(p.GetCounters());
			return this.counters;
		}

		public virtual void SetCounters(Counters counters)
		{
			MaybeInitBuilder();
			if (counters == null)
			{
				builder.ClearCounters();
			}
			this.counters = counters;
		}

		public virtual long GetStartTime()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetStartTime());
		}

		public virtual void SetStartTime(long startTime)
		{
			MaybeInitBuilder();
			builder.SetStartTime((startTime));
		}

		public virtual long GetFinishTime()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetFinishTime());
		}

		public virtual void SetFinishTime(long finishTime)
		{
			MaybeInitBuilder();
			builder.SetFinishTime((finishTime));
		}

		public virtual long GetShuffleFinishTime()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetShuffleFinishTime());
		}

		public virtual void SetShuffleFinishTime(long time)
		{
			MaybeInitBuilder();
			builder.SetShuffleFinishTime(time);
		}

		public virtual long GetSortFinishTime()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetSortFinishTime());
		}

		public virtual void SetSortFinishTime(long time)
		{
			MaybeInitBuilder();
			builder.SetSortFinishTime(time);
		}

		public virtual TaskAttemptId GetTaskAttemptId()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
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

		public virtual TaskAttemptState GetTaskAttemptState()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasTaskAttemptState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetTaskAttemptState());
		}

		public virtual void SetTaskAttemptState(TaskAttemptState taskAttemptState)
		{
			MaybeInitBuilder();
			if (taskAttemptState == null)
			{
				builder.ClearTaskAttemptState();
				return;
			}
			builder.SetTaskAttemptState(ConvertToProtoFormat(taskAttemptState));
		}

		public virtual float GetProgress()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetProgress());
		}

		public virtual void SetProgress(float progress)
		{
			MaybeInitBuilder();
			builder.SetProgress((progress));
		}

		public virtual string GetDiagnosticInfo()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasDiagnosticInfo())
			{
				return null;
			}
			return (p.GetDiagnosticInfo());
		}

		public virtual void SetDiagnosticInfo(string diagnosticInfo)
		{
			MaybeInitBuilder();
			if (diagnosticInfo == null)
			{
				builder.ClearDiagnosticInfo();
				return;
			}
			builder.SetDiagnosticInfo((diagnosticInfo));
		}

		public virtual string GetStateString()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasStateString())
			{
				return null;
			}
			return (p.GetStateString());
		}

		public virtual void SetStateString(string stateString)
		{
			MaybeInitBuilder();
			if (stateString == null)
			{
				builder.ClearStateString();
				return;
			}
			builder.SetStateString((stateString));
		}

		public virtual Phase GetPhase()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasPhase())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetPhase());
		}

		public virtual void SetPhase(Phase phase)
		{
			MaybeInitBuilder();
			if (phase == null)
			{
				builder.ClearPhase();
				return;
			}
			builder.SetPhase(ConvertToProtoFormat(phase));
		}

		public virtual string GetNodeManagerHost()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasNodeManagerHost())
			{
				return null;
			}
			return p.GetNodeManagerHost();
		}

		public virtual void SetNodeManagerHost(string nmHost)
		{
			MaybeInitBuilder();
			if (nmHost == null)
			{
				builder.ClearNodeManagerHost();
				return;
			}
			builder.SetNodeManagerHost(nmHost);
		}

		public virtual int GetNodeManagerPort()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetNodeManagerPort());
		}

		public virtual void SetNodeManagerPort(int nmPort)
		{
			MaybeInitBuilder();
			builder.SetNodeManagerPort(nmPort);
		}

		public virtual int GetNodeManagerHttpPort()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetNodeManagerHttpPort());
		}

		public virtual void SetNodeManagerHttpPort(int nmHttpPort)
		{
			MaybeInitBuilder();
			builder.SetNodeManagerHttpPort(nmHttpPort);
		}

		public virtual ContainerId GetContainerId()
		{
			MRProtos.TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
			if (containerId != null)
			{
				return containerId;
			}
			// Else via proto
			if (!p.HasContainerId())
			{
				return null;
			}
			containerId = ConvertFromProtoFormat(p.GetContainerId());
			return containerId;
		}

		public virtual void SetContainerId(ContainerId containerId)
		{
			MaybeInitBuilder();
			if (containerId == null)
			{
				builder.ClearContainerId();
			}
			this.containerId = containerId;
		}

		private YarnProtos.ContainerIdProto ConvertToProtoFormat(ContainerId t)
		{
			return ((ContainerIdPBImpl)t).GetProto();
		}

		private ContainerIdPBImpl ConvertFromProtoFormat(YarnProtos.ContainerIdProto p)
		{
			return new ContainerIdPBImpl(p);
		}

		private CountersPBImpl ConvertFromProtoFormat(MRProtos.CountersProto p)
		{
			return new CountersPBImpl(p);
		}

		private MRProtos.CountersProto ConvertToProtoFormat(Counters t)
		{
			return ((CountersPBImpl)t).GetProto();
		}

		private TaskAttemptIdPBImpl ConvertFromProtoFormat(MRProtos.TaskAttemptIdProto p)
		{
			return new TaskAttemptIdPBImpl(p);
		}

		private MRProtos.TaskAttemptIdProto ConvertToProtoFormat(TaskAttemptId t)
		{
			return ((TaskAttemptIdPBImpl)t).GetProto();
		}

		private MRProtos.TaskAttemptStateProto ConvertToProtoFormat(TaskAttemptState e)
		{
			return MRProtoUtils.ConvertToProtoFormat(e);
		}

		private TaskAttemptState ConvertFromProtoFormat(MRProtos.TaskAttemptStateProto e)
		{
			return MRProtoUtils.ConvertFromProtoFormat(e);
		}

		private MRProtos.PhaseProto ConvertToProtoFormat(Phase e)
		{
			return MRProtoUtils.ConvertToProtoFormat(e);
		}

		private Phase ConvertFromProtoFormat(MRProtos.PhaseProto e)
		{
			return MRProtoUtils.ConvertFromProtoFormat(e);
		}
	}
}
