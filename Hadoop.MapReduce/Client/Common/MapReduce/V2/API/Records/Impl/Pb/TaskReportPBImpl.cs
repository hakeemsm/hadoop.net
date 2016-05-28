using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB
{
	public class TaskReportPBImpl : ProtoBase<MRProtos.TaskReportProto>, TaskReport
	{
		internal MRProtos.TaskReportProto proto = MRProtos.TaskReportProto.GetDefaultInstance
			();

		internal MRProtos.TaskReportProto.Builder builder = null;

		internal bool viaProto = false;

		private TaskId taskId = null;

		private Counters counters = null;

		private IList<TaskAttemptId> runningAttempts = null;

		private TaskAttemptId successfulAttemptId = null;

		private IList<string> diagnostics = null;

		private string status;

		public TaskReportPBImpl()
		{
			builder = MRProtos.TaskReportProto.NewBuilder();
		}

		public TaskReportPBImpl(MRProtos.TaskReportProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRProtos.TaskReportProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRProtos.TaskReportProto)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.taskId != null)
			{
				builder.SetTaskId(ConvertToProtoFormat(this.taskId));
			}
			if (this.counters != null)
			{
				builder.SetCounters(ConvertToProtoFormat(this.counters));
			}
			if (this.runningAttempts != null)
			{
				AddRunningAttemptsToProto();
			}
			if (this.successfulAttemptId != null)
			{
				builder.SetSuccessfulAttempt(ConvertToProtoFormat(this.successfulAttemptId));
			}
			if (this.diagnostics != null)
			{
				AddDiagnosticsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRProtos.TaskReportProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRProtos.TaskReportProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual Counters GetCounters()
		{
			MRProtos.TaskReportProtoOrBuilder p = viaProto ? proto : builder;
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
			MRProtos.TaskReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetStartTime());
		}

		public virtual void SetStartTime(long startTime)
		{
			MaybeInitBuilder();
			builder.SetStartTime((startTime));
		}

		public virtual long GetFinishTime()
		{
			MRProtos.TaskReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetFinishTime());
		}

		public virtual void SetFinishTime(long finishTime)
		{
			MaybeInitBuilder();
			builder.SetFinishTime((finishTime));
		}

		public virtual TaskId GetTaskId()
		{
			MRProtos.TaskReportProtoOrBuilder p = viaProto ? proto : builder;
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

		public virtual float GetProgress()
		{
			MRProtos.TaskReportProtoOrBuilder p = viaProto ? proto : builder;
			return (p.GetProgress());
		}

		public virtual string GetStatus()
		{
			return status;
		}

		public virtual void SetProgress(float progress)
		{
			MaybeInitBuilder();
			builder.SetProgress((progress));
		}

		public virtual void SetStatus(string status)
		{
			this.status = status;
		}

		public virtual TaskState GetTaskState()
		{
			MRProtos.TaskReportProtoOrBuilder p = viaProto ? proto : builder;
			if (!p.HasTaskState())
			{
				return null;
			}
			return ConvertFromProtoFormat(p.GetTaskState());
		}

		public virtual void SetTaskState(TaskState taskState)
		{
			MaybeInitBuilder();
			if (taskState == null)
			{
				builder.ClearTaskState();
				return;
			}
			builder.SetTaskState(ConvertToProtoFormat(taskState));
		}

		public virtual IList<TaskAttemptId> GetRunningAttemptsList()
		{
			InitRunningAttempts();
			return this.runningAttempts;
		}

		public virtual TaskAttemptId GetRunningAttempt(int index)
		{
			InitRunningAttempts();
			return this.runningAttempts[index];
		}

		public virtual int GetRunningAttemptsCount()
		{
			InitRunningAttempts();
			return this.runningAttempts.Count;
		}

		private void InitRunningAttempts()
		{
			if (this.runningAttempts != null)
			{
				return;
			}
			MRProtos.TaskReportProtoOrBuilder p = viaProto ? proto : builder;
			IList<MRProtos.TaskAttemptIdProto> list = p.GetRunningAttemptsList();
			this.runningAttempts = new AList<TaskAttemptId>();
			foreach (MRProtos.TaskAttemptIdProto c in list)
			{
				this.runningAttempts.AddItem(ConvertFromProtoFormat(c));
			}
		}

		public virtual void AddAllRunningAttempts(IList<TaskAttemptId> runningAttempts)
		{
			if (runningAttempts == null)
			{
				return;
			}
			InitRunningAttempts();
			Sharpen.Collections.AddAll(this.runningAttempts, runningAttempts);
		}

		private void AddRunningAttemptsToProto()
		{
			MaybeInitBuilder();
			builder.ClearRunningAttempts();
			if (runningAttempts == null)
			{
				return;
			}
			IEnumerable<MRProtos.TaskAttemptIdProto> iterable = new _IEnumerable_251(this);
			builder.AddAllRunningAttempts(iterable);
		}

		private sealed class _IEnumerable_251 : IEnumerable<MRProtos.TaskAttemptIdProto>
		{
			public _IEnumerable_251(TaskReportPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<MRProtos.TaskAttemptIdProto> GetEnumerator()
			{
				return new _IEnumerator_254(this);
			}

			private sealed class _IEnumerator_254 : IEnumerator<MRProtos.TaskAttemptIdProto>
			{
				public _IEnumerator_254(_IEnumerable_251 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.runningAttempts.GetEnumerator();
				}

				internal IEnumerator<TaskAttemptId> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override MRProtos.TaskAttemptIdProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_251 _enclosing;
			}

			private readonly TaskReportPBImpl _enclosing;
		}

		public virtual void AddRunningAttempt(TaskAttemptId runningAttempts)
		{
			InitRunningAttempts();
			this.runningAttempts.AddItem(runningAttempts);
		}

		public virtual void RemoveRunningAttempt(int index)
		{
			InitRunningAttempts();
			this.runningAttempts.Remove(index);
		}

		public virtual void ClearRunningAttempts()
		{
			InitRunningAttempts();
			this.runningAttempts.Clear();
		}

		public virtual TaskAttemptId GetSuccessfulAttempt()
		{
			MRProtos.TaskReportProtoOrBuilder p = viaProto ? proto : builder;
			if (this.successfulAttemptId != null)
			{
				return this.successfulAttemptId;
			}
			if (!p.HasSuccessfulAttempt())
			{
				return null;
			}
			this.successfulAttemptId = ConvertFromProtoFormat(p.GetSuccessfulAttempt());
			return this.successfulAttemptId;
		}

		public virtual void SetSuccessfulAttempt(TaskAttemptId successfulAttempt)
		{
			MaybeInitBuilder();
			if (successfulAttempt == null)
			{
				builder.ClearSuccessfulAttempt();
			}
			this.successfulAttemptId = successfulAttempt;
		}

		public virtual IList<string> GetDiagnosticsList()
		{
			InitDiagnostics();
			return this.diagnostics;
		}

		public virtual string GetDiagnostics(int index)
		{
			InitDiagnostics();
			return this.diagnostics[index];
		}

		public virtual int GetDiagnosticsCount()
		{
			InitDiagnostics();
			return this.diagnostics.Count;
		}

		private void InitDiagnostics()
		{
			if (this.diagnostics != null)
			{
				return;
			}
			MRProtos.TaskReportProtoOrBuilder p = viaProto ? proto : builder;
			IList<string> list = p.GetDiagnosticsList();
			this.diagnostics = new AList<string>();
			foreach (string c in list)
			{
				this.diagnostics.AddItem(c);
			}
		}

		public virtual void AddAllDiagnostics(IList<string> diagnostics)
		{
			if (diagnostics == null)
			{
				return;
			}
			InitDiagnostics();
			Sharpen.Collections.AddAll(this.diagnostics, diagnostics);
		}

		private void AddDiagnosticsToProto()
		{
			MaybeInitBuilder();
			builder.ClearDiagnostics();
			if (diagnostics == null)
			{
				return;
			}
			builder.AddAllDiagnostics(diagnostics);
		}

		public virtual void AddDiagnostics(string diagnostics)
		{
			InitDiagnostics();
			this.diagnostics.AddItem(diagnostics);
		}

		public virtual void RemoveDiagnostics(int index)
		{
			InitDiagnostics();
			this.diagnostics.Remove(index);
		}

		public virtual void ClearDiagnostics()
		{
			InitDiagnostics();
			this.diagnostics.Clear();
		}

		private CountersPBImpl ConvertFromProtoFormat(MRProtos.CountersProto p)
		{
			return new CountersPBImpl(p);
		}

		private MRProtos.CountersProto ConvertToProtoFormat(Counters t)
		{
			return ((CountersPBImpl)t).GetProto();
		}

		private TaskIdPBImpl ConvertFromProtoFormat(MRProtos.TaskIdProto p)
		{
			return new TaskIdPBImpl(p);
		}

		private MRProtos.TaskIdProto ConvertToProtoFormat(TaskId t)
		{
			return ((TaskIdPBImpl)t).GetProto();
		}

		private MRProtos.TaskStateProto ConvertToProtoFormat(TaskState e)
		{
			return MRProtoUtils.ConvertToProtoFormat(e);
		}

		private TaskState ConvertFromProtoFormat(MRProtos.TaskStateProto e)
		{
			return MRProtoUtils.ConvertFromProtoFormat(e);
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
