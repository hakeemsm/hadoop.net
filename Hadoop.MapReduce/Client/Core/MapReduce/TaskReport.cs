using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>A report on the state of a task.</summary>
	public class TaskReport : Writable
	{
		private TaskID taskid;

		private float progress;

		private string state;

		private string[] diagnostics;

		private long startTime;

		private long finishTime;

		private Counters counters;

		private TIPStatus currentStatus;

		private ICollection<TaskAttemptID> runningAttempts = new AList<TaskAttemptID>();

		private TaskAttemptID successfulAttempt = new TaskAttemptID();

		public TaskReport()
		{
			taskid = new TaskID();
		}

		/// <summary>Creates a new TaskReport object</summary>
		/// <param name="taskid"/>
		/// <param name="progress"/>
		/// <param name="state"/>
		/// <param name="diagnostics"/>
		/// <param name="currentStatus"/>
		/// <param name="startTime"/>
		/// <param name="finishTime"/>
		/// <param name="counters"/>
		public TaskReport(TaskID taskid, float progress, string state, string[] diagnostics
			, TIPStatus currentStatus, long startTime, long finishTime, Counters counters)
		{
			this.taskid = taskid;
			this.progress = progress;
			this.state = state;
			this.diagnostics = diagnostics;
			this.currentStatus = currentStatus;
			this.startTime = startTime;
			this.finishTime = finishTime;
			this.counters = counters;
		}

		/// <summary>The string of the task ID.</summary>
		public virtual string GetTaskId()
		{
			return taskid.ToString();
		}

		/// <summary>The ID of the task.</summary>
		public virtual TaskID GetTaskID()
		{
			return taskid;
		}

		/// <summary>The amount completed, between zero and one.</summary>
		public virtual float GetProgress()
		{
			return progress;
		}

		/// <summary>The most recent state, reported by the Reporter.</summary>
		public virtual string GetState()
		{
			return state;
		}

		/// <summary>A list of error messages.</summary>
		public virtual string[] GetDiagnostics()
		{
			return diagnostics;
		}

		/// <summary>A table of counters.</summary>
		public virtual Counters GetTaskCounters()
		{
			return counters;
		}

		/// <summary>The current status</summary>
		public virtual TIPStatus GetCurrentStatus()
		{
			return currentStatus;
		}

		/// <summary>Get finish time of task.</summary>
		/// <returns>0, if finish time was not set else returns finish time.</returns>
		public virtual long GetFinishTime()
		{
			return finishTime;
		}

		/// <summary>set finish time of task.</summary>
		/// <param name="finishTime">finish time of task.</param>
		protected internal virtual void SetFinishTime(long finishTime)
		{
			this.finishTime = finishTime;
		}

		/// <summary>Get start time of task.</summary>
		/// <returns>0 if start time was not set, else start time.</returns>
		public virtual long GetStartTime()
		{
			return startTime;
		}

		/// <summary>set start time of the task.</summary>
		protected internal virtual void SetStartTime(long startTime)
		{
			this.startTime = startTime;
		}

		/// <summary>set successful attempt ID of the task.</summary>
		protected internal virtual void SetSuccessfulAttemptId(TaskAttemptID t)
		{
			successfulAttempt = t;
		}

		/// <summary>Get the attempt ID that took this task to completion</summary>
		public virtual TaskAttemptID GetSuccessfulTaskAttemptId()
		{
			return successfulAttempt;
		}

		/// <summary>set running attempt(s) of the task.</summary>
		protected internal virtual void SetRunningTaskAttemptIds(ICollection<TaskAttemptID
			> runningAttempts)
		{
			this.runningAttempts = runningAttempts;
		}

		/// <summary>Get the running task attempt IDs for this task</summary>
		public virtual ICollection<TaskAttemptID> GetRunningTaskAttemptIds()
		{
			return runningAttempts;
		}

		public override bool Equals(object o)
		{
			if (o == null)
			{
				return false;
			}
			if (o.GetType().Equals(this.GetType()))
			{
				Org.Apache.Hadoop.Mapreduce.TaskReport report = (Org.Apache.Hadoop.Mapreduce.TaskReport
					)o;
				return counters.Equals(report.GetTaskCounters()) && Arrays.ToString(this.diagnostics
					).Equals(Arrays.ToString(report.GetDiagnostics())) && this.finishTime == report.
					GetFinishTime() && this.progress == report.GetProgress() && this.startTime == report
					.GetStartTime() && this.state.Equals(report.GetState()) && this.taskid.Equals(report
					.GetTaskID());
			}
			return false;
		}

		public override int GetHashCode()
		{
			return (counters.ToString() + Arrays.ToString(this.diagnostics) + this.finishTime
				 + this.progress + this.startTime + this.state + this.taskid.ToString()).GetHashCode
				();
		}

		//////////////////////////////////////////////
		// Writable
		//////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			taskid.Write(@out);
			@out.WriteFloat(progress);
			Text.WriteString(@out, state);
			@out.WriteLong(startTime);
			@out.WriteLong(finishTime);
			WritableUtils.WriteStringArray(@out, diagnostics);
			counters.Write(@out);
			WritableUtils.WriteEnum(@out, currentStatus);
			if (currentStatus == TIPStatus.Running)
			{
				WritableUtils.WriteVInt(@out, runningAttempts.Count);
				TaskAttemptID[] t = new TaskAttemptID[0];
				t = Sharpen.Collections.ToArray(runningAttempts, t);
				for (int i = 0; i < t.Length; i++)
				{
					t[i].Write(@out);
				}
			}
			else
			{
				if (currentStatus == TIPStatus.Complete)
				{
					successfulAttempt.Write(@out);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			this.taskid.ReadFields(@in);
			this.progress = @in.ReadFloat();
			this.state = StringInterner.WeakIntern(Text.ReadString(@in));
			this.startTime = @in.ReadLong();
			this.finishTime = @in.ReadLong();
			diagnostics = WritableUtils.ReadStringArray(@in);
			counters = new Counters();
			counters.ReadFields(@in);
			currentStatus = WritableUtils.ReadEnum<TIPStatus>(@in);
			if (currentStatus == TIPStatus.Running)
			{
				int num = WritableUtils.ReadVInt(@in);
				for (int i = 0; i < num; i++)
				{
					TaskAttemptID t = new TaskAttemptID();
					t.ReadFields(@in);
					runningAttempts.AddItem(t);
				}
			}
			else
			{
				if (currentStatus == TIPStatus.Complete)
				{
					successfulAttempt.ReadFields(@in);
				}
			}
		}
	}
}
