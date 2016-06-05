using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A report on the state of a task.</summary>
	public class TaskReport : Org.Apache.Hadoop.Mapreduce.TaskReport
	{
		public TaskReport()
			: base()
		{
		}

		/// <summary>Creates a new TaskReport object</summary>
		/// <param name="taskid"/>
		/// <param name="progress"/>
		/// <param name="state"/>
		/// <param name="diagnostics"/>
		/// <param name="startTime"/>
		/// <param name="finishTime"/>
		/// <param name="counters"/>
		[System.ObsoleteAttribute]
		internal TaskReport(TaskID taskid, float progress, string state, string[] diagnostics
			, long startTime, long finishTime, Counters counters)
			: this(taskid, progress, state, diagnostics, null, startTime, finishTime, counters
				)
		{
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
		internal TaskReport(TaskID taskid, float progress, string state, string[] diagnostics
			, TIPStatus currentStatus, long startTime, long finishTime, Counters counters)
			: base(taskid, progress, state, diagnostics, currentStatus, startTime, finishTime
				, new Counters(counters))
		{
		}

		internal static Org.Apache.Hadoop.Mapred.TaskReport Downgrade(Org.Apache.Hadoop.Mapreduce.TaskReport
			 report)
		{
			return new Org.Apache.Hadoop.Mapred.TaskReport(TaskID.Downgrade(report.GetTaskID(
				)), report.GetProgress(), report.GetState(), report.GetDiagnostics(), report.GetCurrentStatus
				(), report.GetStartTime(), report.GetFinishTime(), Counters.Downgrade(report.GetTaskCounters
				()));
		}

		internal static Org.Apache.Hadoop.Mapred.TaskReport[] DowngradeArray(Org.Apache.Hadoop.Mapreduce.TaskReport
			[] reports)
		{
			IList<Org.Apache.Hadoop.Mapred.TaskReport> ret = new AList<Org.Apache.Hadoop.Mapred.TaskReport
				>();
			foreach (Org.Apache.Hadoop.Mapreduce.TaskReport report in reports)
			{
				ret.AddItem(Downgrade(report));
			}
			return Sharpen.Collections.ToArray(ret, new Org.Apache.Hadoop.Mapred.TaskReport[0
				]);
		}

		/// <summary>The string of the task id.</summary>
		public override string GetTaskId()
		{
			return TaskID.Downgrade(base.GetTaskID()).ToString();
		}

		/// <summary>The id of the task.</summary>
		public override TaskID GetTaskID()
		{
			return TaskID.Downgrade(base.GetTaskID());
		}

		public virtual Counters GetCounters()
		{
			return Counters.Downgrade(base.GetTaskCounters());
		}

		/// <summary>set successful attempt ID of the task.</summary>
		public virtual void SetSuccessfulAttempt(TaskAttemptID t)
		{
			base.SetSuccessfulAttemptId(t);
		}

		/// <summary>Get the attempt ID that took this task to completion</summary>
		public virtual TaskAttemptID GetSuccessfulTaskAttempt()
		{
			return TaskAttemptID.Downgrade(base.GetSuccessfulTaskAttemptId());
		}

		/// <summary>set running attempt(s) of the task.</summary>
		public virtual void SetRunningTaskAttempts(ICollection<TaskAttemptID> runningAttempts
			)
		{
			ICollection<TaskAttemptID> attempts = new AList<TaskAttemptID>();
			foreach (TaskAttemptID id in runningAttempts)
			{
				attempts.AddItem(id);
			}
			base.SetRunningTaskAttemptIds(attempts);
		}

		/// <summary>Get the running task attempt IDs for this task</summary>
		public virtual ICollection<TaskAttemptID> GetRunningTaskAttempts()
		{
			ICollection<TaskAttemptID> attempts = new AList<TaskAttemptID>();
			foreach (TaskAttemptID id in base.GetRunningTaskAttemptIds())
			{
				attempts.AddItem(TaskAttemptID.Downgrade(id));
			}
			return attempts;
		}

		/// <summary>set finish time of task.</summary>
		/// <param name="finishTime">finish time of task.</param>
		protected internal override void SetFinishTime(long finishTime)
		{
			base.SetFinishTime(finishTime);
		}

		/// <summary>set start time of the task.</summary>
		protected internal override void SetStartTime(long startTime)
		{
			base.SetStartTime(startTime);
		}
	}
}
