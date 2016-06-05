using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class CompletedTask : Task
	{
		private static readonly Counters EmptyCounters = new Counters();

		private readonly TaskId taskId;

		private readonly JobHistoryParser.TaskInfo taskInfo;

		private TaskReport report;

		private TaskAttemptId successfulAttempt;

		private IList<string> reportDiagnostics = new List<string>();

		private Lock taskAttemptsLock = new ReentrantLock();

		private AtomicBoolean taskAttemptsLoaded = new AtomicBoolean(false);

		private readonly IDictionary<TaskAttemptId, TaskAttempt> attempts = new LinkedHashMap
			<TaskAttemptId, TaskAttempt>();

		internal CompletedTask(TaskId taskId, JobHistoryParser.TaskInfo taskInfo)
		{
			//TODO JobHistoryParser.handleTaskFailedAttempt should use state from the event.
			this.taskInfo = taskInfo;
			this.taskId = taskId;
		}

		public virtual bool CanCommit(TaskAttemptId taskAttemptID)
		{
			return false;
		}

		public virtual TaskAttempt GetAttempt(TaskAttemptId attemptID)
		{
			LoadAllTaskAttempts();
			return attempts[attemptID];
		}

		public virtual IDictionary<TaskAttemptId, TaskAttempt> GetAttempts()
		{
			LoadAllTaskAttempts();
			return attempts;
		}

		public virtual Counters GetCounters()
		{
			return taskInfo.GetCounters();
		}

		public virtual TaskId GetID()
		{
			return taskId;
		}

		public virtual float GetProgress()
		{
			return 1.0f;
		}

		public virtual TaskReport GetReport()
		{
			lock (this)
			{
				if (report == null)
				{
					ConstructTaskReport();
				}
				return report;
			}
		}

		public virtual TaskType GetType()
		{
			return TypeConverter.ToYarn(taskInfo.GetTaskType());
		}

		public virtual bool IsFinished()
		{
			return true;
		}

		public virtual TaskState GetState()
		{
			return taskInfo.GetTaskStatus() == null ? TaskState.Killed : TaskState.ValueOf(taskInfo
				.GetTaskStatus());
		}

		private void ConstructTaskReport()
		{
			LoadAllTaskAttempts();
			this.report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<TaskReport>();
			report.SetTaskId(taskId);
			long minLaunchTime = long.MaxValue;
			foreach (TaskAttempt attempt in attempts.Values)
			{
				minLaunchTime = Math.Min(minLaunchTime, attempt.GetLaunchTime());
			}
			minLaunchTime = minLaunchTime == long.MaxValue ? -1 : minLaunchTime;
			report.SetStartTime(minLaunchTime);
			report.SetFinishTime(taskInfo.GetFinishTime());
			report.SetTaskState(GetState());
			report.SetProgress(GetProgress());
			Counters counters = GetCounters();
			if (counters == null)
			{
				counters = EmptyCounters;
			}
			report.SetCounters(TypeConverter.ToYarn(counters));
			if (successfulAttempt != null)
			{
				report.SetSuccessfulAttempt(successfulAttempt);
			}
			report.AddAllDiagnostics(reportDiagnostics);
			report.AddAllRunningAttempts(new AList<TaskAttemptId>(attempts.Keys));
		}

		private void LoadAllTaskAttempts()
		{
			if (taskAttemptsLoaded.Get())
			{
				return;
			}
			taskAttemptsLock.Lock();
			try
			{
				if (taskAttemptsLoaded.Get())
				{
					return;
				}
				foreach (JobHistoryParser.TaskAttemptInfo attemptHistory in taskInfo.GetAllTaskAttempts
					().Values)
				{
					CompletedTaskAttempt attempt = new CompletedTaskAttempt(taskId, attemptHistory);
					Sharpen.Collections.AddAll(reportDiagnostics, attempt.GetDiagnostics());
					attempts[attempt.GetID()] = attempt;
					if (successfulAttempt == null && attemptHistory.GetTaskStatus() != null && attemptHistory
						.GetTaskStatus().Equals(TaskState.Succeeded.ToString()))
					{
						successfulAttempt = TypeConverter.ToYarn(attemptHistory.GetAttemptId());
					}
				}
				taskAttemptsLoaded.Set(true);
			}
			finally
			{
				taskAttemptsLock.Unlock();
			}
		}
	}
}
