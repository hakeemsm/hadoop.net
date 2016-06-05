using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	internal abstract class StartEndTimesBase : TaskRuntimeEstimator
	{
		internal const float MinimumCompleteProportionToSpeculate = 0.05F;

		internal const int MinimumCompleteNumberToSpeculate = 1;

		protected internal AppContext context = null;

		protected internal readonly IDictionary<TaskAttemptId, long> startTimes = new ConcurrentHashMap
			<TaskAttemptId, long>();

		protected internal readonly IDictionary<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			, DataStatistics> mapperStatistics = new Dictionary<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			, DataStatistics>();

		protected internal readonly IDictionary<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			, DataStatistics> reducerStatistics = new Dictionary<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			, DataStatistics>();

		private readonly IDictionary<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job, float> slowTaskRelativeTresholds
			 = new Dictionary<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job, float>();

		protected internal readonly ICollection<Task> doneTasks = new HashSet<Task>();

		// XXXX This class design assumes that the contents of AppContext.getAllJobs
		//   never changes.  Is that right?
		//
		// This assumption comes in in several places, mostly in data structure that
		//   can grow without limit if a AppContext gets new Job's when the old ones
		//   run out.  Also, these mapper statistics blocks won't cover the Job's
		//   we don't know about.
		public virtual void EnrollAttempt(TaskAttemptStatusUpdateEvent.TaskAttemptStatus 
			status, long timestamp)
		{
			startTimes[status.id] = timestamp;
		}

		public virtual long AttemptEnrolledTime(TaskAttemptId attemptID)
		{
			long result = startTimes[attemptID];
			return result == null ? long.MaxValue : result;
		}

		public virtual void Contextualize(Configuration conf, AppContext context)
		{
			this.context = context;
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> allJobs = context.
				GetAllJobs();
			foreach (KeyValuePair<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> entry in 
				allJobs)
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = entry.Value;
				mapperStatistics[job] = new DataStatistics();
				reducerStatistics[job] = new DataStatistics();
				slowTaskRelativeTresholds[job] = conf.GetFloat(MRJobConfig.SpeculativeSlowtaskThreshold
					, 1.0f);
			}
		}

		protected internal virtual DataStatistics DataStatisticsForTask(TaskId taskID)
		{
			JobId jobID = taskID.GetJobId();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = context.GetJob(jobID);
			if (job == null)
			{
				return null;
			}
			Task task = job.GetTask(taskID);
			if (task == null)
			{
				return null;
			}
			return task.GetType() == TaskType.Map ? mapperStatistics[job] : task.GetType() ==
				 TaskType.Reduce ? reducerStatistics[job] : null;
		}

		public virtual long ThresholdRuntime(TaskId taskID)
		{
			JobId jobID = taskID.GetJobId();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = context.GetJob(jobID);
			TaskType type = taskID.GetTaskType();
			DataStatistics statistics = DataStatisticsForTask(taskID);
			int completedTasksOfType = type == TaskType.Map ? job.GetCompletedMaps() : job.GetCompletedReduces
				();
			int totalTasksOfType = type == TaskType.Map ? job.GetTotalMaps() : job.GetTotalReduces
				();
			if (completedTasksOfType < MinimumCompleteNumberToSpeculate || (((float)completedTasksOfType
				) / totalTasksOfType) < MinimumCompleteProportionToSpeculate)
			{
				return long.MaxValue;
			}
			long result = statistics == null ? long.MaxValue : (long)statistics.Outlier(slowTaskRelativeTresholds
				[job]);
			return result;
		}

		public virtual long EstimatedNewAttemptRuntime(TaskId id)
		{
			DataStatistics statistics = DataStatisticsForTask(id);
			if (statistics == null)
			{
				return -1L;
			}
			return (long)statistics.Mean();
		}

		public virtual void UpdateAttempt(TaskAttemptStatusUpdateEvent.TaskAttemptStatus 
			status, long timestamp)
		{
			TaskAttemptId attemptID = status.id;
			TaskId taskID = attemptID.GetTaskId();
			JobId jobID = taskID.GetJobId();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = context.GetJob(jobID);
			if (job == null)
			{
				return;
			}
			Task task = job.GetTask(taskID);
			if (task == null)
			{
				return;
			}
			long boxedStart = startTimes[attemptID];
			long start = boxedStart == null ? long.MinValue : boxedStart;
			TaskAttempt taskAttempt = task.GetAttempt(attemptID);
			if (taskAttempt.GetState() == TaskAttemptState.Succeeded)
			{
				bool isNew = false;
				// is this  a new success?
				lock (doneTasks)
				{
					if (!doneTasks.Contains(task))
					{
						doneTasks.AddItem(task);
						isNew = true;
					}
				}
				// It's a new completion
				// Note that if a task completes twice [because of a previous speculation
				//  and a race, or a success followed by loss of the machine with the
				//  local data] we only count the first one.
				if (isNew)
				{
					long finish = timestamp;
					if (start > 1L && finish > 1L && start <= finish)
					{
						long duration = finish - start;
						DataStatistics statistics = DataStatisticsForTask(taskID);
						if (statistics != null)
						{
							statistics.Add(duration);
						}
					}
				}
			}
		}

		public abstract long EstimatedRuntime(TaskAttemptId arg1);

		public abstract long RuntimeEstimateVariance(TaskAttemptId arg1);
	}
}
