using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	public class LegacyTaskRuntimeEstimator : StartEndTimesBase
	{
		private readonly IDictionary<TaskAttempt, AtomicLong> attemptRuntimeEstimates = new 
			ConcurrentHashMap<TaskAttempt, AtomicLong>();

		private readonly ConcurrentHashMap<TaskAttempt, AtomicLong> attemptRuntimeEstimateVariances
			 = new ConcurrentHashMap<TaskAttempt, AtomicLong>();

		public override void UpdateAttempt(TaskAttemptStatusUpdateEvent.TaskAttemptStatus
			 status, long timestamp)
		{
			base.UpdateAttempt(status, timestamp);
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
			TaskAttempt taskAttempt = task.GetAttempt(attemptID);
			if (taskAttempt == null)
			{
				return;
			}
			long boxedStart = startTimes[attemptID];
			long start = boxedStart == null ? long.MinValue : boxedStart;
			// We need to do two things.
			//  1: If this is a completion, we accumulate statistics in the superclass
			//  2: If this is not a completion, we learn more about it.
			// This is not a completion, but we're cooking.
			//
			if (taskAttempt.GetState() == TaskAttemptState.Running)
			{
				// See if this task is already in the registry
				AtomicLong estimateContainer = attemptRuntimeEstimates[taskAttempt];
				AtomicLong estimateVarianceContainer = attemptRuntimeEstimateVariances[taskAttempt
					];
				if (estimateContainer == null)
				{
					if (attemptRuntimeEstimates[taskAttempt] == null)
					{
						attemptRuntimeEstimates[taskAttempt] = new AtomicLong();
						estimateContainer = attemptRuntimeEstimates[taskAttempt];
					}
				}
				if (estimateVarianceContainer == null)
				{
					attemptRuntimeEstimateVariances.PutIfAbsent(taskAttempt, new AtomicLong());
					estimateVarianceContainer = attemptRuntimeEstimateVariances[taskAttempt];
				}
				long estimate = -1;
				long varianceEstimate = -1;
				// This code assumes that we'll never consider starting a third
				//  speculative task attempt if two are already running for this task
				if (start > 0 && timestamp > start)
				{
					estimate = (long)((timestamp - start) / Math.Max(0.0001, status.progress));
					varianceEstimate = (long)(estimate * status.progress / 10);
				}
				if (estimateContainer != null)
				{
					estimateContainer.Set(estimate);
				}
				if (estimateVarianceContainer != null)
				{
					estimateVarianceContainer.Set(varianceEstimate);
				}
			}
		}

		private long StoredPerAttemptValue(IDictionary<TaskAttempt, AtomicLong> data, TaskAttemptId
			 attemptID)
		{
			TaskId taskID = attemptID.GetTaskId();
			JobId jobID = taskID.GetJobId();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = context.GetJob(jobID);
			Task task = job.GetTask(taskID);
			if (task == null)
			{
				return -1L;
			}
			TaskAttempt taskAttempt = task.GetAttempt(attemptID);
			if (taskAttempt == null)
			{
				return -1L;
			}
			AtomicLong estimate = data[taskAttempt];
			return estimate == null ? -1L : estimate.Get();
		}

		public override long EstimatedRuntime(TaskAttemptId attemptID)
		{
			return StoredPerAttemptValue(attemptRuntimeEstimates, attemptID);
		}

		public override long RuntimeEstimateVariance(TaskAttemptId attemptID)
		{
			return StoredPerAttemptValue(attemptRuntimeEstimateVariances, attemptID);
		}
	}
}
