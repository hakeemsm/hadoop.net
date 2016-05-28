using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao
{
	public class TestJobInfo
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAverageMergeTime()
		{
			string historyFileName = "job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";
			string confFileName = "job_1329348432655_0001_conf.xml";
			Configuration conf = new Configuration();
			JobACLsManager jobAclsMgr = new JobACLsManager(conf);
			Path fulleHistoryPath = new Path(typeof(TestJobHistoryEntities).GetClassLoader().
				GetResource(historyFileName).GetFile());
			Path fullConfPath = new Path(typeof(TestJobHistoryEntities).GetClassLoader().GetResource
				(confFileName).GetFile());
			HistoryFileManager.HistoryFileInfo info = Org.Mockito.Mockito.Mock<HistoryFileManager.HistoryFileInfo
				>();
			Org.Mockito.Mockito.When(info.GetConfFile()).ThenReturn(fullConfPath);
			JobId jobId = MRBuilderUtils.NewJobId(1329348432655l, 1, 1);
			CompletedJob completedJob = new CompletedJob(conf, jobId, fulleHistoryPath, true, 
				"user", info, jobAclsMgr);
			JobInfo jobInfo = new JobInfo(completedJob);
			// There are 2 tasks with merge time of 45 and 55 respectively. So average
			// merge time should be 50.
			NUnit.Framework.Assert.AreEqual(50L, jobInfo.GetAvgMergeTime());
		}

		[NUnit.Framework.Test]
		public virtual void TestAverageReduceTime()
		{
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<CompletedJob
				>();
			Task task1 = Org.Mockito.Mockito.Mock<Task>();
			Task task2 = Org.Mockito.Mockito.Mock<Task>();
			JobId jobId = MRBuilderUtils.NewJobId(1L, 1, 1);
			TaskId taskId1 = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Reduce);
			TaskId taskId2 = MRBuilderUtils.NewTaskId(jobId, 2, TaskType.Reduce);
			TaskAttemptId taskAttemptId1 = MRBuilderUtils.NewTaskAttemptId(taskId1, 1);
			TaskAttemptId taskAttemptId2 = MRBuilderUtils.NewTaskAttemptId(taskId2, 2);
			TaskAttempt taskAttempt1 = Org.Mockito.Mockito.Mock<TaskAttempt>();
			TaskAttempt taskAttempt2 = Org.Mockito.Mockito.Mock<TaskAttempt>();
			JobReport jobReport = Org.Mockito.Mockito.Mock<JobReport>();
			Org.Mockito.Mockito.When(taskAttempt1.GetState()).ThenReturn(TaskAttemptState.Succeeded
				);
			Org.Mockito.Mockito.When(taskAttempt1.GetLaunchTime()).ThenReturn(0L);
			Org.Mockito.Mockito.When(taskAttempt1.GetShuffleFinishTime()).ThenReturn(4L);
			Org.Mockito.Mockito.When(taskAttempt1.GetSortFinishTime()).ThenReturn(6L);
			Org.Mockito.Mockito.When(taskAttempt1.GetFinishTime()).ThenReturn(8L);
			Org.Mockito.Mockito.When(taskAttempt2.GetState()).ThenReturn(TaskAttemptState.Succeeded
				);
			Org.Mockito.Mockito.When(taskAttempt2.GetLaunchTime()).ThenReturn(5L);
			Org.Mockito.Mockito.When(taskAttempt2.GetShuffleFinishTime()).ThenReturn(10L);
			Org.Mockito.Mockito.When(taskAttempt2.GetSortFinishTime()).ThenReturn(22L);
			Org.Mockito.Mockito.When(taskAttempt2.GetFinishTime()).ThenReturn(42L);
			Org.Mockito.Mockito.When(task1.GetType()).ThenReturn(TaskType.Reduce);
			Org.Mockito.Mockito.When(task2.GetType()).ThenReturn(TaskType.Reduce);
			Org.Mockito.Mockito.When(task1.GetAttempts()).ThenReturn(new _Dictionary_120(taskAttemptId1
				, taskAttempt1));
			Org.Mockito.Mockito.When(task2.GetAttempts()).ThenReturn(new _Dictionary_123(taskAttemptId2
				, taskAttempt2));
			Org.Mockito.Mockito.When(job.GetTasks()).ThenReturn(new _Dictionary_127(taskId1, 
				task1, taskId2, task2));
			Org.Mockito.Mockito.When(job.GetID()).ThenReturn(jobId);
			Org.Mockito.Mockito.When(job.GetReport()).ThenReturn(jobReport);
			Org.Mockito.Mockito.When(job.GetName()).ThenReturn("TestJobInfo");
			Org.Mockito.Mockito.When(job.GetState()).ThenReturn(JobState.Succeeded);
			JobInfo jobInfo = new JobInfo(job);
			NUnit.Framework.Assert.AreEqual(11L, jobInfo.GetAvgReduceTime());
		}

		private sealed class _Dictionary_120 : Dictionary<TaskAttemptId, TaskAttempt>
		{
			public _Dictionary_120(TaskAttemptId taskAttemptId1, TaskAttempt taskAttempt1)
			{
				this.taskAttemptId1 = taskAttemptId1;
				this.taskAttempt1 = taskAttempt1;
				{
					this[taskAttemptId1] = taskAttempt1;
				}
			}

			private readonly TaskAttemptId taskAttemptId1;

			private readonly TaskAttempt taskAttempt1;
		}

		private sealed class _Dictionary_123 : Dictionary<TaskAttemptId, TaskAttempt>
		{
			public _Dictionary_123(TaskAttemptId taskAttemptId2, TaskAttempt taskAttempt2)
			{
				this.taskAttemptId2 = taskAttemptId2;
				this.taskAttempt2 = taskAttempt2;
				{
					this[taskAttemptId2] = taskAttempt2;
				}
			}

			private readonly TaskAttemptId taskAttemptId2;

			private readonly TaskAttempt taskAttempt2;
		}

		private sealed class _Dictionary_127 : Dictionary<TaskId, Task>
		{
			public _Dictionary_127(TaskId taskId1, Task task1, TaskId taskId2, Task task2)
			{
				this.taskId1 = taskId1;
				this.task1 = task1;
				this.taskId2 = taskId2;
				this.task2 = task2;
				{
					this[taskId1] = task1;
					this[taskId2] = task2;
				}
			}

			private readonly TaskId taskId1;

			private readonly Task task1;

			private readonly TaskId taskId2;

			private readonly Task task2;
		}
	}
}
