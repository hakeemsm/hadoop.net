using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class MockHistoryJobs : MockJobs
	{
		public class JobsPair
		{
			public IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> partial;

			public IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> full;
		}

		/// <exception cref="System.IO.IOException"/>
		public static MockHistoryJobs.JobsPair NewHistoryJobs(int numJobs, int numTasksPerJob
			, int numAttemptsPerTask)
		{
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> mocked = NewJobs(numJobs
				, numTasksPerJob, numAttemptsPerTask);
			return Split(mocked);
		}

		/// <exception cref="System.IO.IOException"/>
		public static MockHistoryJobs.JobsPair NewHistoryJobs(ApplicationId appID, int numJobsPerApp
			, int numTasksPerJob, int numAttemptsPerTask)
		{
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> mocked = NewJobs(appID
				, numJobsPerApp, numTasksPerJob, numAttemptsPerTask);
			return Split(mocked);
		}

		/// <exception cref="System.IO.IOException"/>
		public static MockHistoryJobs.JobsPair NewHistoryJobs(ApplicationId appID, int numJobsPerApp
			, int numTasksPerJob, int numAttemptsPerTask, bool hasFailedTasks)
		{
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> mocked = NewJobs(appID
				, numJobsPerApp, numTasksPerJob, numAttemptsPerTask, hasFailedTasks);
			return Split(mocked);
		}

		/// <exception cref="System.IO.IOException"/>
		private static MockHistoryJobs.JobsPair Split(IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			> mocked)
		{
			MockHistoryJobs.JobsPair ret = new MockHistoryJobs.JobsPair();
			ret.full = Maps.NewHashMap();
			ret.partial = Maps.NewHashMap();
			foreach (KeyValuePair<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> entry in 
				mocked)
			{
				JobId id = entry.Key;
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job j = entry.Value;
				MockHistoryJobs.MockCompletedJob mockJob = new MockHistoryJobs.MockCompletedJob(j
					);
				// use MockCompletedJob to set everything below to make sure
				// consistent with what history server would do
				ret.full[id] = mockJob;
				JobReport report = mockJob.GetReport();
				JobIndexInfo info = new JobIndexInfo(report.GetStartTime(), report.GetFinishTime(
					), mockJob.GetUserName(), mockJob.GetName(), id, mockJob.GetCompletedMaps(), mockJob
					.GetCompletedReduces(), mockJob.GetState().ToString());
				info.SetJobStartTime(report.GetStartTime());
				info.SetQueueName(mockJob.GetQueueName());
				ret.partial[id] = new PartialJob(info, id);
			}
			return ret;
		}

		private class MockCompletedJob : CompletedJob
		{
			private Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job;

			/// <exception cref="System.IO.IOException"/>
			public MockCompletedJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
				: base(new Configuration(), job.GetID(), null, true, job.GetUserName(), null, null
					)
			{
				this.job = job;
			}

			public override int GetCompletedMaps()
			{
				// we always return total since this is history server
				// and PartialJob also assumes completed - total
				return job.GetTotalMaps();
			}

			public override int GetCompletedReduces()
			{
				// we always return total since this is history server
				// and PartialJob also assumes completed - total
				return job.GetTotalReduces();
			}

			public override Counters GetAllCounters()
			{
				return job.GetAllCounters();
			}

			public override JobId GetID()
			{
				return job.GetID();
			}

			public override JobReport GetReport()
			{
				return job.GetReport();
			}

			public override float GetProgress()
			{
				return job.GetProgress();
			}

			public override JobState GetState()
			{
				return job.GetState();
			}

			public override Task GetTask(TaskId taskId)
			{
				return job.GetTask(taskId);
			}

			public override TaskAttemptCompletionEvent[] GetTaskAttemptCompletionEvents(int fromEventId
				, int maxEvents)
			{
				return job.GetTaskAttemptCompletionEvents(fromEventId, maxEvents);
			}

			public override TaskCompletionEvent[] GetMapAttemptCompletionEvents(int startIndex
				, int maxEvents)
			{
				return job.GetMapAttemptCompletionEvents(startIndex, maxEvents);
			}

			public override IDictionary<TaskId, Task> GetTasks()
			{
				return job.GetTasks();
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void LoadFullHistoryData(bool loadTasks, Path historyFileAbsolute
				)
			{
			}

			//Empty
			public override IList<string> GetDiagnostics()
			{
				return job.GetDiagnostics();
			}

			public override string GetName()
			{
				return job.GetName();
			}

			public override string GetQueueName()
			{
				return job.GetQueueName();
			}

			public override int GetTotalMaps()
			{
				return job.GetTotalMaps();
			}

			public override int GetTotalReduces()
			{
				return job.GetTotalReduces();
			}

			public override bool IsUber()
			{
				return job.IsUber();
			}

			public override IDictionary<TaskId, Task> GetTasks(TaskType taskType)
			{
				return job.GetTasks();
			}

			public override bool CheckAccess(UserGroupInformation callerUGI, JobACL jobOperation
				)
			{
				return job.CheckAccess(callerUGI, jobOperation);
			}

			public override IDictionary<JobACL, AccessControlList> GetJobACLs()
			{
				return job.GetJobACLs();
			}

			public override string GetUserName()
			{
				return job.GetUserName();
			}

			public override Path GetConfFile()
			{
				return job.GetConfFile();
			}

			public override IList<AMInfo> GetAMInfos()
			{
				return job.GetAMInfos();
			}
		}
	}
}
