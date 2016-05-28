using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class PartialJob : Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.PartialJob
			));

		private JobIndexInfo jobIndexInfo = null;

		private JobId jobId = null;

		private JobReport jobReport = null;

		public PartialJob(JobIndexInfo jobIndexInfo, JobId jobId)
		{
			this.jobIndexInfo = jobIndexInfo;
			this.jobId = jobId;
			jobReport = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance<JobReport
				>();
			jobReport.SetSubmitTime(jobIndexInfo.GetSubmitTime());
			jobReport.SetStartTime(jobIndexInfo.GetJobStartTime());
			jobReport.SetFinishTime(jobIndexInfo.GetFinishTime());
			jobReport.SetJobState(GetState());
		}

		public virtual JobId GetID()
		{
			//    return jobIndexInfo.getJobId();
			return this.jobId;
		}

		public virtual string GetName()
		{
			return jobIndexInfo.GetJobName();
		}

		public virtual string GetQueueName()
		{
			return jobIndexInfo.GetQueueName();
		}

		public virtual JobState GetState()
		{
			JobState js = null;
			try
			{
				js = JobState.ValueOf(jobIndexInfo.GetJobStatus());
			}
			catch (Exception e)
			{
				// Meant for use by the display UI. Exception would prevent it from being
				// rendered.e Defaulting to KILLED
				Log.Warn("Exception while parsing job state. Defaulting to KILLED", e);
				js = JobState.Killed;
			}
			return js;
		}

		public virtual JobReport GetReport()
		{
			return jobReport;
		}

		public virtual float GetProgress()
		{
			return 1.0f;
		}

		public virtual Counters GetAllCounters()
		{
			return null;
		}

		public virtual IDictionary<TaskId, Task> GetTasks()
		{
			return null;
		}

		public virtual IDictionary<TaskId, Task> GetTasks(TaskType taskType)
		{
			return null;
		}

		public virtual Task GetTask(TaskId taskID)
		{
			return null;
		}

		public virtual IList<string> GetDiagnostics()
		{
			return null;
		}

		public virtual int GetTotalMaps()
		{
			return jobIndexInfo.GetNumMaps();
		}

		public virtual int GetTotalReduces()
		{
			return jobIndexInfo.GetNumReduces();
		}

		public virtual int GetCompletedMaps()
		{
			return jobIndexInfo.GetNumMaps();
		}

		public virtual int GetCompletedReduces()
		{
			return jobIndexInfo.GetNumReduces();
		}

		public virtual bool IsUber()
		{
			return false;
		}

		public virtual TaskAttemptCompletionEvent[] GetTaskAttemptCompletionEvents(int fromEventId
			, int maxEvents)
		{
			return null;
		}

		public virtual TaskCompletionEvent[] GetMapAttemptCompletionEvents(int startIndex
			, int maxEvents)
		{
			return null;
		}

		public virtual bool CheckAccess(UserGroupInformation callerUGI, JobACL jobOperation
			)
		{
			return true;
		}

		public virtual string GetUserName()
		{
			return jobIndexInfo.GetUser();
		}

		public virtual Path GetConfFile()
		{
			throw new InvalidOperationException("Not implemented yet");
		}

		public virtual Configuration LoadConfFile()
		{
			throw new InvalidOperationException("Not implemented yet");
		}

		public virtual IDictionary<JobACL, AccessControlList> GetJobACLs()
		{
			throw new InvalidOperationException("Not implemented yet");
		}

		public virtual IList<AMInfo> GetAMInfos()
		{
			return null;
		}

		public virtual void SetQueueName(string queueName)
		{
			throw new NotSupportedException("Can't set job's queue name in history");
		}
	}
}
