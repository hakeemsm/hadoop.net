using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class MockHistoryContext : MockAppContext, HistoryContext
	{
		private readonly IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> partialJobs;

		private readonly IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> fullJobs;

		public MockHistoryContext(int numJobs, int numTasks, int numAttempts)
			: base(0)
		{
			MockHistoryJobs.JobsPair jobs;
			try
			{
				jobs = MockHistoryJobs.NewHistoryJobs(numJobs, numTasks, numAttempts);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
			partialJobs = jobs.partial;
			fullJobs = jobs.full;
		}

		public MockHistoryContext(int appid, int numJobs, int numTasks, int numAttempts)
			: base(appid)
		{
			MockHistoryJobs.JobsPair jobs;
			try
			{
				jobs = MockHistoryJobs.NewHistoryJobs(GetApplicationID(), numJobs, numTasks, numAttempts
					);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
			partialJobs = jobs.partial;
			fullJobs = jobs.full;
		}

		public MockHistoryContext(int appid, int numTasks, int numAttempts, Path confPath
			)
			: base(appid, numTasks, numAttempts, confPath)
		{
			fullJobs = base.GetAllJobs();
			partialJobs = null;
		}

		public MockHistoryContext(int appid, int numJobs, int numTasks, int numAttempts, 
			bool hasFailedTasks)
			: base(appid)
		{
			MockHistoryJobs.JobsPair jobs;
			try
			{
				jobs = MockHistoryJobs.NewHistoryJobs(GetApplicationID(), numJobs, numTasks, numAttempts
					, hasFailedTasks);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
			partialJobs = jobs.partial;
			fullJobs = jobs.full;
		}

		public override Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetJob(JobId jobID)
		{
			return fullJobs[jobID];
		}

		public virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetPartialJob(JobId jobID
			)
		{
			return partialJobs[jobID];
		}

		public override IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllJobs
			()
		{
			return fullJobs;
		}

		public virtual IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllJobs
			(ApplicationId appID)
		{
			return null;
		}

		public virtual JobsInfo GetPartialJobs(long offset, long count, string user, string
			 queue, long sBegin, long sEnd, long fBegin, long fEnd, JobState jobState)
		{
			return CachedHistoryStorage.GetPartialJobs(this.partialJobs.Values, offset, count
				, user, queue, sBegin, sEnd, fBegin, fEnd, jobState);
		}
	}
}
