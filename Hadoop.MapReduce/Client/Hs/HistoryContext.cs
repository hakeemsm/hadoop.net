using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public interface HistoryContext : AppContext
	{
		IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllJobs(ApplicationId
			 appID);

		JobsInfo GetPartialJobs(long offset, long count, string user, string queue, long 
			sBegin, long sEnd, long fBegin, long fEnd, JobState jobState);
	}
}
