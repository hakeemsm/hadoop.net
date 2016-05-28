using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	/// <summary>Provides an API to query jobs that have finished.</summary>
	/// <remarks>
	/// Provides an API to query jobs that have finished.
	/// For those implementing this API be aware that there is no feedback when
	/// files are removed from HDFS.  You may rely on HistoryFileManager to help
	/// you know when that has happened if you have not made a complete backup of
	/// the data stored on HDFS.
	/// </remarks>
	public interface HistoryStorage
	{
		/// <summary>
		/// Give the Storage a reference to a class that can be used to interact with
		/// history files.
		/// </summary>
		/// <param name="hsManager">the class that is used to interact with history files.</param>
		void SetHistoryFileManager(HistoryFileManager hsManager);

		/// <summary>Look for a set of partial jobs.</summary>
		/// <param name="offset">the offset into the list of jobs.</param>
		/// <param name="count">the maximum number of jobs to return.</param>
		/// <param name="user">only return jobs for the given user.</param>
		/// <param name="queue">only return jobs for in the given queue.</param>
		/// <param name="sBegin">only return Jobs that started on or after the given time.</param>
		/// <param name="sEnd">only return Jobs that started on or before the given time.</param>
		/// <param name="fBegin">only return Jobs that ended on or after the given time.</param>
		/// <param name="fEnd">only return Jobs that ended on or before the given time.</param>
		/// <param name="jobState">only return Jobs that are in the given job state.</param>
		/// <returns>The list of filtered jobs.</returns>
		JobsInfo GetPartialJobs(long offset, long count, string user, string queue, long 
			sBegin, long sEnd, long fBegin, long fEnd, JobState jobState);

		/// <summary>Get all of the cached jobs.</summary>
		/// <remarks>
		/// Get all of the cached jobs.  This only returns partial jobs and is here for
		/// legacy reasons.
		/// </remarks>
		/// <returns>all of the cached jobs</returns>
		IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllPartialJobs(
			);

		/// <summary>Get a fully parsed job.</summary>
		/// <param name="jobId">the id of the job</param>
		/// <returns>the job, or null if it is not found.</returns>
		Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetFullJob(JobId jobId);
	}
}
