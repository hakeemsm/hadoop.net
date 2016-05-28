using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class JobsInfo
	{
		protected internal AList<JobInfo> job = new AList<JobInfo>();

		public JobsInfo()
		{
		}

		// JAXB needs this
		public virtual void Add(JobInfo jobInfo)
		{
			job.AddItem(jobInfo);
		}

		public virtual AList<JobInfo> GetJobs()
		{
			return job;
		}
	}
}
