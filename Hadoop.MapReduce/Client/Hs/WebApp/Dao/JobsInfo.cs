using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao
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
			this.job.AddItem(jobInfo);
		}

		public virtual AList<JobInfo> GetJobs()
		{
			return this.job;
		}
	}
}
