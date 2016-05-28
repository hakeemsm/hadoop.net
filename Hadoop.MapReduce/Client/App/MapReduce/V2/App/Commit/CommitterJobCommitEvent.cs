using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Commit
{
	public class CommitterJobCommitEvent : CommitterEvent
	{
		private JobId jobID;

		private JobContext jobContext;

		public CommitterJobCommitEvent(JobId jobID, JobContext jobContext)
			: base(CommitterEventType.JobCommit)
		{
			this.jobID = jobID;
			this.jobContext = jobContext;
		}

		public virtual JobId GetJobID()
		{
			return jobID;
		}

		public virtual JobContext GetJobContext()
		{
			return jobContext;
		}
	}
}
