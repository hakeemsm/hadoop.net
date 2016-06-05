using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Commit
{
	public class CommitterJobSetupEvent : CommitterEvent
	{
		private JobId jobID;

		private JobContext jobContext;

		public CommitterJobSetupEvent(JobId jobID, JobContext jobContext)
			: base(CommitterEventType.JobSetup)
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
