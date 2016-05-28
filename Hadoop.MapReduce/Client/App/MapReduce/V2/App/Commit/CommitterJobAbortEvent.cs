using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Commit
{
	public class CommitterJobAbortEvent : CommitterEvent
	{
		private JobId jobID;

		private JobContext jobContext;

		private JobStatus.State finalState;

		public CommitterJobAbortEvent(JobId jobID, JobContext jobContext, JobStatus.State
			 finalState)
			: base(CommitterEventType.JobAbort)
		{
			this.jobID = jobID;
			this.jobContext = jobContext;
			this.finalState = finalState;
		}

		public virtual JobId GetJobID()
		{
			return jobID;
		}

		public virtual JobContext GetJobContext()
		{
			return jobContext;
		}

		public virtual JobStatus.State GetFinalState()
		{
			return finalState;
		}
	}
}
