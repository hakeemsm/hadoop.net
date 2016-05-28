using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobAbortCompletedEvent : JobEvent
	{
		private JobStatus.State finalState;

		public JobAbortCompletedEvent(JobId jobID, JobStatus.State finalState)
			: base(jobID, JobEventType.JobAbortCompleted)
		{
			this.finalState = finalState;
		}

		public virtual JobStatus.State GetFinalState()
		{
			return finalState;
		}
	}
}
