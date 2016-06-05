using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobCommitFailedEvent : JobEvent
	{
		private string message;

		public JobCommitFailedEvent(JobId jobID, string message)
			: base(jobID, JobEventType.JobCommitFailed)
		{
			this.message = message;
		}

		public virtual string GetMessage()
		{
			return this.message;
		}
	}
}
