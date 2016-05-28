using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobSetupFailedEvent : JobEvent
	{
		private string message;

		public JobSetupFailedEvent(JobId jobID, string message)
			: base(jobID, JobEventType.JobSetupFailed)
		{
			this.message = message;
		}

		public virtual string GetMessage()
		{
			return message;
		}
	}
}
