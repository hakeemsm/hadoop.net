using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobSetupCompletedEvent : JobEvent
	{
		public JobSetupCompletedEvent(JobId jobID)
			: base(jobID, JobEventType.JobSetupCompleted)
		{
		}
	}
}
