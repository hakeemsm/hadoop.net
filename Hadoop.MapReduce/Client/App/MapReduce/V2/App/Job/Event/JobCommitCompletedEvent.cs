using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobCommitCompletedEvent : JobEvent
	{
		public JobCommitCompletedEvent(JobId jobID)
			: base(jobID, JobEventType.JobCommitCompleted)
		{
		}
	}
}
