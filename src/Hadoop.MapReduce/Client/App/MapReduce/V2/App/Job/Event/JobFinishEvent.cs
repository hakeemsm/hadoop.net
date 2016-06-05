using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobFinishEvent : AbstractEvent<JobFinishEvent.Type>
	{
		public enum Type
		{
			StateChanged
		}

		private JobId jobID;

		public JobFinishEvent(JobId jobID)
			: base(JobFinishEvent.Type.StateChanged)
		{
			this.jobID = jobID;
		}

		public virtual JobId GetJobId()
		{
			return jobID;
		}
	}
}
