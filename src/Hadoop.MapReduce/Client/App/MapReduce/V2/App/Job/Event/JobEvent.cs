using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	/// <summary>This class encapsulates job related events.</summary>
	public class JobEvent : AbstractEvent<JobEventType>
	{
		private JobId jobID;

		public JobEvent(JobId jobID, JobEventType type)
			: base(type)
		{
			this.jobID = jobID;
		}

		public virtual JobId GetJobId()
		{
			return jobID;
		}
	}
}
