using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobStartEvent : JobEvent
	{
		internal long recoveredJobStartTime;

		public JobStartEvent(JobId jobID)
			: this(jobID, 0)
		{
		}

		public JobStartEvent(JobId jobID, long recoveredJobStartTime)
			: base(jobID, JobEventType.JobStart)
		{
			this.recoveredJobStartTime = recoveredJobStartTime;
		}

		public virtual long GetRecoveredJobStartTime()
		{
			return recoveredJobStartTime;
		}
	}
}
