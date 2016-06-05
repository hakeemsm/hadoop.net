using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	public class JobQueueChangeEvent : HistoryEvent
	{
		private JobQueueChange datum = new JobQueueChange();

		public JobQueueChangeEvent(JobID id, string queueName)
		{
			datum.jobid = new Utf8(id.ToString());
			datum.jobQueueName = new Utf8(queueName);
		}

		internal JobQueueChangeEvent()
		{
		}

		public virtual EventType GetEventType()
		{
			return EventType.JobQueueChanged;
		}

		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (JobQueueChange)datum;
		}

		/// <summary>Get the Job ID</summary>
		public virtual JobID GetJobId()
		{
			return JobID.ForName(datum.jobid.ToString());
		}

		/// <summary>Get the new Job queue name</summary>
		public virtual string GetJobQueueName()
		{
			if (datum.jobQueueName != null)
			{
				return datum.jobQueueName.ToString();
			}
			return null;
		}
	}
}
