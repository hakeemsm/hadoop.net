using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record the change of status for a job</summary>
	public class JobStatusChangedEvent : HistoryEvent
	{
		private JobStatusChanged datum = new JobStatusChanged();

		/// <summary>Create an event to record the change in the Job Status</summary>
		/// <param name="id">Job ID</param>
		/// <param name="jobStatus">The new job status</param>
		public JobStatusChangedEvent(JobID id, string jobStatus)
		{
			datum.jobid = new Utf8(id.ToString());
			datum.jobStatus = new Utf8(jobStatus);
		}

		internal JobStatusChangedEvent()
		{
		}

		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (JobStatusChanged)datum;
		}

		/// <summary>Get the Job Id</summary>
		public virtual JobID GetJobId()
		{
			return JobID.ForName(datum.jobid.ToString());
		}

		/// <summary>Get the event status</summary>
		public virtual string GetStatus()
		{
			return datum.jobStatus.ToString();
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			return EventType.JobStatusChanged;
		}
	}
}
