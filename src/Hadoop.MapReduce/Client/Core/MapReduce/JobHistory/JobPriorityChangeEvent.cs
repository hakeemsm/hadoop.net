using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record the change of priority of a job</summary>
	public class JobPriorityChangeEvent : HistoryEvent
	{
		private JobPriorityChange datum = new JobPriorityChange();

		/// <summary>Generate an event to record changes in Job priority</summary>
		/// <param name="id">Job Id</param>
		/// <param name="priority">The new priority of the job</param>
		public JobPriorityChangeEvent(JobID id, JobPriority priority)
		{
			datum.jobid = new Utf8(id.ToString());
			datum.priority = new Utf8(priority.ToString());
		}

		internal JobPriorityChangeEvent()
		{
		}

		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (JobPriorityChange)datum;
		}

		/// <summary>Get the Job ID</summary>
		public virtual JobID GetJobId()
		{
			return JobID.ForName(datum.jobid.ToString());
		}

		/// <summary>Get the job priority</summary>
		public virtual JobPriority GetPriority()
		{
			return JobPriority.ValueOf(datum.priority.ToString());
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			return EventType.JobPriorityChanged;
		}
	}
}
