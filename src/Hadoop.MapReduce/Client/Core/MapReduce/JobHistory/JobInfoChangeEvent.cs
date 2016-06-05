using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>
	/// Event to record changes in the submit and launch time of
	/// a job
	/// </summary>
	public class JobInfoChangeEvent : HistoryEvent
	{
		private JobInfoChange datum = new JobInfoChange();

		/// <summary>Create a event to record the submit and launch time of a job</summary>
		/// <param name="id">Job Id</param>
		/// <param name="submitTime">Submit time of the job</param>
		/// <param name="launchTime">Launch time of the job</param>
		public JobInfoChangeEvent(JobID id, long submitTime, long launchTime)
		{
			datum.jobid = new Utf8(id.ToString());
			datum.submitTime = submitTime;
			datum.launchTime = launchTime;
		}

		internal JobInfoChangeEvent()
		{
		}

		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (JobInfoChange)datum;
		}

		/// <summary>Get the Job ID</summary>
		public virtual JobID GetJobId()
		{
			return JobID.ForName(datum.jobid.ToString());
		}

		/// <summary>Get the Job submit time</summary>
		public virtual long GetSubmitTime()
		{
			return datum.submitTime;
		}

		/// <summary>Get the Job launch time</summary>
		public virtual long GetLaunchTime()
		{
			return datum.launchTime;
		}

		public virtual EventType GetEventType()
		{
			return EventType.JobInfoChanged;
		}
	}
}
