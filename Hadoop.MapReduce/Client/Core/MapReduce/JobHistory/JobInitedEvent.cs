using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record the initialization of a job</summary>
	public class JobInitedEvent : HistoryEvent
	{
		private JobInited datum = new JobInited();

		/// <summary>Create an event to record job initialization</summary>
		/// <param name="id"/>
		/// <param name="launchTime"/>
		/// <param name="totalMaps"/>
		/// <param name="totalReduces"/>
		/// <param name="jobStatus"/>
		/// <param name="uberized">True if the job's map and reduce stages were combined</param>
		public JobInitedEvent(JobID id, long launchTime, int totalMaps, int totalReduces, 
			string jobStatus, bool uberized)
		{
			datum.jobid = new Utf8(id.ToString());
			datum.launchTime = launchTime;
			datum.totalMaps = totalMaps;
			datum.totalReduces = totalReduces;
			datum.jobStatus = new Utf8(jobStatus);
			datum.uberized = uberized;
		}

		internal JobInitedEvent()
		{
		}

		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (JobInited)datum;
		}

		/// <summary>Get the job ID</summary>
		public virtual JobID GetJobId()
		{
			return JobID.ForName(datum.jobid.ToString());
		}

		/// <summary>Get the launch time</summary>
		public virtual long GetLaunchTime()
		{
			return datum.launchTime;
		}

		/// <summary>Get the total number of maps</summary>
		public virtual int GetTotalMaps()
		{
			return datum.totalMaps;
		}

		/// <summary>Get the total number of reduces</summary>
		public virtual int GetTotalReduces()
		{
			return datum.totalReduces;
		}

		/// <summary>Get the status</summary>
		public virtual string GetStatus()
		{
			return datum.jobStatus.ToString();
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			return EventType.JobInited;
		}

		/// <summary>Get whether the job's map and reduce stages were combined</summary>
		public virtual bool GetUberized()
		{
			return datum.uberized;
		}
	}
}
