using System.Collections.Generic;
using Com.Google.Common.Base;
using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record Failed and Killed completion of jobs</summary>
	public class JobUnsuccessfulCompletionEvent : HistoryEvent
	{
		private const string Nodiags = string.Empty;

		private static readonly IEnumerable<string> NodiagsList = Sharpen.Collections.SingletonList
			(Nodiags);

		private JobUnsuccessfulCompletion datum = new JobUnsuccessfulCompletion();

		/// <summary>Create an event to record unsuccessful completion (killed/failed) of jobs
		/// 	</summary>
		/// <param name="id">Job ID</param>
		/// <param name="finishTime">Finish time of the job</param>
		/// <param name="finishedMaps">Number of finished maps</param>
		/// <param name="finishedReduces">Number of finished reduces</param>
		/// <param name="status">Status of the job</param>
		public JobUnsuccessfulCompletionEvent(JobID id, long finishTime, int finishedMaps
			, int finishedReduces, string status)
			: this(id, finishTime, finishedMaps, finishedReduces, status, NodiagsList)
		{
		}

		/// <summary>Create an event to record unsuccessful completion (killed/failed) of jobs
		/// 	</summary>
		/// <param name="id">Job ID</param>
		/// <param name="finishTime">Finish time of the job</param>
		/// <param name="finishedMaps">Number of finished maps</param>
		/// <param name="finishedReduces">Number of finished reduces</param>
		/// <param name="status">Status of the job</param>
		/// <param name="diagnostics">job runtime diagnostics</param>
		public JobUnsuccessfulCompletionEvent(JobID id, long finishTime, int finishedMaps
			, int finishedReduces, string status, IEnumerable<string> diagnostics)
		{
			datum.SetJobid(new Utf8(id.ToString()));
			datum.SetFinishTime(finishTime);
			datum.SetFinishedMaps(finishedMaps);
			datum.SetFinishedReduces(finishedReduces);
			datum.SetJobStatus(new Utf8(status));
			if (diagnostics == null)
			{
				diagnostics = NodiagsList;
			}
			datum.SetDiagnostics(new Utf8(Joiner.On('\n').SkipNulls().Join(diagnostics)));
		}

		internal JobUnsuccessfulCompletionEvent()
		{
		}

		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (JobUnsuccessfulCompletion)datum;
		}

		/// <summary>Get the Job ID</summary>
		public virtual JobID GetJobId()
		{
			return JobID.ForName(datum.jobid.ToString());
		}

		/// <summary>Get the job finish time</summary>
		public virtual long GetFinishTime()
		{
			return datum.GetFinishTime();
		}

		/// <summary>Get the number of finished maps</summary>
		public virtual int GetFinishedMaps()
		{
			return datum.GetFinishedMaps();
		}

		/// <summary>Get the number of finished reduces</summary>
		public virtual int GetFinishedReduces()
		{
			return datum.GetFinishedReduces();
		}

		/// <summary>Get the status</summary>
		public virtual string GetStatus()
		{
			return datum.GetJobStatus().ToString();
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			if ("FAILED".Equals(GetStatus()))
			{
				return EventType.JobFailed;
			}
			else
			{
				if ("ERROR".Equals(GetStatus()))
				{
					return EventType.JobError;
				}
				else
				{
					return EventType.JobKilled;
				}
			}
		}

		/// <summary>Retrieves diagnostics information preserved in the history file</summary>
		/// <returns>diagnostics as of the time of job termination</returns>
		public virtual string GetDiagnostics()
		{
			CharSequence diagnostics = datum.GetDiagnostics();
			return diagnostics == null ? Nodiags : diagnostics.ToString();
		}
	}
}
