using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record successful completion of job</summary>
	public class JobFinishedEvent : HistoryEvent
	{
		private JobFinished datum = null;

		private JobID jobId;

		private long finishTime;

		private int finishedMaps;

		private int finishedReduces;

		private int failedMaps;

		private int failedReduces;

		private Counters mapCounters;

		private Counters reduceCounters;

		private Counters totalCounters;

		/// <summary>Create an event to record successful job completion</summary>
		/// <param name="id">Job ID</param>
		/// <param name="finishTime">Finish time of the job</param>
		/// <param name="finishedMaps">The number of finished maps</param>
		/// <param name="finishedReduces">The number of finished reduces</param>
		/// <param name="failedMaps">The number of failed maps</param>
		/// <param name="failedReduces">The number of failed reduces</param>
		/// <param name="mapCounters">Map Counters for the job</param>
		/// <param name="reduceCounters">Reduce Counters for the job</param>
		/// <param name="totalCounters">Total Counters for the job</param>
		public JobFinishedEvent(JobID id, long finishTime, int finishedMaps, int finishedReduces
			, int failedMaps, int failedReduces, Counters mapCounters, Counters reduceCounters
			, Counters totalCounters)
		{
			this.jobId = id;
			this.finishTime = finishTime;
			this.finishedMaps = finishedMaps;
			this.finishedReduces = finishedReduces;
			this.failedMaps = failedMaps;
			this.failedReduces = failedReduces;
			this.mapCounters = mapCounters;
			this.reduceCounters = reduceCounters;
			this.totalCounters = totalCounters;
		}

		internal JobFinishedEvent()
		{
		}

		public virtual object GetDatum()
		{
			if (datum == null)
			{
				datum = new JobFinished();
				datum.jobid = new Utf8(jobId.ToString());
				datum.finishTime = finishTime;
				datum.finishedMaps = finishedMaps;
				datum.finishedReduces = finishedReduces;
				datum.failedMaps = failedMaps;
				datum.failedReduces = failedReduces;
				datum.mapCounters = EventWriter.ToAvro(mapCounters, "MAP_COUNTERS");
				datum.reduceCounters = EventWriter.ToAvro(reduceCounters, "REDUCE_COUNTERS");
				datum.totalCounters = EventWriter.ToAvro(totalCounters, "TOTAL_COUNTERS");
			}
			return datum;
		}

		public virtual void SetDatum(object oDatum)
		{
			this.datum = (JobFinished)oDatum;
			this.jobId = JobID.ForName(datum.jobid.ToString());
			this.finishTime = datum.finishTime;
			this.finishedMaps = datum.finishedMaps;
			this.finishedReduces = datum.finishedReduces;
			this.failedMaps = datum.failedMaps;
			this.failedReduces = datum.failedReduces;
			this.mapCounters = EventReader.FromAvro(datum.mapCounters);
			this.reduceCounters = EventReader.FromAvro(datum.reduceCounters);
			this.totalCounters = EventReader.FromAvro(datum.totalCounters);
		}

		public virtual EventType GetEventType()
		{
			return EventType.JobFinished;
		}

		/// <summary>Get the Job ID</summary>
		public virtual JobID GetJobid()
		{
			return jobId;
		}

		/// <summary>Get the job finish time</summary>
		public virtual long GetFinishTime()
		{
			return finishTime;
		}

		/// <summary>Get the number of finished maps for the job</summary>
		public virtual int GetFinishedMaps()
		{
			return finishedMaps;
		}

		/// <summary>Get the number of finished reducers for the job</summary>
		public virtual int GetFinishedReduces()
		{
			return finishedReduces;
		}

		/// <summary>Get the number of failed maps for the job</summary>
		public virtual int GetFailedMaps()
		{
			return failedMaps;
		}

		/// <summary>Get the number of failed reducers for the job</summary>
		public virtual int GetFailedReduces()
		{
			return failedReduces;
		}

		/// <summary>Get the counters for the job</summary>
		public virtual Counters GetTotalCounters()
		{
			return totalCounters;
		}

		/// <summary>Get the Map counters for the job</summary>
		public virtual Counters GetMapCounters()
		{
			return mapCounters;
		}

		/// <summary>Get the reduce counters for the job</summary>
		public virtual Counters GetReduceCounters()
		{
			return reduceCounters;
		}
	}
}
