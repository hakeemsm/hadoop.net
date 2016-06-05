using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record unsuccessful (Killed/Failed) completion of task attempts
	/// 	</summary>
	public class TaskAttemptUnsuccessfulCompletionEvent : HistoryEvent
	{
		private TaskAttemptUnsuccessfulCompletion datum = null;

		private TaskAttemptID attemptId;

		private TaskType taskType;

		private string status;

		private long finishTime;

		private string hostname;

		private int port;

		private string rackName;

		private string error;

		private Counters counters;

		internal int[][] allSplits;

		internal int[] clockSplits;

		internal int[] cpuUsages;

		internal int[] vMemKbytes;

		internal int[] physMemKbytes;

		private static readonly Counters EmptyCounters = new Counters();

		/// <summary>Create an event to record the unsuccessful completion of attempts</summary>
		/// <param name="id">Attempt ID</param>
		/// <param name="taskType">Type of the task</param>
		/// <param name="status">Status of the attempt</param>
		/// <param name="finishTime">Finish time of the attempt</param>
		/// <param name="hostname">Name of the host where the attempt executed</param>
		/// <param name="port">rpc port for for the tracker</param>
		/// <param name="rackName">Name of the rack where the attempt executed</param>
		/// <param name="error">Error string</param>
		/// <param name="counters">Counters for the attempt</param>
		/// <param name="allSplits">
		/// the "splits", or a pixelated graph of various
		/// measurable worker node state variables against progress.
		/// Currently there are four; wallclock time, CPU time,
		/// virtual memory and physical memory.
		/// </param>
		public TaskAttemptUnsuccessfulCompletionEvent(TaskAttemptID id, TaskType taskType
			, string status, long finishTime, string hostname, int port, string rackName, string
			 error, Counters counters, int[][] allSplits)
		{
			this.attemptId = id;
			this.taskType = taskType;
			this.status = status;
			this.finishTime = finishTime;
			this.hostname = hostname;
			this.port = port;
			this.rackName = rackName;
			this.error = error;
			this.counters = counters;
			this.allSplits = allSplits;
			this.clockSplits = ProgressSplitsBlock.ArrayGetWallclockTime(allSplits);
			this.cpuUsages = ProgressSplitsBlock.ArrayGetCPUTime(allSplits);
			this.vMemKbytes = ProgressSplitsBlock.ArrayGetVMemKbytes(allSplits);
			this.physMemKbytes = ProgressSplitsBlock.ArrayGetPhysMemKbytes(allSplits);
		}

		/// <param name="id">Attempt ID</param>
		/// <param name="taskType">Type of the task</param>
		/// <param name="status">Status of the attempt</param>
		/// <param name="finishTime">Finish time of the attempt</param>
		/// <param name="hostname">Name of the host where the attempt executed</param>
		/// <param name="error">Error string</param>
		[System.ObsoleteAttribute(@"please use the constructor with an additional argument, an array of splits arrays instead.  SeeOrg.Apache.Hadoop.Mapred.ProgressSplitsBlock for an explanation of the meaning of that parameter. Create an event to record the unsuccessful completion of attempts"
			)]
		public TaskAttemptUnsuccessfulCompletionEvent(TaskAttemptID id, TaskType taskType
			, string status, long finishTime, string hostname, string error)
			: this(id, taskType, status, finishTime, hostname, -1, string.Empty, error, EmptyCounters
				, null)
		{
		}

		public TaskAttemptUnsuccessfulCompletionEvent(TaskAttemptID id, TaskType taskType
			, string status, long finishTime, string hostname, int port, string rackName, string
			 error, int[][] allSplits)
			: this(id, taskType, status, finishTime, hostname, port, rackName, error, EmptyCounters
				, null)
		{
		}

		internal TaskAttemptUnsuccessfulCompletionEvent()
		{
		}

		public virtual object GetDatum()
		{
			if (datum == null)
			{
				datum = new TaskAttemptUnsuccessfulCompletion();
				datum.taskid = new Utf8(attemptId.GetTaskID().ToString());
				datum.taskType = new Utf8(taskType.ToString());
				datum.attemptId = new Utf8(attemptId.ToString());
				datum.finishTime = finishTime;
				datum.hostname = new Utf8(hostname);
				if (rackName != null)
				{
					datum.rackname = new Utf8(rackName);
				}
				datum.port = port;
				datum.error = new Utf8(error);
				datum.status = new Utf8(status);
				datum.counters = EventWriter.ToAvro(counters);
				datum.clockSplits = AvroArrayUtils.ToAvro(ProgressSplitsBlock.ArrayGetWallclockTime
					(allSplits));
				datum.cpuUsages = AvroArrayUtils.ToAvro(ProgressSplitsBlock.ArrayGetCPUTime(allSplits
					));
				datum.vMemKbytes = AvroArrayUtils.ToAvro(ProgressSplitsBlock.ArrayGetVMemKbytes(allSplits
					));
				datum.physMemKbytes = AvroArrayUtils.ToAvro(ProgressSplitsBlock.ArrayGetPhysMemKbytes
					(allSplits));
			}
			return datum;
		}

		public virtual void SetDatum(object odatum)
		{
			this.datum = (TaskAttemptUnsuccessfulCompletion)odatum;
			this.attemptId = TaskAttemptID.ForName(datum.attemptId.ToString());
			this.taskType = TaskType.ValueOf(datum.taskType.ToString());
			this.finishTime = datum.finishTime;
			this.hostname = datum.hostname.ToString();
			this.rackName = datum.rackname.ToString();
			this.port = datum.port;
			this.status = datum.status.ToString();
			this.error = datum.error.ToString();
			this.counters = EventReader.FromAvro(datum.counters);
			this.clockSplits = AvroArrayUtils.FromAvro(datum.clockSplits);
			this.cpuUsages = AvroArrayUtils.FromAvro(datum.cpuUsages);
			this.vMemKbytes = AvroArrayUtils.FromAvro(datum.vMemKbytes);
			this.physMemKbytes = AvroArrayUtils.FromAvro(datum.physMemKbytes);
		}

		/// <summary>Get the task id</summary>
		public virtual TaskID GetTaskId()
		{
			return attemptId.GetTaskID();
		}

		/// <summary>Get the task type</summary>
		public virtual TaskType GetTaskType()
		{
			return TaskType.ValueOf(taskType.ToString());
		}

		/// <summary>Get the attempt id</summary>
		public virtual TaskAttemptID GetTaskAttemptId()
		{
			return attemptId;
		}

		/// <summary>Get the finish time</summary>
		public virtual long GetFinishTime()
		{
			return finishTime;
		}

		/// <summary>Get the name of the host where the attempt executed</summary>
		public virtual string GetHostname()
		{
			return hostname;
		}

		/// <summary>Get the rpc port for the host where the attempt executed</summary>
		public virtual int GetPort()
		{
			return port;
		}

		/// <summary>Get the rack name of the node where the attempt ran</summary>
		public virtual string GetRackName()
		{
			return rackName == null ? null : rackName.ToString();
		}

		/// <summary>Get the error string</summary>
		public virtual string GetError()
		{
			return error.ToString();
		}

		/// <summary>Get the task status</summary>
		public virtual string GetTaskStatus()
		{
			return status.ToString();
		}

		/// <summary>Get the counters</summary>
		internal virtual Counters GetCounters()
		{
			return counters;
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			// Note that the task type can be setup/map/reduce/cleanup but the 
			// attempt-type can only be map/reduce.
			// find out if the task failed or got killed
			bool failed = TaskStatus.State.Failed.ToString().Equals(GetTaskStatus());
			return GetTaskId().GetTaskType() == TaskType.Map ? (failed ? EventType.MapAttemptFailed
				 : EventType.MapAttemptKilled) : (failed ? EventType.ReduceAttemptFailed : EventType
				.ReduceAttemptKilled);
		}

		public virtual int[] GetClockSplits()
		{
			return clockSplits;
		}

		public virtual int[] GetCpuUsages()
		{
			return cpuUsages;
		}

		public virtual int[] GetVMemKbytes()
		{
			return vMemKbytes;
		}

		public virtual int[] GetPhysMemKbytes()
		{
			return physMemKbytes;
		}
	}
}
