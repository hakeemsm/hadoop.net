using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record successful task completion</summary>
	public class TaskAttemptFinishedEvent : HistoryEvent
	{
		private TaskAttemptFinished datum = null;

		private TaskAttemptID attemptId;

		private TaskType taskType;

		private string taskStatus;

		private long finishTime;

		private string rackName;

		private string hostname;

		private string state;

		private Counters counters;

		/// <summary>
		/// Create an event to record successful finishes for setup and cleanup
		/// attempts
		/// </summary>
		/// <param name="id">Attempt ID</param>
		/// <param name="taskType">Type of task</param>
		/// <param name="taskStatus">Status of task</param>
		/// <param name="finishTime">Finish time of attempt</param>
		/// <param name="hostname">Host where the attempt executed</param>
		/// <param name="state">State string</param>
		/// <param name="counters">Counters for the attempt</param>
		public TaskAttemptFinishedEvent(TaskAttemptID id, TaskType taskType, string taskStatus
			, long finishTime, string rackName, string hostname, string state, Counters counters
			)
		{
			this.attemptId = id;
			this.taskType = taskType;
			this.taskStatus = taskStatus;
			this.finishTime = finishTime;
			this.rackName = rackName;
			this.hostname = hostname;
			this.state = state;
			this.counters = counters;
		}

		internal TaskAttemptFinishedEvent()
		{
		}

		public virtual object GetDatum()
		{
			if (datum == null)
			{
				datum = new TaskAttemptFinished();
				datum.taskid = new Utf8(attemptId.GetTaskID().ToString());
				datum.attemptId = new Utf8(attemptId.ToString());
				datum.taskType = new Utf8(taskType.ToString());
				datum.taskStatus = new Utf8(taskStatus);
				datum.finishTime = finishTime;
				if (rackName != null)
				{
					datum.rackname = new Utf8(rackName);
				}
				datum.hostname = new Utf8(hostname);
				datum.state = new Utf8(state);
				datum.counters = EventWriter.ToAvro(counters);
			}
			return datum;
		}

		public virtual void SetDatum(object oDatum)
		{
			this.datum = (TaskAttemptFinished)oDatum;
			this.attemptId = TaskAttemptID.ForName(datum.attemptId.ToString());
			this.taskType = TaskType.ValueOf(datum.taskType.ToString());
			this.taskStatus = datum.taskStatus.ToString();
			this.finishTime = datum.finishTime;
			this.rackName = datum.rackname.ToString();
			this.hostname = datum.hostname.ToString();
			this.state = datum.state.ToString();
			this.counters = EventReader.FromAvro(datum.counters);
		}

		/// <summary>Get the task ID</summary>
		public virtual TaskID GetTaskId()
		{
			return attemptId.GetTaskID();
		}

		/// <summary>Get the task attempt id</summary>
		public virtual TaskAttemptID GetAttemptId()
		{
			return attemptId;
		}

		/// <summary>Get the task type</summary>
		public virtual TaskType GetTaskType()
		{
			return taskType;
		}

		/// <summary>Get the task status</summary>
		public virtual string GetTaskStatus()
		{
			return taskStatus.ToString();
		}

		/// <summary>Get the attempt finish time</summary>
		public virtual long GetFinishTime()
		{
			return finishTime;
		}

		/// <summary>Get the host where the attempt executed</summary>
		public virtual string GetHostname()
		{
			return hostname.ToString();
		}

		/// <summary>Get the rackname where the attempt executed</summary>
		public virtual string GetRackName()
		{
			return rackName == null ? null : rackName.ToString();
		}

		/// <summary>Get the state string</summary>
		public virtual string GetState()
		{
			return state.ToString();
		}

		/// <summary>Get the counters for the attempt</summary>
		internal virtual Counters GetCounters()
		{
			return counters;
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			// Note that the task type can be setup/map/reduce/cleanup but the 
			// attempt-type can only be map/reduce.
			return GetTaskId().GetTaskType() == TaskType.Map ? EventType.MapAttemptFinished : 
				EventType.ReduceAttemptFinished;
		}
	}
}
