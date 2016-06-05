using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record the failure of a task</summary>
	public class TaskFailedEvent : HistoryEvent
	{
		private TaskFailed datum = null;

		private TaskAttemptID failedDueToAttempt;

		private TaskID id;

		private TaskType taskType;

		private long finishTime;

		private string status;

		private string error;

		private Counters counters;

		private static readonly Counters EmptyCounters = new Counters();

		/// <summary>Create an event to record task failure</summary>
		/// <param name="id">Task ID</param>
		/// <param name="finishTime">Finish time of the task</param>
		/// <param name="taskType">Type of the task</param>
		/// <param name="error">Error String</param>
		/// <param name="status">Status</param>
		/// <param name="failedDueToAttempt">The attempt id due to which the task failed</param>
		/// <param name="counters">Counters for the task</param>
		public TaskFailedEvent(TaskID id, long finishTime, TaskType taskType, string error
			, string status, TaskAttemptID failedDueToAttempt, Counters counters)
		{
			this.id = id;
			this.finishTime = finishTime;
			this.taskType = taskType;
			this.error = error;
			this.status = status;
			this.failedDueToAttempt = failedDueToAttempt;
			this.counters = counters;
		}

		public TaskFailedEvent(TaskID id, long finishTime, TaskType taskType, string error
			, string status, TaskAttemptID failedDueToAttempt)
			: this(id, finishTime, taskType, error, status, failedDueToAttempt, EmptyCounters
				)
		{
		}

		internal TaskFailedEvent()
		{
		}

		public virtual object GetDatum()
		{
			if (datum == null)
			{
				datum = new TaskFailed();
				datum.taskid = new Utf8(id.ToString());
				datum.error = new Utf8(error);
				datum.finishTime = finishTime;
				datum.taskType = new Utf8(taskType.ToString());
				datum.failedDueToAttempt = failedDueToAttempt == null ? null : new Utf8(failedDueToAttempt
					.ToString());
				datum.status = new Utf8(status);
				datum.counters = EventWriter.ToAvro(counters);
			}
			return datum;
		}

		public virtual void SetDatum(object odatum)
		{
			this.datum = (TaskFailed)odatum;
			this.id = TaskID.ForName(datum.taskid.ToString());
			this.taskType = TaskType.ValueOf(datum.taskType.ToString());
			this.finishTime = datum.finishTime;
			this.error = datum.error.ToString();
			this.failedDueToAttempt = datum.failedDueToAttempt == null ? null : TaskAttemptID
				.ForName(datum.failedDueToAttempt.ToString());
			this.status = datum.status.ToString();
			this.counters = EventReader.FromAvro(datum.counters);
		}

		/// <summary>Get the task id</summary>
		public virtual TaskID GetTaskId()
		{
			return id;
		}

		/// <summary>Get the error string</summary>
		public virtual string GetError()
		{
			return error;
		}

		/// <summary>Get the finish time of the attempt</summary>
		public virtual long GetFinishTime()
		{
			return finishTime;
		}

		/// <summary>Get the task type</summary>
		public virtual TaskType GetTaskType()
		{
			return taskType;
		}

		/// <summary>Get the attempt id due to which the task failed</summary>
		public virtual TaskAttemptID GetFailedAttemptID()
		{
			return failedDueToAttempt;
		}

		/// <summary>Get the task status</summary>
		public virtual string GetTaskStatus()
		{
			return status;
		}

		/// <summary>Get task counters</summary>
		public virtual Counters GetCounters()
		{
			return counters;
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			return EventType.TaskFailed;
		}
	}
}
