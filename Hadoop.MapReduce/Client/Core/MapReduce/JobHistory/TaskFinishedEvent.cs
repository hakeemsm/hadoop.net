using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record the successful completion of a task</summary>
	public class TaskFinishedEvent : HistoryEvent
	{
		private TaskFinished datum = null;

		private TaskID taskid;

		private TaskAttemptID successfulAttemptId;

		private long finishTime;

		private TaskType taskType;

		private string status;

		private Counters counters;

		/// <summary>Create an event to record the successful completion of a task</summary>
		/// <param name="id">Task ID</param>
		/// <param name="attemptId">Task Attempt ID of the successful attempt for this task</param>
		/// <param name="finishTime">Finish time of the task</param>
		/// <param name="taskType">Type of the task</param>
		/// <param name="status">Status string</param>
		/// <param name="counters">Counters for the task</param>
		public TaskFinishedEvent(TaskID id, TaskAttemptID attemptId, long finishTime, TaskType
			 taskType, string status, Counters counters)
		{
			this.taskid = id;
			this.successfulAttemptId = attemptId;
			this.finishTime = finishTime;
			this.taskType = taskType;
			this.status = status;
			this.counters = counters;
		}

		internal TaskFinishedEvent()
		{
		}

		public virtual object GetDatum()
		{
			if (datum == null)
			{
				datum = new TaskFinished();
				datum.taskid = new Utf8(taskid.ToString());
				if (successfulAttemptId != null)
				{
					datum.successfulAttemptId = new Utf8(successfulAttemptId.ToString());
				}
				datum.finishTime = finishTime;
				datum.counters = EventWriter.ToAvro(counters);
				datum.taskType = new Utf8(taskType.ToString());
				datum.status = new Utf8(status);
			}
			return datum;
		}

		public virtual void SetDatum(object oDatum)
		{
			this.datum = (TaskFinished)oDatum;
			this.taskid = TaskID.ForName(datum.taskid.ToString());
			if (datum.successfulAttemptId != null)
			{
				this.successfulAttemptId = TaskAttemptID.ForName(datum.successfulAttemptId.ToString
					());
			}
			this.finishTime = datum.finishTime;
			this.taskType = TaskType.ValueOf(datum.taskType.ToString());
			this.status = datum.status.ToString();
			this.counters = EventReader.FromAvro(datum.counters);
		}

		/// <summary>Get task id</summary>
		public virtual TaskID GetTaskId()
		{
			return taskid;
		}

		/// <summary>Get successful task attempt id</summary>
		public virtual TaskAttemptID GetSuccessfulTaskAttemptId()
		{
			return successfulAttemptId;
		}

		/// <summary>Get the task finish time</summary>
		public virtual long GetFinishTime()
		{
			return finishTime;
		}

		/// <summary>Get task counters</summary>
		public virtual Counters GetCounters()
		{
			return counters;
		}

		/// <summary>Get task type</summary>
		public virtual TaskType GetTaskType()
		{
			return taskType;
		}

		/// <summary>Get task status</summary>
		public virtual string GetTaskStatus()
		{
			return status.ToString();
		}

		/// <summary>Get event type</summary>
		public virtual EventType GetEventType()
		{
			return EventType.TaskFinished;
		}
	}
}
