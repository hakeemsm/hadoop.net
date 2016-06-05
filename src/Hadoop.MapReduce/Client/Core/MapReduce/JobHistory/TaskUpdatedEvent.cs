using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record updates to a task</summary>
	public class TaskUpdatedEvent : HistoryEvent
	{
		private TaskUpdated datum = new TaskUpdated();

		/// <summary>Create an event to record task updates</summary>
		/// <param name="id">Id of the task</param>
		/// <param name="finishTime">Finish time of the task</param>
		public TaskUpdatedEvent(TaskID id, long finishTime)
		{
			datum.taskid = new Utf8(id.ToString());
			datum.finishTime = finishTime;
		}

		internal TaskUpdatedEvent()
		{
		}

		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (TaskUpdated)datum;
		}

		/// <summary>Get the task ID</summary>
		public virtual TaskID GetTaskId()
		{
			return TaskID.ForName(datum.taskid.ToString());
		}

		/// <summary>Get the task finish time</summary>
		public virtual long GetFinishTime()
		{
			return datum.finishTime;
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			return EventType.TaskUpdated;
		}
	}
}
