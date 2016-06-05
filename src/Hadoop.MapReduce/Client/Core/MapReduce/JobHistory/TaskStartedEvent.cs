using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record the start of a task</summary>
	public class TaskStartedEvent : HistoryEvent
	{
		private TaskStarted datum = new TaskStarted();

		/// <summary>Create an event to record start of a task</summary>
		/// <param name="id">Task Id</param>
		/// <param name="startTime">Start time of the task</param>
		/// <param name="taskType">Type of the task</param>
		/// <param name="splitLocations">Split locations, applicable for map tasks</param>
		public TaskStartedEvent(TaskID id, long startTime, TaskType taskType, string splitLocations
			)
		{
			datum.taskid = new Utf8(id.ToString());
			datum.splitLocations = new Utf8(splitLocations);
			datum.startTime = startTime;
			datum.taskType = new Utf8(taskType.ToString());
		}

		internal TaskStartedEvent()
		{
		}

		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (TaskStarted)datum;
		}

		/// <summary>Get the task id</summary>
		public virtual TaskID GetTaskId()
		{
			return TaskID.ForName(datum.taskid.ToString());
		}

		/// <summary>Get the split locations, applicable for map tasks</summary>
		public virtual string GetSplitLocations()
		{
			return datum.splitLocations.ToString();
		}

		/// <summary>Get the start time of the task</summary>
		public virtual long GetStartTime()
		{
			return datum.startTime;
		}

		/// <summary>Get the task type</summary>
		public virtual TaskType GetTaskType()
		{
			return TaskType.ValueOf(datum.taskType.ToString());
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			return EventType.TaskStarted;
		}
	}
}
