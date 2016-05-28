using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	/// <summary>this class encapsulates task related events.</summary>
	public class TaskEvent : AbstractEvent<TaskEventType>
	{
		private TaskId taskID;

		public TaskEvent(TaskId taskID, TaskEventType type)
			: base(type)
		{
			this.taskID = taskID;
		}

		public virtual TaskId GetTaskID()
		{
			return taskID;
		}
	}
}
