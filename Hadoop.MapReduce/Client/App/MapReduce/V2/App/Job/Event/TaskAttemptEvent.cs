using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	/// <summary>This class encapsulates task attempt related events.</summary>
	public class TaskAttemptEvent : AbstractEvent<TaskAttemptEventType>
	{
		private TaskAttemptId attemptID;

		/// <summary>Create a new TaskAttemptEvent.</summary>
		/// <param name="id">the id of the task attempt</param>
		/// <param name="type">the type of event that happened.</param>
		public TaskAttemptEvent(TaskAttemptId id, TaskAttemptEventType type)
			: base(type)
		{
			this.attemptID = id;
		}

		public virtual TaskAttemptId GetTaskAttemptID()
		{
			return attemptID;
		}
	}
}
