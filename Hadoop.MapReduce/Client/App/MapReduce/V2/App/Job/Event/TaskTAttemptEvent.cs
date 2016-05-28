using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class TaskTAttemptEvent : TaskEvent
	{
		private TaskAttemptId attemptID;

		public TaskTAttemptEvent(TaskAttemptId id, TaskEventType type)
			: base(id.GetTaskId(), type)
		{
			this.attemptID = id;
		}

		public virtual TaskAttemptId GetTaskAttemptID()
		{
			return attemptID;
		}
	}
}
