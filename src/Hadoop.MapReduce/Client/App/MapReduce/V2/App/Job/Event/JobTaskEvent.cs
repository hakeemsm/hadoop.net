using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobTaskEvent : JobEvent
	{
		private TaskId taskID;

		private TaskState taskState;

		public JobTaskEvent(TaskId taskID, TaskState taskState)
			: base(taskID.GetJobId(), JobEventType.JobTaskCompleted)
		{
			this.taskID = taskID;
			this.taskState = taskState;
		}

		public virtual TaskId GetTaskID()
		{
			return taskID;
		}

		public virtual TaskState GetState()
		{
			return taskState;
		}
	}
}
