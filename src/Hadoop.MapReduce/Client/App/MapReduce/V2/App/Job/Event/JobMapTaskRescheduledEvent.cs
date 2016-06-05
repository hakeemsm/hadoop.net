using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobMapTaskRescheduledEvent : JobEvent
	{
		private TaskId taskID;

		public JobMapTaskRescheduledEvent(TaskId taskID)
			: base(taskID.GetJobId(), JobEventType.JobMapTaskRescheduled)
		{
			this.taskID = taskID;
		}

		public virtual TaskId GetTaskID()
		{
			return taskID;
		}
	}
}
