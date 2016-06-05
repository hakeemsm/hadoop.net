using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class TaskAttemptKillEvent : TaskAttemptEvent
	{
		private readonly string message;

		public TaskAttemptKillEvent(TaskAttemptId attemptID, string message)
			: base(attemptID, TaskAttemptEventType.TaKill)
		{
			this.message = message;
		}

		public virtual string GetMessage()
		{
			return message;
		}
	}
}
