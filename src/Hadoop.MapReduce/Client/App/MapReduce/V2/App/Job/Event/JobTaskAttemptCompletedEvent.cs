using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobTaskAttemptCompletedEvent : JobEvent
	{
		private TaskAttemptCompletionEvent completionEvent;

		public JobTaskAttemptCompletedEvent(TaskAttemptCompletionEvent completionEvent)
			: base(completionEvent.GetAttemptId().GetTaskId().GetJobId(), JobEventType.JobTaskAttemptCompleted
				)
		{
			this.completionEvent = completionEvent;
		}

		public virtual TaskAttemptCompletionEvent GetCompletionEvent()
		{
			return completionEvent;
		}
	}
}
