using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Commit
{
	public class CommitterTaskAbortEvent : CommitterEvent
	{
		private readonly TaskAttemptId attemptID;

		private readonly TaskAttemptContext attemptContext;

		public CommitterTaskAbortEvent(TaskAttemptId attemptID, TaskAttemptContext attemptContext
			)
			: base(CommitterEventType.TaskAbort)
		{
			this.attemptID = attemptID;
			this.attemptContext = attemptContext;
		}

		public virtual TaskAttemptId GetAttemptID()
		{
			return attemptID;
		}

		public virtual TaskAttemptContext GetAttemptContext()
		{
			return attemptContext;
		}
	}
}
