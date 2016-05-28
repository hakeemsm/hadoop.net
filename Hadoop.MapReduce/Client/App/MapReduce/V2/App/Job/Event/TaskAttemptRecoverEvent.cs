using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class TaskAttemptRecoverEvent : TaskAttemptEvent
	{
		private JobHistoryParser.TaskAttemptInfo taInfo;

		private OutputCommitter committer;

		private bool recoverAttemptOutput;

		public TaskAttemptRecoverEvent(TaskAttemptId id, JobHistoryParser.TaskAttemptInfo
			 taInfo, OutputCommitter committer, bool recoverOutput)
			: base(id, TaskAttemptEventType.TaRecover)
		{
			this.taInfo = taInfo;
			this.committer = committer;
			this.recoverAttemptOutput = recoverOutput;
		}

		public virtual JobHistoryParser.TaskAttemptInfo GetTaskAttemptInfo()
		{
			return taInfo;
		}

		public virtual OutputCommitter GetCommitter()
		{
			return committer;
		}

		public virtual bool GetRecoverOutput()
		{
			return recoverAttemptOutput;
		}
	}
}
