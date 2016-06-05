using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class TaskRecoverEvent : TaskEvent
	{
		private JobHistoryParser.TaskInfo taskInfo;

		private OutputCommitter committer;

		private bool recoverTaskOutput;

		public TaskRecoverEvent(TaskId taskID, JobHistoryParser.TaskInfo taskInfo, OutputCommitter
			 committer, bool recoverTaskOutput)
			: base(taskID, TaskEventType.TRecover)
		{
			this.taskInfo = taskInfo;
			this.committer = committer;
			this.recoverTaskOutput = recoverTaskOutput;
		}

		public virtual JobHistoryParser.TaskInfo GetTaskInfo()
		{
			return taskInfo;
		}

		public virtual OutputCommitter GetOutputCommitter()
		{
			return committer;
		}

		public virtual bool GetRecoverTaskOutput()
		{
			return recoverTaskOutput;
		}
	}
}
