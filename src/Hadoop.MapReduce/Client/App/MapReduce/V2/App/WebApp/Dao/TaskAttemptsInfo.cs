using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class TaskAttemptsInfo
	{
		protected internal AList<TaskAttemptInfo> taskAttempt = new AList<TaskAttemptInfo
			>();

		public TaskAttemptsInfo()
		{
		}

		// JAXB needs this
		public virtual void Add(TaskAttemptInfo taskattemptInfo)
		{
			taskAttempt.AddItem(taskattemptInfo);
		}

		public virtual AList<TaskAttemptInfo> GetTaskAttempts()
		{
			return taskAttempt;
		}
	}
}
