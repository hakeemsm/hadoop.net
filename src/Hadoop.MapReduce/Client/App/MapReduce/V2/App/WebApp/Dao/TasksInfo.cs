using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class TasksInfo
	{
		protected internal AList<TaskInfo> task = new AList<TaskInfo>();

		public TasksInfo()
		{
		}

		// JAXB needs this
		public virtual void Add(TaskInfo taskInfo)
		{
			task.AddItem(taskInfo);
		}

		public virtual AList<TaskInfo> GetTasks()
		{
			return task;
		}
	}
}
