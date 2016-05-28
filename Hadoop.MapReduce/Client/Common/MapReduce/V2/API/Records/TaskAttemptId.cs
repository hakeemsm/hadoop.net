using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	/// <summary>
	/// <p>
	/// <code>TaskAttemptId</code> represents the unique identifier for a task
	/// attempt.
	/// </summary>
	/// <remarks>
	/// <p>
	/// <code>TaskAttemptId</code> represents the unique identifier for a task
	/// attempt. Each task attempt is one particular instance of a Map or Reduce Task
	/// identified by its TaskId.
	/// </p>
	/// <p>
	/// TaskAttemptId consists of 2 parts. First part is the <code>TaskId</code>,
	/// that this <code>TaskAttemptId</code> belongs to. Second part is the task
	/// attempt number.
	/// </p>
	/// </remarks>
	public abstract class TaskAttemptId : Comparable<TaskAttemptId>
	{
		/// <returns>the associated TaskId.</returns>
		public abstract TaskId GetTaskId();

		/// <returns>the attempt id.</returns>
		public abstract int GetId();

		public abstract void SetTaskId(TaskId taskId);

		public abstract void SetId(int id);

		protected internal const string Taskattempt = "attempt";

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + GetId();
			result = prime * result + ((GetTaskId() == null) ? 0 : GetTaskId().GetHashCode());
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			TaskAttemptId other = (TaskAttemptId)obj;
			if (GetId() != other.GetId())
			{
				return false;
			}
			if (!GetTaskId().Equals(other.GetTaskId()))
			{
				return false;
			}
			return true;
		}

		public override string ToString()
		{
			StringBuilder builder = new StringBuilder(Taskattempt);
			TaskId taskId = GetTaskId();
			builder.Append("_").Append(taskId.GetJobId().GetAppId().GetClusterTimestamp());
			builder.Append("_").Append(JobId.jobIdFormat.Get().Format(GetTaskId().GetJobId().
				GetAppId().GetId()));
			builder.Append("_");
			builder.Append(taskId.GetTaskType() == TaskType.Map ? "m" : "r");
			builder.Append("_").Append(TaskId.taskIdFormat.Get().Format(taskId.GetId()));
			builder.Append("_");
			builder.Append(GetId());
			return builder.ToString();
		}

		public virtual int CompareTo(TaskAttemptId other)
		{
			int taskIdComp = this.GetTaskId().CompareTo(other.GetTaskId());
			if (taskIdComp == 0)
			{
				return this.GetId() - other.GetId();
			}
			else
			{
				return taskIdComp;
			}
		}
	}
}
