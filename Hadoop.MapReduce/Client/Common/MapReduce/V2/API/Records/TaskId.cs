using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	/// <summary>
	/// <p>
	/// <code>TaskId</code> represents the unique identifier for a Map or Reduce
	/// Task.
	/// </summary>
	/// <remarks>
	/// <p>
	/// <code>TaskId</code> represents the unique identifier for a Map or Reduce
	/// Task.
	/// </p>
	/// <p>
	/// TaskId consists of 3 parts. First part is <code>JobId</code>, that this Task
	/// belongs to. Second part of the TaskId is either 'm' or 'r' representing
	/// whether the task is a map task or a reduce task. And the third part is the
	/// task number.
	/// </p>
	/// </remarks>
	public abstract class TaskId : Comparable<TaskId>
	{
		/// <returns>the associated <code>JobId</code></returns>
		public abstract JobId GetJobId();

		/// <returns>the type of the task - MAP/REDUCE</returns>
		public abstract TaskType GetTaskType();

		/// <returns>the task number.</returns>
		public abstract int GetId();

		public abstract void SetJobId(JobId jobId);

		public abstract void SetTaskType(TaskType taskType);

		public abstract void SetId(int id);

		protected internal const string Task = "task";

		private sealed class _ThreadLocal_62 : ThreadLocal<NumberFormat>
		{
			public _ThreadLocal_62()
			{
			}

			protected override NumberFormat InitialValue()
			{
				NumberFormat fmt = NumberFormat.GetInstance();
				fmt.SetGroupingUsed(false);
				fmt.SetMinimumIntegerDigits(6);
				return fmt;
			}
		}

		internal static readonly ThreadLocal<NumberFormat> taskIdFormat = new _ThreadLocal_62
			();

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + GetId();
			result = prime * result + GetJobId().GetHashCode();
			result = prime * result + GetTaskType().GetHashCode();
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
			TaskId other = (TaskId)obj;
			if (GetId() != other.GetId())
			{
				return false;
			}
			if (!GetJobId().Equals(other.GetJobId()))
			{
				return false;
			}
			if (GetTaskType() != other.GetTaskType())
			{
				return false;
			}
			return true;
		}

		public override string ToString()
		{
			StringBuilder builder = new StringBuilder(Task);
			JobId jobId = GetJobId();
			builder.Append("_").Append(jobId.GetAppId().GetClusterTimestamp());
			builder.Append("_").Append(JobId.jobIdFormat.Get().Format(jobId.GetAppId().GetId(
				)));
			builder.Append("_");
			builder.Append(GetTaskType() == TaskType.Map ? "m" : "r").Append("_");
			builder.Append(taskIdFormat.Get().Format(GetId()));
			return builder.ToString();
		}

		public virtual int CompareTo(TaskId other)
		{
			int jobIdComp = this.GetJobId().CompareTo(other.GetJobId());
			if (jobIdComp == 0)
			{
				if (this.GetTaskType() == other.GetTaskType())
				{
					return this.GetId() - other.GetId();
				}
				else
				{
					return this.GetTaskType().CompareTo(other.GetTaskType());
				}
			}
			else
			{
				return jobIdComp;
			}
		}
	}
}
