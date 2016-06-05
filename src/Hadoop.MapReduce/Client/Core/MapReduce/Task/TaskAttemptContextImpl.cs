using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task
{
	/// <summary>The context for task attempts.</summary>
	public class TaskAttemptContextImpl : JobContextImpl, TaskAttemptContext
	{
		private readonly TaskAttemptID taskId;

		private string status = string.Empty;

		private StatusReporter reporter;

		public TaskAttemptContextImpl(Configuration conf, TaskAttemptID taskId)
			: this(conf, taskId, new TaskAttemptContextImpl.DummyReporter())
		{
		}

		public TaskAttemptContextImpl(Configuration conf, TaskAttemptID taskId, StatusReporter
			 reporter)
			: base(conf, taskId.GetJobID())
		{
			this.taskId = taskId;
			this.reporter = reporter;
		}

		/// <summary>Get the unique name for this task attempt.</summary>
		public virtual TaskAttemptID GetTaskAttemptID()
		{
			return taskId;
		}

		/// <summary>Get the last set status message.</summary>
		/// <returns>the current status message</returns>
		public virtual string GetStatus()
		{
			return status;
		}

		public virtual Counter GetCounter<_T0>(Enum<_T0> counterName)
			where _T0 : Enum<E>
		{
			return reporter.GetCounter(counterName);
		}

		public virtual Counter GetCounter(string groupName, string counterName)
		{
			return reporter.GetCounter(groupName, counterName);
		}

		/// <summary>Report progress.</summary>
		public virtual void Progress()
		{
			reporter.Progress();
		}

		protected internal virtual void SetStatusString(string status)
		{
			this.status = status;
		}

		/// <summary>Set the current status of the task to the given string.</summary>
		public virtual void SetStatus(string status)
		{
			string normalizedStatus = Org.Apache.Hadoop.Mapred.Task.NormalizeStatus(status, conf
				);
			SetStatusString(normalizedStatus);
			reporter.SetStatus(normalizedStatus);
		}

		public class DummyReporter : StatusReporter
		{
			public override void SetStatus(string s)
			{
			}

			public override void Progress()
			{
			}

			public override Counter GetCounter<_T0>(Enum<_T0> name)
			{
				return new Counters().FindCounter(name);
			}

			public override Counter GetCounter(string group, string name)
			{
				return new Counters().FindCounter(group, name);
			}

			public override float GetProgress()
			{
				return 0f;
			}
		}

		public virtual float GetProgress()
		{
			return reporter.GetProgress();
		}
	}
}
