using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class TaskInfo
	{
		protected internal long startTime;

		protected internal long finishTime;

		protected internal long elapsedTime;

		protected internal float progress;

		protected internal string id;

		protected internal TaskState state;

		protected internal string type;

		protected internal string successfulAttempt;

		protected internal string status;

		[XmlTransient]
		internal int taskNum;

		[XmlTransient]
		internal TaskAttempt successful;

		public TaskInfo()
		{
		}

		public TaskInfo(Task task)
		{
			TaskType ttype = task.GetType();
			this.type = ttype.ToString();
			TaskReport report = task.GetReport();
			this.startTime = report.GetStartTime();
			this.finishTime = report.GetFinishTime();
			this.state = report.GetTaskState();
			this.elapsedTime = Times.Elapsed(this.startTime, this.finishTime, this.state == TaskState
				.Running);
			if (this.elapsedTime == -1)
			{
				this.elapsedTime = 0;
			}
			this.progress = report.GetProgress() * 100;
			this.status = report.GetStatus();
			this.id = MRApps.ToString(task.GetID());
			this.taskNum = task.GetID().GetId();
			this.successful = GetSuccessfulAttempt(task);
			if (successful != null)
			{
				this.successfulAttempt = MRApps.ToString(successful.GetID());
			}
			else
			{
				this.successfulAttempt = string.Empty;
			}
		}

		public virtual float GetProgress()
		{
			return this.progress;
		}

		public virtual string GetState()
		{
			return this.state.ToString();
		}

		public virtual string GetId()
		{
			return this.id;
		}

		public virtual int GetTaskNum()
		{
			return this.taskNum;
		}

		public virtual long GetStartTime()
		{
			return this.startTime;
		}

		public virtual long GetFinishTime()
		{
			return this.finishTime;
		}

		public virtual long GetElapsedTime()
		{
			return this.elapsedTime;
		}

		public virtual string GetSuccessfulAttempt()
		{
			return this.successfulAttempt;
		}

		public virtual TaskAttempt GetSuccessful()
		{
			return this.successful;
		}

		private TaskAttempt GetSuccessfulAttempt(Task task)
		{
			foreach (TaskAttempt attempt in task.GetAttempts().Values)
			{
				if (attempt.GetState() == TaskAttemptState.Succeeded)
				{
					return attempt;
				}
			}
			return null;
		}

		public virtual string GetStatus()
		{
			return status;
		}
	}
}
