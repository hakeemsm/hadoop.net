using System.Collections.Generic;
using System.IO;
using Org.Apache.Log4j;
using Org.Apache.Log4j.Spi;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// A simple log4j-appender for the task child's
	/// map-reduce system logs.
	/// </summary>
	public class TaskLogAppender : FileAppender, Flushable
	{
		private string taskId;

		private int maxEvents;

		private Queue<LoggingEvent> tail = null;

		private bool isCleanup;

		internal const string IscleanupProperty = "hadoop.tasklog.iscleanup";

		internal const string LogsizeProperty = "hadoop.tasklog.totalLogFileSize";

		internal const string TaskidProperty = "hadoop.tasklog.taskid";

		//taskId should be managed as String rather than TaskID object
		//so that log4j can configure it from the configuration(log4j.properties). 
		// System properties passed in from JVM runner
		public override void ActivateOptions()
		{
			lock (this)
			{
				SetOptionsFromSystemProperties();
				if (maxEvents > 0)
				{
					tail = new List<LoggingEvent>();
				}
				SetFile(TaskLog.GetTaskLogFile(((TaskAttemptID)TaskAttemptID.ForName(taskId)), isCleanup
					, TaskLog.LogName.Syslog).ToString());
				SetAppend(true);
				base.ActivateOptions();
			}
		}

		/// <summary>The Task Runner passes in the options as system properties.</summary>
		/// <remarks>
		/// The Task Runner passes in the options as system properties. Set
		/// the options if the setters haven't already been called.
		/// </remarks>
		private void SetOptionsFromSystemProperties()
		{
			lock (this)
			{
				if (isCleanup == null)
				{
					string propValue = Runtime.GetProperty(IscleanupProperty, "false");
					isCleanup = Sharpen.Extensions.ValueOf(propValue);
				}
				if (taskId == null)
				{
					taskId = Runtime.GetProperty(TaskidProperty);
				}
				if (maxEvents == null)
				{
					string propValue = Runtime.GetProperty(LogsizeProperty, "0");
					SetTotalLogFileSize(long.Parse(propValue));
				}
			}
		}

		protected override void Append(LoggingEvent @event)
		{
			lock (this)
			{
				if (tail == null)
				{
					base.Append(@event);
				}
				else
				{
					if (tail.Count >= maxEvents)
					{
						tail.Remove();
					}
					tail.AddItem(@event);
				}
			}
		}

		public virtual void Flush()
		{
			if (qw != null)
			{
				qw.Flush();
			}
		}

		public override void Close()
		{
			lock (this)
			{
				if (tail != null)
				{
					foreach (LoggingEvent @event in tail)
					{
						base.Append(@event);
					}
				}
				base.Close();
			}
		}

		/// <summary>Getter/Setter methods for log4j.</summary>
		public virtual string GetTaskId()
		{
			lock (this)
			{
				return taskId;
			}
		}

		public virtual void SetTaskId(string taskId)
		{
			lock (this)
			{
				this.taskId = taskId;
			}
		}

		private const int EventSize = 100;

		public virtual long GetTotalLogFileSize()
		{
			lock (this)
			{
				return maxEvents * EventSize;
			}
		}

		public virtual void SetTotalLogFileSize(long logSize)
		{
			lock (this)
			{
				maxEvents = (int)logSize / EventSize;
			}
		}

		/// <summary>Set whether the task is a cleanup attempt or not.</summary>
		/// <param name="isCleanup">true if the task is cleanup attempt, false otherwise.</param>
		public virtual void SetIsCleanup(bool isCleanup)
		{
			lock (this)
			{
				this.isCleanup = isCleanup;
			}
		}

		/// <summary>Get whether task is cleanup attempt or not.</summary>
		/// <returns>true if the task is cleanup attempt, false otherwise.</returns>
		public virtual bool GetIsCleanup()
		{
			lock (this)
			{
				return isCleanup;
			}
		}
	}
}
