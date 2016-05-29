using System.Collections.Generic;
using System.IO;
using Org.Apache.Log4j;
using Org.Apache.Log4j.Spi;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	/// <summary>A simple log4j-appender for container's logs.</summary>
	public class ContainerLogAppender : FileAppender, Flushable
	{
		private string containerLogDir;

		private string containerLogFile;

		private int maxEvents;

		private Queue<LoggingEvent> tail = null;

		private bool closing = false;

		//so that log4j can configure it from the configuration(log4j.properties). 
		public override void ActivateOptions()
		{
			lock (this)
			{
				if (maxEvents > 0)
				{
					tail = new List<LoggingEvent>();
				}
				SetFile(new FilePath(this.containerLogDir, containerLogFile).ToString());
				SetAppend(true);
				base.ActivateOptions();
			}
		}

		protected override void Append(LoggingEvent @event)
		{
			lock (this)
			{
				if (closing)
				{
					// When closing drop any new/transitive CLA appending
					return;
				}
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
				closing = true;
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
		public virtual string GetContainerLogDir()
		{
			return this.containerLogDir;
		}

		public virtual void SetContainerLogDir(string containerLogDir)
		{
			this.containerLogDir = containerLogDir;
		}

		public virtual string GetContainerLogFile()
		{
			return containerLogFile;
		}

		public virtual void SetContainerLogFile(string containerLogFile)
		{
			this.containerLogFile = containerLogFile;
		}

		private const int EventSize = 100;

		public virtual long GetTotalLogFileSize()
		{
			return maxEvents * EventSize;
		}

		public virtual void SetTotalLogFileSize(long logSize)
		{
			maxEvents = (int)logSize / EventSize;
		}
	}
}
