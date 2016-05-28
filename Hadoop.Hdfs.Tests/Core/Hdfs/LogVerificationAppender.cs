using System.Collections.Generic;
using Org.Apache.Log4j;
using Org.Apache.Log4j.Spi;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Used to verify that certain exceptions or messages are present in log output.
	/// 	</summary>
	public class LogVerificationAppender : AppenderSkeleton
	{
		private readonly IList<LoggingEvent> log = new AList<LoggingEvent>();

		public override bool RequiresLayout()
		{
			return false;
		}

		protected override void Append(LoggingEvent loggingEvent)
		{
			log.AddItem(loggingEvent);
		}

		public override void Close()
		{
		}

		public virtual IList<LoggingEvent> GetLog()
		{
			return new AList<LoggingEvent>(log);
		}

		public virtual int CountExceptionsWithMessage(string text)
		{
			int count = 0;
			foreach (LoggingEvent e in GetLog())
			{
				ThrowableInformation t = e.GetThrowableInformation();
				if (t != null)
				{
					string m = t.GetThrowable().Message;
					if (m.Contains(text))
					{
						count++;
					}
				}
			}
			return count;
		}

		public virtual int CountLinesWithMessage(string text)
		{
			int count = 0;
			foreach (LoggingEvent e in GetLog())
			{
				string msg = e.GetRenderedMessage();
				if (msg != null && msg.Contains(text))
				{
					count++;
				}
			}
			return count;
		}
	}
}
