using System;
using System.IO;
using Org.Apache.Log4j;
using Org.Apache.Log4j.Spi;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestTaskLogAppender
	{
		/// <summary>test TaskLogAppender</summary>
		public virtual void TestTaskLogAppender()
		{
			TaskLogAppender appender = new TaskLogAppender();
			Runtime.SetProperty(TaskLogAppender.TaskidProperty, "attempt_01_02_m03_04_001");
			Runtime.SetProperty(TaskLogAppender.LogsizeProperty, "1003");
			appender.ActivateOptions();
			NUnit.Framework.Assert.AreEqual(appender.GetTaskId(), "attempt_01_02_m03_04_001");
			NUnit.Framework.Assert.AreEqual(appender.GetTotalLogFileSize(), 1000);
			NUnit.Framework.Assert.AreEqual(appender.GetIsCleanup(), false);
			// test writer   
			TextWriter writer = new StringWriter();
			appender.SetWriter(writer);
			Layout layout = new PatternLayout("%-5p [%t]: %m%n");
			appender.SetLayout(layout);
			Category logger = Logger.GetLogger(GetType().FullName);
			LoggingEvent @event = new LoggingEvent("fqnOfCategoryClass", logger, Priority.Info
				, "message", new Exception());
			appender.Append(@event);
			appender.Flush();
			appender.Close();
			NUnit.Framework.Assert.IsTrue(writer.ToString().Length > 0);
			// test cleanup should not changed 
			appender = new TaskLogAppender();
			appender.SetIsCleanup(true);
			appender.ActivateOptions();
			NUnit.Framework.Assert.AreEqual(appender.GetIsCleanup(), true);
		}
	}
}
