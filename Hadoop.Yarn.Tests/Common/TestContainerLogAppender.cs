using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	public class TestContainerLogAppender
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendInClose()
		{
			ContainerLogAppender claAppender = new ContainerLogAppender();
			claAppender.SetName("testCLA");
			claAppender.SetLayout(new PatternLayout("%-5p [%t]: %m%n"));
			claAppender.SetContainerLogDir("target/testAppendInClose/logDir");
			claAppender.SetContainerLogFile("syslog");
			claAppender.SetTotalLogFileSize(1000);
			claAppender.ActivateOptions();
			Logger claLog = Logger.GetLogger("testAppendInClose-catergory");
			claLog.SetAdditivity(false);
			claLog.AddAppender(claAppender);
			claLog.Info(new _object_39(claLog));
			claAppender.Close();
		}

		private sealed class _object_39 : object
		{
			public _object_39(Logger claLog)
			{
				this.claLog = claLog;
			}

			public override string ToString()
			{
				claLog.Info("message1");
				return "return message1";
			}

			private readonly Logger claLog;
		}
	}
}
