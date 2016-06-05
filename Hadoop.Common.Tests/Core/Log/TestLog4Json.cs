using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Apache.Log4j.Spi;
using Org.Codehaus.Jackson;
using Org.Codehaus.Jackson.Map;
using Org.Codehaus.Jackson.Node;
using Sharpen;

namespace Org.Apache.Hadoop.Log
{
	public class TestLog4Json : TestCase
	{
		private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog(typeof(
			TestLog4Json));

		private static readonly JsonFactory factory = new MappingJsonFactory();

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConstruction()
		{
			Log4Json l4j = new Log4Json();
			string outcome = l4j.ToJson(new StringWriter(), "name", 0, "DEBUG", "thread1", "hello, world"
				, null).ToString();
			Println("testConstruction", outcome);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestException()
		{
			Exception e = new NoRouteToHostException("that box caught fire 3 years ago");
			ThrowableInformation ti = new ThrowableInformation(e);
			Log4Json l4j = new Log4Json();
			long timeStamp = Time.Now();
			string outcome = l4j.ToJson(new StringWriter(), "testException", timeStamp, "INFO"
				, "quoted\"", "new line\n and {}", ti).ToString();
			Println("testException", outcome);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNestedException()
		{
			Exception e = new NoRouteToHostException("that box caught fire 3 years ago");
			Exception ioe = new IOException("Datacenter problems", e);
			ThrowableInformation ti = new ThrowableInformation(ioe);
			Log4Json l4j = new Log4Json();
			long timeStamp = Time.Now();
			string outcome = l4j.ToJson(new StringWriter(), "testNestedException", timeStamp, 
				"INFO", "quoted\"", "new line\n and {}", ti).ToString();
			Println("testNestedException", outcome);
			ContainerNode rootNode = Log4Json.Parse(outcome);
			AssertEntryEquals(rootNode, Log4Json.Level, "INFO");
			AssertEntryEquals(rootNode, Log4Json.Name, "testNestedException");
			AssertEntryEquals(rootNode, Log4Json.Time, timeStamp);
			AssertEntryEquals(rootNode, Log4Json.ExceptionClass, ioe.GetType().FullName);
			JsonNode node = AssertNodeContains(rootNode, Log4Json.Stack);
			Assert.True("Not an array: " + node, node.IsArray());
			node = AssertNodeContains(rootNode, Log4Json.Date);
			Assert.True("Not a string: " + node, node.IsTextual());
			//rather than try and make assertions about the format of the text
			//message equalling another ISO date, this test asserts that the hypen
			//and colon characters are in the string.
			string dateText = node.GetTextValue();
			Assert.True("No '-' in " + dateText, dateText.Contains("-"));
			Assert.True("No '-' in " + dateText, dateText.Contains(":"));
		}

		/// <summary>Create a log instance and and log to it</summary>
		/// <exception cref="System.Exception">if it all goes wrong</exception>
		[Fact]
		public virtual void TestLog()
		{
			string message = "test message";
			Exception throwable = null;
			string json = LogOut(message, throwable);
			Println("testLog", json);
		}

		/// <summary>Create a log instance and and log to it</summary>
		/// <exception cref="System.Exception">if it all goes wrong</exception>
		[Fact]
		public virtual void TestLogExceptions()
		{
			string message = "test message";
			Exception inner = new IOException("Directory / not found");
			Exception throwable = new IOException("startup failure", inner);
			string json = LogOut(message, throwable);
			Println("testLogExceptions", json);
		}

		internal virtual void AssertEntryEquals(ContainerNode rootNode, string key, string
			 value)
		{
			JsonNode node = AssertNodeContains(rootNode, key);
			Assert.Equal(value, node.GetTextValue());
		}

		private JsonNode AssertNodeContains(ContainerNode rootNode, string key)
		{
			JsonNode node = rootNode.Get(key);
			if (node == null)
			{
				Fail("No entry of name \"" + key + "\" found in " + rootNode.ToString());
			}
			return node;
		}

		internal virtual void AssertEntryEquals(ContainerNode rootNode, string key, long 
			value)
		{
			JsonNode node = AssertNodeContains(rootNode, key);
			Assert.Equal(value, node.GetNumberValue());
		}

		/// <summary>Print out what's going on.</summary>
		/// <remarks>
		/// Print out what's going on. The logging APIs aren't used and the text
		/// delimited for more details
		/// </remarks>
		/// <param name="name">name of operation</param>
		/// <param name="text">text to print</param>
		private void Println(string name, string text)
		{
			System.Console.Out.WriteLine(name + ": #" + text + "#");
		}

		private string LogOut(string message, Exception throwable)
		{
			StringWriter writer = new StringWriter();
			Logger logger = CreateLogger(writer);
			logger.Info(message, throwable);
			//remove and close the appender
			logger.RemoveAllAppenders();
			return writer.ToString();
		}

		public virtual Logger CreateLogger(TextWriter writer)
		{
			TestLog4Json.TestLoggerRepository repo = new TestLog4Json.TestLoggerRepository();
			Logger logger = repo.GetLogger("test");
			Log4Json layout = new Log4Json();
			WriterAppender appender = new WriterAppender(layout, writer);
			logger.AddAppender(appender);
			return logger;
		}

		/// <summary>
		/// This test logger avoids integrating with the main runtimes Logger hierarchy
		/// in ways the reader does not want to know.
		/// </summary>
		private class TestLogger : Logger
		{
			private TestLogger(string name, LoggerRepository repo)
				: base(name)
			{
				repository = repo;
				SetLevel(Level.Info);
			}
		}

		public class TestLoggerRepository : LoggerRepository
		{
			public virtual void AddHierarchyEventListener(HierarchyEventListener listener)
			{
			}

			public virtual bool IsDisabled(int level)
			{
				return false;
			}

			public virtual void SetThreshold(Level level)
			{
			}

			public virtual void SetThreshold(string val)
			{
			}

			public virtual void EmitNoAppenderWarning(Category cat)
			{
			}

			public virtual Level GetThreshold()
			{
				return Level.All;
			}

			public virtual Logger GetLogger(string name)
			{
				return new TestLog4Json.TestLogger(name, this);
			}

			public virtual Logger GetLogger(string name, LoggerFactory factory)
			{
				return new TestLog4Json.TestLogger(name, this);
			}

			public virtual Logger GetRootLogger()
			{
				return new TestLog4Json.TestLogger("root", this);
			}

			public virtual Logger Exists(string name)
			{
				return null;
			}

			public virtual void Shutdown()
			{
			}

			public virtual IEnumeration GetCurrentLoggers()
			{
				return new ArrayList().GetEnumerator();
			}

			public virtual IEnumeration GetCurrentCategories()
			{
				return new ArrayList().GetEnumerator();
			}

			public virtual void FireAddAppenderEvent(Category logger, Appender appender)
			{
			}

			public virtual void ResetConfiguration()
			{
			}
		}
	}
}
