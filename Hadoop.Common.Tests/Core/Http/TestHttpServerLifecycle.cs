using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	public class TestHttpServerLifecycle : HttpServerFunctionalTest
	{
		/// <summary>
		/// Check that a server is alive by probing the
		/// <see cref="HttpServer2.IsAlive()"/>
		/// method
		/// and the text of its toString() description
		/// </summary>
		/// <param name="server">server</param>
		private void AssertAlive(HttpServer2 server)
		{
			NUnit.Framework.Assert.IsTrue("Server is not alive", server.IsAlive());
			AssertToStringContains(server, HttpServer2.StateDescriptionAlive);
		}

		private void AssertNotLive(HttpServer2 server)
		{
			NUnit.Framework.Assert.IsTrue("Server should not be live", !server.IsAlive());
			AssertToStringContains(server, HttpServer2.StateDescriptionNotLive);
		}

		/// <summary>Test that the server is alive once started</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void TestCreatedServerIsNotAlive()
		{
			HttpServer2 server = CreateTestServer();
			AssertNotLive(server);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStopUnstartedServer()
		{
			HttpServer2 server = CreateTestServer();
			Stop(server);
		}

		/// <summary>Test that the server is alive once started</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void TestStartedServerIsAlive()
		{
			HttpServer2 server = null;
			server = CreateTestServer();
			AssertNotLive(server);
			server.Start();
			AssertAlive(server);
			Stop(server);
		}

		/// <summary>Test that the server with request logging enabled</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void TestStartedServerWithRequestLog()
		{
			HttpRequestLogAppender requestLogAppender = new HttpRequestLogAppender();
			requestLogAppender.SetName("httprequestlog");
			requestLogAppender.SetFilename(Runtime.GetProperty("test.build.data", "/tmp/") + 
				"jetty-name-yyyy_mm_dd.log");
			Logger.GetLogger(typeof(HttpServer2).FullName + ".test").AddAppender(requestLogAppender
				);
			HttpServer2 server = null;
			server = CreateTestServer();
			AssertNotLive(server);
			server.Start();
			AssertAlive(server);
			Stop(server);
			Logger.GetLogger(typeof(HttpServer2).FullName + ".test").RemoveAppender(requestLogAppender
				);
		}

		/// <summary>
		/// Assert that the result of
		/// <see cref="HttpServer2.ToString()"/>
		/// contains the specific text
		/// </summary>
		/// <param name="server">server to examine</param>
		/// <param name="text">text to search for</param>
		private void AssertToStringContains(HttpServer2 server, string text)
		{
			string description = server.ToString();
			NUnit.Framework.Assert.IsTrue("Did not find \"" + text + "\" in \"" + description
				 + "\"", description.Contains(text));
		}

		/// <summary>Test that the server is not alive once stopped</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void TestStoppedServerIsNotAlive()
		{
			HttpServer2 server = CreateAndStartTestServer();
			AssertAlive(server);
			Stop(server);
			AssertNotLive(server);
		}

		/// <summary>Test that the server is not alive once stopped</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void TestStoppingTwiceServerIsAllowed()
		{
			HttpServer2 server = CreateAndStartTestServer();
			AssertAlive(server);
			Stop(server);
			AssertNotLive(server);
			Stop(server);
			AssertNotLive(server);
		}

		/// <summary>Test that the server is alive once started</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void TestWepAppContextAfterServerStop()
		{
			HttpServer2 server = null;
			string key = "test.attribute.key";
			string value = "test.attribute.value";
			server = CreateTestServer();
			AssertNotLive(server);
			server.Start();
			server.SetAttribute(key, value);
			AssertAlive(server);
			NUnit.Framework.Assert.AreEqual(value, server.GetAttribute(key));
			Stop(server);
			NUnit.Framework.Assert.IsNull("Server context should have cleared", server.GetAttribute
				(key));
		}
	}
}
