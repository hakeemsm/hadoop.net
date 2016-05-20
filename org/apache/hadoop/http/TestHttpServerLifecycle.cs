using Sharpen;

namespace org.apache.hadoop.http
{
	public class TestHttpServerLifecycle : org.apache.hadoop.http.HttpServerFunctionalTest
	{
		/// <summary>
		/// Check that a server is alive by probing the
		/// <see cref="HttpServer2.isAlive()"/>
		/// method
		/// and the text of its toString() description
		/// </summary>
		/// <param name="server">server</param>
		private void assertAlive(org.apache.hadoop.http.HttpServer2 server)
		{
			NUnit.Framework.Assert.IsTrue("Server is not alive", server.isAlive());
			assertToStringContains(server, org.apache.hadoop.http.HttpServer2.STATE_DESCRIPTION_ALIVE
				);
		}

		private void assertNotLive(org.apache.hadoop.http.HttpServer2 server)
		{
			NUnit.Framework.Assert.IsTrue("Server should not be live", !server.isAlive());
			assertToStringContains(server, org.apache.hadoop.http.HttpServer2.STATE_DESCRIPTION_NOT_LIVE
				);
		}

		/// <summary>Test that the server is alive once started</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void testCreatedServerIsNotAlive()
		{
			org.apache.hadoop.http.HttpServer2 server = createTestServer();
			assertNotLive(server);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testStopUnstartedServer()
		{
			org.apache.hadoop.http.HttpServer2 server = createTestServer();
			stop(server);
		}

		/// <summary>Test that the server is alive once started</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void testStartedServerIsAlive()
		{
			org.apache.hadoop.http.HttpServer2 server = null;
			server = createTestServer();
			assertNotLive(server);
			server.start();
			assertAlive(server);
			stop(server);
		}

		/// <summary>Test that the server with request logging enabled</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void testStartedServerWithRequestLog()
		{
			org.apache.hadoop.http.HttpRequestLogAppender requestLogAppender = new org.apache.hadoop.http.HttpRequestLogAppender
				();
			requestLogAppender.setName("httprequestlog");
			requestLogAppender.setFilename(Sharpen.Runtime.getProperty("test.build.data", "/tmp/"
				) + "jetty-name-yyyy_mm_dd.log");
			org.apache.log4j.Logger.getLogger(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpServer2
				)).getName() + ".test").addAppender(requestLogAppender);
			org.apache.hadoop.http.HttpServer2 server = null;
			server = createTestServer();
			assertNotLive(server);
			server.start();
			assertAlive(server);
			stop(server);
			org.apache.log4j.Logger.getLogger(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpServer2
				)).getName() + ".test").removeAppender(requestLogAppender);
		}

		/// <summary>
		/// Assert that the result of
		/// <see cref="HttpServer2.ToString()"/>
		/// contains the specific text
		/// </summary>
		/// <param name="server">server to examine</param>
		/// <param name="text">text to search for</param>
		private void assertToStringContains(org.apache.hadoop.http.HttpServer2 server, string
			 text)
		{
			string description = server.ToString();
			NUnit.Framework.Assert.IsTrue("Did not find \"" + text + "\" in \"" + description
				 + "\"", description.contains(text));
		}

		/// <summary>Test that the server is not alive once stopped</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void testStoppedServerIsNotAlive()
		{
			org.apache.hadoop.http.HttpServer2 server = createAndStartTestServer();
			assertAlive(server);
			stop(server);
			assertNotLive(server);
		}

		/// <summary>Test that the server is not alive once stopped</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void testStoppingTwiceServerIsAllowed()
		{
			org.apache.hadoop.http.HttpServer2 server = createAndStartTestServer();
			assertAlive(server);
			stop(server);
			assertNotLive(server);
			stop(server);
			assertNotLive(server);
		}

		/// <summary>Test that the server is alive once started</summary>
		/// <exception cref="System.Exception">on failure</exception>
		[NUnit.Framework.Test]
		public virtual void testWepAppContextAfterServerStop()
		{
			org.apache.hadoop.http.HttpServer2 server = null;
			string key = "test.attribute.key";
			string value = "test.attribute.value";
			server = createTestServer();
			assertNotLive(server);
			server.start();
			server.setAttribute(key, value);
			assertAlive(server);
			NUnit.Framework.Assert.AreEqual(value, server.getAttribute(key));
			stop(server);
			NUnit.Framework.Assert.IsNull("Server context should have cleared", server.getAttribute
				(key));
		}
	}
}
