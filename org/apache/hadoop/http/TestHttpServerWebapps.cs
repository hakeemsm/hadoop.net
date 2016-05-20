using Sharpen;

namespace org.apache.hadoop.http
{
	/// <summary>Test webapp loading</summary>
	public class TestHttpServerWebapps : org.apache.hadoop.http.HttpServerFunctionalTest
	{
		private static readonly org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpServerWebapps
			)));

		/// <summary>Test that the test server is loadable on the classpath</summary>
		/// <exception cref="System.Exception">if something went wrong</exception>
		[NUnit.Framework.Test]
		public virtual void testValidServerResource()
		{
			org.apache.hadoop.http.HttpServer2 server = null;
			try
			{
				server = createServer("test");
			}
			finally
			{
				stop(server);
			}
		}

		/// <summary>Test that an invalid webapp triggers an exception</summary>
		/// <exception cref="System.Exception">if something went wrong</exception>
		[NUnit.Framework.Test]
		public virtual void testMissingServerResource()
		{
			try
			{
				org.apache.hadoop.http.HttpServer2 server = createServer("NoSuchWebapp");
				//should not have got here.
				//close the server
				string serverDescription = server.ToString();
				stop(server);
				NUnit.Framework.Assert.Fail("Expected an exception, got " + serverDescription);
			}
			catch (java.io.FileNotFoundException expected)
			{
				log.debug("Expected exception " + expected, expected);
			}
		}
	}
}
