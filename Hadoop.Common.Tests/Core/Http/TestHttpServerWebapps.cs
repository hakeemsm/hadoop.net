using System.IO;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	/// <summary>Test webapp loading</summary>
	public class TestHttpServerWebapps : HttpServerFunctionalTest
	{
		private static readonly Log log = LogFactory.GetLog(typeof(TestHttpServerWebapps)
			);

		/// <summary>Test that the test server is loadable on the classpath</summary>
		/// <exception cref="System.Exception">if something went wrong</exception>
		[NUnit.Framework.Test]
		public virtual void TestValidServerResource()
		{
			HttpServer2 server = null;
			try
			{
				server = CreateServer("test");
			}
			finally
			{
				Stop(server);
			}
		}

		/// <summary>Test that an invalid webapp triggers an exception</summary>
		/// <exception cref="System.Exception">if something went wrong</exception>
		[NUnit.Framework.Test]
		public virtual void TestMissingServerResource()
		{
			try
			{
				HttpServer2 server = CreateServer("NoSuchWebapp");
				//should not have got here.
				//close the server
				string serverDescription = server.ToString();
				Stop(server);
				NUnit.Framework.Assert.Fail("Expected an exception, got " + serverDescription);
			}
			catch (FileNotFoundException expected)
			{
				log.Debug("Expected exception " + expected, expected);
			}
		}
	}
}
