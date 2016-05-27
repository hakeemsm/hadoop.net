using System;
using System.IO;
using System.Text;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	/// <summary>
	/// This is a base class for functional tests of the
	/// <see cref="HttpServer2"/>
	/// .
	/// The methods are static for other classes to import statically.
	/// </summary>
	public class HttpServerFunctionalTest : Assert
	{
		[System.Serializable]
		public class LongHeaderServlet : HttpServlet
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				NUnit.Framework.Assert.AreEqual(63 * 1024, request.GetHeader("longheader").Length
					);
				response.SetStatus(HttpServletResponse.ScOk);
			}
		}

		/// <summary>
		/// JVM property for the webapp test dir :
		/// <value/>
		/// 
		/// </summary>
		public const string TestBuildWebapps = "test.build.webapps";

		/// <summary>
		/// expected location of the test.build.webapps dir:
		/// <value/>
		/// 
		/// </summary>
		private const string BuildWebappsDir = "build/test/webapps";

		/// <summary>
		/// name of the test webapp:
		/// <value/>
		/// 
		/// </summary>
		private const string Test = "test";

		protected internal static Uri baseUrl;

		/// <summary>Create but do not start the test webapp server.</summary>
		/// <remarks>
		/// Create but do not start the test webapp server. The test webapp dir is
		/// prepared/checked in advance.
		/// </remarks>
		/// <returns>the server instance</returns>
		/// <exception cref="System.IO.IOException">if a problem occurs</exception>
		/// <exception cref="System.Exception">if a condition was not met</exception>
		public static HttpServer2 CreateTestServer()
		{
			PrepareTestWebapp();
			return CreateServer(Test);
		}

		/// <summary>Create but do not start the test webapp server.</summary>
		/// <remarks>
		/// Create but do not start the test webapp server. The test webapp dir is
		/// prepared/checked in advance.
		/// </remarks>
		/// <param name="conf">the server configuration to use</param>
		/// <returns>the server instance</returns>
		/// <exception cref="System.IO.IOException">if a problem occurs</exception>
		/// <exception cref="System.Exception">if a condition was not met</exception>
		public static HttpServer2 CreateTestServer(Configuration conf)
		{
			PrepareTestWebapp();
			return CreateServer(Test, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public static HttpServer2 CreateTestServer(Configuration conf, AccessControlList 
			adminsAcl)
		{
			PrepareTestWebapp();
			return CreateServer(Test, conf, adminsAcl);
		}

		/// <summary>Create but do not start the test webapp server.</summary>
		/// <remarks>
		/// Create but do not start the test webapp server. The test webapp dir is
		/// prepared/checked in advance.
		/// </remarks>
		/// <param name="conf">the server configuration to use</param>
		/// <returns>the server instance</returns>
		/// <exception cref="System.IO.IOException">if a problem occurs</exception>
		/// <exception cref="System.Exception">if a condition was not met</exception>
		public static HttpServer2 CreateTestServer(Configuration conf, string[] pathSpecs
			)
		{
			PrepareTestWebapp();
			return CreateServer(Test, conf, pathSpecs);
		}

		/// <summary>
		/// Prepare the test webapp by creating the directory from the test properties
		/// fail if the directory cannot be created.
		/// </summary>
		/// <exception cref="System.Exception">if a condition was not met</exception>
		protected internal static void PrepareTestWebapp()
		{
			string webapps = Runtime.GetProperty(TestBuildWebapps, BuildWebappsDir);
			FilePath testWebappDir = new FilePath(webapps + FilePath.separatorChar + Test);
			try
			{
				if (!testWebappDir.Exists())
				{
					NUnit.Framework.Assert.Fail("Test webapp dir " + testWebappDir.GetCanonicalPath()
						 + " missing");
				}
			}
			catch (IOException)
			{
			}
		}

		/// <summary>Create an HttpServer instance on the given address for the given webapp</summary>
		/// <param name="host">to bind</param>
		/// <param name="port">to bind</param>
		/// <returns>the server</returns>
		/// <exception cref="System.IO.IOException">if it could not be created</exception>
		public static HttpServer2 CreateServer(string host, int port)
		{
			PrepareTestWebapp();
			return new HttpServer2.Builder().SetName(Test).AddEndpoint(URI.Create("http://" +
				 host + ":" + port)).SetFindPort(true).Build();
		}

		/// <summary>Create an HttpServer instance for the given webapp</summary>
		/// <param name="webapp">the webapp to work with</param>
		/// <returns>the server</returns>
		/// <exception cref="System.IO.IOException">if it could not be created</exception>
		public static HttpServer2 CreateServer(string webapp)
		{
			return LocalServerBuilder(webapp).SetFindPort(true).Build();
		}

		/// <summary>Create an HttpServer instance for the given webapp</summary>
		/// <param name="webapp">the webapp to work with</param>
		/// <param name="conf">the configuration to use for the server</param>
		/// <returns>the server</returns>
		/// <exception cref="System.IO.IOException">if it could not be created</exception>
		public static HttpServer2 CreateServer(string webapp, Configuration conf)
		{
			return LocalServerBuilder(webapp).SetFindPort(true).SetConf(conf).Build();
		}

		/// <exception cref="System.IO.IOException"/>
		public static HttpServer2 CreateServer(string webapp, Configuration conf, AccessControlList
			 adminsAcl)
		{
			return LocalServerBuilder(webapp).SetFindPort(true).SetConf(conf).SetACL(adminsAcl
				).Build();
		}

		private static HttpServer2.Builder LocalServerBuilder(string webapp)
		{
			return new HttpServer2.Builder().SetName(webapp).AddEndpoint(URI.Create("http://localhost:0"
				));
		}

		/// <summary>Create an HttpServer instance for the given webapp</summary>
		/// <param name="webapp">the webapp to work with</param>
		/// <param name="conf">the configuration to use for the server</param>
		/// <param name="pathSpecs">the paths specifications the server will service</param>
		/// <returns>the server</returns>
		/// <exception cref="System.IO.IOException">if it could not be created</exception>
		public static HttpServer2 CreateServer(string webapp, Configuration conf, string[]
			 pathSpecs)
		{
			return LocalServerBuilder(webapp).SetFindPort(true).SetConf(conf).SetPathSpec(pathSpecs
				).Build();
		}

		/// <summary>Create and start a server with the test webapp</summary>
		/// <returns>the newly started server</returns>
		/// <exception cref="System.IO.IOException">on any failure</exception>
		/// <exception cref="System.Exception">if a condition was not met</exception>
		public static HttpServer2 CreateAndStartTestServer()
		{
			HttpServer2 server = CreateTestServer();
			server.Start();
			return server;
		}

		/// <summary>If the server is non null, stop it</summary>
		/// <param name="server">to stop</param>
		/// <exception cref="System.Exception">on any failure</exception>
		public static void Stop(HttpServer2 server)
		{
			if (server != null)
			{
				server.Stop();
			}
		}

		/// <summary>Pass in a server, return a URL bound to localhost and its port</summary>
		/// <param name="server">server</param>
		/// <returns>a URL bonded to the base of the server</returns>
		/// <exception cref="System.UriFormatException">if the URL cannot be created.</exception>
		public static Uri GetServerURL(HttpServer2 server)
		{
			NUnit.Framework.Assert.IsNotNull("No server", server);
			return new Uri("http://" + NetUtils.GetHostPortString(server.GetConnectorAddress(
				0)));
		}

		/// <summary>Read in the content from a URL</summary>
		/// <param name="url">URL To read</param>
		/// <returns>the text from the output</returns>
		/// <exception cref="System.IO.IOException">if something went wrong</exception>
		protected internal static string ReadOutput(Uri url)
		{
			StringBuilder @out = new StringBuilder();
			InputStream @in = url.OpenConnection().GetInputStream();
			byte[] buffer = new byte[64 * 1024];
			int len = @in.Read(buffer);
			while (len > 0)
			{
				@out.Append(Sharpen.Runtime.GetStringForBytes(buffer, 0, len));
				len = @in.Read(buffer);
			}
			return @out.ToString();
		}

		/// <summary>Test that verifies headers can be up to 64K long.</summary>
		/// <remarks>
		/// Test that verifies headers can be up to 64K long.
		/// The test adds a 63K header leaving 1K for other headers.
		/// This is because the header buffer setting is for ALL headers,
		/// names and values included.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void TestLongHeader(HttpURLConnection conn)
		{
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < 63 * 1024; i++)
			{
				sb.Append("a");
			}
			conn.SetRequestProperty("longheader", sb.ToString());
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
		}
	}
}
