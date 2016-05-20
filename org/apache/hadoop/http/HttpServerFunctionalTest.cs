using Sharpen;

namespace org.apache.hadoop.http
{
	/// <summary>
	/// This is a base class for functional tests of the
	/// <see cref="HttpServer2"/>
	/// .
	/// The methods are static for other classes to import statically.
	/// </summary>
	public class HttpServerFunctionalTest : NUnit.Framework.Assert
	{
		[System.Serializable]
		public class LongHeaderServlet : javax.servlet.http.HttpServlet
		{
			/// <exception cref="javax.servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
				 response)
			{
				NUnit.Framework.Assert.AreEqual(63 * 1024, request.getHeader("longheader").Length
					);
				response.setStatus(javax.servlet.http.HttpServletResponse.SC_OK);
			}
		}

		/// <summary>
		/// JVM property for the webapp test dir :
		/// <value/>
		/// 
		/// </summary>
		public const string TEST_BUILD_WEBAPPS = "test.build.webapps";

		/// <summary>
		/// expected location of the test.build.webapps dir:
		/// <value/>
		/// 
		/// </summary>
		private const string BUILD_WEBAPPS_DIR = "build/test/webapps";

		/// <summary>
		/// name of the test webapp:
		/// <value/>
		/// 
		/// </summary>
		private const string TEST = "test";

		protected internal static java.net.URL baseUrl;

		/// <summary>Create but do not start the test webapp server.</summary>
		/// <remarks>
		/// Create but do not start the test webapp server. The test webapp dir is
		/// prepared/checked in advance.
		/// </remarks>
		/// <returns>the server instance</returns>
		/// <exception cref="System.IO.IOException">if a problem occurs</exception>
		/// <exception cref="java.lang.AssertionError">if a condition was not met</exception>
		public static org.apache.hadoop.http.HttpServer2 createTestServer()
		{
			prepareTestWebapp();
			return createServer(TEST);
		}

		/// <summary>Create but do not start the test webapp server.</summary>
		/// <remarks>
		/// Create but do not start the test webapp server. The test webapp dir is
		/// prepared/checked in advance.
		/// </remarks>
		/// <param name="conf">the server configuration to use</param>
		/// <returns>the server instance</returns>
		/// <exception cref="System.IO.IOException">if a problem occurs</exception>
		/// <exception cref="java.lang.AssertionError">if a condition was not met</exception>
		public static org.apache.hadoop.http.HttpServer2 createTestServer(org.apache.hadoop.conf.Configuration
			 conf)
		{
			prepareTestWebapp();
			return createServer(TEST, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.http.HttpServer2 createTestServer(org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.security.authorize.AccessControlList adminsAcl)
		{
			prepareTestWebapp();
			return createServer(TEST, conf, adminsAcl);
		}

		/// <summary>Create but do not start the test webapp server.</summary>
		/// <remarks>
		/// Create but do not start the test webapp server. The test webapp dir is
		/// prepared/checked in advance.
		/// </remarks>
		/// <param name="conf">the server configuration to use</param>
		/// <returns>the server instance</returns>
		/// <exception cref="System.IO.IOException">if a problem occurs</exception>
		/// <exception cref="java.lang.AssertionError">if a condition was not met</exception>
		public static org.apache.hadoop.http.HttpServer2 createTestServer(org.apache.hadoop.conf.Configuration
			 conf, string[] pathSpecs)
		{
			prepareTestWebapp();
			return createServer(TEST, conf, pathSpecs);
		}

		/// <summary>
		/// Prepare the test webapp by creating the directory from the test properties
		/// fail if the directory cannot be created.
		/// </summary>
		/// <exception cref="java.lang.AssertionError">if a condition was not met</exception>
		protected internal static void prepareTestWebapp()
		{
			string webapps = Sharpen.Runtime.getProperty(TEST_BUILD_WEBAPPS, BUILD_WEBAPPS_DIR
				);
			java.io.File testWebappDir = new java.io.File(webapps + java.io.File.separatorChar
				 + TEST);
			try
			{
				if (!testWebappDir.exists())
				{
					NUnit.Framework.Assert.Fail("Test webapp dir " + testWebappDir.getCanonicalPath()
						 + " missing");
				}
			}
			catch (System.IO.IOException)
			{
			}
		}

		/// <summary>Create an HttpServer instance on the given address for the given webapp</summary>
		/// <param name="host">to bind</param>
		/// <param name="port">to bind</param>
		/// <returns>the server</returns>
		/// <exception cref="System.IO.IOException">if it could not be created</exception>
		public static org.apache.hadoop.http.HttpServer2 createServer(string host, int port
			)
		{
			prepareTestWebapp();
			return new org.apache.hadoop.http.HttpServer2.Builder().setName(TEST).addEndpoint
				(java.net.URI.create("http://" + host + ":" + port)).setFindPort(true).build();
		}

		/// <summary>Create an HttpServer instance for the given webapp</summary>
		/// <param name="webapp">the webapp to work with</param>
		/// <returns>the server</returns>
		/// <exception cref="System.IO.IOException">if it could not be created</exception>
		public static org.apache.hadoop.http.HttpServer2 createServer(string webapp)
		{
			return localServerBuilder(webapp).setFindPort(true).build();
		}

		/// <summary>Create an HttpServer instance for the given webapp</summary>
		/// <param name="webapp">the webapp to work with</param>
		/// <param name="conf">the configuration to use for the server</param>
		/// <returns>the server</returns>
		/// <exception cref="System.IO.IOException">if it could not be created</exception>
		public static org.apache.hadoop.http.HttpServer2 createServer(string webapp, org.apache.hadoop.conf.Configuration
			 conf)
		{
			return localServerBuilder(webapp).setFindPort(true).setConf(conf).build();
		}

		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.http.HttpServer2 createServer(string webapp, org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.security.authorize.AccessControlList adminsAcl)
		{
			return localServerBuilder(webapp).setFindPort(true).setConf(conf).setACL(adminsAcl
				).build();
		}

		private static org.apache.hadoop.http.HttpServer2.Builder localServerBuilder(string
			 webapp)
		{
			return new org.apache.hadoop.http.HttpServer2.Builder().setName(webapp).addEndpoint
				(java.net.URI.create("http://localhost:0"));
		}

		/// <summary>Create an HttpServer instance for the given webapp</summary>
		/// <param name="webapp">the webapp to work with</param>
		/// <param name="conf">the configuration to use for the server</param>
		/// <param name="pathSpecs">the paths specifications the server will service</param>
		/// <returns>the server</returns>
		/// <exception cref="System.IO.IOException">if it could not be created</exception>
		public static org.apache.hadoop.http.HttpServer2 createServer(string webapp, org.apache.hadoop.conf.Configuration
			 conf, string[] pathSpecs)
		{
			return localServerBuilder(webapp).setFindPort(true).setConf(conf).setPathSpec(pathSpecs
				).build();
		}

		/// <summary>Create and start a server with the test webapp</summary>
		/// <returns>the newly started server</returns>
		/// <exception cref="System.IO.IOException">on any failure</exception>
		/// <exception cref="java.lang.AssertionError">if a condition was not met</exception>
		public static org.apache.hadoop.http.HttpServer2 createAndStartTestServer()
		{
			org.apache.hadoop.http.HttpServer2 server = createTestServer();
			server.start();
			return server;
		}

		/// <summary>If the server is non null, stop it</summary>
		/// <param name="server">to stop</param>
		/// <exception cref="System.Exception">on any failure</exception>
		public static void stop(org.apache.hadoop.http.HttpServer2 server)
		{
			if (server != null)
			{
				server.stop();
			}
		}

		/// <summary>Pass in a server, return a URL bound to localhost and its port</summary>
		/// <param name="server">server</param>
		/// <returns>a URL bonded to the base of the server</returns>
		/// <exception cref="java.net.MalformedURLException">if the URL cannot be created.</exception>
		public static java.net.URL getServerURL(org.apache.hadoop.http.HttpServer2 server
			)
		{
			NUnit.Framework.Assert.IsNotNull("No server", server);
			return new java.net.URL("http://" + org.apache.hadoop.net.NetUtils.getHostPortString
				(server.getConnectorAddress(0)));
		}

		/// <summary>Read in the content from a URL</summary>
		/// <param name="url">URL To read</param>
		/// <returns>the text from the output</returns>
		/// <exception cref="System.IO.IOException">if something went wrong</exception>
		protected internal static string readOutput(java.net.URL url)
		{
			java.lang.StringBuilder @out = new java.lang.StringBuilder();
			java.io.InputStream @in = url.openConnection().getInputStream();
			byte[] buffer = new byte[64 * 1024];
			int len = @in.read(buffer);
			while (len > 0)
			{
				@out.Append(Sharpen.Runtime.getStringForBytes(buffer, 0, len));
				len = @in.read(buffer);
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
		protected internal virtual void testLongHeader(java.net.HttpURLConnection conn)
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			for (int i = 0; i < 63 * 1024; i++)
			{
				sb.Append("a");
			}
			conn.setRequestProperty("longheader", sb.ToString());
			NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_OK, conn.getResponseCode
				());
		}
	}
}
