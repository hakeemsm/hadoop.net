using Sharpen;

namespace org.apache.hadoop.http
{
	public class TestHttpServer : org.apache.hadoop.http.HttpServerFunctionalTest
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpServer
			)));

		private static org.apache.hadoop.http.HttpServer2 server;

		private const int MAX_THREADS = 10;

		[System.Serializable]
		public class EchoMapServlet : javax.servlet.http.HttpServlet
		{
			/// <exception cref="javax.servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
				 response)
			{
				java.io.PrintWriter @out = response.getWriter();
				System.Collections.Generic.IDictionary<string, string[]> @params = request.getParameterMap
					();
				java.util.SortedSet<string> keys = new java.util.TreeSet<string>(@params.Keys);
				foreach (string key in keys)
				{
					@out.print(key);
					@out.print(':');
					string[] values = @params[key];
					if (values.Length > 0)
					{
						@out.print(values[0]);
						for (int i = 1; i < values.Length; ++i)
						{
							@out.print(',');
							@out.print(values[i]);
						}
					}
					@out.print('\n');
				}
				@out.close();
			}
		}

		[System.Serializable]
		public class EchoServlet : javax.servlet.http.HttpServlet
		{
			/// <exception cref="javax.servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
				 response)
			{
				java.io.PrintWriter @out = response.getWriter();
				java.util.SortedSet<string> sortedKeys = new java.util.TreeSet<string>();
				java.util.Enumeration<string> keys = request.getParameterNames();
				while (keys.MoveNext())
				{
					sortedKeys.add(keys.Current);
				}
				foreach (string key in sortedKeys)
				{
					@out.print(key);
					@out.print(':');
					@out.print(request.getParameter(key));
					@out.print('\n');
				}
				@out.close();
			}
		}

		[System.Serializable]
		public class HtmlContentServlet : javax.servlet.http.HttpServlet
		{
			/// <exception cref="javax.servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
				 response)
			{
				response.setContentType("text/html");
				java.io.PrintWriter @out = response.getWriter();
				@out.print("hello world");
				@out.close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setInt(org.apache.hadoop.http.HttpServer2.HTTP_MAX_THREADS, 10);
			server = createTestServer(conf);
			server.addServlet("echo", "/echo", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpServer.EchoServlet
				)));
			server.addServlet("echomap", "/echomap", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpServer.EchoMapServlet
				)));
			server.addServlet("htmlcontent", "/htmlcontent", Sharpen.Runtime.getClassForType(
				typeof(org.apache.hadoop.http.TestHttpServer.HtmlContentServlet)));
			server.addServlet("longheader", "/longheader", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.http.HttpServerFunctionalTest.LongHeaderServlet)));
			server.addJerseyResourcePackage(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.resource.JerseyResource
				)).getPackage().getName(), "/jersey/*");
			server.start();
			baseUrl = getServerURL(server);
			LOG.info("HTTP server started: " + baseUrl);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.AfterClass]
		public static void cleanup()
		{
			server.stop();
		}

		/// <summary>Test the maximum number of threads cannot be exceeded.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMaxThreads()
		{
			int clientThreads = MAX_THREADS * 10;
			java.util.concurrent.Executor executor = java.util.concurrent.Executors.newFixedThreadPool
				(clientThreads);
			// Run many clients to make server reach its maximum number of threads
			java.util.concurrent.CountDownLatch ready = new java.util.concurrent.CountDownLatch
				(clientThreads);
			java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch
				(1);
			for (int i = 0; i < clientThreads; i++)
			{
				executor.execute(new _Runnable_162(ready, start));
			}
			// do nothing
			// Start the client threads when they are all ready
			ready.await();
			start.countDown();
		}

		private sealed class _Runnable_162 : java.lang.Runnable
		{
			public _Runnable_162(java.util.concurrent.CountDownLatch ready, java.util.concurrent.CountDownLatch
				 start)
			{
				this.ready = ready;
				this.start = start;
			}

			public void run()
			{
				ready.countDown();
				try
				{
					start.await();
					NUnit.Framework.Assert.AreEqual("a:b\nc:d\n", org.apache.hadoop.http.HttpServerFunctionalTest
						.readOutput(new java.net.URL(org.apache.hadoop.http.HttpServerFunctionalTest.baseUrl
						, "/echo?a=b&c=d")));
					int serverThreads = org.apache.hadoop.http.TestHttpServer.server.webServer.getThreadPool
						().getThreads();
					NUnit.Framework.Assert.IsTrue("More threads are started than expected, Server Threads count: "
						 + serverThreads, serverThreads <= org.apache.hadoop.http.TestHttpServer.MAX_THREADS
						);
					System.Console.Out.WriteLine("Number of threads = " + serverThreads + " which is less or equal than the max = "
						 + org.apache.hadoop.http.TestHttpServer.MAX_THREADS);
				}
				catch (System.Exception)
				{
				}
			}

			private readonly java.util.concurrent.CountDownLatch ready;

			private readonly java.util.concurrent.CountDownLatch start;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testEcho()
		{
			NUnit.Framework.Assert.AreEqual("a:b\nc:d\n", readOutput(new java.net.URL(baseUrl
				, "/echo?a=b&c=d")));
			NUnit.Framework.Assert.AreEqual("a:b\nc&lt;:d\ne:&gt;\n", readOutput(new java.net.URL
				(baseUrl, "/echo?a=b&c<=d&e=>")));
		}

		/// <summary>Test the echo map servlet that uses getParameterMap.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testEchoMap()
		{
			NUnit.Framework.Assert.AreEqual("a:b\nc:d\n", readOutput(new java.net.URL(baseUrl
				, "/echomap?a=b&c=d")));
			NUnit.Framework.Assert.AreEqual("a:b,&gt;\nc&lt;:d\n", readOutput(new java.net.URL
				(baseUrl, "/echomap?a=b&c<=d&a=>")));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLongHeader()
		{
			java.net.URL url = new java.net.URL(baseUrl, "/longheader");
			java.net.HttpURLConnection conn = (java.net.HttpURLConnection)url.openConnection(
				);
			testLongHeader(conn);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testContentTypes()
		{
			// Static CSS files should have text/css
			java.net.URL cssUrl = new java.net.URL(baseUrl, "/static/test.css");
			java.net.HttpURLConnection conn = (java.net.HttpURLConnection)cssUrl.openConnection
				();
			conn.connect();
			NUnit.Framework.Assert.AreEqual(200, conn.getResponseCode());
			NUnit.Framework.Assert.AreEqual("text/css", conn.getContentType());
			// Servlets should have text/plain with proper encoding by default
			java.net.URL servletUrl = new java.net.URL(baseUrl, "/echo?a=b");
			conn = (java.net.HttpURLConnection)servletUrl.openConnection();
			conn.connect();
			NUnit.Framework.Assert.AreEqual(200, conn.getResponseCode());
			NUnit.Framework.Assert.AreEqual("text/plain; charset=utf-8", conn.getContentType(
				));
			// We should ignore parameters for mime types - ie a parameter
			// ending in .css should not change mime type
			servletUrl = new java.net.URL(baseUrl, "/echo?a=b.css");
			conn = (java.net.HttpURLConnection)servletUrl.openConnection();
			conn.connect();
			NUnit.Framework.Assert.AreEqual(200, conn.getResponseCode());
			NUnit.Framework.Assert.AreEqual("text/plain; charset=utf-8", conn.getContentType(
				));
			// Servlets that specify text/html should get that content type
			servletUrl = new java.net.URL(baseUrl, "/htmlcontent");
			conn = (java.net.HttpURLConnection)servletUrl.openConnection();
			conn.connect();
			NUnit.Framework.Assert.AreEqual(200, conn.getResponseCode());
			NUnit.Framework.Assert.AreEqual("text/html; charset=utf-8", conn.getContentType()
				);
		}

		/// <summary>Dummy filter that mimics as an authentication filter.</summary>
		/// <remarks>
		/// Dummy filter that mimics as an authentication filter. Obtains user identity
		/// from the request parameter user.name. Wraps around the request so that
		/// request.getRemoteUser() returns the user identity.
		/// </remarks>
		public class DummyServletFilter : javax.servlet.Filter
		{
			public virtual void destroy()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="javax.servlet.ServletException"/>
			public virtual void doFilter(javax.servlet.ServletRequest request, javax.servlet.ServletResponse
				 response, javax.servlet.FilterChain filterChain)
			{
				string userName = request.getParameter("user.name");
				javax.servlet.ServletRequest requestModified = new _HttpServletRequestWrapper_253
					(userName, (javax.servlet.http.HttpServletRequest)request);
				filterChain.doFilter(requestModified, response);
			}

			private sealed class _HttpServletRequestWrapper_253 : javax.servlet.http.HttpServletRequestWrapper
			{
				public _HttpServletRequestWrapper_253(string userName, javax.servlet.http.HttpServletRequest
					 baseArg1)
					: base(baseArg1)
				{
					this.userName = userName;
				}

				public override string getRemoteUser()
				{
					return userName;
				}

				private readonly string userName;
			}

			/// <exception cref="javax.servlet.ServletException"/>
			public virtual void init(javax.servlet.FilterConfig arg0)
			{
			}
		}

		/// <summary>FilterInitializer that initialized the DummyFilter.</summary>
		public class DummyFilterInitializer : org.apache.hadoop.http.FilterInitializer
		{
			public DummyFilterInitializer()
			{
			}

			public override void initFilter(org.apache.hadoop.http.FilterContainer container, 
				org.apache.hadoop.conf.Configuration conf)
			{
				container.addFilter("DummyFilter", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpServer.DummyServletFilter
					)).getName(), null);
			}
		}

		/// <summary>Access a URL and get the corresponding return Http status code.</summary>
		/// <remarks>
		/// Access a URL and get the corresponding return Http status code. The URL
		/// will be accessed as the passed user, by sending user.name request
		/// parameter.
		/// </remarks>
		/// <param name="urlstring"/>
		/// <param name="userName"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		internal static int getHttpStatusCode(string urlstring, string userName)
		{
			java.net.URL url = new java.net.URL(urlstring + "?user.name=" + userName);
			System.Console.Out.WriteLine("Accessing " + url + " as user " + userName);
			java.net.HttpURLConnection connection = (java.net.HttpURLConnection)url.openConnection
				();
			connection.connect();
			return connection.getResponseCode();
		}

		/// <summary>Custom user-&gt;group mapping service.</summary>
		public class MyGroupsProvider : org.apache.hadoop.security.ShellBasedUnixGroupsMapping
		{
			internal static System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList
				<string>> mapping = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.IList
				<string>>();

			internal static void clearMapping()
			{
				mapping.clear();
			}

			/// <exception cref="System.IO.IOException"/>
			public override System.Collections.Generic.IList<string> getGroups(string user)
			{
				return mapping[user];
			}
		}

		/// <summary>
		/// Verify the access for /logs, /stacks, /conf, /logLevel and /metrics
		/// servlets, when authentication filters are set, but authorization is not
		/// enabled.
		/// </summary>
		/// <exception cref="System.Exception"></exception>
		[NUnit.Framework.Test]
		public virtual void testDisabledAuthorizationOfDefaultServlets()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			// Authorization is disabled by default
			conf.set(org.apache.hadoop.http.HttpServer2.FILTER_INITIALIZER_PROPERTY, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.http.TestHttpServer.DummyFilterInitializer)).getName()
				);
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING
				, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpServer.MyGroupsProvider
				)).getName());
			org.apache.hadoop.security.Groups.getUserToGroupsMappingService(conf);
			org.apache.hadoop.http.TestHttpServer.MyGroupsProvider.clearMapping();
			org.apache.hadoop.http.TestHttpServer.MyGroupsProvider.mapping["userA"] = java.util.Arrays
				.asList("groupA");
			org.apache.hadoop.http.TestHttpServer.MyGroupsProvider.mapping["userB"] = java.util.Arrays
				.asList("groupB");
			org.apache.hadoop.http.HttpServer2 myServer = new org.apache.hadoop.http.HttpServer2.Builder
				().setName("test").addEndpoint(new java.net.URI("http://localhost:0")).setFindPort
				(true).build();
			myServer.setAttribute(org.apache.hadoop.http.HttpServer2.CONF_CONTEXT_ATTRIBUTE, 
				conf);
			myServer.start();
			string serverURL = "http://" + org.apache.hadoop.net.NetUtils.getHostPortString(myServer
				.getConnectorAddress(0)) + "/";
			foreach (string servlet in new string[] { "conf", "logs", "stacks", "logLevel", "metrics"
				 })
			{
				foreach (string user in new string[] { "userA", "userB" })
				{
					NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_OK, getHttpStatusCode
						(serverURL + servlet, user));
				}
			}
			myServer.stop();
		}

		/// <summary>
		/// Verify the administrator access for /logs, /stacks, /conf, /logLevel and
		/// /metrics servlets.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAuthorizationOfDefaultServlets()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION
				, true);
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN
				, true);
			conf.set(org.apache.hadoop.http.HttpServer2.FILTER_INITIALIZER_PROPERTY, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.http.TestHttpServer.DummyFilterInitializer)).getName()
				);
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING
				, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpServer.MyGroupsProvider
				)).getName());
			org.apache.hadoop.security.Groups.getUserToGroupsMappingService(conf);
			org.apache.hadoop.http.TestHttpServer.MyGroupsProvider.clearMapping();
			org.apache.hadoop.http.TestHttpServer.MyGroupsProvider.mapping["userA"] = java.util.Arrays
				.asList("groupA");
			org.apache.hadoop.http.TestHttpServer.MyGroupsProvider.mapping["userB"] = java.util.Arrays
				.asList("groupB");
			org.apache.hadoop.http.TestHttpServer.MyGroupsProvider.mapping["userC"] = java.util.Arrays
				.asList("groupC");
			org.apache.hadoop.http.TestHttpServer.MyGroupsProvider.mapping["userD"] = java.util.Arrays
				.asList("groupD");
			org.apache.hadoop.http.TestHttpServer.MyGroupsProvider.mapping["userE"] = java.util.Arrays
				.asList("groupE");
			org.apache.hadoop.http.HttpServer2 myServer = new org.apache.hadoop.http.HttpServer2.Builder
				().setName("test").addEndpoint(new java.net.URI("http://localhost:0")).setFindPort
				(true).setConf(conf).setACL(new org.apache.hadoop.security.authorize.AccessControlList
				("userA,userB groupC,groupD")).build();
			myServer.setAttribute(org.apache.hadoop.http.HttpServer2.CONF_CONTEXT_ATTRIBUTE, 
				conf);
			myServer.start();
			string serverURL = "http://" + org.apache.hadoop.net.NetUtils.getHostPortString(myServer
				.getConnectorAddress(0)) + "/";
			foreach (string servlet in new string[] { "conf", "logs", "stacks", "logLevel", "metrics"
				 })
			{
				foreach (string user in new string[] { "userA", "userB", "userC", "userD" })
				{
					NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_OK, getHttpStatusCode
						(serverURL + servlet, user));
				}
				NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_FORBIDDEN, getHttpStatusCode
					(serverURL + servlet, "userE"));
			}
			myServer.stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRequestQuoterWithNull()
		{
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			org.mockito.Mockito.doReturn(null).when(request).getParameterValues("dummy");
			org.apache.hadoop.http.HttpServer2.QuotingInputFilter.RequestQuoter requestQuoter
				 = new org.apache.hadoop.http.HttpServer2.QuotingInputFilter.RequestQuoter(request
				);
			string[] parameterValues = requestQuoter.getParameterValues("dummy");
			NUnit.Framework.Assert.IsNull("It should return null " + "when there are no values for the parameter"
				, parameterValues);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRequestQuoterWithNotNull()
		{
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			string[] values = new string[] { "abc", "def" };
			org.mockito.Mockito.doReturn(values).when(request).getParameterValues("dummy");
			org.apache.hadoop.http.HttpServer2.QuotingInputFilter.RequestQuoter requestQuoter
				 = new org.apache.hadoop.http.HttpServer2.QuotingInputFilter.RequestQuoter(request
				);
			string[] parameterValues = requestQuoter.getParameterValues("dummy");
			NUnit.Framework.Assert.IsTrue("It should return Parameter Values", java.util.Arrays
				.equals(values, parameterValues));
		}

		private static System.Collections.Generic.IDictionary<string, object> parse(string
			 jsonString)
		{
			return (System.Collections.Generic.IDictionary<string, object>)org.mortbay.util.ajax.JSON
				.parse(jsonString);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testJersey()
		{
			LOG.info("BEGIN testJersey()");
			string js = readOutput(new java.net.URL(baseUrl, "/jersey/foo?op=bar"));
			System.Collections.Generic.IDictionary<string, object> m = parse(js);
			LOG.info("m=" + m);
			NUnit.Framework.Assert.AreEqual("foo", m[org.apache.hadoop.http.resource.JerseyResource
				.PATH]);
			NUnit.Framework.Assert.AreEqual("bar", m[org.apache.hadoop.http.resource.JerseyResource
				.OP]);
			LOG.info("END testJersey()");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testHasAdministratorAccess()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION
				, false);
			javax.servlet.ServletContext context = org.mockito.Mockito.mock<javax.servlet.ServletContext
				>();
			org.mockito.Mockito.when(context.getAttribute(org.apache.hadoop.http.HttpServer2.
				CONF_CONTEXT_ATTRIBUTE)).thenReturn(conf);
			org.mockito.Mockito.when(context.getAttribute(org.apache.hadoop.http.HttpServer2.
				ADMINS_ACL)).thenReturn(null);
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			org.mockito.Mockito.when(request.getRemoteUser()).thenReturn(null);
			javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
				>();
			//authorization OFF
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.http.HttpServer2.hasAdministratorAccess
				(context, request, response));
			//authorization ON & user NULL
			response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse>();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION
				, true);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.http.HttpServer2.hasAdministratorAccess
				(context, request, response));
			org.mockito.Mockito.verify(response).sendError(org.mockito.Mockito.eq(javax.servlet.http.HttpServletResponse
				.SC_FORBIDDEN), org.mockito.Mockito.anyString());
			//authorization ON & user NOT NULL & ACLs NULL
			response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse>();
			org.mockito.Mockito.when(request.getRemoteUser()).thenReturn("foo");
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.http.HttpServer2.hasAdministratorAccess
				(context, request, response));
			//authorization ON & user NOT NULL & ACLs NOT NULL & user not in ACLs
			response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse>();
			org.apache.hadoop.security.authorize.AccessControlList acls = org.mockito.Mockito
				.mock<org.apache.hadoop.security.authorize.AccessControlList>();
			org.mockito.Mockito.when(acls.isUserAllowed(org.mockito.Mockito.any<org.apache.hadoop.security.UserGroupInformation
				>())).thenReturn(false);
			org.mockito.Mockito.when(context.getAttribute(org.apache.hadoop.http.HttpServer2.
				ADMINS_ACL)).thenReturn(acls);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.http.HttpServer2.hasAdministratorAccess
				(context, request, response));
			org.mockito.Mockito.verify(response).sendError(org.mockito.Mockito.eq(javax.servlet.http.HttpServletResponse
				.SC_FORBIDDEN), org.mockito.Mockito.anyString());
			//authorization ON & user NOT NULL & ACLs NOT NULL & user in in ACLs
			response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse>();
			org.mockito.Mockito.when(acls.isUserAllowed(org.mockito.Mockito.any<org.apache.hadoop.security.UserGroupInformation
				>())).thenReturn(true);
			org.mockito.Mockito.when(context.getAttribute(org.apache.hadoop.http.HttpServer2.
				ADMINS_ACL)).thenReturn(acls);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.http.HttpServer2.hasAdministratorAccess
				(context, request, response));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRequiresAuthorizationAccess()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			javax.servlet.ServletContext context = org.mockito.Mockito.mock<javax.servlet.ServletContext
				>();
			org.mockito.Mockito.when(context.getAttribute(org.apache.hadoop.http.HttpServer2.
				CONF_CONTEXT_ATTRIBUTE)).thenReturn(conf);
			javax.servlet.http.HttpServletRequest request = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			javax.servlet.http.HttpServletResponse response = org.mockito.Mockito.mock<javax.servlet.http.HttpServletResponse
				>();
			//requires admin access to instrumentation, FALSE by default
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.http.HttpServer2.isInstrumentationAccessAllowed
				(context, request, response));
			//requires admin access to instrumentation, TRUE
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN
				, true);
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION
				, true);
			org.apache.hadoop.security.authorize.AccessControlList acls = org.mockito.Mockito
				.mock<org.apache.hadoop.security.authorize.AccessControlList>();
			org.mockito.Mockito.when(acls.isUserAllowed(org.mockito.Mockito.any<org.apache.hadoop.security.UserGroupInformation
				>())).thenReturn(false);
			org.mockito.Mockito.when(context.getAttribute(org.apache.hadoop.http.HttpServer2.
				ADMINS_ACL)).thenReturn(acls);
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.http.HttpServer2.isInstrumentationAccessAllowed
				(context, request, response));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testBindAddress()
		{
			checkBindAddress("localhost", 0, false).stop();
			// hang onto this one for a bit more testing
			org.apache.hadoop.http.HttpServer2 myServer = checkBindAddress("localhost", 0, false
				);
			org.apache.hadoop.http.HttpServer2 myServer2 = null;
			try
			{
				int port = myServer.getConnectorAddress(0).getPort();
				// it's already in use, true = expect a higher port
				myServer2 = checkBindAddress("localhost", port, true);
				// try to reuse the port
				port = myServer2.getConnectorAddress(0).getPort();
				myServer2.stop();
				NUnit.Framework.Assert.IsNull(myServer2.getConnectorAddress(0));
				// not bound
				myServer2.openListeners();
				NUnit.Framework.Assert.AreEqual(port, myServer2.getConnectorAddress(0).getPort());
			}
			finally
			{
				// expect same port
				myServer.stop();
				if (myServer2 != null)
				{
					myServer2.stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private org.apache.hadoop.http.HttpServer2 checkBindAddress(string host, int port
			, bool findPort)
		{
			org.apache.hadoop.http.HttpServer2 server = createServer(host, port);
			try
			{
				// not bound, ephemeral should return requested port (0 for ephemeral)
				System.Collections.Generic.IList<object> listeners = (System.Collections.Generic.IList
					<object>)org.mockito.@internal.util.reflection.Whitebox.getInternalState(server, 
					"listeners");
				org.mortbay.jetty.Connector listener = (org.mortbay.jetty.Connector)listeners[0];
				NUnit.Framework.Assert.AreEqual(port, listener.getPort());
				// verify hostname is what was given
				server.openListeners();
				NUnit.Framework.Assert.AreEqual(host, server.getConnectorAddress(0).getHostName()
					);
				int boundPort = server.getConnectorAddress(0).getPort();
				if (port == 0)
				{
					NUnit.Framework.Assert.IsTrue(boundPort != 0);
				}
				else
				{
					// ephemeral should now return bound port
					if (findPort)
					{
						NUnit.Framework.Assert.IsTrue(boundPort > port);
						// allow a little wiggle room to prevent random test failures if
						// some consecutive ports are already in use
						NUnit.Framework.Assert.IsTrue(boundPort - port < 8);
					}
				}
			}
			catch (System.Exception e)
			{
				server.stop();
				throw;
			}
			return server;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNoCacheHeader()
		{
			java.net.URL url = new java.net.URL(baseUrl, "/echo?a=b&c=d");
			java.net.HttpURLConnection conn = (java.net.HttpURLConnection)url.openConnection(
				);
			NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_OK, conn.getResponseCode
				());
			NUnit.Framework.Assert.AreEqual("no-cache", conn.getHeaderField("Cache-Control"));
			NUnit.Framework.Assert.AreEqual("no-cache", conn.getHeaderField("Pragma"));
			NUnit.Framework.Assert.IsNotNull(conn.getHeaderField("Expires"));
			NUnit.Framework.Assert.IsNotNull(conn.getHeaderField("Date"));
			NUnit.Framework.Assert.AreEqual(conn.getHeaderField("Expires"), conn.getHeaderField
				("Date"));
		}
	}
}
