using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Http.Resource;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Mockito.Internal.Util.Reflection;
using Org.Mortbay.Jetty;
using Org.Mortbay.Util.Ajax;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	public class TestHttpServer : HttpServerFunctionalTest
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestHttpServer));

		private static HttpServer2 server;

		private const int MaxThreads = 10;

		[System.Serializable]
		public class EchoMapServlet : HttpServlet
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				PrintWriter @out = response.GetWriter();
				IDictionary<string, string[]> @params = request.GetParameterMap();
				ICollection<string> keys = new TreeSet<string>(@params.Keys);
				foreach (string key in keys)
				{
					@out.Write(key);
					@out.Write(':');
					string[] values = @params[key];
					if (values.Length > 0)
					{
						@out.Write(values[0]);
						for (int i = 1; i < values.Length; ++i)
						{
							@out.Write(',');
							@out.Write(values[i]);
						}
					}
					@out.Write('\n');
				}
				@out.Close();
			}
		}

		[System.Serializable]
		public class EchoServlet : HttpServlet
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				PrintWriter @out = response.GetWriter();
				ICollection<string> sortedKeys = new TreeSet<string>();
				Enumeration<string> keys = request.GetParameterNames();
				while (keys.MoveNext())
				{
					sortedKeys.AddItem(keys.Current);
				}
				foreach (string key in sortedKeys)
				{
					@out.Write(key);
					@out.Write(':');
					@out.Write(request.GetParameter(key));
					@out.Write('\n');
				}
				@out.Close();
			}
		}

		[System.Serializable]
		public class HtmlContentServlet : HttpServlet
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				response.SetContentType("text/html");
				PrintWriter @out = response.GetWriter();
				@out.Write("hello world");
				@out.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			Configuration conf = new Configuration();
			conf.SetInt(HttpServer2.HttpMaxThreads, 10);
			server = CreateTestServer(conf);
			server.AddServlet("echo", "/echo", typeof(TestHttpServer.EchoServlet));
			server.AddServlet("echomap", "/echomap", typeof(TestHttpServer.EchoMapServlet));
			server.AddServlet("htmlcontent", "/htmlcontent", typeof(TestHttpServer.HtmlContentServlet
				));
			server.AddServlet("longheader", "/longheader", typeof(HttpServerFunctionalTest.LongHeaderServlet
				));
			server.AddJerseyResourcePackage(typeof(JerseyResource).Assembly.GetName(), "/jersey/*"
				);
			server.Start();
			baseUrl = GetServerURL(server);
			Log.Info("HTTP server started: " + baseUrl);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Cleanup()
		{
			server.Stop();
		}

		/// <summary>Test the maximum number of threads cannot be exceeded.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxThreads()
		{
			int clientThreads = MaxThreads * 10;
			Executor executor = Executors.NewFixedThreadPool(clientThreads);
			// Run many clients to make server reach its maximum number of threads
			CountDownLatch ready = new CountDownLatch(clientThreads);
			CountDownLatch start = new CountDownLatch(1);
			for (int i = 0; i < clientThreads; i++)
			{
				executor.Execute(new _Runnable_162(ready, start));
			}
			// do nothing
			// Start the client threads when they are all ready
			ready.Await();
			start.CountDown();
		}

		private sealed class _Runnable_162 : Runnable
		{
			public _Runnable_162(CountDownLatch ready, CountDownLatch start)
			{
				this.ready = ready;
				this.start = start;
			}

			public void Run()
			{
				ready.CountDown();
				try
				{
					start.Await();
					NUnit.Framework.Assert.AreEqual("a:b\nc:d\n", HttpServerFunctionalTest.ReadOutput
						(new Uri(HttpServerFunctionalTest.baseUrl, "/echo?a=b&c=d")));
					int serverThreads = TestHttpServer.server.webServer.GetThreadPool().GetThreads();
					NUnit.Framework.Assert.IsTrue("More threads are started than expected, Server Threads count: "
						 + serverThreads, serverThreads <= TestHttpServer.MaxThreads);
					System.Console.Out.WriteLine("Number of threads = " + serverThreads + " which is less or equal than the max = "
						 + TestHttpServer.MaxThreads);
				}
				catch (Exception)
				{
				}
			}

			private readonly CountDownLatch ready;

			private readonly CountDownLatch start;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEcho()
		{
			NUnit.Framework.Assert.AreEqual("a:b\nc:d\n", ReadOutput(new Uri(baseUrl, "/echo?a=b&c=d"
				)));
			NUnit.Framework.Assert.AreEqual("a:b\nc&lt;:d\ne:&gt;\n", ReadOutput(new Uri(baseUrl
				, "/echo?a=b&c<=d&e=>")));
		}

		/// <summary>Test the echo map servlet that uses getParameterMap.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEchoMap()
		{
			NUnit.Framework.Assert.AreEqual("a:b\nc:d\n", ReadOutput(new Uri(baseUrl, "/echomap?a=b&c=d"
				)));
			NUnit.Framework.Assert.AreEqual("a:b,&gt;\nc&lt;:d\n", ReadOutput(new Uri(baseUrl
				, "/echomap?a=b&c<=d&a=>")));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLongHeader()
		{
			Uri url = new Uri(baseUrl, "/longheader");
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			TestLongHeader(conn);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContentTypes()
		{
			// Static CSS files should have text/css
			Uri cssUrl = new Uri(baseUrl, "/static/test.css");
			HttpURLConnection conn = (HttpURLConnection)cssUrl.OpenConnection();
			conn.Connect();
			NUnit.Framework.Assert.AreEqual(200, conn.GetResponseCode());
			NUnit.Framework.Assert.AreEqual("text/css", conn.GetContentType());
			// Servlets should have text/plain with proper encoding by default
			Uri servletUrl = new Uri(baseUrl, "/echo?a=b");
			conn = (HttpURLConnection)servletUrl.OpenConnection();
			conn.Connect();
			NUnit.Framework.Assert.AreEqual(200, conn.GetResponseCode());
			NUnit.Framework.Assert.AreEqual("text/plain; charset=utf-8", conn.GetContentType(
				));
			// We should ignore parameters for mime types - ie a parameter
			// ending in .css should not change mime type
			servletUrl = new Uri(baseUrl, "/echo?a=b.css");
			conn = (HttpURLConnection)servletUrl.OpenConnection();
			conn.Connect();
			NUnit.Framework.Assert.AreEqual(200, conn.GetResponseCode());
			NUnit.Framework.Assert.AreEqual("text/plain; charset=utf-8", conn.GetContentType(
				));
			// Servlets that specify text/html should get that content type
			servletUrl = new Uri(baseUrl, "/htmlcontent");
			conn = (HttpURLConnection)servletUrl.OpenConnection();
			conn.Connect();
			NUnit.Framework.Assert.AreEqual(200, conn.GetResponseCode());
			NUnit.Framework.Assert.AreEqual("text/html; charset=utf-8", conn.GetContentType()
				);
		}

		/// <summary>Dummy filter that mimics as an authentication filter.</summary>
		/// <remarks>
		/// Dummy filter that mimics as an authentication filter. Obtains user identity
		/// from the request parameter user.name. Wraps around the request so that
		/// request.getRemoteUser() returns the user identity.
		/// </remarks>
		public class DummyServletFilter : Filter
		{
			public virtual void Destroy()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public virtual void DoFilter(ServletRequest request, ServletResponse response, FilterChain
				 filterChain)
			{
				string userName = request.GetParameter("user.name");
				ServletRequest requestModified = new _HttpServletRequestWrapper_253(userName, (HttpServletRequest
					)request);
				filterChain.DoFilter(requestModified, response);
			}

			private sealed class _HttpServletRequestWrapper_253 : HttpServletRequestWrapper
			{
				public _HttpServletRequestWrapper_253(string userName, HttpServletRequest baseArg1
					)
					: base(baseArg1)
				{
					this.userName = userName;
				}

				public override string GetRemoteUser()
				{
					return userName;
				}

				private readonly string userName;
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			public virtual void Init(FilterConfig arg0)
			{
			}
		}

		/// <summary>FilterInitializer that initialized the DummyFilter.</summary>
		public class DummyFilterInitializer : FilterInitializer
		{
			public DummyFilterInitializer()
			{
			}

			public override void InitFilter(FilterContainer container, Configuration conf)
			{
				container.AddFilter("DummyFilter", typeof(TestHttpServer.DummyServletFilter).FullName
					, null);
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
		internal static int GetHttpStatusCode(string urlstring, string userName)
		{
			Uri url = new Uri(urlstring + "?user.name=" + userName);
			System.Console.Out.WriteLine("Accessing " + url + " as user " + userName);
			HttpURLConnection connection = (HttpURLConnection)url.OpenConnection();
			connection.Connect();
			return connection.GetResponseCode();
		}

		/// <summary>Custom user-&gt;group mapping service.</summary>
		public class MyGroupsProvider : ShellBasedUnixGroupsMapping
		{
			internal static IDictionary<string, IList<string>> mapping = new Dictionary<string
				, IList<string>>();

			internal static void ClearMapping()
			{
				mapping.Clear();
			}

			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
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
		public virtual void TestDisabledAuthorizationOfDefaultServlets()
		{
			Configuration conf = new Configuration();
			// Authorization is disabled by default
			conf.Set(HttpServer2.FilterInitializerProperty, typeof(TestHttpServer.DummyFilterInitializer
				).FullName);
			conf.Set(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(TestHttpServer.MyGroupsProvider
				).FullName);
			Groups.GetUserToGroupsMappingService(conf);
			TestHttpServer.MyGroupsProvider.ClearMapping();
			TestHttpServer.MyGroupsProvider.mapping["userA"] = Arrays.AsList("groupA");
			TestHttpServer.MyGroupsProvider.mapping["userB"] = Arrays.AsList("groupB");
			HttpServer2 myServer = new HttpServer2.Builder().SetName("test").AddEndpoint(new 
				URI("http://localhost:0")).SetFindPort(true).Build();
			myServer.SetAttribute(HttpServer2.ConfContextAttribute, conf);
			myServer.Start();
			string serverURL = "http://" + NetUtils.GetHostPortString(myServer.GetConnectorAddress
				(0)) + "/";
			foreach (string servlet in new string[] { "conf", "logs", "stacks", "logLevel", "metrics"
				 })
			{
				foreach (string user in new string[] { "userA", "userB" })
				{
					NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, GetHttpStatusCode(serverURL
						 + servlet, user));
				}
			}
			myServer.Stop();
		}

		/// <summary>
		/// Verify the administrator access for /logs, /stacks, /conf, /logLevel and
		/// /metrics servlets.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuthorizationOfDefaultServlets()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, true);
			conf.SetBoolean(CommonConfigurationKeys.HadoopSecurityInstrumentationRequiresAdmin
				, true);
			conf.Set(HttpServer2.FilterInitializerProperty, typeof(TestHttpServer.DummyFilterInitializer
				).FullName);
			conf.Set(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(TestHttpServer.MyGroupsProvider
				).FullName);
			Groups.GetUserToGroupsMappingService(conf);
			TestHttpServer.MyGroupsProvider.ClearMapping();
			TestHttpServer.MyGroupsProvider.mapping["userA"] = Arrays.AsList("groupA");
			TestHttpServer.MyGroupsProvider.mapping["userB"] = Arrays.AsList("groupB");
			TestHttpServer.MyGroupsProvider.mapping["userC"] = Arrays.AsList("groupC");
			TestHttpServer.MyGroupsProvider.mapping["userD"] = Arrays.AsList("groupD");
			TestHttpServer.MyGroupsProvider.mapping["userE"] = Arrays.AsList("groupE");
			HttpServer2 myServer = new HttpServer2.Builder().SetName("test").AddEndpoint(new 
				URI("http://localhost:0")).SetFindPort(true).SetConf(conf).SetACL(new AccessControlList
				("userA,userB groupC,groupD")).Build();
			myServer.SetAttribute(HttpServer2.ConfContextAttribute, conf);
			myServer.Start();
			string serverURL = "http://" + NetUtils.GetHostPortString(myServer.GetConnectorAddress
				(0)) + "/";
			foreach (string servlet in new string[] { "conf", "logs", "stacks", "logLevel", "metrics"
				 })
			{
				foreach (string user in new string[] { "userA", "userB", "userC", "userD" })
				{
					NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, GetHttpStatusCode(serverURL
						 + servlet, user));
				}
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpForbidden, GetHttpStatusCode
					(serverURL + servlet, "userE"));
			}
			myServer.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRequestQuoterWithNull()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.DoReturn(null).When(request).GetParameterValues("dummy");
			HttpServer2.QuotingInputFilter.RequestQuoter requestQuoter = new HttpServer2.QuotingInputFilter.RequestQuoter
				(request);
			string[] parameterValues = requestQuoter.GetParameterValues("dummy");
			NUnit.Framework.Assert.IsNull("It should return null " + "when there are no values for the parameter"
				, parameterValues);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRequestQuoterWithNotNull()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			string[] values = new string[] { "abc", "def" };
			Org.Mockito.Mockito.DoReturn(values).When(request).GetParameterValues("dummy");
			HttpServer2.QuotingInputFilter.RequestQuoter requestQuoter = new HttpServer2.QuotingInputFilter.RequestQuoter
				(request);
			string[] parameterValues = requestQuoter.GetParameterValues("dummy");
			NUnit.Framework.Assert.IsTrue("It should return Parameter Values", Arrays.Equals(
				values, parameterValues));
		}

		private static IDictionary<string, object> Parse(string jsonString)
		{
			return (IDictionary<string, object>)JSON.Parse(jsonString);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJersey()
		{
			Log.Info("BEGIN testJersey()");
			string js = ReadOutput(new Uri(baseUrl, "/jersey/foo?op=bar"));
			IDictionary<string, object> m = Parse(js);
			Log.Info("m=" + m);
			NUnit.Framework.Assert.AreEqual("foo", m[JerseyResource.Path]);
			NUnit.Framework.Assert.AreEqual("bar", m[JerseyResource.Op]);
			Log.Info("END testJersey()");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHasAdministratorAccess()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, false);
			ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
			Org.Mockito.Mockito.When(context.GetAttribute(HttpServer2.ConfContextAttribute)).
				ThenReturn(conf);
			Org.Mockito.Mockito.When(context.GetAttribute(HttpServer2.AdminsAcl)).ThenReturn(
				null);
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(request.GetRemoteUser()).ThenReturn(null);
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			//authorization OFF
			NUnit.Framework.Assert.IsTrue(HttpServer2.HasAdministratorAccess(context, request
				, response));
			//authorization ON & user NULL
			response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			conf.SetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, true);
			NUnit.Framework.Assert.IsFalse(HttpServer2.HasAdministratorAccess(context, request
				, response));
			Org.Mockito.Mockito.Verify(response).SendError(Org.Mockito.Mockito.Eq(HttpServletResponse
				.ScForbidden), Org.Mockito.Mockito.AnyString());
			//authorization ON & user NOT NULL & ACLs NULL
			response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(request.GetRemoteUser()).ThenReturn("foo");
			NUnit.Framework.Assert.IsTrue(HttpServer2.HasAdministratorAccess(context, request
				, response));
			//authorization ON & user NOT NULL & ACLs NOT NULL & user not in ACLs
			response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			AccessControlList acls = Org.Mockito.Mockito.Mock<AccessControlList>();
			Org.Mockito.Mockito.When(acls.IsUserAllowed(Org.Mockito.Mockito.Any<UserGroupInformation
				>())).ThenReturn(false);
			Org.Mockito.Mockito.When(context.GetAttribute(HttpServer2.AdminsAcl)).ThenReturn(
				acls);
			NUnit.Framework.Assert.IsFalse(HttpServer2.HasAdministratorAccess(context, request
				, response));
			Org.Mockito.Mockito.Verify(response).SendError(Org.Mockito.Mockito.Eq(HttpServletResponse
				.ScForbidden), Org.Mockito.Mockito.AnyString());
			//authorization ON & user NOT NULL & ACLs NOT NULL & user in in ACLs
			response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(acls.IsUserAllowed(Org.Mockito.Mockito.Any<UserGroupInformation
				>())).ThenReturn(true);
			Org.Mockito.Mockito.When(context.GetAttribute(HttpServer2.AdminsAcl)).ThenReturn(
				acls);
			NUnit.Framework.Assert.IsTrue(HttpServer2.HasAdministratorAccess(context, request
				, response));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRequiresAuthorizationAccess()
		{
			Configuration conf = new Configuration();
			ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
			Org.Mockito.Mockito.When(context.GetAttribute(HttpServer2.ConfContextAttribute)).
				ThenReturn(conf);
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			//requires admin access to instrumentation, FALSE by default
			NUnit.Framework.Assert.IsTrue(HttpServer2.IsInstrumentationAccessAllowed(context, 
				request, response));
			//requires admin access to instrumentation, TRUE
			conf.SetBoolean(CommonConfigurationKeys.HadoopSecurityInstrumentationRequiresAdmin
				, true);
			conf.SetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, true);
			AccessControlList acls = Org.Mockito.Mockito.Mock<AccessControlList>();
			Org.Mockito.Mockito.When(acls.IsUserAllowed(Org.Mockito.Mockito.Any<UserGroupInformation
				>())).ThenReturn(false);
			Org.Mockito.Mockito.When(context.GetAttribute(HttpServer2.AdminsAcl)).ThenReturn(
				acls);
			NUnit.Framework.Assert.IsFalse(HttpServer2.IsInstrumentationAccessAllowed(context
				, request, response));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBindAddress()
		{
			CheckBindAddress("localhost", 0, false).Stop();
			// hang onto this one for a bit more testing
			HttpServer2 myServer = CheckBindAddress("localhost", 0, false);
			HttpServer2 myServer2 = null;
			try
			{
				int port = myServer.GetConnectorAddress(0).Port;
				// it's already in use, true = expect a higher port
				myServer2 = CheckBindAddress("localhost", port, true);
				// try to reuse the port
				port = myServer2.GetConnectorAddress(0).Port;
				myServer2.Stop();
				NUnit.Framework.Assert.IsNull(myServer2.GetConnectorAddress(0));
				// not bound
				myServer2.OpenListeners();
				NUnit.Framework.Assert.AreEqual(port, myServer2.GetConnectorAddress(0).Port);
			}
			finally
			{
				// expect same port
				myServer.Stop();
				if (myServer2 != null)
				{
					myServer2.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private HttpServer2 CheckBindAddress(string host, int port, bool findPort)
		{
			HttpServer2 server = CreateServer(host, port);
			try
			{
				// not bound, ephemeral should return requested port (0 for ephemeral)
				IList<object> listeners = (IList<object>)Whitebox.GetInternalState(server, "listeners"
					);
				Connector listener = (Connector)listeners[0];
				NUnit.Framework.Assert.AreEqual(port, listener.GetPort());
				// verify hostname is what was given
				server.OpenListeners();
				NUnit.Framework.Assert.AreEqual(host, server.GetConnectorAddress(0).GetHostName()
					);
				int boundPort = server.GetConnectorAddress(0).Port;
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
			catch (Exception e)
			{
				server.Stop();
				throw;
			}
			return server;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoCacheHeader()
		{
			Uri url = new Uri(baseUrl, "/echo?a=b&c=d");
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			NUnit.Framework.Assert.AreEqual("no-cache", conn.GetHeaderField("Cache-Control"));
			NUnit.Framework.Assert.AreEqual("no-cache", conn.GetHeaderField("Pragma"));
			NUnit.Framework.Assert.IsNotNull(conn.GetHeaderField("Expires"));
			NUnit.Framework.Assert.IsNotNull(conn.GetHeaderField("Date"));
			NUnit.Framework.Assert.AreEqual(conn.GetHeaderField("Expires"), conn.GetHeaderField
				("Date"));
		}
	}
}
