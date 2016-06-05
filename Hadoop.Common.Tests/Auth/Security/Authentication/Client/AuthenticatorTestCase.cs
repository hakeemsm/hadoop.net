using System;
using System.IO;
using System.Net.Sockets;
using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Catalina;
using Org.Apache.Catalina.Deploy;
using Org.Apache.Catalina.Startup;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Http;
using Org.Apache.Http.Auth;
using Org.Apache.Http.Client;
using Org.Apache.Http.Client.Methods;
using Org.Apache.Http.Client.Params;
using Org.Apache.Http.Entity;
using Org.Apache.Http.Impl.Auth;
using Org.Apache.Http.Impl.Client;
using Org.Apache.Http.Util;
using Org.Mortbay.Jetty;
using Org.Mortbay.Jetty.Servlet;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Client
{
	public class AuthenticatorTestCase
	{
		private Server server;

		private string host = null;

		private int port = -1;

		private bool useTomcat = false;

		private Tomcat tomcat = null;

		internal Context context;

		private static Properties authenticatorConfig;

		public AuthenticatorTestCase()
		{
		}

		public AuthenticatorTestCase(bool useTomcat)
		{
			this.useTomcat = useTomcat;
		}

		protected internal static void SetAuthenticationHandlerConfig(Properties config)
		{
			authenticatorConfig = config;
		}

		public class TestFilter : AuthenticationFilter
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			protected internal override Properties GetConfiguration(string configPrefix, FilterConfig
				 filterConfig)
			{
				return authenticatorConfig;
			}
		}

		[System.Serializable]
		public class TestServlet : HttpServlet
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
			{
				resp.SetStatus(HttpServletResponse.ScOk);
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoPost(HttpServletRequest req, HttpServletResponse resp)
			{
				InputStream @is = req.GetInputStream();
				OutputStream os = resp.GetOutputStream();
				int c = @is.Read();
				while (c > -1)
				{
					os.Write(c);
					c = @is.Read();
				}
				@is.Close();
				os.Close();
				resp.SetStatus(HttpServletResponse.ScOk);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual int GetLocalPort()
		{
			Socket ss = Sharpen.Extensions.CreateServerSocket(0);
			int ret = ss.GetLocalPort();
			ss.Close();
			return ret;
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void Start()
		{
			if (useTomcat)
			{
				StartTomcat();
			}
			else
			{
				StartJetty();
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void StartJetty()
		{
			server = new Org.Mortbay.Jetty.Server(0);
			context = new Context();
			context.SetContextPath("/foo");
			server.SetHandler(context);
			context.AddFilter(new FilterHolder(typeof(AuthenticatorTestCase.TestFilter)), "/*"
				, 0);
			context.AddServlet(new ServletHolder(typeof(AuthenticatorTestCase.TestServlet)), 
				"/bar");
			host = "localhost";
			port = GetLocalPort();
			server.GetConnectors()[0].SetHost(host);
			server.GetConnectors()[0].SetPort(port);
			server.Start();
			System.Console.Out.WriteLine("Running embedded servlet container at: http://" + host
				 + ":" + port);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void StartTomcat()
		{
			tomcat = new Tomcat();
			FilePath @base = new FilePath(Runtime.GetProperty("java.io.tmpdir"));
			Context ctx = tomcat.AddContext("/foo", @base.GetAbsolutePath());
			FilterDef fd = new FilterDef();
			fd.SetFilterClass(typeof(AuthenticatorTestCase.TestFilter).FullName);
			fd.SetFilterName("TestFilter");
			FilterMap fm = new FilterMap();
			fm.SetFilterName("TestFilter");
			fm.AddURLPattern("/*");
			fm.AddServletName("/bar");
			ctx.AddFilterDef(fd);
			ctx.AddFilterMap(fm);
			Tomcat.AddServlet(ctx, "/bar", typeof(AuthenticatorTestCase.TestServlet).FullName
				);
			ctx.AddServletMapping("/bar", "/bar");
			host = "localhost";
			port = GetLocalPort();
			tomcat.SetHostname(host);
			tomcat.SetPort(port);
			tomcat.Start();
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void Stop()
		{
			if (useTomcat)
			{
				StopTomcat();
			}
			else
			{
				StopJetty();
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void StopJetty()
		{
			try
			{
				server.Stop();
			}
			catch (Exception)
			{
			}
			try
			{
				server.Destroy();
			}
			catch (Exception)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void StopTomcat()
		{
			try
			{
				tomcat.Stop();
			}
			catch (Exception)
			{
			}
			try
			{
				tomcat.Destroy();
			}
			catch (Exception)
			{
			}
		}

		protected internal virtual string GetBaseURL()
		{
			return "http://" + host + ":" + port + "/foo/bar";
		}

		private class TestConnectionConfigurator : ConnectionConfigurator
		{
			internal bool invoked;

			/// <exception cref="System.IO.IOException"/>
			public virtual HttpURLConnection Configure(HttpURLConnection conn)
			{
				invoked = true;
				return conn;
			}
		}

		private string Post = "test";

		/// <exception cref="System.Exception"/>
		protected internal virtual void _testAuthentication(Authenticator authenticator, 
			bool doPost)
		{
			Start();
			try
			{
				Uri url = new Uri(GetBaseURL());
				AuthenticatedURL.Token token = new AuthenticatedURL.Token();
				NUnit.Framework.Assert.IsFalse(token.IsSet());
				AuthenticatorTestCase.TestConnectionConfigurator connConf = new AuthenticatorTestCase.TestConnectionConfigurator
					();
				AuthenticatedURL aUrl = new AuthenticatedURL(authenticator, connConf);
				HttpURLConnection conn = aUrl.OpenConnection(url, token);
				Assert.True(connConf.invoked);
				string tokenStr = token.ToString();
				if (doPost)
				{
					conn.SetRequestMethod("POST");
					conn.SetDoOutput(true);
				}
				conn.Connect();
				if (doPost)
				{
					TextWriter writer = new OutputStreamWriter(conn.GetOutputStream());
					writer.Write(Post);
					writer.Close();
				}
				Assert.Equal(HttpURLConnection.HttpOk, conn.GetResponseCode());
				if (doPost)
				{
					BufferedReader reader = new BufferedReader(new InputStreamReader(conn.GetInputStream
						()));
					string echo = reader.ReadLine();
					Assert.Equal(Post, echo);
					NUnit.Framework.Assert.IsNull(reader.ReadLine());
				}
				aUrl = new AuthenticatedURL();
				conn = aUrl.OpenConnection(url, token);
				conn.Connect();
				Assert.Equal(HttpURLConnection.HttpOk, conn.GetResponseCode());
				Assert.Equal(tokenStr, token.ToString());
			}
			finally
			{
				Stop();
			}
		}

		private SystemDefaultHttpClient GetHttpClient()
		{
			SystemDefaultHttpClient httpClient = new SystemDefaultHttpClient();
			httpClient.GetAuthSchemes().Register(AuthPolicy.Spnego, new SPNegoSchemeFactory(true
				));
			Credentials use_jaas_creds = new _Credentials_247();
			httpClient.GetCredentialsProvider().SetCredentials(AuthScope.Any, use_jaas_creds);
			return httpClient;
		}

		private sealed class _Credentials_247 : Credentials
		{
			public _Credentials_247()
			{
			}

			public string GetPassword()
			{
				return null;
			}

			public Principal GetUserPrincipal()
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoHttpClientRequest(HttpClient httpClient, HttpUriRequest request)
		{
			HttpResponse response = null;
			try
			{
				response = httpClient.Execute(request);
				int httpStatus = response.GetStatusLine().GetStatusCode();
				Assert.Equal(HttpURLConnection.HttpOk, httpStatus);
			}
			finally
			{
				if (response != null)
				{
					EntityUtils.ConsumeQuietly(response.GetEntity());
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void _testAuthenticationHttpClient(Authenticator authenticator
			, bool doPost)
		{
			Start();
			try
			{
				SystemDefaultHttpClient httpClient = GetHttpClient();
				DoHttpClientRequest(httpClient, new HttpGet(GetBaseURL()));
				// Always do a GET before POST to trigger the SPNego negotiation
				if (doPost)
				{
					HttpPost post = new HttpPost(GetBaseURL());
					byte[] postBytes = Sharpen.Runtime.GetBytesForString(Post);
					ByteArrayInputStream bis = new ByteArrayInputStream(postBytes);
					InputStreamEntity entity = new InputStreamEntity(bis, postBytes.Length);
					// Important that the entity is not repeatable -- this means if
					// we have to renegotiate (e.g. b/c the cookie wasn't handled properly)
					// the test will fail.
					NUnit.Framework.Assert.IsFalse(entity.IsRepeatable());
					post.SetEntity(entity);
					DoHttpClientRequest(httpClient, post);
				}
			}
			finally
			{
				Stop();
			}
		}
	}
}
