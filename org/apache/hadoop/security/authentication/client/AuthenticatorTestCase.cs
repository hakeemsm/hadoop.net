using Sharpen;

namespace org.apache.hadoop.security.authentication.client
{
	public class AuthenticatorTestCase
	{
		private org.mortbay.jetty.Server server;

		private string host = null;

		private int port = -1;

		private bool useTomcat = false;

		private org.apache.catalina.startup.Tomcat tomcat = null;

		internal org.mortbay.jetty.servlet.Context context;

		private static java.util.Properties authenticatorConfig;

		public AuthenticatorTestCase()
		{
		}

		public AuthenticatorTestCase(bool useTomcat)
		{
			this.useTomcat = useTomcat;
		}

		protected internal static void setAuthenticationHandlerConfig(java.util.Properties
			 config)
		{
			authenticatorConfig = config;
		}

		public class TestFilter : org.apache.hadoop.security.authentication.server.AuthenticationFilter
		{
			/// <exception cref="javax.servlet.ServletException"/>
			protected internal override java.util.Properties getConfiguration(string configPrefix
				, javax.servlet.FilterConfig filterConfig)
			{
				return authenticatorConfig;
			}
		}

		[System.Serializable]
		public class TestServlet : javax.servlet.http.HttpServlet
		{
			/// <exception cref="javax.servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void doGet(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse
				 resp)
			{
				resp.setStatus(javax.servlet.http.HttpServletResponse.SC_OK);
			}

			/// <exception cref="javax.servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void doPost(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse
				 resp)
			{
				java.io.InputStream @is = req.getInputStream();
				java.io.OutputStream os = resp.getOutputStream();
				int c = @is.read();
				while (c > -1)
				{
					os.write(c);
					c = @is.read();
				}
				@is.close();
				os.close();
				resp.setStatus(javax.servlet.http.HttpServletResponse.SC_OK);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual int getLocalPort()
		{
			java.net.ServerSocket ss = new java.net.ServerSocket(0);
			int ret = ss.getLocalPort();
			ss.close();
			return ret;
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void start()
		{
			if (useTomcat)
			{
				startTomcat();
			}
			else
			{
				startJetty();
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void startJetty()
		{
			server = new org.mortbay.jetty.Server(0);
			context = new org.mortbay.jetty.servlet.Context();
			context.setContextPath("/foo");
			server.setHandler(context);
			context.addFilter(new org.mortbay.jetty.servlet.FilterHolder(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.TestFilter
				))), "/*", 0);
			context.addServlet(new org.mortbay.jetty.servlet.ServletHolder(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.TestServlet
				))), "/bar");
			host = "localhost";
			port = getLocalPort();
			server.getConnectors()[0].setHost(host);
			server.getConnectors()[0].setPort(port);
			server.start();
			System.Console.Out.WriteLine("Running embedded servlet container at: http://" + host
				 + ":" + port);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void startTomcat()
		{
			tomcat = new org.apache.catalina.startup.Tomcat();
			java.io.File @base = new java.io.File(Sharpen.Runtime.getProperty("java.io.tmpdir"
				));
			org.apache.catalina.Context ctx = tomcat.addContext("/foo", @base.getAbsolutePath
				());
			org.apache.catalina.deploy.FilterDef fd = new org.apache.catalina.deploy.FilterDef
				();
			fd.setFilterClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.TestFilter
				)).getName());
			fd.setFilterName("TestFilter");
			org.apache.catalina.deploy.FilterMap fm = new org.apache.catalina.deploy.FilterMap
				();
			fm.setFilterName("TestFilter");
			fm.addURLPattern("/*");
			fm.addServletName("/bar");
			ctx.addFilterDef(fd);
			ctx.addFilterMap(fm);
			org.apache.catalina.startup.Tomcat.addServlet(ctx, "/bar", Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.TestServlet
				)).getName());
			ctx.addServletMapping("/bar", "/bar");
			host = "localhost";
			port = getLocalPort();
			tomcat.setHostname(host);
			tomcat.setPort(port);
			tomcat.start();
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void stop()
		{
			if (useTomcat)
			{
				stopTomcat();
			}
			else
			{
				stopJetty();
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void stopJetty()
		{
			try
			{
				server.stop();
			}
			catch (System.Exception)
			{
			}
			try
			{
				server.destroy();
			}
			catch (System.Exception)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void stopTomcat()
		{
			try
			{
				tomcat.stop();
			}
			catch (System.Exception)
			{
			}
			try
			{
				tomcat.destroy();
			}
			catch (System.Exception)
			{
			}
		}

		protected internal virtual string getBaseURL()
		{
			return "http://" + host + ":" + port + "/foo/bar";
		}

		private class TestConnectionConfigurator : org.apache.hadoop.security.authentication.client.ConnectionConfigurator
		{
			internal bool invoked;

			/// <exception cref="System.IO.IOException"/>
			public virtual java.net.HttpURLConnection configure(java.net.HttpURLConnection conn
				)
			{
				invoked = true;
				return conn;
			}
		}

		private string POST = "test";

		/// <exception cref="System.Exception"/>
		protected internal virtual void _testAuthentication(org.apache.hadoop.security.authentication.client.Authenticator
			 authenticator, bool doPost)
		{
			start();
			try
			{
				java.net.URL url = new java.net.URL(getBaseURL());
				org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token token = new 
					org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token();
				NUnit.Framework.Assert.IsFalse(token.isSet());
				org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.TestConnectionConfigurator
					 connConf = new org.apache.hadoop.security.authentication.client.AuthenticatorTestCase.TestConnectionConfigurator
					();
				org.apache.hadoop.security.authentication.client.AuthenticatedURL aUrl = new org.apache.hadoop.security.authentication.client.AuthenticatedURL
					(authenticator, connConf);
				java.net.HttpURLConnection conn = aUrl.openConnection(url, token);
				NUnit.Framework.Assert.IsTrue(connConf.invoked);
				string tokenStr = token.ToString();
				if (doPost)
				{
					conn.setRequestMethod("POST");
					conn.setDoOutput(true);
				}
				conn.connect();
				if (doPost)
				{
					System.IO.TextWriter writer = new java.io.OutputStreamWriter(conn.getOutputStream
						());
					writer.write(POST);
					writer.close();
				}
				NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_OK, conn.getResponseCode
					());
				if (doPost)
				{
					java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader
						(conn.getInputStream()));
					string echo = reader.readLine();
					NUnit.Framework.Assert.AreEqual(POST, echo);
					NUnit.Framework.Assert.IsNull(reader.readLine());
				}
				aUrl = new org.apache.hadoop.security.authentication.client.AuthenticatedURL();
				conn = aUrl.openConnection(url, token);
				conn.connect();
				NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_OK, conn.getResponseCode
					());
				NUnit.Framework.Assert.AreEqual(tokenStr, token.ToString());
			}
			finally
			{
				stop();
			}
		}

		private org.apache.http.impl.client.SystemDefaultHttpClient getHttpClient()
		{
			org.apache.http.impl.client.SystemDefaultHttpClient httpClient = new org.apache.http.impl.client.SystemDefaultHttpClient
				();
			httpClient.getAuthSchemes().register(org.apache.http.client.@params.AuthPolicy.SPNEGO
				, new org.apache.http.impl.auth.SPNegoSchemeFactory(true));
			org.apache.http.auth.Credentials use_jaas_creds = new _Credentials_247();
			httpClient.getCredentialsProvider().setCredentials(org.apache.http.auth.AuthScope
				.ANY, use_jaas_creds);
			return httpClient;
		}

		private sealed class _Credentials_247 : org.apache.http.auth.Credentials
		{
			public _Credentials_247()
			{
			}

			public string getPassword()
			{
				return null;
			}

			public java.security.Principal getUserPrincipal()
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		private void doHttpClientRequest(org.apache.http.client.HttpClient httpClient, org.apache.http.client.methods.HttpUriRequest
			 request)
		{
			org.apache.http.HttpResponse response = null;
			try
			{
				response = httpClient.execute(request);
				int httpStatus = response.getStatusLine().getStatusCode();
				NUnit.Framework.Assert.AreEqual(java.net.HttpURLConnection.HTTP_OK, httpStatus);
			}
			finally
			{
				if (response != null)
				{
					org.apache.http.util.EntityUtils.consumeQuietly(response.getEntity());
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void _testAuthenticationHttpClient(org.apache.hadoop.security.authentication.client.Authenticator
			 authenticator, bool doPost)
		{
			start();
			try
			{
				org.apache.http.impl.client.SystemDefaultHttpClient httpClient = getHttpClient();
				doHttpClientRequest(httpClient, new org.apache.http.client.methods.HttpGet(getBaseURL
					()));
				// Always do a GET before POST to trigger the SPNego negotiation
				if (doPost)
				{
					org.apache.http.client.methods.HttpPost post = new org.apache.http.client.methods.HttpPost
						(getBaseURL());
					byte[] postBytes = Sharpen.Runtime.getBytesForString(POST);
					java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(postBytes);
					org.apache.http.entity.InputStreamEntity entity = new org.apache.http.entity.InputStreamEntity
						(bis, postBytes.Length);
					// Important that the entity is not repeatable -- this means if
					// we have to renegotiate (e.g. b/c the cookie wasn't handled properly)
					// the test will fail.
					NUnit.Framework.Assert.IsFalse(entity.isRepeatable());
					post.setEntity(entity);
					doHttpClientRequest(httpClient, post);
				}
			}
			finally
			{
				stop();
			}
		}
	}
}
