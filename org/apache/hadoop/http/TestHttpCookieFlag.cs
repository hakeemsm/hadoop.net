using Sharpen;

namespace org.apache.hadoop.http
{
	public class TestHttpCookieFlag
	{
		private static readonly string BASEDIR = Sharpen.Runtime.getProperty("test.build.dir"
			, "target/test-dir") + "/" + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpCookieFlag
			)).getSimpleName();

		private static string keystoresDir;

		private static string sslConfDir;

		private static org.apache.hadoop.security.ssl.SSLFactory clientSslFactory;

		private static org.apache.hadoop.http.HttpServer2 server;

		public class DummyAuthenticationFilter : javax.servlet.Filter
		{
			/// <exception cref="javax.servlet.ServletException"/>
			public virtual void init(javax.servlet.FilterConfig filterConfig)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="javax.servlet.ServletException"/>
			public virtual void doFilter(javax.servlet.ServletRequest request, javax.servlet.ServletResponse
				 response, javax.servlet.FilterChain chain)
			{
				javax.servlet.http.HttpServletResponse resp = (javax.servlet.http.HttpServletResponse
					)response;
				bool isHttps = "https".Equals(request.getScheme());
				org.apache.hadoop.security.authentication.server.AuthenticationFilter.createAuthCookie
					(resp, "token", null, null, -1, isHttps);
				chain.doFilter(request, resp);
			}

			public virtual void destroy()
			{
			}
		}

		public class DummyFilterInitializer : org.apache.hadoop.http.FilterInitializer
		{
			public override void initFilter(org.apache.hadoop.http.FilterContainer container, 
				org.apache.hadoop.conf.Configuration conf)
			{
				container.addFilter("DummyAuth", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpCookieFlag.DummyAuthenticationFilter
					)).getName(), null);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void setUp()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.http.HttpServer2.FILTER_INITIALIZER_PROPERTY, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.http.TestHttpCookieFlag.DummyFilterInitializer)).getName
				());
			java.io.File @base = new java.io.File(BASEDIR);
			org.apache.hadoop.fs.FileUtil.fullyDelete(@base);
			@base.mkdirs();
			keystoresDir = new java.io.File(BASEDIR).getAbsolutePath();
			sslConfDir = org.apache.hadoop.security.ssl.KeyStoreTestUtil.getClasspathDir(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.http.TestSSLHttpServer)));
			org.apache.hadoop.security.ssl.KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir
				, conf, false);
			org.apache.hadoop.conf.Configuration sslConf = new org.apache.hadoop.conf.Configuration
				(false);
			sslConf.addResource("ssl-server.xml");
			sslConf.addResource("ssl-client.xml");
			clientSslFactory = new org.apache.hadoop.security.ssl.SSLFactory(org.apache.hadoop.security.ssl.SSLFactory.Mode
				.CLIENT, sslConf);
			clientSslFactory.init();
			server = new org.apache.hadoop.http.HttpServer2.Builder().setName("test").addEndpoint
				(new java.net.URI("http://localhost")).addEndpoint(new java.net.URI("https://localhost"
				)).setConf(conf).keyPassword(sslConf.get("ssl.server.keystore.keypassword")).keyStore
				(sslConf.get("ssl.server.keystore.location"), sslConf.get("ssl.server.keystore.password"
				), sslConf.get("ssl.server.keystore.type", "jks")).trustStore(sslConf.get("ssl.server.truststore.location"
				), sslConf.get("ssl.server.truststore.password"), sslConf.get("ssl.server.truststore.type"
				, "jks")).build();
			server.addServlet("echo", "/echo", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpServer.EchoServlet
				)));
			server.start();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testHttpCookie()
		{
			java.net.URL @base = new java.net.URL("http://" + org.apache.hadoop.net.NetUtils.
				getHostPortString(server.getConnectorAddress(0)));
			java.net.HttpURLConnection conn = (java.net.HttpURLConnection)new java.net.URL(@base
				, "/echo").openConnection();
			string header = conn.getHeaderField("Set-Cookie");
			System.Collections.Generic.IList<java.net.HttpCookie> cookies = java.net.HttpCookie
				.parse(header);
			NUnit.Framework.Assert.IsTrue(!cookies.isEmpty());
			NUnit.Framework.Assert.IsTrue(header.contains("; HttpOnly"));
			NUnit.Framework.Assert.IsTrue("token".Equals(cookies[0].getValue()));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.GeneralSecurityException"/>
		[NUnit.Framework.Test]
		public virtual void testHttpsCookie()
		{
			java.net.URL @base = new java.net.URL("https://" + org.apache.hadoop.net.NetUtils
				.getHostPortString(server.getConnectorAddress(1)));
			javax.net.ssl.HttpsURLConnection conn = (javax.net.ssl.HttpsURLConnection)new java.net.URL
				(@base, "/echo").openConnection();
			conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
			string header = conn.getHeaderField("Set-Cookie");
			System.Collections.Generic.IList<java.net.HttpCookie> cookies = java.net.HttpCookie
				.parse(header);
			NUnit.Framework.Assert.IsTrue(!cookies.isEmpty());
			NUnit.Framework.Assert.IsTrue(header.contains("; HttpOnly"));
			NUnit.Framework.Assert.IsTrue(cookies[0].getSecure());
			NUnit.Framework.Assert.IsTrue("token".Equals(cookies[0].getValue()));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.AfterClass]
		public static void cleanup()
		{
			server.stop();
			org.apache.hadoop.fs.FileUtil.fullyDelete(new java.io.File(BASEDIR));
			org.apache.hadoop.security.ssl.KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir
				);
			clientSslFactory.destroy();
		}
	}
}
