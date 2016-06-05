using System;
using System.Collections.Generic;
using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Ssl;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	public class TestHttpCookieFlag
	{
		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(TestHttpCookieFlag).Name;

		private static string keystoresDir;

		private static string sslConfDir;

		private static SSLFactory clientSslFactory;

		private static HttpServer2 server;

		public class DummyAuthenticationFilter : Filter
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			public virtual void Init(FilterConfig filterConfig)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public virtual void DoFilter(ServletRequest request, ServletResponse response, FilterChain
				 chain)
			{
				HttpServletResponse resp = (HttpServletResponse)response;
				bool isHttps = "https".Equals(request.GetScheme());
				AuthenticationFilter.CreateAuthCookie(resp, "token", null, null, -1, isHttps);
				chain.DoFilter(request, resp);
			}

			public virtual void Destroy()
			{
			}
		}

		public class DummyFilterInitializer : FilterInitializer
		{
			public override void InitFilter(FilterContainer container, Configuration conf)
			{
				container.AddFilter("DummyAuth", typeof(TestHttpCookieFlag.DummyAuthenticationFilter
					).FullName, null);
			}
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			Configuration conf = new Configuration();
			conf.Set(HttpServer2.FilterInitializerProperty, typeof(TestHttpCookieFlag.DummyFilterInitializer
				).FullName);
			FilePath @base = new FilePath(Basedir);
			FileUtil.FullyDelete(@base);
			@base.Mkdirs();
			keystoresDir = new FilePath(Basedir).GetAbsolutePath();
			sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestSSLHttpServer));
			KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, conf, false);
			Configuration sslConf = new Configuration(false);
			sslConf.AddResource("ssl-server.xml");
			sslConf.AddResource("ssl-client.xml");
			clientSslFactory = new SSLFactory(SSLFactory.Mode.Client, sslConf);
			clientSslFactory.Init();
			server = new HttpServer2.Builder().SetName("test").AddEndpoint(new URI("http://localhost"
				)).AddEndpoint(new URI("https://localhost")).SetConf(conf).KeyPassword(sslConf.Get
				("ssl.server.keystore.keypassword")).KeyStore(sslConf.Get("ssl.server.keystore.location"
				), sslConf.Get("ssl.server.keystore.password"), sslConf.Get("ssl.server.keystore.type"
				, "jks")).TrustStore(sslConf.Get("ssl.server.truststore.location"), sslConf.Get(
				"ssl.server.truststore.password"), sslConf.Get("ssl.server.truststore.type", "jks"
				)).Build();
			server.AddServlet("echo", "/echo", typeof(TestHttpServer.EchoServlet));
			server.Start();
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestHttpCookie()
		{
			Uri @base = new Uri("http://" + NetUtils.GetHostPortString(server.GetConnectorAddress
				(0)));
			HttpURLConnection conn = (HttpURLConnection)new Uri(@base, "/echo").OpenConnection
				();
			string header = conn.GetHeaderField("Set-Cookie");
			IList<HttpCookie> cookies = HttpCookie.Parse(header);
			Assert.True(!cookies.IsEmpty());
			Assert.True(header.Contains("; HttpOnly"));
			Assert.True("token".Equals(cookies[0].GetValue()));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		[Fact]
		public virtual void TestHttpsCookie()
		{
			Uri @base = new Uri("https://" + NetUtils.GetHostPortString(server.GetConnectorAddress
				(1)));
			HttpsURLConnection conn = (HttpsURLConnection)new Uri(@base, "/echo").OpenConnection
				();
			conn.SetSSLSocketFactory(clientSslFactory.CreateSSLSocketFactory());
			string header = conn.GetHeaderField("Set-Cookie");
			IList<HttpCookie> cookies = HttpCookie.Parse(header);
			Assert.True(!cookies.IsEmpty());
			Assert.True(header.Contains("; HttpOnly"));
			Assert.True(cookies[0].GetSecure());
			Assert.True("token".Equals(cookies[0].GetValue()));
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Cleanup()
		{
			server.Stop();
			FileUtil.FullyDelete(new FilePath(Basedir));
			KeyStoreTestUtil.CleanupSSLConfig(keystoresDir, sslConfDir);
			clientSslFactory.Destroy();
		}
	}
}
