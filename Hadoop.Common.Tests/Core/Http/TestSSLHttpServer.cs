using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Ssl;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	/// <summary>
	/// This testcase issues SSL certificates configures the HttpServer to serve
	/// HTTPS using the created certficates and calls an echo servlet using the
	/// corresponding HTTPS URL.
	/// </summary>
	public class TestSSLHttpServer : HttpServerFunctionalTest
	{
		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(TestSSLHttpServer).Name;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestSSLHttpServer));

		private static Configuration conf;

		private static HttpServer2 server;

		private static string keystoresDir;

		private static string sslConfDir;

		private static SSLFactory clientSslFactory;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			conf = new Configuration();
			conf.SetInt(HttpServer2.HttpMaxThreads, 10);
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
			server = new HttpServer2.Builder().SetName("test").AddEndpoint(new URI("https://localhost"
				)).SetConf(conf).KeyPassword(sslConf.Get("ssl.server.keystore.keypassword")).KeyStore
				(sslConf.Get("ssl.server.keystore.location"), sslConf.Get("ssl.server.keystore.password"
				), sslConf.Get("ssl.server.keystore.type", "jks")).TrustStore(sslConf.Get("ssl.server.truststore.location"
				), sslConf.Get("ssl.server.truststore.password"), sslConf.Get("ssl.server.truststore.type"
				, "jks")).Build();
			server.AddServlet("echo", "/echo", typeof(TestHttpServer.EchoServlet));
			server.AddServlet("longheader", "/longheader", typeof(HttpServerFunctionalTest.LongHeaderServlet
				));
			server.Start();
			baseUrl = new Uri("https://" + NetUtils.GetHostPortString(server.GetConnectorAddress
				(0)));
			Log.Info("HTTP server started: " + baseUrl);
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

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestEcho()
		{
			Assert.Equal("a:b\nc:d\n", ReadOut(new Uri(baseUrl, "/echo?a=b&c=d"
				)));
			Assert.Equal("a:b\nc&lt;:d\ne:&gt;\n", ReadOut(new Uri(baseUrl
				, "/echo?a=b&c<=d&e=>")));
		}

		/// <summary>Test that verifies headers can be up to 64K long.</summary>
		/// <remarks>
		/// Test that verifies headers can be up to 64K long.
		/// The test adds a 63K header leaving 1K for other headers.
		/// This is because the header buffer setting is for ALL headers,
		/// names and values included.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLongHeader()
		{
			Uri url = new Uri(baseUrl, "/longheader");
			HttpsURLConnection conn = (HttpsURLConnection)url.OpenConnection();
			conn.SetSSLSocketFactory(clientSslFactory.CreateSSLSocketFactory());
			TestLongHeader(conn);
		}

		/// <exception cref="System.Exception"/>
		private static string ReadOut(Uri url)
		{
			HttpsURLConnection conn = (HttpsURLConnection)url.OpenConnection();
			conn.SetSSLSocketFactory(clientSslFactory.CreateSSLSocketFactory());
			InputStream @in = conn.GetInputStream();
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			IOUtils.CopyBytes(@in, @out, 1024);
			return @out.ToString();
		}
	}
}
