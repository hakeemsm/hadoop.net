using Sharpen;

namespace org.apache.hadoop.http
{
	/// <summary>
	/// This testcase issues SSL certificates configures the HttpServer to serve
	/// HTTPS using the created certficates and calls an echo servlet using the
	/// corresponding HTTPS URL.
	/// </summary>
	public class TestSSLHttpServer : org.apache.hadoop.http.HttpServerFunctionalTest
	{
		private static readonly string BASEDIR = Sharpen.Runtime.getProperty("test.build.dir"
			, "target/test-dir") + "/" + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestSSLHttpServer
			)).getSimpleName();

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestSSLHttpServer
			)));

		private static org.apache.hadoop.conf.Configuration conf;

		private static org.apache.hadoop.http.HttpServer2 server;

		private static string keystoresDir;

		private static string sslConfDir;

		private static org.apache.hadoop.security.ssl.SSLFactory clientSslFactory;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			conf.setInt(org.apache.hadoop.http.HttpServer2.HTTP_MAX_THREADS, 10);
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
				(new java.net.URI("https://localhost")).setConf(conf).keyPassword(sslConf.get("ssl.server.keystore.keypassword"
				)).keyStore(sslConf.get("ssl.server.keystore.location"), sslConf.get("ssl.server.keystore.password"
				), sslConf.get("ssl.server.keystore.type", "jks")).trustStore(sslConf.get("ssl.server.truststore.location"
				), sslConf.get("ssl.server.truststore.password"), sslConf.get("ssl.server.truststore.type"
				, "jks")).build();
			server.addServlet("echo", "/echo", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.TestHttpServer.EchoServlet
				)));
			server.addServlet("longheader", "/longheader", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.http.HttpServerFunctionalTest.LongHeaderServlet)));
			server.start();
			baseUrl = new java.net.URL("https://" + org.apache.hadoop.net.NetUtils.getHostPortString
				(server.getConnectorAddress(0)));
			LOG.info("HTTP server started: " + baseUrl);
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

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testEcho()
		{
			NUnit.Framework.Assert.AreEqual("a:b\nc:d\n", readOut(new java.net.URL(baseUrl, "/echo?a=b&c=d"
				)));
			NUnit.Framework.Assert.AreEqual("a:b\nc&lt;:d\ne:&gt;\n", readOut(new java.net.URL
				(baseUrl, "/echo?a=b&c<=d&e=>")));
		}

		/// <summary>Test that verifies headers can be up to 64K long.</summary>
		/// <remarks>
		/// Test that verifies headers can be up to 64K long.
		/// The test adds a 63K header leaving 1K for other headers.
		/// This is because the header buffer setting is for ALL headers,
		/// names and values included.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLongHeader()
		{
			java.net.URL url = new java.net.URL(baseUrl, "/longheader");
			javax.net.ssl.HttpsURLConnection conn = (javax.net.ssl.HttpsURLConnection)url.openConnection
				();
			conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
			testLongHeader(conn);
		}

		/// <exception cref="System.Exception"/>
		private static string readOut(java.net.URL url)
		{
			javax.net.ssl.HttpsURLConnection conn = (javax.net.ssl.HttpsURLConnection)url.openConnection
				();
			conn.setSSLSocketFactory(clientSslFactory.createSSLSocketFactory());
			java.io.InputStream @in = conn.getInputStream();
			java.io.ByteArrayOutputStream @out = new java.io.ByteArrayOutputStream();
			org.apache.hadoop.io.IOUtils.copyBytes(@in, @out, 1024);
			return @out.ToString();
		}
	}
}
