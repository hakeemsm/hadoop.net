using System;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security.Ssl;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	public class TestNfs3HttpServer
	{
		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(TestNfs3HttpServer).Name;

		private static NfsConfiguration conf = new NfsConfiguration();

		private static MiniDFSCluster cluster;

		private static string keystoresDir;

		private static string sslConfDir;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			conf.Set(DFSConfigKeys.DfsHttpPolicyKey, HttpConfig.Policy.HttpAndHttps.ToString(
				));
			conf.Set(NfsConfigKeys.NfsHttpAddressKey, "localhost:0");
			conf.Set(NfsConfigKeys.NfsHttpsAddressKey, "localhost:0");
			// Use emphral port in case tests are running in parallel
			conf.SetInt(NfsConfigKeys.DfsNfsServerPortKey, 0);
			conf.SetInt(NfsConfigKeys.DfsNfsMountdPortKey, 0);
			FilePath @base = new FilePath(Basedir);
			FileUtil.FullyDelete(@base);
			@base.Mkdirs();
			keystoresDir = new FilePath(Basedir).GetAbsolutePath();
			sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestNfs3HttpServer));
			KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, conf, false);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			FileUtil.FullyDelete(new FilePath(Basedir));
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			KeyStoreTestUtil.CleanupSSLConfig(keystoresDir, sslConfDir);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHttpServer()
		{
			Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3 nfs = new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3
				(conf);
			nfs.StartServiceInternal(false);
			RpcProgramNfs3 nfsd = (RpcProgramNfs3)nfs.GetRpcProgram();
			Nfs3HttpServer infoServer = nfsd.GetInfoServer();
			string urlRoot = infoServer.GetServerURI().ToString();
			// Check default servlets.
			string pageContents = DFSTestUtil.UrlGet(new Uri(urlRoot + "/jmx"));
			NUnit.Framework.Assert.IsTrue("Bad contents: " + pageContents, pageContents.Contains
				("java.lang:type="));
			System.Console.Out.WriteLine("pc:" + pageContents);
			int port = infoServer.GetSecurePort();
			NUnit.Framework.Assert.IsTrue("Can't get https port", port > 0);
		}
	}
}
