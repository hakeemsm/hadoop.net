using System;
using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Ssl;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestNameNodeHttpServer
	{
		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestNameNodeHttpServer).
			Name;

		private static string keystoresDir;

		private static string sslConfDir;

		private static Configuration conf;

		private static URLConnectionFactory connectionFactory;

		[Parameterized.Parameters]
		public static ICollection<object[]> Policy()
		{
			object[][] @params = new object[][] { new object[] { HttpConfig.Policy.HttpOnly }
				, new object[] { HttpConfig.Policy.HttpsOnly }, new object[] { HttpConfig.Policy
				.HttpAndHttps } };
			return Arrays.AsList(@params);
		}

		private readonly HttpConfig.Policy policy;

		public TestNameNodeHttpServer(HttpConfig.Policy policy)
			: base()
		{
			this.policy = policy;
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			FilePath @base = new FilePath(Basedir);
			FileUtil.FullyDelete(@base);
			@base.Mkdirs();
			conf = new Configuration();
			keystoresDir = new FilePath(Basedir).GetAbsolutePath();
			sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestNameNodeHttpServer
				));
			KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, conf, false);
			connectionFactory = URLConnectionFactory.NewDefaultURLConnectionFactory(conf);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			FileUtil.FullyDelete(new FilePath(Basedir));
			KeyStoreTestUtil.CleanupSSLConfig(keystoresDir, sslConfDir);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHttpPolicy()
		{
			conf.Set(DFSConfigKeys.DfsHttpPolicyKey, policy.ToString());
			conf.Set(DFSConfigKeys.DfsNamenodeHttpsAddressKey, "localhost:0");
			IPEndPoint addr = IPEndPoint.CreateUnresolved("localhost", 0);
			NameNodeHttpServer server = null;
			try
			{
				server = new NameNodeHttpServer(conf, null, addr);
				server.Start();
				NUnit.Framework.Assert.IsTrue(Implies(policy.IsHttpEnabled(), CanAccess("http", server
					.GetHttpAddress())));
				NUnit.Framework.Assert.IsTrue(Implies(!policy.IsHttpEnabled(), server.GetHttpAddress
					() == null));
				NUnit.Framework.Assert.IsTrue(Implies(policy.IsHttpsEnabled(), CanAccess("https", 
					server.GetHttpsAddress())));
				NUnit.Framework.Assert.IsTrue(Implies(!policy.IsHttpsEnabled(), server.GetHttpsAddress
					() == null));
			}
			finally
			{
				if (server != null)
				{
					server.Stop();
				}
			}
		}

		private static bool CanAccess(string scheme, IPEndPoint addr)
		{
			if (addr == null)
			{
				return false;
			}
			try
			{
				Uri url = new Uri(scheme + "://" + NetUtils.GetHostPortString(addr));
				URLConnection conn = connectionFactory.OpenConnection(url);
				conn.Connect();
				conn.GetContent();
			}
			catch (Exception)
			{
				return false;
			}
			return true;
		}

		private static bool Implies(bool a, bool b)
		{
			return !a || b;
		}
	}
}
