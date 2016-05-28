using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security.Ssl;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestHttpsFileSystem
	{
		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(TestHttpsFileSystem).Name;

		private static MiniDFSCluster cluster;

		private static Configuration conf;

		private static string keystoresDir;

		private static string sslConfDir;

		private static string nnAddr;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			conf.Set(DFSConfigKeys.DfsHttpPolicyKey, HttpConfig.Policy.HttpsOnly.ToString());
			conf.Set(DFSConfigKeys.DfsNamenodeHttpsAddressKey, "localhost:0");
			conf.Set(DFSConfigKeys.DfsDatanodeHttpsAddressKey, "localhost:0");
			FilePath @base = new FilePath(Basedir);
			FileUtil.FullyDelete(@base);
			@base.Mkdirs();
			keystoresDir = new FilePath(Basedir).GetAbsolutePath();
			sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestHttpsFileSystem));
			KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, conf, false);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			OutputStream os = cluster.GetFileSystem().Create(new Path("/test"));
			os.Write(23);
			os.Close();
			IPEndPoint addr = cluster.GetNameNode().GetHttpsAddress();
			nnAddr = NetUtils.GetHostPortString(addr);
			conf.Set(DFSConfigKeys.DfsNamenodeHttpsAddressKey, nnAddr);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			cluster.Shutdown();
			FileUtil.FullyDelete(new FilePath(Basedir));
			KeyStoreTestUtil.CleanupSSLConfig(keystoresDir, sslConfDir);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHsftpFileSystem()
		{
			FileSystem fs = FileSystem.Get(new URI("hsftp://" + nnAddr), conf);
			NUnit.Framework.Assert.IsTrue(fs.Exists(new Path("/test")));
			InputStream @is = fs.Open(new Path("/test"));
			NUnit.Framework.Assert.AreEqual(23, @is.Read());
			@is.Close();
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSWebHdfsFileSystem()
		{
			FileSystem fs = WebHdfsTestUtil.GetWebHdfsFileSystem(conf, "swebhdfs");
			Path f = new Path("/testswebhdfs");
			FSDataOutputStream os = fs.Create(f);
			os.Write(23);
			os.Close();
			NUnit.Framework.Assert.IsTrue(fs.Exists(f));
			InputStream @is = fs.Open(f);
			NUnit.Framework.Assert.AreEqual(23, @is.Read());
			@is.Close();
			fs.Close();
		}
	}
}
