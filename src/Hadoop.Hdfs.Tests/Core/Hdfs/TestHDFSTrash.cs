using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Test trash using HDFS</summary>
	public class TestHDFSTrash
	{
		private static MiniDFSCluster cluster = null;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			Configuration conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
		}

		[AfterClass]
		public static void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTrash()
		{
			Org.Apache.Hadoop.FS.TestTrash.TrashShell(cluster.GetFileSystem(), new Path("/"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNonDefaultFS()
		{
			FileSystem fs = cluster.GetFileSystem();
			Configuration conf = fs.GetConf();
			conf.Set(DFSConfigKeys.FsDefaultNameKey, fs.GetUri().ToString());
			Org.Apache.Hadoop.FS.TestTrash.TrashNonDefaultFS(conf);
		}
	}
}
