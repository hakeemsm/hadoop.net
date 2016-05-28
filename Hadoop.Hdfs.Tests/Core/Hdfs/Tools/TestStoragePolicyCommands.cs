using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>Test StoragePolicyAdmin commands</summary>
	public class TestStoragePolicyCommands
	{
		private const short Repl = 1;

		private const int Size = 128;

		private static Configuration conf;

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem fs;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void ClusterSetUp()
		{
			conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Repl).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ClusterShutdown()
		{
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetAndGetStoragePolicy()
		{
			Path foo = new Path("/foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(fs, bar, Size, Repl, 0);
			StoragePolicyAdmin admin = new StoragePolicyAdmin(conf);
			DFSTestUtil.ToolRun(admin, "-getStoragePolicy -path /foo", 0, "The storage policy of "
				 + foo.ToString() + " is unspecified");
			DFSTestUtil.ToolRun(admin, "-getStoragePolicy -path /foo/bar", 0, "The storage policy of "
				 + bar.ToString() + " is unspecified");
			DFSTestUtil.ToolRun(admin, "-setStoragePolicy -path /foo -policy WARM", 0, "Set storage policy WARM on "
				 + foo.ToString());
			DFSTestUtil.ToolRun(admin, "-setStoragePolicy -path /foo/bar -policy COLD", 0, "Set storage policy COLD on "
				 + bar.ToString());
			DFSTestUtil.ToolRun(admin, "-setStoragePolicy -path /fooz -policy WARM", 2, "File/Directory does not exist: /fooz"
				);
			BlockStoragePolicySuite suite = BlockStoragePolicySuite.CreateDefaultSuite();
			BlockStoragePolicy warm = suite.GetPolicy("WARM");
			BlockStoragePolicy cold = suite.GetPolicy("COLD");
			DFSTestUtil.ToolRun(admin, "-getStoragePolicy -path /foo", 0, "The storage policy of "
				 + foo.ToString() + ":\n" + warm);
			DFSTestUtil.ToolRun(admin, "-getStoragePolicy -path /foo/bar", 0, "The storage policy of "
				 + bar.ToString() + ":\n" + cold);
			DFSTestUtil.ToolRun(admin, "-getStoragePolicy -path /fooz", 2, "File/Directory does not exist: /fooz"
				);
		}
	}
}
