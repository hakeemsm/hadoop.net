using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Tests interaction of encryption zones with HA failover.</summary>
	public class TestEncryptionZonesWithHA
	{
		private Configuration conf;

		private MiniDFSCluster cluster;

		private NameNode nn0;

		private NameNode nn1;

		private DistributedFileSystem fs;

		private HdfsAdmin dfsAdmin0;

		private HdfsAdmin dfsAdmin1;

		private FileSystemTestHelper fsHelper;

		private FilePath testRootDir;

		private readonly string TestKey = "test_key";

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupCluster()
		{
			conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			HAUtil.SetAllowStandbyReads(conf, true);
			fsHelper = new FileSystemTestHelper();
			string testRoot = fsHelper.GetTestRootDir();
			testRootDir = new FilePath(testRoot).GetAbsoluteFile();
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, JavaKeyStoreProvider.SchemeName
				 + "://file" + new Path(testRootDir.ToString(), "test.jks").ToUri());
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(1).Build();
			cluster.WaitActive();
			cluster.TransitionToActive(0);
			fs = (DistributedFileSystem)HATestUtil.ConfigureFailoverFs(cluster, conf);
			DFSTestUtil.CreateKey(TestKey, cluster, 0, conf);
			DFSTestUtil.CreateKey(TestKey, cluster, 1, conf);
			nn0 = cluster.GetNameNode(0);
			nn1 = cluster.GetNameNode(1);
			dfsAdmin0 = new HdfsAdmin(cluster.GetURI(0), conf);
			dfsAdmin1 = new HdfsAdmin(cluster.GetURI(1), conf);
			KeyProviderCryptoExtension nn0Provider = cluster.GetNameNode(0).GetNamesystem().GetProvider
				();
			fs.GetClient().SetKeyProvider(nn0Provider);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test that encryption zones are properly tracked by the standby.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestEncryptionZonesTrackedOnStandby()
		{
			int len = 8196;
			Path dir = new Path("/enc");
			Path dirChild = new Path(dir, "child");
			Path dirFile = new Path(dir, "file");
			fs.Mkdir(dir, FsPermission.GetDirDefault());
			dfsAdmin0.CreateEncryptionZone(dir, TestKey);
			fs.Mkdir(dirChild, FsPermission.GetDirDefault());
			DFSTestUtil.CreateFile(fs, dirFile, len, (short)1, unchecked((int)(0xFEED)));
			string contents = DFSTestUtil.ReadFile(fs, dirFile);
			// Failover the current standby to active.
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			cluster.ShutdownNameNode(0);
			cluster.TransitionToActive(1);
			NUnit.Framework.Assert.AreEqual("Got unexpected ez path", dir.ToString(), dfsAdmin1
				.GetEncryptionZoneForPath(dir).GetPath().ToString());
			NUnit.Framework.Assert.AreEqual("Got unexpected ez path", dir.ToString(), dfsAdmin1
				.GetEncryptionZoneForPath(dirChild).GetPath().ToString());
			NUnit.Framework.Assert.AreEqual("File contents after failover were changed", contents
				, DFSTestUtil.ReadFile(fs, dirFile));
		}
	}
}
