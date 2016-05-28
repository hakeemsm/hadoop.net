using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestQuotasWithHA
	{
		private static readonly Path TestDir = new Path("/test");

		private static readonly Path TestFile = new Path(TestDir, "file");

		private static readonly string TestDirStr = TestDir.ToUri().GetPath();

		private const long NsQuota = 10000;

		private const long DsQuota = 10000;

		private const long BlockSize = 1024;

		private MiniDFSCluster cluster;

		private NameNode nn0;

		private NameNode nn1;

		private FileSystem fs;

		// 1KB blocks
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupCluster()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			HAUtil.SetAllowStandbyReads(conf, true);
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(1).WaitSafeMode(false).Build();
			cluster.WaitActive();
			nn0 = cluster.GetNameNode(0);
			nn1 = cluster.GetNameNode(1);
			fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			cluster.TransitionToActive(0);
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

		/// <summary>
		/// Test that quotas are properly tracked by the standby through
		/// create, append, delete.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestQuotasTrackedOnStandby()
		{
			fs.Mkdirs(TestDir);
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			dfs.SetQuota(TestDir, NsQuota, DsQuota);
			long expectedSize = 3 * BlockSize + BlockSize / 2;
			DFSTestUtil.CreateFile(fs, TestFile, expectedSize, (short)1, 1L);
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			ContentSummary cs = nn1.GetRpcServer().GetContentSummary(TestDirStr);
			NUnit.Framework.Assert.AreEqual(NsQuota, cs.GetQuota());
			NUnit.Framework.Assert.AreEqual(DsQuota, cs.GetSpaceQuota());
			NUnit.Framework.Assert.AreEqual(expectedSize, cs.GetSpaceConsumed());
			NUnit.Framework.Assert.AreEqual(1, cs.GetDirectoryCount());
			NUnit.Framework.Assert.AreEqual(1, cs.GetFileCount());
			// Append to the file and make sure quota is updated correctly.
			FSDataOutputStream stm = fs.Append(TestFile);
			try
			{
				byte[] data = new byte[(int)(BlockSize * 3 / 2)];
				stm.Write(data);
				expectedSize += data.Length;
			}
			finally
			{
				IOUtils.CloseStream(stm);
			}
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			cs = nn1.GetRpcServer().GetContentSummary(TestDirStr);
			NUnit.Framework.Assert.AreEqual(NsQuota, cs.GetQuota());
			NUnit.Framework.Assert.AreEqual(DsQuota, cs.GetSpaceQuota());
			NUnit.Framework.Assert.AreEqual(expectedSize, cs.GetSpaceConsumed());
			NUnit.Framework.Assert.AreEqual(1, cs.GetDirectoryCount());
			NUnit.Framework.Assert.AreEqual(1, cs.GetFileCount());
			fs.Delete(TestFile, true);
			expectedSize = 0;
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			cs = nn1.GetRpcServer().GetContentSummary(TestDirStr);
			NUnit.Framework.Assert.AreEqual(NsQuota, cs.GetQuota());
			NUnit.Framework.Assert.AreEqual(DsQuota, cs.GetSpaceQuota());
			NUnit.Framework.Assert.AreEqual(expectedSize, cs.GetSpaceConsumed());
			NUnit.Framework.Assert.AreEqual(1, cs.GetDirectoryCount());
			NUnit.Framework.Assert.AreEqual(0, cs.GetFileCount());
		}

		/// <summary>
		/// Test that getContentSummary on Standby should should throw standby
		/// exception.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestgetContentSummaryOnStandby()
		{
			Configuration nn1conf = cluster.GetConfiguration(1);
			// just reset the standby reads to default i.e False on standby.
			HAUtil.SetAllowStandbyReads(nn1conf, false);
			cluster.RestartNameNode(1);
			cluster.GetNameNodeRpc(1).GetContentSummary("/");
		}
	}
}
