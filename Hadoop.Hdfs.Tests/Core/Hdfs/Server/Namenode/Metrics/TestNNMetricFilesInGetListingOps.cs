using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics
{
	/// <summary>Test case for FilesInGetListingOps metric in Namenode</summary>
	public class TestNNMetricFilesInGetListingOps
	{
		private static readonly Configuration Conf = new HdfsConfiguration();

		private const string NnMetrics = "NameNodeActivity";

		static TestNNMetricFilesInGetListingOps()
		{
			Conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 100);
			Conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 1);
			Conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
			Conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1);
		}

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		private readonly Random rand = new Random();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			cluster = new MiniDFSCluster.Builder(Conf).Build();
			cluster.WaitActive();
			cluster.GetNameNode();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			cluster.Shutdown();
		}

		/// <summary>create a file with a length of <code>fileLen</code></summary>
		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(string fileName, long fileLen, short replicas)
		{
			Path filePath = new Path(fileName);
			DFSTestUtil.CreateFile(fs, filePath, fileLen, replicas, rand.NextLong());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFilesInGetListingOps()
		{
			CreateFile("/tmp1/t1", 3200, (short)3);
			CreateFile("/tmp1/t2", 3200, (short)3);
			CreateFile("/tmp2/t1", 3200, (short)3);
			CreateFile("/tmp2/t2", 3200, (short)3);
			cluster.GetNameNodeRpc().GetListing("/tmp1", HdfsFileStatus.EmptyName, false);
			MetricsAsserts.AssertCounter("FilesInGetListingOps", 2L, MetricsAsserts.GetMetrics
				(NnMetrics));
			cluster.GetNameNodeRpc().GetListing("/tmp2", HdfsFileStatus.EmptyName, false);
			MetricsAsserts.AssertCounter("FilesInGetListingOps", 4L, MetricsAsserts.GetMetrics
				(NnMetrics));
		}
	}
}
