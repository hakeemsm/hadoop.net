using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Ipc.Metrics;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Tests for ensuring the namenode retry cache metrics works correctly for
	/// non-idempotent requests.
	/// </summary>
	/// <remarks>
	/// Tests for ensuring the namenode retry cache metrics works correctly for
	/// non-idempotent requests.
	/// Retry cache works based on tracking previously received request based on the
	/// ClientId and CallId received in RPC requests and storing the response. The
	/// response is replayed on retry when the same request is received again.
	/// </remarks>
	public class TestNameNodeRetryCacheMetrics
	{
		private MiniDFSCluster cluster;

		private FSNamesystem namesystem;

		private DistributedFileSystem filesystem;

		private readonly int namenodeId = 0;

		private Configuration conf;

		private RetryCacheMetrics metrics;

		private DFSClient client;

		/// <summary>Start a cluster</summary>
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeEnableRetryCacheKey, true);
			conf.SetInt(DFSConfigKeys.DfsClientTestDropNamenodeResponseNumKey, 2);
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(3).Build();
			cluster.WaitActive();
			cluster.TransitionToActive(namenodeId);
			HATestUtil.SetFailoverConfigurations(cluster, conf);
			filesystem = (DistributedFileSystem)HATestUtil.ConfigureFailoverFs(cluster, conf);
			namesystem = cluster.GetNamesystem(namenodeId);
			metrics = namesystem.GetRetryCache().GetMetricsForTests();
		}

		/// <summary>Cleanup after the test</summary>
		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Cleanup()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRetryCacheMetrics()
		{
			CheckMetrics(0, 0, 0);
			// DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY is 2 ,
			// so 2 requests are dropped at first.
			// After that, 1 request will reach NameNode correctly.
			TrySaveNamespace();
			CheckMetrics(2, 0, 1);
			// RetryCache will be cleared after Namesystem#close()
			namesystem.Close();
			CheckMetrics(2, 1, 1);
		}

		private void CheckMetrics(long hit, long cleared, long updated)
		{
			NUnit.Framework.Assert.AreEqual("CacheHit", hit, metrics.GetCacheHit());
			NUnit.Framework.Assert.AreEqual("CacheCleared", cleared, metrics.GetCacheCleared(
				));
			NUnit.Framework.Assert.AreEqual("CacheUpdated", updated, metrics.GetCacheUpdated(
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private void TrySaveNamespace()
		{
			filesystem.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			filesystem.SaveNamespace();
			filesystem.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
		}
	}
}
