using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestEditLogAutoroll
	{
		private Configuration conf;

		private MiniDFSCluster cluster;

		private NameNode nn0;

		private FileSystem fs;

		private FSEditLog editLog;

		private readonly Random random = new Random();

		private static readonly Log Log = LogFactory.GetLog(typeof(TestEditLog));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			// Stall the standby checkpointer in two ways
			conf.SetLong(DFSConfigKeys.DfsNamenodeCheckpointPeriodKey, long.MaxValue);
			conf.SetLong(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey, 20);
			// Make it autoroll after 10 edits
			conf.SetFloat(DFSConfigKeys.DfsNamenodeEditLogAutorollMultiplierThreshold, 0.5f);
			conf.SetInt(DFSConfigKeys.DfsNamenodeEditLogAutorollCheckIntervalMs, 100);
			int retryCount = 0;
			while (true)
			{
				try
				{
					int basePort = 10060 + random.Next(100) * 2;
					MiniDFSNNTopology topology = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
						("ns1").AddNN(new MiniDFSNNTopology.NNConf("nn1").SetHttpPort(basePort)).AddNN(new 
						MiniDFSNNTopology.NNConf("nn2").SetHttpPort(basePort + 1)));
					cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes(0).Build
						();
					cluster.WaitActive();
					nn0 = cluster.GetNameNode(0);
					fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
					cluster.TransitionToActive(0);
					fs = cluster.GetFileSystem(0);
					editLog = nn0.GetNamesystem().GetEditLog();
					++retryCount;
					break;
				}
				catch (BindException)
				{
					Log.Info("Set up MiniDFSCluster failed due to port conflicts, retry " + retryCount
						 + " times");
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
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
		public virtual void TestEditLogAutoroll()
		{
			// Make some edits
			long startTxId = editLog.GetCurSegmentTxId();
			for (int i = 0; i < 11; i++)
			{
				fs.Mkdirs(new Path("testEditLogAutoroll-" + i));
			}
			// Wait for the NN to autoroll
			GenericTestUtils.WaitFor(new _Supplier_114(this, startTxId), 1000, 5000);
			// Transition to standby and make sure the roller stopped
			nn0.TransitionToStandby();
			GenericTestUtils.AssertNoThreadsMatching(".*" + typeof(FSNamesystem.NameNodeEditLogRoller
				).Name + ".*");
		}

		private sealed class _Supplier_114 : Supplier<bool>
		{
			public _Supplier_114(TestEditLogAutoroll _enclosing, long startTxId)
			{
				this._enclosing = _enclosing;
				this.startTxId = startTxId;
			}

			public bool Get()
			{
				return this._enclosing.editLog.GetCurSegmentTxId() > startTxId;
			}

			private readonly TestEditLogAutoroll _enclosing;

			private readonly long startTxId;
		}
	}
}
