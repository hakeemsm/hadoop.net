using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// Makes sure that standby doesn't do the unnecessary block management such as
	/// invalidate block, etc.
	/// </summary>
	public class TestStandbyBlockManagement
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(TestStandbyBlockManagement
			));

		private const string TestFileData = "hello world";

		private const string TestFile = "/TestStandbyBlockManagement";

		private static readonly Path TestFilePath = new Path(TestFile);

		static TestStandbyBlockManagement()
		{
			DFSTestUtil.SetNameNodeLogLevel(Level.All);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidateBlock()
		{
			Configuration conf = new Configuration();
			HAUtil.SetAllowStandbyReads(conf, true);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(3).Build();
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				NameNode nn1 = cluster.GetNameNode(0);
				NameNode nn2 = cluster.GetNameNode(1);
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				Sharpen.Thread.Sleep(1000);
				Log.Info("==================================");
				DFSTestUtil.WriteFile(fs, TestFilePath, TestFileData);
				// Have to force an edit log roll so that the standby catches up
				nn1.GetRpcServer().RollEditLog();
				Log.Info("==================================");
				// delete the file
				fs.Delete(TestFilePath, false);
				BlockManagerTestUtil.ComputeAllPendingWork(nn1.GetNamesystem().GetBlockManager());
				nn1.GetRpcServer().RollEditLog();
				// standby nn doesn't need to invalidate blocks.
				NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetBlockManager().GetPendingDeletionBlocksCount
					());
				cluster.TriggerHeartbeats();
				cluster.TriggerBlockReports();
				// standby nn doesn't need to invalidate blocks.
				NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetBlockManager().GetPendingDeletionBlocksCount
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
