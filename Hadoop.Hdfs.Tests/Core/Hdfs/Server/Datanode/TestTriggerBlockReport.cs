using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Test manually requesting that the DataNode send a block report.</summary>
	public sealed class TestTriggerBlockReport
	{
		/// <exception cref="System.Exception"/>
		private void TestTriggerBlockReport(bool incremental)
		{
			Configuration conf = new HdfsConfiguration();
			// Set a really long value for dfs.blockreport.intervalMsec and
			// dfs.heartbeat.interval, so that incremental block reports and heartbeats
			// won't be sent during this test unless they're triggered
			// manually.
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10800000L);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1080L);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			DatanodeProtocolClientSideTranslatorPB spy = DataNodeTestUtils.SpyOnBposToNN(cluster
				.GetDataNodes()[0], cluster.GetNameNode());
			DFSTestUtil.CreateFile(fs, new Path("/abc"), 16, (short)1, 1L);
			// We should get 1 incremental block report.
			Org.Mockito.Mockito.Verify(spy, Org.Mockito.Mockito.Timeout(60000).Times(1)).BlockReceivedAndDeleted
				(Matchers.Any<DatanodeRegistration>(), Matchers.AnyString(), Matchers.Any<StorageReceivedDeletedBlocks
				[]>());
			// We should not receive any more incremental or incremental block reports,
			// since the interval we configured is so long.
			for (int i = 0; i < 3; i++)
			{
				Sharpen.Thread.Sleep(10);
				Org.Mockito.Mockito.Verify(spy, Org.Mockito.Mockito.Times(0)).BlockReport(Matchers.Any
					<DatanodeRegistration>(), Matchers.AnyString(), Matchers.Any<StorageBlockReport[]
					>(), Org.Mockito.Mockito.AnyObject<BlockReportContext>());
				Org.Mockito.Mockito.Verify(spy, Org.Mockito.Mockito.Times(1)).BlockReceivedAndDeleted
					(Matchers.Any<DatanodeRegistration>(), Matchers.AnyString(), Matchers.Any<StorageReceivedDeletedBlocks
					[]>());
			}
			// Create a fake block deletion notification on the DataNode.
			// This will be sent with the next incremental block report.
			ReceivedDeletedBlockInfo rdbi = new ReceivedDeletedBlockInfo(new Block(5678, 512, 
				1000), ReceivedDeletedBlockInfo.BlockStatus.DeletedBlock, null);
			DataNode datanode = cluster.GetDataNodes()[0];
			BPServiceActor actor = datanode.GetAllBpOs()[0].GetBPServiceActors()[0];
			string storageUuid = datanode.GetFSDataset().GetVolumes()[0].GetStorageID();
			actor.NotifyNamenodeDeletedBlock(rdbi, storageUuid);
			// Manually trigger a block report.
			datanode.TriggerBlockReport(new BlockReportOptions.Factory().SetIncremental(incremental
				).Build());
			// triggerBlockReport returns before the block report is
			// actually sent.  Wait for it to be sent here.
			if (incremental)
			{
				Org.Mockito.Mockito.Verify(spy, Org.Mockito.Mockito.Timeout(60000).Times(2)).BlockReceivedAndDeleted
					(Matchers.Any<DatanodeRegistration>(), Matchers.AnyString(), Matchers.Any<StorageReceivedDeletedBlocks
					[]>());
			}
			else
			{
				Org.Mockito.Mockito.Verify(spy, Org.Mockito.Mockito.Timeout(60000)).BlockReport(Matchers.Any
					<DatanodeRegistration>(), Matchers.AnyString(), Matchers.Any<StorageBlockReport[]
					>(), Org.Mockito.Mockito.AnyObject<BlockReportContext>());
			}
			cluster.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public void TestTriggerFullBlockReport()
		{
			TestTriggerBlockReport(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public void TestTriggerIncrementalBlockReport()
		{
			TestTriggerBlockReport(true);
		}
	}
}
