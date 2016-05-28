using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// This test verifies that incremental block reports from a single DataNode are
	/// correctly handled by NN.
	/// </summary>
	/// <remarks>
	/// This test verifies that incremental block reports from a single DataNode are
	/// correctly handled by NN. Tests the following variations:
	/// #1 - Incremental BRs from all storages combined in a single call.
	/// #2 - Incremental BRs from separate storages sent in separate calls.
	/// #3 - Incremental BR from an unknown storage should be rejected.
	/// We also verify that the DataNode is not splitting the reports (it may do so
	/// in the future).
	/// </remarks>
	public class TestIncrementalBrVariations
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestIncrementalBrVariations
			));

		private const short NumDatanodes = 1;

		internal const int BlockSize = 1024;

		internal const int NumBlocks = 10;

		private const long seed = unchecked((long)(0xFACEFEEDL));

		private const string NnMetrics = "NameNodeActivity";

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		private DFSClient client;

		private static Configuration conf;

		private string poolId;

		private DataNode dn0;

		private DatanodeRegistration dn0Reg;

		static TestIncrementalBrVariations()
		{
			// DataNode at index0 in the MiniDFSCluster
			// DataNodeRegistration for dn0
			GenericTestUtils.SetLogLevel(NameNode.stateChangeLog, Level.All);
			GenericTestUtils.SetLogLevel(BlockManager.blockLog, Level.All);
			GenericTestUtils.SetLogLevel(NameNode.blockStateChangeLog, Level.All);
			GenericTestUtils.SetLogLevel(LogFactory.GetLog(typeof(FSNamesystem)), Level.All);
			GenericTestUtils.SetLogLevel(DataNode.Log, Level.All);
			GenericTestUtils.SetLogLevel(TestIncrementalBrVariations.Log, Level.All);
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartUpCluster()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDatanodes).Build();
			fs = cluster.GetFileSystem();
			client = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort()), cluster
				.GetConfiguration(0));
			dn0 = cluster.GetDataNodes()[0];
			poolId = cluster.GetNamesystem().GetBlockPoolId();
			dn0Reg = dn0.GetDNRegistrationForBP(poolId);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutDownCluster()
		{
			client.Close();
			fs.Close();
			cluster.ShutdownDataNodes();
			cluster.Shutdown();
		}

		/// <summary>Incremental BRs from all storages combined in a single message.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCombinedIncrementalBlockReport()
		{
			VerifyIncrementalBlockReports(false);
		}

		/// <summary>One incremental BR per storage.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSplitIncrementalBlockReport()
		{
			VerifyIncrementalBlockReports(true);
		}

		/// <exception cref="System.IO.IOException"/>
		private LocatedBlocks CreateFileGetBlocks(string filenamePrefix)
		{
			Path filePath = new Path("/" + filenamePrefix + ".dat");
			// Write out a file with a few blocks, get block locations.
			DFSTestUtil.CreateFile(fs, filePath, BlockSize, BlockSize * NumBlocks, BlockSize, 
				NumDatanodes, seed);
			// Get the block list for the file with the block locations.
			LocatedBlocks blocks = client.GetLocatedBlocks(filePath.ToString(), 0, BlockSize 
				* NumBlocks);
			Assert.AssertThat(cluster.GetNamesystem().GetUnderReplicatedBlocks(), IS.Is(0L));
			return blocks;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void VerifyIncrementalBlockReports(bool splitReports)
		{
			// Get the block list for the file with the block locations.
			LocatedBlocks blocks = CreateFileGetBlocks(GenericTestUtils.GetMethodName());
			// We will send 'fake' incremental block reports to the NN that look
			// like they originated from DN 0.
			StorageReceivedDeletedBlocks[] reports = new StorageReceivedDeletedBlocks[dn0.GetFSDataset
				().GetVolumes().Count];
			// Lie to the NN that one block on each storage has been deleted.
			for (int i = 0; i < reports.Length; ++i)
			{
				FsVolumeSpi volume = dn0.GetFSDataset().GetVolumes()[i];
				bool foundBlockOnStorage = false;
				ReceivedDeletedBlockInfo[] rdbi = new ReceivedDeletedBlockInfo[1];
				// Find the first block on this storage and mark it as deleted for the
				// report.
				foreach (LocatedBlock block in blocks.GetLocatedBlocks())
				{
					if (block.GetStorageIDs()[0].Equals(volume.GetStorageID()))
					{
						rdbi[0] = new ReceivedDeletedBlockInfo(block.GetBlock().GetLocalBlock(), ReceivedDeletedBlockInfo.BlockStatus
							.DeletedBlock, null);
						foundBlockOnStorage = true;
						break;
					}
				}
				NUnit.Framework.Assert.IsTrue(foundBlockOnStorage);
				reports[i] = new StorageReceivedDeletedBlocks(volume.GetStorageID(), rdbi);
				if (splitReports)
				{
					// If we are splitting reports then send the report for this storage now.
					StorageReceivedDeletedBlocks[] singletonReport = new StorageReceivedDeletedBlocks
						[] { reports[i] };
					cluster.GetNameNodeRpc().BlockReceivedAndDeleted(dn0Reg, poolId, singletonReport);
				}
			}
			if (!splitReports)
			{
				// Send a combined report.
				cluster.GetNameNodeRpc().BlockReceivedAndDeleted(dn0Reg, poolId, reports);
			}
			// Make sure that the deleted block from each storage was picked up
			// by the NameNode.
			Assert.AssertThat(cluster.GetNamesystem().GetMissingBlocksCount(), IS.Is((long)reports
				.Length));
		}

		/// <summary>
		/// Verify that the DataNode sends a single incremental block report for all
		/// storages.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestDataNodeDoesNotSplitReports()
		{
			LocatedBlocks blocks = CreateFileGetBlocks(GenericTestUtils.GetMethodName());
			Assert.AssertThat(cluster.GetDataNodes().Count, IS.Is(1));
			// Remove all blocks from the DataNode.
			foreach (LocatedBlock block in blocks.GetLocatedBlocks())
			{
				dn0.NotifyNamenodeDeletedBlock(block.GetBlock(), block.GetStorageIDs()[0]);
			}
			Log.Info("Triggering report after deleting blocks");
			long ops = MetricsAsserts.GetLongCounter("BlockReceivedAndDeletedOps", MetricsAsserts.GetMetrics
				(NnMetrics));
			// Trigger a report to the NameNode and give it a few seconds.
			DataNodeTestUtils.TriggerBlockReport(dn0);
			Sharpen.Thread.Sleep(5000);
			// Ensure that NameNodeRpcServer.blockReceivedAndDeletes is invoked
			// exactly once after we triggered the report.
			MetricsAsserts.AssertCounter("BlockReceivedAndDeletedOps", ops + 1, MetricsAsserts.GetMetrics
				(NnMetrics));
		}

		private static Block GetDummyBlock()
		{
			return new Block(10000000L, 100L, 1048576L);
		}

		private static StorageReceivedDeletedBlocks[] MakeReportForReceivedBlock(Block block
			, DatanodeStorage storage)
		{
			ReceivedDeletedBlockInfo[] receivedBlocks = new ReceivedDeletedBlockInfo[1];
			receivedBlocks[0] = new ReceivedDeletedBlockInfo(block, ReceivedDeletedBlockInfo.BlockStatus
				.ReceivedBlock, null);
			StorageReceivedDeletedBlocks[] reports = new StorageReceivedDeletedBlocks[1];
			reports[0] = new StorageReceivedDeletedBlocks(storage, receivedBlocks);
			return reports;
		}

		/// <summary>
		/// Verify that the NameNode can learn about new storages from incremental
		/// block reports.
		/// </summary>
		/// <remarks>
		/// Verify that the NameNode can learn about new storages from incremental
		/// block reports.
		/// This tests the fix for the error condition seen in HDFS-6904.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestNnLearnsNewStorages()
		{
			// Generate a report for a fake block on a fake storage.
			string newStorageUuid = UUID.RandomUUID().ToString();
			DatanodeStorage newStorage = new DatanodeStorage(newStorageUuid);
			StorageReceivedDeletedBlocks[] reports = MakeReportForReceivedBlock(GetDummyBlock
				(), newStorage);
			// Send the report to the NN.
			cluster.GetNameNodeRpc().BlockReceivedAndDeleted(dn0Reg, poolId, reports);
			// Make sure that the NN has learned of the new storage.
			DatanodeStorageInfo storageInfo = cluster.GetNameNode().GetNamesystem().GetBlockManager
				().GetDatanodeManager().GetDatanode(dn0.GetDatanodeId()).GetStorageInfo(newStorageUuid
				);
			NUnit.Framework.Assert.IsNotNull(storageInfo);
		}
	}
}
