using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// This class tests the internals of PendingReplicationBlocks.java,
	/// as well as how PendingReplicationBlocks acts in BlockManager
	/// </summary>
	public class TestPendingReplication
	{
		internal const int Timeout = 3;

		private const int DfsReplicationInterval = 1;

		private const int DatanodeCount = 5;

		// 3 seconds
		// Number of datanodes in the cluster
		[NUnit.Framework.Test]
		public virtual void TestPendingReplication()
		{
			PendingReplicationBlocks pendingReplications;
			pendingReplications = new PendingReplicationBlocks(Timeout * 1000);
			pendingReplications.Start();
			//
			// Add 10 blocks to pendingReplications.
			//
			DatanodeStorageInfo[] storages = DFSTestUtil.CreateDatanodeStorageInfos(10);
			for (int i = 0; i < storages.Length; i++)
			{
				Block block = new Block(i, i, 0);
				DatanodeStorageInfo[] targets = new DatanodeStorageInfo[i];
				System.Array.Copy(storages, 0, targets, 0, i);
				pendingReplications.Increment(block, DatanodeStorageInfo.ToDatanodeDescriptors(targets
					));
			}
			NUnit.Framework.Assert.AreEqual("Size of pendingReplications ", 10, pendingReplications
				.Size());
			//
			// remove one item and reinsert it
			//
			Block blk = new Block(8, 8, 0);
			pendingReplications.Decrement(blk, storages[7].GetDatanodeDescriptor());
			// removes one replica
			NUnit.Framework.Assert.AreEqual("pendingReplications.getNumReplicas ", 7, pendingReplications
				.GetNumReplicas(blk));
			for (int i_1 = 0; i_1 < 7; i_1++)
			{
				// removes all replicas
				pendingReplications.Decrement(blk, storages[i_1].GetDatanodeDescriptor());
			}
			NUnit.Framework.Assert.IsTrue(pendingReplications.Size() == 9);
			pendingReplications.Increment(blk, DatanodeStorageInfo.ToDatanodeDescriptors(DFSTestUtil
				.CreateDatanodeStorageInfos(8)));
			NUnit.Framework.Assert.IsTrue(pendingReplications.Size() == 10);
			//
			// verify that the number of replicas returned
			// are sane.
			//
			for (int i_2 = 0; i_2 < 10; i_2++)
			{
				Block block = new Block(i_2, i_2, 0);
				int numReplicas = pendingReplications.GetNumReplicas(block);
				NUnit.Framework.Assert.IsTrue(numReplicas == i_2);
			}
			//
			// verify that nothing has timed out so far
			//
			NUnit.Framework.Assert.IsTrue(pendingReplications.GetTimedOutBlocks() == null);
			//
			// Wait for one second and then insert some more items.
			//
			try
			{
				Sharpen.Thread.Sleep(1000);
			}
			catch (Exception)
			{
			}
			for (int i_3 = 10; i_3 < 15; i_3++)
			{
				Block block = new Block(i_3, i_3, 0);
				pendingReplications.Increment(block, DatanodeStorageInfo.ToDatanodeDescriptors(DFSTestUtil
					.CreateDatanodeStorageInfos(i_3)));
			}
			NUnit.Framework.Assert.IsTrue(pendingReplications.Size() == 15);
			//
			// Wait for everything to timeout.
			//
			int loop = 0;
			while (pendingReplications.Size() > 0)
			{
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
				loop++;
			}
			System.Console.Out.WriteLine("Had to wait for " + loop + " seconds for the lot to timeout"
				);
			//
			// Verify that everything has timed out.
			//
			NUnit.Framework.Assert.AreEqual("Size of pendingReplications ", 0, pendingReplications
				.Size());
			Block[] timedOut = pendingReplications.GetTimedOutBlocks();
			NUnit.Framework.Assert.IsTrue(timedOut != null && timedOut.Length == 15);
			for (int i_4 = 0; i_4 < timedOut.Length; i_4++)
			{
				NUnit.Framework.Assert.IsTrue(timedOut[i_4].GetBlockId() < 15);
			}
			pendingReplications.Stop();
		}

		/* Test that processPendingReplications will use the most recent
		* blockinfo from the blocksmap by placing a larger genstamp into
		* the blocksmap.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProcessPendingReplications()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, Timeout);
			MiniDFSCluster cluster = null;
			Block block;
			BlockInfoContiguous blockInfo;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeCount).Build();
				cluster.WaitActive();
				FSNamesystem fsn = cluster.GetNamesystem();
				BlockManager blkManager = fsn.GetBlockManager();
				PendingReplicationBlocks pendingReplications = blkManager.pendingReplications;
				UnderReplicatedBlocks neededReplications = blkManager.neededReplications;
				BlocksMap blocksMap = blkManager.blocksMap;
				//
				// Add 1 block to pendingReplications with GenerationStamp = 0.
				//
				block = new Block(1, 1, 0);
				blockInfo = new BlockInfoContiguous(block, (short)3);
				pendingReplications.Increment(block, DatanodeStorageInfo.ToDatanodeDescriptors(DFSTestUtil
					.CreateDatanodeStorageInfos(1)));
				BlockCollection bc = Org.Mockito.Mockito.Mock<BlockCollection>();
				Org.Mockito.Mockito.DoReturn((short)3).When(bc).GetBlockReplication();
				// Place into blocksmap with GenerationStamp = 1
				blockInfo.SetGenerationStamp(1);
				blocksMap.AddBlockCollection(blockInfo, bc);
				NUnit.Framework.Assert.AreEqual("Size of pendingReplications ", 1, pendingReplications
					.Size());
				// Add a second block to pendingReplications that has no
				// corresponding entry in blocksmap
				block = new Block(2, 2, 0);
				pendingReplications.Increment(block, DatanodeStorageInfo.ToDatanodeDescriptors(DFSTestUtil
					.CreateDatanodeStorageInfos(1)));
				// verify 2 blocks in pendingReplications
				NUnit.Framework.Assert.AreEqual("Size of pendingReplications ", 2, pendingReplications
					.Size());
				//
				// Wait for everything to timeout.
				//
				while (pendingReplications.Size() > 0)
				{
					try
					{
						Sharpen.Thread.Sleep(100);
					}
					catch (Exception)
					{
					}
				}
				//
				// Verify that block moves to neededReplications
				//
				while (neededReplications.Size() == 0)
				{
					try
					{
						Sharpen.Thread.Sleep(100);
					}
					catch (Exception)
					{
					}
				}
				// Verify that the generation stamp we will try to replicate
				// is now 1
				foreach (Block b in neededReplications)
				{
					NUnit.Framework.Assert.AreEqual("Generation stamp is 1 ", 1, b.GetGenerationStamp
						());
				}
				// Verify size of neededReplications is exactly 1.
				NUnit.Framework.Assert.AreEqual("size of neededReplications is 1 ", 1, neededReplications
					.Size());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test if DatanodeProtocol#blockReceivedAndDeleted can correctly update the
		/// pending replications.
		/// </summary>
		/// <remarks>
		/// Test if DatanodeProtocol#blockReceivedAndDeleted can correctly update the
		/// pending replications. Also make sure the blockReceivedAndDeleted call is
		/// idempotent to the pending replications.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReceived()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 1024);
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeCount).Build();
				cluster.WaitActive();
				DistributedFileSystem hdfs = cluster.GetFileSystem();
				FSNamesystem fsn = cluster.GetNamesystem();
				BlockManager blkManager = fsn.GetBlockManager();
				string file = "/tmp.txt";
				Path filePath = new Path(file);
				short replFactor = 1;
				DFSTestUtil.CreateFile(hdfs, filePath, 1024L, replFactor, 0);
				// temporarily stop the heartbeat
				AList<DataNode> datanodes = cluster.GetDataNodes();
				for (int i = 0; i < DatanodeCount; i++)
				{
					DataNodeTestUtils.SetHeartbeatsDisabledForTests(datanodes[i], true);
				}
				hdfs.SetReplication(filePath, (short)DatanodeCount);
				BlockManagerTestUtil.ComputeAllPendingWork(blkManager);
				NUnit.Framework.Assert.AreEqual(1, blkManager.pendingReplications.Size());
				INodeFile fileNode = fsn.GetFSDirectory().GetINode4Write(file).AsFile();
				Block[] blocks = fileNode.GetBlocks();
				NUnit.Framework.Assert.AreEqual(DatanodeCount - 1, blkManager.pendingReplications
					.GetNumReplicas(blocks[0]));
				LocatedBlock locatedBlock = hdfs.GetClient().GetLocatedBlocks(file, 0).Get(0);
				DatanodeInfo existingDn = (locatedBlock.GetLocations())[0];
				int reportDnNum = 0;
				string poolId = cluster.GetNamesystem().GetBlockPoolId();
				// let two datanodes (other than the one that already has the data) to
				// report to NN
				for (int i_1 = 0; i_1 < DatanodeCount && reportDnNum < 2; i_1++)
				{
					if (!datanodes[i_1].GetDatanodeId().Equals(existingDn))
					{
						DatanodeRegistration dnR = datanodes[i_1].GetDNRegistrationForBP(poolId);
						StorageReceivedDeletedBlocks[] report = new StorageReceivedDeletedBlocks[] { new 
							StorageReceivedDeletedBlocks("Fake-storage-ID-Ignored", new ReceivedDeletedBlockInfo
							[] { new ReceivedDeletedBlockInfo(blocks[0], ReceivedDeletedBlockInfo.BlockStatus
							.ReceivedBlock, string.Empty) }) };
						cluster.GetNameNodeRpc().BlockReceivedAndDeleted(dnR, poolId, report);
						reportDnNum++;
					}
				}
				NUnit.Framework.Assert.AreEqual(DatanodeCount - 3, blkManager.pendingReplications
					.GetNumReplicas(blocks[0]));
				// let the same datanodes report again
				for (int i_2 = 0; i_2 < DatanodeCount && reportDnNum < 2; i_2++)
				{
					if (!datanodes[i_2].GetDatanodeId().Equals(existingDn))
					{
						DatanodeRegistration dnR = datanodes[i_2].GetDNRegistrationForBP(poolId);
						StorageReceivedDeletedBlocks[] report = new StorageReceivedDeletedBlocks[] { new 
							StorageReceivedDeletedBlocks("Fake-storage-ID-Ignored", new ReceivedDeletedBlockInfo
							[] { new ReceivedDeletedBlockInfo(blocks[0], ReceivedDeletedBlockInfo.BlockStatus
							.ReceivedBlock, string.Empty) }) };
						cluster.GetNameNodeRpc().BlockReceivedAndDeleted(dnR, poolId, report);
						reportDnNum++;
					}
				}
				NUnit.Framework.Assert.AreEqual(DatanodeCount - 3, blkManager.pendingReplications
					.GetNumReplicas(blocks[0]));
				// re-enable heartbeat for the datanode that has data
				for (int i_3 = 0; i_3 < DatanodeCount; i_3++)
				{
					DataNodeTestUtils.SetHeartbeatsDisabledForTests(datanodes[i_3], false);
					DataNodeTestUtils.TriggerHeartbeat(datanodes[i_3]);
				}
				Sharpen.Thread.Sleep(5000);
				NUnit.Framework.Assert.AreEqual(0, blkManager.pendingReplications.Size());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test if BlockManager can correctly remove corresponding pending records
		/// when a file is deleted
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPendingAndInvalidate()
		{
			Configuration Conf = new HdfsConfiguration();
			Conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 1024);
			Conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, DfsReplicationInterval);
			Conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, DfsReplicationInterval
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(DatanodeCount
				).Build();
			cluster.WaitActive();
			FSNamesystem namesystem = cluster.GetNamesystem();
			BlockManager bm = namesystem.GetBlockManager();
			DistributedFileSystem fs = cluster.GetFileSystem();
			try
			{
				// 1. create a file
				Path filePath = new Path("/tmp.txt");
				DFSTestUtil.CreateFile(fs, filePath, 1024, (short)3, 0L);
				// 2. disable the heartbeats
				foreach (DataNode dn in cluster.GetDataNodes())
				{
					DataNodeTestUtils.SetHeartbeatsDisabledForTests(dn, true);
				}
				// 3. mark a couple of blocks as corrupt
				LocatedBlock block = NameNodeAdapter.GetBlockLocations(cluster.GetNameNode(), filePath
					.ToString(), 0, 1).Get(0);
				cluster.GetNamesystem().WriteLock();
				try
				{
					bm.FindAndMarkBlockAsCorrupt(block.GetBlock(), block.GetLocations()[0], "STORAGE_ID"
						, "TEST");
					bm.FindAndMarkBlockAsCorrupt(block.GetBlock(), block.GetLocations()[1], "STORAGE_ID"
						, "TEST");
				}
				finally
				{
					cluster.GetNamesystem().WriteUnlock();
				}
				BlockManagerTestUtil.ComputeAllPendingWork(bm);
				BlockManagerTestUtil.UpdateState(bm);
				NUnit.Framework.Assert.AreEqual(bm.GetPendingReplicationBlocksCount(), 1L);
				NUnit.Framework.Assert.AreEqual(bm.pendingReplications.GetNumReplicas(block.GetBlock
					().GetLocalBlock()), 2);
				// 4. delete the file
				fs.Delete(filePath, true);
				// retry at most 10 times, each time sleep for 1s. Note that 10s is much
				// less than the default pending record timeout (5~10min)
				int retries = 10;
				long pendingNum = bm.GetPendingReplicationBlocksCount();
				while (pendingNum != 0 && retries-- > 0)
				{
					Sharpen.Thread.Sleep(1000);
					// let NN do the deletion
					BlockManagerTestUtil.UpdateState(bm);
					pendingNum = bm.GetPendingReplicationBlocksCount();
				}
				NUnit.Framework.Assert.AreEqual(pendingNum, 0L);
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
