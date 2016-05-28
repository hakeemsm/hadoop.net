using System;
using System.Net;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Test proper
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockManager"/>
	/// replication counting for
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeStorage"/>
	/// s
	/// with
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeStorage.State.ReadOnlyShared
	/// 	">READ_ONLY</see>
	/// state.
	/// Uses
	/// <see cref="SimulatedFSDataset"/>
	/// to inject read-only replicas into a DataNode.
	/// </summary>
	public class TestReadOnlySharedStorage
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestReadOnlySharedStorage
			));

		private const short NumDatanodes = 3;

		private const int RoNodeIndex = 0;

		private const int BlockSize = 1024;

		private const long seed = unchecked((long)(0x1BADF00DL));

		private static readonly Path Path = new Path("/" + typeof(TestReadOnlySharedStorage
			).FullName + ".dat");

		private const int Retries = 10;

		private Configuration conf;

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		private DFSClient client;

		private BlockManager blockManager;

		private DatanodeManager datanodeManager;

		private DatanodeInfo normalDataNode;

		private DatanodeInfo readOnlyDataNode;

		private Block block;

		private ExtendedBlock extendedBlock;

		/// <summary>
		/// Setup a
		/// <see cref="Org.Apache.Hadoop.Hdfs.MiniDFSCluster"/>
		/// .
		/// Create a block with both
		/// <see cref="State#NORMAL"/>
		/// and
		/// <see cref="State#READ_ONLY_SHARED"/>
		/// replicas.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new HdfsConfiguration();
			SimulatedFSDataset.SetFactory(conf);
			Configuration[] overlays = new Configuration[NumDatanodes];
			for (int i = 0; i < overlays.Length; i++)
			{
				overlays[i] = new Configuration();
				if (i == RoNodeIndex)
				{
					overlays[i].SetEnum(SimulatedFSDataset.ConfigPropertyState, i == RoNodeIndex ? DatanodeStorage.State
						.ReadOnlyShared : DatanodeStorage.State.Normal);
				}
			}
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDatanodes).DataNodeConfOverlays
				(overlays).Build();
			fs = cluster.GetFileSystem();
			blockManager = cluster.GetNameNode().GetNamesystem().GetBlockManager();
			datanodeManager = blockManager.GetDatanodeManager();
			client = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort()), cluster
				.GetConfiguration(0));
			for (int i_1 = 0; i_1 < NumDatanodes; i_1++)
			{
				DataNode dataNode = cluster.GetDataNodes()[i_1];
				ValidateStorageState(BlockManagerTestUtil.GetStorageReportsForDatanode(datanodeManager
					.GetDatanode(dataNode.GetDatanodeId())), i_1 == RoNodeIndex ? DatanodeStorage.State
					.ReadOnlyShared : DatanodeStorage.State.Normal);
			}
			// Create a 1 block file
			DFSTestUtil.CreateFile(fs, Path, BlockSize, BlockSize, BlockSize, (short)1, seed);
			LocatedBlock locatedBlock = GetLocatedBlock();
			extendedBlock = locatedBlock.GetBlock();
			block = extendedBlock.GetLocalBlock();
			Assert.AssertThat(locatedBlock.GetLocations().Length, CoreMatchers.Is(1));
			normalDataNode = locatedBlock.GetLocations()[0];
			readOnlyDataNode = datanodeManager.GetDatanode(cluster.GetDataNodes()[RoNodeIndex
				].GetDatanodeId());
			Assert.AssertThat(normalDataNode, CoreMatchers.Is(CoreMatchers.Not(readOnlyDataNode
				)));
			ValidateNumberReplicas(1);
			// Inject the block into the datanode with READ_ONLY_SHARED storage 
			cluster.InjectBlocks(0, RoNodeIndex, Collections.Singleton(block));
			// There should now be 2 *locations* for the block
			// Must wait until the NameNode has processed the block report for the injected blocks
			WaitForLocations(2);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fs.Delete(Path, false);
			if (cluster != null)
			{
				fs.Close();
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void WaitForLocations(int locations)
		{
			for (int tries = 0; tries < Retries; )
			{
				try
				{
					LocatedBlock locatedBlock = GetLocatedBlock();
					Assert.AssertThat(locatedBlock.GetLocations().Length, CoreMatchers.Is(locations));
					break;
				}
				catch (Exception e)
				{
					if (++tries < Retries)
					{
						Sharpen.Thread.Sleep(1000);
					}
					else
					{
						throw;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private LocatedBlock GetLocatedBlock()
		{
			LocatedBlocks locatedBlocks = client.GetLocatedBlocks(Path.ToString(), 0, BlockSize
				);
			Assert.AssertThat(locatedBlocks.GetLocatedBlocks().Count, CoreMatchers.Is(1));
			return Iterables.GetOnlyElement(locatedBlocks.GetLocatedBlocks());
		}

		private void ValidateStorageState(StorageReport[] storageReports, DatanodeStorage.State
			 state)
		{
			foreach (StorageReport storageReport in storageReports)
			{
				DatanodeStorage storage = storageReport.GetStorage();
				Assert.AssertThat(storage.GetState(), CoreMatchers.Is(state));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ValidateNumberReplicas(int expectedReplicas)
		{
			NumberReplicas numberReplicas = blockManager.CountNodes(block);
			Assert.AssertThat(numberReplicas.LiveReplicas(), CoreMatchers.Is(expectedReplicas
				));
			Assert.AssertThat(numberReplicas.ExcessReplicas(), CoreMatchers.Is(0));
			Assert.AssertThat(numberReplicas.CorruptReplicas(), CoreMatchers.Is(0));
			Assert.AssertThat(numberReplicas.DecommissionedReplicas(), CoreMatchers.Is(0));
			Assert.AssertThat(numberReplicas.ReplicasOnStaleNodes(), CoreMatchers.Is(0));
			BlockManagerTestUtil.UpdateState(blockManager);
			Assert.AssertThat(blockManager.GetUnderReplicatedBlocksCount(), CoreMatchers.Is(0L
				));
			Assert.AssertThat(blockManager.GetExcessBlocksCount(), CoreMatchers.Is(0L));
		}

		/// <summary>
		/// Verify that <tt>READ_ONLY_SHARED</tt> replicas are <i>not</i> counted towards the overall
		/// replication count, but <i>are</i> included as replica locations returned to clients for reads.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplicaCounting()
		{
			// There should only be 1 *replica* (the READ_ONLY_SHARED doesn't count)
			ValidateNumberReplicas(1);
			fs.SetReplication(Path, (short)2);
			// There should now be 3 *locations* for the block, and 2 *replicas*
			WaitForLocations(3);
			ValidateNumberReplicas(2);
		}

		/// <summary>
		/// Verify that the NameNode is able to still use <tt>READ_ONLY_SHARED</tt> replicas even
		/// when the single NORMAL replica is offline (and the effective replication count is 0).
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNormalReplicaOffline()
		{
			// Stop the datanode hosting the NORMAL replica
			cluster.StopDataNode(normalDataNode.GetXferAddr());
			// Force NameNode to detect that the datanode is down
			BlockManagerTestUtil.NoticeDeadDatanode(cluster.GetNameNode(), normalDataNode.GetXferAddr
				());
			// The live replica count should now be zero (since the NORMAL replica is offline)
			NumberReplicas numberReplicas = blockManager.CountNodes(block);
			Assert.AssertThat(numberReplicas.LiveReplicas(), CoreMatchers.Is(0));
			// The block should be reported as under-replicated
			BlockManagerTestUtil.UpdateState(blockManager);
			Assert.AssertThat(blockManager.GetUnderReplicatedBlocksCount(), CoreMatchers.Is(1L
				));
			// The BlockManager should be able to heal the replication count back to 1
			// by triggering an inter-datanode replication from one of the READ_ONLY_SHARED replicas
			BlockManagerTestUtil.ComputeAllPendingWork(blockManager);
			DFSTestUtil.WaitForReplication(cluster, extendedBlock, 1, 1, 0);
			// There should now be 2 *locations* for the block, and 1 *replica*
			Assert.AssertThat(GetLocatedBlock().GetLocations().Length, CoreMatchers.Is(2));
			ValidateNumberReplicas(1);
		}

		/// <summary>
		/// Verify that corrupt <tt>READ_ONLY_SHARED</tt> replicas aren't counted
		/// towards the corrupt replicas total.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadOnlyReplicaCorrupt()
		{
			// "Corrupt" a READ_ONLY_SHARED replica by reporting it as a bad replica
			client.ReportBadBlocks(new LocatedBlock[] { new LocatedBlock(extendedBlock, new DatanodeInfo
				[] { readOnlyDataNode }) });
			// There should now be only 1 *location* for the block as the READ_ONLY_SHARED is corrupt
			WaitForLocations(1);
			// However, the corrupt READ_ONLY_SHARED replica should *not* affect the overall corrupt replicas count
			NumberReplicas numberReplicas = blockManager.CountNodes(block);
			Assert.AssertThat(numberReplicas.CorruptReplicas(), CoreMatchers.Is(0));
		}
	}
}
