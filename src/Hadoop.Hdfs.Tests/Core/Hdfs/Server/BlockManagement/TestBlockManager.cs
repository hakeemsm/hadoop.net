using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestBlockManager
	{
		private DatanodeStorageInfo[] storages;

		private IList<DatanodeDescriptor> nodes;

		private IList<DatanodeDescriptor> rackA;

		private IList<DatanodeDescriptor> rackB;

		/// <summary>
		/// Some of these tests exercise code which has some randomness involved -
		/// ie even if there's a bug, they may pass because the random node selection
		/// chooses the correct result.
		/// </summary>
		/// <remarks>
		/// Some of these tests exercise code which has some randomness involved -
		/// ie even if there's a bug, they may pass because the random node selection
		/// chooses the correct result.
		/// Since they're true unit tests and run quickly, we loop them a number
		/// of times trying to trigger the incorrect behavior.
		/// </remarks>
		private const int NumTestIters = 30;

		private const int BlockSize = 64 * 1024;

		private FSNamesystem fsn;

		private BlockManager bm;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetupMockCluster()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.NetTopologyScriptFileNameKey, "need to set a dummy value here so it assumes a multi-rack cluster"
				);
			fsn = Org.Mockito.Mockito.Mock<FSNamesystem>();
			Org.Mockito.Mockito.DoReturn(true).When(fsn).HasWriteLock();
			bm = new BlockManager(fsn, conf);
			string[] racks = new string[] { "/rackA", "/rackA", "/rackA", "/rackB", "/rackB", 
				"/rackB" };
			storages = DFSTestUtil.CreateDatanodeStorageInfos(racks);
			nodes = Arrays.AsList(DFSTestUtil.ToDatanodeDescriptor(storages));
			rackA = nodes.SubList(0, 3);
			rackB = nodes.SubList(3, 6);
		}

		private void AddNodes(IEnumerable<DatanodeDescriptor> nodesToAdd)
		{
			NetworkTopology cluster = bm.GetDatanodeManager().GetNetworkTopology();
			// construct network topology
			foreach (DatanodeDescriptor dn in nodesToAdd)
			{
				cluster.Add(dn);
				dn.GetStorageInfos()[0].SetUtilizationForTesting(2 * HdfsConstants.MinBlocksForWrite
					 * BlockSize, 0L, 2 * HdfsConstants.MinBlocksForWrite * BlockSize, 0L);
				dn.UpdateHeartbeat(BlockManagerTestUtil.GetStorageReportsForDatanode(dn), 0L, 0L, 
					0, 0, null);
				bm.GetDatanodeManager().CheckIfClusterIsNowMultiRack(dn);
			}
		}

		private void RemoveNode(DatanodeDescriptor deadNode)
		{
			NetworkTopology cluster = bm.GetDatanodeManager().GetNetworkTopology();
			cluster.Remove(deadNode);
			bm.RemoveBlocksAssociatedTo(deadNode);
		}

		/// <summary>
		/// Test that replication of under-replicated blocks is detected
		/// and basically works
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBasicReplication()
		{
			AddNodes(nodes);
			for (int i = 0; i < NumTestIters; i++)
			{
				DoBasicTest(i);
			}
		}

		private void DoBasicTest(int testIndex)
		{
			IList<DatanodeStorageInfo> origStorages = GetStorages(0, 1);
			IList<DatanodeDescriptor> origNodes = GetNodes(origStorages);
			BlockInfoContiguous blockInfo = AddBlockOnNodes(testIndex, origNodes);
			DatanodeStorageInfo[] pipeline = ScheduleSingleReplication(blockInfo);
			NUnit.Framework.Assert.AreEqual(2, pipeline.Length);
			NUnit.Framework.Assert.IsTrue("Source of replication should be one of the nodes the block "
				 + "was on. Was: " + pipeline[0], origStorages.Contains(pipeline[0]));
			NUnit.Framework.Assert.IsTrue("Destination of replication should be on the other rack. "
				 + "Was: " + pipeline[1], rackB.Contains(pipeline[1].GetDatanodeDescriptor()));
		}

		/// <summary>
		/// Regression test for HDFS-1480
		/// - Cluster has 2 racks, A and B, each with three nodes.
		/// </summary>
		/// <remarks>
		/// Regression test for HDFS-1480
		/// - Cluster has 2 racks, A and B, each with three nodes.
		/// - Block initially written on A1, A2, B1
		/// - Admin decommissions two of these nodes (let's say A1 and A2 but it doesn't matter)
		/// - Re-replication should respect rack policy
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTwoOfThreeNodesDecommissioned()
		{
			AddNodes(nodes);
			for (int i = 0; i < NumTestIters; i++)
			{
				DoTestTwoOfThreeNodesDecommissioned(i);
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoTestTwoOfThreeNodesDecommissioned(int testIndex)
		{
			// Block originally on A1, A2, B1
			IList<DatanodeStorageInfo> origStorages = GetStorages(0, 1, 3);
			IList<DatanodeDescriptor> origNodes = GetNodes(origStorages);
			BlockInfoContiguous blockInfo = AddBlockOnNodes(testIndex, origNodes);
			// Decommission two of the nodes (A1, A2)
			IList<DatanodeDescriptor> decomNodes = StartDecommission(0, 1);
			DatanodeStorageInfo[] pipeline = ScheduleSingleReplication(blockInfo);
			NUnit.Framework.Assert.IsTrue("Source of replication should be one of the nodes the block "
				 + "was on. Was: " + pipeline[0], origStorages.Contains(pipeline[0]));
			NUnit.Framework.Assert.AreEqual("Should have three targets", 3, pipeline.Length);
			bool foundOneOnRackA = false;
			for (int i = 1; i < pipeline.Length; i++)
			{
				DatanodeDescriptor target = pipeline[i].GetDatanodeDescriptor();
				if (rackA.Contains(target))
				{
					foundOneOnRackA = true;
				}
				NUnit.Framework.Assert.IsFalse(decomNodes.Contains(target));
				NUnit.Framework.Assert.IsFalse(origNodes.Contains(target));
			}
			NUnit.Framework.Assert.IsTrue("Should have at least one target on rack A. Pipeline: "
				 + Joiner.On(",").Join(pipeline), foundOneOnRackA);
		}

		/// <summary>
		/// Test what happens when a block is on three nodes, and all three of those
		/// nodes are decommissioned.
		/// </summary>
		/// <remarks>
		/// Test what happens when a block is on three nodes, and all three of those
		/// nodes are decommissioned. It should properly re-replicate to three new
		/// nodes.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllNodesHoldingReplicasDecommissioned()
		{
			AddNodes(nodes);
			for (int i = 0; i < NumTestIters; i++)
			{
				DoTestAllNodesHoldingReplicasDecommissioned(i);
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoTestAllNodesHoldingReplicasDecommissioned(int testIndex)
		{
			// Block originally on A1, A2, B1
			IList<DatanodeStorageInfo> origStorages = GetStorages(0, 1, 3);
			IList<DatanodeDescriptor> origNodes = GetNodes(origStorages);
			BlockInfoContiguous blockInfo = AddBlockOnNodes(testIndex, origNodes);
			// Decommission all of the nodes
			IList<DatanodeDescriptor> decomNodes = StartDecommission(0, 1, 3);
			DatanodeStorageInfo[] pipeline = ScheduleSingleReplication(blockInfo);
			NUnit.Framework.Assert.IsTrue("Source of replication should be one of the nodes the block "
				 + "was on. Was: " + pipeline[0], origStorages.Contains(pipeline[0]));
			NUnit.Framework.Assert.AreEqual("Should have three targets", 4, pipeline.Length);
			bool foundOneOnRackA = false;
			bool foundOneOnRackB = false;
			for (int i = 1; i < pipeline.Length; i++)
			{
				DatanodeDescriptor target = pipeline[i].GetDatanodeDescriptor();
				if (rackA.Contains(target))
				{
					foundOneOnRackA = true;
				}
				else
				{
					if (rackB.Contains(target))
					{
						foundOneOnRackB = true;
					}
				}
				NUnit.Framework.Assert.IsFalse(decomNodes.Contains(target));
				NUnit.Framework.Assert.IsFalse(origNodes.Contains(target));
			}
			NUnit.Framework.Assert.IsTrue("Should have at least one target on rack A. Pipeline: "
				 + Joiner.On(",").Join(pipeline), foundOneOnRackA);
			NUnit.Framework.Assert.IsTrue("Should have at least one target on rack B. Pipeline: "
				 + Joiner.On(",").Join(pipeline), foundOneOnRackB);
		}

		/// <summary>
		/// Test what happens when there are two racks, and an entire rack is
		/// decommissioned.
		/// </summary>
		/// <remarks>
		/// Test what happens when there are two racks, and an entire rack is
		/// decommissioned.
		/// Since the cluster is multi-rack, it will consider the block
		/// under-replicated rather than create a third replica on the
		/// same rack. Adding a new node on a third rack should cause re-replication
		/// to that node.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOneOfTwoRacksDecommissioned()
		{
			AddNodes(nodes);
			for (int i = 0; i < NumTestIters; i++)
			{
				DoTestOneOfTwoRacksDecommissioned(i);
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoTestOneOfTwoRacksDecommissioned(int testIndex)
		{
			// Block originally on A1, A2, B1
			IList<DatanodeStorageInfo> origStorages = GetStorages(0, 1, 3);
			IList<DatanodeDescriptor> origNodes = GetNodes(origStorages);
			BlockInfoContiguous blockInfo = AddBlockOnNodes(testIndex, origNodes);
			// Decommission all of the nodes in rack A
			IList<DatanodeDescriptor> decomNodes = StartDecommission(0, 1, 2);
			DatanodeStorageInfo[] pipeline = ScheduleSingleReplication(blockInfo);
			NUnit.Framework.Assert.IsTrue("Source of replication should be one of the nodes the block "
				 + "was on. Was: " + pipeline[0], origStorages.Contains(pipeline[0]));
			// Only up to two nodes can be picked per rack when there are two racks.
			NUnit.Framework.Assert.AreEqual("Should have two targets", 2, pipeline.Length);
			bool foundOneOnRackB = false;
			for (int i = 1; i < pipeline.Length; i++)
			{
				DatanodeDescriptor target = pipeline[i].GetDatanodeDescriptor();
				if (rackB.Contains(target))
				{
					foundOneOnRackB = true;
				}
				NUnit.Framework.Assert.IsFalse(decomNodes.Contains(target));
				NUnit.Framework.Assert.IsFalse(origNodes.Contains(target));
			}
			NUnit.Framework.Assert.IsTrue("Should have at least one target on rack B. Pipeline: "
				 + Joiner.On(",").Join(pipeline), foundOneOnRackB);
			// Mark the block as received on the target nodes in the pipeline
			FulfillPipeline(blockInfo, pipeline);
			// the block is still under-replicated. Add a new node. This should allow
			// the third off-rack replica.
			DatanodeDescriptor rackCNode = DFSTestUtil.GetDatanodeDescriptor("7.7.7.7", "/rackC"
				);
			rackCNode.UpdateStorage(new DatanodeStorage(DatanodeStorage.GenerateUuid()));
			AddNodes(ImmutableList.Of(rackCNode));
			try
			{
				DatanodeStorageInfo[] pipeline2 = ScheduleSingleReplication(blockInfo);
				NUnit.Framework.Assert.AreEqual(2, pipeline2.Length);
				NUnit.Framework.Assert.AreEqual(rackCNode, pipeline2[1].GetDatanodeDescriptor());
			}
			finally
			{
				RemoveNode(rackCNode);
			}
		}

		/// <summary>
		/// Unit test version of testSufficientlyReplBlocksUsesNewRack from
		/// <see cref="TestBlocksWithNotEnoughRacks"/>
		/// .
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSufficientlyReplBlocksUsesNewRack()
		{
			AddNodes(nodes);
			for (int i = 0; i < NumTestIters; i++)
			{
				DoTestSufficientlyReplBlocksUsesNewRack(i);
			}
		}

		private void DoTestSufficientlyReplBlocksUsesNewRack(int testIndex)
		{
			// Originally on only nodes in rack A.
			IList<DatanodeDescriptor> origNodes = rackA;
			BlockInfoContiguous blockInfo = AddBlockOnNodes(testIndex, origNodes);
			DatanodeStorageInfo[] pipeline = ScheduleSingleReplication(blockInfo);
			NUnit.Framework.Assert.AreEqual(2, pipeline.Length);
			// single new copy
			NUnit.Framework.Assert.IsTrue("Source of replication should be one of the nodes the block "
				 + "was on. Was: " + pipeline[0], origNodes.Contains(pipeline[0].GetDatanodeDescriptor
				()));
			NUnit.Framework.Assert.IsTrue("Destination of replication should be on the other rack. "
				 + "Was: " + pipeline[1], rackB.Contains(pipeline[1].GetDatanodeDescriptor()));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlocksAreNotUnderreplicatedInSingleRack()
		{
			IList<DatanodeDescriptor> nodes = ImmutableList.Of(BlockManagerTestUtil.GetDatanodeDescriptor
				("1.1.1.1", "/rackA", true), BlockManagerTestUtil.GetDatanodeDescriptor("2.2.2.2"
				, "/rackA", true), BlockManagerTestUtil.GetDatanodeDescriptor("3.3.3.3", "/rackA"
				, true), BlockManagerTestUtil.GetDatanodeDescriptor("4.4.4.4", "/rackA", true), 
				BlockManagerTestUtil.GetDatanodeDescriptor("5.5.5.5", "/rackA", true), BlockManagerTestUtil
				.GetDatanodeDescriptor("6.6.6.6", "/rackA", true));
			AddNodes(nodes);
			IList<DatanodeDescriptor> origNodes = nodes.SubList(0, 3);
			for (int i = 0; i < NumTestIters; i++)
			{
				DoTestSingleRackClusterIsSufficientlyReplicated(i, origNodes);
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoTestSingleRackClusterIsSufficientlyReplicated(int testIndex, IList
			<DatanodeDescriptor> origNodes)
		{
			NUnit.Framework.Assert.AreEqual(0, bm.NumOfUnderReplicatedBlocks());
			AddBlockOnNodes(testIndex, origNodes);
			bm.ProcessMisReplicatedBlocks();
			NUnit.Framework.Assert.AreEqual(0, bm.NumOfUnderReplicatedBlocks());
		}

		/// <summary>
		/// Tell the block manager that replication is completed for the given
		/// pipeline.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void FulfillPipeline(BlockInfoContiguous blockInfo, DatanodeStorageInfo[]
			 pipeline)
		{
			for (int i = 1; i < pipeline.Length; i++)
			{
				DatanodeStorageInfo storage = pipeline[i];
				bm.AddBlock(storage, blockInfo, null);
				blockInfo.AddStorage(storage);
			}
		}

		private BlockInfoContiguous BlockOnNodes(long blkId, IList<DatanodeDescriptor> nodes
			)
		{
			Block block = new Block(blkId);
			BlockInfoContiguous blockInfo = new BlockInfoContiguous(block, (short)3);
			foreach (DatanodeDescriptor dn in nodes)
			{
				foreach (DatanodeStorageInfo storage in dn.GetStorageInfos())
				{
					blockInfo.AddStorage(storage);
				}
			}
			return blockInfo;
		}

		private IList<DatanodeDescriptor> GetNodes(params int[] indexes)
		{
			IList<DatanodeDescriptor> ret = Lists.NewArrayList();
			foreach (int idx in indexes)
			{
				ret.AddItem(nodes[idx]);
			}
			return ret;
		}

		private IList<DatanodeDescriptor> GetNodes(IList<DatanodeStorageInfo> storages)
		{
			IList<DatanodeDescriptor> ret = Lists.NewArrayList();
			foreach (DatanodeStorageInfo s in storages)
			{
				ret.AddItem(s.GetDatanodeDescriptor());
			}
			return ret;
		}

		private IList<DatanodeStorageInfo> GetStorages(params int[] indexes)
		{
			IList<DatanodeStorageInfo> ret = Lists.NewArrayList();
			foreach (int idx in indexes)
			{
				ret.AddItem(storages[idx]);
			}
			return ret;
		}

		private IList<DatanodeDescriptor> StartDecommission(params int[] indexes)
		{
			IList<DatanodeDescriptor> nodes = GetNodes(indexes);
			foreach (DatanodeDescriptor node in nodes)
			{
				node.StartDecommission();
			}
			return nodes;
		}

		private BlockInfoContiguous AddBlockOnNodes(long blockId, IList<DatanodeDescriptor
			> nodes)
		{
			BlockCollection bc = Org.Mockito.Mockito.Mock<BlockCollection>();
			Org.Mockito.Mockito.DoReturn((short)3).When(bc).GetBlockReplication();
			BlockInfoContiguous blockInfo = BlockOnNodes(blockId, nodes);
			bm.blocksMap.AddBlockCollection(blockInfo, bc);
			return blockInfo;
		}

		private DatanodeStorageInfo[] ScheduleSingleReplication(Block block)
		{
			// list for priority 1
			IList<Block> list_p1 = new AList<Block>();
			list_p1.AddItem(block);
			// list of lists for each priority
			IList<IList<Block>> list_all = new AList<IList<Block>>();
			list_all.AddItem(new AList<Block>());
			// for priority 0
			list_all.AddItem(list_p1);
			// for priority 1
			NUnit.Framework.Assert.AreEqual("Block not initially pending replication", 0, bm.
				pendingReplications.GetNumReplicas(block));
			NUnit.Framework.Assert.AreEqual("computeReplicationWork should indicate replication is needed"
				, 1, bm.ComputeReplicationWorkForBlocks(list_all));
			NUnit.Framework.Assert.IsTrue("replication is pending after work is computed", bm
				.pendingReplications.GetNumReplicas(block) > 0);
			LinkedListMultimap<DatanodeStorageInfo, DatanodeDescriptor.BlockTargetPair> repls
				 = GetAllPendingReplications();
			NUnit.Framework.Assert.AreEqual(1, repls.Size());
			KeyValuePair<DatanodeStorageInfo, DatanodeDescriptor.BlockTargetPair> repl = repls
				.Entries().GetEnumerator().Next();
			DatanodeStorageInfo[] targets = repl.Value.targets;
			DatanodeStorageInfo[] pipeline = new DatanodeStorageInfo[1 + targets.Length];
			pipeline[0] = repl.Key;
			System.Array.Copy(targets, 0, pipeline, 1, targets.Length);
			return pipeline;
		}

		private LinkedListMultimap<DatanodeStorageInfo, DatanodeDescriptor.BlockTargetPair
			> GetAllPendingReplications()
		{
			LinkedListMultimap<DatanodeStorageInfo, DatanodeDescriptor.BlockTargetPair> repls
				 = LinkedListMultimap.Create();
			foreach (DatanodeDescriptor dn in nodes)
			{
				IList<DatanodeDescriptor.BlockTargetPair> thisRepls = dn.GetReplicationCommand(10
					);
				if (thisRepls != null)
				{
					foreach (DatanodeStorageInfo storage in dn.GetStorageInfos())
					{
						repls.PutAll(storage, thisRepls);
					}
				}
			}
			return repls;
		}

		/// <summary>
		/// Test that a source node for a highest-priority replication is chosen even if all available
		/// source nodes have reached their replication limits.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHighestPriReplSrcChosenDespiteMaxReplLimit()
		{
			bm.maxReplicationStreams = 0;
			bm.replicationStreamsHardLimit = 1;
			long blockId = 42;
			// arbitrary
			Block aBlock = new Block(blockId, 0, 0);
			IList<DatanodeDescriptor> origNodes = GetNodes(0, 1);
			// Add the block to the first node.
			AddBlockOnNodes(blockId, origNodes.SubList(0, 1));
			IList<DatanodeDescriptor> cntNodes = new List<DatanodeDescriptor>();
			IList<DatanodeStorageInfo> liveNodes = new List<DatanodeStorageInfo>();
			NUnit.Framework.Assert.IsNotNull("Chooses source node for a highest-priority replication"
				 + " even if all available source nodes have reached their replication" + " limits below the hard limit."
				, bm.ChooseSourceDatanode(aBlock, cntNodes, liveNodes, new NumberReplicas(), UnderReplicatedBlocks
				.QueueHighestPriority));
			NUnit.Framework.Assert.IsNull("Does not choose a source node for a less-than-highest-priority"
				 + " replication since all available source nodes have reached" + " their replication limits."
				, bm.ChooseSourceDatanode(aBlock, cntNodes, liveNodes, new NumberReplicas(), UnderReplicatedBlocks
				.QueueVeryUnderReplicated));
			// Increase the replication count to test replication count > hard limit
			DatanodeStorageInfo[] targets = new DatanodeStorageInfo[] { origNodes[1].GetStorageInfos
				()[0] };
			origNodes[0].AddBlockToBeReplicated(aBlock, targets);
			NUnit.Framework.Assert.IsNull("Does not choose a source node for a highest-priority"
				 + " replication when all available nodes exceed the hard limit.", bm.ChooseSourceDatanode
				(aBlock, cntNodes, liveNodes, new NumberReplicas(), UnderReplicatedBlocks.QueueHighestPriority
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFavorDecomUntilHardLimit()
		{
			bm.maxReplicationStreams = 0;
			bm.replicationStreamsHardLimit = 1;
			long blockId = 42;
			// arbitrary
			Block aBlock = new Block(blockId, 0, 0);
			IList<DatanodeDescriptor> origNodes = GetNodes(0, 1);
			// Add the block to the first node.
			AddBlockOnNodes(blockId, origNodes.SubList(0, 1));
			origNodes[0].StartDecommission();
			IList<DatanodeDescriptor> cntNodes = new List<DatanodeDescriptor>();
			IList<DatanodeStorageInfo> liveNodes = new List<DatanodeStorageInfo>();
			NUnit.Framework.Assert.IsNotNull("Chooses decommissioning source node for a normal replication"
				 + " if all available source nodes have reached their replication" + " limits below the hard limit."
				, bm.ChooseSourceDatanode(aBlock, cntNodes, liveNodes, new NumberReplicas(), UnderReplicatedBlocks
				.QueueUnderReplicated));
			// Increase the replication count to test replication count > hard limit
			DatanodeStorageInfo[] targets = new DatanodeStorageInfo[] { origNodes[1].GetStorageInfos
				()[0] };
			origNodes[0].AddBlockToBeReplicated(aBlock, targets);
			NUnit.Framework.Assert.IsNull("Does not choose a source decommissioning node for a normal"
				 + " replication when all available nodes exceed the hard limit.", bm.ChooseSourceDatanode
				(aBlock, cntNodes, liveNodes, new NumberReplicas(), UnderReplicatedBlocks.QueueUnderReplicated
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSafeModeIBR()
		{
			DatanodeDescriptor node = Org.Mockito.Mockito.Spy(nodes[0]);
			DatanodeStorageInfo ds = node.GetStorageInfos()[0];
			node.isAlive = true;
			DatanodeRegistration nodeReg = new DatanodeRegistration(node, null, null, string.Empty
				);
			// pretend to be in safemode
			Org.Mockito.Mockito.DoReturn(true).When(fsn).IsInStartupSafeMode();
			// register new node
			bm.GetDatanodeManager().RegisterDatanode(nodeReg);
			bm.GetDatanodeManager().AddDatanode(node);
			// swap in spy    
			NUnit.Framework.Assert.AreEqual(node, bm.GetDatanodeManager().GetDatanode(node));
			NUnit.Framework.Assert.AreEqual(0, ds.GetBlockReportCount());
			// send block report, should be processed
			Org.Mockito.Mockito.Reset(node);
			bm.ProcessReport(node, new DatanodeStorage(ds.GetStorageID()), BlockListAsLongs.Empty
				, null, false);
			NUnit.Framework.Assert.AreEqual(1, ds.GetBlockReportCount());
			// send block report again, should NOT be processed
			Org.Mockito.Mockito.Reset(node);
			bm.ProcessReport(node, new DatanodeStorage(ds.GetStorageID()), BlockListAsLongs.Empty
				, null, false);
			NUnit.Framework.Assert.AreEqual(1, ds.GetBlockReportCount());
			// re-register as if node restarted, should update existing node
			bm.GetDatanodeManager().RemoveDatanode(node);
			Org.Mockito.Mockito.Reset(node);
			bm.GetDatanodeManager().RegisterDatanode(nodeReg);
			Org.Mockito.Mockito.Verify(node).UpdateRegInfo(nodeReg);
			// send block report, should be processed after restart
			Org.Mockito.Mockito.Reset(node);
			bm.ProcessReport(node, new DatanodeStorage(ds.GetStorageID()), BlockListAsLongs.Empty
				, null, false);
			// Reinitialize as registration with empty storage list pruned
			// node.storageMap.
			ds = node.GetStorageInfos()[0];
			NUnit.Framework.Assert.AreEqual(1, ds.GetBlockReportCount());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSafeModeIBRAfterIncremental()
		{
			DatanodeDescriptor node = Org.Mockito.Mockito.Spy(nodes[0]);
			DatanodeStorageInfo ds = node.GetStorageInfos()[0];
			node.isAlive = true;
			DatanodeRegistration nodeReg = new DatanodeRegistration(node, null, null, string.Empty
				);
			// pretend to be in safemode
			Org.Mockito.Mockito.DoReturn(true).When(fsn).IsInStartupSafeMode();
			// register new node
			bm.GetDatanodeManager().RegisterDatanode(nodeReg);
			bm.GetDatanodeManager().AddDatanode(node);
			// swap in spy    
			NUnit.Framework.Assert.AreEqual(node, bm.GetDatanodeManager().GetDatanode(node));
			NUnit.Framework.Assert.AreEqual(0, ds.GetBlockReportCount());
			// send block report while pretending to already have blocks
			Org.Mockito.Mockito.Reset(node);
			Org.Mockito.Mockito.DoReturn(1).When(node).NumBlocks();
			bm.ProcessReport(node, new DatanodeStorage(ds.GetStorageID()), BlockListAsLongs.Empty
				, null, false);
			NUnit.Framework.Assert.AreEqual(1, ds.GetBlockReportCount());
		}

		/// <summary>
		/// test when NN starts and in same mode, it receives an incremental blockReport
		/// firstly.
		/// </summary>
		/// <remarks>
		/// test when NN starts and in same mode, it receives an incremental blockReport
		/// firstly. Then receives first full block report.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSafeModeIBRBeforeFirstFullBR()
		{
			// pretend to be in safemode
			Org.Mockito.Mockito.DoReturn(true).When(fsn).IsInStartupSafeMode();
			DatanodeDescriptor node = nodes[0];
			DatanodeStorageInfo ds = node.GetStorageInfos()[0];
			node.isAlive = true;
			DatanodeRegistration nodeReg = new DatanodeRegistration(node, null, null, string.Empty
				);
			// register new node
			bm.GetDatanodeManager().RegisterDatanode(nodeReg);
			bm.GetDatanodeManager().AddDatanode(node);
			NUnit.Framework.Assert.AreEqual(node, bm.GetDatanodeManager().GetDatanode(node));
			NUnit.Framework.Assert.AreEqual(0, ds.GetBlockReportCount());
			// Build a incremental report
			IList<ReceivedDeletedBlockInfo> rdbiList = new AList<ReceivedDeletedBlockInfo>();
			// Build a full report
			BlockListAsLongs.Builder builder = BlockListAsLongs.Builder();
			// blk_42 is finalized.
			long receivedBlockId = 42;
			// arbitrary
			BlockInfoContiguous receivedBlock = AddBlockToBM(receivedBlockId);
			rdbiList.AddItem(new ReceivedDeletedBlockInfo(new Block(receivedBlock), ReceivedDeletedBlockInfo.BlockStatus
				.ReceivedBlock, null));
			builder.Add(new FinalizedReplica(receivedBlock, null, null));
			// blk_43 is under construction.
			long receivingBlockId = 43;
			BlockInfoContiguous receivingBlock = AddUcBlockToBM(receivingBlockId);
			rdbiList.AddItem(new ReceivedDeletedBlockInfo(new Block(receivingBlock), ReceivedDeletedBlockInfo.BlockStatus
				.ReceivingBlock, null));
			builder.Add(new ReplicaBeingWritten(receivingBlock, null, null, null));
			// blk_44 has 2 records in IBR. It's finalized. So full BR has 1 record.
			long receivingReceivedBlockId = 44;
			BlockInfoContiguous receivingReceivedBlock = AddBlockToBM(receivingReceivedBlockId
				);
			rdbiList.AddItem(new ReceivedDeletedBlockInfo(new Block(receivingReceivedBlock), 
				ReceivedDeletedBlockInfo.BlockStatus.ReceivingBlock, null));
			rdbiList.AddItem(new ReceivedDeletedBlockInfo(new Block(receivingReceivedBlock), 
				ReceivedDeletedBlockInfo.BlockStatus.ReceivedBlock, null));
			builder.Add(new FinalizedReplica(receivingReceivedBlock, null, null));
			// blk_45 is not in full BR, because it's deleted.
			long ReceivedDeletedBlockId = 45;
			rdbiList.AddItem(new ReceivedDeletedBlockInfo(new Block(ReceivedDeletedBlockId), 
				ReceivedDeletedBlockInfo.BlockStatus.ReceivedBlock, null));
			rdbiList.AddItem(new ReceivedDeletedBlockInfo(new Block(ReceivedDeletedBlockId), 
				ReceivedDeletedBlockInfo.BlockStatus.DeletedBlock, null));
			// blk_46 exists in DN for a long time, so it's in full BR, but not in IBR.
			long existedBlockId = 46;
			BlockInfoContiguous existedBlock = AddBlockToBM(existedBlockId);
			builder.Add(new FinalizedReplica(existedBlock, null, null));
			// process IBR and full BR
			StorageReceivedDeletedBlocks srdb = new StorageReceivedDeletedBlocks(new DatanodeStorage
				(ds.GetStorageID()), Sharpen.Collections.ToArray(rdbiList, new ReceivedDeletedBlockInfo
				[rdbiList.Count]));
			bm.ProcessIncrementalBlockReport(node, srdb);
			// Make sure it's the first full report
			NUnit.Framework.Assert.AreEqual(0, ds.GetBlockReportCount());
			bm.ProcessReport(node, new DatanodeStorage(ds.GetStorageID()), builder.Build(), null
				, false);
			NUnit.Framework.Assert.AreEqual(1, ds.GetBlockReportCount());
			// verify the storage info is correct
			NUnit.Framework.Assert.IsTrue(bm.GetStoredBlock(new Block(receivedBlockId)).FindStorageInfo
				(ds) >= 0);
			NUnit.Framework.Assert.IsTrue(((BlockInfoContiguousUnderConstruction)bm.GetStoredBlock
				(new Block(receivingBlockId))).GetNumExpectedLocations() > 0);
			NUnit.Framework.Assert.IsTrue(bm.GetStoredBlock(new Block(receivingReceivedBlockId
				)).FindStorageInfo(ds) >= 0);
			NUnit.Framework.Assert.IsNull(bm.GetStoredBlock(new Block(ReceivedDeletedBlockId)
				));
			NUnit.Framework.Assert.IsTrue(bm.GetStoredBlock(new Block(existedBlock)).FindStorageInfo
				(ds) >= 0);
		}

		private BlockInfoContiguous AddBlockToBM(long blkId)
		{
			Block block = new Block(blkId);
			BlockInfoContiguous blockInfo = new BlockInfoContiguous(block, (short)3);
			BlockCollection bc = Org.Mockito.Mockito.Mock<BlockCollection>();
			Org.Mockito.Mockito.DoReturn((short)3).When(bc).GetBlockReplication();
			bm.blocksMap.AddBlockCollection(blockInfo, bc);
			return blockInfo;
		}

		private BlockInfoContiguous AddUcBlockToBM(long blkId)
		{
			Block block = new Block(blkId);
			BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction
				(block, (short)3);
			BlockCollection bc = Org.Mockito.Mockito.Mock<BlockCollection>();
			Org.Mockito.Mockito.DoReturn((short)3).When(bc).GetBlockReplication();
			bm.blocksMap.AddBlockCollection(blockInfo, bc);
			return blockInfo;
		}

		/// <summary>
		/// Tests that a namenode doesn't choose a datanode with full disks to
		/// store blocks.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStorageWithRemainingCapacity()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = FileSystem.Get(conf);
			Path file1 = null;
			try
			{
				cluster.WaitActive();
				FSNamesystem namesystem = cluster.GetNamesystem();
				string poolId = namesystem.GetBlockPoolId();
				DatanodeRegistration nodeReg = DataNodeTestUtils.GetDNRegistrationForBP(cluster.GetDataNodes
					()[0], poolId);
				DatanodeDescriptor dd = NameNodeAdapter.GetDatanode(namesystem, nodeReg);
				// By default, MiniDFSCluster will create 1 datanode with 2 storages.
				// Assigning 64k for remaining storage capacity and will 
				//create a file with 100k.
				foreach (DatanodeStorageInfo storage in dd.GetStorageInfos())
				{
					storage.SetUtilizationForTesting(65536, 0, 65536, 0);
				}
				//sum of the remaining capacity of both the storages
				dd.SetRemaining(131072);
				file1 = new Path("testRemainingStorage.dat");
				try
				{
					DFSTestUtil.CreateFile(fs, file1, 102400, 102400, 102400, (short)1, unchecked((int
						)(0x1BAD5EED)));
				}
				catch (RemoteException re)
				{
					GenericTestUtils.AssertExceptionContains("nodes instead of " + "minReplication", 
						re);
				}
			}
			finally
			{
				// Clean up
				NUnit.Framework.Assert.IsTrue(fs.Exists(file1));
				fs.Delete(file1, true);
				NUnit.Framework.Assert.IsTrue(!fs.Exists(file1));
				cluster.Shutdown();
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUseDelHint()
		{
			DatanodeStorageInfo delHint = new DatanodeStorageInfo(DFSTestUtil.GetLocalDatanodeDescriptor
				(), new DatanodeStorage("id"));
			IList<DatanodeStorageInfo> moreThan1Racks = Arrays.AsList(delHint);
			IList<StorageType> excessTypes = new AList<StorageType>();
			excessTypes.AddItem(StorageType.Default);
			NUnit.Framework.Assert.IsTrue(BlockManager.UseDelHint(true, delHint, null, moreThan1Racks
				, excessTypes));
			excessTypes.Remove(0);
			excessTypes.AddItem(StorageType.Ssd);
			NUnit.Framework.Assert.IsFalse(BlockManager.UseDelHint(true, delHint, null, moreThan1Racks
				, excessTypes));
		}

		/// <summary>
		/// <see cref="BlockManager#blockHasEnoughRacks(BlockInfo)"/>
		/// should return false
		/// if all the replicas are on the same rack and shouldn't be dependent on
		/// CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllReplicasOnSameRack()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Unset(DFSConfigKeys.NetTopologyScriptFileNameKey);
			fsn = Org.Mockito.Mockito.Mock<FSNamesystem>();
			Org.Mockito.Mockito.DoReturn(true).When(fsn).HasWriteLock();
			Org.Mockito.Mockito.DoReturn(true).When(fsn).HasReadLock();
			bm = new BlockManager(fsn, conf);
			// Add nodes on two racks
			AddNodes(nodes);
			// Added a new block in blocksMap and all the replicas are on the same rack
			BlockInfoContiguous blockInfo = AddBlockOnNodes(1, rackA);
			// Since the network toppolgy is multi-rack, the blockHasEnoughRacks 
			// should return false.
			NUnit.Framework.Assert.IsFalse("Replicas for block is not stored on enough racks"
				, bm.BlockHasEnoughRacks(blockInfo));
		}
	}
}
