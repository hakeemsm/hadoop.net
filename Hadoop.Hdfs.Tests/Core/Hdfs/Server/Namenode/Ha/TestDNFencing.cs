using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito.Invocation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestDNFencing
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(TestDNFencing
			));

		private const string TestFile = "/testStandbyIsHot";

		private static readonly Path TestFilePath = new Path(TestFile);

		private const int SmallBlock = 1024;

		private Configuration conf;

		private MiniDFSCluster cluster;

		private NameNode nn1;

		private NameNode nn2;

		private FileSystem fs;

		static TestDNFencing()
		{
			DFSTestUtil.SetNameNodeLogLevel(Level.All);
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupCluster()
		{
			conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, SmallBlock);
			// Bump up replication interval so that we only run replication
			// checks explicitly.
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 600);
			// Increase max streams so that we re-replicate quickly.
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationMaxStreamsKey, 1000);
			// See RandomDeleterPolicy javadoc.
			conf.SetClass(DFSConfigKeys.DfsBlockReplicatorClassnameKey, typeof(TestDNFencing.RandomDeleterPolicy
				), typeof(BlockPlacementPolicy));
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(3).Build();
			nn1 = cluster.GetNameNode(0);
			nn2 = cluster.GetNameNode(1);
			cluster.WaitActive();
			cluster.TransitionToActive(0);
			// Trigger block reports so that the first NN trusts all
			// of the DNs, and will issue deletions
			cluster.TriggerBlockReports();
			fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void ShutdownCluster()
		{
			if (cluster != null)
			{
				Banner("Shutting down cluster. NN1 metadata:");
				DoMetasave(nn1);
				Banner("Shutting down cluster. NN2 metadata:");
				DoMetasave(nn2);
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDnFencing()
		{
			// Create a file with replication level 3.
			DFSTestUtil.CreateFile(fs, TestFilePath, 30 * SmallBlock, (short)3, 1L);
			ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, TestFilePath);
			// Drop its replication count to 1, so it becomes over-replicated.
			// Then compute the invalidation of the extra blocks and trigger
			// heartbeats so the invalidations are flushed to the DNs.
			nn1.GetRpcServer().SetReplication(TestFile, (short)1);
			BlockManagerTestUtil.ComputeInvalidationWork(nn1.GetNamesystem().GetBlockManager(
				));
			cluster.TriggerHeartbeats();
			// Transition nn2 to active even though nn1 still thinks it's active.
			Banner("Failing to NN2 but let NN1 continue to think it's active");
			NameNodeAdapter.AbortEditLogs(nn1);
			NameNodeAdapter.EnterSafeMode(nn1, false);
			cluster.TransitionToActive(1);
			// Check that the standby picked up the replication change.
			NUnit.Framework.Assert.AreEqual(1, nn2.GetRpcServer().GetFileInfo(TestFile).GetReplication
				());
			// Dump some info for debugging purposes.
			Banner("NN2 Metadata immediately after failover");
			DoMetasave(nn2);
			Banner("Triggering heartbeats and block reports so that fencing is completed");
			cluster.TriggerHeartbeats();
			cluster.TriggerBlockReports();
			Banner("Metadata after nodes have all block-reported");
			DoMetasave(nn2);
			// Force a rescan of postponedMisreplicatedBlocks.
			BlockManager nn2BM = nn2.GetNamesystem().GetBlockManager();
			BlockManagerTestUtil.CheckHeartbeat(nn2BM);
			BlockManagerTestUtil.RescanPostponedMisreplicatedBlocks(nn2BM);
			// The blocks should no longer be postponed.
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetPostponedMisreplicatedBlocks
				());
			// Wait for NN2 to enact its deletions (replication monitor has to run, etc)
			BlockManagerTestUtil.ComputeInvalidationWork(nn2.GetNamesystem().GetBlockManager(
				));
			cluster.TriggerHeartbeats();
			HATestUtil.WaitForDNDeletions(cluster);
			cluster.TriggerDeletionReports();
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetUnderReplicatedBlocks()
				);
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetPendingReplicationBlocks
				());
			Banner("Making sure the file is still readable");
			FileSystem fs2 = cluster.GetFileSystem(1);
			DFSTestUtil.ReadFile(fs2, TestFilePath);
			Banner("Waiting for the actual block files to get deleted from DNs.");
			WaitForTrueReplication(cluster, block, 1);
		}

		/// <summary>
		/// Test case which restarts the standby node in such a way that,
		/// when it exits safemode, it will want to invalidate a bunch
		/// of over-replicated block replicas.
		/// </summary>
		/// <remarks>
		/// Test case which restarts the standby node in such a way that,
		/// when it exits safemode, it will want to invalidate a bunch
		/// of over-replicated block replicas. Ensures that if we failover
		/// at this point it won't lose data.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNNClearsCommandsOnFailoverAfterStartup()
		{
			// Make lots of blocks to increase chances of triggering a bug.
			DFSTestUtil.CreateFile(fs, TestFilePath, 30 * SmallBlock, (short)3, 1L);
			Banner("Shutting down NN2");
			cluster.ShutdownNameNode(1);
			Banner("Setting replication to 1, rolling edit log.");
			nn1.GetRpcServer().SetReplication(TestFile, (short)1);
			nn1.GetRpcServer().RollEditLog();
			// Start NN2 again. When it starts up, it will see all of the
			// blocks as over-replicated, since it has the metadata for
			// replication=1, but the DNs haven't yet processed the deletions.
			Banner("Starting NN2 again.");
			cluster.RestartNameNode(1);
			nn2 = cluster.GetNameNode(1);
			Banner("triggering BRs");
			cluster.TriggerBlockReports();
			// We expect that both NN1 and NN2 will have some number of
			// deletions queued up for the DNs.
			Banner("computing invalidation on nn1");
			BlockManagerTestUtil.ComputeInvalidationWork(nn1.GetNamesystem().GetBlockManager(
				));
			Banner("computing invalidation on nn2");
			BlockManagerTestUtil.ComputeInvalidationWork(nn2.GetNamesystem().GetBlockManager(
				));
			// Dump some info for debugging purposes.
			Banner("Metadata immediately before failover");
			DoMetasave(nn2);
			// Transition nn2 to active even though nn1 still thinks it's active
			Banner("Failing to NN2 but let NN1 continue to think it's active");
			NameNodeAdapter.AbortEditLogs(nn1);
			NameNodeAdapter.EnterSafeMode(nn1, false);
			cluster.TransitionToActive(1);
			// Check that the standby picked up the replication change.
			NUnit.Framework.Assert.AreEqual(1, nn2.GetRpcServer().GetFileInfo(TestFile).GetReplication
				());
			// Dump some info for debugging purposes.
			Banner("Metadata immediately after failover");
			DoMetasave(nn2);
			Banner("Triggering heartbeats and block reports so that fencing is completed");
			cluster.TriggerHeartbeats();
			cluster.TriggerBlockReports();
			Banner("Metadata after nodes have all block-reported");
			DoMetasave(nn2);
			// Force a rescan of postponedMisreplicatedBlocks.
			BlockManager nn2BM = nn2.GetNamesystem().GetBlockManager();
			BlockManagerTestUtil.CheckHeartbeat(nn2BM);
			BlockManagerTestUtil.RescanPostponedMisreplicatedBlocks(nn2BM);
			// The block should no longer be postponed.
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetPostponedMisreplicatedBlocks
				());
			// Wait for NN2 to enact its deletions (replication monitor has to run, etc)
			BlockManagerTestUtil.ComputeInvalidationWork(nn2.GetNamesystem().GetBlockManager(
				));
			HATestUtil.WaitForNNToIssueDeletions(nn2);
			cluster.TriggerHeartbeats();
			HATestUtil.WaitForDNDeletions(cluster);
			cluster.TriggerDeletionReports();
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetUnderReplicatedBlocks()
				);
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetPendingReplicationBlocks
				());
			Banner("Making sure the file is still readable");
			FileSystem fs2 = cluster.GetFileSystem(1);
			DFSTestUtil.ReadFile(fs2, TestFilePath);
		}

		/// <summary>
		/// Test case that reduces replication of a file with a lot of blocks
		/// and then fails over right after those blocks enter the DN invalidation
		/// queues on the active.
		/// </summary>
		/// <remarks>
		/// Test case that reduces replication of a file with a lot of blocks
		/// and then fails over right after those blocks enter the DN invalidation
		/// queues on the active. Ensures that fencing is correct and no replicas
		/// are lost.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNNClearsCommandsOnFailoverWithReplChanges()
		{
			// Make lots of blocks to increase chances of triggering a bug.
			DFSTestUtil.CreateFile(fs, TestFilePath, 30 * SmallBlock, (short)1, 1L);
			Banner("rolling NN1's edit log, forcing catch-up");
			HATestUtil.WaitForStandbyToCatchUp(nn1, nn2);
			// Get some new replicas reported so that NN2 now considers
			// them over-replicated and schedules some more deletions
			nn1.GetRpcServer().SetReplication(TestFile, (short)2);
			while (BlockManagerTestUtil.GetComputedDatanodeWork(nn1.GetNamesystem().GetBlockManager
				()) > 0)
			{
				Log.Info("Getting more replication work computed");
			}
			BlockManager bm1 = nn1.GetNamesystem().GetBlockManager();
			while (bm1.GetPendingReplicationBlocksCount() > 0)
			{
				BlockManagerTestUtil.UpdateState(bm1);
				cluster.TriggerHeartbeats();
				Sharpen.Thread.Sleep(1000);
			}
			Banner("triggering BRs");
			cluster.TriggerBlockReports();
			nn1.GetRpcServer().SetReplication(TestFile, (short)1);
			Banner("computing invalidation on nn1");
			BlockManagerTestUtil.ComputeInvalidationWork(nn1.GetNamesystem().GetBlockManager(
				));
			DoMetasave(nn1);
			Banner("computing invalidation on nn2");
			BlockManagerTestUtil.ComputeInvalidationWork(nn2.GetNamesystem().GetBlockManager(
				));
			DoMetasave(nn2);
			// Dump some info for debugging purposes.
			Banner("Metadata immediately before failover");
			DoMetasave(nn2);
			// Transition nn2 to active even though nn1 still thinks it's active
			Banner("Failing to NN2 but let NN1 continue to think it's active");
			NameNodeAdapter.AbortEditLogs(nn1);
			NameNodeAdapter.EnterSafeMode(nn1, false);
			BlockManagerTestUtil.ComputeInvalidationWork(nn2.GetNamesystem().GetBlockManager(
				));
			cluster.TransitionToActive(1);
			// Check that the standby picked up the replication change.
			NUnit.Framework.Assert.AreEqual(1, nn2.GetRpcServer().GetFileInfo(TestFile).GetReplication
				());
			// Dump some info for debugging purposes.
			Banner("Metadata immediately after failover");
			DoMetasave(nn2);
			Banner("Triggering heartbeats and block reports so that fencing is completed");
			cluster.TriggerHeartbeats();
			cluster.TriggerBlockReports();
			Banner("Metadata after nodes have all block-reported");
			DoMetasave(nn2);
			// Force a rescan of postponedMisreplicatedBlocks.
			BlockManager nn2BM = nn2.GetNamesystem().GetBlockManager();
			BlockManagerTestUtil.CheckHeartbeat(nn2BM);
			BlockManagerTestUtil.RescanPostponedMisreplicatedBlocks(nn2BM);
			// The block should no longer be postponed.
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetPostponedMisreplicatedBlocks
				());
			// Wait for NN2 to enact its deletions (replication monitor has to run, etc)
			BlockManagerTestUtil.ComputeInvalidationWork(nn2.GetNamesystem().GetBlockManager(
				));
			HATestUtil.WaitForNNToIssueDeletions(nn2);
			cluster.TriggerHeartbeats();
			HATestUtil.WaitForDNDeletions(cluster);
			cluster.TriggerDeletionReports();
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetUnderReplicatedBlocks()
				);
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetPendingReplicationBlocks
				());
			Banner("Making sure the file is still readable");
			FileSystem fs2 = cluster.GetFileSystem(1);
			DFSTestUtil.ReadFile(fs2, TestFilePath);
		}

		/// <summary>Regression test for HDFS-2742.</summary>
		/// <remarks>
		/// Regression test for HDFS-2742. The issue in this bug was:
		/// - DN does a block report while file is open. This BR contains
		/// the block in RBW state.
		/// - Standby queues the RBW state in PendingDatanodeMessages
		/// - Standby processes edit logs during failover. Before fixing
		/// this bug, it was mistakenly applying the RBW reported state
		/// after the block had been completed, causing the block to get
		/// marked corrupt. Instead, we should now be applying the RBW
		/// message on OP_ADD, and then the FINALIZED message on OP_CLOSE.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockReportsWhileFileBeingWritten()
		{
			FSDataOutputStream @out = fs.Create(TestFilePath);
			try
			{
				AppendTestUtil.Write(@out, 0, 10);
				@out.Hflush();
				// Block report will include the RBW replica, but will be
				// queued on the StandbyNode.
				cluster.TriggerBlockReports();
			}
			finally
			{
				IOUtils.CloseStream(@out);
			}
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			// Verify that no replicas are marked corrupt, and that the
			// file is readable from the failed-over standby.
			BlockManagerTestUtil.UpdateState(nn1.GetNamesystem().GetBlockManager());
			BlockManagerTestUtil.UpdateState(nn2.GetNamesystem().GetBlockManager());
			NUnit.Framework.Assert.AreEqual(0, nn1.GetNamesystem().GetCorruptReplicaBlocks());
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetCorruptReplicaBlocks());
			DFSTestUtil.ReadFile(fs, TestFilePath);
		}

		/// <summary>
		/// Test that, when a block is re-opened for append, the related
		/// datanode messages are correctly queued by the SBN because
		/// they have future states and genstamps.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueingWithAppend()
		{
			int numQueued = 0;
			int numDN = cluster.GetDataNodes().Count;
			// case 1: create file and call hflush after write
			FSDataOutputStream @out = fs.Create(TestFilePath);
			try
			{
				AppendTestUtil.Write(@out, 0, 10);
				@out.Hflush();
				// Opening the file will report RBW replicas, but will be
				// queued on the StandbyNode.
				// However, the delivery of RBW messages is delayed by HDFS-7217 fix.
				// Apply cluster.triggerBlockReports() to trigger the reporting sooner.
				//
				cluster.TriggerBlockReports();
				numQueued += numDN;
				// RBW messages
				// The cluster.triggerBlockReports() call above does a full 
				// block report that incurs 3 extra RBW messages
				numQueued += numDN;
			}
			finally
			{
				// RBW messages      
				IOUtils.CloseStream(@out);
				numQueued += numDN;
			}
			// blockReceived messages
			cluster.TriggerBlockReports();
			numQueued += numDN;
			NUnit.Framework.Assert.AreEqual(numQueued, cluster.GetNameNode(1).GetNamesystem()
				.GetPendingDataNodeMessageCount());
			// case 2: append to file and call hflush after write
			try
			{
				@out = fs.Append(TestFilePath);
				AppendTestUtil.Write(@out, 10, 10);
				@out.Hflush();
				cluster.TriggerBlockReports();
				numQueued += numDN * 2;
			}
			finally
			{
				// RBW messages, see comments in case 1
				IOUtils.CloseStream(@out);
				numQueued += numDN;
			}
			// blockReceived
			NUnit.Framework.Assert.AreEqual(numQueued, cluster.GetNameNode(1).GetNamesystem()
				.GetPendingDataNodeMessageCount());
			// case 3: similar to case 2, except no hflush is called.
			try
			{
				@out = fs.Append(TestFilePath);
				AppendTestUtil.Write(@out, 20, 10);
			}
			finally
			{
				// The write operation in the try block is buffered, thus no RBW message
				// is reported yet until the closeStream call here. When closeStream is
				// called, before HDFS-7217 fix, there would be three RBW messages
				// (blockReceiving), plus three FINALIZED messages (blockReceived)
				// delivered to NN. However, because of HDFS-7217 fix, the reporting of
				// RBW  messages is postponed. In this case, they are even overwritten 
				// by the blockReceived messages of the same block when they are waiting
				// to be delivered. All this happens within the closeStream() call.
				// What's delivered to NN is the three blockReceived messages. See 
				//    BPServiceActor#addPendingReplicationBlockInfo 
				//
				IOUtils.CloseStream(@out);
				numQueued += numDN;
			}
			// blockReceived
			cluster.TriggerBlockReports();
			numQueued += numDN;
			Log.Info("Expect " + numQueued + " and got: " + cluster.GetNameNode(1).GetNamesystem
				().GetPendingDataNodeMessageCount());
			NUnit.Framework.Assert.AreEqual(numQueued, cluster.GetNameNode(1).GetNamesystem()
				.GetPendingDataNodeMessageCount());
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			// Verify that no replicas are marked corrupt, and that the
			// file is readable from the failed-over standby.
			BlockManagerTestUtil.UpdateState(nn1.GetNamesystem().GetBlockManager());
			BlockManagerTestUtil.UpdateState(nn2.GetNamesystem().GetBlockManager());
			NUnit.Framework.Assert.AreEqual(0, nn1.GetNamesystem().GetCorruptReplicaBlocks());
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetCorruptReplicaBlocks());
			AppendTestUtil.Check(fs, TestFilePath, 30);
		}

		/// <summary>Another regression test for HDFS-2742.</summary>
		/// <remarks>
		/// Another regression test for HDFS-2742. This tests the following sequence:
		/// - DN does a block report while file is open. This BR contains
		/// the block in RBW state.
		/// - The block report is delayed in reaching the standby.
		/// - The file is closed.
		/// - The standby processes the OP_ADD and OP_CLOSE operations before
		/// the RBW block report arrives.
		/// - The standby should not mark the block as corrupt.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRBWReportArrivesAfterEdits()
		{
			CountDownLatch brFinished = new CountDownLatch(1);
			GenericTestUtils.DelayAnswer delayer = new _DelayAnswer_521(brFinished, Log);
			// inform the test that our block report went through.
			FSDataOutputStream @out = fs.Create(TestFilePath);
			try
			{
				AppendTestUtil.Write(@out, 0, 10);
				@out.Hflush();
				DataNode dn = cluster.GetDataNodes()[0];
				DatanodeProtocolClientSideTranslatorPB spy = DataNodeTestUtils.SpyOnBposToNN(dn, 
					nn2);
				Org.Mockito.Mockito.DoAnswer(delayer).When(spy).BlockReport(Org.Mockito.Mockito.AnyObject
					<DatanodeRegistration>(), Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito.AnyObject
					<StorageBlockReport[]>(), Org.Mockito.Mockito.AnyObject<BlockReportContext>());
				dn.ScheduleAllBlockReport(0);
				delayer.WaitForCall();
			}
			finally
			{
				IOUtils.CloseStream(@out);
			}
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			delayer.Proceed();
			brFinished.Await();
			// Verify that no replicas are marked corrupt, and that the
			// file is readable from the failed-over standby.
			BlockManagerTestUtil.UpdateState(nn1.GetNamesystem().GetBlockManager());
			BlockManagerTestUtil.UpdateState(nn2.GetNamesystem().GetBlockManager());
			NUnit.Framework.Assert.AreEqual(0, nn1.GetNamesystem().GetCorruptReplicaBlocks());
			NUnit.Framework.Assert.AreEqual(0, nn2.GetNamesystem().GetCorruptReplicaBlocks());
			DFSTestUtil.ReadFile(fs, TestFilePath);
		}

		private sealed class _DelayAnswer_521 : GenericTestUtils.DelayAnswer
		{
			public _DelayAnswer_521(CountDownLatch brFinished, Log baseArg1)
				: base(baseArg1)
			{
				this.brFinished = brFinished;
			}

			/// <exception cref="System.Exception"/>
			protected override object PassThrough(InvocationOnMock invocation)
			{
				try
				{
					return base.PassThrough(invocation);
				}
				finally
				{
					brFinished.CountDown();
				}
			}

			private readonly CountDownLatch brFinished;
		}

		/// <summary>Print a big banner in the test log to make debug easier.</summary>
		private void Banner(string @string)
		{
			Log.Info("\n\n\n\n================================================\n" + @string +
				 "\n" + "==================================================\n\n");
		}

		private void DoMetasave(NameNode nn2)
		{
			nn2.GetNamesystem().WriteLock();
			try
			{
				PrintWriter pw = new PrintWriter(System.Console.Error);
				nn2.GetNamesystem().GetBlockManager().MetaSave(pw);
				pw.Flush();
			}
			finally
			{
				nn2.GetNamesystem().WriteUnlock();
			}
		}

		/// <exception cref="System.Exception"/>
		private void WaitForTrueReplication(MiniDFSCluster cluster, ExtendedBlock block, 
			int waitFor)
		{
			GenericTestUtils.WaitFor(new _Supplier_594(this, cluster, block, waitFor), 500, 10000
				);
		}

		private sealed class _Supplier_594 : Supplier<bool>
		{
			public _Supplier_594(TestDNFencing _enclosing, MiniDFSCluster cluster, ExtendedBlock
				 block, int waitFor)
			{
				this._enclosing = _enclosing;
				this.cluster = cluster;
				this.block = block;
				this.waitFor = waitFor;
			}

			public bool Get()
			{
				try
				{
					return this._enclosing.GetTrueReplication(cluster, block) == waitFor;
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}

			private readonly TestDNFencing _enclosing;

			private readonly MiniDFSCluster cluster;

			private readonly ExtendedBlock block;

			private readonly int waitFor;
		}

		/// <exception cref="System.IO.IOException"/>
		private int GetTrueReplication(MiniDFSCluster cluster, ExtendedBlock block)
		{
			int count = 0;
			foreach (DataNode dn in cluster.GetDataNodes())
			{
				if (DataNodeTestUtils.GetFSDataset(dn).GetStoredBlock(block.GetBlockPoolId(), block
					.GetBlockId()) != null)
				{
					count++;
				}
			}
			return count;
		}

		/// <summary>
		/// A BlockPlacementPolicy which, rather than using space available, makes
		/// random decisions about which excess replica to delete.
		/// </summary>
		/// <remarks>
		/// A BlockPlacementPolicy which, rather than using space available, makes
		/// random decisions about which excess replica to delete. This is because,
		/// in the test cases, the two NNs will usually (but not quite always)
		/// make the same decision of which replica to delete. The fencing issues
		/// are exacerbated when the two NNs make different decisions, which can
		/// happen in "real life" when they have slightly out-of-sync heartbeat
		/// information regarding disk usage.
		/// </remarks>
		public class RandomDeleterPolicy : BlockPlacementPolicyDefault
		{
			public RandomDeleterPolicy()
				: base()
			{
			}

			public override DatanodeStorageInfo ChooseReplicaToDelete(BlockCollection inode, 
				Block block, short replicationFactor, ICollection<DatanodeStorageInfo> first, ICollection
				<DatanodeStorageInfo> second, IList<StorageType> excessTypes)
			{
				ICollection<DatanodeStorageInfo> chooseFrom = !first.IsEmpty() ? first : second;
				IList<DatanodeStorageInfo> l = Lists.NewArrayList(chooseFrom);
				return l[DFSUtil.GetRandom().Next(l.Count)];
			}
		}
	}
}
