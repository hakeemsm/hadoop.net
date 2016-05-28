using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Hamcrest.Core;
using Org.Mockito.Invocation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// This is the base class for simulating a variety of situations
	/// when blocks are being intentionally corrupted, unexpectedly modified,
	/// and so on before a block report is happening.
	/// </summary>
	/// <remarks>
	/// This is the base class for simulating a variety of situations
	/// when blocks are being intentionally corrupted, unexpectedly modified,
	/// and so on before a block report is happening.
	/// By overriding
	/// <see cref="SendBlockReports(Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeRegistration, string, Org.Apache.Hadoop.Hdfs.Server.Protocol.StorageBlockReport[])
	/// 	"/>
	/// , derived classes can test
	/// different variations of how block reports are split across storages
	/// and messages.
	/// </remarks>
	public abstract class BlockReportTestBase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(BlockReportTestBase));

		private static short ReplFactor = 1;

		private const int RandLimit = 2000;

		private const long DnRescanInterval = 1;

		private const long DnRescanExtraWait = 3 * DnRescanInterval;

		private const int DnN0 = 0;

		private const int FileStart = 0;

		private const int BlockSize = 1024;

		private const int NumBlocks = 10;

		private const int FileSize = NumBlocks * BlockSize + 1;

		protected internal MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		private static readonly Random rand = new Random(RandLimit);

		private static Configuration conf;

		static BlockReportTestBase()
		{
			InitLoggers();
			ResetConfiguration();
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartUpCluster()
		{
			ReplFactor = 1;
			//Reset if case a test has modified the value
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(ReplFactor).Build();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutDownCluster()
		{
			fs.Close();
			cluster.ShutdownDataNodes();
			cluster.Shutdown();
		}

		protected internal static void ResetConfiguration()
		{
			conf = new Configuration();
			int customPerChecksumSize = 512;
			int customBlockSize = customPerChecksumSize * 3;
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, customPerChecksumSize);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, customBlockSize);
			conf.SetLong(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, DnRescanInterval);
		}

		// Generate a block report, optionally corrupting the generation
		// stamp and/or length of one block.
		private static StorageBlockReport[] GetBlockReports(DataNode dn, string bpid, bool
			 corruptOneBlockGs, bool corruptOneBlockLen)
		{
			IDictionary<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists = dn.GetFSDataset
				().GetBlockReports(bpid);
			// Send block report
			StorageBlockReport[] reports = new StorageBlockReport[perVolumeBlockLists.Count];
			bool corruptedGs = false;
			bool corruptedLen = false;
			int reportIndex = 0;
			foreach (KeyValuePair<DatanodeStorage, BlockListAsLongs> kvPair in perVolumeBlockLists)
			{
				DatanodeStorage dnStorage = kvPair.Key;
				BlockListAsLongs blockList = kvPair.Value;
				// Walk the list of blocks until we find one each to corrupt the
				// generation stamp and length, if so requested.
				BlockListAsLongs.Builder builder = BlockListAsLongs.Builder();
				foreach (BlockListAsLongs.BlockReportReplica block in blockList)
				{
					if (corruptOneBlockGs && !corruptedGs)
					{
						long gsOld = block.GetGenerationStamp();
						long gsNew;
						do
						{
							gsNew = rand.Next();
						}
						while (gsNew == gsOld);
						block.SetGenerationStamp(gsNew);
						Log.Info("Corrupted the GS for block ID " + block);
						corruptedGs = true;
					}
					else
					{
						if (corruptOneBlockLen && !corruptedLen)
						{
							long lenOld = block.GetNumBytes();
							long lenNew;
							do
							{
								lenNew = rand.Next((int)lenOld - 1);
							}
							while (lenNew == lenOld);
							block.SetNumBytes(lenNew);
							Log.Info("Corrupted the length for block ID " + block);
							corruptedLen = true;
						}
					}
					builder.Add(new BlockListAsLongs.BlockReportReplica(block));
				}
				reports[reportIndex++] = new StorageBlockReport(dnStorage, builder.Build());
			}
			return reports;
		}

		/// <summary>
		/// Utility routine to send block reports to the NN, either in a single call
		/// or reporting one storage per call.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void SendBlockReports(DatanodeRegistration dnR, string
			 poolId, StorageBlockReport[] reports);

		/// <summary>Test write a file, verifies and closes it.</summary>
		/// <remarks>
		/// Test write a file, verifies and closes it. Then the length of the blocks
		/// are messed up and BlockReport is forced.
		/// The modification of blocks' length has to be ignored
		/// </remarks>
		/// <exception cref="System.IO.IOException">on an error</exception>
		public virtual void BlockReport_01()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path filePath = new Path("/" + MethodName + ".dat");
			AList<Block> blocks = PrepareForRide(filePath, MethodName, FileSize);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Number of blocks allocated " + blocks.Count);
			}
			long[] oldLengths = new long[blocks.Count];
			int tempLen;
			for (int i = 0; i < blocks.Count; i++)
			{
				Block b = blocks[i];
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Block " + b.GetBlockName() + " before\t" + "Size " + b.GetNumBytes());
				}
				oldLengths[i] = b.GetNumBytes();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Setting new length");
				}
				tempLen = rand.Next(BlockSize);
				b.Set(b.GetBlockId(), tempLen, b.GetGenerationStamp());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Block " + b.GetBlockName() + " after\t " + "Size " + b.GetNumBytes());
				}
			}
			// all blocks belong to the same file, hence same BP
			DataNode dn = cluster.GetDataNodes()[DnN0];
			string poolId = cluster.GetNamesystem().GetBlockPoolId();
			DatanodeRegistration dnR = dn.GetDNRegistrationForBP(poolId);
			StorageBlockReport[] reports = GetBlockReports(dn, poolId, false, false);
			SendBlockReports(dnR, poolId, reports);
			IList<LocatedBlock> blocksAfterReport = DFSTestUtil.GetAllBlocks(fs.Open(filePath
				));
			if (Log.IsDebugEnabled())
			{
				Log.Debug("After mods: Number of blocks allocated " + blocksAfterReport.Count);
			}
			for (int i_1 = 0; i_1 < blocksAfterReport.Count; i_1++)
			{
				ExtendedBlock b = blocksAfterReport[i_1].GetBlock();
				NUnit.Framework.Assert.AreEqual("Length of " + i_1 + "th block is incorrect", oldLengths
					[i_1], b.GetNumBytes());
			}
		}

		/// <summary>Test write a file, verifies and closes it.</summary>
		/// <remarks>
		/// Test write a file, verifies and closes it. Then a couple of random blocks
		/// is removed and BlockReport is forced; the FSNamesystem is pushed to
		/// recalculate required DN's activities such as replications and so on.
		/// The number of missing and under-replicated blocks should be the same in
		/// case of a single-DN cluster.
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of errors</exception>
		public virtual void BlockReport_02()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Log.Info("Running test " + MethodName);
			Path filePath = new Path("/" + MethodName + ".dat");
			DFSTestUtil.CreateFile(fs, filePath, FileSize, ReplFactor, rand.NextLong());
			// mock around with newly created blocks and delete some
			FilePath dataDir = new FilePath(cluster.GetDataDirectory());
			NUnit.Framework.Assert.IsTrue(dataDir.IsDirectory());
			IList<ExtendedBlock> blocks2Remove = new AList<ExtendedBlock>();
			IList<int> removedIndex = new AList<int>();
			IList<LocatedBlock> lBlocks = cluster.GetNameNodeRpc().GetBlockLocations(filePath
				.ToString(), FileStart, FileSize).GetLocatedBlocks();
			while (removedIndex.Count != 2)
			{
				int newRemoveIndex = rand.Next(lBlocks.Count);
				if (!removedIndex.Contains(newRemoveIndex))
				{
					removedIndex.AddItem(newRemoveIndex);
				}
			}
			foreach (int aRemovedIndex in removedIndex)
			{
				blocks2Remove.AddItem(lBlocks[aRemovedIndex].GetBlock());
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Number of blocks allocated " + lBlocks.Count);
			}
			DataNode dn0 = cluster.GetDataNodes()[DnN0];
			foreach (ExtendedBlock b in blocks2Remove)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Removing the block " + b.GetBlockName());
				}
				foreach (FilePath f in FindAllFiles(dataDir, new BlockReportTestBase.MyFileFilter
					(this, b.GetBlockName(), true)))
				{
					DataNodeTestUtils.GetFSDataset(dn0).UnfinalizeBlock(b);
					if (!f.Delete())
					{
						Log.Warn("Couldn't delete " + b.GetBlockName());
					}
					else
					{
						Log.Debug("Deleted file " + f.ToString());
					}
				}
			}
			WaitTil(TimeUnit.Seconds.ToMillis(DnRescanExtraWait));
			// all blocks belong to the same file, hence same BP
			string poolId = cluster.GetNamesystem().GetBlockPoolId();
			DatanodeRegistration dnR = dn0.GetDNRegistrationForBP(poolId);
			StorageBlockReport[] reports = GetBlockReports(dn0, poolId, false, false);
			SendBlockReports(dnR, poolId, reports);
			BlockManagerTestUtil.GetComputedDatanodeWork(cluster.GetNamesystem().GetBlockManager
				());
			PrintStats();
			NUnit.Framework.Assert.AreEqual("Wrong number of MissingBlocks is found", blocks2Remove
				.Count, cluster.GetNamesystem().GetMissingBlocksCount());
			NUnit.Framework.Assert.AreEqual("Wrong number of UnderReplicatedBlocks is found", 
				blocks2Remove.Count, cluster.GetNamesystem().GetUnderReplicatedBlocks());
		}

		/// <summary>Test writes a file and closes it.</summary>
		/// <remarks>
		/// Test writes a file and closes it.
		/// Block reported is generated with a bad GS for a single block.
		/// Block report is forced and the check for # of corrupted blocks is performed.
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		public virtual void BlockReport_03()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path filePath = new Path("/" + MethodName + ".dat");
			WriteFile(MethodName, FileSize, filePath);
			// all blocks belong to the same file, hence same BP
			DataNode dn = cluster.GetDataNodes()[DnN0];
			string poolId = cluster.GetNamesystem().GetBlockPoolId();
			DatanodeRegistration dnR = dn.GetDNRegistrationForBP(poolId);
			StorageBlockReport[] reports = GetBlockReports(dn, poolId, true, false);
			SendBlockReports(dnR, poolId, reports);
			PrintStats();
			Assert.AssertThat("Wrong number of corrupt blocks", cluster.GetNamesystem().GetCorruptReplicaBlocks
				(), IS.Is(1L));
			Assert.AssertThat("Wrong number of PendingDeletion blocks", cluster.GetNamesystem
				().GetPendingDeletionBlocks(), IS.Is(0L));
		}

		/// <summary>Test writes a file and closes it.</summary>
		/// <remarks>
		/// Test writes a file and closes it.
		/// Block reported is generated with an extra block.
		/// Block report is forced and the check for # of pendingdeletion
		/// blocks is performed.
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		public virtual void BlockReport_04()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path filePath = new Path("/" + MethodName + ".dat");
			DFSTestUtil.CreateFile(fs, filePath, FileSize, ReplFactor, rand.NextLong());
			DataNode dn = cluster.GetDataNodes()[DnN0];
			// all blocks belong to the same file, hence same BP
			string poolId = cluster.GetNamesystem().GetBlockPoolId();
			// Create a bogus new block which will not be present on the namenode.
			ExtendedBlock b = new ExtendedBlock(poolId, rand.NextLong(), 1024L, rand.NextLong
				());
			dn.GetFSDataset().CreateRbw(StorageType.Default, b, false);
			DatanodeRegistration dnR = dn.GetDNRegistrationForBP(poolId);
			StorageBlockReport[] reports = GetBlockReports(dn, poolId, false, false);
			SendBlockReports(dnR, poolId, reports);
			PrintStats();
			Assert.AssertThat("Wrong number of corrupt blocks", cluster.GetNamesystem().GetCorruptReplicaBlocks
				(), IS.Is(0L));
			Assert.AssertThat("Wrong number of PendingDeletion blocks", cluster.GetNamesystem
				().GetPendingDeletionBlocks(), IS.Is(1L));
		}

		/// <summary>Test creates a file and closes it.</summary>
		/// <remarks>
		/// Test creates a file and closes it.
		/// The second datanode is started in the cluster.
		/// As soon as the replication process is completed test runs
		/// Block report and checks that no underreplicated blocks are left
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		/// <exception cref="System.Exception"/>
		public virtual void BlockReport_06()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path filePath = new Path("/" + MethodName + ".dat");
			int DnN1 = DnN0 + 1;
			WriteFile(MethodName, FileSize, filePath);
			StartDNandWait(filePath, true);
			// all blocks belong to the same file, hence same BP
			DataNode dn = cluster.GetDataNodes()[DnN1];
			string poolId = cluster.GetNamesystem().GetBlockPoolId();
			DatanodeRegistration dnR = dn.GetDNRegistrationForBP(poolId);
			StorageBlockReport[] reports = GetBlockReports(dn, poolId, false, false);
			SendBlockReports(dnR, poolId, reports);
			PrintStats();
			NUnit.Framework.Assert.AreEqual("Wrong number of PendingReplication Blocks", 0, cluster
				.GetNamesystem().GetUnderReplicatedBlocks());
		}

		/// <summary>
		/// Similar to BlockReport_03() but works with two DNs
		/// Test writes a file and closes it.
		/// </summary>
		/// <remarks>
		/// Similar to BlockReport_03() but works with two DNs
		/// Test writes a file and closes it.
		/// The second datanode is started in the cluster.
		/// As soon as the replication process is completed test finds a block from
		/// the second DN and sets its GS to be &lt; of original one.
		/// this is the markBlockAsCorrupt case 3 so we expect one pending deletion
		/// Block report is forced and the check for # of currupted blocks is performed.
		/// Another block is chosen and its length is set to a lesser than original.
		/// A check for another corrupted block is performed after yet another
		/// BlockReport
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		/// <exception cref="System.Exception"/>
		public virtual void BlockReport_07()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path filePath = new Path("/" + MethodName + ".dat");
			int DnN1 = DnN0 + 1;
			// write file and start second node to be "older" than the original
			WriteFile(MethodName, FileSize, filePath);
			StartDNandWait(filePath, true);
			// all blocks belong to the same file, hence same BP
			DataNode dn = cluster.GetDataNodes()[DnN1];
			string poolId = cluster.GetNamesystem().GetBlockPoolId();
			DatanodeRegistration dnR = dn.GetDNRegistrationForBP(poolId);
			StorageBlockReport[] reports = GetBlockReports(dn, poolId, true, false);
			SendBlockReports(dnR, poolId, reports);
			PrintStats();
			Assert.AssertThat("Wrong number of corrupt blocks", cluster.GetNamesystem().GetCorruptReplicaBlocks
				(), IS.Is(0L));
			Assert.AssertThat("Wrong number of PendingDeletion blocks", cluster.GetNamesystem
				().GetPendingDeletionBlocks(), IS.Is(1L));
			Assert.AssertThat("Wrong number of PendingReplication blocks", cluster.GetNamesystem
				().GetPendingReplicationBlocks(), IS.Is(0L));
			reports = GetBlockReports(dn, poolId, false, true);
			SendBlockReports(dnR, poolId, reports);
			PrintStats();
			Assert.AssertThat("Wrong number of corrupt blocks", cluster.GetNamesystem().GetCorruptReplicaBlocks
				(), IS.Is(1L));
			Assert.AssertThat("Wrong number of PendingDeletion blocks", cluster.GetNamesystem
				().GetPendingDeletionBlocks(), IS.Is(1L));
			Assert.AssertThat("Wrong number of PendingReplication blocks", cluster.GetNamesystem
				().GetPendingReplicationBlocks(), IS.Is(0L));
			PrintStats();
		}

		/// <summary>
		/// The test set the configuration parameters for a large block size and
		/// restarts initiated single-node cluster.
		/// </summary>
		/// <remarks>
		/// The test set the configuration parameters for a large block size and
		/// restarts initiated single-node cluster.
		/// Then it writes a file &gt; block_size and closes it.
		/// The second datanode is started in the cluster.
		/// As soon as the replication process is started and at least one TEMPORARY
		/// replica is found test forces BlockReport process and checks
		/// if the TEMPORARY replica isn't reported on it.
		/// Eventually, the configuration is being restored into the original state.
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		public virtual void BlockReport_08()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path filePath = new Path("/" + MethodName + ".dat");
			int DnN1 = DnN0 + 1;
			int bytesChkSum = 1024 * 1000;
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, bytesChkSum);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 6 * bytesChkSum);
			ShutDownCluster();
			StartUpCluster();
			try
			{
				AList<Block> blocks = WriteFile(MethodName, 12 * bytesChkSum, filePath);
				Block bl = FindBlock(filePath, 12 * bytesChkSum);
				BlockReportTestBase.BlockChecker bc = new BlockReportTestBase.BlockChecker(this, 
					filePath);
				bc.Start();
				WaitForTempReplica(bl, DnN1);
				// all blocks belong to the same file, hence same BP
				DataNode dn = cluster.GetDataNodes()[DnN1];
				string poolId = cluster.GetNamesystem().GetBlockPoolId();
				DatanodeRegistration dnR = dn.GetDNRegistrationForBP(poolId);
				StorageBlockReport[] reports = GetBlockReports(dn, poolId, false, false);
				SendBlockReports(dnR, poolId, reports);
				PrintStats();
				NUnit.Framework.Assert.AreEqual("Wrong number of PendingReplication blocks", blocks
					.Count, cluster.GetNamesystem().GetPendingReplicationBlocks());
				try
				{
					bc.Join();
				}
				catch (Exception)
				{
				}
			}
			finally
			{
				ResetConfiguration();
			}
		}

		// return the initial state of the configuration
		// Similar to BlockReport_08 but corrupts GS and len of the TEMPORARY's
		// replica block. Expect the same behaviour: NN should simply ignore this
		// block
		/// <exception cref="System.IO.IOException"/>
		public virtual void BlockReport_09()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path filePath = new Path("/" + MethodName + ".dat");
			int DnN1 = DnN0 + 1;
			int bytesChkSum = 1024 * 1000;
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, bytesChkSum);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 6 * bytesChkSum);
			ShutDownCluster();
			StartUpCluster();
			// write file and start second node to be "older" than the original
			try
			{
				WriteFile(MethodName, 12 * bytesChkSum, filePath);
				Block bl = FindBlock(filePath, 12 * bytesChkSum);
				BlockReportTestBase.BlockChecker bc = new BlockReportTestBase.BlockChecker(this, 
					filePath);
				bc.Start();
				WaitForTempReplica(bl, DnN1);
				// all blocks belong to the same file, hence same BP
				DataNode dn = cluster.GetDataNodes()[DnN1];
				string poolId = cluster.GetNamesystem().GetBlockPoolId();
				DatanodeRegistration dnR = dn.GetDNRegistrationForBP(poolId);
				StorageBlockReport[] reports = GetBlockReports(dn, poolId, true, true);
				SendBlockReports(dnR, poolId, reports);
				PrintStats();
				NUnit.Framework.Assert.AreEqual("Wrong number of PendingReplication blocks", 2, cluster
					.GetNamesystem().GetPendingReplicationBlocks());
				try
				{
					bc.Join();
				}
				catch (Exception)
				{
				}
			}
			finally
			{
				ResetConfiguration();
			}
		}

		// return the initial state of the configuration
		/// <summary>
		/// Test for the case where one of the DNs in the pipeline is in the
		/// process of doing a block report exactly when the block is closed.
		/// </summary>
		/// <remarks>
		/// Test for the case where one of the DNs in the pipeline is in the
		/// process of doing a block report exactly when the block is closed.
		/// In this case, the block report becomes delayed until after the
		/// block is marked completed on the NN, and hence it reports an RBW
		/// replica for a COMPLETE block. Such a report should not be marked
		/// corrupt.
		/// This is a regression test for HDFS-2791.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestOneReplicaRbwReportArrivesAfterBlockCompleted()
		{
			CountDownLatch brFinished = new CountDownLatch(1);
			GenericTestUtils.DelayAnswer delayer = new _DelayAnswer_579(brFinished, Log);
			// inform the test that our block report went through.
			string MethodName = GenericTestUtils.GetMethodName();
			Path filePath = new Path("/" + MethodName + ".dat");
			// Start a second DN for this test -- we're checking
			// what happens when one of the DNs is slowed for some reason.
			ReplFactor = 2;
			StartDNandWait(null, false);
			NameNode nn = cluster.GetNameNode();
			FSDataOutputStream @out = fs.Create(filePath, ReplFactor);
			try
			{
				AppendTestUtil.Write(@out, 0, 10);
				@out.Hflush();
				// Set up a spy so that we can delay the block report coming
				// from this node.
				DataNode dn = cluster.GetDataNodes()[0];
				DatanodeProtocolClientSideTranslatorPB spy = DataNodeTestUtils.SpyOnBposToNN(dn, 
					nn);
				Org.Mockito.Mockito.DoAnswer(delayer).When(spy).BlockReport(Org.Mockito.Mockito.AnyObject
					<DatanodeRegistration>(), Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito.AnyObject
					<StorageBlockReport[]>(), Org.Mockito.Mockito.AnyObject<BlockReportContext>());
				// Force a block report to be generated. The block report will have
				// an RBW replica in it. Wait for the RPC to be sent, but block
				// it before it gets to the NN.
				dn.ScheduleAllBlockReport(0);
				delayer.WaitForCall();
			}
			finally
			{
				IOUtils.CloseStream(@out);
			}
			// Now that the stream is closed, the NN will have the block in COMPLETE
			// state.
			delayer.Proceed();
			brFinished.Await();
			// Verify that no replicas are marked corrupt, and that the
			// file is still readable.
			BlockManagerTestUtil.UpdateState(nn.GetNamesystem().GetBlockManager());
			NUnit.Framework.Assert.AreEqual(0, nn.GetNamesystem().GetCorruptReplicaBlocks());
			DFSTestUtil.ReadFile(fs, filePath);
			// Ensure that the file is readable even from the DN that we futzed with.
			cluster.StopDataNode(1);
			DFSTestUtil.ReadFile(fs, filePath);
		}

		private sealed class _DelayAnswer_579 : GenericTestUtils.DelayAnswer
		{
			public _DelayAnswer_579(CountDownLatch brFinished, Log baseArg1)
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

		/// <exception cref="System.IO.IOException"/>
		private void WaitForTempReplica(Block bl, int DnN1)
		{
			bool tooLongWait = false;
			int Timeout = 40000;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Wait for datanode " + DnN1 + " to appear");
			}
			while (cluster.GetDataNodes().Count <= DnN1)
			{
				WaitTil(20);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Total number of DNs " + cluster.GetDataNodes().Count);
			}
			cluster.WaitActive();
			// Look about specified DN for the replica of the block from 1st DN
			DataNode dn1 = cluster.GetDataNodes()[DnN1];
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			Replica r = DataNodeTestUtils.FetchReplicaInfo(dn1, bpid, bl.GetBlockId());
			long start = Time.MonotonicNow();
			int count = 0;
			while (r == null)
			{
				WaitTil(5);
				r = DataNodeTestUtils.FetchReplicaInfo(dn1, bpid, bl.GetBlockId());
				long waiting_period = Time.MonotonicNow() - start;
				if (count++ % 100 == 0)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Has been waiting for " + waiting_period + " ms.");
					}
				}
				if (waiting_period > Timeout)
				{
					NUnit.Framework.Assert.IsTrue("Was waiting too long to get ReplicaInfo from a datanode"
						, tooLongWait);
				}
			}
			HdfsServerConstants.ReplicaState state = r.GetState();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Replica state before the loop " + state.GetValue());
			}
			start = Time.MonotonicNow();
			while (state != HdfsServerConstants.ReplicaState.Temporary)
			{
				WaitTil(5);
				state = r.GetState();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Keep waiting for " + bl.GetBlockName() + " is in state " + state.GetValue
						());
				}
				if (Time.MonotonicNow() - start > Timeout)
				{
					NUnit.Framework.Assert.IsTrue("Was waiting too long for a replica to become TEMPORARY"
						, tooLongWait);
				}
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Replica state after the loop " + state.GetValue());
			}
		}

		// Helper methods from here below...
		// Write file and start second data node.
		private AList<Block> WriteFile(string MethodName, long fileSize, Path filePath)
		{
			AList<Block> blocks = null;
			try
			{
				ReplFactor = 2;
				blocks = PrepareForRide(filePath, MethodName, fileSize);
			}
			catch (IOException e)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Caught exception ", e);
				}
			}
			return blocks;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private void StartDNandWait(Path filePath, bool waitReplicas)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Before next DN start: " + cluster.GetDataNodes().Count);
			}
			cluster.StartDataNodes(conf, 1, true, null, null);
			cluster.WaitClusterUp();
			AList<DataNode> datanodes = cluster.GetDataNodes();
			NUnit.Framework.Assert.AreEqual(datanodes.Count, 2);
			if (Log.IsDebugEnabled())
			{
				int lastDn = datanodes.Count - 1;
				Log.Debug("New datanode " + cluster.GetDataNodes()[lastDn].GetDisplayName() + " has been started"
					);
			}
			if (waitReplicas)
			{
				DFSTestUtil.WaitReplication(fs, filePath, ReplFactor);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private AList<Block> PrepareForRide(Path filePath, string MethodName, long fileSize
			)
		{
			Log.Info("Running test " + MethodName);
			DFSTestUtil.CreateFile(fs, filePath, fileSize, ReplFactor, rand.NextLong());
			return LocatedToBlocks(cluster.GetNameNodeRpc().GetBlockLocations(filePath.ToString
				(), FileStart, fileSize).GetLocatedBlocks(), null);
		}

		private void PrintStats()
		{
			BlockManagerTestUtil.UpdateState(cluster.GetNamesystem().GetBlockManager());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Missing " + cluster.GetNamesystem().GetMissingBlocksCount());
				Log.Debug("Corrupted " + cluster.GetNamesystem().GetCorruptReplicaBlocks());
				Log.Debug("Under-replicated " + cluster.GetNamesystem().GetUnderReplicatedBlocks(
					));
				Log.Debug("Pending delete " + cluster.GetNamesystem().GetPendingDeletionBlocks());
				Log.Debug("Pending replications " + cluster.GetNamesystem().GetPendingReplicationBlocks
					());
				Log.Debug("Excess " + cluster.GetNamesystem().GetExcessBlocks());
				Log.Debug("Total " + cluster.GetNamesystem().GetBlocksTotal());
			}
		}

		private AList<Block> LocatedToBlocks(IList<LocatedBlock> locatedBlks, IList<int> 
			positionsToRemove)
		{
			AList<Block> newList = new AList<Block>();
			for (int i = 0; i < locatedBlks.Count; i++)
			{
				if (positionsToRemove != null && positionsToRemove.Contains(i))
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(i + " block to be omitted");
					}
					continue;
				}
				newList.AddItem(new Block(locatedBlks[i].GetBlock().GetLocalBlock()));
			}
			return newList;
		}

		private void WaitTil(long waitPeriod)
		{
			try
			{
				//Wait til next re-scan
				Sharpen.Thread.Sleep(waitPeriod);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
		}

		private IList<FilePath> FindAllFiles(FilePath top, FilenameFilter mask)
		{
			if (top == null)
			{
				return null;
			}
			AList<FilePath> ret = new AList<FilePath>();
			foreach (FilePath f in top.ListFiles())
			{
				if (f.IsDirectory())
				{
					Sharpen.Collections.AddAll(ret, FindAllFiles(f, mask));
				}
				else
				{
					if (mask.Accept(f, f.GetName()))
					{
						ret.AddItem(f);
					}
				}
			}
			return ret;
		}

		private class MyFileFilter : FilenameFilter
		{
			private string nameToAccept = string.Empty;

			private bool all = false;

			public MyFileFilter(BlockReportTestBase _enclosing, string nameToAccept, bool all
				)
			{
				this._enclosing = _enclosing;
				if (nameToAccept == null)
				{
					throw new ArgumentException("Argument isn't suppose to be null");
				}
				this.nameToAccept = nameToAccept;
				this.all = all;
			}

			public virtual bool Accept(FilePath file, string s)
			{
				if (this.all)
				{
					return s != null && s.StartsWith(this.nameToAccept);
				}
				else
				{
					return s != null && s.Equals(this.nameToAccept);
				}
			}

			private readonly BlockReportTestBase _enclosing;
		}

		private static void InitLoggers()
		{
			DFSTestUtil.SetNameNodeLogLevel(Level.All);
			GenericTestUtils.SetLogLevel(DataNode.Log, Level.All);
			GenericTestUtils.SetLogLevel(BlockReportTestBase.Log, Level.All);
		}

		/// <exception cref="System.IO.IOException"/>
		private Block FindBlock(Path path, long size)
		{
			Block ret;
			IList<LocatedBlock> lbs = cluster.GetNameNodeRpc().GetBlockLocations(path.ToString
				(), FileStart, size).GetLocatedBlocks();
			LocatedBlock lb = lbs[lbs.Count - 1];
			// Get block from the first DN
			ret = cluster.GetDataNodes()[DnN0].data.GetStoredBlock(lb.GetBlock().GetBlockPoolId
				(), lb.GetBlock().GetBlockId());
			return ret;
		}

		private class BlockChecker : Sharpen.Thread
		{
			internal readonly Path filePath;

			public BlockChecker(BlockReportTestBase _enclosing, Path filePath)
			{
				this._enclosing = _enclosing;
				this.filePath = filePath;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.StartDNandWait(this.filePath, true);
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
					NUnit.Framework.Assert.Fail("Failed to start BlockChecker: " + e);
				}
			}

			private readonly BlockReportTestBase _enclosing;
		}
	}
}
