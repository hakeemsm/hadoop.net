using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This tests if sync all replicas in block recovery works correctly</summary>
	public class TestBlockRecovery
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestBlockRecovery));

		private static readonly string DataDir = MiniDFSCluster.GetBaseDirectory() + "data";

		private DataNode dn;

		private Configuration conf;

		private const long RecoveryId = 3000L;

		private const string ClusterId = "testClusterID";

		private const string PoolId = "BP-TEST";

		private static readonly IPEndPoint NnAddr = new IPEndPoint("localhost", 5020);

		private const long BlockId = 1000L;

		private const long GenStamp = 2000L;

		private const long BlockLen = 3000L;

		private const long ReplicaLen1 = 6000L;

		private const long ReplicaLen2 = 5000L;

		private static readonly ExtendedBlock block = new ExtendedBlock(PoolId, BlockId, 
			BlockLen, GenStamp);

		static TestBlockRecovery()
		{
			((Log4JLogger)LogFactory.GetLog(typeof(FSNamesystem))).GetLogger().SetLevel(Level
				.All);
			((Log4JLogger)Log).GetLogger().SetLevel(Level.All);
		}

		/// <summary>Starts an instance of DataNode</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[SetUp]
		public virtual void StartUp()
		{
			conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, DataDir);
			conf.Set(DFSConfigKeys.DfsDatanodeAddressKey, "0.0.0.0:0");
			conf.Set(DFSConfigKeys.DfsDatanodeHttpAddressKey, "0.0.0.0:0");
			conf.Set(DFSConfigKeys.DfsDatanodeIpcAddressKey, "0.0.0.0:0");
			conf.SetInt(CommonConfigurationKeys.IpcClientConnectMaxRetriesKey, 0);
			FileSystem.SetDefaultUri(conf, "hdfs://" + NnAddr.GetHostName() + ":" + NnAddr.Port
				);
			AList<StorageLocation> locations = new AList<StorageLocation>();
			FilePath dataDir = new FilePath(DataDir);
			FileUtil.FullyDelete(dataDir);
			dataDir.Mkdirs();
			StorageLocation location = StorageLocation.Parse(dataDir.GetPath());
			locations.AddItem(location);
			DatanodeProtocolClientSideTranslatorPB namenode = Org.Mockito.Mockito.Mock<DatanodeProtocolClientSideTranslatorPB
				>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_143()).When(namenode).RegisterDatanode(Org.Mockito.Mockito
				.Any<DatanodeRegistration>());
			Org.Mockito.Mockito.When(namenode.VersionRequest()).ThenReturn(new NamespaceInfo(
				1, ClusterId, PoolId, 1L));
			Org.Mockito.Mockito.When(namenode.SendHeartbeat(Org.Mockito.Mockito.Any<DatanodeRegistration
				>(), Org.Mockito.Mockito.Any<StorageReport[]>(), Org.Mockito.Mockito.AnyLong(), 
				Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito
				.AnyInt(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito.Any<VolumeFailureSummary
				>())).ThenReturn(new HeartbeatResponse(new DatanodeCommand[0], new NNHAStatusHeartbeat
				(HAServiceProtocol.HAServiceState.Active, 1), null));
			dn = new _DataNode_169(namenode, conf, locations, null);
			// Trigger a heartbeat so that it acknowledges the NN as active.
			dn.GetAllBpOs()[0].TriggerHeartbeatForTests();
		}

		private sealed class _Answer_143 : Answer<DatanodeRegistration>
		{
			public _Answer_143()
			{
			}

			/// <exception cref="System.Exception"/>
			public DatanodeRegistration Answer(InvocationOnMock invocation)
			{
				return (DatanodeRegistration)invocation.GetArguments()[0];
			}
		}

		private sealed class _DataNode_169 : DataNode
		{
			public _DataNode_169(DatanodeProtocolClientSideTranslatorPB namenode, Configuration
				 baseArg1, IList<StorageLocation> baseArg2, SecureDataNodeStarter.SecureResources
				 baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
				this.namenode = namenode;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override DatanodeProtocolClientSideTranslatorPB ConnectToNN(IPEndPoint nnAddr
				)
			{
				NUnit.Framework.Assert.AreEqual(TestBlockRecovery.NnAddr, nnAddr);
				return namenode;
			}

			private readonly DatanodeProtocolClientSideTranslatorPB namenode;
		}

		/// <summary>Cleans the resources and closes the instance of datanode</summary>
		/// <exception cref="System.IO.IOException">if an error occurred</exception>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (dn != null)
			{
				try
				{
					dn.Shutdown();
				}
				catch (Exception e)
				{
					Log.Error("Cannot close: ", e);
				}
				finally
				{
					FilePath dir = new FilePath(DataDir);
					if (dir.Exists())
					{
						NUnit.Framework.Assert.IsTrue("Cannot delete data-node dirs", FileUtil.FullyDelete
							(dir));
					}
				}
			}
		}

		/// <summary>Sync two replicas</summary>
		/// <exception cref="System.IO.IOException"/>
		private void TestSyncReplicas(ReplicaRecoveryInfo replica1, ReplicaRecoveryInfo replica2
			, InterDatanodeProtocol dn1, InterDatanodeProtocol dn2, long expectLen)
		{
			DatanodeInfo[] locs = new DatanodeInfo[] { Org.Mockito.Mockito.Mock<DatanodeInfo>
				(), Org.Mockito.Mockito.Mock<DatanodeInfo>() };
			BlockRecoveryCommand.RecoveringBlock rBlock = new BlockRecoveryCommand.RecoveringBlock
				(block, locs, RecoveryId);
			AList<DataNode.BlockRecord> syncList = new AList<DataNode.BlockRecord>(2);
			DataNode.BlockRecord record1 = new DataNode.BlockRecord(DFSTestUtil.GetDatanodeInfo
				("1.2.3.4", "bogus", 1234), dn1, replica1);
			DataNode.BlockRecord record2 = new DataNode.BlockRecord(DFSTestUtil.GetDatanodeInfo
				("1.2.3.4", "bogus", 1234), dn2, replica2);
			syncList.AddItem(record1);
			syncList.AddItem(record2);
			Org.Mockito.Mockito.When(dn1.UpdateReplicaUnderRecovery((ExtendedBlock)Matchers.AnyObject
				(), Matchers.AnyLong(), Matchers.AnyLong(), Matchers.AnyLong())).ThenReturn("storage1"
				);
			Org.Mockito.Mockito.When(dn2.UpdateReplicaUnderRecovery((ExtendedBlock)Matchers.AnyObject
				(), Matchers.AnyLong(), Matchers.AnyLong(), Matchers.AnyLong())).ThenReturn("storage2"
				);
			dn.SyncBlock(rBlock, syncList);
		}

		/// <summary>BlockRecovery_02.8.</summary>
		/// <remarks>
		/// BlockRecovery_02.8.
		/// Two replicas are in Finalized state
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		[NUnit.Framework.Test]
		public virtual void TestFinalizedReplicas()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp
				 - 1, HdfsServerConstants.ReplicaState.Finalized);
			ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp
				 - 2, HdfsServerConstants.ReplicaState.Finalized);
			InterDatanodeProtocol dn1 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			InterDatanodeProtocol dn2 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			TestSyncReplicas(replica1, replica2, dn1, dn2, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn1).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn2).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, ReplicaLen1);
			// two finalized replicas have different length
			replica1 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp - 1, HdfsServerConstants.ReplicaState
				.Finalized);
			replica2 = new ReplicaRecoveryInfo(BlockId, ReplicaLen2, GenStamp - 2, HdfsServerConstants.ReplicaState
				.Finalized);
			try
			{
				TestSyncReplicas(replica1, replica2, dn1, dn2, ReplicaLen1);
				NUnit.Framework.Assert.Fail("Two finalized replicas should not have different lengthes!"
					);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("Inconsistent size of finalized replicas. "
					));
			}
		}

		/// <summary>BlockRecovery_02.9.</summary>
		/// <remarks>
		/// BlockRecovery_02.9.
		/// One replica is Finalized and another is RBW.
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		[NUnit.Framework.Test]
		public virtual void TestFinalizedRbwReplicas()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			// rbw and finalized replicas have the same length
			ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp
				 - 1, HdfsServerConstants.ReplicaState.Finalized);
			ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp
				 - 2, HdfsServerConstants.ReplicaState.Rbw);
			InterDatanodeProtocol dn1 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			InterDatanodeProtocol dn2 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			TestSyncReplicas(replica1, replica2, dn1, dn2, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn1).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn2).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, ReplicaLen1);
			// rbw replica has a different length from the finalized one
			replica1 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp - 1, HdfsServerConstants.ReplicaState
				.Finalized);
			replica2 = new ReplicaRecoveryInfo(BlockId, ReplicaLen2, GenStamp - 2, HdfsServerConstants.ReplicaState
				.Rbw);
			dn1 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			dn2 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			TestSyncReplicas(replica1, replica2, dn1, dn2, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn1).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn2, Org.Mockito.Mockito.Never()).UpdateReplicaUnderRecovery
				(block, RecoveryId, BlockId, ReplicaLen1);
		}

		/// <summary>BlockRecovery_02.10.</summary>
		/// <remarks>
		/// BlockRecovery_02.10.
		/// One replica is Finalized and another is RWR.
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		[NUnit.Framework.Test]
		public virtual void TestFinalizedRwrReplicas()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			// rbw and finalized replicas have the same length
			ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp
				 - 1, HdfsServerConstants.ReplicaState.Finalized);
			ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp
				 - 2, HdfsServerConstants.ReplicaState.Rwr);
			InterDatanodeProtocol dn1 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			InterDatanodeProtocol dn2 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			TestSyncReplicas(replica1, replica2, dn1, dn2, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn1).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn2, Org.Mockito.Mockito.Never()).UpdateReplicaUnderRecovery
				(block, RecoveryId, BlockId, ReplicaLen1);
			// rbw replica has a different length from the finalized one
			replica1 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp - 1, HdfsServerConstants.ReplicaState
				.Finalized);
			replica2 = new ReplicaRecoveryInfo(BlockId, ReplicaLen2, GenStamp - 2, HdfsServerConstants.ReplicaState
				.Rbw);
			dn1 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			dn2 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			TestSyncReplicas(replica1, replica2, dn1, dn2, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn1).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn2, Org.Mockito.Mockito.Never()).UpdateReplicaUnderRecovery
				(block, RecoveryId, BlockId, ReplicaLen1);
		}

		/// <summary>BlockRecovery_02.11.</summary>
		/// <remarks>
		/// BlockRecovery_02.11.
		/// Two replicas are RBW.
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		[NUnit.Framework.Test]
		public virtual void TestRBWReplicas()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp
				 - 1, HdfsServerConstants.ReplicaState.Rbw);
			ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BlockId, ReplicaLen2, GenStamp
				 - 2, HdfsServerConstants.ReplicaState.Rbw);
			InterDatanodeProtocol dn1 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			InterDatanodeProtocol dn2 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			long minLen = Math.Min(ReplicaLen1, ReplicaLen2);
			TestSyncReplicas(replica1, replica2, dn1, dn2, minLen);
			Org.Mockito.Mockito.Verify(dn1).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, minLen);
			Org.Mockito.Mockito.Verify(dn2).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, minLen);
		}

		/// <summary>BlockRecovery_02.12.</summary>
		/// <remarks>
		/// BlockRecovery_02.12.
		/// One replica is RBW and another is RWR.
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		[NUnit.Framework.Test]
		public virtual void TestRBW_RWRReplicas()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp
				 - 1, HdfsServerConstants.ReplicaState.Rbw);
			ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp
				 - 2, HdfsServerConstants.ReplicaState.Rwr);
			InterDatanodeProtocol dn1 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			InterDatanodeProtocol dn2 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			TestSyncReplicas(replica1, replica2, dn1, dn2, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn1).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, ReplicaLen1);
			Org.Mockito.Mockito.Verify(dn2, Org.Mockito.Mockito.Never()).UpdateReplicaUnderRecovery
				(block, RecoveryId, BlockId, ReplicaLen1);
		}

		/// <summary>BlockRecovery_02.13.</summary>
		/// <remarks>
		/// BlockRecovery_02.13.
		/// Two replicas are RWR.
		/// </remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		[NUnit.Framework.Test]
		public virtual void TestRWRReplicas()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			ReplicaRecoveryInfo replica1 = new ReplicaRecoveryInfo(BlockId, ReplicaLen1, GenStamp
				 - 1, HdfsServerConstants.ReplicaState.Rwr);
			ReplicaRecoveryInfo replica2 = new ReplicaRecoveryInfo(BlockId, ReplicaLen2, GenStamp
				 - 2, HdfsServerConstants.ReplicaState.Rwr);
			InterDatanodeProtocol dn1 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			InterDatanodeProtocol dn2 = Org.Mockito.Mockito.Mock<InterDatanodeProtocol>();
			long minLen = Math.Min(ReplicaLen1, ReplicaLen2);
			TestSyncReplicas(replica1, replica2, dn1, dn2, minLen);
			Org.Mockito.Mockito.Verify(dn1).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, minLen);
			Org.Mockito.Mockito.Verify(dn2).UpdateReplicaUnderRecovery(block, RecoveryId, BlockId
				, minLen);
		}

		/// <exception cref="System.IO.IOException"/>
		private ICollection<BlockRecoveryCommand.RecoveringBlock> InitRecoveringBlocks()
		{
			ICollection<BlockRecoveryCommand.RecoveringBlock> blocks = new AList<BlockRecoveryCommand.RecoveringBlock
				>(1);
			DatanodeInfo mockOtherDN = DFSTestUtil.GetLocalDatanodeInfo();
			DatanodeInfo[] locs = new DatanodeInfo[] { new DatanodeInfo(dn.GetDNRegistrationForBP
				(block.GetBlockPoolId())), mockOtherDN };
			BlockRecoveryCommand.RecoveringBlock rBlock = new BlockRecoveryCommand.RecoveringBlock
				(block, locs, RecoveryId);
			blocks.AddItem(rBlock);
			return blocks;
		}

		/// <summary>BlockRecoveryFI_05.</summary>
		/// <remarks>BlockRecoveryFI_05. One DN throws RecoveryInProgressException.</remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoveryInProgressException()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			DataNode spyDN = Org.Mockito.Mockito.Spy(dn);
			Org.Mockito.Mockito.DoThrow(new RecoveryInProgressException("Replica recovery is in progress"
				)).When(spyDN).InitReplicaRecovery(Matchers.Any<BlockRecoveryCommand.RecoveringBlock
				>());
			Daemon d = spyDN.RecoverBlocks("fake NN", InitRecoveringBlocks());
			d.Join();
			Org.Mockito.Mockito.Verify(spyDN, Org.Mockito.Mockito.Never()).SyncBlock(Matchers.Any
				<BlockRecoveryCommand.RecoveringBlock>(), Matchers.AnyListOf<DataNode.BlockRecord
				>());
		}

		/// <summary>BlockRecoveryFI_06.</summary>
		/// <remarks>BlockRecoveryFI_06. all datanodes throws an exception.</remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestErrorReplicas()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			DataNode spyDN = Org.Mockito.Mockito.Spy(dn);
			Org.Mockito.Mockito.DoThrow(new IOException()).When(spyDN).InitReplicaRecovery(Matchers.Any
				<BlockRecoveryCommand.RecoveringBlock>());
			Daemon d = spyDN.RecoverBlocks("fake NN", InitRecoveringBlocks());
			d.Join();
			Org.Mockito.Mockito.Verify(spyDN, Org.Mockito.Mockito.Never()).SyncBlock(Matchers.Any
				<BlockRecoveryCommand.RecoveringBlock>(), Matchers.AnyListOf<DataNode.BlockRecord
				>());
		}

		/// <summary>BlockRecoveryFI_07.</summary>
		/// <remarks>BlockRecoveryFI_07. max replica length from all DNs is zero.</remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZeroLenReplicas()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			DataNode spyDN = Org.Mockito.Mockito.Spy(dn);
			Org.Mockito.Mockito.DoReturn(new ReplicaRecoveryInfo(block.GetBlockId(), 0, block
				.GetGenerationStamp(), HdfsServerConstants.ReplicaState.Finalized)).When(spyDN).
				InitReplicaRecovery(Matchers.Any<BlockRecoveryCommand.RecoveringBlock>());
			Daemon d = spyDN.RecoverBlocks("fake NN", InitRecoveringBlocks());
			d.Join();
			DatanodeProtocol dnP = dn.GetActiveNamenodeForBP(PoolId);
			Org.Mockito.Mockito.Verify(dnP).CommitBlockSynchronization(block, RecoveryId, 0, 
				true, true, DatanodeID.EmptyArray, null);
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<DataNode.BlockRecord> InitBlockRecords(DataNode spyDN)
		{
			IList<DataNode.BlockRecord> blocks = new AList<DataNode.BlockRecord>(1);
			DatanodeRegistration dnR = dn.GetDNRegistrationForBP(block.GetBlockPoolId());
			DataNode.BlockRecord blockRecord = new DataNode.BlockRecord(new DatanodeID(dnR), 
				spyDN, new ReplicaRecoveryInfo(block.GetBlockId(), block.GetNumBytes(), block.GetGenerationStamp
				(), HdfsServerConstants.ReplicaState.Finalized));
			blocks.AddItem(blockRecord);
			return blocks;
		}

		private static readonly BlockRecoveryCommand.RecoveringBlock rBlock = new BlockRecoveryCommand.RecoveringBlock
			(block, null, RecoveryId);

		/// <summary>BlockRecoveryFI_09.</summary>
		/// <remarks>BlockRecoveryFI_09. some/all DNs failed to update replicas.</remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		[NUnit.Framework.Test]
		public virtual void TestFailedReplicaUpdate()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			DataNode spyDN = Org.Mockito.Mockito.Spy(dn);
			Org.Mockito.Mockito.DoThrow(new IOException()).When(spyDN).UpdateReplicaUnderRecovery
				(block, RecoveryId, BlockId, block.GetNumBytes());
			try
			{
				spyDN.SyncBlock(rBlock, InitBlockRecords(spyDN));
				NUnit.Framework.Assert.Fail("Sync should fail");
			}
			catch (IOException e)
			{
				e.Message.StartsWith("Cannot recover ");
			}
		}

		/// <summary>BlockRecoveryFI_10.</summary>
		/// <remarks>BlockRecoveryFI_10. DN has no ReplicaUnderRecovery.</remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		[NUnit.Framework.Test]
		public virtual void TestNoReplicaUnderRecovery()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			dn.data.CreateRbw(StorageType.Default, block, false);
			try
			{
				dn.SyncBlock(rBlock, InitBlockRecords(dn));
				NUnit.Framework.Assert.Fail("Sync should fail");
			}
			catch (IOException e)
			{
				e.Message.StartsWith("Cannot recover ");
			}
			DatanodeProtocol namenode = dn.GetActiveNamenodeForBP(PoolId);
			Org.Mockito.Mockito.Verify(namenode, Org.Mockito.Mockito.Never()).CommitBlockSynchronization
				(Matchers.Any<ExtendedBlock>(), Matchers.AnyLong(), Matchers.AnyLong(), Matchers.AnyBoolean
				(), Matchers.AnyBoolean(), Matchers.Any<DatanodeID[]>(), Matchers.Any<string[]>(
				));
		}

		/// <summary>BlockRecoveryFI_11.</summary>
		/// <remarks>BlockRecoveryFI_11. a replica's recovery id does not match new GS.</remarks>
		/// <exception cref="System.IO.IOException">in case of an error</exception>
		[NUnit.Framework.Test]
		public virtual void TestNotMatchedReplicaID()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Running " + GenericTestUtils.GetMethodName());
			}
			ReplicaInPipelineInterface replicaInfo = dn.data.CreateRbw(StorageType.Default, block
				, false).GetReplica();
			ReplicaOutputStreams streams = null;
			try
			{
				streams = replicaInfo.CreateStreams(true, DataChecksum.NewDataChecksum(DataChecksum.Type
					.Crc32, 512));
				streams.GetChecksumOut().Write('a');
				dn.data.InitReplicaRecovery(new BlockRecoveryCommand.RecoveringBlock(block, null, 
					RecoveryId + 1));
				try
				{
					dn.SyncBlock(rBlock, InitBlockRecords(dn));
					NUnit.Framework.Assert.Fail("Sync should fail");
				}
				catch (IOException e)
				{
					e.Message.StartsWith("Cannot recover ");
				}
				DatanodeProtocol namenode = dn.GetActiveNamenodeForBP(PoolId);
				Org.Mockito.Mockito.Verify(namenode, Org.Mockito.Mockito.Never()).CommitBlockSynchronization
					(Matchers.Any<ExtendedBlock>(), Matchers.AnyLong(), Matchers.AnyLong(), Matchers.AnyBoolean
					(), Matchers.AnyBoolean(), Matchers.Any<DatanodeID[]>(), Matchers.Any<string[]>(
					));
			}
			finally
			{
				streams.Close();
			}
		}

		/// <summary>Test to verify the race between finalizeBlock and Lease recovery</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRaceBetweenReplicaRecoveryAndFinalizeBlock()
		{
			TearDown();
			// Stop the Mocked DN started in startup()
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsDatanodeXceiverStopTimeoutMillisKey, "1000");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				cluster.WaitClusterUp();
				DistributedFileSystem fs = cluster.GetFileSystem();
				Path path = new Path("/test");
				FSDataOutputStream @out = fs.Create(path);
				@out.WriteBytes("data");
				@out.Hsync();
				IList<LocatedBlock> blocks = DFSTestUtil.GetAllBlocks(fs.Open(path));
				LocatedBlock block = blocks[0];
				DataNode dataNode = cluster.GetDataNodes()[0];
				AtomicBoolean recoveryInitResult = new AtomicBoolean(true);
				Sharpen.Thread recoveryThread = new _Thread_612(block, dataNode, recoveryInitResult
					);
				recoveryThread.Start();
				try
				{
					@out.Close();
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue("Writing should fail", e.Message.Contains("are bad. Aborting..."
						));
				}
				finally
				{
					recoveryThread.Join();
				}
				NUnit.Framework.Assert.IsTrue("Recovery should be initiated successfully", recoveryInitResult
					.Get());
				dataNode.UpdateReplicaUnderRecovery(block.GetBlock(), block.GetBlock().GetGenerationStamp
					() + 1, block.GetBlock().GetBlockId(), block.GetBlockSize());
			}
			finally
			{
				if (null != cluster)
				{
					cluster.Shutdown();
					cluster = null;
				}
			}
		}

		private sealed class _Thread_612 : Sharpen.Thread
		{
			public _Thread_612(LocatedBlock block, DataNode dataNode, AtomicBoolean recoveryInitResult
				)
			{
				this.block = block;
				this.dataNode = dataNode;
				this.recoveryInitResult = recoveryInitResult;
			}

			public override void Run()
			{
				try
				{
					DatanodeInfo[] locations = block.GetLocations();
					BlockRecoveryCommand.RecoveringBlock recoveringBlock = new BlockRecoveryCommand.RecoveringBlock
						(block.GetBlock(), locations, block.GetBlock().GetGenerationStamp() + 1);
					lock (dataNode.data)
					{
						Sharpen.Thread.Sleep(2000);
						dataNode.InitReplicaRecovery(recoveringBlock);
					}
				}
				catch (Exception)
				{
					recoveryInitResult.Set(false);
				}
			}

			private readonly LocatedBlock block;

			private readonly DataNode dataNode;

			private readonly AtomicBoolean recoveryInitResult;
		}
	}
}
