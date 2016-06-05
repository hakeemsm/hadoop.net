using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Invocation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestBPOfferService
	{
		private const string FakeBpid = "fake bpid";

		private const string FakeClusterid = "fake cluster";

		protected internal static readonly Log Log = LogFactory.GetLog(typeof(TestBPOfferService
			));

		private static readonly ExtendedBlock FakeBlock = new ExtendedBlock(FakeBpid, 12345L
			);

		private static readonly FilePath TestBuildData = PathUtils.GetTestDir(typeof(TestBPOfferService
			));

		private long firstCallTime = 0;

		private long secondCallTime = 0;

		static TestBPOfferService()
		{
			((Log4JLogger)DataNode.Log).GetLogger().SetLevel(Level.All);
		}

		private DatanodeProtocolClientSideTranslatorPB mockNN1;

		private DatanodeProtocolClientSideTranslatorPB mockNN2;

		private readonly NNHAStatusHeartbeat[] mockHaStatuses = new NNHAStatusHeartbeat[2
			];

		private readonly int[] heartbeatCounts = new int[2];

		private DataNode mockDn;

		private FsDatasetSpi<object> mockFSDataset;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupMocks()
		{
			mockNN1 = SetupNNMock(0);
			mockNN2 = SetupNNMock(1);
			// Set up a mock DN with the bare-bones configuration
			// objects, etc.
			mockDn = Org.Mockito.Mockito.Mock<DataNode>();
			Org.Mockito.Mockito.DoReturn(true).When(mockDn).ShouldRun();
			Configuration conf = new Configuration();
			FilePath dnDataDir = new FilePath(new FilePath(TestBuildData, "dfs"), "data");
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dnDataDir.ToURI().ToString());
			Org.Mockito.Mockito.DoReturn(conf).When(mockDn).GetConf();
			Org.Mockito.Mockito.DoReturn(new DNConf(conf)).When(mockDn).GetDnConf();
			Org.Mockito.Mockito.DoReturn(DataNodeMetrics.Create(conf, "fake dn")).When(mockDn
				).GetMetrics();
			// Set up a simulated dataset with our fake BP
			mockFSDataset = Org.Mockito.Mockito.Spy(new SimulatedFSDataset(null, conf));
			mockFSDataset.AddBlockPool(FakeBpid, conf);
			// Wire the dataset to the DN.
			Org.Mockito.Mockito.DoReturn(mockFSDataset).When(mockDn).GetFSDataset();
		}

		/// <summary>Set up a mock NN with the bare minimum for a DN to register to it.</summary>
		/// <exception cref="System.Exception"/>
		private DatanodeProtocolClientSideTranslatorPB SetupNNMock(int nnIdx)
		{
			DatanodeProtocolClientSideTranslatorPB mock = Org.Mockito.Mockito.Mock<DatanodeProtocolClientSideTranslatorPB
				>();
			Org.Mockito.Mockito.DoReturn(new NamespaceInfo(1, FakeClusterid, FakeBpid, 0)).When
				(mock).VersionRequest();
			Org.Mockito.Mockito.DoReturn(DFSTestUtil.GetLocalDatanodeRegistration()).When(mock
				).RegisterDatanode(Org.Mockito.Mockito.Any<DatanodeRegistration>());
			Org.Mockito.Mockito.DoAnswer(new TestBPOfferService.HeartbeatAnswer(this, nnIdx))
				.When(mock).SendHeartbeat(Org.Mockito.Mockito.Any<DatanodeRegistration>(), Org.Mockito.Mockito
				.Any<StorageReport[]>(), Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito
				.AnyInt(), Org.Mockito.Mockito.Any<VolumeFailureSummary>());
			mockHaStatuses[nnIdx] = new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.
				Standby, 0);
			return mock;
		}

		/// <summary>
		/// Mock answer for heartbeats which returns an empty set of commands
		/// and the HA status for the chosen NN from the
		/// <see cref="TestBPOfferService.mockHaStatuses"/>
		/// array.
		/// </summary>
		private class HeartbeatAnswer : Org.Mockito.Stubbing.Answer<HeartbeatResponse>
		{
			private readonly int nnIdx;

			public HeartbeatAnswer(TestBPOfferService _enclosing, int nnIdx)
			{
				this._enclosing = _enclosing;
				this.nnIdx = nnIdx;
			}

			/// <exception cref="System.Exception"/>
			public virtual HeartbeatResponse Answer(InvocationOnMock invocation)
			{
				this._enclosing.heartbeatCounts[this.nnIdx]++;
				return new HeartbeatResponse(new DatanodeCommand[0], this._enclosing.mockHaStatuses
					[this.nnIdx], null);
			}

			private readonly TestBPOfferService _enclosing;
		}

		/// <summary>
		/// Test that the BPOS can register to talk to two different NNs,
		/// sends block reports to both, etc.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBasicFunctionality()
		{
			BPOfferService bpos = SetupBPOSForNNs(mockNN1, mockNN2);
			bpos.Start();
			try
			{
				WaitForInitialization(bpos);
				// The DN should have register to both NNs.
				Org.Mockito.Mockito.Verify(mockNN1).RegisterDatanode(Org.Mockito.Mockito.Any<DatanodeRegistration
					>());
				Org.Mockito.Mockito.Verify(mockNN2).RegisterDatanode(Org.Mockito.Mockito.Any<DatanodeRegistration
					>());
				// Should get block reports from both NNs
				WaitForBlockReport(mockNN1);
				WaitForBlockReport(mockNN2);
				// When we receive a block, it should report it to both NNs
				bpos.NotifyNamenodeReceivedBlock(FakeBlock, string.Empty, string.Empty);
				ReceivedDeletedBlockInfo[] ret = WaitForBlockReceived(FakeBlock, mockNN1);
				NUnit.Framework.Assert.AreEqual(1, ret.Length);
				NUnit.Framework.Assert.AreEqual(FakeBlock.GetLocalBlock(), ret[0].GetBlock());
				ret = WaitForBlockReceived(FakeBlock, mockNN2);
				NUnit.Framework.Assert.AreEqual(1, ret.Length);
				NUnit.Framework.Assert.AreEqual(FakeBlock.GetLocalBlock(), ret[0].GetBlock());
			}
			finally
			{
				bpos.Stop();
			}
		}

		/// <summary>Test that DNA_INVALIDATE commands from the standby are ignored.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIgnoreDeletionsFromNonActive()
		{
			BPOfferService bpos = SetupBPOSForNNs(mockNN1, mockNN2);
			// Ask to invalidate FAKE_BLOCK when block report hits the
			// standby
			Org.Mockito.Mockito.DoReturn(new BlockCommand(DatanodeProtocol.DnaInvalidate, FakeBpid
				, new Block[] { FakeBlock.GetLocalBlock() })).When(mockNN2).BlockReport(Org.Mockito.Mockito
				.AnyObject<DatanodeRegistration>(), Org.Mockito.Mockito.Eq(FakeBpid), Org.Mockito.Mockito
				.AnyObject<StorageBlockReport[]>(), Org.Mockito.Mockito.AnyObject<BlockReportContext
				>());
			bpos.Start();
			try
			{
				WaitForInitialization(bpos);
				// Should get block reports from both NNs
				WaitForBlockReport(mockNN1);
				WaitForBlockReport(mockNN2);
			}
			finally
			{
				bpos.Stop();
			}
			// Should ignore the delete command from the standby
			Org.Mockito.Mockito.Verify(mockFSDataset, Org.Mockito.Mockito.Never()).Invalidate
				(Org.Mockito.Mockito.Eq(FakeBpid), (Block[])Org.Mockito.Mockito.AnyObject());
		}

		/// <summary>
		/// Ensure that, if the two NNs configured for a block pool
		/// have different block pool IDs, they will refuse to both
		/// register.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNNsFromDifferentClusters()
		{
			Org.Mockito.Mockito.DoReturn(new NamespaceInfo(1, "fake foreign cluster", FakeBpid
				, 0)).When(mockNN1).VersionRequest();
			BPOfferService bpos = SetupBPOSForNNs(mockNN1, mockNN2);
			bpos.Start();
			try
			{
				WaitForOneToFail(bpos);
			}
			finally
			{
				bpos.Stop();
			}
		}

		/// <summary>
		/// Test that the DataNode determines the active NameNode correctly
		/// based on the HA-related information in heartbeat responses.
		/// </summary>
		/// <remarks>
		/// Test that the DataNode determines the active NameNode correctly
		/// based on the HA-related information in heartbeat responses.
		/// See HDFS-2627.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPickActiveNameNode()
		{
			BPOfferService bpos = SetupBPOSForNNs(mockNN1, mockNN2);
			bpos.Start();
			try
			{
				WaitForInitialization(bpos);
				// Should start with neither NN as active.
				NUnit.Framework.Assert.IsNull(bpos.GetActiveNN());
				// Have NN1 claim active at txid 1
				mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Active
					, 1);
				bpos.TriggerHeartbeatForTests();
				NUnit.Framework.Assert.AreSame(mockNN1, bpos.GetActiveNN());
				// NN2 claims active at a higher txid
				mockHaStatuses[1] = new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Active
					, 2);
				bpos.TriggerHeartbeatForTests();
				NUnit.Framework.Assert.AreSame(mockNN2, bpos.GetActiveNN());
				// Even after another heartbeat from the first NN, it should
				// think NN2 is active, since it claimed a higher txid
				bpos.TriggerHeartbeatForTests();
				NUnit.Framework.Assert.AreSame(mockNN2, bpos.GetActiveNN());
				// Even if NN2 goes to standby, DN shouldn't reset to talking to NN1,
				// because NN1's txid is lower than the last active txid. Instead,
				// it should consider neither active.
				mockHaStatuses[1] = new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Standby
					, 2);
				bpos.TriggerHeartbeatForTests();
				NUnit.Framework.Assert.IsNull(bpos.GetActiveNN());
				// Now if NN1 goes back to a higher txid, it should be considered active
				mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Active
					, 3);
				bpos.TriggerHeartbeatForTests();
				NUnit.Framework.Assert.AreSame(mockNN1, bpos.GetActiveNN());
			}
			finally
			{
				bpos.Stop();
			}
		}

		/// <summary>Test datanode block pool initialization error handling.</summary>
		/// <remarks>
		/// Test datanode block pool initialization error handling.
		/// Failure in initializing a block pool should not cause NPE.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBPInitErrorHandling()
		{
			DataNode mockDn = Org.Mockito.Mockito.Mock<DataNode>();
			Org.Mockito.Mockito.DoReturn(true).When(mockDn).ShouldRun();
			Configuration conf = new Configuration();
			FilePath dnDataDir = new FilePath(new FilePath(TestBuildData, "testBPInitErrorHandling"
				), "data");
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dnDataDir.ToURI().ToString());
			Org.Mockito.Mockito.DoReturn(conf).When(mockDn).GetConf();
			Org.Mockito.Mockito.DoReturn(new DNConf(conf)).When(mockDn).GetDnConf();
			Org.Mockito.Mockito.DoReturn(DataNodeMetrics.Create(conf, "fake dn")).When(mockDn
				).GetMetrics();
			AtomicInteger count = new AtomicInteger();
			Org.Mockito.Mockito.DoAnswer(new _Answer_328(this, count, mockDn)).When(mockDn).InitBlockPool
				(Org.Mockito.Mockito.Any<BPOfferService>());
			// The initBlockPool is called again. Now mock init is done.
			BPOfferService bpos = SetupBPOSForNNs(mockDn, mockNN1, mockNN2);
			IList<BPServiceActor> actors = bpos.GetBPServiceActors();
			NUnit.Framework.Assert.AreEqual(2, actors.Count);
			bpos.Start();
			try
			{
				WaitForInitialization(bpos);
				// even if one of the actor initialization fails, the other one will be
				// finish block report.
				WaitForBlockReport(mockNN1, mockNN2);
			}
			finally
			{
				bpos.Stop();
			}
		}

		private sealed class _Answer_328 : Org.Mockito.Stubbing.Answer<Void>
		{
			public _Answer_328(TestBPOfferService _enclosing, AtomicInteger count, DataNode mockDn
				)
			{
				this._enclosing = _enclosing;
				this.count = count;
				this.mockDn = mockDn;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				if (count.GetAndIncrement() == 0)
				{
					throw new IOException("faked initBlockPool exception");
				}
				Org.Mockito.Mockito.DoReturn(this._enclosing.mockFSDataset).When(mockDn).GetFSDataset
					();
				return null;
			}

			private readonly TestBPOfferService _enclosing;

			private readonly AtomicInteger count;

			private readonly DataNode mockDn;
		}

		/// <exception cref="System.Exception"/>
		private void WaitForOneToFail(BPOfferService bpos)
		{
			GenericTestUtils.WaitFor(new _Supplier_355(bpos), 100, 10000);
		}

		private sealed class _Supplier_355 : Supplier<bool>
		{
			public _Supplier_355(BPOfferService bpos)
			{
				this.bpos = bpos;
			}

			public bool Get()
			{
				IList<BPServiceActor> actors = bpos.GetBPServiceActors();
				int failedcount = 0;
				foreach (BPServiceActor actor in actors)
				{
					if (!actor.IsAlive())
					{
						failedcount++;
					}
				}
				return failedcount == 1;
			}

			private readonly BPOfferService bpos;
		}

		/// <summary>
		/// Create a BPOfferService which registers with and heartbeats with the
		/// specified namenode proxy objects.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		private BPOfferService SetupBPOSForNNs(params DatanodeProtocolClientSideTranslatorPB
			[] nns)
		{
			return SetupBPOSForNNs(mockDn, nns);
		}

		/// <exception cref="System.IO.IOException"/>
		private BPOfferService SetupBPOSForNNs(DataNode mockDn, params DatanodeProtocolClientSideTranslatorPB
			[] nns)
		{
			// Set up some fake InetAddresses, then override the connectToNN
			// function to return the corresponding proxies.
			IDictionary<IPEndPoint, DatanodeProtocolClientSideTranslatorPB> nnMap = Maps.NewLinkedHashMap
				();
			for (int port = 0; port < nns.Length; port++)
			{
				nnMap[new IPEndPoint(port)] = nns[port];
				Org.Mockito.Mockito.DoReturn(nns[port]).When(mockDn).ConnectToNN(Org.Mockito.Mockito
					.Eq(new IPEndPoint(port)));
			}
			return new BPOfferService(Lists.NewArrayList(nnMap.Keys), mockDn);
		}

		/// <exception cref="System.Exception"/>
		private void WaitForInitialization(BPOfferService bpos)
		{
			GenericTestUtils.WaitFor(new _Supplier_397(bpos), 100, 10000);
		}

		private sealed class _Supplier_397 : Supplier<bool>
		{
			public _Supplier_397(BPOfferService bpos)
			{
				this.bpos = bpos;
			}

			public bool Get()
			{
				return bpos.IsAlive() && bpos.IsInitialized();
			}

			private readonly BPOfferService bpos;
		}

		/// <exception cref="System.Exception"/>
		private void WaitForBlockReport(DatanodeProtocolClientSideTranslatorPB mockNN)
		{
			GenericTestUtils.WaitFor(new _Supplier_407(mockNN), 500, 10000);
		}

		private sealed class _Supplier_407 : Supplier<bool>
		{
			public _Supplier_407(DatanodeProtocolClientSideTranslatorPB mockNN)
			{
				this.mockNN = mockNN;
			}

			public bool Get()
			{
				try
				{
					Org.Mockito.Mockito.Verify(mockNN).BlockReport(Org.Mockito.Mockito.AnyObject<DatanodeRegistration
						>(), Org.Mockito.Mockito.Eq(TestBPOfferService.FakeBpid), Org.Mockito.Mockito.AnyObject
						<StorageBlockReport[]>(), Org.Mockito.Mockito.AnyObject<BlockReportContext>());
					return true;
				}
				catch (Exception t)
				{
					TestBPOfferService.Log.Info("waiting on block report: " + t.Message);
					return false;
				}
			}

			private readonly DatanodeProtocolClientSideTranslatorPB mockNN;
		}

		/// <exception cref="System.Exception"/>
		private void WaitForBlockReport(DatanodeProtocolClientSideTranslatorPB mockNN1, DatanodeProtocolClientSideTranslatorPB
			 mockNN2)
		{
			GenericTestUtils.WaitFor(new _Supplier_429(mockNN1, mockNN2), 500, 10000);
		}

		private sealed class _Supplier_429 : Supplier<bool>
		{
			public _Supplier_429(DatanodeProtocolClientSideTranslatorPB mockNN1, DatanodeProtocolClientSideTranslatorPB
				 mockNN2)
			{
				this.mockNN1 = mockNN1;
				this.mockNN2 = mockNN2;
			}

			public bool Get()
			{
				return this.Get(mockNN1) || this.Get(mockNN2);
			}

			private bool Get(DatanodeProtocolClientSideTranslatorPB mockNN)
			{
				try
				{
					Org.Mockito.Mockito.Verify(mockNN).BlockReport(Org.Mockito.Mockito.AnyObject<DatanodeRegistration
						>(), Org.Mockito.Mockito.Eq(TestBPOfferService.FakeBpid), Org.Mockito.Mockito.AnyObject
						<StorageBlockReport[]>(), Org.Mockito.Mockito.AnyObject<BlockReportContext>());
					return true;
				}
				catch (Exception t)
				{
					TestBPOfferService.Log.Info("waiting on block report: " + t.Message);
					return false;
				}
			}

			private readonly DatanodeProtocolClientSideTranslatorPB mockNN1;

			private readonly DatanodeProtocolClientSideTranslatorPB mockNN2;
		}

		/// <exception cref="System.Exception"/>
		private ReceivedDeletedBlockInfo[] WaitForBlockReceived(ExtendedBlock fakeBlock, 
			DatanodeProtocolClientSideTranslatorPB mockNN)
		{
			string fakeBlockPoolId = fakeBlock.GetBlockPoolId();
			ArgumentCaptor<StorageReceivedDeletedBlocks[]> captor = ArgumentCaptor.ForClass<StorageReceivedDeletedBlocks
				[]>();
			GenericTestUtils.WaitFor(new _Supplier_457(mockNN, fakeBlockPoolId, captor), 100, 
				10000);
			return captor.GetValue()[0].GetBlocks();
		}

		private sealed class _Supplier_457 : Supplier<bool>
		{
			public _Supplier_457(DatanodeProtocolClientSideTranslatorPB mockNN, string fakeBlockPoolId
				, ArgumentCaptor<StorageReceivedDeletedBlocks[]> captor)
			{
				this.mockNN = mockNN;
				this.fakeBlockPoolId = fakeBlockPoolId;
				this.captor = captor;
			}

			public bool Get()
			{
				try
				{
					Org.Mockito.Mockito.Verify(mockNN).BlockReceivedAndDeleted(Org.Mockito.Mockito.AnyObject
						<DatanodeRegistration>(), Org.Mockito.Mockito.Eq(fakeBlockPoolId), captor.Capture
						());
					return true;
				}
				catch
				{
					return false;
				}
			}

			private readonly DatanodeProtocolClientSideTranslatorPB mockNN;

			private readonly string fakeBlockPoolId;

			private readonly ArgumentCaptor<StorageReceivedDeletedBlocks[]> captor;
		}

		private void SetTimeForSynchronousBPOSCalls()
		{
			if (firstCallTime == 0)
			{
				firstCallTime = Time.Now();
			}
			else
			{
				secondCallTime = Time.Now();
			}
		}

		private class BPOfferServiceSynchronousCallAnswer : Org.Mockito.Stubbing.Answer<Void
			>
		{
			private readonly int nnIdx;

			public BPOfferServiceSynchronousCallAnswer(TestBPOfferService _enclosing, int nnIdx
				)
			{
				this._enclosing = _enclosing;
				this.nnIdx = nnIdx;
			}

			// For active namenode we will record the processTime and for standby
			// namenode we will sleep for 5 seconds (This will simulate the situation
			// where the standby namenode is down ) .
			/// <exception cref="System.Exception"/>
			public virtual Void Answer(InvocationOnMock invocation)
			{
				if (this.nnIdx == 0)
				{
					this._enclosing.SetTimeForSynchronousBPOSCalls();
				}
				else
				{
					Sharpen.Thread.Sleep(5000);
				}
				return null;
			}

			private readonly TestBPOfferService _enclosing;
		}

		/// <summary>
		/// This test case test the
		/// <see cref="BPOfferService.ReportBadBlocks(Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, string, Org.Apache.Hadoop.FS.StorageType)
		/// 	"/>
		/// method
		/// such that if call to standby namenode times out then that should not
		/// affect the active namenode heartbeat processing since this function
		/// are in writeLock.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReportBadBlockWhenStandbyNNTimesOut()
		{
			BPOfferService bpos = SetupBPOSForNNs(mockNN1, mockNN2);
			bpos.Start();
			try
			{
				WaitForInitialization(bpos);
				// Should start with neither NN as active.
				NUnit.Framework.Assert.IsNull(bpos.GetActiveNN());
				// Have NN1 claim active at txid 1
				mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Active
					, 1);
				bpos.TriggerHeartbeatForTests();
				// Now mockNN1 is acting like active namenode and mockNN2 as Standby
				NUnit.Framework.Assert.AreSame(mockNN1, bpos.GetActiveNN());
				Org.Mockito.Mockito.DoAnswer(new TestBPOfferService.BPOfferServiceSynchronousCallAnswer
					(this, 0)).When(mockNN1).ReportBadBlocks(Org.Mockito.Mockito.Any<LocatedBlock[]>
					());
				Org.Mockito.Mockito.DoAnswer(new TestBPOfferService.BPOfferServiceSynchronousCallAnswer
					(this, 1)).When(mockNN2).ReportBadBlocks(Org.Mockito.Mockito.Any<LocatedBlock[]>
					());
				bpos.ReportBadBlocks(FakeBlock, mockFSDataset.GetVolume(FakeBlock).GetStorageID()
					, mockFSDataset.GetVolume(FakeBlock).GetStorageType());
				bpos.ReportBadBlocks(FakeBlock, mockFSDataset.GetVolume(FakeBlock).GetStorageID()
					, mockFSDataset.GetVolume(FakeBlock).GetStorageType());
				Sharpen.Thread.Sleep(10000);
				long difference = secondCallTime - firstCallTime;
				NUnit.Framework.Assert.IsTrue("Active namenode reportBadBlock processing should be "
					 + "independent of standby namenode reportBadBlock processing ", difference < 5000
					);
			}
			finally
			{
				bpos.Stop();
			}
		}

		/// <summary>
		/// This test case test the
		/// <see cref="BPOfferService.TrySendErrorReport(int, string)"/>
		/// method
		/// such that if call to standby namenode times out then that should not
		/// affect the active namenode heartbeat processing since this function
		/// are in writeLock.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTrySendErrorReportWhenStandbyNNTimesOut()
		{
			BPOfferService bpos = SetupBPOSForNNs(mockNN1, mockNN2);
			bpos.Start();
			try
			{
				WaitForInitialization(bpos);
				// Should start with neither NN as active.
				NUnit.Framework.Assert.IsNull(bpos.GetActiveNN());
				// Have NN1 claim active at txid 1
				mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Active
					, 1);
				bpos.TriggerHeartbeatForTests();
				// Now mockNN1 is acting like active namenode and mockNN2 as Standby
				NUnit.Framework.Assert.AreSame(mockNN1, bpos.GetActiveNN());
				Org.Mockito.Mockito.DoAnswer(new TestBPOfferService.BPOfferServiceSynchronousCallAnswer
					(this, 0)).When(mockNN1).ErrorReport(Org.Mockito.Mockito.Any<DatanodeRegistration
					>(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito.AnyString());
				Org.Mockito.Mockito.DoAnswer(new TestBPOfferService.BPOfferServiceSynchronousCallAnswer
					(this, 1)).When(mockNN2).ErrorReport(Org.Mockito.Mockito.Any<DatanodeRegistration
					>(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito.AnyString());
				string errorString = "Can't send invalid block " + FakeBlock;
				bpos.TrySendErrorReport(DatanodeProtocol.InvalidBlock, errorString);
				bpos.TrySendErrorReport(DatanodeProtocol.InvalidBlock, errorString);
				Sharpen.Thread.Sleep(10000);
				long difference = secondCallTime - firstCallTime;
				NUnit.Framework.Assert.IsTrue("Active namenode trySendErrorReport processing " + 
					"should be independent of standby namenode trySendErrorReport" + " processing ", 
					difference < 5000);
			}
			finally
			{
				bpos.Stop();
			}
		}

		/// <summary>
		/// This test case tests whether the
		/// <BPServiceActor>#processQueueMessages</BPServiceActor>
		/// adds back the error report back to the queue when
		/// {BPServiceActorAction#reportTo} throws an IOException
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTrySendErrorReportWhenNNThrowsIOException()
		{
			BPOfferService bpos = SetupBPOSForNNs(mockNN1, mockNN2);
			bpos.Start();
			try
			{
				WaitForInitialization(bpos);
				// Should start with neither NN as active.
				NUnit.Framework.Assert.IsNull(bpos.GetActiveNN());
				// Have NN1 claim active at txid 1
				mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Active
					, 1);
				bpos.TriggerHeartbeatForTests();
				// Now mockNN1 is acting like active namenode and mockNN2 as Standby
				NUnit.Framework.Assert.AreSame(mockNN1, bpos.GetActiveNN());
				Org.Mockito.Mockito.DoAnswer(new _Answer_602(this)).When(mockNN1).ErrorReport(Org.Mockito.Mockito
					.Any<DatanodeRegistration>(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito.
					AnyString());
				// Throw an IOException when this function is first called which will
				// in turn add that errorReport back to the bpThreadQueue and let it 
				// process the next time. 
				string errorString = "Can't send invalid block " + FakeBlock;
				bpos.TrySendErrorReport(DatanodeProtocol.InvalidBlock, errorString);
				Sharpen.Thread.Sleep(10000);
				NUnit.Framework.Assert.IsTrue("Active namenode didn't add the report back to the queue "
					 + "when errorReport threw IOException", secondCallTime != 0);
			}
			finally
			{
				bpos.Stop();
			}
		}

		private sealed class _Answer_602 : Org.Mockito.Stubbing.Answer<Void>
		{
			public _Answer_602(TestBPOfferService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				if (this._enclosing.firstCallTime == 0)
				{
					this._enclosing.firstCallTime = Time.Now();
					throw new IOException();
				}
				else
				{
					this._enclosing.secondCallTime = Time.Now();
					return null;
				}
			}

			private readonly TestBPOfferService _enclosing;
		}

		/// <summary>
		/// This test case doesn't add the reportBadBlock request to
		/// <see cref="BPServiceActor.BpThreadEnqueue(BPServiceActorAction)"/>
		/// when the Standby namenode throws
		/// <see cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReportBadBlocksWhenNNThrowsStandbyException()
		{
			BPOfferService bpos = SetupBPOSForNNs(mockNN1, mockNN2);
			bpos.Start();
			try
			{
				WaitForInitialization(bpos);
				// Should start with neither NN as active.
				NUnit.Framework.Assert.IsNull(bpos.GetActiveNN());
				// Have NN1 claim active at txid 1
				mockHaStatuses[0] = new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Active
					, 1);
				bpos.TriggerHeartbeatForTests();
				// Now mockNN1 is acting like active namenode and mockNN2 as Standby
				NUnit.Framework.Assert.AreSame(mockNN1, bpos.GetActiveNN());
				// Return nothing when active Active Namenode calls reportBadBlocks
				Org.Mockito.Mockito.DoNothing().When(mockNN1).ReportBadBlocks(Org.Mockito.Mockito
					.Any<LocatedBlock[]>());
				RemoteException re = new RemoteException(typeof(StandbyException).FullName, "Operation category WRITE is not supported in state "
					 + "standby", RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ErrorApplication
					);
				// Return StandbyException wrapped in RemoteException when Standby NN
				// calls reportBadBlocks
				Org.Mockito.Mockito.DoThrow(re).When(mockNN2).ReportBadBlocks(Org.Mockito.Mockito
					.Any<LocatedBlock[]>());
				bpos.ReportBadBlocks(FakeBlock, mockFSDataset.GetVolume(FakeBlock).GetStorageID()
					, mockFSDataset.GetVolume(FakeBlock).GetStorageType());
				// Send heartbeat so that the BpServiceActor can report bad block to
				// namenode
				bpos.TriggerHeartbeatForTests();
				Org.Mockito.Mockito.Verify(mockNN2, Org.Mockito.Mockito.Times(1)).ReportBadBlocks
					(Org.Mockito.Mockito.Any<LocatedBlock[]>());
				// Trigger another heartbeat, this will send reportBadBlock again if it
				// is present in the queue.
				bpos.TriggerHeartbeatForTests();
				Org.Mockito.Mockito.Verify(mockNN2, Org.Mockito.Mockito.Times(1)).ReportBadBlocks
					(Org.Mockito.Mockito.Any<LocatedBlock[]>());
			}
			finally
			{
				bpos.Stop();
			}
		}
	}
}
