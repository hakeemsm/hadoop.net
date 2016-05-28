using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Verify that incremental block reports are generated in response to
	/// block additions/deletions.
	/// </summary>
	public class TestIncrementalBlockReports
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestIncrementalBlockReports
			));

		private const short DnCount = 1;

		private const long DummyBlockId = 5678;

		private const long DummyBlockLength = 1024 * 1024;

		private const long DummyBlockGenstamp = 1000;

		private MiniDFSCluster cluster = null;

		private DistributedFileSystem fs;

		private Configuration conf;

		private NameNode singletonNn;

		private DataNode singletonDn;

		private BPOfferService bpos;

		private BPServiceActor actor;

		private string storageUuid;

		// BPOS to use for block injection.
		// BPSA to use for block injection.
		// DatanodeStorage to use for block injection.
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartCluster()
		{
			conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DnCount).Build();
			fs = cluster.GetFileSystem();
			singletonNn = cluster.GetNameNode();
			singletonDn = cluster.GetDataNodes()[0];
			bpos = singletonDn.GetAllBpOs()[0];
			actor = bpos.GetBPServiceActors()[0];
			storageUuid = singletonDn.GetFSDataset().GetVolumes()[0].GetStorageID();
		}

		private static Block GetDummyBlock()
		{
			return new Block(DummyBlockId, DummyBlockLength, DummyBlockGenstamp);
		}

		/// <summary>Inject a fake 'received' block into the BPServiceActor state.</summary>
		private void InjectBlockReceived()
		{
			ReceivedDeletedBlockInfo rdbi = new ReceivedDeletedBlockInfo(GetDummyBlock(), ReceivedDeletedBlockInfo.BlockStatus
				.ReceivedBlock, null);
			actor.NotifyNamenodeBlock(rdbi, storageUuid, true);
		}

		/// <summary>Inject a fake 'deleted' block into the BPServiceActor state.</summary>
		private void InjectBlockDeleted()
		{
			ReceivedDeletedBlockInfo rdbi = new ReceivedDeletedBlockInfo(GetDummyBlock(), ReceivedDeletedBlockInfo.BlockStatus
				.DeletedBlock, null);
			actor.NotifyNamenodeDeletedBlock(rdbi, storageUuid);
		}

		/// <summary>Spy on calls from the DN to the NN.</summary>
		/// <returns>spy object that can be used for Mockito verification.</returns>
		internal virtual DatanodeProtocolClientSideTranslatorPB SpyOnDnCallsToNn()
		{
			return DataNodeTestUtils.SpyOnBposToNN(singletonDn, singletonNn);
		}

		/// <summary>
		/// Ensure that an IBR is generated immediately for a block received by
		/// the DN.
		/// </summary>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReportBlockReceived()
		{
			try
			{
				DatanodeProtocolClientSideTranslatorPB nnSpy = SpyOnDnCallsToNn();
				InjectBlockReceived();
				// Sleep for a very short time, this is necessary since the IBR is
				// generated asynchronously.
				Sharpen.Thread.Sleep(2000);
				// Ensure that the received block was reported immediately.
				Org.Mockito.Mockito.Verify(nnSpy, Org.Mockito.Mockito.Times(1)).BlockReceivedAndDeleted
					(Matchers.Any<DatanodeRegistration>(), Matchers.AnyString(), Matchers.Any<StorageReceivedDeletedBlocks
					[]>());
			}
			finally
			{
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <summary>Ensure that a delayed IBR is generated for a block deleted on the DN.</summary>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReportBlockDeleted()
		{
			try
			{
				// Trigger a block report to reset the IBR timer.
				DataNodeTestUtils.TriggerBlockReport(singletonDn);
				// Spy on calls from the DN to the NN
				DatanodeProtocolClientSideTranslatorPB nnSpy = SpyOnDnCallsToNn();
				InjectBlockDeleted();
				// Sleep for a very short time since IBR is generated
				// asynchronously.
				Sharpen.Thread.Sleep(2000);
				// Ensure that no block report was generated immediately.
				// Deleted blocks are reported when the IBR timer elapses.
				Org.Mockito.Mockito.Verify(nnSpy, Org.Mockito.Mockito.Times(0)).BlockReceivedAndDeleted
					(Matchers.Any<DatanodeRegistration>(), Matchers.AnyString(), Matchers.Any<StorageReceivedDeletedBlocks
					[]>());
				// Trigger a block report, this also triggers an IBR.
				DataNodeTestUtils.TriggerBlockReport(singletonDn);
				Sharpen.Thread.Sleep(2000);
				// Ensure that the deleted block is reported.
				Org.Mockito.Mockito.Verify(nnSpy, Org.Mockito.Mockito.Times(1)).BlockReceivedAndDeleted
					(Matchers.Any<DatanodeRegistration>(), Matchers.AnyString(), Matchers.Any<StorageReceivedDeletedBlocks
					[]>());
			}
			finally
			{
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <summary>Add a received block entry and then replace it.</summary>
		/// <remarks>
		/// Add a received block entry and then replace it. Ensure that a single
		/// IBR is generated and that pending receive request state is cleared.
		/// This test case verifies the failure in HDFS-5922.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReplaceReceivedBlock()
		{
			try
			{
				// Spy on calls from the DN to the NN
				DatanodeProtocolClientSideTranslatorPB nnSpy = SpyOnDnCallsToNn();
				InjectBlockReceived();
				InjectBlockReceived();
				// Overwrite the existing entry.
				// Sleep for a very short time since IBR is generated
				// asynchronously.
				Sharpen.Thread.Sleep(2000);
				// Ensure that the received block is reported.
				Org.Mockito.Mockito.Verify(nnSpy, Org.Mockito.Mockito.AtLeastOnce()).BlockReceivedAndDeleted
					(Matchers.Any<DatanodeRegistration>(), Matchers.AnyString(), Matchers.Any<StorageReceivedDeletedBlocks
					[]>());
				// Ensure that no more IBRs are pending.
				NUnit.Framework.Assert.IsFalse(actor.HasPendingIBR());
			}
			finally
			{
				cluster.Shutdown();
				cluster = null;
			}
		}
	}
}
