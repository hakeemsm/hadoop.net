using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Test if we can correctly delay the deletion of blocks.</summary>
	public class TestPendingInvalidateBlock
	{
		private const int Blocksize = 1024;

		private const short Replication = 2;

		private Configuration conf;

		private MiniDFSCluster cluster;

		private DistributedFileSystem dfs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, Blocksize);
			// block deletion pending period
			conf.SetLong(DFSConfigKeys.DfsNamenodeStartupDelayBlockDeletionSecKey, 5L);
			// set the block report interval to 2s
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 2000);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1);
			// disable the RPC timeout for debug
			conf.SetLong(CommonConfigurationKeys.IpcPingIntervalKey, 0);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			dfs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPendingDeletion()
		{
			Path foo = new Path("/foo");
			DFSTestUtil.CreateFile(dfs, foo, Blocksize, Replication, 0);
			// restart NN
			cluster.RestartNameNode(true);
			dfs.Delete(foo, true);
			NUnit.Framework.Assert.AreEqual(0, cluster.GetNamesystem().GetBlocksTotal());
			NUnit.Framework.Assert.AreEqual(Replication, cluster.GetNamesystem().GetPendingDeletionBlocks
				());
			Sharpen.Thread.Sleep(6000);
			NUnit.Framework.Assert.AreEqual(0, cluster.GetNamesystem().GetBlocksTotal());
			NUnit.Framework.Assert.AreEqual(0, cluster.GetNamesystem().GetPendingDeletionBlocks
				());
			string nnStartedStr = cluster.GetNamesystem().GetNNStarted();
			long nnStarted = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy").Parse(nnStartedStr
				).GetTime();
			long blockDeletionStartTime = cluster.GetNamesystem().GetBlockDeletionStartTime();
			NUnit.Framework.Assert.IsTrue(string.Format("Expect blockDeletionStartTime = %d > nnStarted = %d/nnStartedStr = %s."
				, blockDeletionStartTime, nnStarted, nnStartedStr), blockDeletionStartTime > nnStarted
				);
		}

		/// <summary>
		/// Test whether we can delay the deletion of unknown blocks in DataNode's
		/// first several block reports.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPendingDeleteUnknownBlocks()
		{
			int fileNum = 5;
			// 5 files
			Path[] files = new Path[fileNum];
			MiniDFSCluster.DataNodeProperties[] dnprops = new MiniDFSCluster.DataNodeProperties
				[Replication];
			// create a group of files, each file contains 1 block
			for (int i = 0; i < fileNum; i++)
			{
				files[i] = new Path("/file" + i);
				DFSTestUtil.CreateFile(dfs, files[i], Blocksize, Replication, i);
			}
			// wait until all DataNodes have replicas
			WaitForReplication();
			for (int i_1 = Replication - 1; i_1 >= 0; i_1--)
			{
				dnprops[i_1] = cluster.StopDataNode(i_1);
			}
			Sharpen.Thread.Sleep(2000);
			// delete 2 files, we still have 3 files remaining so that we can cover
			// every DN storage
			for (int i_2 = 0; i_2 < 2; i_2++)
			{
				dfs.Delete(files[i_2], true);
			}
			// restart NameNode
			cluster.RestartNameNode(false);
			InvalidateBlocks invalidateBlocks = (InvalidateBlocks)Whitebox.GetInternalState(cluster
				.GetNamesystem().GetBlockManager(), "invalidateBlocks");
			InvalidateBlocks mockIb = Org.Mockito.Mockito.Spy(invalidateBlocks);
			Org.Mockito.Mockito.DoReturn(1L).When(mockIb).GetInvalidationDelay();
			Whitebox.SetInternalState(cluster.GetNamesystem().GetBlockManager(), "invalidateBlocks"
				, mockIb);
			NUnit.Framework.Assert.AreEqual(0L, cluster.GetNamesystem().GetPendingDeletionBlocks
				());
			// restart DataNodes
			for (int i_3 = 0; i_3 < Replication; i_3++)
			{
				cluster.RestartDataNode(dnprops[i_3], true);
			}
			cluster.WaitActive();
			for (int i_4 = 0; i_4 < Replication; i_4++)
			{
				DataNodeTestUtils.TriggerBlockReport(cluster.GetDataNodes()[i_4]);
			}
			Sharpen.Thread.Sleep(2000);
			// make sure we have received block reports by checking the total block #
			NUnit.Framework.Assert.AreEqual(3, cluster.GetNamesystem().GetBlocksTotal());
			NUnit.Framework.Assert.AreEqual(4, cluster.GetNamesystem().GetPendingDeletionBlocks
				());
			cluster.RestartNameNode(true);
			Sharpen.Thread.Sleep(6000);
			NUnit.Framework.Assert.AreEqual(3, cluster.GetNamesystem().GetBlocksTotal());
			NUnit.Framework.Assert.AreEqual(0, cluster.GetNamesystem().GetPendingDeletionBlocks
				());
		}

		/// <exception cref="System.Exception"/>
		private long WaitForReplication()
		{
			for (int i = 0; i < 10; i++)
			{
				long ur = cluster.GetNamesystem().GetUnderReplicatedBlocks();
				if (ur == 0)
				{
					return 0;
				}
				else
				{
					Sharpen.Thread.Sleep(1000);
				}
			}
			return cluster.GetNamesystem().GetUnderReplicatedBlocks();
		}

		public TestPendingInvalidateBlock()
		{
			{
				GenericTestUtils.SetLogLevel(BlockManager.Log, Level.Debug);
			}
		}
	}
}
