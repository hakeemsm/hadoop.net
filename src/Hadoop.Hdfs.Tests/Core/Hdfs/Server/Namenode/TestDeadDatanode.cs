using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Test to ensure requests from dead datnodes are rejected by namenode with
	/// appropriate exceptions/failure response
	/// </summary>
	public class TestDeadDatanode
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestDeadDatanode
			));

		private MiniDFSCluster cluster;

		[TearDown]
		public virtual void Cleanup()
		{
			cluster.Shutdown();
		}

		/// <summary>
		/// Test to ensure namenode rejects request from dead datanode
		/// - Start a cluster
		/// - Shutdown the datanode and wait for it to be marked dead at the namenode
		/// - Send datanode requests to Namenode and make sure it is rejected
		/// appropriately.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeadDatanode()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 500);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
			cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitActive();
			string poolId = cluster.GetNamesystem().GetBlockPoolId();
			// wait for datanode to be marked live
			DataNode dn = cluster.GetDataNodes()[0];
			DatanodeRegistration reg = DataNodeTestUtils.GetDNRegistrationForBP(cluster.GetDataNodes
				()[0], poolId);
			DFSTestUtil.WaitForDatanodeState(cluster, reg.GetDatanodeUuid(), true, 20000);
			// Shutdown and wait for datanode to be marked dead
			dn.Shutdown();
			DFSTestUtil.WaitForDatanodeState(cluster, reg.GetDatanodeUuid(), false, 20000);
			DatanodeProtocol dnp = cluster.GetNameNodeRpc();
			ReceivedDeletedBlockInfo[] blocks = new ReceivedDeletedBlockInfo[] { new ReceivedDeletedBlockInfo
				(new Block(0), ReceivedDeletedBlockInfo.BlockStatus.ReceivedBlock, null) };
			StorageReceivedDeletedBlocks[] storageBlocks = new StorageReceivedDeletedBlocks[]
				 { new StorageReceivedDeletedBlocks(reg.GetDatanodeUuid(), blocks) };
			// Ensure blockReceived call from dead datanode is rejected with IOException
			try
			{
				dnp.BlockReceivedAndDeleted(reg, poolId, storageBlocks);
				NUnit.Framework.Assert.Fail("Expected IOException is not thrown");
			}
			catch (IOException)
			{
			}
			// Expected
			// Ensure blockReport from dead datanode is rejected with IOException
			StorageBlockReport[] report = new StorageBlockReport[] { new StorageBlockReport(new 
				DatanodeStorage(reg.GetDatanodeUuid()), BlockListAsLongs.Empty) };
			try
			{
				dnp.BlockReport(reg, poolId, report, new BlockReportContext(1, 0, Runtime.NanoTime
					()));
				NUnit.Framework.Assert.Fail("Expected IOException is not thrown");
			}
			catch (IOException)
			{
			}
			// Expected
			// Ensure heartbeat from dead datanode is rejected with a command
			// that asks datanode to register again
			StorageReport[] rep = new StorageReport[] { new StorageReport(new DatanodeStorage
				(reg.GetDatanodeUuid()), false, 0, 0, 0, 0) };
			DatanodeCommand[] cmd = dnp.SendHeartbeat(reg, rep, 0L, 0L, 0, 0, 0, null).GetCommands
				();
			NUnit.Framework.Assert.AreEqual(1, cmd.Length);
			NUnit.Framework.Assert.AreEqual(cmd[0].GetAction(), RegisterCommand.Register.GetAction
				());
		}
	}
}
