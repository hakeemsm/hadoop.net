using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Test if FSNamesystem handles heartbeat right</summary>
	public class TestHeartbeatHandling
	{
		/// <summary>
		/// Test if
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem.HandleHeartbeat(Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeRegistration, Org.Apache.Hadoop.Hdfs.Server.Protocol.StorageReport[], long, long, int, int, int, Org.Apache.Hadoop.Hdfs.Server.Protocol.VolumeFailureSummary)
		/// 	"/>
		/// can pick up replication and/or invalidate requests and observes the max
		/// limit
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHeartbeat()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitActive();
				FSNamesystem namesystem = cluster.GetNamesystem();
				HeartbeatManager hm = namesystem.GetBlockManager().GetDatanodeManager().GetHeartbeatManager
					();
				string poolId = namesystem.GetBlockPoolId();
				DatanodeRegistration nodeReg = DataNodeTestUtils.GetDNRegistrationForBP(cluster.GetDataNodes
					()[0], poolId);
				DatanodeDescriptor dd = NameNodeAdapter.GetDatanode(namesystem, nodeReg);
				string storageID = DatanodeStorage.GenerateUuid();
				dd.UpdateStorage(new DatanodeStorage(storageID));
				int RemainingBlocks = 1;
				int MaxReplicateLimit = conf.GetInt(DFSConfigKeys.DfsNamenodeReplicationMaxStreamsKey
					, 2);
				int MaxInvalidateLimit = DFSConfigKeys.DfsBlockInvalidateLimitDefault;
				int MaxInvalidateBlocks = 2 * MaxInvalidateLimit + RemainingBlocks;
				int MaxReplicateBlocks = 2 * MaxReplicateLimit + RemainingBlocks;
				DatanodeStorageInfo[] OneTarget = new DatanodeStorageInfo[] { dd.GetStorageInfo(storageID
					) };
				try
				{
					namesystem.WriteLock();
					lock (hm)
					{
						for (int i = 0; i < MaxReplicateBlocks; i++)
						{
							dd.AddBlockToBeReplicated(new Block(i, 0, GenerationStamp.LastReservedStamp), OneTarget
								);
						}
						DatanodeCommand[] cmds = NameNodeAdapter.SendHeartBeat(nodeReg, dd, namesystem).GetCommands
							();
						NUnit.Framework.Assert.AreEqual(1, cmds.Length);
						NUnit.Framework.Assert.AreEqual(DatanodeProtocol.DnaTransfer, cmds[0].GetAction()
							);
						NUnit.Framework.Assert.AreEqual(MaxReplicateLimit, ((BlockCommand)cmds[0]).GetBlocks
							().Length);
						AList<Block> blockList = new AList<Block>(MaxInvalidateBlocks);
						for (int i_1 = 0; i_1 < MaxInvalidateBlocks; i_1++)
						{
							blockList.AddItem(new Block(i_1, 0, GenerationStamp.LastReservedStamp));
						}
						dd.AddBlocksToBeInvalidated(blockList);
						cmds = NameNodeAdapter.SendHeartBeat(nodeReg, dd, namesystem).GetCommands();
						NUnit.Framework.Assert.AreEqual(2, cmds.Length);
						NUnit.Framework.Assert.AreEqual(DatanodeProtocol.DnaTransfer, cmds[0].GetAction()
							);
						NUnit.Framework.Assert.AreEqual(MaxReplicateLimit, ((BlockCommand)cmds[0]).GetBlocks
							().Length);
						NUnit.Framework.Assert.AreEqual(DatanodeProtocol.DnaInvalidate, cmds[1].GetAction
							());
						NUnit.Framework.Assert.AreEqual(MaxInvalidateLimit, ((BlockCommand)cmds[1]).GetBlocks
							().Length);
						cmds = NameNodeAdapter.SendHeartBeat(nodeReg, dd, namesystem).GetCommands();
						NUnit.Framework.Assert.AreEqual(2, cmds.Length);
						NUnit.Framework.Assert.AreEqual(DatanodeProtocol.DnaTransfer, cmds[0].GetAction()
							);
						NUnit.Framework.Assert.AreEqual(RemainingBlocks, ((BlockCommand)cmds[0]).GetBlocks
							().Length);
						NUnit.Framework.Assert.AreEqual(DatanodeProtocol.DnaInvalidate, cmds[1].GetAction
							());
						NUnit.Framework.Assert.AreEqual(MaxInvalidateLimit, ((BlockCommand)cmds[1]).GetBlocks
							().Length);
						cmds = NameNodeAdapter.SendHeartBeat(nodeReg, dd, namesystem).GetCommands();
						NUnit.Framework.Assert.AreEqual(1, cmds.Length);
						NUnit.Framework.Assert.AreEqual(DatanodeProtocol.DnaInvalidate, cmds[0].GetAction
							());
						NUnit.Framework.Assert.AreEqual(RemainingBlocks, ((BlockCommand)cmds[0]).GetBlocks
							().Length);
						cmds = NameNodeAdapter.SendHeartBeat(nodeReg, dd, namesystem).GetCommands();
						NUnit.Framework.Assert.AreEqual(0, cmds.Length);
					}
				}
				finally
				{
					namesystem.WriteUnlock();
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test if
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem.HandleHeartbeat(Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeRegistration, Org.Apache.Hadoop.Hdfs.Server.Protocol.StorageReport[], long, long, int, int, int, Org.Apache.Hadoop.Hdfs.Server.Protocol.VolumeFailureSummary)
		/// 	"/>
		/// correctly selects data node targets for block recovery.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHeartbeatBlockRecovery()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			try
			{
				cluster.WaitActive();
				FSNamesystem namesystem = cluster.GetNamesystem();
				HeartbeatManager hm = namesystem.GetBlockManager().GetDatanodeManager().GetHeartbeatManager
					();
				string poolId = namesystem.GetBlockPoolId();
				DatanodeRegistration nodeReg1 = DataNodeTestUtils.GetDNRegistrationForBP(cluster.
					GetDataNodes()[0], poolId);
				DatanodeDescriptor dd1 = NameNodeAdapter.GetDatanode(namesystem, nodeReg1);
				dd1.UpdateStorage(new DatanodeStorage(DatanodeStorage.GenerateUuid()));
				DatanodeRegistration nodeReg2 = DataNodeTestUtils.GetDNRegistrationForBP(cluster.
					GetDataNodes()[1], poolId);
				DatanodeDescriptor dd2 = NameNodeAdapter.GetDatanode(namesystem, nodeReg2);
				dd2.UpdateStorage(new DatanodeStorage(DatanodeStorage.GenerateUuid()));
				DatanodeRegistration nodeReg3 = DataNodeTestUtils.GetDNRegistrationForBP(cluster.
					GetDataNodes()[2], poolId);
				DatanodeDescriptor dd3 = NameNodeAdapter.GetDatanode(namesystem, nodeReg3);
				dd3.UpdateStorage(new DatanodeStorage(DatanodeStorage.GenerateUuid()));
				try
				{
					namesystem.WriteLock();
					lock (hm)
					{
						NameNodeAdapter.SendHeartBeat(nodeReg1, dd1, namesystem);
						NameNodeAdapter.SendHeartBeat(nodeReg2, dd2, namesystem);
						NameNodeAdapter.SendHeartBeat(nodeReg3, dd3, namesystem);
						// Test with all alive nodes.
						DFSTestUtil.ResetLastUpdatesWithOffset(dd1, 0);
						DFSTestUtil.ResetLastUpdatesWithOffset(dd2, 0);
						DFSTestUtil.ResetLastUpdatesWithOffset(dd3, 0);
						DatanodeStorageInfo[] storages = new DatanodeStorageInfo[] { dd1.GetStorageInfos(
							)[0], dd2.GetStorageInfos()[0], dd3.GetStorageInfos()[0] };
						BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction
							(new Block(0, 0, GenerationStamp.LastReservedStamp), (short)3, HdfsServerConstants.BlockUCState
							.UnderRecovery, storages);
						dd1.AddBlockToBeRecovered(blockInfo);
						DatanodeCommand[] cmds = NameNodeAdapter.SendHeartBeat(nodeReg1, dd1, namesystem)
							.GetCommands();
						NUnit.Framework.Assert.AreEqual(1, cmds.Length);
						NUnit.Framework.Assert.AreEqual(DatanodeProtocol.DnaRecoverblock, cmds[0].GetAction
							());
						BlockRecoveryCommand recoveryCommand = (BlockRecoveryCommand)cmds[0];
						NUnit.Framework.Assert.AreEqual(1, recoveryCommand.GetRecoveringBlocks().Count);
						DatanodeInfo[] recoveringNodes = Sharpen.Collections.ToArray(recoveryCommand.GetRecoveringBlocks
							(), new BlockRecoveryCommand.RecoveringBlock[0])[0].GetLocations();
						NUnit.Framework.Assert.AreEqual(3, recoveringNodes.Length);
						NUnit.Framework.Assert.AreEqual(recoveringNodes[0], dd1);
						NUnit.Framework.Assert.AreEqual(recoveringNodes[1], dd2);
						NUnit.Framework.Assert.AreEqual(recoveringNodes[2], dd3);
						// Test with one stale node.
						DFSTestUtil.ResetLastUpdatesWithOffset(dd1, 0);
						// More than the default stale interval of 30 seconds.
						DFSTestUtil.ResetLastUpdatesWithOffset(dd2, -40 * 1000);
						DFSTestUtil.ResetLastUpdatesWithOffset(dd3, 0);
						blockInfo = new BlockInfoContiguousUnderConstruction(new Block(0, 0, GenerationStamp
							.LastReservedStamp), (short)3, HdfsServerConstants.BlockUCState.UnderRecovery, storages
							);
						dd1.AddBlockToBeRecovered(blockInfo);
						cmds = NameNodeAdapter.SendHeartBeat(nodeReg1, dd1, namesystem).GetCommands();
						NUnit.Framework.Assert.AreEqual(1, cmds.Length);
						NUnit.Framework.Assert.AreEqual(DatanodeProtocol.DnaRecoverblock, cmds[0].GetAction
							());
						recoveryCommand = (BlockRecoveryCommand)cmds[0];
						NUnit.Framework.Assert.AreEqual(1, recoveryCommand.GetRecoveringBlocks().Count);
						recoveringNodes = Sharpen.Collections.ToArray(recoveryCommand.GetRecoveringBlocks
							(), new BlockRecoveryCommand.RecoveringBlock[0])[0].GetLocations();
						NUnit.Framework.Assert.AreEqual(2, recoveringNodes.Length);
						// dd2 is skipped.
						NUnit.Framework.Assert.AreEqual(recoveringNodes[0], dd1);
						NUnit.Framework.Assert.AreEqual(recoveringNodes[1], dd3);
						// Test with all stale node.
						DFSTestUtil.ResetLastUpdatesWithOffset(dd1, -60 * 1000);
						// More than the default stale interval of 30 seconds.
						DFSTestUtil.ResetLastUpdatesWithOffset(dd2, -40 * 1000);
						DFSTestUtil.ResetLastUpdatesWithOffset(dd3, -80 * 1000);
						blockInfo = new BlockInfoContiguousUnderConstruction(new Block(0, 0, GenerationStamp
							.LastReservedStamp), (short)3, HdfsServerConstants.BlockUCState.UnderRecovery, storages
							);
						dd1.AddBlockToBeRecovered(blockInfo);
						cmds = NameNodeAdapter.SendHeartBeat(nodeReg1, dd1, namesystem).GetCommands();
						NUnit.Framework.Assert.AreEqual(1, cmds.Length);
						NUnit.Framework.Assert.AreEqual(DatanodeProtocol.DnaRecoverblock, cmds[0].GetAction
							());
						recoveryCommand = (BlockRecoveryCommand)cmds[0];
						NUnit.Framework.Assert.AreEqual(1, recoveryCommand.GetRecoveringBlocks().Count);
						recoveringNodes = Sharpen.Collections.ToArray(recoveryCommand.GetRecoveringBlocks
							(), new BlockRecoveryCommand.RecoveringBlock[0])[0].GetLocations();
						// Only dd1 is included since it heart beated and hence its not stale
						// when the list of recovery blocks is constructed.
						NUnit.Framework.Assert.AreEqual(3, recoveringNodes.Length);
						NUnit.Framework.Assert.AreEqual(recoveringNodes[0], dd1);
						NUnit.Framework.Assert.AreEqual(recoveringNodes[1], dd2);
						NUnit.Framework.Assert.AreEqual(recoveringNodes[2], dd3);
					}
				}
				finally
				{
					namesystem.WriteUnlock();
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
