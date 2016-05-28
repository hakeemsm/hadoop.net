using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestOverReplicatedBlocks
	{
		/// <summary>Test processOverReplicatedBlock can handle corrupt replicas fine.</summary>
		/// <remarks>
		/// Test processOverReplicatedBlock can handle corrupt replicas fine.
		/// It make sure that it won't treat corrupt replicas as valid ones
		/// thus prevents NN deleting valid replicas but keeping
		/// corrupt ones.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProcesOverReplicateBlock()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, 100L);
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000L);
			conf.Set(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, Sharpen.Extensions.ToString
				(2));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				Path fileName = new Path("/foo1");
				DFSTestUtil.CreateFile(fs, fileName, 2, (short)3, 0L);
				DFSTestUtil.WaitReplication(fs, fileName, (short)3);
				// corrupt the block on datanode 0
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, fileName);
				NUnit.Framework.Assert.IsTrue(cluster.CorruptReplica(0, block));
				MiniDFSCluster.DataNodeProperties dnProps = cluster.StopDataNode(0);
				// remove block scanner log to trigger block scanning
				FilePath scanCursor = new FilePath(new FilePath(MiniDFSCluster.GetFinalizedDir(cluster
					.GetInstanceStorageDir(0, 0), cluster.GetNamesystem().GetBlockPoolId()).GetParent
					()).GetParent(), "scanner.cursor");
				//wait for one minute for deletion to succeed;
				for (int i = 0; !scanCursor.Delete(); i++)
				{
					NUnit.Framework.Assert.IsTrue("Could not delete " + scanCursor.GetAbsolutePath() 
						+ " in one minute", i < 60);
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
				}
				// restart the datanode so the corrupt replica will be detected
				cluster.RestartDataNode(dnProps);
				DFSTestUtil.WaitReplication(fs, fileName, (short)2);
				string blockPoolId = cluster.GetNamesystem().GetBlockPoolId();
				DatanodeID corruptDataNode = DataNodeTestUtils.GetDNRegistrationForBP(cluster.GetDataNodes
					()[2], blockPoolId);
				FSNamesystem namesystem = cluster.GetNamesystem();
				BlockManager bm = namesystem.GetBlockManager();
				HeartbeatManager hm = bm.GetDatanodeManager().GetHeartbeatManager();
				try
				{
					namesystem.WriteLock();
					lock (hm)
					{
						// set live datanode's remaining space to be 0 
						// so they will be chosen to be deleted when over-replication occurs
						string corruptMachineName = corruptDataNode.GetXferAddr();
						foreach (DatanodeDescriptor datanode in hm.GetDatanodes())
						{
							if (!corruptMachineName.Equals(datanode.GetXferAddr()))
							{
								datanode.GetStorageInfos()[0].SetUtilizationForTesting(100L, 100L, 0, 100L);
								datanode.UpdateHeartbeat(BlockManagerTestUtil.GetStorageReportsForDatanode(datanode
									), 0L, 0L, 0, 0, null);
							}
						}
						// decrease the replication factor to 1; 
						NameNodeAdapter.SetReplication(namesystem, fileName.ToString(), (short)1);
						// corrupt one won't be chosen to be excess one
						// without 4910 the number of live replicas would be 0: block gets lost
						NUnit.Framework.Assert.AreEqual(1, bm.CountNodes(block.GetLocalBlock()).LiveReplicas
							());
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

		internal const long SmallBlockSize = DFSConfigKeys.DfsBytesPerChecksumDefault;

		internal const long SmallFileLength = SmallBlockSize * 4;

		/// <summary>
		/// The test verifies that replica for deletion is chosen on a node,
		/// with the oldest heartbeat, when this heartbeat is larger than the
		/// tolerable heartbeat interval.
		/// </summary>
		/// <remarks>
		/// The test verifies that replica for deletion is chosen on a node,
		/// with the oldest heartbeat, when this heartbeat is larger than the
		/// tolerable heartbeat interval.
		/// It creates a file with several blocks and replication 4.
		/// The last DN is configured to send heartbeats rarely.
		/// Test waits until the tolerable heartbeat interval expires, and reduces
		/// replication of the file. All replica deletions should be scheduled for the
		/// last node. No replicas will actually be deleted, since last DN doesn't
		/// send heartbeats.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChooseReplicaToDelete()
		{
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, SmallBlockSize);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				fs = cluster.GetFileSystem();
				FSNamesystem namesystem = cluster.GetNamesystem();
				conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 300);
				cluster.StartDataNodes(conf, 1, true, null, null, null);
				DataNode lastDN = cluster.GetDataNodes()[3];
				DatanodeRegistration dnReg = DataNodeTestUtils.GetDNRegistrationForBP(lastDN, namesystem
					.GetBlockPoolId());
				string lastDNid = dnReg.GetDatanodeUuid();
				Path fileName = new Path("/foo2");
				DFSTestUtil.CreateFile(fs, fileName, SmallFileLength, (short)4, 0L);
				DFSTestUtil.WaitReplication(fs, fileName, (short)4);
				// Wait for tolerable number of heartbeats plus one
				DatanodeDescriptor nodeInfo = null;
				long lastHeartbeat = 0;
				long waitTime = DFSConfigKeys.DfsHeartbeatIntervalDefault * 1000 * (DFSConfigKeys
					.DfsNamenodeTolerateHeartbeatMultiplierDefault + 1);
				do
				{
					nodeInfo = namesystem.GetBlockManager().GetDatanodeManager().GetDatanode(dnReg);
					lastHeartbeat = nodeInfo.GetLastUpdateMonotonic();
				}
				while (Time.MonotonicNow() - lastHeartbeat < waitTime);
				fs.SetReplication(fileName, (short)3);
				BlockLocation[] locs = fs.GetFileBlockLocations(fs.GetFileStatus(fileName), 0, long.MaxValue
					);
				// All replicas for deletion should be scheduled on lastDN.
				// And should not actually be deleted, because lastDN does not heartbeat.
				namesystem.ReadLock();
				ICollection<Block> dnBlocks = namesystem.GetBlockManager().excessReplicateMap[lastDNid
					];
				NUnit.Framework.Assert.AreEqual("Replicas on node " + lastDNid + " should have been deleted"
					, SmallFileLength / SmallBlockSize, dnBlocks.Count);
				namesystem.ReadUnlock();
				foreach (BlockLocation location in locs)
				{
					NUnit.Framework.Assert.AreEqual("Block should still have 4 replicas", 4, location
						.GetNames().Length);
				}
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test over replicated block should get invalidated when decreasing the
		/// replication for a partial block.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidateOverReplicatedBlock()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			try
			{
				FSNamesystem namesystem = cluster.GetNamesystem();
				BlockManager bm = namesystem.GetBlockManager();
				FileSystem fs = cluster.GetFileSystem();
				Path p = new Path(MiniDFSCluster.GetBaseDirectory(), "/foo1");
				FSDataOutputStream @out = fs.Create(p, (short)2);
				@out.WriteBytes("HDFS-3119: " + p);
				@out.Hsync();
				fs.SetReplication(p, (short)1);
				@out.Close();
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, p);
				NUnit.Framework.Assert.AreEqual("Expected only one live replica for the block", 1
					, bm.CountNodes(block.GetLocalBlock()).LiveReplicas());
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
