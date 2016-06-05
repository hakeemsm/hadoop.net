using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestBlocksWithNotEnoughRacks
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestBlocksWithNotEnoughRacks
			));

		static TestBlocksWithNotEnoughRacks()
		{
			((Log4JLogger)LogFactory.GetLog(typeof(FSNamesystem))).GetLogger().SetLevel(Level
				.All);
			((Log4JLogger)Log).GetLogger().SetLevel(Level.All);
		}

		/*
		* Return a configuration object with low timeouts for testing and
		* a topology script set (which enables rack awareness).
		*/
		private Configuration GetConf()
		{
			Configuration conf = new HdfsConfiguration();
			// Lower the heart beat interval so the NN quickly learns of dead
			// or decommissioned DNs and the NN issues replication and invalidation
			// commands quickly (as replies to heartbeats)
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
			// Have the NN ReplicationMonitor compute the replication and
			// invalidation commands to send DNs every second.
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1);
			// Have the NN check for pending replications every second so it
			// quickly schedules additional replicas as they are identified.
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 1);
			// The DNs report blocks every second.
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000L);
			// Indicates we have multiple racks
			conf.Set(DFSConfigKeys.NetTopologyScriptFileNameKey, "xyz");
			return conf;
		}

		/*
		* Creates a block with all datanodes on the same rack, though the block
		* is sufficiently replicated. Adds an additional datanode on a new rack.
		* The block should be replicated to the new rack.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSufficientlyReplBlocksUsesNewRack()
		{
			Configuration conf = GetConf();
			short ReplicationFactor = 3;
			Path filePath = new Path("/testFile");
			// All datanodes are on the same rack
			string[] racks = new string[] { "/rack1", "/rack1", "/rack1" };
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(racks.Length
				).Racks(racks).Build();
			try
			{
				// Create a file with one block with a replication factor of 3
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, filePath, 1L, ReplicationFactor, 1L);
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, filePath);
				DFSTestUtil.WaitForReplication(cluster, b, 1, ReplicationFactor, 0);
				// Add a new datanode on a different rack
				string[] newRacks = new string[] { "/rack2" };
				cluster.StartDataNodes(conf, 1, true, null, newRacks);
				cluster.WaitActive();
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/*
		* Like the previous test but the block starts with a single replica,
		* and therefore unlike the previous test the block does not start
		* off needing replicas.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSufficientlySingleReplBlockUsesNewRack()
		{
			Configuration conf = GetConf();
			short ReplicationFactor = 1;
			Path filePath = new Path("/testFile");
			string[] racks = new string[] { "/rack1", "/rack1", "/rack1", "/rack2" };
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(racks.Length
				).Racks(racks).Build();
			FSNamesystem ns = cluster.GetNameNode().GetNamesystem();
			try
			{
				// Create a file with one block with a replication factor of 1
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, filePath, 1L, ReplicationFactor, 1L);
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, filePath);
				DFSTestUtil.WaitForReplication(cluster, b, 1, ReplicationFactor, 0);
				ReplicationFactor = 2;
				NameNodeAdapter.SetReplication(ns, "/testFile", ReplicationFactor);
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/*
		* Creates a block with all datanodes on the same rack. Add additional
		* datanodes on a different rack and increase the replication factor,
		* making sure there are enough replicas across racks. If the previous
		* test passes this one should too, however this test may pass when
		* the previous one fails because the replication code is explicitly
		* triggered by setting the replication factor.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnderReplicatedUsesNewRacks()
		{
			Configuration conf = GetConf();
			short ReplicationFactor = 3;
			Path filePath = new Path("/testFile");
			// All datanodes are on the same rack
			string[] racks = new string[] { "/rack1", "/rack1", "/rack1", "/rack1", "/rack1" };
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(racks.Length
				).Racks(racks).Build();
			FSNamesystem ns = cluster.GetNameNode().GetNamesystem();
			try
			{
				// Create a file with one block
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, filePath, 1L, ReplicationFactor, 1L);
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, filePath);
				DFSTestUtil.WaitForReplication(cluster, b, 1, ReplicationFactor, 0);
				// Add new datanodes on a different rack and increase the
				// replication factor so the block is underreplicated and make
				// sure at least one of the hosts on the new rack is used. 
				string[] newRacks = new string[] { "/rack2", "/rack2" };
				cluster.StartDataNodes(conf, 2, true, null, newRacks);
				ReplicationFactor = 5;
				NameNodeAdapter.SetReplication(ns, "/testFile", ReplicationFactor);
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/*
		* Test that a block that is re-replicated because one of its replicas
		* is found to be corrupt and is re-replicated across racks.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCorruptBlockRereplicatedAcrossRacks()
		{
			Configuration conf = GetConf();
			short ReplicationFactor = 2;
			int fileLen = 512;
			Path filePath = new Path("/testFile");
			// Datanodes are spread across two racks
			string[] racks = new string[] { "/rack1", "/rack1", "/rack2", "/rack2" };
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(racks.Length
				).Racks(racks).Build();
			FSNamesystem ns = cluster.GetNameNode().GetNamesystem();
			try
			{
				// Create a file with one block with a replication factor of 2
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, filePath, fileLen, ReplicationFactor, 1L);
				string fileContent = DFSTestUtil.ReadFile(fs, filePath);
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, filePath);
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
				// Corrupt a replica of the block
				int dnToCorrupt = DFSTestUtil.FirstDnWithBlock(cluster, b);
				NUnit.Framework.Assert.IsTrue(cluster.CorruptReplica(dnToCorrupt, b));
				// Restart the datanode so blocks are re-scanned, and the corrupt
				// block is detected.
				cluster.RestartDataNode(dnToCorrupt);
				// Wait for the namenode to notice the corrupt replica
				DFSTestUtil.WaitCorruptReplicas(fs, ns, filePath, b, 1);
				// The rack policy is still respected
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
				// Ensure all replicas are valid (the corrupt replica may not
				// have been cleaned up yet).
				for (int i = 0; i < racks.Length; i++)
				{
					string blockContent = cluster.ReadBlockOnDataNode(i, b);
					if (blockContent != null && i != dnToCorrupt)
					{
						NUnit.Framework.Assert.AreEqual("Corrupt replica", fileContent, blockContent);
					}
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/*
		* Reduce the replication factor of a file, making sure that the only
		* cross rack replica is not removed when deleting replicas.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReduceReplFactorRespectsRackPolicy()
		{
			Configuration conf = GetConf();
			short ReplicationFactor = 3;
			Path filePath = new Path("/testFile");
			string[] racks = new string[] { "/rack1", "/rack1", "/rack2", "/rack2" };
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(racks.Length
				).Racks(racks).Build();
			FSNamesystem ns = cluster.GetNameNode().GetNamesystem();
			try
			{
				// Create a file with one block
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, filePath, 1L, ReplicationFactor, 1L);
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, filePath);
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
				// Decrease the replication factor, make sure the deleted replica
				// was not the one that lived on the rack with only one replica,
				// ie we should still have 2 racks after reducing the repl factor.
				ReplicationFactor = 2;
				NameNodeAdapter.SetReplication(ns, "/testFile", ReplicationFactor);
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/*
		* Test that when a block is replicated because a replica is lost due
		* to host failure the the rack policy is preserved.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplDueToNodeFailRespectsRackPolicy()
		{
			Configuration conf = GetConf();
			short ReplicationFactor = 3;
			Path filePath = new Path("/testFile");
			// Last datanode is on a different rack
			string[] racks = new string[] { "/rack1", "/rack1", "/rack1", "/rack2", "/rack2" };
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(racks.Length
				).Racks(racks).Build();
			FSNamesystem ns = cluster.GetNameNode().GetNamesystem();
			DatanodeManager dm = ns.GetBlockManager().GetDatanodeManager();
			try
			{
				// Create a file with one block with a replication factor of 2
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, filePath, 1L, ReplicationFactor, 1L);
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, filePath);
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
				// Make the last datanode look like it failed to heartbeat by 
				// calling removeDatanode and stopping it.
				AList<DataNode> datanodes = cluster.GetDataNodes();
				int idx = datanodes.Count - 1;
				DataNode dataNode = datanodes[idx];
				DatanodeID dnId = dataNode.GetDatanodeId();
				cluster.StopDataNode(idx);
				dm.RemoveDatanode(dnId);
				// The block should still have sufficient # replicas, across racks.
				// The last node may not have contained a replica, but if it did
				// it should have been replicated within the same rack.
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
				// Fail the last datanode again, it's also on rack2 so there is
				// only 1 rack for all the replicas
				datanodes = cluster.GetDataNodes();
				idx = datanodes.Count - 1;
				dataNode = datanodes[idx];
				dnId = dataNode.GetDatanodeId();
				cluster.StopDataNode(idx);
				dm.RemoveDatanode(dnId);
				// Make sure we have enough live replicas even though we are
				// short one rack and therefore need one replica
				DFSTestUtil.WaitForReplication(cluster, b, 1, ReplicationFactor, 1);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/*
		* Test that when the excess replicas of a block are reduced due to
		* a node re-joining the cluster the rack policy is not violated.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReduceReplFactorDueToRejoinRespectsRackPolicy()
		{
			Configuration conf = GetConf();
			short ReplicationFactor = 2;
			Path filePath = new Path("/testFile");
			// Last datanode is on a different rack
			string[] racks = new string[] { "/rack1", "/rack1", "/rack2" };
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(racks.Length
				).Racks(racks).Build();
			FSNamesystem ns = cluster.GetNameNode().GetNamesystem();
			DatanodeManager dm = ns.GetBlockManager().GetDatanodeManager();
			try
			{
				// Create a file with one block
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, filePath, 1L, ReplicationFactor, 1L);
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, filePath);
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
				// Make the last (cross rack) datanode look like it failed
				// to heartbeat by stopping it and calling removeDatanode.
				AList<DataNode> datanodes = cluster.GetDataNodes();
				NUnit.Framework.Assert.AreEqual(3, datanodes.Count);
				DataNode dataNode = datanodes[2];
				DatanodeID dnId = dataNode.GetDatanodeId();
				cluster.StopDataNode(2);
				dm.RemoveDatanode(dnId);
				// The block gets re-replicated to another datanode so it has a 
				// sufficient # replicas, but not across racks, so there should
				// be 1 rack, and 1 needed replica (even though there are 2 hosts 
				// available and only 2 replicas required).
				DFSTestUtil.WaitForReplication(cluster, b, 1, ReplicationFactor, 1);
				// Start the "failed" datanode, which has a replica so the block is
				// now over-replicated and therefore a replica should be removed but
				// not on the restarted datanode as that would violate the rack policy.
				string[] rack2 = new string[] { "/rack2" };
				cluster.StartDataNodes(conf, 1, true, null, rack2);
				cluster.WaitActive();
				// The block now has sufficient # replicas, across racks
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/*
		* Test that rack policy is still respected when blocks are replicated
		* due to node decommissioning.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeDecomissionRespectsRackPolicy()
		{
			Configuration conf = GetConf();
			short ReplicationFactor = 2;
			Path filePath = new Path("/testFile");
			// Configure an excludes file
			FileSystem localFileSys = FileSystem.GetLocal(conf);
			Path workingDir = localFileSys.GetWorkingDirectory();
			Path dir = new Path(workingDir, "build/test/data/temp/decommission");
			Path excludeFile = new Path(dir, "exclude");
			Path includeFile = new Path(dir, "include");
			NUnit.Framework.Assert.IsTrue(localFileSys.Mkdirs(dir));
			DFSTestUtil.WriteFile(localFileSys, excludeFile, string.Empty);
			DFSTestUtil.WriteFile(localFileSys, includeFile, string.Empty);
			conf.Set(DFSConfigKeys.DfsHostsExclude, excludeFile.ToUri().GetPath());
			conf.Set(DFSConfigKeys.DfsHosts, includeFile.ToUri().GetPath());
			// Two blocks and four racks
			string[] racks = new string[] { "/rack1", "/rack1", "/rack2", "/rack2" };
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(racks.Length
				).Racks(racks).Build();
			FSNamesystem ns = cluster.GetNameNode().GetNamesystem();
			try
			{
				// Create a file with one block
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, filePath, 1L, ReplicationFactor, 1L);
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, filePath);
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
				// Decommission one of the hosts with the block, this should cause 
				// the block to get replicated to another host on the same rack,
				// otherwise the rack policy is violated.
				BlockLocation[] locs = fs.GetFileBlockLocations(fs.GetFileStatus(filePath), 0, long.MaxValue
					);
				string name = locs[0].GetNames()[0];
				DFSTestUtil.WriteFile(localFileSys, excludeFile, name);
				ns.GetBlockManager().GetDatanodeManager().RefreshNodes(conf);
				DFSTestUtil.WaitForDecommission(fs, name);
				// Check the block still has sufficient # replicas across racks
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/*
		* Test that rack policy is still respected when blocks are replicated
		* due to node decommissioning, when the blocks are over-replicated.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeDecomissionWithOverreplicationRespectsRackPolicy()
		{
			Configuration conf = GetConf();
			short ReplicationFactor = 5;
			Path filePath = new Path("/testFile");
			// Configure an excludes file
			FileSystem localFileSys = FileSystem.GetLocal(conf);
			Path workingDir = localFileSys.GetWorkingDirectory();
			Path dir = new Path(workingDir, "build/test/data/temp/decommission");
			Path excludeFile = new Path(dir, "exclude");
			Path includeFile = new Path(dir, "include");
			NUnit.Framework.Assert.IsTrue(localFileSys.Mkdirs(dir));
			DFSTestUtil.WriteFile(localFileSys, excludeFile, string.Empty);
			DFSTestUtil.WriteFile(localFileSys, includeFile, string.Empty);
			conf.Set(DFSConfigKeys.DfsHosts, includeFile.ToUri().GetPath());
			conf.Set(DFSConfigKeys.DfsHostsExclude, excludeFile.ToUri().GetPath());
			// All hosts are on two racks, only one host on /rack2
			string[] racks = new string[] { "/rack1", "/rack2", "/rack1", "/rack1", "/rack1" };
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(racks.Length
				).Racks(racks).Build();
			FSNamesystem ns = cluster.GetNameNode().GetNamesystem();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, filePath, 1L, ReplicationFactor, 1L);
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, filePath);
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
				// Lower the replication factor so the blocks are over replicated
				ReplicationFactor = 2;
				fs.SetReplication(filePath, ReplicationFactor);
				// Decommission one of the hosts with the block that is not on
				// the lone host on rack2 (if we decomission that host it would
				// be impossible to respect the rack policy).
				BlockLocation[] locs = fs.GetFileBlockLocations(fs.GetFileStatus(filePath), 0, long.MaxValue
					);
				foreach (string top in locs[0].GetTopologyPaths())
				{
					if (!top.StartsWith("/rack2"))
					{
						string name = Sharpen.Runtime.Substring(top, "/rack1".Length + 1);
						DFSTestUtil.WriteFile(localFileSys, excludeFile, name);
						ns.GetBlockManager().GetDatanodeManager().RefreshNodes(conf);
						DFSTestUtil.WaitForDecommission(fs, name);
						break;
					}
				}
				// Check the block still has sufficient # replicas across racks,
				// ie we didn't remove the replica on the host on /rack1.
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
