using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestProcessCorruptBlocks
	{
		/// <summary>
		/// The corrupt block has to be removed when the number of valid replicas
		/// matches replication factor for the file.
		/// </summary>
		/// <remarks>
		/// The corrupt block has to be removed when the number of valid replicas
		/// matches replication factor for the file. In this the above condition is
		/// tested by reducing the replication factor
		/// The test strategy :
		/// Bring up Cluster with 3 DataNodes
		/// Create a file of replication factor 3
		/// Corrupt one replica of a block of the file
		/// Verify that there are still 2 good replicas and 1 corrupt replica
		/// (corrupt replica should not be removed since number of good
		/// replicas (2) is less than replication factor (3))
		/// Set the replication factor to 2
		/// Verify that the corrupt replica is removed.
		/// (corrupt replica  should not be removed since number of good
		/// replicas (2) is equal to replication factor (2))
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWhenDecreasingReplication()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000L);
			conf.Set(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, Sharpen.Extensions.ToString
				(2));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			FileSystem fs = cluster.GetFileSystem();
			FSNamesystem namesystem = cluster.GetNamesystem();
			try
			{
				Path fileName = new Path("/foo1");
				DFSTestUtil.CreateFile(fs, fileName, 2, (short)3, 0L);
				DFSTestUtil.WaitReplication(fs, fileName, (short)3);
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, fileName);
				CorruptBlock(cluster, fs, fileName, 0, block);
				DFSTestUtil.WaitReplication(fs, fileName, (short)2);
				NUnit.Framework.Assert.AreEqual(2, CountReplicas(namesystem, block).LiveReplicas(
					));
				NUnit.Framework.Assert.AreEqual(1, CountReplicas(namesystem, block).CorruptReplicas
					());
				namesystem.SetReplication(fileName.ToString(), (short)2);
				// wait for 3 seconds so that all block reports are processed.
				try
				{
					Sharpen.Thread.Sleep(3000);
				}
				catch (Exception)
				{
				}
				NUnit.Framework.Assert.AreEqual(2, CountReplicas(namesystem, block).LiveReplicas(
					));
				NUnit.Framework.Assert.AreEqual(0, CountReplicas(namesystem, block).CorruptReplicas
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// The corrupt block has to be removed when the number of valid replicas
		/// matches replication factor for the file.
		/// </summary>
		/// <remarks>
		/// The corrupt block has to be removed when the number of valid replicas
		/// matches replication factor for the file. In this test, the above
		/// condition is achieved by increasing the number of good replicas by
		/// replicating on a new Datanode.
		/// The test strategy :
		/// Bring up Cluster with 3 DataNodes
		/// Create a file  of replication factor 3
		/// Corrupt one replica of a block of the file
		/// Verify that there are still 2 good replicas and 1 corrupt replica
		/// (corrupt replica should not be removed since number of good replicas
		/// (2) is less  than replication factor (3))
		/// Start a new data node
		/// Verify that the a new replica is created and corrupt replica is
		/// removed.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestByAddingAnExtraDataNode()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000L);
			conf.Set(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, Sharpen.Extensions.ToString
				(2));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
			FileSystem fs = cluster.GetFileSystem();
			FSNamesystem namesystem = cluster.GetNamesystem();
			MiniDFSCluster.DataNodeProperties dnPropsFourth = cluster.StopDataNode(3);
			try
			{
				Path fileName = new Path("/foo1");
				DFSTestUtil.CreateFile(fs, fileName, 2, (short)3, 0L);
				DFSTestUtil.WaitReplication(fs, fileName, (short)3);
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, fileName);
				CorruptBlock(cluster, fs, fileName, 0, block);
				DFSTestUtil.WaitReplication(fs, fileName, (short)2);
				NUnit.Framework.Assert.AreEqual(2, CountReplicas(namesystem, block).LiveReplicas(
					));
				NUnit.Framework.Assert.AreEqual(1, CountReplicas(namesystem, block).CorruptReplicas
					());
				cluster.RestartDataNode(dnPropsFourth);
				DFSTestUtil.WaitReplication(fs, fileName, (short)3);
				NUnit.Framework.Assert.AreEqual(3, CountReplicas(namesystem, block).LiveReplicas(
					));
				NUnit.Framework.Assert.AreEqual(0, CountReplicas(namesystem, block).CorruptReplicas
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// The corrupt block has to be removed when the number of valid replicas
		/// matches replication factor for the file.
		/// </summary>
		/// <remarks>
		/// The corrupt block has to be removed when the number of valid replicas
		/// matches replication factor for the file. The above condition should hold
		/// true as long as there is one good replica. This test verifies that.
		/// The test strategy :
		/// Bring up Cluster with 2 DataNodes
		/// Create a file of replication factor 2
		/// Corrupt one replica of a block of the file
		/// Verify that there is  one good replicas and 1 corrupt replica
		/// (corrupt replica should not be removed since number of good
		/// replicas (1) is less than replication factor (2)).
		/// Set the replication factor to 1
		/// Verify that the corrupt replica is removed.
		/// (corrupt replica should  be removed since number of good
		/// replicas (1) is equal to replication factor (1))
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestWithReplicationFactorAsOne()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000L);
			conf.Set(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, Sharpen.Extensions.ToString
				(2));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FileSystem fs = cluster.GetFileSystem();
			FSNamesystem namesystem = cluster.GetNamesystem();
			try
			{
				Path fileName = new Path("/foo1");
				DFSTestUtil.CreateFile(fs, fileName, 2, (short)2, 0L);
				DFSTestUtil.WaitReplication(fs, fileName, (short)2);
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, fileName);
				CorruptBlock(cluster, fs, fileName, 0, block);
				DFSTestUtil.WaitReplication(fs, fileName, (short)1);
				NUnit.Framework.Assert.AreEqual(1, CountReplicas(namesystem, block).LiveReplicas(
					));
				NUnit.Framework.Assert.AreEqual(1, CountReplicas(namesystem, block).CorruptReplicas
					());
				namesystem.SetReplication(fileName.ToString(), (short)1);
				// wait for 3 seconds so that all block reports are processed.
				for (int i = 0; i < 10; i++)
				{
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
					if (CountReplicas(namesystem, block).CorruptReplicas() == 0)
					{
						break;
					}
				}
				NUnit.Framework.Assert.AreEqual(1, CountReplicas(namesystem, block).LiveReplicas(
					));
				NUnit.Framework.Assert.AreEqual(0, CountReplicas(namesystem, block).CorruptReplicas
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>None of the blocks can be removed if all blocks are corrupt.</summary>
		/// <remarks>
		/// None of the blocks can be removed if all blocks are corrupt.
		/// The test strategy :
		/// Bring up Cluster with 3 DataNodes
		/// Create a file of replication factor 3
		/// Corrupt all three replicas
		/// Verify that all replicas are corrupt and 3 replicas are present.
		/// Set the replication factor to 1
		/// Verify that all replicas are corrupt and 3 replicas are present.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithAllCorruptReplicas()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000L);
			conf.Set(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, Sharpen.Extensions.ToString
				(2));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			FileSystem fs = cluster.GetFileSystem();
			FSNamesystem namesystem = cluster.GetNamesystem();
			try
			{
				Path fileName = new Path("/foo1");
				DFSTestUtil.CreateFile(fs, fileName, 2, (short)3, 0L);
				DFSTestUtil.WaitReplication(fs, fileName, (short)3);
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, fileName);
				CorruptBlock(cluster, fs, fileName, 0, block);
				CorruptBlock(cluster, fs, fileName, 1, block);
				CorruptBlock(cluster, fs, fileName, 2, block);
				// wait for 3 seconds so that all block reports are processed.
				try
				{
					Sharpen.Thread.Sleep(3000);
				}
				catch (Exception)
				{
				}
				NUnit.Framework.Assert.AreEqual(0, CountReplicas(namesystem, block).LiveReplicas(
					));
				NUnit.Framework.Assert.AreEqual(3, CountReplicas(namesystem, block).CorruptReplicas
					());
				namesystem.SetReplication(fileName.ToString(), (short)1);
				// wait for 3 seconds so that all block reports are processed.
				try
				{
					Sharpen.Thread.Sleep(3000);
				}
				catch (Exception)
				{
				}
				NUnit.Framework.Assert.AreEqual(0, CountReplicas(namesystem, block).LiveReplicas(
					));
				NUnit.Framework.Assert.AreEqual(3, CountReplicas(namesystem, block).CorruptReplicas
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private static NumberReplicas CountReplicas(FSNamesystem namesystem, ExtendedBlock
			 block)
		{
			return namesystem.GetBlockManager().CountNodes(block.GetLocalBlock());
		}

		/// <exception cref="System.IO.IOException"/>
		private void CorruptBlock(MiniDFSCluster cluster, FileSystem fs, Path fileName, int
			 dnIndex, ExtendedBlock block)
		{
			// corrupt the block on datanode dnIndex
			// the indexes change once the nodes are restarted.
			// But the datadirectory will not change
			NUnit.Framework.Assert.IsTrue(cluster.CorruptReplica(dnIndex, block));
			MiniDFSCluster.DataNodeProperties dnProps = cluster.StopDataNode(0);
			// Each datanode has multiple data dirs, check each
			for (int dirIndex = 0; dirIndex < 2; dirIndex++)
			{
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				FilePath storageDir = cluster.GetStorageDir(dnIndex, dirIndex);
				FilePath dataDir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
				FilePath scanLogFile = new FilePath(dataDir, "dncp_block_verification.log.curr");
				if (scanLogFile.Exists())
				{
					// wait for one minute for deletion to succeed;
					for (int i = 0; !scanLogFile.Delete(); i++)
					{
						NUnit.Framework.Assert.IsTrue("Could not delete log file in one minute", i < 60);
						try
						{
							Sharpen.Thread.Sleep(1000);
						}
						catch (Exception)
						{
						}
					}
				}
			}
			// restart the detained so the corrupt replica will be detected
			cluster.RestartDataNode(dnProps);
		}
	}
}
