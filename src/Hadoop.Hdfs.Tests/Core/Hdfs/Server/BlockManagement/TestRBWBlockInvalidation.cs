using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>Test when RBW block is removed.</summary>
	/// <remarks>
	/// Test when RBW block is removed. Invalidation of the corrupted block happens
	/// and then the under replicated block gets replicated to the datanode.
	/// </remarks>
	public class TestRBWBlockInvalidation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestRBWBlockInvalidation
			));

		private static NumberReplicas CountReplicas(FSNamesystem namesystem, ExtendedBlock
			 block)
		{
			return namesystem.GetBlockManager().CountNodes(block.GetLocalBlock());
		}

		/// <summary>
		/// Test when a block's replica is removed from RBW folder in one of the
		/// datanode, namenode should ask to invalidate that corrupted block and
		/// schedule replication for one more replica for that under replicated block.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestBlockInvalidationWhenRBWReplicaMissedInDN()
		{
			// This test cannot pass on Windows due to file locking enforcement.  It will
			// reject the attempt to delete the block file from the RBW folder.
			Assume.AssumeTrue(!Path.Windows);
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 2);
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 300);
			conf.SetLong(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, 1);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FSDataOutputStream @out = null;
			try
			{
				FSNamesystem namesystem = cluster.GetNamesystem();
				FileSystem fs = cluster.GetFileSystem();
				Path testPath = new Path("/tmp/TestRBWBlockInvalidation", "foo1");
				@out = fs.Create(testPath, (short)2);
				@out.WriteBytes("HDFS-3157: " + testPath);
				@out.Hsync();
				cluster.StartDataNodes(conf, 1, true, null, null, null);
				string bpid = namesystem.GetBlockPoolId();
				ExtendedBlock blk = DFSTestUtil.GetFirstBlock(fs, testPath);
				Block block = blk.GetLocalBlock();
				DataNode dn = cluster.GetDataNodes()[0];
				// Delete partial block and its meta information from the RBW folder
				// of first datanode.
				FilePath blockFile = DataNodeTestUtils.GetBlockFile(dn, bpid, block);
				FilePath metaFile = DataNodeTestUtils.GetMetaFile(dn, bpid, block);
				NUnit.Framework.Assert.IsTrue("Could not delete the block file from the RBW folder"
					, blockFile.Delete());
				NUnit.Framework.Assert.IsTrue("Could not delete the block meta file from the RBW folder"
					, metaFile.Delete());
				@out.Close();
				int liveReplicas = 0;
				while (true)
				{
					if ((liveReplicas = CountReplicas(namesystem, blk).LiveReplicas()) < 2)
					{
						// This confirms we have a corrupt replica
						Log.Info("Live Replicas after corruption: " + liveReplicas);
						break;
					}
					Sharpen.Thread.Sleep(100);
				}
				NUnit.Framework.Assert.AreEqual("There should be less than 2 replicas in the " + 
					"liveReplicasMap", 1, liveReplicas);
				while (true)
				{
					if ((liveReplicas = CountReplicas(namesystem, blk).LiveReplicas()) > 1)
					{
						//Wait till the live replica count becomes equal to Replication Factor
						Log.Info("Live Replicas after Rereplication: " + liveReplicas);
						break;
					}
					Sharpen.Thread.Sleep(100);
				}
				NUnit.Framework.Assert.AreEqual("There should be two live replicas", 2, liveReplicas
					);
				while (true)
				{
					Sharpen.Thread.Sleep(100);
					if (CountReplicas(namesystem, blk).CorruptReplicas() == 0)
					{
						Log.Info("Corrupt Replicas becomes 0");
						break;
					}
				}
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Regression test for HDFS-4799, a case where, upon restart, if there
		/// were RWR replicas with out-of-date genstamps, the NN could accidentally
		/// delete good replicas instead of the bad replicas.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRWRInvalidation()
		{
			Configuration conf = new HdfsConfiguration();
			// Set the deletion policy to be randomized rather than the default.
			// The default is based on disk space, which isn't controllable
			// in the context of the test, whereas a random one is more accurate
			// to what is seen in real clusters (nodes have random amounts of free
			// space)
			conf.SetClass(DFSConfigKeys.DfsBlockReplicatorClassnameKey, typeof(TestDNFencing.RandomDeleterPolicy
				), typeof(BlockPlacementPolicy));
			// Speed up the test a bit with faster heartbeats.
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			// Test with a bunch of separate files, since otherwise the test may
			// fail just due to "good luck", even if a bug is present.
			IList<Path> testPaths = Lists.NewArrayList();
			for (int i = 0; i < 10; i++)
			{
				testPaths.AddItem(new Path("/test" + i));
			}
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			try
			{
				IList<FSDataOutputStream> streams = Lists.NewArrayList();
				try
				{
					// Open the test files and write some data to each
					foreach (Path path in testPaths)
					{
						FSDataOutputStream @out = cluster.GetFileSystem().Create(path, (short)2);
						streams.AddItem(@out);
						@out.WriteBytes("old gs data\n");
						@out.Hflush();
					}
					// Shutdown one of the nodes in the pipeline
					MiniDFSCluster.DataNodeProperties oldGenstampNode = cluster.StopDataNode(0);
					// Write some more data and flush again. This data will only
					// be in the latter genstamp copy of the blocks.
					for (int i_1 = 0; i_1 < streams.Count; i_1++)
					{
						Path path_1 = testPaths[i_1];
						FSDataOutputStream @out = streams[i_1];
						@out.WriteBytes("new gs data\n");
						@out.Hflush();
						// Set replication so that only one node is necessary for this block,
						// and close it.
						cluster.GetFileSystem().SetReplication(path_1, (short)1);
						@out.Close();
					}
					// Upon restart, there will be two replicas, one with an old genstamp
					// and one current copy. This test wants to ensure that the old genstamp
					// copy is the one that is deleted.
					Log.Info("=========================== restarting cluster");
					MiniDFSCluster.DataNodeProperties otherNode = cluster.StopDataNode(0);
					cluster.RestartNameNode();
					// Restart the datanode with the corrupt replica first.
					cluster.RestartDataNode(oldGenstampNode);
					cluster.WaitActive();
					// Then the other node
					cluster.RestartDataNode(otherNode);
					cluster.WaitActive();
					// Compute and send invalidations, waiting until they're fully processed.
					cluster.GetNameNode().GetNamesystem().GetBlockManager().ComputeInvalidateWork(2);
					cluster.TriggerHeartbeats();
					HATestUtil.WaitForDNDeletions(cluster);
					cluster.TriggerDeletionReports();
					// Make sure we can still read the blocks.
					foreach (Path path_2 in testPaths)
					{
						string ret = DFSTestUtil.ReadFile(cluster.GetFileSystem(), path_2);
						NUnit.Framework.Assert.AreEqual("old gs data\n" + "new gs data\n", ret);
					}
				}
				finally
				{
					IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(streams, new IDisposable[0]));
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
