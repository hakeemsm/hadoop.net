using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// The hotornot.com of unit tests: makes sure that the standby not only
	/// has namespace information, but also has the correct block reports, etc.
	/// </summary>
	public class TestStandbyIsHot
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.TestStandbyIsHot
			));

		private const string TestFileData = "hello highly available world";

		private const string TestFile = "/testStandbyIsHot";

		private static readonly Path TestFilePath = new Path(TestFile);

		static TestStandbyIsHot()
		{
			DFSTestUtil.SetNameNodeLogLevel(Level.All);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStandbyIsHot()
		{
			Configuration conf = new Configuration();
			// We read from the standby to watch block locations
			HAUtil.SetAllowStandbyReads(conf, true);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(3).Build();
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				NameNode nn1 = cluster.GetNameNode(0);
				NameNode nn2 = cluster.GetNameNode(1);
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				Sharpen.Thread.Sleep(1000);
				System.Console.Error.WriteLine("==================================");
				DFSTestUtil.WriteFile(fs, TestFilePath, TestFileData);
				// Have to force an edit log roll so that the standby catches up
				nn1.GetRpcServer().RollEditLog();
				System.Console.Error.WriteLine("==================================");
				// Block locations should show up on standby.
				Log.Info("Waiting for block locations to appear on standby node");
				WaitForBlockLocations(cluster, nn2, TestFile, 3);
				// Trigger immediate heartbeats and block reports so
				// that the active "trusts" all of the DNs
				cluster.TriggerHeartbeats();
				cluster.TriggerBlockReports();
				// Change replication
				Log.Info("Changing replication to 1");
				fs.SetReplication(TestFilePath, (short)1);
				BlockManagerTestUtil.ComputeAllPendingWork(nn1.GetNamesystem().GetBlockManager());
				WaitForBlockLocations(cluster, nn1, TestFile, 1);
				nn1.GetRpcServer().RollEditLog();
				Log.Info("Waiting for lowered replication to show up on standby");
				WaitForBlockLocations(cluster, nn2, TestFile, 1);
				// Change back to 3
				Log.Info("Changing replication to 3");
				fs.SetReplication(TestFilePath, (short)3);
				BlockManagerTestUtil.ComputeAllPendingWork(nn1.GetNamesystem().GetBlockManager());
				nn1.GetRpcServer().RollEditLog();
				Log.Info("Waiting for higher replication to show up on standby");
				WaitForBlockLocations(cluster, nn2, TestFile, 3);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Regression test for HDFS-2795:
		/// - Start an HA cluster with a DN.
		/// </summary>
		/// <remarks>
		/// Regression test for HDFS-2795:
		/// - Start an HA cluster with a DN.
		/// - Write several blocks to the FS with replication 1.
		/// - Shutdown the DN
		/// - Wait for the NNs to declare the DN dead. All blocks will be under-replicated.
		/// - Restart the DN.
		/// In the bug, the standby node would only very slowly notice the blocks returning
		/// to the cluster.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDatanodeRestarts()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, 1024);
			// We read from the standby to watch block locations
			HAUtil.SetAllowStandbyReads(conf, true);
			conf.SetLong(DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey, 0);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(1).Build();
			try
			{
				NameNode nn0 = cluster.GetNameNode(0);
				NameNode nn1 = cluster.GetNameNode(1);
				cluster.TransitionToActive(0);
				// Create 5 blocks.
				DFSTestUtil.CreateFile(cluster.GetFileSystem(0), TestFilePath, 5 * 1024, (short)1
					, 1L);
				HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
				// Stop the DN.
				DataNode dn = cluster.GetDataNodes()[0];
				string dnName = dn.GetDatanodeId().GetXferAddr();
				MiniDFSCluster.DataNodeProperties dnProps = cluster.StopDataNode(0);
				// Make sure both NNs register it as dead.
				BlockManagerTestUtil.NoticeDeadDatanode(nn0, dnName);
				BlockManagerTestUtil.NoticeDeadDatanode(nn1, dnName);
				BlockManagerTestUtil.UpdateState(nn0.GetNamesystem().GetBlockManager());
				BlockManagerTestUtil.UpdateState(nn1.GetNamesystem().GetBlockManager());
				NUnit.Framework.Assert.AreEqual(5, nn0.GetNamesystem().GetUnderReplicatedBlocks()
					);
				// The SBN will not have any blocks in its neededReplication queue
				// since the SBN doesn't process replication.
				NUnit.Framework.Assert.AreEqual(0, nn1.GetNamesystem().GetUnderReplicatedBlocks()
					);
				LocatedBlocks locs = nn1.GetRpcServer().GetBlockLocations(TestFile, 0, 1);
				NUnit.Framework.Assert.AreEqual("Standby should have registered that the block has no replicas"
					, 0, locs.Get(0).GetLocations().Length);
				cluster.RestartDataNode(dnProps);
				// Wait for both NNs to re-register the DN.
				cluster.WaitActive(0);
				cluster.WaitActive(1);
				BlockManagerTestUtil.UpdateState(nn0.GetNamesystem().GetBlockManager());
				BlockManagerTestUtil.UpdateState(nn1.GetNamesystem().GetBlockManager());
				NUnit.Framework.Assert.AreEqual(0, nn0.GetNamesystem().GetUnderReplicatedBlocks()
					);
				NUnit.Framework.Assert.AreEqual(0, nn1.GetNamesystem().GetUnderReplicatedBlocks()
					);
				locs = nn1.GetRpcServer().GetBlockLocations(TestFile, 0, 1);
				NUnit.Framework.Assert.AreEqual("Standby should have registered that the block has replicas again"
					, 1, locs.Get(0).GetLocations().Length);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		internal static void WaitForBlockLocations(MiniDFSCluster cluster, NameNode nn, string
			 path, int expectedReplicas)
		{
			GenericTestUtils.WaitFor(new _Supplier_204(nn, path, expectedReplicas, cluster), 
				500, 20000);
		}

		private sealed class _Supplier_204 : Supplier<bool>
		{
			public _Supplier_204(NameNode nn, string path, int expectedReplicas, MiniDFSCluster
				 cluster)
			{
				this.nn = nn;
				this.path = path;
				this.expectedReplicas = expectedReplicas;
				this.cluster = cluster;
			}

			public bool Get()
			{
				try
				{
					LocatedBlocks locs = NameNodeAdapter.GetBlockLocations(nn, path, 0, 1000);
					DatanodeInfo[] dnis = locs.GetLastLocatedBlock().GetLocations();
					foreach (DatanodeInfo dni in dnis)
					{
						NUnit.Framework.Assert.IsNotNull(dni);
					}
					int numReplicas = dnis.Length;
					Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.TestStandbyIsHot.Log.Info("Got " + numReplicas
						 + " locs: " + locs);
					if (numReplicas > expectedReplicas)
					{
						cluster.TriggerDeletionReports();
					}
					cluster.TriggerHeartbeats();
					return numReplicas == expectedReplicas;
				}
				catch (IOException e)
				{
					Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.TestStandbyIsHot.Log.Warn("No block locations yet: "
						 + e.Message);
					return false;
				}
			}

			private readonly NameNode nn;

			private readonly string path;

			private readonly int expectedReplicas;

			private readonly MiniDFSCluster cluster;
		}
	}
}
