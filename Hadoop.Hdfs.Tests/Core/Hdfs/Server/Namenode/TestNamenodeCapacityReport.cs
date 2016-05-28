using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This tests InterDataNodeProtocol for block handling.</summary>
	public class TestNamenodeCapacityReport
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestNamenodeCapacityReport
			));

		/// <summary>The following test first creates a file.</summary>
		/// <remarks>
		/// The following test first creates a file.
		/// It verifies the block information from a datanode.
		/// Then, it updates the block with new information and verifies again.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVolumeSize()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			// Set aside fifth of the total capacity as reserved
			long reserved = 10000;
			conf.SetLong(DFSConfigKeys.DfsDatanodeDuReservedKey, reserved);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				FSNamesystem namesystem = cluster.GetNamesystem();
				DatanodeManager dm = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
					();
				// Ensure the data reported for each data node is right
				IList<DatanodeDescriptor> live = new AList<DatanodeDescriptor>();
				IList<DatanodeDescriptor> dead = new AList<DatanodeDescriptor>();
				dm.FetchDatanodes(live, dead, false);
				NUnit.Framework.Assert.IsTrue(live.Count == 1);
				long used;
				long remaining;
				long configCapacity;
				long nonDFSUsed;
				long bpUsed;
				float percentUsed;
				float percentRemaining;
				float percentBpUsed;
				foreach (DatanodeDescriptor datanode in live)
				{
					used = datanode.GetDfsUsed();
					remaining = datanode.GetRemaining();
					nonDFSUsed = datanode.GetNonDfsUsed();
					configCapacity = datanode.GetCapacity();
					percentUsed = datanode.GetDfsUsedPercent();
					percentRemaining = datanode.GetRemainingPercent();
					bpUsed = datanode.GetBlockPoolUsed();
					percentBpUsed = datanode.GetBlockPoolUsedPercent();
					Log.Info("Datanode configCapacity " + configCapacity + " used " + used + " non DFS used "
						 + nonDFSUsed + " remaining " + remaining + " perentUsed " + percentUsed + " percentRemaining "
						 + percentRemaining);
					NUnit.Framework.Assert.IsTrue(configCapacity == (used + remaining + nonDFSUsed));
					NUnit.Framework.Assert.IsTrue(percentUsed == DFSUtil.GetPercentUsed(used, configCapacity
						));
					NUnit.Framework.Assert.IsTrue(percentRemaining == DFSUtil.GetPercentRemaining(remaining
						, configCapacity));
					NUnit.Framework.Assert.IsTrue(percentBpUsed == DFSUtil.GetPercentUsed(bpUsed, configCapacity
						));
				}
				DF df = new DF(new FilePath(cluster.GetDataDirectory()), conf);
				//
				// Currently two data directories are created by the data node
				// in the MiniDFSCluster. This results in each data directory having
				// capacity equals to the disk capacity of the data directory.
				// Hence the capacity reported by the data node is twice the disk space
				// the disk capacity
				//
				// So multiply the disk capacity and reserved space by two 
				// for accommodating it
				//
				int numOfDataDirs = 2;
				long diskCapacity = numOfDataDirs * df.GetCapacity();
				reserved *= numOfDataDirs;
				configCapacity = namesystem.GetCapacityTotal();
				used = namesystem.GetCapacityUsed();
				nonDFSUsed = namesystem.GetNonDfsUsedSpace();
				remaining = namesystem.GetCapacityRemaining();
				percentUsed = namesystem.GetPercentUsed();
				percentRemaining = namesystem.GetPercentRemaining();
				bpUsed = namesystem.GetBlockPoolUsedSpace();
				percentBpUsed = namesystem.GetPercentBlockPoolUsed();
				Log.Info("Data node directory " + cluster.GetDataDirectory());
				Log.Info("Name node diskCapacity " + diskCapacity + " configCapacity " + configCapacity
					 + " reserved " + reserved + " used " + used + " remaining " + remaining + " nonDFSUsed "
					 + nonDFSUsed + " remaining " + remaining + " percentUsed " + percentUsed + " percentRemaining "
					 + percentRemaining + " bpUsed " + bpUsed + " percentBpUsed " + percentBpUsed);
				// Ensure new total capacity reported excludes the reserved space
				NUnit.Framework.Assert.IsTrue(configCapacity == diskCapacity - reserved);
				// Ensure new total capacity reported excludes the reserved space
				NUnit.Framework.Assert.IsTrue(configCapacity == (used + remaining + nonDFSUsed));
				// Ensure percent used is calculated based on used and present capacity
				NUnit.Framework.Assert.IsTrue(percentUsed == DFSUtil.GetPercentUsed(used, configCapacity
					));
				// Ensure percent used is calculated based on used and present capacity
				NUnit.Framework.Assert.IsTrue(percentBpUsed == DFSUtil.GetPercentUsed(bpUsed, configCapacity
					));
				// Ensure percent used is calculated based on used and present capacity
				NUnit.Framework.Assert.IsTrue(percentRemaining == ((float)remaining * 100.0f) / (
					float)configCapacity);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private const float Epsilon = 0.0001f;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestXceiverCount()
		{
			Configuration conf = new HdfsConfiguration();
			// retry one time, if close fails
			conf.SetInt(DFSConfigKeys.DfsClientBlockWriteLocatefollowingblockRetriesKey, 1);
			MiniDFSCluster cluster = null;
			int nodes = 8;
			int fileCount = 5;
			short fileRepl = 3;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(nodes).Build();
				cluster.WaitActive();
				FSNamesystem namesystem = cluster.GetNamesystem();
				DatanodeManager dnm = namesystem.GetBlockManager().GetDatanodeManager();
				IList<DataNode> datanodes = cluster.GetDataNodes();
				DistributedFileSystem fs = cluster.GetFileSystem();
				// trigger heartbeats in case not already sent
				TriggerHeartbeats(datanodes);
				// check that all nodes are live and in service
				int expectedTotalLoad = nodes;
				// xceiver server adds 1 to load
				int expectedInServiceNodes = nodes;
				int expectedInServiceLoad = nodes;
				CheckClusterHealth(nodes, namesystem, expectedTotalLoad, expectedInServiceNodes, 
					expectedInServiceLoad);
				// shutdown half the nodes and force a heartbeat check to ensure
				// counts are accurate
				for (int i = 0; i < nodes / 2; i++)
				{
					DataNode dn = datanodes[i];
					DatanodeDescriptor dnd = dnm.GetDatanode(dn.GetDatanodeId());
					dn.Shutdown();
					DFSTestUtil.SetDatanodeDead(dnd);
					BlockManagerTestUtil.CheckHeartbeat(namesystem.GetBlockManager());
					//Verify decommission of dead node won't impact nodesInService metrics.
					dnm.GetDecomManager().StartDecommission(dnd);
					expectedInServiceNodes--;
					NUnit.Framework.Assert.AreEqual(expectedInServiceNodes, namesystem.GetNumLiveDataNodes
						());
					NUnit.Framework.Assert.AreEqual(expectedInServiceNodes, GetNumDNInService(namesystem
						));
					//Verify recommission of dead node won't impact nodesInService metrics.
					dnm.GetDecomManager().StopDecommission(dnd);
					NUnit.Framework.Assert.AreEqual(expectedInServiceNodes, GetNumDNInService(namesystem
						));
				}
				// restart the nodes to verify that counts are correct after
				// node re-registration 
				cluster.RestartDataNodes();
				cluster.WaitActive();
				datanodes = cluster.GetDataNodes();
				expectedInServiceNodes = nodes;
				NUnit.Framework.Assert.AreEqual(nodes, datanodes.Count);
				CheckClusterHealth(nodes, namesystem, expectedTotalLoad, expectedInServiceNodes, 
					expectedInServiceLoad);
				// create streams and hsync to force datastreamers to start
				DFSOutputStream[] streams = new DFSOutputStream[fileCount];
				for (int i_1 = 0; i_1 < fileCount; i_1++)
				{
					streams[i_1] = (DFSOutputStream)fs.Create(new Path("/f" + i_1), fileRepl).GetWrappedStream
						();
					streams[i_1].Write(Sharpen.Runtime.GetBytesForString("1"));
					streams[i_1].Hsync();
					// the load for writers is 2 because both the write xceiver & packet
					// responder threads are counted in the load
					expectedTotalLoad += 2 * fileRepl;
					expectedInServiceLoad += 2 * fileRepl;
				}
				// force nodes to send load update
				TriggerHeartbeats(datanodes);
				CheckClusterHealth(nodes, namesystem, expectedTotalLoad, expectedInServiceNodes, 
					expectedInServiceLoad);
				// decomm a few nodes, substract their load from the expected load,
				// trigger heartbeat to force load update
				for (int i_2 = 0; i_2 < fileRepl; i_2++)
				{
					expectedInServiceNodes--;
					DatanodeDescriptor dnd = dnm.GetDatanode(datanodes[i_2].GetDatanodeId());
					expectedInServiceLoad -= dnd.GetXceiverCount();
					dnm.GetDecomManager().StartDecommission(dnd);
					DataNodeTestUtils.TriggerHeartbeat(datanodes[i_2]);
					Sharpen.Thread.Sleep(100);
					CheckClusterHealth(nodes, namesystem, expectedTotalLoad, expectedInServiceNodes, 
						expectedInServiceLoad);
				}
				// check expected load while closing each stream.  recalc expected
				// load based on whether the nodes in the pipeline are decomm
				for (int i_3 = 0; i_3 < fileCount; i_3++)
				{
					int decomm = 0;
					foreach (DatanodeInfo dni in streams[i_3].GetPipeline())
					{
						DatanodeDescriptor dnd = dnm.GetDatanode(dni);
						expectedTotalLoad -= 2;
						if (dnd.IsDecommissionInProgress() || dnd.IsDecommissioned())
						{
							decomm++;
						}
						else
						{
							expectedInServiceLoad -= 2;
						}
					}
					try
					{
						streams[i_3].Close();
					}
					catch (IOException ioe)
					{
						// nodes will go decommissioned even if there's a UC block whose
						// other locations are decommissioned too.  we'll ignore that
						// bug for now
						if (decomm < fileRepl)
						{
							throw;
						}
					}
					TriggerHeartbeats(datanodes);
					// verify node count and loads 
					CheckClusterHealth(nodes, namesystem, expectedTotalLoad, expectedInServiceNodes, 
						expectedInServiceLoad);
				}
				// shutdown each node, verify node counts based on decomm state
				for (int i_4 = 0; i_4 < nodes; i_4++)
				{
					DataNode dn = datanodes[i_4];
					dn.Shutdown();
					// force it to appear dead so live count decreases
					DatanodeDescriptor dnDesc = dnm.GetDatanode(dn.GetDatanodeId());
					DFSTestUtil.SetDatanodeDead(dnDesc);
					BlockManagerTestUtil.CheckHeartbeat(namesystem.GetBlockManager());
					NUnit.Framework.Assert.AreEqual(nodes - 1 - i_4, namesystem.GetNumLiveDataNodes()
						);
					// first few nodes are already out of service
					if (i_4 >= fileRepl)
					{
						expectedInServiceNodes--;
					}
					NUnit.Framework.Assert.AreEqual(expectedInServiceNodes, GetNumDNInService(namesystem
						));
					// live nodes always report load of 1.  no nodes is load 0
					double expectedXceiverAvg = (i_4 == nodes - 1) ? 0.0 : 1.0;
					NUnit.Framework.Assert.AreEqual((double)expectedXceiverAvg, GetInServiceXceiverAverage
						(namesystem), Epsilon);
				}
				// final sanity check
				CheckClusterHealth(0, namesystem, 0.0, 0, 0.0);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private static void CheckClusterHealth(int numOfLiveNodes, FSNamesystem namesystem
			, double expectedTotalLoad, int expectedInServiceNodes, double expectedInServiceLoad
			)
		{
			NUnit.Framework.Assert.AreEqual(numOfLiveNodes, namesystem.GetNumLiveDataNodes());
			NUnit.Framework.Assert.AreEqual(expectedInServiceNodes, GetNumDNInService(namesystem
				));
			NUnit.Framework.Assert.AreEqual(expectedTotalLoad, namesystem.GetTotalLoad(), Epsilon
				);
			if (expectedInServiceNodes != 0)
			{
				NUnit.Framework.Assert.AreEqual(expectedInServiceLoad / expectedInServiceNodes, GetInServiceXceiverAverage
					(namesystem), Epsilon);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(0.0, GetInServiceXceiverAverage(namesystem), Epsilon
					);
			}
		}

		private static int GetNumDNInService(FSNamesystem fsn)
		{
			return fsn.GetBlockManager().GetDatanodeManager().GetFSClusterStats().GetNumDatanodesInService
				();
		}

		private static double GetInServiceXceiverAverage(FSNamesystem fsn)
		{
			return fsn.GetBlockManager().GetDatanodeManager().GetFSClusterStats().GetInServiceXceiverAverage
				();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void TriggerHeartbeats(IList<DataNode> datanodes)
		{
			foreach (DataNode dn in datanodes)
			{
				DataNodeTestUtils.TriggerHeartbeat(dn);
			}
			Sharpen.Thread.Sleep(100);
		}
	}
}
