using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	public class TestNetworkTopology
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestNetworkTopology));

		private static readonly NetworkTopology cluster = new NetworkTopology();

		private DatanodeDescriptor[] dataNodes;

		[SetUp]
		public virtual void SetupDatanodes()
		{
			dataNodes = new DatanodeDescriptor[] { DFSTestUtil.GetDatanodeDescriptor("1.1.1.1"
				, "/d1/r1"), DFSTestUtil.GetDatanodeDescriptor("2.2.2.2", "/d1/r1"), DFSTestUtil
				.GetDatanodeDescriptor("3.3.3.3", "/d1/r2"), DFSTestUtil.GetDatanodeDescriptor("4.4.4.4"
				, "/d1/r2"), DFSTestUtil.GetDatanodeDescriptor("5.5.5.5", "/d1/r2"), DFSTestUtil
				.GetDatanodeDescriptor("6.6.6.6", "/d2/r3"), DFSTestUtil.GetDatanodeDescriptor("7.7.7.7"
				, "/d2/r3"), DFSTestUtil.GetDatanodeDescriptor("8.8.8.8", "/d2/r3"), DFSTestUtil
				.GetDatanodeDescriptor("9.9.9.9", "/d3/r1"), DFSTestUtil.GetDatanodeDescriptor("10.10.10.10"
				, "/d3/r1"), DFSTestUtil.GetDatanodeDescriptor("11.11.11.11", "/d3/r1"), DFSTestUtil
				.GetDatanodeDescriptor("12.12.12.12", "/d3/r2"), DFSTestUtil.GetDatanodeDescriptor
				("13.13.13.13", "/d3/r2"), DFSTestUtil.GetDatanodeDescriptor("14.14.14.14", "/d4/r1"
				), DFSTestUtil.GetDatanodeDescriptor("15.15.15.15", "/d4/r1"), DFSTestUtil.GetDatanodeDescriptor
				("16.16.16.16", "/d4/r1"), DFSTestUtil.GetDatanodeDescriptor("17.17.17.17", "/d4/r1"
				), DFSTestUtil.GetDatanodeDescriptor("18.18.18.18", "/d4/r1"), DFSTestUtil.GetDatanodeDescriptor
				("19.19.19.19", "/d4/r1"), DFSTestUtil.GetDatanodeDescriptor("20.20.20.20", "/d4/r1"
				) };
			for (int i = 0; i < dataNodes.Length; i++)
			{
				cluster.Add(dataNodes[i]);
			}
			dataNodes[9].SetDecommissioned();
			dataNodes[10].SetDecommissioned();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContains()
		{
			DatanodeDescriptor nodeNotInMap = DFSTestUtil.GetDatanodeDescriptor("8.8.8.8", "/d2/r4"
				);
			for (int i = 0; i < dataNodes.Length; i++)
			{
				NUnit.Framework.Assert.IsTrue(cluster.Contains(dataNodes[i]));
			}
			NUnit.Framework.Assert.IsFalse(cluster.Contains(nodeNotInMap));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNumOfChildren()
		{
			NUnit.Framework.Assert.AreEqual(cluster.GetNumOfLeaves(), dataNodes.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateInvalidTopology()
		{
			NetworkTopology invalCluster = new NetworkTopology();
			DatanodeDescriptor[] invalDataNodes = new DatanodeDescriptor[] { DFSTestUtil.GetDatanodeDescriptor
				("1.1.1.1", "/d1/r1"), DFSTestUtil.GetDatanodeDescriptor("2.2.2.2", "/d1/r1"), DFSTestUtil
				.GetDatanodeDescriptor("3.3.3.3", "/d1") };
			invalCluster.Add(invalDataNodes[0]);
			invalCluster.Add(invalDataNodes[1]);
			try
			{
				invalCluster.Add(invalDataNodes[2]);
				NUnit.Framework.Assert.Fail("expected InvalidTopologyException");
			}
			catch (NetworkTopology.InvalidTopologyException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("Failed to add "));
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("You cannot have a rack and a non-rack node at the same "
					 + "level of the network topology."));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRacks()
		{
			NUnit.Framework.Assert.AreEqual(cluster.GetNumOfRacks(), 6);
			NUnit.Framework.Assert.IsTrue(cluster.IsOnSameRack(dataNodes[0], dataNodes[1]));
			NUnit.Framework.Assert.IsFalse(cluster.IsOnSameRack(dataNodes[1], dataNodes[2]));
			NUnit.Framework.Assert.IsTrue(cluster.IsOnSameRack(dataNodes[2], dataNodes[3]));
			NUnit.Framework.Assert.IsTrue(cluster.IsOnSameRack(dataNodes[3], dataNodes[4]));
			NUnit.Framework.Assert.IsFalse(cluster.IsOnSameRack(dataNodes[4], dataNodes[5]));
			NUnit.Framework.Assert.IsTrue(cluster.IsOnSameRack(dataNodes[5], dataNodes[6]));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetDistance()
		{
			NUnit.Framework.Assert.AreEqual(cluster.GetDistance(dataNodes[0], dataNodes[0]), 
				0);
			NUnit.Framework.Assert.AreEqual(cluster.GetDistance(dataNodes[0], dataNodes[1]), 
				2);
			NUnit.Framework.Assert.AreEqual(cluster.GetDistance(dataNodes[0], dataNodes[3]), 
				4);
			NUnit.Framework.Assert.AreEqual(cluster.GetDistance(dataNodes[0], dataNodes[6]), 
				6);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSortByDistance()
		{
			DatanodeDescriptor[] testNodes = new DatanodeDescriptor[3];
			// array contains both local node & local rack node
			testNodes[0] = dataNodes[1];
			testNodes[1] = dataNodes[2];
			testNodes[2] = dataNodes[0];
			cluster.SetRandomSeed(unchecked((int)(0xDEADBEEF)));
			cluster.SortByDistance(dataNodes[0], testNodes, testNodes.Length);
			NUnit.Framework.Assert.IsTrue(testNodes[0] == dataNodes[0]);
			NUnit.Framework.Assert.IsTrue(testNodes[1] == dataNodes[1]);
			NUnit.Framework.Assert.IsTrue(testNodes[2] == dataNodes[2]);
			// array contains both local node & local rack node & decommissioned node
			DatanodeDescriptor[] dtestNodes = new DatanodeDescriptor[5];
			dtestNodes[0] = dataNodes[8];
			dtestNodes[1] = dataNodes[12];
			dtestNodes[2] = dataNodes[11];
			dtestNodes[3] = dataNodes[9];
			dtestNodes[4] = dataNodes[10];
			cluster.SetRandomSeed(unchecked((int)(0xDEADBEEF)));
			cluster.SortByDistance(dataNodes[8], dtestNodes, dtestNodes.Length - 2);
			NUnit.Framework.Assert.IsTrue(dtestNodes[0] == dataNodes[8]);
			NUnit.Framework.Assert.IsTrue(dtestNodes[1] == dataNodes[11]);
			NUnit.Framework.Assert.IsTrue(dtestNodes[2] == dataNodes[12]);
			NUnit.Framework.Assert.IsTrue(dtestNodes[3] == dataNodes[9]);
			NUnit.Framework.Assert.IsTrue(dtestNodes[4] == dataNodes[10]);
			// array contains local node
			testNodes[0] = dataNodes[1];
			testNodes[1] = dataNodes[3];
			testNodes[2] = dataNodes[0];
			cluster.SetRandomSeed(unchecked((int)(0xDEADBEEF)));
			cluster.SortByDistance(dataNodes[0], testNodes, testNodes.Length);
			NUnit.Framework.Assert.IsTrue(testNodes[0] == dataNodes[0]);
			NUnit.Framework.Assert.IsTrue(testNodes[1] == dataNodes[1]);
			NUnit.Framework.Assert.IsTrue(testNodes[2] == dataNodes[3]);
			// array contains local rack node
			testNodes[0] = dataNodes[5];
			testNodes[1] = dataNodes[3];
			testNodes[2] = dataNodes[1];
			cluster.SetRandomSeed(unchecked((int)(0xDEADBEEF)));
			cluster.SortByDistance(dataNodes[0], testNodes, testNodes.Length);
			NUnit.Framework.Assert.IsTrue(testNodes[0] == dataNodes[1]);
			NUnit.Framework.Assert.IsTrue(testNodes[1] == dataNodes[3]);
			NUnit.Framework.Assert.IsTrue(testNodes[2] == dataNodes[5]);
			// array contains local rack node which happens to be in position 0
			testNodes[0] = dataNodes[1];
			testNodes[1] = dataNodes[5];
			testNodes[2] = dataNodes[3];
			cluster.SetRandomSeed(unchecked((int)(0xDEADBEEF)));
			cluster.SortByDistance(dataNodes[0], testNodes, testNodes.Length);
			NUnit.Framework.Assert.IsTrue(testNodes[0] == dataNodes[1]);
			NUnit.Framework.Assert.IsTrue(testNodes[1] == dataNodes[3]);
			NUnit.Framework.Assert.IsTrue(testNodes[2] == dataNodes[5]);
			// Same as previous, but with a different random seed to test randomization
			testNodes[0] = dataNodes[1];
			testNodes[1] = dataNodes[5];
			testNodes[2] = dataNodes[3];
			cluster.SetRandomSeed(unchecked((int)(0xDEAD)));
			cluster.SortByDistance(dataNodes[0], testNodes, testNodes.Length);
			// sortByDistance does not take the "data center" layer into consideration
			// and it doesn't sort by getDistance, so 1, 5, 3 is also valid here
			NUnit.Framework.Assert.IsTrue(testNodes[0] == dataNodes[1]);
			NUnit.Framework.Assert.IsTrue(testNodes[1] == dataNodes[5]);
			NUnit.Framework.Assert.IsTrue(testNodes[2] == dataNodes[3]);
			// Array of just rack-local nodes
			// Expect a random first node
			DatanodeDescriptor first = null;
			bool foundRandom = false;
			for (int i = 5; i <= 7; i++)
			{
				testNodes[0] = dataNodes[5];
				testNodes[1] = dataNodes[6];
				testNodes[2] = dataNodes[7];
				cluster.SortByDistance(dataNodes[i], testNodes, testNodes.Length);
				if (first == null)
				{
					first = testNodes[0];
				}
				else
				{
					if (first != testNodes[0])
					{
						foundRandom = true;
						break;
					}
				}
			}
			NUnit.Framework.Assert.IsTrue("Expected to find a different first location", foundRandom
				);
			// Array of just remote nodes
			// Expect random first node
			first = null;
			for (int i_1 = 1; i_1 <= 4; i_1++)
			{
				testNodes[0] = dataNodes[13];
				testNodes[1] = dataNodes[14];
				testNodes[2] = dataNodes[15];
				cluster.SortByDistance(dataNodes[i_1], testNodes, testNodes.Length);
				if (first == null)
				{
					first = testNodes[0];
				}
				else
				{
					if (first != testNodes[0])
					{
						foundRandom = true;
						break;
					}
				}
			}
			NUnit.Framework.Assert.IsTrue("Expected to find a different first location", foundRandom
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemove()
		{
			for (int i = 0; i < dataNodes.Length; i++)
			{
				cluster.Remove(dataNodes[i]);
			}
			for (int i_1 = 0; i_1 < dataNodes.Length; i_1++)
			{
				NUnit.Framework.Assert.IsFalse(cluster.Contains(dataNodes[i_1]));
			}
			NUnit.Framework.Assert.AreEqual(0, cluster.GetNumOfLeaves());
			for (int i_2 = 0; i_2 < dataNodes.Length; i_2++)
			{
				cluster.Add(dataNodes[i_2]);
			}
		}

		/// <summary>This picks a large number of nodes at random in order to ensure coverage
		/// 	</summary>
		/// <param name="numNodes">the number of nodes</param>
		/// <param name="excludedScope">the excluded scope</param>
		/// <returns>the frequency that nodes were chosen</returns>
		private IDictionary<Node, int> PickNodesAtRandom(int numNodes, string excludedScope
			)
		{
			IDictionary<Node, int> frequency = new Dictionary<Node, int>();
			foreach (DatanodeDescriptor dnd in dataNodes)
			{
				frequency[dnd] = 0;
			}
			for (int j = 0; j < numNodes; j++)
			{
				Node random = cluster.ChooseRandom(excludedScope);
				frequency[random] = frequency[random] + 1;
			}
			return frequency;
		}

		/// <summary>This test checks that chooseRandom works for an excluded node.</summary>
		[NUnit.Framework.Test]
		public virtual void TestChooseRandomExcludedNode()
		{
			string scope = "~" + NodeBase.GetPath(dataNodes[0]);
			IDictionary<Node, int> frequency = PickNodesAtRandom(100, scope);
			foreach (Node key in dataNodes)
			{
				// all nodes except the first should be more than zero
				NUnit.Framework.Assert.IsTrue(frequency[key] > 0 || key == dataNodes[0]);
			}
		}

		/// <summary>This test checks that chooseRandom works for an excluded rack.</summary>
		[NUnit.Framework.Test]
		public virtual void TestChooseRandomExcludedRack()
		{
			IDictionary<Node, int> frequency = PickNodesAtRandom(100, "~" + "/d2");
			// all the nodes on the second rack should be zero
			for (int j = 0; j < dataNodes.Length; j++)
			{
				int freq = frequency[dataNodes[j]];
				if (dataNodes[j].GetNetworkLocation().StartsWith("/d2"))
				{
					NUnit.Framework.Assert.AreEqual(0, freq);
				}
				else
				{
					NUnit.Framework.Assert.IsTrue(freq > 0);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidNetworkTopologiesNotCachedInHdfs()
		{
			// start a cluster
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				// bad rack topology
				string[] racks = new string[] { "/a/b", "/c" };
				string[] hosts = new string[] { "foo1.example.com", "foo2.example.com" };
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Racks(racks).Hosts(hosts
					).Build();
				cluster.WaitActive();
				NamenodeProtocols nn = cluster.GetNameNodeRpc();
				NUnit.Framework.Assert.IsNotNull(nn);
				// Wait for one DataNode to register.
				// The other DataNode will not be able to register up because of the rack mismatch.
				DatanodeInfo[] info;
				while (true)
				{
					info = nn.GetDatanodeReport(HdfsConstants.DatanodeReportType.Live);
					NUnit.Framework.Assert.IsFalse(info.Length == 2);
					if (info.Length == 1)
					{
						break;
					}
					Sharpen.Thread.Sleep(1000);
				}
				// Set the network topology of the other node to the match the network
				// topology of the node that came up.
				int validIdx = info[0].GetHostName().Equals(hosts[0]) ? 0 : 1;
				int invalidIdx = validIdx == 1 ? 0 : 1;
				StaticMapping.AddNodeToRack(hosts[invalidIdx], racks[validIdx]);
				Log.Info("datanode " + validIdx + " came up with network location " + info[0].GetNetworkLocation
					());
				// Restart the DN with the invalid topology and wait for it to register.
				cluster.RestartDataNode(invalidIdx);
				Sharpen.Thread.Sleep(5000);
				while (true)
				{
					info = nn.GetDatanodeReport(HdfsConstants.DatanodeReportType.Live);
					if (info.Length == 2)
					{
						break;
					}
					if (info.Length == 0)
					{
						Log.Info("got no valid DNs");
					}
					else
					{
						if (info.Length == 1)
						{
							Log.Info("got one valid DN: " + info[0].GetHostName() + " (at " + info[0].GetNetworkLocation
								() + ")");
						}
					}
					Sharpen.Thread.Sleep(1000);
				}
				NUnit.Framework.Assert.AreEqual(info[0].GetNetworkLocation(), info[1].GetNetworkLocation
					());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
