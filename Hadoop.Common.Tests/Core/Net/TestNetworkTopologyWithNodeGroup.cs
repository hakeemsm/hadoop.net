using System;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	public class TestNetworkTopologyWithNodeGroup
	{
		private static readonly NetworkTopologyWithNodeGroup cluster = new NetworkTopologyWithNodeGroup
			();

		private static readonly NodeBase[] dataNodes = new NodeBase[] { new NodeBase("h1"
			, "/d1/r1/s1"), new NodeBase("h2", "/d1/r1/s1"), new NodeBase("h3", "/d1/r1/s2")
			, new NodeBase("h4", "/d1/r2/s3"), new NodeBase("h5", "/d1/r2/s3"), new NodeBase
			("h6", "/d1/r2/s4"), new NodeBase("h7", "/d2/r3/s5"), new NodeBase("h8", "/d2/r3/s6"
			) };

		private static readonly NodeBase computeNode = new NodeBase("/d1/r1/s1/h9");

		private static readonly NodeBase rackOnlyNode = new NodeBase("h10", "/r2");

		static TestNetworkTopologyWithNodeGroup()
		{
			for (int i = 0; i < dataNodes.Length; i++)
			{
				cluster.Add(dataNodes[i]);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNumOfChildren()
		{
			Assert.Equal(dataNodes.Length, cluster.GetNumOfLeaves());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNumOfRacks()
		{
			Assert.Equal(3, cluster.GetNumOfRacks());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRacks()
		{
			Assert.Equal(3, cluster.GetNumOfRacks());
			Assert.True(cluster.IsOnSameRack(dataNodes[0], dataNodes[1]));
			Assert.True(cluster.IsOnSameRack(dataNodes[1], dataNodes[2]));
			NUnit.Framework.Assert.IsFalse(cluster.IsOnSameRack(dataNodes[2], dataNodes[3]));
			Assert.True(cluster.IsOnSameRack(dataNodes[3], dataNodes[4]));
			Assert.True(cluster.IsOnSameRack(dataNodes[4], dataNodes[5]));
			NUnit.Framework.Assert.IsFalse(cluster.IsOnSameRack(dataNodes[5], dataNodes[6]));
			Assert.True(cluster.IsOnSameRack(dataNodes[6], dataNodes[7]));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNodeGroups()
		{
			Assert.Equal(3, cluster.GetNumOfRacks());
			Assert.True(cluster.IsOnSameNodeGroup(dataNodes[0], dataNodes[1
				]));
			NUnit.Framework.Assert.IsFalse(cluster.IsOnSameNodeGroup(dataNodes[1], dataNodes[
				2]));
			NUnit.Framework.Assert.IsFalse(cluster.IsOnSameNodeGroup(dataNodes[2], dataNodes[
				3]));
			Assert.True(cluster.IsOnSameNodeGroup(dataNodes[3], dataNodes[4
				]));
			NUnit.Framework.Assert.IsFalse(cluster.IsOnSameNodeGroup(dataNodes[4], dataNodes[
				5]));
			NUnit.Framework.Assert.IsFalse(cluster.IsOnSameNodeGroup(dataNodes[5], dataNodes[
				6]));
			NUnit.Framework.Assert.IsFalse(cluster.IsOnSameNodeGroup(dataNodes[6], dataNodes[
				7]));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetDistance()
		{
			Assert.Equal(0, cluster.GetDistance(dataNodes[0], dataNodes[0]
				));
			Assert.Equal(2, cluster.GetDistance(dataNodes[0], dataNodes[1]
				));
			Assert.Equal(4, cluster.GetDistance(dataNodes[0], dataNodes[2]
				));
			Assert.Equal(6, cluster.GetDistance(dataNodes[0], dataNodes[3]
				));
			Assert.Equal(8, cluster.GetDistance(dataNodes[0], dataNodes[6]
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSortByDistance()
		{
			NodeBase[] testNodes = new NodeBase[4];
			// array contains both local node, local node group & local rack node
			testNodes[0] = dataNodes[1];
			testNodes[1] = dataNodes[2];
			testNodes[2] = dataNodes[3];
			testNodes[3] = dataNodes[0];
			cluster.SortByDistance(dataNodes[0], testNodes, testNodes.Length);
			Assert.True(testNodes[0] == dataNodes[0]);
			Assert.True(testNodes[1] == dataNodes[1]);
			Assert.True(testNodes[2] == dataNodes[2]);
			Assert.True(testNodes[3] == dataNodes[3]);
			// array contains local node & local node group
			testNodes[0] = dataNodes[3];
			testNodes[1] = dataNodes[4];
			testNodes[2] = dataNodes[1];
			testNodes[3] = dataNodes[0];
			cluster.SortByDistance(dataNodes[0], testNodes, testNodes.Length);
			Assert.True(testNodes[0] == dataNodes[0]);
			Assert.True(testNodes[1] == dataNodes[1]);
			// array contains local node & rack node
			testNodes[0] = dataNodes[5];
			testNodes[1] = dataNodes[3];
			testNodes[2] = dataNodes[2];
			testNodes[3] = dataNodes[0];
			cluster.SortByDistance(dataNodes[0], testNodes, testNodes.Length);
			Assert.True(testNodes[0] == dataNodes[0]);
			Assert.True(testNodes[1] == dataNodes[2]);
			// array contains local-nodegroup node (not a data node also) & rack node
			testNodes[0] = dataNodes[6];
			testNodes[1] = dataNodes[7];
			testNodes[2] = dataNodes[2];
			testNodes[3] = dataNodes[0];
			cluster.SortByDistance(computeNode, testNodes, testNodes.Length);
			Assert.True(testNodes[0] == dataNodes[0]);
			Assert.True(testNodes[1] == dataNodes[2]);
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
			foreach (NodeBase dnd in dataNodes)
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

		/// <summary>Test replica placement policy in case last node is invalid.</summary>
		/// <remarks>
		/// Test replica placement policy in case last node is invalid.
		/// We create 6 nodes but the last node is in fault topology (with rack info),
		/// so cannot be added to cluster. We should test proper exception is thrown in
		/// adding node but shouldn't affect the cluster.
		/// </remarks>
		[Fact]
		public virtual void TestChooseRandomExcludedNode()
		{
			string scope = "~" + NodeBase.GetPath(dataNodes[0]);
			IDictionary<Node, int> frequency = PickNodesAtRandom(100, scope);
			foreach (Node key in dataNodes)
			{
				// all nodes except the first should be more than zero
				Assert.True(frequency[key] > 0 || key == dataNodes[0]);
			}
		}

		/// <summary>
		/// This test checks that adding a node with invalid topology will be failed
		/// with an exception to show topology is invalid.
		/// </summary>
		[Fact]
		public virtual void TestAddNodeWithInvalidTopology()
		{
			// The last node is a node with invalid topology
			try
			{
				cluster.Add(rackOnlyNode);
				NUnit.Framework.Assert.Fail("Exception should be thrown, so we should not have reached here."
					);
			}
			catch (Exception e)
			{
				if (!(e is ArgumentException))
				{
					NUnit.Framework.Assert.Fail("Expecting IllegalArgumentException, but caught:" + e
						);
				}
				Assert.True(e.Message.Contains("illegal network location"));
			}
		}
	}
}
