using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	public class TestClusterTopology : Assert
	{
		public class NodeElement : Node
		{
			private string location;

			private string name;

			private Node parent;

			private int level;

			public NodeElement(string name)
			{
				this.name = name;
			}

			public virtual string GetNetworkLocation()
			{
				return location;
			}

			public virtual void SetNetworkLocation(string location)
			{
				this.location = location;
			}

			public virtual string GetName()
			{
				return name;
			}

			public virtual Node GetParent()
			{
				return parent;
			}

			public virtual void SetParent(Node parent)
			{
				this.parent = parent;
			}

			public virtual int GetLevel()
			{
				return level;
			}

			public virtual void SetLevel(int i)
			{
				this.level = i;
			}
		}

		/// <summary>Test the count of nodes with exclude list</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCountNumNodes()
		{
			// create the topology
			NetworkTopology cluster = new NetworkTopology();
			cluster.Add(GetNewNode("node1", "/d1/r1"));
			TestClusterTopology.NodeElement node2 = GetNewNode("node2", "/d1/r2");
			cluster.Add(node2);
			cluster.Add(GetNewNode("node3", "/d1/r3"));
			TestClusterTopology.NodeElement node3 = GetNewNode("node4", "/d1/r4");
			cluster.Add(node3);
			// create exclude list
			IList<Node> excludedNodes = new AList<Node>();
			NUnit.Framework.Assert.AreEqual("4 nodes should be available", 4, cluster.CountNumOfAvailableNodes
				(NodeBase.Root, excludedNodes));
			TestClusterTopology.NodeElement deadNode = GetNewNode("node5", "/d1/r2");
			excludedNodes.AddItem(deadNode);
			NUnit.Framework.Assert.AreEqual("4 nodes should be available with extra excluded Node"
				, 4, cluster.CountNumOfAvailableNodes(NodeBase.Root, excludedNodes));
			// add one existing node to exclude list
			excludedNodes.AddItem(node3);
			NUnit.Framework.Assert.AreEqual("excluded nodes with ROOT scope should be considered"
				, 3, cluster.CountNumOfAvailableNodes(NodeBase.Root, excludedNodes));
			NUnit.Framework.Assert.AreEqual("excluded nodes without ~ scope should be considered"
				, 2, cluster.CountNumOfAvailableNodes("~" + deadNode.GetNetworkLocation(), excludedNodes
				));
			NUnit.Framework.Assert.AreEqual("excluded nodes with rack scope should be considered"
				, 1, cluster.CountNumOfAvailableNodes(deadNode.GetNetworkLocation(), excludedNodes
				));
			// adding the node in excluded scope to excluded list
			excludedNodes.AddItem(node2);
			NUnit.Framework.Assert.AreEqual("excluded nodes with ~ scope should be considered"
				, 2, cluster.CountNumOfAvailableNodes("~" + deadNode.GetNetworkLocation(), excludedNodes
				));
			// getting count with non-exist scope.
			NUnit.Framework.Assert.AreEqual("No nodes should be considered for non-exist scope"
				, 0, cluster.CountNumOfAvailableNodes("/non-exist", excludedNodes));
		}

		private TestClusterTopology.NodeElement GetNewNode(string name, string rackLocation
			)
		{
			TestClusterTopology.NodeElement node = new TestClusterTopology.NodeElement(name);
			node.SetNetworkLocation(rackLocation);
			return node;
		}
	}
}
