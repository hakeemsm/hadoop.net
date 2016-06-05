using System;


namespace Org.Apache.Hadoop.Net
{
	/// <summary>
	/// The class extends NetworkTopology to represents a cluster of computer with
	/// a 4-layers hierarchical network topology.
	/// </summary>
	/// <remarks>
	/// The class extends NetworkTopology to represents a cluster of computer with
	/// a 4-layers hierarchical network topology.
	/// In this network topology, leaves represent data nodes (computers) and inner
	/// nodes represent switches/routers that manage traffic in/out of data centers,
	/// racks or physical host (with virtual switch).
	/// </remarks>
	/// <seealso cref="NetworkTopology"/>
	public class NetworkTopologyWithNodeGroup : NetworkTopology
	{
		public const string DefaultNodegroup = "/default-nodegroup";

		public NetworkTopologyWithNodeGroup()
		{
			clusterMap = new NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup(NetworkTopology.InnerNode
				.Root);
		}

		protected internal override Node GetNodeForNetworkLocation(Node node)
		{
			// if node only with default rack info, here we need to add default
			// nodegroup info
			if (NetworkTopology.DefaultRack.Equals(node.GetNetworkLocation()))
			{
				node.SetNetworkLocation(node.GetNetworkLocation() + DefaultNodegroup);
			}
			Node nodeGroup = GetNode(node.GetNetworkLocation());
			if (nodeGroup == null)
			{
				nodeGroup = new NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup(node.GetNetworkLocation
					());
			}
			return GetNode(nodeGroup.GetNetworkLocation());
		}

		public override string GetRack(string loc)
		{
			netlock.ReadLock().Lock();
			try
			{
				loc = NetworkTopology.InnerNode.Normalize(loc);
				Node locNode = GetNode(loc);
				if (locNode is NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup)
				{
					NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup node = (NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup
						)locNode;
					if (node.IsRack())
					{
						return loc;
					}
					else
					{
						if (node.IsNodeGroup())
						{
							return node.GetNetworkLocation();
						}
						else
						{
							// may be a data center
							return null;
						}
					}
				}
				else
				{
					// not in cluster map, don't handle it
					return loc;
				}
			}
			finally
			{
				netlock.ReadLock().Unlock();
			}
		}

		/// <summary>
		/// Given a string representation of a node group for a specific network
		/// location
		/// </summary>
		/// <param name="loc">a path-like string representation of a network location</param>
		/// <returns>a node group string</returns>
		public virtual string GetNodeGroup(string loc)
		{
			netlock.ReadLock().Lock();
			try
			{
				loc = NetworkTopology.InnerNode.Normalize(loc);
				Node locNode = GetNode(loc);
				if (locNode is NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup)
				{
					NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup node = (NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup
						)locNode;
					if (node.IsNodeGroup())
					{
						return loc;
					}
					else
					{
						if (node.IsRack())
						{
							// not sure the node group for a rack
							return null;
						}
						else
						{
							// may be a leaf node
							return GetNodeGroup(node.GetNetworkLocation());
						}
					}
				}
				else
				{
					// not in cluster map, don't handle it
					return loc;
				}
			}
			finally
			{
				netlock.ReadLock().Unlock();
			}
		}

		public override bool IsOnSameRack(Node node1, Node node2)
		{
			if (node1 == null || node2 == null || node1.GetParent() == null || node2.GetParent
				() == null)
			{
				return false;
			}
			netlock.ReadLock().Lock();
			try
			{
				return IsSameParents(node1.GetParent(), node2.GetParent());
			}
			finally
			{
				netlock.ReadLock().Unlock();
			}
		}

		/// <summary>
		/// Check if two nodes are on the same node group (hypervisor) The
		/// assumption here is: each nodes are leaf nodes.
		/// </summary>
		/// <param name="node1">one node (can be null)</param>
		/// <param name="node2">another node (can be null)</param>
		/// <returns>
		/// true if node1 and node2 are on the same node group; false
		/// otherwise
		/// </returns>
		/// <exception>
		/// IllegalArgumentException
		/// when either node1 or node2 is null, or node1 or node2 do
		/// not belong to the cluster
		/// </exception>
		public override bool IsOnSameNodeGroup(Node node1, Node node2)
		{
			if (node1 == null || node2 == null)
			{
				return false;
			}
			netlock.ReadLock().Lock();
			try
			{
				return IsSameParents(node1, node2);
			}
			finally
			{
				netlock.ReadLock().Unlock();
			}
		}

		/// <summary>Check if network topology is aware of NodeGroup</summary>
		public override bool IsNodeGroupAware()
		{
			return true;
		}

		/// <summary>
		/// Add a leaf node
		/// Update node counter & rack counter if necessary
		/// </summary>
		/// <param name="node">node to be added; can be null</param>
		/// <exception>
		/// IllegalArgumentException
		/// if add a node to a leave
		/// or node to be added is not a leaf
		/// </exception>
		public override void Add(Node node)
		{
			if (node == null)
			{
				return;
			}
			if (node is NetworkTopology.InnerNode)
			{
				throw new ArgumentException("Not allow to add an inner node: " + NodeBase.GetPath
					(node));
			}
			netlock.WriteLock().Lock();
			try
			{
				Node rack = null;
				// if node only with default rack info, here we need to add default 
				// nodegroup info
				if (NetworkTopology.DefaultRack.Equals(node.GetNetworkLocation()))
				{
					node.SetNetworkLocation(node.GetNetworkLocation() + Org.Apache.Hadoop.Net.NetworkTopologyWithNodeGroup
						.DefaultNodegroup);
				}
				Node nodeGroup = GetNode(node.GetNetworkLocation());
				if (nodeGroup == null)
				{
					nodeGroup = new NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup(node.GetNetworkLocation
						());
				}
				rack = GetNode(nodeGroup.GetNetworkLocation());
				// rack should be an innerNode and with parent.
				// note: rack's null parent case is: node's topology only has one layer, 
				//       so rack is recognized as "/" and no parent. 
				// This will be recognized as a node with fault topology.
				if (rack != null && (!(rack is NetworkTopology.InnerNode) || rack.GetParent() == 
					null))
				{
					throw new ArgumentException("Unexpected data node " + node.ToString() + " at an illegal network location"
						);
				}
				if (clusterMap.Add(node))
				{
					Log.Info("Adding a new node: " + NodeBase.GetPath(node));
					if (rack == null)
					{
						// We only track rack number here
						numOfRacks++;
					}
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("NetworkTopology became:\n" + this.ToString());
				}
			}
			finally
			{
				netlock.WriteLock().Unlock();
			}
		}

		/// <summary>
		/// Remove a node
		/// Update node counter and rack counter if necessary
		/// </summary>
		/// <param name="node">node to be removed; can be null</param>
		public override void Remove(Node node)
		{
			if (node == null)
			{
				return;
			}
			if (node is NetworkTopology.InnerNode)
			{
				throw new ArgumentException("Not allow to remove an inner node: " + NodeBase.GetPath
					(node));
			}
			Log.Info("Removing a node: " + NodeBase.GetPath(node));
			netlock.WriteLock().Lock();
			try
			{
				if (clusterMap.Remove(node))
				{
					Node nodeGroup = GetNode(node.GetNetworkLocation());
					if (nodeGroup == null)
					{
						nodeGroup = new NetworkTopology.InnerNode(node.GetNetworkLocation());
					}
					NetworkTopology.InnerNode rack = (NetworkTopology.InnerNode)GetNode(nodeGroup.GetNetworkLocation
						());
					if (rack == null)
					{
						numOfRacks--;
					}
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("NetworkTopology became:\n" + this.ToString());
				}
			}
			finally
			{
				netlock.WriteLock().Unlock();
			}
		}

		protected internal override int GetWeight(Node reader, Node node)
		{
			// 0 is local, 1 is same node group, 2 is same rack, 3 is off rack
			// Start off by initializing to off rack
			int weight = 3;
			if (reader != null)
			{
				if (reader.Equals(node))
				{
					weight = 0;
				}
				else
				{
					if (IsOnSameNodeGroup(reader, node))
					{
						weight = 1;
					}
					else
					{
						if (IsOnSameRack(reader, node))
						{
							weight = 2;
						}
					}
				}
			}
			return weight;
		}

		/// <summary>Sort nodes array by their distances to <i>reader</i>.</summary>
		/// <remarks>
		/// Sort nodes array by their distances to <i>reader</i>.
		/// <p/>
		/// This is the same as
		/// <see cref="NetworkTopology.SortByDistance(Node, Node[], int)"/>
		/// except with a four-level network topology which contains the
		/// additional network distance of a "node group" which is between local and
		/// same rack.
		/// </remarks>
		/// <param name="reader">Node where data will be read</param>
		/// <param name="nodes">Available replicas with the requested data</param>
		/// <param name="activeLen">Number of active nodes at the front of the array</param>
		public override void SortByDistance(Node reader, Node[] nodes, int activeLen)
		{
			// If reader is not a datanode (not in NetworkTopology tree), we need to
			// replace this reader with a sibling leaf node in tree.
			if (reader != null && !this.Contains(reader))
			{
				Node nodeGroup = GetNode(reader.GetNetworkLocation());
				if (nodeGroup != null && nodeGroup is NetworkTopology.InnerNode)
				{
					NetworkTopology.InnerNode parentNode = (NetworkTopology.InnerNode)nodeGroup;
					// replace reader with the first children of its parent in tree
					reader = parentNode.GetLeaf(0, null);
				}
				else
				{
					return;
				}
			}
			base.SortByDistance(reader, nodes, activeLen);
		}

		/// <summary>
		/// InnerNodeWithNodeGroup represents a switch/router of a data center, rack
		/// or physical host.
		/// </summary>
		/// <remarks>
		/// InnerNodeWithNodeGroup represents a switch/router of a data center, rack
		/// or physical host. Different from a leaf node, it has non-null children.
		/// </remarks>
		internal class InnerNodeWithNodeGroup : NetworkTopology.InnerNode
		{
			public InnerNodeWithNodeGroup(string name, string location, NetworkTopology.InnerNode
				 parent, int level)
				: base(name, location, parent, level)
			{
			}

			public InnerNodeWithNodeGroup(string name, string location)
				: base(name, location)
			{
			}

			public InnerNodeWithNodeGroup(string path)
				: base(path)
			{
			}

			internal override bool IsRack()
			{
				// it is node group
				if (GetChildren().IsEmpty())
				{
					return false;
				}
				Node firstChild = children[0];
				if (firstChild is NetworkTopology.InnerNode)
				{
					Node firstGrandChild = (((NetworkTopology.InnerNode)firstChild).children)[0];
					if (firstGrandChild is NetworkTopology.InnerNode)
					{
						// it is datacenter
						return false;
					}
					else
					{
						return true;
					}
				}
				return false;
			}

			/// <summary>Judge if this node represents a node group</summary>
			/// <returns>true if it has no child or its children are not InnerNodes</returns>
			internal virtual bool IsNodeGroup()
			{
				if (children.IsEmpty())
				{
					return true;
				}
				Node firstChild = children[0];
				if (firstChild is NetworkTopology.InnerNode)
				{
					// it is rack or datacenter
					return false;
				}
				return true;
			}

			protected internal override bool IsLeafParent()
			{
				return IsNodeGroup();
			}

			protected internal override NetworkTopology.InnerNode CreateParentNode(string parentName
				)
			{
				return new NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup(parentName, GetPath
					(this), this, this.GetLevel() + 1);
			}

			protected internal override bool AreChildrenLeaves()
			{
				return IsNodeGroup();
			}
		}
	}
}
