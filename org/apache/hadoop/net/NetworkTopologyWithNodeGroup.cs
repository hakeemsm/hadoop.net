using Sharpen;

namespace org.apache.hadoop.net
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
	public class NetworkTopologyWithNodeGroup : org.apache.hadoop.net.NetworkTopology
	{
		public const string DEFAULT_NODEGROUP = "/default-nodegroup";

		public NetworkTopologyWithNodeGroup()
		{
			clusterMap = new org.apache.hadoop.net.NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup
				(org.apache.hadoop.net.NetworkTopology.InnerNode.ROOT);
		}

		protected internal override org.apache.hadoop.net.Node getNodeForNetworkLocation(
			org.apache.hadoop.net.Node node)
		{
			// if node only with default rack info, here we need to add default
			// nodegroup info
			if (org.apache.hadoop.net.NetworkTopology.DEFAULT_RACK.Equals(node.getNetworkLocation
				()))
			{
				node.setNetworkLocation(node.getNetworkLocation() + DEFAULT_NODEGROUP);
			}
			org.apache.hadoop.net.Node nodeGroup = getNode(node.getNetworkLocation());
			if (nodeGroup == null)
			{
				nodeGroup = new org.apache.hadoop.net.NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup
					(node.getNetworkLocation());
			}
			return getNode(nodeGroup.getNetworkLocation());
		}

		public override string getRack(string loc)
		{
			netlock.readLock().Lock();
			try
			{
				loc = org.apache.hadoop.net.NetworkTopology.InnerNode.normalize(loc);
				org.apache.hadoop.net.Node locNode = getNode(loc);
				if (locNode is org.apache.hadoop.net.NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup)
				{
					org.apache.hadoop.net.NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup node = 
						(org.apache.hadoop.net.NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup)locNode;
					if (node.isRack())
					{
						return loc;
					}
					else
					{
						if (node.isNodeGroup())
						{
							return node.getNetworkLocation();
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
				netlock.readLock().unlock();
			}
		}

		/// <summary>
		/// Given a string representation of a node group for a specific network
		/// location
		/// </summary>
		/// <param name="loc">a path-like string representation of a network location</param>
		/// <returns>a node group string</returns>
		public virtual string getNodeGroup(string loc)
		{
			netlock.readLock().Lock();
			try
			{
				loc = org.apache.hadoop.net.NetworkTopology.InnerNode.normalize(loc);
				org.apache.hadoop.net.Node locNode = getNode(loc);
				if (locNode is org.apache.hadoop.net.NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup)
				{
					org.apache.hadoop.net.NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup node = 
						(org.apache.hadoop.net.NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup)locNode;
					if (node.isNodeGroup())
					{
						return loc;
					}
					else
					{
						if (node.isRack())
						{
							// not sure the node group for a rack
							return null;
						}
						else
						{
							// may be a leaf node
							return getNodeGroup(node.getNetworkLocation());
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
				netlock.readLock().unlock();
			}
		}

		public override bool isOnSameRack(org.apache.hadoop.net.Node node1, org.apache.hadoop.net.Node
			 node2)
		{
			if (node1 == null || node2 == null || node1.getParent() == null || node2.getParent
				() == null)
			{
				return false;
			}
			netlock.readLock().Lock();
			try
			{
				return isSameParents(node1.getParent(), node2.getParent());
			}
			finally
			{
				netlock.readLock().unlock();
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
		public override bool isOnSameNodeGroup(org.apache.hadoop.net.Node node1, org.apache.hadoop.net.Node
			 node2)
		{
			if (node1 == null || node2 == null)
			{
				return false;
			}
			netlock.readLock().Lock();
			try
			{
				return isSameParents(node1, node2);
			}
			finally
			{
				netlock.readLock().unlock();
			}
		}

		/// <summary>Check if network topology is aware of NodeGroup</summary>
		public override bool isNodeGroupAware()
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
		public override void add(org.apache.hadoop.net.Node node)
		{
			if (node == null)
			{
				return;
			}
			if (node is org.apache.hadoop.net.NetworkTopology.InnerNode)
			{
				throw new System.ArgumentException("Not allow to add an inner node: " + org.apache.hadoop.net.NodeBase
					.getPath(node));
			}
			netlock.writeLock().Lock();
			try
			{
				org.apache.hadoop.net.Node rack = null;
				// if node only with default rack info, here we need to add default 
				// nodegroup info
				if (org.apache.hadoop.net.NetworkTopology.DEFAULT_RACK.Equals(node.getNetworkLocation
					()))
				{
					node.setNetworkLocation(node.getNetworkLocation() + org.apache.hadoop.net.NetworkTopologyWithNodeGroup
						.DEFAULT_NODEGROUP);
				}
				org.apache.hadoop.net.Node nodeGroup = getNode(node.getNetworkLocation());
				if (nodeGroup == null)
				{
					nodeGroup = new org.apache.hadoop.net.NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup
						(node.getNetworkLocation());
				}
				rack = getNode(nodeGroup.getNetworkLocation());
				// rack should be an innerNode and with parent.
				// note: rack's null parent case is: node's topology only has one layer, 
				//       so rack is recognized as "/" and no parent. 
				// This will be recognized as a node with fault topology.
				if (rack != null && (!(rack is org.apache.hadoop.net.NetworkTopology.InnerNode) ||
					 rack.getParent() == null))
				{
					throw new System.ArgumentException("Unexpected data node " + node.ToString() + " at an illegal network location"
						);
				}
				if (clusterMap.add(node))
				{
					LOG.info("Adding a new node: " + org.apache.hadoop.net.NodeBase.getPath(node));
					if (rack == null)
					{
						// We only track rack number here
						numOfRacks++;
					}
				}
				if (LOG.isDebugEnabled())
				{
					LOG.debug("NetworkTopology became:\n" + this.ToString());
				}
			}
			finally
			{
				netlock.writeLock().unlock();
			}
		}

		/// <summary>
		/// Remove a node
		/// Update node counter and rack counter if necessary
		/// </summary>
		/// <param name="node">node to be removed; can be null</param>
		public override void remove(org.apache.hadoop.net.Node node)
		{
			if (node == null)
			{
				return;
			}
			if (node is org.apache.hadoop.net.NetworkTopology.InnerNode)
			{
				throw new System.ArgumentException("Not allow to remove an inner node: " + org.apache.hadoop.net.NodeBase
					.getPath(node));
			}
			LOG.info("Removing a node: " + org.apache.hadoop.net.NodeBase.getPath(node));
			netlock.writeLock().Lock();
			try
			{
				if (clusterMap.remove(node))
				{
					org.apache.hadoop.net.Node nodeGroup = getNode(node.getNetworkLocation());
					if (nodeGroup == null)
					{
						nodeGroup = new org.apache.hadoop.net.NetworkTopology.InnerNode(node.getNetworkLocation
							());
					}
					org.apache.hadoop.net.NetworkTopology.InnerNode rack = (org.apache.hadoop.net.NetworkTopology.InnerNode
						)getNode(nodeGroup.getNetworkLocation());
					if (rack == null)
					{
						numOfRacks--;
					}
				}
				if (LOG.isDebugEnabled())
				{
					LOG.debug("NetworkTopology became:\n" + this.ToString());
				}
			}
			finally
			{
				netlock.writeLock().unlock();
			}
		}

		protected internal override int getWeight(org.apache.hadoop.net.Node reader, org.apache.hadoop.net.Node
			 node)
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
					if (isOnSameNodeGroup(reader, node))
					{
						weight = 1;
					}
					else
					{
						if (isOnSameRack(reader, node))
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
		/// <see cref="NetworkTopology.sortByDistance(Node, Node[], int)"/>
		/// except with a four-level network topology which contains the
		/// additional network distance of a "node group" which is between local and
		/// same rack.
		/// </remarks>
		/// <param name="reader">Node where data will be read</param>
		/// <param name="nodes">Available replicas with the requested data</param>
		/// <param name="activeLen">Number of active nodes at the front of the array</param>
		public override void sortByDistance(org.apache.hadoop.net.Node reader, org.apache.hadoop.net.Node
			[] nodes, int activeLen)
		{
			// If reader is not a datanode (not in NetworkTopology tree), we need to
			// replace this reader with a sibling leaf node in tree.
			if (reader != null && !this.contains(reader))
			{
				org.apache.hadoop.net.Node nodeGroup = getNode(reader.getNetworkLocation());
				if (nodeGroup != null && nodeGroup is org.apache.hadoop.net.NetworkTopology.InnerNode)
				{
					org.apache.hadoop.net.NetworkTopology.InnerNode parentNode = (org.apache.hadoop.net.NetworkTopology.InnerNode
						)nodeGroup;
					// replace reader with the first children of its parent in tree
					reader = parentNode.getLeaf(0, null);
				}
				else
				{
					return;
				}
			}
			base.sortByDistance(reader, nodes, activeLen);
		}

		/// <summary>
		/// InnerNodeWithNodeGroup represents a switch/router of a data center, rack
		/// or physical host.
		/// </summary>
		/// <remarks>
		/// InnerNodeWithNodeGroup represents a switch/router of a data center, rack
		/// or physical host. Different from a leaf node, it has non-null children.
		/// </remarks>
		internal class InnerNodeWithNodeGroup : org.apache.hadoop.net.NetworkTopology.InnerNode
		{
			public InnerNodeWithNodeGroup(string name, string location, org.apache.hadoop.net.NetworkTopology.InnerNode
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

			internal override bool isRack()
			{
				// it is node group
				if (getChildren().isEmpty())
				{
					return false;
				}
				org.apache.hadoop.net.Node firstChild = children[0];
				if (firstChild is org.apache.hadoop.net.NetworkTopology.InnerNode)
				{
					org.apache.hadoop.net.Node firstGrandChild = (((org.apache.hadoop.net.NetworkTopology.InnerNode
						)firstChild).children)[0];
					if (firstGrandChild is org.apache.hadoop.net.NetworkTopology.InnerNode)
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
			internal virtual bool isNodeGroup()
			{
				if (children.isEmpty())
				{
					return true;
				}
				org.apache.hadoop.net.Node firstChild = children[0];
				if (firstChild is org.apache.hadoop.net.NetworkTopology.InnerNode)
				{
					// it is rack or datacenter
					return false;
				}
				return true;
			}

			protected internal override bool isLeafParent()
			{
				return isNodeGroup();
			}

			protected internal override org.apache.hadoop.net.NetworkTopology.InnerNode createParentNode
				(string parentName)
			{
				return new org.apache.hadoop.net.NetworkTopologyWithNodeGroup.InnerNodeWithNodeGroup
					(parentName, getPath(this), this, this.getLevel() + 1);
			}

			protected internal override bool areChildrenLeaves()
			{
				return isNodeGroup();
			}
		}
	}
}
