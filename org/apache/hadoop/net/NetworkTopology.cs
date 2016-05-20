using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>
	/// The class represents a cluster of computer with a tree hierarchical
	/// network topology.
	/// </summary>
	/// <remarks>
	/// The class represents a cluster of computer with a tree hierarchical
	/// network topology.
	/// For example, a cluster may be consists of many data centers filled
	/// with racks of computers.
	/// In a network topology, leaves represent data nodes (computers) and inner
	/// nodes represent switches/routers that manage traffic in/out of data centers
	/// or racks.
	/// </remarks>
	public class NetworkTopology
	{
		public const string DEFAULT_RACK = "/default-rack";

		public const int DEFAULT_HOST_LEVEL = 2;

		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.net.NetworkTopology
			)));

		[System.Serializable]
		public class InvalidTopologyException : System.Exception
		{
			private const long serialVersionUID = 1L;

			public InvalidTopologyException(string msg)
				: base(msg)
			{
			}
		}

		/// <summary>
		/// Get an instance of NetworkTopology based on the value of the configuration
		/// parameter net.topology.impl.
		/// </summary>
		/// <param name="conf">the configuration to be used</param>
		/// <returns>an instance of NetworkTopology</returns>
		public static org.apache.hadoop.net.NetworkTopology getInstance(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return org.apache.hadoop.util.ReflectionUtils.newInstance(conf.getClass<org.apache.hadoop.net.NetworkTopology
				>(org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_IMPL_KEY, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.net.NetworkTopology))), conf);
		}

		/// <summary>InnerNode represents a switch/router of a data center or rack.</summary>
		/// <remarks>
		/// InnerNode represents a switch/router of a data center or rack.
		/// Different from a leaf node, it has non-null children.
		/// </remarks>
		internal class InnerNode : org.apache.hadoop.net.NodeBase
		{
			protected internal System.Collections.Generic.IList<org.apache.hadoop.net.Node> children
				 = new System.Collections.Generic.List<org.apache.hadoop.net.Node>();

			private int numOfLeaves;

			/// <summary>Construct an InnerNode from a path-like string</summary>
			internal InnerNode(string path)
				: base(path)
			{
			}

			/// <summary>Construct an InnerNode from its name and its network location</summary>
			internal InnerNode(string name, string location)
				: base(name, location)
			{
			}

			/// <summary>
			/// Construct an InnerNode
			/// from its name, its network location, its parent, and its level
			/// </summary>
			internal InnerNode(string name, string location, org.apache.hadoop.net.NetworkTopology.InnerNode
				 parent, int level)
				: base(name, location, parent, level)
			{
			}

			/// <returns>its children</returns>
			internal virtual System.Collections.Generic.IList<org.apache.hadoop.net.Node> getChildren
				()
			{
				return children;
			}

			/// <returns>the number of children this node has</returns>
			internal virtual int getNumOfChildren()
			{
				return children.Count;
			}

			/// <summary>Judge if this node represents a rack</summary>
			/// <returns>true if it has no child or its children are not InnerNodes</returns>
			internal virtual bool isRack()
			{
				if (children.isEmpty())
				{
					return true;
				}
				org.apache.hadoop.net.Node firstChild = children[0];
				if (firstChild is org.apache.hadoop.net.NetworkTopology.InnerNode)
				{
					return false;
				}
				return true;
			}

			/// <summary>Judge if this node is an ancestor of node <i>n</i></summary>
			/// <param name="n">a node</param>
			/// <returns>true if this node is an ancestor of <i>n</i></returns>
			internal virtual bool isAncestor(org.apache.hadoop.net.Node n)
			{
				return getPath(this).Equals(org.apache.hadoop.net.NodeBase.PATH_SEPARATOR_STR) ||
					 (n.getNetworkLocation() + org.apache.hadoop.net.NodeBase.PATH_SEPARATOR_STR).StartsWith
					(getPath(this) + org.apache.hadoop.net.NodeBase.PATH_SEPARATOR_STR);
			}

			/// <summary>Judge if this node is the parent of node <i>n</i></summary>
			/// <param name="n">a node</param>
			/// <returns>true if this node is the parent of <i>n</i></returns>
			internal virtual bool isParent(org.apache.hadoop.net.Node n)
			{
				return n.getNetworkLocation().Equals(getPath(this));
			}

			/* Return a child name of this node who is an ancestor of node <i>n</i> */
			private string getNextAncestorName(org.apache.hadoop.net.Node n)
			{
				if (!isAncestor(n))
				{
					throw new System.ArgumentException(this + "is not an ancestor of " + n);
				}
				string name = Sharpen.Runtime.substring(n.getNetworkLocation(), getPath(this).Length
					);
				if (name[0] == PATH_SEPARATOR)
				{
					name = Sharpen.Runtime.substring(name, 1);
				}
				int index = name.IndexOf(PATH_SEPARATOR);
				if (index != -1)
				{
					name = Sharpen.Runtime.substring(name, 0, index);
				}
				return name;
			}

			/// <summary>Add node <i>n</i> to the subtree of this node</summary>
			/// <param name="n">node to be added</param>
			/// <returns>true if the node is added; false otherwise</returns>
			internal virtual bool add(org.apache.hadoop.net.Node n)
			{
				if (!isAncestor(n))
				{
					throw new System.ArgumentException(n.getName() + ", which is located at " + n.getNetworkLocation
						() + ", is not a decendent of " + getPath(this));
				}
				if (isParent(n))
				{
					// this node is the parent of n; add n directly
					n.setParent(this);
					n.setLevel(this.level + 1);
					for (int i = 0; i < children.Count; i++)
					{
						if (children[i].getName().Equals(n.getName()))
						{
							children.set(i, n);
							return false;
						}
					}
					children.add(n);
					numOfLeaves++;
					return true;
				}
				else
				{
					// find the next ancestor node
					string parentName = getNextAncestorName(n);
					org.apache.hadoop.net.NetworkTopology.InnerNode parentNode = null;
					for (int i = 0; i < children.Count; i++)
					{
						if (children[i].getName().Equals(parentName))
						{
							parentNode = (org.apache.hadoop.net.NetworkTopology.InnerNode)children[i];
							break;
						}
					}
					if (parentNode == null)
					{
						// create a new InnerNode
						parentNode = createParentNode(parentName);
						children.add(parentNode);
					}
					// add n to the subtree of the next ancestor node
					if (parentNode.add(n))
					{
						numOfLeaves++;
						return true;
					}
					else
					{
						return false;
					}
				}
			}

			/// <summary>Creates a parent node to be added to the list of children.</summary>
			/// <remarks>
			/// Creates a parent node to be added to the list of children.
			/// Creates a node using the InnerNode four argument constructor specifying
			/// the name, location, parent, and level of this node.
			/// <p>To be overridden in subclasses for specific InnerNode implementations,
			/// as alternative to overriding the full
			/// <see cref="add(Node)"/>
			/// method.
			/// </remarks>
			/// <param name="parentName">The name of the parent node</param>
			/// <returns>A new inner node</returns>
			/// <seealso cref="InnerNode(string, string, InnerNode, int)"/>
			protected internal virtual org.apache.hadoop.net.NetworkTopology.InnerNode createParentNode
				(string parentName)
			{
				return new org.apache.hadoop.net.NetworkTopology.InnerNode(parentName, getPath(this
					), this, this.getLevel() + 1);
			}

			/// <summary>Remove node <i>n</i> from the subtree of this node</summary>
			/// <param name="n">node to be deleted</param>
			/// <returns>true if the node is deleted; false otherwise</returns>
			internal virtual bool remove(org.apache.hadoop.net.Node n)
			{
				string parent = n.getNetworkLocation();
				string currentPath = getPath(this);
				if (!isAncestor(n))
				{
					throw new System.ArgumentException(n.getName() + ", which is located at " + parent
						 + ", is not a descendent of " + currentPath);
				}
				if (isParent(n))
				{
					// this node is the parent of n; remove n directly
					for (int i = 0; i < children.Count; i++)
					{
						if (children[i].getName().Equals(n.getName()))
						{
							children.remove(i);
							numOfLeaves--;
							n.setParent(null);
							return true;
						}
					}
					return false;
				}
				else
				{
					// find the next ancestor node: the parent node
					string parentName = getNextAncestorName(n);
					org.apache.hadoop.net.NetworkTopology.InnerNode parentNode = null;
					int i;
					for (i = 0; i < children.Count; i++)
					{
						if (children[i].getName().Equals(parentName))
						{
							parentNode = (org.apache.hadoop.net.NetworkTopology.InnerNode)children[i];
							break;
						}
					}
					if (parentNode == null)
					{
						return false;
					}
					// remove n from the parent node
					bool isRemoved = parentNode.remove(n);
					// if the parent node has no children, remove the parent node too
					if (isRemoved)
					{
						if (parentNode.getNumOfChildren() == 0)
						{
							children.remove(i);
						}
						numOfLeaves--;
					}
					return isRemoved;
				}
			}

			// end of remove
			/// <summary>Given a node's string representation, return a reference to the node</summary>
			/// <param name="loc">string location of the form /rack/node</param>
			/// <returns>
			/// null if the node is not found or the childnode is there but
			/// not an instance of
			/// <see cref="InnerNode"/>
			/// </returns>
			private org.apache.hadoop.net.Node getLoc(string loc)
			{
				if (loc == null || loc.Length == 0)
				{
					return this;
				}
				string[] path = loc.split(PATH_SEPARATOR_STR, 2);
				org.apache.hadoop.net.Node childnode = null;
				for (int i = 0; i < children.Count; i++)
				{
					if (children[i].getName().Equals(path[0]))
					{
						childnode = children[i];
					}
				}
				if (childnode == null)
				{
					return null;
				}
				// non-existing node
				if (path.Length == 1)
				{
					return childnode;
				}
				if (childnode is org.apache.hadoop.net.NetworkTopology.InnerNode)
				{
					return ((org.apache.hadoop.net.NetworkTopology.InnerNode)childnode).getLoc(path[1
						]);
				}
				else
				{
					return null;
				}
			}

			/// <summary>
			/// get <i>leafIndex</i> leaf of this subtree
			/// if it is not in the <i>excludedNode</i>
			/// </summary>
			/// <param name="leafIndex">an indexed leaf of the node</param>
			/// <param name="excludedNode">an excluded node (can be null)</param>
			/// <returns/>
			internal virtual org.apache.hadoop.net.Node getLeaf(int leafIndex, org.apache.hadoop.net.Node
				 excludedNode)
			{
				int count = 0;
				// check if the excluded node a leaf
				bool isLeaf = excludedNode == null || !(excludedNode is org.apache.hadoop.net.NetworkTopology.InnerNode
					);
				// calculate the total number of excluded leaf nodes
				int numOfExcludedLeaves = isLeaf ? 1 : ((org.apache.hadoop.net.NetworkTopology.InnerNode
					)excludedNode).getNumOfLeaves();
				if (isLeafParent())
				{
					// children are leaves
					if (isLeaf)
					{
						// excluded node is a leaf node
						int excludedIndex = children.indexOf(excludedNode);
						if (excludedIndex != -1 && leafIndex >= 0)
						{
							// excluded node is one of the children so adjust the leaf index
							leafIndex = leafIndex >= excludedIndex ? leafIndex + 1 : leafIndex;
						}
					}
					// range check
					if (leafIndex < 0 || leafIndex >= this.getNumOfChildren())
					{
						return null;
					}
					return children[leafIndex];
				}
				else
				{
					for (int i = 0; i < children.Count; i++)
					{
						org.apache.hadoop.net.NetworkTopology.InnerNode child = (org.apache.hadoop.net.NetworkTopology.InnerNode
							)children[i];
						if (excludedNode == null || excludedNode != child)
						{
							// not the excludedNode
							int numOfLeaves = child.getNumOfLeaves();
							if (excludedNode != null && child.isAncestor(excludedNode))
							{
								numOfLeaves -= numOfExcludedLeaves;
							}
							if (count + numOfLeaves > leafIndex)
							{
								// the leaf is in the child subtree
								return child.getLeaf(leafIndex - count, excludedNode);
							}
							else
							{
								// go to the next child
								count = count + numOfLeaves;
							}
						}
						else
						{
							// it is the excluededNode
							// skip it and set the excludedNode to be null
							excludedNode = null;
						}
					}
					return null;
				}
			}

			protected internal virtual bool isLeafParent()
			{
				return isRack();
			}

			/// <summary>
			/// Determine if children a leaves, default implementation calls
			/// <see cref="isRack()"/>
			/// <p>To be overridden in subclasses for specific InnerNode implementations,
			/// as alternative to overriding the full
			/// <see cref="getLeaf(int, Node)"/>
			/// method.
			/// </summary>
			/// <returns>true if children are leaves, false otherwise</returns>
			protected internal virtual bool areChildrenLeaves()
			{
				return isRack();
			}

			/// <summary>Get number of leaves.</summary>
			internal virtual int getNumOfLeaves()
			{
				return numOfLeaves;
			}
		}

		/// <summary>the root cluster map</summary>
		internal org.apache.hadoop.net.NetworkTopology.InnerNode clusterMap;

		/// <summary>Depth of all leaf nodes</summary>
		private int depthOfAllLeaves = -1;

		/// <summary>rack counter</summary>
		protected internal int numOfRacks = 0;

		/// <summary>the lock used to manage access</summary>
		protected internal java.util.concurrent.locks.ReadWriteLock netlock = new java.util.concurrent.locks.ReentrantReadWriteLock
			();

		public NetworkTopology()
		{
			// end of InnerNode
			clusterMap = new org.apache.hadoop.net.NetworkTopology.InnerNode(org.apache.hadoop.net.NetworkTopology.InnerNode
				.ROOT);
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
		public virtual void add(org.apache.hadoop.net.Node node)
		{
			if (node == null)
			{
				return;
			}
			int newDepth = org.apache.hadoop.net.NodeBase.locationToDepth(node.getNetworkLocation
				()) + 1;
			netlock.writeLock().Lock();
			try
			{
				string oldTopoStr = this.ToString();
				if (node is org.apache.hadoop.net.NetworkTopology.InnerNode)
				{
					throw new System.ArgumentException("Not allow to add an inner node: " + org.apache.hadoop.net.NodeBase
						.getPath(node));
				}
				if ((depthOfAllLeaves != -1) && (depthOfAllLeaves != newDepth))
				{
					LOG.error("Error: can't add leaf node " + org.apache.hadoop.net.NodeBase.getPath(
						node) + " at depth " + newDepth + " to topology:\n" + oldTopoStr);
					throw new org.apache.hadoop.net.NetworkTopology.InvalidTopologyException("Failed to add "
						 + org.apache.hadoop.net.NodeBase.getPath(node) + ": You cannot have a rack and a non-rack node at the same "
						 + "level of the network topology.");
				}
				org.apache.hadoop.net.Node rack = getNodeForNetworkLocation(node);
				if (rack != null && !(rack is org.apache.hadoop.net.NetworkTopology.InnerNode))
				{
					throw new System.ArgumentException("Unexpected data node " + node.ToString() + " at an illegal network location"
						);
				}
				if (clusterMap.add(node))
				{
					LOG.info("Adding a new node: " + org.apache.hadoop.net.NodeBase.getPath(node));
					if (rack == null)
					{
						numOfRacks++;
					}
					if (!(node is org.apache.hadoop.net.NetworkTopology.InnerNode))
					{
						if (depthOfAllLeaves == -1)
						{
							depthOfAllLeaves = node.getLevel();
						}
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

		/// <summary>Return a reference to the node given its string representation.</summary>
		/// <remarks>
		/// Return a reference to the node given its string representation.
		/// Default implementation delegates to
		/// <see cref="getNode(string)"/>
		/// .
		/// <p>To be overridden in subclasses for specific NetworkTopology
		/// implementations, as alternative to overriding the full
		/// <see cref="add(Node)"/>
		/// method.
		/// </remarks>
		/// <param name="node">
		/// The string representation of this node's network location is
		/// used to retrieve a Node object.
		/// </param>
		/// <returns>a reference to the node; null if the node is not in the tree</returns>
		/// <seealso cref="add(Node)"/>
		/// <seealso cref="getNode(string)"/>
		protected internal virtual org.apache.hadoop.net.Node getNodeForNetworkLocation(org.apache.hadoop.net.Node
			 node)
		{
			return getNode(node.getNetworkLocation());
		}

		/// <summary>Given a string representation of a rack, return its children</summary>
		/// <param name="loc">a path-like string representation of a rack</param>
		/// <returns>a newly allocated list with all the node's children</returns>
		public virtual System.Collections.Generic.IList<org.apache.hadoop.net.Node> getDatanodesInRack
			(string loc)
		{
			netlock.readLock().Lock();
			try
			{
				loc = org.apache.hadoop.net.NodeBase.normalize(loc);
				if (!org.apache.hadoop.net.NodeBase.ROOT.Equals(loc))
				{
					loc = Sharpen.Runtime.substring(loc, 1);
				}
				org.apache.hadoop.net.NetworkTopology.InnerNode rack = (org.apache.hadoop.net.NetworkTopology.InnerNode
					)clusterMap.getLoc(loc);
				if (rack == null)
				{
					return null;
				}
				return new System.Collections.Generic.List<org.apache.hadoop.net.Node>(rack.getChildren
					());
			}
			finally
			{
				netlock.readLock().unlock();
			}
		}

		/// <summary>
		/// Remove a node
		/// Update node counter and rack counter if necessary
		/// </summary>
		/// <param name="node">node to be removed; can be null</param>
		public virtual void remove(org.apache.hadoop.net.Node node)
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
					org.apache.hadoop.net.NetworkTopology.InnerNode rack = (org.apache.hadoop.net.NetworkTopology.InnerNode
						)getNode(node.getNetworkLocation());
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

		/// <summary>Check if the tree contains node <i>node</i></summary>
		/// <param name="node">a node</param>
		/// <returns>true if <i>node</i> is already in the tree; false otherwise</returns>
		public virtual bool contains(org.apache.hadoop.net.Node node)
		{
			if (node == null)
			{
				return false;
			}
			netlock.readLock().Lock();
			try
			{
				org.apache.hadoop.net.Node parent = node.getParent();
				for (int level = node.getLevel(); parent != null && level > 0; parent = parent.getParent
					(), level--)
				{
					if (parent == clusterMap)
					{
						return true;
					}
				}
			}
			finally
			{
				netlock.readLock().unlock();
			}
			return false;
		}

		/// <summary>Given a string representation of a node, return its reference</summary>
		/// <param name="loc">a path-like string representation of a node</param>
		/// <returns>a reference to the node; null if the node is not in the tree</returns>
		public virtual org.apache.hadoop.net.Node getNode(string loc)
		{
			netlock.readLock().Lock();
			try
			{
				loc = org.apache.hadoop.net.NodeBase.normalize(loc);
				if (!org.apache.hadoop.net.NodeBase.ROOT.Equals(loc))
				{
					loc = Sharpen.Runtime.substring(loc, 1);
				}
				return clusterMap.getLoc(loc);
			}
			finally
			{
				netlock.readLock().unlock();
			}
		}

		/// <summary>
		/// Given a string representation of a rack for a specific network
		/// location
		/// To be overridden in subclasses for specific NetworkTopology
		/// implementations, as alternative to overriding the full
		/// <see cref="getRack(string)"/>
		/// method.
		/// </summary>
		/// <param name="loc">a path-like string representation of a network location</param>
		/// <returns>a rack string</returns>
		public virtual string getRack(string loc)
		{
			return loc;
		}

		/// <returns>the total number of racks</returns>
		public virtual int getNumOfRacks()
		{
			netlock.readLock().Lock();
			try
			{
				return numOfRacks;
			}
			finally
			{
				netlock.readLock().unlock();
			}
		}

		/// <returns>the total number of leaf nodes</returns>
		public virtual int getNumOfLeaves()
		{
			netlock.readLock().Lock();
			try
			{
				return clusterMap.getNumOfLeaves();
			}
			finally
			{
				netlock.readLock().unlock();
			}
		}

		/// <summary>
		/// Return the distance between two nodes
		/// It is assumed that the distance from one node to its parent is 1
		/// The distance between two nodes is calculated by summing up their distances
		/// to their closest common ancestor.
		/// </summary>
		/// <param name="node1">one node</param>
		/// <param name="node2">another node</param>
		/// <returns>
		/// the distance between node1 and node2 which is zero if they are the same
		/// or
		/// <see cref="int.MaxValue"/>
		/// if node1 or node2 do not belong to the cluster
		/// </returns>
		public virtual int getDistance(org.apache.hadoop.net.Node node1, org.apache.hadoop.net.Node
			 node2)
		{
			if (node1 == node2)
			{
				return 0;
			}
			org.apache.hadoop.net.Node n1 = node1;
			org.apache.hadoop.net.Node n2 = node2;
			int dis = 0;
			netlock.readLock().Lock();
			try
			{
				int level1 = node1.getLevel();
				int level2 = node2.getLevel();
				while (n1 != null && level1 > level2)
				{
					n1 = n1.getParent();
					level1--;
					dis++;
				}
				while (n2 != null && level2 > level1)
				{
					n2 = n2.getParent();
					level2--;
					dis++;
				}
				while (n1 != null && n2 != null && n1.getParent() != n2.getParent())
				{
					n1 = n1.getParent();
					n2 = n2.getParent();
					dis += 2;
				}
			}
			finally
			{
				netlock.readLock().unlock();
			}
			if (n1 == null)
			{
				LOG.warn("The cluster does not contain node: " + org.apache.hadoop.net.NodeBase.getPath
					(node1));
				return int.MaxValue;
			}
			if (n2 == null)
			{
				LOG.warn("The cluster does not contain node: " + org.apache.hadoop.net.NodeBase.getPath
					(node2));
				return int.MaxValue;
			}
			return dis + 2;
		}

		/// <summary>Check if two nodes are on the same rack</summary>
		/// <param name="node1">one node (can be null)</param>
		/// <param name="node2">another node (can be null)</param>
		/// <returns>true if node1 and node2 are on the same rack; false otherwise</returns>
		/// <exception>
		/// IllegalArgumentException
		/// when either node1 or node2 is null, or
		/// node1 or node2 do not belong to the cluster
		/// </exception>
		public virtual bool isOnSameRack(org.apache.hadoop.net.Node node1, org.apache.hadoop.net.Node
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
		public virtual bool isNodeGroupAware()
		{
			return false;
		}

		/// <summary>Return false directly as not aware of NodeGroup, to be override in sub-class
		/// 	</summary>
		public virtual bool isOnSameNodeGroup(org.apache.hadoop.net.Node node1, org.apache.hadoop.net.Node
			 node2)
		{
			return false;
		}

		/// <summary>
		/// Compare the parents of each node for equality
		/// <p>To be overridden in subclasses for specific NetworkTopology
		/// implementations, as alternative to overriding the full
		/// <see cref="isOnSameRack(Node, Node)"/>
		/// method.
		/// </summary>
		/// <param name="node1">the first node to compare</param>
		/// <param name="node2">the second node to compare</param>
		/// <returns>true if their parents are equal, false otherwise</returns>
		/// <seealso cref="isOnSameRack(Node, Node)"/>
		protected internal virtual bool isSameParents(org.apache.hadoop.net.Node node1, org.apache.hadoop.net.Node
			 node2)
		{
			return node1.getParent() == node2.getParent();
		}

		private static readonly java.util.Random r = new java.util.Random();

		[com.google.common.annotations.VisibleForTesting]
		internal virtual void setRandomSeed(long seed)
		{
			r.setSeed(seed);
		}

		/// <summary>
		/// randomly choose one node from <i>scope</i>
		/// if scope starts with ~, choose one from the all nodes except for the
		/// ones in <i>scope</i>; otherwise, choose one from <i>scope</i>
		/// </summary>
		/// <param name="scope">range of nodes from which a node will be chosen</param>
		/// <returns>the chosen node</returns>
		public virtual org.apache.hadoop.net.Node chooseRandom(string scope)
		{
			netlock.readLock().Lock();
			try
			{
				if (scope.StartsWith("~"))
				{
					return chooseRandom(org.apache.hadoop.net.NodeBase.ROOT, Sharpen.Runtime.substring
						(scope, 1));
				}
				else
				{
					return chooseRandom(scope, null);
				}
			}
			finally
			{
				netlock.readLock().unlock();
			}
		}

		private org.apache.hadoop.net.Node chooseRandom(string scope, string excludedScope
			)
		{
			if (excludedScope != null)
			{
				if (scope.StartsWith(excludedScope))
				{
					return null;
				}
				if (!excludedScope.StartsWith(scope))
				{
					excludedScope = null;
				}
			}
			org.apache.hadoop.net.Node node = getNode(scope);
			if (!(node is org.apache.hadoop.net.NetworkTopology.InnerNode))
			{
				return node;
			}
			org.apache.hadoop.net.NetworkTopology.InnerNode innerNode = (org.apache.hadoop.net.NetworkTopology.InnerNode
				)node;
			int numOfDatanodes = innerNode.getNumOfLeaves();
			if (excludedScope == null)
			{
				node = null;
			}
			else
			{
				node = getNode(excludedScope);
				if (!(node is org.apache.hadoop.net.NetworkTopology.InnerNode))
				{
					numOfDatanodes -= 1;
				}
				else
				{
					numOfDatanodes -= ((org.apache.hadoop.net.NetworkTopology.InnerNode)node).getNumOfLeaves
						();
				}
			}
			if (numOfDatanodes == 0)
			{
				throw new org.apache.hadoop.net.NetworkTopology.InvalidTopologyException("Failed to find datanode (scope=\""
					 + Sharpen.Runtime.getStringValueOf(scope) + "\" excludedScope=\"" + Sharpen.Runtime.getStringValueOf
					(excludedScope) + "\").");
			}
			int leaveIndex = r.nextInt(numOfDatanodes);
			return innerNode.getLeaf(leaveIndex, node);
		}

		/// <summary>return leaves in <i>scope</i></summary>
		/// <param name="scope">a path string</param>
		/// <returns>leaves nodes under specific scope</returns>
		public virtual System.Collections.Generic.IList<org.apache.hadoop.net.Node> getLeaves
			(string scope)
		{
			org.apache.hadoop.net.Node node = getNode(scope);
			System.Collections.Generic.IList<org.apache.hadoop.net.Node> leafNodes = new System.Collections.Generic.List
				<org.apache.hadoop.net.Node>();
			if (!(node is org.apache.hadoop.net.NetworkTopology.InnerNode))
			{
				leafNodes.add(node);
			}
			else
			{
				org.apache.hadoop.net.NetworkTopology.InnerNode innerNode = (org.apache.hadoop.net.NetworkTopology.InnerNode
					)node;
				for (int i = 0; i < innerNode.getNumOfLeaves(); i++)
				{
					leafNodes.add(innerNode.getLeaf(i, null));
				}
			}
			return leafNodes;
		}

		/// <summary>
		/// return the number of leaves in <i>scope</i> but not in <i>excludedNodes</i>
		/// if scope starts with ~, return the number of nodes that are not
		/// in <i>scope</i> and <i>excludedNodes</i>;
		/// </summary>
		/// <param name="scope">a path string that may start with ~</param>
		/// <param name="excludedNodes">a list of nodes</param>
		/// <returns>number of available nodes</returns>
		public virtual int countNumOfAvailableNodes(string scope, System.Collections.Generic.ICollection
			<org.apache.hadoop.net.Node> excludedNodes)
		{
			bool isExcluded = false;
			if (scope.StartsWith("~"))
			{
				isExcluded = true;
				scope = Sharpen.Runtime.substring(scope, 1);
			}
			scope = org.apache.hadoop.net.NodeBase.normalize(scope);
			int excludedCountInScope = 0;
			// the number of nodes in both scope & excludedNodes
			int excludedCountOffScope = 0;
			// the number of nodes outside scope & excludedNodes
			netlock.readLock().Lock();
			try
			{
				foreach (org.apache.hadoop.net.Node node in excludedNodes)
				{
					node = getNode(org.apache.hadoop.net.NodeBase.getPath(node));
					if (node == null)
					{
						continue;
					}
					if ((org.apache.hadoop.net.NodeBase.getPath(node) + org.apache.hadoop.net.NodeBase
						.PATH_SEPARATOR_STR).StartsWith(scope + org.apache.hadoop.net.NodeBase.PATH_SEPARATOR_STR
						))
					{
						excludedCountInScope++;
					}
					else
					{
						excludedCountOffScope++;
					}
				}
				org.apache.hadoop.net.Node n = getNode(scope);
				int scopeNodeCount = 0;
				if (n != null)
				{
					scopeNodeCount++;
				}
				if (n is org.apache.hadoop.net.NetworkTopology.InnerNode)
				{
					scopeNodeCount = ((org.apache.hadoop.net.NetworkTopology.InnerNode)n).getNumOfLeaves
						();
				}
				if (isExcluded)
				{
					return clusterMap.getNumOfLeaves() - scopeNodeCount - excludedCountOffScope;
				}
				else
				{
					return scopeNodeCount - excludedCountInScope;
				}
			}
			finally
			{
				netlock.readLock().unlock();
			}
		}

		/// <summary>convert a network tree to a string</summary>
		public override string ToString()
		{
			// print the number of racks
			java.lang.StringBuilder tree = new java.lang.StringBuilder();
			tree.Append("Number of racks: ");
			tree.Append(numOfRacks);
			tree.Append("\n");
			// print the number of leaves
			int numOfLeaves = getNumOfLeaves();
			tree.Append("Expected number of leaves:");
			tree.Append(numOfLeaves);
			tree.Append("\n");
			// print nodes
			for (int i = 0; i < numOfLeaves; i++)
			{
				tree.Append(org.apache.hadoop.net.NodeBase.getPath(clusterMap.getLeaf(i, null)));
				tree.Append("\n");
			}
			return tree.ToString();
		}

		/// <summary>
		/// Divide networklocation string into two parts by last separator, and get
		/// the first part here.
		/// </summary>
		/// <param name="networkLocation"/>
		/// <returns/>
		public static string getFirstHalf(string networkLocation)
		{
			int index = networkLocation.LastIndexOf(org.apache.hadoop.net.NodeBase.PATH_SEPARATOR_STR
				);
			return Sharpen.Runtime.substring(networkLocation, 0, index);
		}

		/// <summary>
		/// Divide networklocation string into two parts by last separator, and get
		/// the second part here.
		/// </summary>
		/// <param name="networkLocation"/>
		/// <returns/>
		public static string getLastHalf(string networkLocation)
		{
			int index = networkLocation.LastIndexOf(org.apache.hadoop.net.NodeBase.PATH_SEPARATOR_STR
				);
			return Sharpen.Runtime.substring(networkLocation, index);
		}

		/// <summary>
		/// Returns an integer weight which specifies how far away {node} is away from
		/// {reader}.
		/// </summary>
		/// <remarks>
		/// Returns an integer weight which specifies how far away {node} is away from
		/// {reader}. A lower value signifies that a node is closer.
		/// </remarks>
		/// <param name="reader">Node where data will be read</param>
		/// <param name="node">Replica of data</param>
		/// <returns>weight</returns>
		protected internal virtual int getWeight(org.apache.hadoop.net.Node reader, org.apache.hadoop.net.Node
			 node)
		{
			// 0 is local, 1 is same rack, 2 is off rack
			// Start off by initializing to off rack
			int weight = 2;
			if (reader != null)
			{
				if (reader.Equals(node))
				{
					weight = 0;
				}
				else
				{
					if (isOnSameRack(reader, node))
					{
						weight = 1;
					}
				}
			}
			return weight;
		}

		/// <summary>Sort nodes array by network distance to <i>reader</i>.</summary>
		/// <remarks>
		/// Sort nodes array by network distance to <i>reader</i>.
		/// <p/>
		/// In a three-level topology, a node can be either local, on the same rack,
		/// or on a different rack from the reader. Sorting the nodes based on network
		/// distance from the reader reduces network traffic and improves
		/// performance.
		/// <p/>
		/// As an additional twist, we also randomize the nodes at each network
		/// distance. This helps with load balancing when there is data skew.
		/// </remarks>
		/// <param name="reader">Node where data will be read</param>
		/// <param name="nodes">Available replicas with the requested data</param>
		/// <param name="activeLen">Number of active nodes at the front of the array</param>
		public virtual void sortByDistance(org.apache.hadoop.net.Node reader, org.apache.hadoop.net.Node
			[] nodes, int activeLen)
		{
			int[] weights = new int[activeLen];
			for (int i = 0; i < activeLen; i++)
			{
				weights[i] = getWeight(reader, nodes[i]);
			}
			// Add weight/node pairs to a TreeMap to sort
			System.Collections.Generic.SortedDictionary<int, System.Collections.Generic.IList
				<org.apache.hadoop.net.Node>> tree = new System.Collections.Generic.SortedDictionary
				<int, System.Collections.Generic.IList<org.apache.hadoop.net.Node>>();
			for (int i_1 = 0; i_1 < activeLen; i_1++)
			{
				int weight = weights[i_1];
				org.apache.hadoop.net.Node node = nodes[i_1];
				System.Collections.Generic.IList<org.apache.hadoop.net.Node> list = tree[weight];
				if (list == null)
				{
					list = com.google.common.collect.Lists.newArrayListWithExpectedSize(1);
					tree[weight] = list;
				}
				list.add(node);
			}
			int idx = 0;
			foreach (System.Collections.Generic.IList<org.apache.hadoop.net.Node> list_1 in tree
				.Values)
			{
				if (list_1 != null)
				{
					java.util.Collections.shuffle(list_1, r);
					foreach (org.apache.hadoop.net.Node n in list_1)
					{
						nodes[idx] = n;
						idx++;
					}
				}
			}
			com.google.common.@base.Preconditions.checkState(idx == activeLen, "Sorted the wrong number of nodes!"
				);
		}
	}
}
