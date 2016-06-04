using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Net
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
		public const string DefaultRack = "/default-rack";

		public const int DefaultHostLevel = 2;

		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Net.NetworkTopology
			));

		[System.Serializable]
		public class InvalidTopologyException : RuntimeException
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
		public static NetworkTopology GetInstance(Configuration conf)
		{
			return ReflectionUtils.NewInstance(conf.GetClass<NetworkTopology>(CommonConfigurationKeysPublic
				.NetTopologyImplKey, typeof(NetworkTopology)), conf);
		}

		/// <summary>InnerNode represents a switch/router of a data center or rack.</summary>
		/// <remarks>
		/// InnerNode represents a switch/router of a data center or rack.
		/// Different from a leaf node, it has non-null children.
		/// </remarks>
		internal class InnerNode : NodeBase
		{
			protected internal IList<Node> children = new AList<Node>();

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
			internal InnerNode(string name, string location, NetworkTopology.InnerNode parent
				, int level)
				: base(name, location, parent, level)
			{
			}

			/// <returns>its children</returns>
			internal virtual IList<Node> GetChildren()
			{
				return children;
			}

			/// <returns>the number of children this node has</returns>
			internal virtual int GetNumOfChildren()
			{
				return children.Count;
			}

			/// <summary>Judge if this node represents a rack</summary>
			/// <returns>true if it has no child or its children are not InnerNodes</returns>
			internal virtual bool IsRack()
			{
				if (children.IsEmpty())
				{
					return true;
				}
				Node firstChild = children[0];
				if (firstChild is NetworkTopology.InnerNode)
				{
					return false;
				}
				return true;
			}

			/// <summary>Judge if this node is an ancestor of node <i>n</i></summary>
			/// <param name="n">a node</param>
			/// <returns>true if this node is an ancestor of <i>n</i></returns>
			internal virtual bool IsAncestor(Node n)
			{
				return GetPath(this).Equals(NodeBase.PathSeparatorStr) || (n.GetNetworkLocation()
					 + NodeBase.PathSeparatorStr).StartsWith(GetPath(this) + NodeBase.PathSeparatorStr
					);
			}

			/// <summary>Judge if this node is the parent of node <i>n</i></summary>
			/// <param name="n">a node</param>
			/// <returns>true if this node is the parent of <i>n</i></returns>
			internal virtual bool IsParent(Node n)
			{
				return n.GetNetworkLocation().Equals(GetPath(this));
			}

			/* Return a child name of this node who is an ancestor of node <i>n</i> */
			private string GetNextAncestorName(Node n)
			{
				if (!IsAncestor(n))
				{
					throw new ArgumentException(this + "is not an ancestor of " + n);
				}
				string name = Sharpen.Runtime.Substring(n.GetNetworkLocation(), GetPath(this).Length
					);
				if (name[0] == PathSeparator)
				{
					name = Sharpen.Runtime.Substring(name, 1);
				}
				int index = name.IndexOf(PathSeparator);
				if (index != -1)
				{
					name = Sharpen.Runtime.Substring(name, 0, index);
				}
				return name;
			}

			/// <summary>Add node <i>n</i> to the subtree of this node</summary>
			/// <param name="n">node to be added</param>
			/// <returns>true if the node is added; false otherwise</returns>
			internal virtual bool Add(Node n)
			{
				if (!IsAncestor(n))
				{
					throw new ArgumentException(n.GetName() + ", which is located at " + n.GetNetworkLocation
						() + ", is not a decendent of " + GetPath(this));
				}
				if (IsParent(n))
				{
					// this node is the parent of n; add n directly
					n.SetParent(this);
					n.SetLevel(this.level + 1);
					for (int i = 0; i < children.Count; i++)
					{
						if (children[i].GetName().Equals(n.GetName()))
						{
							children.Set(i, n);
							return false;
						}
					}
					children.AddItem(n);
					numOfLeaves++;
					return true;
				}
				else
				{
					// find the next ancestor node
					string parentName = GetNextAncestorName(n);
					NetworkTopology.InnerNode parentNode = null;
					for (int i = 0; i < children.Count; i++)
					{
						if (children[i].GetName().Equals(parentName))
						{
							parentNode = (NetworkTopology.InnerNode)children[i];
							break;
						}
					}
					if (parentNode == null)
					{
						// create a new InnerNode
						parentNode = CreateParentNode(parentName);
						children.AddItem(parentNode);
					}
					// add n to the subtree of the next ancestor node
					if (parentNode.Add(n))
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
			/// <see cref="Add(Node)"/>
			/// method.
			/// </remarks>
			/// <param name="parentName">The name of the parent node</param>
			/// <returns>A new inner node</returns>
			/// <seealso cref="InnerNode(string, string, InnerNode, int)"/>
			protected internal virtual NetworkTopology.InnerNode CreateParentNode(string parentName
				)
			{
				return new NetworkTopology.InnerNode(parentName, GetPath(this), this, this.GetLevel
					() + 1);
			}

			/// <summary>Remove node <i>n</i> from the subtree of this node</summary>
			/// <param name="n">node to be deleted</param>
			/// <returns>true if the node is deleted; false otherwise</returns>
			internal virtual bool Remove(Node n)
			{
				string parent = n.GetNetworkLocation();
				string currentPath = GetPath(this);
				if (!IsAncestor(n))
				{
					throw new ArgumentException(n.GetName() + ", which is located at " + parent + ", is not a descendent of "
						 + currentPath);
				}
				if (IsParent(n))
				{
					// this node is the parent of n; remove n directly
					for (int i = 0; i < children.Count; i++)
					{
						if (children[i].GetName().Equals(n.GetName()))
						{
							children.Remove(i);
							numOfLeaves--;
							n.SetParent(null);
							return true;
						}
					}
					return false;
				}
				else
				{
					// find the next ancestor node: the parent node
					string parentName = GetNextAncestorName(n);
					NetworkTopology.InnerNode parentNode = null;
					int i;
					for (i = 0; i < children.Count; i++)
					{
						if (children[i].GetName().Equals(parentName))
						{
							parentNode = (NetworkTopology.InnerNode)children[i];
							break;
						}
					}
					if (parentNode == null)
					{
						return false;
					}
					// remove n from the parent node
					bool isRemoved = parentNode.Remove(n);
					// if the parent node has no children, remove the parent node too
					if (isRemoved)
					{
						if (parentNode.GetNumOfChildren() == 0)
						{
							children.Remove(i);
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
			private Node GetLoc(string loc)
			{
				if (loc == null || loc.Length == 0)
				{
					return this;
				}
				string[] path = loc.Split(PathSeparatorStr, 2);
				Node childnode = null;
				for (int i = 0; i < children.Count; i++)
				{
					if (children[i].GetName().Equals(path[0]))
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
				if (childnode is NetworkTopology.InnerNode)
				{
					return ((NetworkTopology.InnerNode)childnode).GetLoc(path[1]);
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
			internal virtual Node GetLeaf(int leafIndex, Node excludedNode)
			{
				int count = 0;
				// check if the excluded node a leaf
				bool isLeaf = excludedNode == null || !(excludedNode is NetworkTopology.InnerNode
					);
				// calculate the total number of excluded leaf nodes
				int numOfExcludedLeaves = isLeaf ? 1 : ((NetworkTopology.InnerNode)excludedNode).
					GetNumOfLeaves();
				if (IsLeafParent())
				{
					// children are leaves
					if (isLeaf)
					{
						// excluded node is a leaf node
						int excludedIndex = children.IndexOf(excludedNode);
						if (excludedIndex != -1 && leafIndex >= 0)
						{
							// excluded node is one of the children so adjust the leaf index
							leafIndex = leafIndex >= excludedIndex ? leafIndex + 1 : leafIndex;
						}
					}
					// range check
					if (leafIndex < 0 || leafIndex >= this.GetNumOfChildren())
					{
						return null;
					}
					return children[leafIndex];
				}
				else
				{
					for (int i = 0; i < children.Count; i++)
					{
						NetworkTopology.InnerNode child = (NetworkTopology.InnerNode)children[i];
						if (excludedNode == null || excludedNode != child)
						{
							// not the excludedNode
							int numOfLeaves = child.GetNumOfLeaves();
							if (excludedNode != null && child.IsAncestor(excludedNode))
							{
								numOfLeaves -= numOfExcludedLeaves;
							}
							if (count + numOfLeaves > leafIndex)
							{
								// the leaf is in the child subtree
								return child.GetLeaf(leafIndex - count, excludedNode);
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

			protected internal virtual bool IsLeafParent()
			{
				return IsRack();
			}

			/// <summary>
			/// Determine if children a leaves, default implementation calls
			/// <see cref="IsRack()"/>
			/// <p>To be overridden in subclasses for specific InnerNode implementations,
			/// as alternative to overriding the full
			/// <see cref="GetLeaf(int, Node)"/>
			/// method.
			/// </summary>
			/// <returns>true if children are leaves, false otherwise</returns>
			protected internal virtual bool AreChildrenLeaves()
			{
				return IsRack();
			}

			/// <summary>Get number of leaves.</summary>
			internal virtual int GetNumOfLeaves()
			{
				return numOfLeaves;
			}
		}

		/// <summary>the root cluster map</summary>
		internal NetworkTopology.InnerNode clusterMap;

		/// <summary>Depth of all leaf nodes</summary>
		private int depthOfAllLeaves = -1;

		/// <summary>rack counter</summary>
		protected internal int numOfRacks = 0;

		/// <summary>the lock used to manage access</summary>
		protected internal ReadWriteLock netlock = new ReentrantReadWriteLock();

		public NetworkTopology()
		{
			// end of InnerNode
			clusterMap = new NetworkTopology.InnerNode(NetworkTopology.InnerNode.Root);
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
		public virtual void Add(Node node)
		{
			if (node == null)
			{
				return;
			}
			int newDepth = NodeBase.LocationToDepth(node.GetNetworkLocation()) + 1;
			netlock.WriteLock().Lock();
			try
			{
				string oldTopoStr = this.ToString();
				if (node is NetworkTopology.InnerNode)
				{
					throw new ArgumentException("Not allow to add an inner node: " + NodeBase.GetPath
						(node));
				}
				if ((depthOfAllLeaves != -1) && (depthOfAllLeaves != newDepth))
				{
					Log.Error("Error: can't add leaf node " + NodeBase.GetPath(node) + " at depth " +
						 newDepth + " to topology:\n" + oldTopoStr);
					throw new NetworkTopology.InvalidTopologyException("Failed to add " + NodeBase.GetPath
						(node) + ": You cannot have a rack and a non-rack node at the same " + "level of the network topology."
						);
				}
				Node rack = GetNodeForNetworkLocation(node);
				if (rack != null && !(rack is NetworkTopology.InnerNode))
				{
					throw new ArgumentException("Unexpected data node " + node.ToString() + " at an illegal network location"
						);
				}
				if (clusterMap.Add(node))
				{
					Log.Info("Adding a new node: " + NodeBase.GetPath(node));
					if (rack == null)
					{
						numOfRacks++;
					}
					if (!(node is NetworkTopology.InnerNode))
					{
						if (depthOfAllLeaves == -1)
						{
							depthOfAllLeaves = node.GetLevel();
						}
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

		/// <summary>Return a reference to the node given its string representation.</summary>
		/// <remarks>
		/// Return a reference to the node given its string representation.
		/// Default implementation delegates to
		/// <see cref="GetNode(string)"/>
		/// .
		/// <p>To be overridden in subclasses for specific NetworkTopology
		/// implementations, as alternative to overriding the full
		/// <see cref="Add(Node)"/>
		/// method.
		/// </remarks>
		/// <param name="node">
		/// The string representation of this node's network location is
		/// used to retrieve a Node object.
		/// </param>
		/// <returns>a reference to the node; null if the node is not in the tree</returns>
		/// <seealso cref="Add(Node)"/>
		/// <seealso cref="GetNode(string)"/>
		protected internal virtual Node GetNodeForNetworkLocation(Node node)
		{
			return GetNode(node.GetNetworkLocation());
		}

		/// <summary>Given a string representation of a rack, return its children</summary>
		/// <param name="loc">a path-like string representation of a rack</param>
		/// <returns>a newly allocated list with all the node's children</returns>
		public virtual IList<Node> GetDatanodesInRack(string loc)
		{
			netlock.ReadLock().Lock();
			try
			{
				loc = NodeBase.Normalize(loc);
				if (!NodeBase.Root.Equals(loc))
				{
					loc = Sharpen.Runtime.Substring(loc, 1);
				}
				NetworkTopology.InnerNode rack = (NetworkTopology.InnerNode)clusterMap.GetLoc(loc
					);
				if (rack == null)
				{
					return null;
				}
				return new AList<Node>(rack.GetChildren());
			}
			finally
			{
				netlock.ReadLock().Unlock();
			}
		}

		/// <summary>
		/// Remove a node
		/// Update node counter and rack counter if necessary
		/// </summary>
		/// <param name="node">node to be removed; can be null</param>
		public virtual void Remove(Node node)
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
					NetworkTopology.InnerNode rack = (NetworkTopology.InnerNode)GetNode(node.GetNetworkLocation
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

		/// <summary>Check if the tree contains node <i>node</i></summary>
		/// <param name="node">a node</param>
		/// <returns>true if <i>node</i> is already in the tree; false otherwise</returns>
		public virtual bool Contains(Node node)
		{
			if (node == null)
			{
				return false;
			}
			netlock.ReadLock().Lock();
			try
			{
				Node parent = node.GetParent();
				for (int level = node.GetLevel(); parent != null && level > 0; parent = parent.GetParent
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
				netlock.ReadLock().Unlock();
			}
			return false;
		}

		/// <summary>Given a string representation of a node, return its reference</summary>
		/// <param name="loc">a path-like string representation of a node</param>
		/// <returns>a reference to the node; null if the node is not in the tree</returns>
		public virtual Node GetNode(string loc)
		{
			netlock.ReadLock().Lock();
			try
			{
				loc = NodeBase.Normalize(loc);
				if (!NodeBase.Root.Equals(loc))
				{
					loc = Sharpen.Runtime.Substring(loc, 1);
				}
				return clusterMap.GetLoc(loc);
			}
			finally
			{
				netlock.ReadLock().Unlock();
			}
		}

		/// <summary>
		/// Given a string representation of a rack for a specific network
		/// location
		/// To be overridden in subclasses for specific NetworkTopology
		/// implementations, as alternative to overriding the full
		/// <see cref="GetRack(string)"/>
		/// method.
		/// </summary>
		/// <param name="loc">a path-like string representation of a network location</param>
		/// <returns>a rack string</returns>
		public virtual string GetRack(string loc)
		{
			return loc;
		}

		/// <returns>the total number of racks</returns>
		public virtual int GetNumOfRacks()
		{
			netlock.ReadLock().Lock();
			try
			{
				return numOfRacks;
			}
			finally
			{
				netlock.ReadLock().Unlock();
			}
		}

		/// <returns>the total number of leaf nodes</returns>
		public virtual int GetNumOfLeaves()
		{
			netlock.ReadLock().Lock();
			try
			{
				return clusterMap.GetNumOfLeaves();
			}
			finally
			{
				netlock.ReadLock().Unlock();
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
		public virtual int GetDistance(Node node1, Node node2)
		{
			if (node1 == node2)
			{
				return 0;
			}
			Node n1 = node1;
			Node n2 = node2;
			int dis = 0;
			netlock.ReadLock().Lock();
			try
			{
				int level1 = node1.GetLevel();
				int level2 = node2.GetLevel();
				while (n1 != null && level1 > level2)
				{
					n1 = n1.GetParent();
					level1--;
					dis++;
				}
				while (n2 != null && level2 > level1)
				{
					n2 = n2.GetParent();
					level2--;
					dis++;
				}
				while (n1 != null && n2 != null && n1.GetParent() != n2.GetParent())
				{
					n1 = n1.GetParent();
					n2 = n2.GetParent();
					dis += 2;
				}
			}
			finally
			{
				netlock.ReadLock().Unlock();
			}
			if (n1 == null)
			{
				Log.Warn("The cluster does not contain node: " + NodeBase.GetPath(node1));
				return int.MaxValue;
			}
			if (n2 == null)
			{
				Log.Warn("The cluster does not contain node: " + NodeBase.GetPath(node2));
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
		public virtual bool IsOnSameRack(Node node1, Node node2)
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
		public virtual bool IsNodeGroupAware()
		{
			return false;
		}

		/// <summary>Return false directly as not aware of NodeGroup, to be override in sub-class
		/// 	</summary>
		public virtual bool IsOnSameNodeGroup(Node node1, Node node2)
		{
			return false;
		}

		/// <summary>
		/// Compare the parents of each node for equality
		/// <p>To be overridden in subclasses for specific NetworkTopology
		/// implementations, as alternative to overriding the full
		/// <see cref="IsOnSameRack(Node, Node)"/>
		/// method.
		/// </summary>
		/// <param name="node1">the first node to compare</param>
		/// <param name="node2">the second node to compare</param>
		/// <returns>true if their parents are equal, false otherwise</returns>
		/// <seealso cref="IsOnSameRack(Node, Node)"/>
		protected internal virtual bool IsSameParents(Node node1, Node node2)
		{
			return node1.GetParent() == node2.GetParent();
		}

		private static readonly Random r = new Random();

		[VisibleForTesting]
		internal virtual void SetRandomSeed(long seed)
		{
			r.SetSeed(seed);
		}

		/// <summary>
		/// randomly choose one node from <i>scope</i>
		/// if scope starts with ~, choose one from the all nodes except for the
		/// ones in <i>scope</i>; otherwise, choose one from <i>scope</i>
		/// </summary>
		/// <param name="scope">range of nodes from which a node will be chosen</param>
		/// <returns>the chosen node</returns>
		public virtual Node ChooseRandom(string scope)
		{
			netlock.ReadLock().Lock();
			try
			{
				if (scope.StartsWith("~"))
				{
					return ChooseRandom(NodeBase.Root, Sharpen.Runtime.Substring(scope, 1));
				}
				else
				{
					return ChooseRandom(scope, null);
				}
			}
			finally
			{
				netlock.ReadLock().Unlock();
			}
		}

		private Node ChooseRandom(string scope, string excludedScope)
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
			Node node = GetNode(scope);
			if (!(node is NetworkTopology.InnerNode))
			{
				return node;
			}
			NetworkTopology.InnerNode innerNode = (NetworkTopology.InnerNode)node;
			int numOfDatanodes = innerNode.GetNumOfLeaves();
			if (excludedScope == null)
			{
				node = null;
			}
			else
			{
				node = GetNode(excludedScope);
				if (!(node is NetworkTopology.InnerNode))
				{
					numOfDatanodes -= 1;
				}
				else
				{
					numOfDatanodes -= ((NetworkTopology.InnerNode)node).GetNumOfLeaves();
				}
			}
			if (numOfDatanodes == 0)
			{
				throw new NetworkTopology.InvalidTopologyException("Failed to find datanode (scope=\""
					 + scope.ToString() + "\" excludedScope=\"" + excludedScope.ToString() + "\").");
			}
			int leaveIndex = r.Next(numOfDatanodes);
			return innerNode.GetLeaf(leaveIndex, node);
		}

		/// <summary>return leaves in <i>scope</i></summary>
		/// <param name="scope">a path string</param>
		/// <returns>leaves nodes under specific scope</returns>
		public virtual IList<Node> GetLeaves(string scope)
		{
			Node node = GetNode(scope);
			IList<Node> leafNodes = new AList<Node>();
			if (!(node is NetworkTopology.InnerNode))
			{
				leafNodes.AddItem(node);
			}
			else
			{
				NetworkTopology.InnerNode innerNode = (NetworkTopology.InnerNode)node;
				for (int i = 0; i < innerNode.GetNumOfLeaves(); i++)
				{
					leafNodes.AddItem(innerNode.GetLeaf(i, null));
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
		public virtual int CountNumOfAvailableNodes(string scope, ICollection<Node> excludedNodes
			)
		{
			bool isExcluded = false;
			if (scope.StartsWith("~"))
			{
				isExcluded = true;
				scope = Sharpen.Runtime.Substring(scope, 1);
			}
			scope = NodeBase.Normalize(scope);
			int excludedCountInScope = 0;
			// the number of nodes in both scope & excludedNodes
			int excludedCountOffScope = 0;
			// the number of nodes outside scope & excludedNodes
			netlock.ReadLock().Lock();
			try
			{
				foreach (Node node in excludedNodes)
				{
					node = GetNode(NodeBase.GetPath(node));
					if (node == null)
					{
						continue;
					}
					if ((NodeBase.GetPath(node) + NodeBase.PathSeparatorStr).StartsWith(scope + NodeBase
						.PathSeparatorStr))
					{
						excludedCountInScope++;
					}
					else
					{
						excludedCountOffScope++;
					}
				}
				Node n = GetNode(scope);
				int scopeNodeCount = 0;
				if (n != null)
				{
					scopeNodeCount++;
				}
				if (n is NetworkTopology.InnerNode)
				{
					scopeNodeCount = ((NetworkTopology.InnerNode)n).GetNumOfLeaves();
				}
				if (isExcluded)
				{
					return clusterMap.GetNumOfLeaves() - scopeNodeCount - excludedCountOffScope;
				}
				else
				{
					return scopeNodeCount - excludedCountInScope;
				}
			}
			finally
			{
				netlock.ReadLock().Unlock();
			}
		}

		/// <summary>convert a network tree to a string</summary>
		public override string ToString()
		{
			// print the number of racks
			StringBuilder tree = new StringBuilder();
			tree.Append("Number of racks: ");
			tree.Append(numOfRacks);
			tree.Append("\n");
			// print the number of leaves
			int numOfLeaves = GetNumOfLeaves();
			tree.Append("Expected number of leaves:");
			tree.Append(numOfLeaves);
			tree.Append("\n");
			// print nodes
			for (int i = 0; i < numOfLeaves; i++)
			{
				tree.Append(NodeBase.GetPath(clusterMap.GetLeaf(i, null)));
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
		public static string GetFirstHalf(string networkLocation)
		{
			int index = networkLocation.LastIndexOf(NodeBase.PathSeparatorStr);
			return Sharpen.Runtime.Substring(networkLocation, 0, index);
		}

		/// <summary>
		/// Divide networklocation string into two parts by last separator, and get
		/// the second part here.
		/// </summary>
		/// <param name="networkLocation"/>
		/// <returns/>
		public static string GetLastHalf(string networkLocation)
		{
			int index = networkLocation.LastIndexOf(NodeBase.PathSeparatorStr);
			return Sharpen.Runtime.Substring(networkLocation, index);
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
		protected internal virtual int GetWeight(Node reader, Node node)
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
					if (IsOnSameRack(reader, node))
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
		public virtual void SortByDistance(Node reader, Node[] nodes, int activeLen)
		{
			int[] weights = new int[activeLen];
			for (int i = 0; i < activeLen; i++)
			{
				weights[i] = GetWeight(reader, nodes[i]);
			}
			// Add weight/node pairs to a TreeMap to sort
			SortedDictionary<int, IList<Node>> tree = new SortedDictionary<int, IList<Node>>(
				);
			for (int i_1 = 0; i_1 < activeLen; i_1++)
			{
				int weight = weights[i_1];
				Node node = nodes[i_1];
				IList<Node> list = tree[weight];
				if (list == null)
				{
					list = Lists.NewArrayListWithExpectedSize(1);
					tree[weight] = list;
				}
				list.AddItem(node);
			}
			int idx = 0;
			foreach (IList<Node> list_1 in tree.Values)
			{
				if (list_1 != null)
				{
					Sharpen.Collections.Shuffle(list_1, r);
					foreach (Node n in list_1)
					{
						nodes[idx] = n;
						idx++;
					}
				}
			}
			Preconditions.CheckState(idx == activeLen, "Sorted the wrong number of nodes!");
		}
	}
}
