using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>A map from host names to datanode descriptors.</summary>
	internal class Host2NodesMap
	{
		private Dictionary<string, string> mapHost = new Dictionary<string, string>();

		private readonly Dictionary<string, DatanodeDescriptor[]> map = new Dictionary<string
			, DatanodeDescriptor[]>();

		private readonly ReadWriteLock hostmapLock = new ReentrantReadWriteLock();

		/// <summary>Check if node is already in the map.</summary>
		internal virtual bool Contains(DatanodeDescriptor node)
		{
			if (node == null)
			{
				return false;
			}
			string ipAddr = node.GetIpAddr();
			hostmapLock.ReadLock().Lock();
			try
			{
				DatanodeDescriptor[] nodes = map[ipAddr];
				if (nodes != null)
				{
					foreach (DatanodeDescriptor containedNode in nodes)
					{
						if (node == containedNode)
						{
							return true;
						}
					}
				}
			}
			finally
			{
				hostmapLock.ReadLock().Unlock();
			}
			return false;
		}

		/// <summary>
		/// add node to the map
		/// return true if the node is added; false otherwise.
		/// </summary>
		internal virtual bool Add(DatanodeDescriptor node)
		{
			hostmapLock.WriteLock().Lock();
			try
			{
				if (node == null || Contains(node))
				{
					return false;
				}
				string ipAddr = node.GetIpAddr();
				string hostname = node.GetHostName();
				mapHost[hostname] = ipAddr;
				DatanodeDescriptor[] nodes = map[ipAddr];
				DatanodeDescriptor[] newNodes;
				if (nodes == null)
				{
					newNodes = new DatanodeDescriptor[1];
					newNodes[0] = node;
				}
				else
				{
					// rare case: more than one datanode on the host
					newNodes = new DatanodeDescriptor[nodes.Length + 1];
					System.Array.Copy(nodes, 0, newNodes, 0, nodes.Length);
					newNodes[nodes.Length] = node;
				}
				map[ipAddr] = newNodes;
				return true;
			}
			finally
			{
				hostmapLock.WriteLock().Unlock();
			}
		}

		/// <summary>
		/// remove node from the map
		/// return true if the node is removed; false otherwise.
		/// </summary>
		internal virtual bool Remove(DatanodeDescriptor node)
		{
			if (node == null)
			{
				return false;
			}
			string ipAddr = node.GetIpAddr();
			string hostname = node.GetHostName();
			hostmapLock.WriteLock().Lock();
			try
			{
				DatanodeDescriptor[] nodes = map[ipAddr];
				if (nodes == null)
				{
					return false;
				}
				if (nodes.Length == 1)
				{
					if (nodes[0] == node)
					{
						Sharpen.Collections.Remove(map, ipAddr);
						//remove hostname key since last datanode is removed
						Sharpen.Collections.Remove(mapHost, hostname);
						return true;
					}
					else
					{
						return false;
					}
				}
				//rare case
				int i = 0;
				for (; i < nodes.Length; i++)
				{
					if (nodes[i] == node)
					{
						break;
					}
				}
				if (i == nodes.Length)
				{
					return false;
				}
				else
				{
					DatanodeDescriptor[] newNodes;
					newNodes = new DatanodeDescriptor[nodes.Length - 1];
					System.Array.Copy(nodes, 0, newNodes, 0, i);
					System.Array.Copy(nodes, i + 1, newNodes, i, nodes.Length - i - 1);
					map[ipAddr] = newNodes;
					return true;
				}
			}
			finally
			{
				hostmapLock.WriteLock().Unlock();
			}
		}

		/// <summary>Get a data node by its IP address.</summary>
		/// <returns>DatanodeDescriptor if found, null otherwise</returns>
		internal virtual DatanodeDescriptor GetDatanodeByHost(string ipAddr)
		{
			if (ipAddr == null)
			{
				return null;
			}
			hostmapLock.ReadLock().Lock();
			try
			{
				DatanodeDescriptor[] nodes = map[ipAddr];
				// no entry
				if (nodes == null)
				{
					return null;
				}
				// one node
				if (nodes.Length == 1)
				{
					return nodes[0];
				}
				// more than one node
				return nodes[DFSUtil.GetRandom().Next(nodes.Length)];
			}
			finally
			{
				hostmapLock.ReadLock().Unlock();
			}
		}

		/// <summary>Find data node by its transfer address</summary>
		/// <returns>DatanodeDescriptor if found or null otherwise</returns>
		public virtual DatanodeDescriptor GetDatanodeByXferAddr(string ipAddr, int xferPort
			)
		{
			if (ipAddr == null)
			{
				return null;
			}
			hostmapLock.ReadLock().Lock();
			try
			{
				DatanodeDescriptor[] nodes = map[ipAddr];
				// no entry
				if (nodes == null)
				{
					return null;
				}
				foreach (DatanodeDescriptor containedNode in nodes)
				{
					if (xferPort == containedNode.GetXferPort())
					{
						return containedNode;
					}
				}
				return null;
			}
			finally
			{
				hostmapLock.ReadLock().Unlock();
			}
		}

		/// <summary>get a data node by its hostname.</summary>
		/// <remarks>
		/// get a data node by its hostname. This should be used if only one
		/// datanode service is running on a hostname. If multiple datanodes
		/// are running on a hostname then use methods getDataNodeByXferAddr and
		/// getDataNodeByHostNameAndPort.
		/// </remarks>
		/// <returns>DatanodeDescriptor if found; otherwise null.</returns>
		internal virtual DatanodeDescriptor GetDataNodeByHostName(string hostname)
		{
			if (hostname == null)
			{
				return null;
			}
			hostmapLock.ReadLock().Lock();
			try
			{
				string ipAddr = mapHost[hostname];
				if (ipAddr == null)
				{
					return null;
				}
				else
				{
					return GetDatanodeByHost(ipAddr);
				}
			}
			finally
			{
				hostmapLock.ReadLock().Unlock();
			}
		}

		public override string ToString()
		{
			StringBuilder b = new StringBuilder(GetType().Name).Append("[");
			foreach (KeyValuePair<string, string> host in mapHost)
			{
				DatanodeDescriptor[] e = map[host.Value];
				b.Append("\n  " + host.Key + " => " + host.Value + " => " + Arrays.AsList(e));
			}
			return b.Append("\n]").ToString();
		}
	}
}
