using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Represents the network location of a block, information about the hosts
	/// that contain block replicas, and other block metadata (E.g.
	/// </summary>
	/// <remarks>
	/// Represents the network location of a block, information about the hosts
	/// that contain block replicas, and other block metadata (E.g. the file
	/// offset associated with the block, length, whether it is corrupt, etc).
	/// </remarks>
	public class BlockLocation
	{
		private string[] hosts;

		private string[] cachedHosts;

		private string[] names;

		private string[] topologyPaths;

		private long offset;

		private long length;

		private bool corrupt;

		private static readonly string[] EMPTY_STR_ARRAY = new string[0];

		/// <summary>Default Constructor</summary>
		public BlockLocation()
			: this(EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, 0L, 0L)
		{
		}

		/// <summary>Copy constructor</summary>
		public BlockLocation(org.apache.hadoop.fs.BlockLocation that)
		{
			// Datanode hostnames
			// Datanode hostnames with a cached replica
			// Datanode IP:xferPort for accessing the block
			// Full path name in network topology
			// Offset of the block in the file
			this.hosts = that.hosts;
			this.cachedHosts = that.cachedHosts;
			this.names = that.names;
			this.topologyPaths = that.topologyPaths;
			this.offset = that.offset;
			this.length = that.length;
			this.corrupt = that.corrupt;
		}

		/// <summary>Constructor with host, name, offset and length</summary>
		public BlockLocation(string[] names, string[] hosts, long offset, long length)
			: this(names, hosts, offset, length, false)
		{
		}

		/// <summary>Constructor with host, name, offset, length and corrupt flag</summary>
		public BlockLocation(string[] names, string[] hosts, long offset, long length, bool
			 corrupt)
			: this(names, hosts, null, offset, length, corrupt)
		{
		}

		/// <summary>Constructor with host, name, network topology, offset and length</summary>
		public BlockLocation(string[] names, string[] hosts, string[] topologyPaths, long
			 offset, long length)
			: this(names, hosts, topologyPaths, offset, length, false)
		{
		}

		/// <summary>
		/// Constructor with host, name, network topology, offset, length
		/// and corrupt flag
		/// </summary>
		public BlockLocation(string[] names, string[] hosts, string[] topologyPaths, long
			 offset, long length, bool corrupt)
			: this(names, hosts, null, topologyPaths, offset, length, corrupt)
		{
		}

		public BlockLocation(string[] names, string[] hosts, string[] cachedHosts, string
			[] topologyPaths, long offset, long length, bool corrupt)
		{
			if (names == null)
			{
				this.names = EMPTY_STR_ARRAY;
			}
			else
			{
				this.names = names;
			}
			if (hosts == null)
			{
				this.hosts = EMPTY_STR_ARRAY;
			}
			else
			{
				this.hosts = hosts;
			}
			if (cachedHosts == null)
			{
				this.cachedHosts = EMPTY_STR_ARRAY;
			}
			else
			{
				this.cachedHosts = cachedHosts;
			}
			if (topologyPaths == null)
			{
				this.topologyPaths = EMPTY_STR_ARRAY;
			}
			else
			{
				this.topologyPaths = topologyPaths;
			}
			this.offset = offset;
			this.length = length;
			this.corrupt = corrupt;
		}

		/// <summary>Get the list of hosts (hostname) hosting this block</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual string[] getHosts()
		{
			return hosts;
		}

		/// <summary>Get the list of hosts (hostname) hosting a cached replica of the block</summary>
		public virtual string[] getCachedHosts()
		{
			return cachedHosts;
		}

		/// <summary>Get the list of names (IP:xferPort) hosting this block</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual string[] getNames()
		{
			return names;
		}

		/// <summary>Get the list of network topology paths for each of the hosts.</summary>
		/// <remarks>
		/// Get the list of network topology paths for each of the hosts.
		/// The last component of the path is the "name" (IP:xferPort).
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual string[] getTopologyPaths()
		{
			return topologyPaths;
		}

		/// <summary>Get the start offset of file associated with this block</summary>
		public virtual long getOffset()
		{
			return offset;
		}

		/// <summary>Get the length of the block</summary>
		public virtual long getLength()
		{
			return length;
		}

		/// <summary>Get the corrupt flag.</summary>
		public virtual bool isCorrupt()
		{
			return corrupt;
		}

		/// <summary>Set the start offset of file associated with this block</summary>
		public virtual void setOffset(long offset)
		{
			this.offset = offset;
		}

		/// <summary>Set the length of block</summary>
		public virtual void setLength(long length)
		{
			this.length = length;
		}

		/// <summary>Set the corrupt flag.</summary>
		public virtual void setCorrupt(bool corrupt)
		{
			this.corrupt = corrupt;
		}

		/// <summary>Set the hosts hosting this block</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void setHosts(string[] hosts)
		{
			if (hosts == null)
			{
				this.hosts = EMPTY_STR_ARRAY;
			}
			else
			{
				this.hosts = hosts;
			}
		}

		/// <summary>Set the hosts hosting a cached replica of this block</summary>
		public virtual void setCachedHosts(string[] cachedHosts)
		{
			if (cachedHosts == null)
			{
				this.cachedHosts = EMPTY_STR_ARRAY;
			}
			else
			{
				this.cachedHosts = cachedHosts;
			}
		}

		/// <summary>Set the names (host:port) hosting this block</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void setNames(string[] names)
		{
			if (names == null)
			{
				this.names = EMPTY_STR_ARRAY;
			}
			else
			{
				this.names = names;
			}
		}

		/// <summary>Set the network topology paths of the hosts</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void setTopologyPaths(string[] topologyPaths)
		{
			if (topologyPaths == null)
			{
				this.topologyPaths = EMPTY_STR_ARRAY;
			}
			else
			{
				this.topologyPaths = topologyPaths;
			}
		}

		public override string ToString()
		{
			java.lang.StringBuilder result = new java.lang.StringBuilder();
			result.Append(offset);
			result.Append(',');
			result.Append(length);
			if (corrupt)
			{
				result.Append("(corrupt)");
			}
			foreach (string h in hosts)
			{
				result.Append(',');
				result.Append(h);
			}
			return result.ToString();
		}
	}
}
