using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// This class extends the primary identifier of a Datanode with ephemeral
	/// state, eg usage information, current administrative state, and the
	/// network location that is communicated to clients.
	/// </summary>
	public class DatanodeInfo : DatanodeID, Node
	{
		private long capacity;

		private long dfsUsed;

		private long remaining;

		private long blockPoolUsed;

		private long cacheCapacity;

		private long cacheUsed;

		private long lastUpdate;

		private long lastUpdateMonotonic;

		private int xceiverCount;

		private string location = NetworkTopology.DefaultRack;

		private string softwareVersion;

		private IList<string> dependentHostNames = new List<string>();

		[System.Serializable]
		public sealed class AdminStates
		{
			public static readonly DatanodeInfo.AdminStates Normal = new DatanodeInfo.AdminStates
				("In Service");

			public static readonly DatanodeInfo.AdminStates DecommissionInprogress = new DatanodeInfo.AdminStates
				("Decommission In Progress");

			public static readonly DatanodeInfo.AdminStates Decommissioned = new DatanodeInfo.AdminStates
				("Decommissioned");

			internal readonly string value;

			internal AdminStates(string v)
			{
				// Datanode administrative states
				this.value = v;
			}

			public override string ToString()
			{
				return DatanodeInfo.AdminStates.value;
			}

			public static DatanodeInfo.AdminStates FromValue(string value)
			{
				foreach (DatanodeInfo.AdminStates @as in DatanodeInfo.AdminStates.Values())
				{
					if (@as.value.Equals(value))
					{
						return @as;
					}
				}
				return DatanodeInfo.AdminStates.Normal;
			}
		}

		protected internal DatanodeInfo.AdminStates adminState;

		public DatanodeInfo(DatanodeInfo from)
			: base(from)
		{
			this.capacity = from.GetCapacity();
			this.dfsUsed = from.GetDfsUsed();
			this.remaining = from.GetRemaining();
			this.blockPoolUsed = from.GetBlockPoolUsed();
			this.cacheCapacity = from.GetCacheCapacity();
			this.cacheUsed = from.GetCacheUsed();
			this.lastUpdate = from.GetLastUpdate();
			this.lastUpdateMonotonic = from.GetLastUpdateMonotonic();
			this.xceiverCount = from.GetXceiverCount();
			this.location = from.GetNetworkLocation();
			this.adminState = from.GetAdminState();
		}

		public DatanodeInfo(DatanodeID nodeID)
			: base(nodeID)
		{
			this.capacity = 0L;
			this.dfsUsed = 0L;
			this.remaining = 0L;
			this.blockPoolUsed = 0L;
			this.cacheCapacity = 0L;
			this.cacheUsed = 0L;
			this.lastUpdate = 0L;
			this.lastUpdateMonotonic = 0L;
			this.xceiverCount = 0;
			this.adminState = null;
		}

		public DatanodeInfo(DatanodeID nodeID, string location)
			: this(nodeID)
		{
			this.location = location;
		}

		public DatanodeInfo(DatanodeID nodeID, string location, long capacity, long dfsUsed
			, long remaining, long blockPoolUsed, long cacheCapacity, long cacheUsed, long lastUpdate
			, long lastUpdateMonotonic, int xceiverCount, DatanodeInfo.AdminStates adminState
			)
			: this(nodeID.GetIpAddr(), nodeID.GetHostName(), nodeID.GetDatanodeUuid(), nodeID
				.GetXferPort(), nodeID.GetInfoPort(), nodeID.GetInfoSecurePort(), nodeID.GetIpcPort
				(), capacity, dfsUsed, remaining, blockPoolUsed, cacheCapacity, cacheUsed, lastUpdate
				, lastUpdateMonotonic, xceiverCount, location, adminState)
		{
		}

		/// <summary>Constructor</summary>
		public DatanodeInfo(string ipAddr, string hostName, string datanodeUuid, int xferPort
			, int infoPort, int infoSecurePort, int ipcPort, long capacity, long dfsUsed, long
			 remaining, long blockPoolUsed, long cacheCapacity, long cacheUsed, long lastUpdate
			, long lastUpdateMonotonic, int xceiverCount, string networkLocation, DatanodeInfo.AdminStates
			 adminState)
			: base(ipAddr, hostName, datanodeUuid, xferPort, infoPort, infoSecurePort, ipcPort
				)
		{
			this.capacity = capacity;
			this.dfsUsed = dfsUsed;
			this.remaining = remaining;
			this.blockPoolUsed = blockPoolUsed;
			this.cacheCapacity = cacheCapacity;
			this.cacheUsed = cacheUsed;
			this.lastUpdate = lastUpdate;
			this.lastUpdateMonotonic = lastUpdateMonotonic;
			this.xceiverCount = xceiverCount;
			this.location = networkLocation;
			this.adminState = adminState;
		}

		/// <summary>Network location name</summary>
		public virtual string GetName()
		{
			return GetXferAddr();
		}

		/// <summary>The raw capacity.</summary>
		public virtual long GetCapacity()
		{
			return capacity;
		}

		/// <summary>The used space by the data node.</summary>
		public virtual long GetDfsUsed()
		{
			return dfsUsed;
		}

		/// <summary>The used space by the block pool on data node.</summary>
		public virtual long GetBlockPoolUsed()
		{
			return blockPoolUsed;
		}

		/// <summary>The used space by the data node.</summary>
		public virtual long GetNonDfsUsed()
		{
			long nonDFSUsed = capacity - dfsUsed - remaining;
			return nonDFSUsed < 0 ? 0 : nonDFSUsed;
		}

		/// <summary>The used space by the data node as percentage of present capacity</summary>
		public virtual float GetDfsUsedPercent()
		{
			return DFSUtil.GetPercentUsed(dfsUsed, capacity);
		}

		/// <summary>The raw free space.</summary>
		public virtual long GetRemaining()
		{
			return remaining;
		}

		/// <summary>Used space by the block pool as percentage of present capacity</summary>
		public virtual float GetBlockPoolUsedPercent()
		{
			return DFSUtil.GetPercentUsed(blockPoolUsed, capacity);
		}

		/// <summary>The remaining space as percentage of configured capacity.</summary>
		public virtual float GetRemainingPercent()
		{
			return DFSUtil.GetPercentRemaining(remaining, capacity);
		}

		/// <returns>Amount of cache capacity in bytes</returns>
		public virtual long GetCacheCapacity()
		{
			return cacheCapacity;
		}

		/// <returns>Amount of cache used in bytes</returns>
		public virtual long GetCacheUsed()
		{
			return cacheUsed;
		}

		/// <returns>Cache used as a percentage of the datanode's total cache capacity</returns>
		public virtual float GetCacheUsedPercent()
		{
			return DFSUtil.GetPercentUsed(cacheUsed, cacheCapacity);
		}

		/// <returns>Amount of cache remaining in bytes</returns>
		public virtual long GetCacheRemaining()
		{
			return cacheCapacity - cacheUsed;
		}

		/// <returns>
		/// Cache remaining as a percentage of the datanode's total cache
		/// capacity
		/// </returns>
		public virtual float GetCacheRemainingPercent()
		{
			return DFSUtil.GetPercentRemaining(GetCacheRemaining(), cacheCapacity);
		}

		/// <summary>Get the last update timestamp.</summary>
		/// <remarks>
		/// Get the last update timestamp.
		/// Return value is suitable for Date conversion.
		/// </remarks>
		public virtual long GetLastUpdate()
		{
			return lastUpdate;
		}

		/// <summary>The time when this information was accurate.</summary>
		/// <remarks>
		/// The time when this information was accurate. <br />
		/// Ps: So return value is ideal for calculation of time differences.
		/// Should not be used to convert to Date.
		/// </remarks>
		public virtual long GetLastUpdateMonotonic()
		{
			return lastUpdateMonotonic;
		}

		/// <summary>Set lastUpdate monotonic time</summary>
		public virtual void SetLastUpdateMonotonic(long lastUpdateMonotonic)
		{
			this.lastUpdateMonotonic = lastUpdateMonotonic;
		}

		/// <summary>number of active connections</summary>
		public virtual int GetXceiverCount()
		{
			return xceiverCount;
		}

		/// <summary>Sets raw capacity.</summary>
		public virtual void SetCapacity(long capacity)
		{
			this.capacity = capacity;
		}

		/// <summary>Sets the used space for the datanode.</summary>
		public virtual void SetDfsUsed(long dfsUsed)
		{
			this.dfsUsed = dfsUsed;
		}

		/// <summary>Sets raw free space.</summary>
		public virtual void SetRemaining(long remaining)
		{
			this.remaining = remaining;
		}

		/// <summary>Sets block pool used space</summary>
		public virtual void SetBlockPoolUsed(long bpUsed)
		{
			this.blockPoolUsed = bpUsed;
		}

		/// <summary>Sets cache capacity.</summary>
		public virtual void SetCacheCapacity(long cacheCapacity)
		{
			this.cacheCapacity = cacheCapacity;
		}

		/// <summary>Sets cache used.</summary>
		public virtual void SetCacheUsed(long cacheUsed)
		{
			this.cacheUsed = cacheUsed;
		}

		/// <summary>Sets time when this information was accurate.</summary>
		public virtual void SetLastUpdate(long lastUpdate)
		{
			this.lastUpdate = lastUpdate;
		}

		/// <summary>Sets number of active connections</summary>
		public virtual void SetXceiverCount(int xceiverCount)
		{
			this.xceiverCount = xceiverCount;
		}

		/// <summary>network location</summary>
		public virtual string GetNetworkLocation()
		{
			lock (this)
			{
				return location;
			}
		}

		/// <summary>Sets the network location</summary>
		public virtual void SetNetworkLocation(string location)
		{
			lock (this)
			{
				this.location = NodeBase.Normalize(location);
			}
		}

		/// <summary>Add a hostname to a list of network dependencies</summary>
		public virtual void AddDependentHostName(string hostname)
		{
			dependentHostNames.AddItem(hostname);
		}

		/// <summary>List of Network dependencies</summary>
		public virtual IList<string> GetDependentHostNames()
		{
			return dependentHostNames;
		}

		/// <summary>Sets the network dependencies</summary>
		public virtual void SetDependentHostNames(IList<string> dependencyList)
		{
			dependentHostNames = dependencyList;
		}

		/// <summary>A formatted string for reporting the status of the DataNode.</summary>
		public virtual string GetDatanodeReport()
		{
			StringBuilder buffer = new StringBuilder();
			long c = GetCapacity();
			long r = GetRemaining();
			long u = GetDfsUsed();
			long nonDFSUsed = GetNonDfsUsed();
			float usedPercent = GetDfsUsedPercent();
			float remainingPercent = GetRemainingPercent();
			long cc = GetCacheCapacity();
			long cr = GetCacheRemaining();
			long cu = GetCacheUsed();
			float cacheUsedPercent = GetCacheUsedPercent();
			float cacheRemainingPercent = GetCacheRemainingPercent();
			string lookupName = NetUtils.GetHostNameOfIP(GetName());
			buffer.Append("Name: " + GetName());
			if (lookupName != null)
			{
				buffer.Append(" (" + lookupName + ")");
			}
			buffer.Append("\n");
			buffer.Append("Hostname: " + GetHostName() + "\n");
			if (!NetworkTopology.DefaultRack.Equals(location))
			{
				buffer.Append("Rack: " + location + "\n");
			}
			buffer.Append("Decommission Status : ");
			if (IsDecommissioned())
			{
				buffer.Append("Decommissioned\n");
			}
			else
			{
				if (IsDecommissionInProgress())
				{
					buffer.Append("Decommission in progress\n");
				}
				else
				{
					buffer.Append("Normal\n");
				}
			}
			buffer.Append("Configured Capacity: " + c + " (" + StringUtils.ByteDesc(c) + ")" 
				+ "\n");
			buffer.Append("DFS Used: " + u + " (" + StringUtils.ByteDesc(u) + ")" + "\n");
			buffer.Append("Non DFS Used: " + nonDFSUsed + " (" + StringUtils.ByteDesc(nonDFSUsed
				) + ")" + "\n");
			buffer.Append("DFS Remaining: " + r + " (" + StringUtils.ByteDesc(r) + ")" + "\n"
				);
			buffer.Append("DFS Used%: " + DFSUtil.Percent2String(usedPercent) + "\n");
			buffer.Append("DFS Remaining%: " + DFSUtil.Percent2String(remainingPercent) + "\n"
				);
			buffer.Append("Configured Cache Capacity: " + cc + " (" + StringUtils.ByteDesc(cc
				) + ")" + "\n");
			buffer.Append("Cache Used: " + cu + " (" + StringUtils.ByteDesc(cu) + ")" + "\n");
			buffer.Append("Cache Remaining: " + cr + " (" + StringUtils.ByteDesc(cr) + ")" + 
				"\n");
			buffer.Append("Cache Used%: " + DFSUtil.Percent2String(cacheUsedPercent) + "\n");
			buffer.Append("Cache Remaining%: " + DFSUtil.Percent2String(cacheRemainingPercent
				) + "\n");
			buffer.Append("Xceivers: " + GetXceiverCount() + "\n");
			buffer.Append("Last contact: " + Sharpen.Extensions.CreateDate(lastUpdate) + "\n"
				);
			return buffer.ToString();
		}

		/// <summary>A formatted string for printing the status of the DataNode.</summary>
		public virtual string DumpDatanode()
		{
			StringBuilder buffer = new StringBuilder();
			long c = GetCapacity();
			long r = GetRemaining();
			long u = GetDfsUsed();
			float usedPercent = GetDfsUsedPercent();
			long cc = GetCacheCapacity();
			long cr = GetCacheRemaining();
			long cu = GetCacheUsed();
			float cacheUsedPercent = GetCacheUsedPercent();
			buffer.Append(GetName());
			if (!NetworkTopology.DefaultRack.Equals(location))
			{
				buffer.Append(" " + location);
			}
			if (IsDecommissioned())
			{
				buffer.Append(" DD");
			}
			else
			{
				if (IsDecommissionInProgress())
				{
					buffer.Append(" DP");
				}
				else
				{
					buffer.Append(" IN");
				}
			}
			buffer.Append(" " + c + "(" + StringUtils.ByteDesc(c) + ")");
			buffer.Append(" " + u + "(" + StringUtils.ByteDesc(u) + ")");
			buffer.Append(" " + DFSUtil.Percent2String(usedPercent));
			buffer.Append(" " + r + "(" + StringUtils.ByteDesc(r) + ")");
			buffer.Append(" " + cc + "(" + StringUtils.ByteDesc(cc) + ")");
			buffer.Append(" " + cu + "(" + StringUtils.ByteDesc(cu) + ")");
			buffer.Append(" " + DFSUtil.Percent2String(cacheUsedPercent));
			buffer.Append(" " + cr + "(" + StringUtils.ByteDesc(cr) + ")");
			buffer.Append(" " + Sharpen.Extensions.CreateDate(lastUpdate));
			return buffer.ToString();
		}

		/// <summary>Start decommissioning a node.</summary>
		/// <remarks>
		/// Start decommissioning a node.
		/// old state.
		/// </remarks>
		public virtual void StartDecommission()
		{
			adminState = DatanodeInfo.AdminStates.DecommissionInprogress;
		}

		/// <summary>Stop decommissioning a node.</summary>
		/// <remarks>
		/// Stop decommissioning a node.
		/// old state.
		/// </remarks>
		public virtual void StopDecommission()
		{
			adminState = null;
		}

		/// <summary>Returns true if the node is in the process of being decommissioned</summary>
		public virtual bool IsDecommissionInProgress()
		{
			return adminState == DatanodeInfo.AdminStates.DecommissionInprogress;
		}

		/// <summary>Returns true if the node has been decommissioned.</summary>
		public virtual bool IsDecommissioned()
		{
			return adminState == DatanodeInfo.AdminStates.Decommissioned;
		}

		/// <summary>Sets the admin state to indicate that decommission is complete.</summary>
		public virtual void SetDecommissioned()
		{
			adminState = DatanodeInfo.AdminStates.Decommissioned;
		}

		/// <summary>Retrieves the admin state of this node.</summary>
		public virtual DatanodeInfo.AdminStates GetAdminState()
		{
			if (adminState == null)
			{
				return DatanodeInfo.AdminStates.Normal;
			}
			return adminState;
		}

		/// <summary>Check if the datanode is in stale state.</summary>
		/// <remarks>
		/// Check if the datanode is in stale state. Here if
		/// the namenode has not received heartbeat msg from a
		/// datanode for more than staleInterval (default value is
		/// <see cref="Org.Apache.Hadoop.Hdfs.DFSConfigKeys.DfsNamenodeStaleDatanodeIntervalDefault
		/// 	"/>
		/// ),
		/// the datanode will be treated as stale node.
		/// </remarks>
		/// <param name="staleInterval">
		/// the time interval for marking the node as stale. If the last
		/// update time is beyond the given time interval, the node will be
		/// marked as stale.
		/// </param>
		/// <returns>true if the node is stale</returns>
		public virtual bool IsStale(long staleInterval)
		{
			return (Time.MonotonicNow() - lastUpdateMonotonic) >= staleInterval;
		}

		/// <summary>Sets the admin state of this node.</summary>
		protected internal virtual void SetAdminState(DatanodeInfo.AdminStates newState)
		{
			if (newState == DatanodeInfo.AdminStates.Normal)
			{
				adminState = null;
			}
			else
			{
				adminState = newState;
			}
		}

		[System.NonSerialized]
		private int level;

		[System.NonSerialized]
		private Node parent;

		//which level of the tree the node resides
		//its parent
		/// <summary>Return this node's parent</summary>
		public virtual Node GetParent()
		{
			return parent;
		}

		public virtual void SetParent(Node parent)
		{
			this.parent = parent;
		}

		/// <summary>Return this node's level in the tree.</summary>
		/// <remarks>
		/// Return this node's level in the tree.
		/// E.g. the root of a tree returns 0 and its children return 1
		/// </remarks>
		public virtual int GetLevel()
		{
			return level;
		}

		public virtual void SetLevel(int level)
		{
			this.level = level;
		}

		public override int GetHashCode()
		{
			// Super implementation is sufficient
			return base.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			// Sufficient to use super equality as datanodes are uniquely identified
			// by DatanodeID
			return (this == obj) || base.Equals(obj);
		}

		public virtual string GetSoftwareVersion()
		{
			return softwareVersion;
		}

		public virtual void SetSoftwareVersion(string softwareVersion)
		{
			this.softwareVersion = softwareVersion;
		}
	}
}
