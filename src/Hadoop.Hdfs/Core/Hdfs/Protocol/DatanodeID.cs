using Com.Google.Common.Annotations;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>This class represents the primary identifier for a Datanode.</summary>
	/// <remarks>
	/// This class represents the primary identifier for a Datanode.
	/// Datanodes are identified by how they can be contacted (hostname
	/// and ports) and their storage ID, a unique number that associates
	/// the Datanodes blocks with a particular Datanode.
	/// <see cref="DatanodeInfo.GetName()"/>
	/// should be used to get the network
	/// location (for topology) of a datanode, instead of using
	/// <see cref="GetXferAddr()"/>
	/// here. Helpers are defined below
	/// for each context in which a DatanodeID is used.
	/// </remarks>
	public class DatanodeID : Comparable<Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID>
	{
		public static readonly Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID[] EmptyArray = 
			new Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID[] {  };

		private string ipAddr;

		private string hostName;

		private string peerHostName;

		private int xferPort;

		private int infoPort;

		private int infoSecurePort;

		private int ipcPort;

		private string xferAddr;

		/// <summary>UUID identifying a given datanode.</summary>
		/// <remarks>
		/// UUID identifying a given datanode. For upgraded Datanodes this is the
		/// same as the StorageID that was previously used by this Datanode.
		/// For newly formatted Datanodes it is a UUID.
		/// </remarks>
		private readonly string datanodeUuid;

		public DatanodeID(Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID from)
			: this(from.GetDatanodeUuid(), from)
		{
		}

		[VisibleForTesting]
		public DatanodeID(string datanodeUuid, Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID
			 from)
			: this(from.GetIpAddr(), from.GetHostName(), datanodeUuid, from.GetXferPort(), from
				.GetInfoPort(), from.GetInfoSecurePort(), from.GetIpcPort())
		{
			// IP address
			// hostname claimed by datanode
			// hostname from the actual connection
			// data streaming port
			// info server port
			// info server port
			// IPC server port
			this.peerHostName = from.GetPeerHostName();
		}

		/// <summary>Create a DatanodeID</summary>
		/// <param name="ipAddr">IP</param>
		/// <param name="hostName">hostname</param>
		/// <param name="datanodeUuid">
		/// data node ID, UUID for new Datanodes, may be the
		/// storage ID for pre-UUID datanodes. NULL if unknown
		/// e.g. if this is a new datanode. A new UUID will
		/// be assigned by the namenode.
		/// </param>
		/// <param name="xferPort">data transfer port</param>
		/// <param name="infoPort">info server port</param>
		/// <param name="ipcPort">ipc server port</param>
		public DatanodeID(string ipAddr, string hostName, string datanodeUuid, int xferPort
			, int infoPort, int infoSecurePort, int ipcPort)
		{
			SetIpAndXferPort(ipAddr, xferPort);
			this.hostName = hostName;
			this.datanodeUuid = CheckDatanodeUuid(datanodeUuid);
			this.infoPort = infoPort;
			this.infoSecurePort = infoSecurePort;
			this.ipcPort = ipcPort;
		}

		public virtual void SetIpAddr(string ipAddr)
		{
			//updated during registration, preserve former xferPort
			SetIpAndXferPort(ipAddr, xferPort);
		}

		private void SetIpAndXferPort(string ipAddr, int xferPort)
		{
			// build xferAddr string to reduce cost of frequent use
			this.ipAddr = ipAddr;
			this.xferPort = xferPort;
			this.xferAddr = ipAddr + ":" + xferPort;
		}

		public virtual void SetPeerHostName(string peerHostName)
		{
			this.peerHostName = peerHostName;
		}

		/// <returns>data node ID.</returns>
		public virtual string GetDatanodeUuid()
		{
			return datanodeUuid;
		}

		private string CheckDatanodeUuid(string uuid)
		{
			if (uuid == null || uuid.IsEmpty())
			{
				return null;
			}
			else
			{
				return uuid;
			}
		}

		/// <returns>ipAddr;</returns>
		public virtual string GetIpAddr()
		{
			return ipAddr;
		}

		/// <returns>hostname</returns>
		public virtual string GetHostName()
		{
			return hostName;
		}

		/// <returns>hostname from the actual connection</returns>
		public virtual string GetPeerHostName()
		{
			return peerHostName;
		}

		/// <returns>IP:xferPort string</returns>
		public virtual string GetXferAddr()
		{
			return xferAddr;
		}

		/// <returns>IP:ipcPort string</returns>
		private string GetIpcAddr()
		{
			return ipAddr + ":" + ipcPort;
		}

		/// <returns>IP:infoPort string</returns>
		public virtual string GetInfoAddr()
		{
			return ipAddr + ":" + infoPort;
		}

		/// <returns>IP:infoPort string</returns>
		public virtual string GetInfoSecureAddr()
		{
			return ipAddr + ":" + infoSecurePort;
		}

		/// <returns>hostname:xferPort</returns>
		public virtual string GetXferAddrWithHostname()
		{
			return hostName + ":" + xferPort;
		}

		/// <returns>hostname:ipcPort</returns>
		private string GetIpcAddrWithHostname()
		{
			return hostName + ":" + ipcPort;
		}

		/// <param name="useHostname">true to use the DN hostname, use the IP otherwise</param>
		/// <returns>name:xferPort</returns>
		public virtual string GetXferAddr(bool useHostname)
		{
			return useHostname ? GetXferAddrWithHostname() : GetXferAddr();
		}

		/// <param name="useHostname">true to use the DN hostname, use the IP otherwise</param>
		/// <returns>name:ipcPort</returns>
		public virtual string GetIpcAddr(bool useHostname)
		{
			return useHostname ? GetIpcAddrWithHostname() : GetIpcAddr();
		}

		/// <returns>xferPort (the port for data streaming)</returns>
		public virtual int GetXferPort()
		{
			return xferPort;
		}

		/// <returns>infoPort (the port at which the HTTP server bound to)</returns>
		public virtual int GetInfoPort()
		{
			return infoPort;
		}

		/// <returns>infoSecurePort (the port at which the HTTPS server bound to)</returns>
		public virtual int GetInfoSecurePort()
		{
			return infoSecurePort;
		}

		/// <returns>ipcPort (the port at which the IPC server bound to)</returns>
		public virtual int GetIpcPort()
		{
			return ipcPort;
		}

		public override bool Equals(object to)
		{
			if (this == to)
			{
				return true;
			}
			if (!(to is Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID))
			{
				return false;
			}
			return (GetXferAddr().Equals(((Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID)to).GetXferAddr
				()) && datanodeUuid.Equals(((Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID)to).GetDatanodeUuid
				()));
		}

		public override int GetHashCode()
		{
			return datanodeUuid.GetHashCode();
		}

		public override string ToString()
		{
			return GetXferAddr();
		}

		/// <summary>Update fields when a new registration request comes in.</summary>
		/// <remarks>
		/// Update fields when a new registration request comes in.
		/// Note that this does not update storageID.
		/// </remarks>
		public virtual void UpdateRegInfo(Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID nodeReg
			)
		{
			SetIpAndXferPort(nodeReg.GetIpAddr(), nodeReg.GetXferPort());
			hostName = nodeReg.GetHostName();
			peerHostName = nodeReg.GetPeerHostName();
			infoPort = nodeReg.GetInfoPort();
			infoSecurePort = nodeReg.GetInfoSecurePort();
			ipcPort = nodeReg.GetIpcPort();
		}

		/// <summary>Compare based on data transfer address.</summary>
		/// <param name="that">datanode to compare with</param>
		/// <returns>as specified by Comparable</returns>
		public virtual int CompareTo(Org.Apache.Hadoop.Hdfs.Protocol.DatanodeID that)
		{
			return string.CompareOrdinal(GetXferAddr(), that.GetXferAddr());
		}
	}
}
