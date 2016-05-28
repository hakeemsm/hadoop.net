using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// DatanodeRegistration class contains all information the name-node needs
	/// to identify and verify a data-node when it contacts the name-node.
	/// </summary>
	/// <remarks>
	/// DatanodeRegistration class contains all information the name-node needs
	/// to identify and verify a data-node when it contacts the name-node.
	/// This information is sent by data-node with each communication request.
	/// </remarks>
	public class DatanodeRegistration : DatanodeID, NodeRegistration
	{
		private readonly StorageInfo storageInfo;

		private ExportedBlockKeys exportedKeys;

		private readonly string softwareVersion;

		private NamespaceInfo nsInfo;

		[VisibleForTesting]
		public DatanodeRegistration(string uuid, Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeRegistration
			 dnr)
			: this(new DatanodeID(uuid, dnr), dnr.GetStorageInfo(), dnr.GetExportedKeys(), dnr
				.GetSoftwareVersion())
		{
		}

		public DatanodeRegistration(DatanodeID dn, StorageInfo info, ExportedBlockKeys keys
			, string softwareVersion)
			: base(dn)
		{
			this.storageInfo = info;
			this.exportedKeys = keys;
			this.softwareVersion = softwareVersion;
		}

		public virtual StorageInfo GetStorageInfo()
		{
			return storageInfo;
		}

		public virtual void SetExportedKeys(ExportedBlockKeys keys)
		{
			this.exportedKeys = keys;
		}

		public virtual ExportedBlockKeys GetExportedKeys()
		{
			return exportedKeys;
		}

		public virtual string GetSoftwareVersion()
		{
			return softwareVersion;
		}

		public virtual int GetVersion()
		{
			// NodeRegistration
			return storageInfo.GetLayoutVersion();
		}

		public virtual void SetNamespaceInfo(NamespaceInfo nsInfo)
		{
			this.nsInfo = nsInfo;
		}

		public virtual NamespaceInfo GetNamespaceInfo()
		{
			return nsInfo;
		}

		public virtual string GetRegistrationID()
		{
			// NodeRegistration
			return Storage.GetRegistrationID(storageInfo);
		}

		public virtual string GetAddress()
		{
			// NodeRegistration
			return GetXferAddr();
		}

		public override string ToString()
		{
			return GetType().Name + "(" + base.ToString() + ", datanodeUuid=" + GetDatanodeUuid
				() + ", infoPort=" + GetInfoPort() + ", infoSecurePort=" + GetInfoSecurePort() +
				 ", ipcPort=" + GetIpcPort() + ", storageInfo=" + storageInfo + ")";
		}

		public override bool Equals(object to)
		{
			return base.Equals(to);
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}
	}
}
