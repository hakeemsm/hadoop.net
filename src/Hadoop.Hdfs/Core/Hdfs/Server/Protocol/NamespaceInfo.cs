using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// NamespaceInfo is returned by the name-node in reply
	/// to a data-node handshake.
	/// </summary>
	public class NamespaceInfo : StorageInfo
	{
		internal readonly string buildVersion;

		internal string blockPoolID = string.Empty;

		internal string softwareVersion;

		internal long capabilities;

		private static readonly long CapabilitiesSupported = GetSupportedCapabilities();

		// id of the block pool
		// only authoritative on the server-side to determine advertisement to
		// clients.  enum will update the supported values
		private static long GetSupportedCapabilities()
		{
			long mask = 0;
			foreach (NamespaceInfo.Capability c in NamespaceInfo.Capability.Values())
			{
				if (c.supported)
				{
					mask |= c.mask;
				}
			}
			return mask;
		}

		[System.Serializable]
		public sealed class Capability
		{
			public static readonly NamespaceInfo.Capability Unknown = new NamespaceInfo.Capability
				(false);

			public static readonly NamespaceInfo.Capability StorageBlockReportBuffers = new NamespaceInfo.Capability
				(true);

			private readonly bool supported;

			private readonly long mask;

			internal Capability(bool isSupported)
			{
				// use optimized ByteString buffers
				NamespaceInfo.Capability.supported = isSupported;
				int bits = Ordinal() - 1;
				NamespaceInfo.Capability.mask = (bits < 0) ? 0 : (1L << bits);
			}

			public long GetMask()
			{
				return NamespaceInfo.Capability.mask;
			}
		}

		public NamespaceInfo()
			: base(HdfsServerConstants.NodeType.NameNode)
		{
			// defaults to enabled capabilites since this ctor is for server
			buildVersion = null;
			capabilities = CapabilitiesSupported;
		}

		public NamespaceInfo(int nsID, string clusterID, string bpID, long cT, string buildVersion
			, string softwareVersion)
			: this(nsID, clusterID, bpID, cT, buildVersion, softwareVersion, CapabilitiesSupported
				)
		{
		}

		public NamespaceInfo(int nsID, string clusterID, string bpID, long cT, string buildVersion
			, string softwareVersion, long capabilities)
			: base(HdfsConstants.NamenodeLayoutVersion, nsID, clusterID, cT, HdfsServerConstants.NodeType
				.NameNode)
		{
			// defaults to enabled capabilites since this ctor is for server
			// for use by server and/or client
			blockPoolID = bpID;
			this.buildVersion = buildVersion;
			this.softwareVersion = softwareVersion;
			this.capabilities = capabilities;
		}

		public NamespaceInfo(int nsID, string clusterID, string bpID, long cT)
			: this(nsID, clusterID, bpID, cT, Storage.GetBuildVersion(), VersionInfo.GetVersion
				())
		{
		}

		public virtual long GetCapabilities()
		{
			return capabilities;
		}

		[VisibleForTesting]
		public virtual void SetCapabilities(long capabilities)
		{
			this.capabilities = capabilities;
		}

		public virtual bool IsCapabilitySupported(NamespaceInfo.Capability capability)
		{
			Preconditions.CheckArgument(capability != NamespaceInfo.Capability.Unknown, "cannot test for unknown capability"
				);
			long mask = capability.GetMask();
			return (capabilities & mask) == mask;
		}

		public virtual string GetBuildVersion()
		{
			return buildVersion;
		}

		public virtual string GetBlockPoolID()
		{
			return blockPoolID;
		}

		public virtual string GetSoftwareVersion()
		{
			return softwareVersion;
		}

		public override string ToString()
		{
			return base.ToString() + ";bpid=" + blockPoolID;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ValidateStorage(NNStorage storage)
		{
			if (layoutVersion != storage.GetLayoutVersion() || namespaceID != storage.GetNamespaceID
				() || cTime != storage.cTime || !clusterID.Equals(storage.GetClusterID()) || !blockPoolID
				.Equals(storage.GetBlockPoolID()))
			{
				throw new IOException("Inconsistent namespace information:\n" + "NamespaceInfo has:\n"
					 + "LV=" + layoutVersion + ";" + "NS=" + namespaceID + ";" + "cTime=" + cTime + 
					";" + "CID=" + clusterID + ";" + "BPID=" + blockPoolID + ".\nStorage has:\n" + "LV="
					 + storage.GetLayoutVersion() + ";" + "NS=" + storage.GetNamespaceID() + ";" + "cTime="
					 + storage.GetCTime() + ";" + "CID=" + storage.GetClusterID() + ";" + "BPID=" + 
					storage.GetBlockPoolID() + ".");
			}
		}
	}
}
