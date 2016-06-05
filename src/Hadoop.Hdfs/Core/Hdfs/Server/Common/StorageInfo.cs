using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	/// <summary>Common class for storage information.</summary>
	/// <remarks>
	/// Common class for storage information.
	/// TODO namespaceID should be long and computed as hash(address + port)
	/// </remarks>
	public class StorageInfo
	{
		public int layoutVersion;

		public int namespaceID;

		public string clusterID;

		public long cTime;

		protected internal readonly HdfsServerConstants.NodeType storageType;

		protected internal const string StorageFileVersion = "VERSION";

		public StorageInfo(HdfsServerConstants.NodeType type)
			: this(0, 0, string.Empty, 0L, type)
		{
		}

		public StorageInfo(int layoutV, int nsID, string cid, long cT, HdfsServerConstants.NodeType
			 type)
		{
			// layout version of the storage data
			// id of the file system
			// id of the cluster
			// creation time of the file system state
			// Type of the node using this storage 
			layoutVersion = layoutV;
			clusterID = cid;
			namespaceID = nsID;
			cTime = cT;
			storageType = type;
		}

		public StorageInfo(Org.Apache.Hadoop.Hdfs.Server.Common.StorageInfo from)
			: this(from.layoutVersion, from.namespaceID, from.clusterID, from.cTime, from.storageType
				)
		{
		}

		/// <summary>Layout version of the storage data.</summary>
		public virtual int GetLayoutVersion()
		{
			return layoutVersion;
		}

		/// <summary>
		/// Namespace id of the file system.<p>
		/// Assigned to the file system at formatting and never changes after that.
		/// </summary>
		/// <remarks>
		/// Namespace id of the file system.<p>
		/// Assigned to the file system at formatting and never changes after that.
		/// Shared by all file system components.
		/// </remarks>
		public virtual int GetNamespaceID()
		{
			return namespaceID;
		}

		/// <summary>cluster id of the file system.<p></summary>
		public virtual string GetClusterID()
		{
			return clusterID;
		}

		/// <summary>
		/// Creation time of the file system state.<p>
		/// Modified during upgrades.
		/// </summary>
		public virtual long GetCTime()
		{
			return cTime;
		}

		public virtual void SetStorageInfo(Org.Apache.Hadoop.Hdfs.Server.Common.StorageInfo
			 from)
		{
			layoutVersion = from.layoutVersion;
			clusterID = from.clusterID;
			namespaceID = from.namespaceID;
			cTime = from.cTime;
		}

		public virtual bool VersionSupportsFederation(IDictionary<int, ICollection<LayoutVersion.LayoutFeature
			>> map)
		{
			return LayoutVersion.Supports(map, LayoutVersion.Feature.Federation, layoutVersion
				);
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("lv=").Append(layoutVersion).Append(";cid=").Append(clusterID).Append(";nsid="
				).Append(namespaceID).Append(";c=").Append(cTime);
			return sb.ToString();
		}

		public virtual string ToColonSeparatedString()
		{
			return Joiner.On(":").Join(layoutVersion, namespaceID, cTime, clusterID);
		}

		/// <summary>Get common storage fields.</summary>
		/// <remarks>
		/// Get common storage fields.
		/// Should be overloaded if additional fields need to be get.
		/// </remarks>
		/// <param name="props">properties</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		protected internal virtual void SetFieldsFromProperties(Properties props, Storage.StorageDirectory
			 sd)
		{
			SetLayoutVersion(props, sd);
			SetNamespaceID(props, sd);
			SetcTime(props, sd);
			SetClusterId(props, layoutVersion, sd);
			CheckStorageType(props, sd);
		}

		/// <summary>
		/// Validate and set storage type from
		/// <see cref="Sharpen.Properties"/>
		/// 
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.InconsistentFSStateException
		/// 	"/>
		protected internal virtual void CheckStorageType(Properties props, Storage.StorageDirectory
			 sd)
		{
			if (storageType == null)
			{
				//don't care about storage type
				return;
			}
			HdfsServerConstants.NodeType type = HdfsServerConstants.NodeType.ValueOf(GetProperty
				(props, sd, "storageType"));
			if (!storageType.Equals(type))
			{
				throw new InconsistentFSStateException(sd.root, "Incompatible node types: storageType="
					 + storageType + " but StorageDirectory type=" + type);
			}
		}

		/// <summary>
		/// Validate and set ctime from
		/// <see cref="Sharpen.Properties"/>
		/// 
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.InconsistentFSStateException
		/// 	"/>
		protected internal virtual void SetcTime(Properties props, Storage.StorageDirectory
			 sd)
		{
			cTime = long.Parse(GetProperty(props, sd, "cTime"));
		}

		/// <summary>
		/// Validate and set clusterId from
		/// <see cref="Sharpen.Properties"/>
		/// 
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.InconsistentFSStateException
		/// 	"/>
		protected internal virtual void SetClusterId(Properties props, int layoutVersion, 
			Storage.StorageDirectory sd)
		{
			// Set cluster ID in version that supports federation
			if (LayoutVersion.Supports(GetServiceLayoutFeatureMap(), LayoutVersion.Feature.Federation
				, layoutVersion))
			{
				string cid = GetProperty(props, sd, "clusterID");
				if (!(clusterID.Equals(string.Empty) || cid.Equals(string.Empty) || clusterID.Equals
					(cid)))
				{
					throw new InconsistentFSStateException(sd.GetRoot(), "cluster Id is incompatible with others."
						);
				}
				clusterID = cid;
			}
		}

		/// <summary>
		/// Validate and set layout version from
		/// <see cref="Sharpen.Properties"/>
		/// 
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.IncorrectVersionException"/
		/// 	>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.InconsistentFSStateException
		/// 	"/>
		protected internal virtual void SetLayoutVersion(Properties props, Storage.StorageDirectory
			 sd)
		{
			int lv = System.Convert.ToInt32(GetProperty(props, sd, "layoutVersion"));
			if (lv < GetServiceLayoutVersion())
			{
				// future version
				throw new IncorrectVersionException(GetServiceLayoutVersion(), lv, "storage directory "
					 + sd.root.GetAbsolutePath());
			}
			layoutVersion = lv;
		}

		/// <summary>
		/// Validate and set namespaceID version from
		/// <see cref="Sharpen.Properties"/>
		/// 
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.InconsistentFSStateException
		/// 	"/>
		protected internal virtual void SetNamespaceID(Properties props, Storage.StorageDirectory
			 sd)
		{
			int nsId = System.Convert.ToInt32(GetProperty(props, sd, "namespaceID"));
			if (namespaceID != 0 && nsId != 0 && namespaceID != nsId)
			{
				throw new InconsistentFSStateException(sd.root, "namespaceID is incompatible with others."
					);
			}
			namespaceID = nsId;
		}

		public virtual void SetServiceLayoutVersion(int lv)
		{
			this.layoutVersion = lv;
		}

		public virtual int GetServiceLayoutVersion()
		{
			return storageType == HdfsServerConstants.NodeType.DataNode ? HdfsConstants.DatanodeLayoutVersion
				 : HdfsConstants.NamenodeLayoutVersion;
		}

		public virtual IDictionary<int, ICollection<LayoutVersion.LayoutFeature>> GetServiceLayoutFeatureMap
			()
		{
			return storageType == HdfsServerConstants.NodeType.DataNode ? DataNodeLayoutVersion
				.Features : NameNodeLayoutVersion.Features;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.InconsistentFSStateException
		/// 	"/>
		protected internal static string GetProperty(Properties props, Storage.StorageDirectory
			 sd, string name)
		{
			string property = props.GetProperty(name);
			if (property == null)
			{
				throw new InconsistentFSStateException(sd.root, "file " + StorageFileVersion + " has "
					 + name + " missing.");
			}
			return property;
		}

		public static int GetNsIdFromColonSeparatedString(string @in)
		{
			return System.Convert.ToInt32(@in.Split(":")[1]);
		}

		public static string GetClusterIdFromColonSeparatedString(string @in)
		{
			return @in.Split(":")[3];
		}

		/// <summary>Read properties from the VERSION file in the given storage directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadProperties(Storage.StorageDirectory sd)
		{
			Properties props = ReadPropertiesFile(sd.GetVersionFile());
			SetFieldsFromProperties(props, sd);
		}

		/// <summary>Read properties from the the previous/VERSION file in the given storage directory.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadPreviousVersionProperties(Storage.StorageDirectory sd)
		{
			Properties props = ReadPropertiesFile(sd.GetPreviousVersionFile());
			SetFieldsFromProperties(props, sd);
		}

		/// <exception cref="System.IO.IOException"/>
		public static Properties ReadPropertiesFile(FilePath from)
		{
			RandomAccessFile file = new RandomAccessFile(from, "rws");
			FileInputStream @in = null;
			Properties props = new Properties();
			try
			{
				@in = new FileInputStream(file.GetFD());
				file.Seek(0);
				props.Load(@in);
			}
			finally
			{
				if (@in != null)
				{
					@in.Close();
				}
				file.Close();
			}
			return props;
		}
	}
}
