using System;
using System.Collections.Generic;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>This class tracks changes in the layout version of HDFS.</summary>
	/// <remarks>
	/// This class tracks changes in the layout version of HDFS.
	/// Layout version is changed for following reasons:
	/// <ol>
	/// <li>The layout of how namenode or datanode stores information
	/// on disk changes.</li>
	/// <li>A new operation code is added to the editlog.</li>
	/// <li>Modification such as format of a record, content of a record
	/// in editlog or fsimage.</li>
	/// </ol>
	/// <br />
	/// <b>How to update layout version:<br /></b>
	/// When a change requires new layout version, please add an entry into
	/// <see cref="Feature"/>
	/// with a short enum name, new layout version and description
	/// of the change. Please see
	/// <see cref="Feature"/>
	/// for further details.
	/// <br />
	/// </remarks>
	public class LayoutVersion
	{
		/// <summary>Version in which HDFS-2991 was fixed.</summary>
		/// <remarks>
		/// Version in which HDFS-2991 was fixed. This bug caused OP_ADD to
		/// sometimes be skipped for append() calls. If we see such a case when
		/// loading the edits, but the version is known to have that bug, we
		/// workaround the issue. Otherwise we should consider it a corruption
		/// and bail.
		/// </remarks>
		public const int BugfixHdfs2991Version = -40;

		/// <summary>The interface to be implemented by NameNode and DataNode layout features
		/// 	</summary>
		public interface LayoutFeature
		{
			LayoutVersion.FeatureInfo GetInfo();
		}

		/// <summary>
		/// Enums for features that change the layout version before rolling
		/// upgrade is supported.
		/// </summary>
		/// <remarks>
		/// Enums for features that change the layout version before rolling
		/// upgrade is supported.
		/// <br /><br />
		/// To add a new layout version:
		/// <ul>
		/// <li>Define a new enum constant with a short enum name, the new layout version
		/// and description of the added feature.</li>
		/// <li>When adding a layout version with an ancestor that is not same as
		/// its immediate predecessor, use the constructor where a specific ancestor
		/// can be passed.
		/// </li>
		/// </ul>
		/// </remarks>
		[System.Serializable]
		public sealed class Feature : LayoutVersion.LayoutFeature
		{
			public static readonly LayoutVersion.Feature NamespaceQuota = new LayoutVersion.Feature
				(-16, "Support for namespace quotas");

			public static readonly LayoutVersion.Feature FileAccessTime = new LayoutVersion.Feature
				(-17, "Support for access time on files");

			public static readonly LayoutVersion.Feature DiskspaceQuota = new LayoutVersion.Feature
				(-18, "Support for disk space quotas");

			public static readonly LayoutVersion.Feature StickyBit = new LayoutVersion.Feature
				(-19, "Support for sticky bits");

			public static readonly LayoutVersion.Feature AppendRbwDir = new LayoutVersion.Feature
				(-20, "Datanode has \"rbw\" subdirectory for append");

			public static readonly LayoutVersion.Feature AtomicRename = new LayoutVersion.Feature
				(-21, "Support for atomic rename");

			public static readonly LayoutVersion.Feature Concat = new LayoutVersion.Feature(-
				22, "Support for concat operation");

			public static readonly LayoutVersion.Feature Symlinks = new LayoutVersion.Feature
				(-23, "Support for symbolic links");

			public static readonly LayoutVersion.Feature DelegationToken = new LayoutVersion.Feature
				(-24, "Support for delegation tokens for security");

			public static readonly LayoutVersion.Feature FsimageCompression = new LayoutVersion.Feature
				(-25, "Support for fsimage compression");

			public static readonly LayoutVersion.Feature FsimageChecksum = new LayoutVersion.Feature
				(-26, "Support checksum for fsimage");

			public static readonly LayoutVersion.Feature RemoveRel13DiskLayoutSupport = new LayoutVersion.Feature
				(-27, "Remove support for 0.13 disk layout");

			public static readonly LayoutVersion.Feature EditsCheskum = new LayoutVersion.Feature
				(-28, "Support checksum for editlog");

			public static readonly LayoutVersion.Feature Unused = new LayoutVersion.Feature(-
				29, "Skipped version");

			public static readonly LayoutVersion.Feature FsimageNameOptimization = new LayoutVersion.Feature
				(-30, "Store only last part of path in fsimage");

			public static readonly LayoutVersion.Feature ReservedRel20203 = new LayoutVersion.Feature
				(-31, -19, "Reserved for release 0.20.203", true, LayoutVersion.Feature.DelegationToken
				);

			public static readonly LayoutVersion.Feature ReservedRel20204 = new LayoutVersion.Feature
				(-32, -31, "Reserved for release 0.20.204", true);

			public static readonly LayoutVersion.Feature ReservedRel22 = new LayoutVersion.Feature
				(-33, -27, "Reserved for release 0.22", true);

			public static readonly LayoutVersion.Feature ReservedRel23 = new LayoutVersion.Feature
				(-34, -30, "Reserved for release 0.23", true);

			public static readonly LayoutVersion.Feature Federation = new LayoutVersion.Feature
				(-35, "Support for namenode federation");

			public static readonly LayoutVersion.Feature LeaseReassignment = new LayoutVersion.Feature
				(-36, "Support for persisting lease holder reassignment");

			public static readonly LayoutVersion.Feature StoredTxids = new LayoutVersion.Feature
				(-37, "Transaction IDs are stored in edits log and image files");

			public static readonly LayoutVersion.Feature TxidBasedLayout = new LayoutVersion.Feature
				(-38, "File names in NN Storage are based on transaction IDs");

			public static readonly LayoutVersion.Feature EditlogOpOptimization = new LayoutVersion.Feature
				(-39, "Use LongWritable and ShortWritable directly instead of ArrayWritable of UTF8"
				);

			public static readonly LayoutVersion.Feature OptimizePersistBlocks = new LayoutVersion.Feature
				(-40, "Serialize block lists with delta-encoded variable length ints, " + "add OP_UPDATE_BLOCKS"
				);

			public static readonly LayoutVersion.Feature ReservedRel120 = new LayoutVersion.Feature
				(-41, -32, "Reserved for release 1.2.0", true, LayoutVersion.Feature.Concat);

			public static readonly LayoutVersion.Feature AddInodeId = new LayoutVersion.Feature
				(-42, -40, "Assign a unique inode id for each inode", false);

			public static readonly LayoutVersion.Feature Snapshot = new LayoutVersion.Feature
				(-43, "Support for snapshot feature");

			public static readonly LayoutVersion.Feature ReservedRel130 = new LayoutVersion.Feature
				(-44, -41, "Reserved for release 1.3.0", true, LayoutVersion.Feature.AddInodeId, 
				LayoutVersion.Feature.Snapshot, LayoutVersion.Feature.FsimageNameOptimization);

			public static readonly LayoutVersion.Feature OptimizeSnapshotInodes = new LayoutVersion.Feature
				(-45, -43, "Reduce snapshot inode memory footprint", false);

			public static readonly LayoutVersion.Feature SequentialBlockId = new LayoutVersion.Feature
				(-46, "Allocate block IDs sequentially and store " + "block IDs in the edits log and image files"
				);

			public static readonly LayoutVersion.Feature EditlogSupportRetrycache = new LayoutVersion.Feature
				(-47, "Record ClientId and CallId in editlog to " + "enable rebuilding retry cache in case of HA failover"
				);

			public static readonly LayoutVersion.Feature EditlogAddBlock = new LayoutVersion.Feature
				(-48, "Add new editlog that only records allocation of " + "the new block instead of the entire block list"
				);

			public static readonly LayoutVersion.Feature AddDatanodeAndStorageUuids = new LayoutVersion.Feature
				(-49, "Replace StorageID with DatanodeUuid." + " Use distinct StorageUuid per storage directory."
				);

			public static readonly LayoutVersion.Feature AddLayoutFlags = new LayoutVersion.Feature
				(-50, "Add support for layout flags.");

			public static readonly LayoutVersion.Feature Caching = new LayoutVersion.Feature(
				-51, "Support for cache pools and path-based caching");

			public static readonly LayoutVersion.Feature ProtobufFormat = new LayoutVersion.Feature
				(-52, "Use protobuf to serialize FSImage");

			public static readonly LayoutVersion.Feature ExtendedAcl = new LayoutVersion.Feature
				(-53, "Extended ACL");

			public static readonly LayoutVersion.Feature ReservedRel240 = new LayoutVersion.Feature
				(-54, -51, "Reserved for release 2.4.0", true, LayoutVersion.Feature.ProtobufFormat
				, LayoutVersion.Feature.ExtendedAcl);

			private readonly LayoutVersion.FeatureInfo info;

			/// <summary>
			/// Feature that is added at layout version
			/// <paramref name="lv"/>
			/// - 1.
			/// </summary>
			/// <param name="lv">new layout version with the addition of this feature</param>
			/// <param name="description">description of the feature</param>
			internal Feature(int lv, string description)
				: this(lv, lv + 1, description, false)
			{
			}

			/// <summary>
			/// Feature that is added at layout version
			/// <c>ancestoryLV</c>
			/// .
			/// </summary>
			/// <param name="lv">new layout version with the addition of this feature</param>
			/// <param name="ancestorLV">layout version from which the new lv is derived from.</param>
			/// <param name="description">description of the feature</param>
			/// <param name="reserved">
			/// true when this is a layout version reserved for previous
			/// version
			/// </param>
			/// <param name="features">set of features that are to be enabled for this version</param>
			internal Feature(int lv, int ancestorLV, string description, bool reserved, params 
				LayoutVersion.Feature[] features)
			{
				// Hadoop 2.4.0
				LayoutVersion.Feature.info = new LayoutVersion.FeatureInfo(lv, ancestorLV, description
					, reserved, features);
			}

			public LayoutVersion.FeatureInfo GetInfo()
			{
				return LayoutVersion.Feature.info;
			}
		}

		/// <summary>Feature information.</summary>
		public class FeatureInfo
		{
			private readonly int lv;

			private readonly int ancestorLV;

			private readonly string description;

			private readonly bool reserved;

			private readonly LayoutVersion.LayoutFeature[] specialFeatures;

			public FeatureInfo(int lv, int ancestorLV, string description, bool reserved, params 
				LayoutVersion.LayoutFeature[] specialFeatures)
			{
				this.lv = lv;
				this.ancestorLV = ancestorLV;
				this.description = description;
				this.reserved = reserved;
				this.specialFeatures = specialFeatures;
			}

			/// <summary>Accessor method for feature layout version</summary>
			/// <returns>int lv value</returns>
			public virtual int GetLayoutVersion()
			{
				return lv;
			}

			/// <summary>Accessor method for feature ancestor layout version</summary>
			/// <returns>int ancestor LV value</returns>
			public virtual int GetAncestorLayoutVersion()
			{
				return ancestorLV;
			}

			/// <summary>Accessor method for feature description</summary>
			/// <returns>String feature description</returns>
			public virtual string GetDescription()
			{
				return description;
			}

			public virtual bool IsReservedForOldRelease()
			{
				return reserved;
			}

			public virtual LayoutVersion.LayoutFeature[] GetSpecialFeatures()
			{
				return specialFeatures;
			}
		}

		internal class LayoutFeatureComparator : IComparer<LayoutVersion.LayoutFeature>
		{
			public virtual int Compare(LayoutVersion.LayoutFeature arg0, LayoutVersion.LayoutFeature
				 arg1)
			{
				return arg0.GetInfo().GetLayoutVersion() - arg1.GetInfo().GetLayoutVersion();
			}
		}

		public static void UpdateMap(IDictionary<int, ICollection<LayoutVersion.LayoutFeature
			>> map, LayoutVersion.LayoutFeature[] features)
		{
			// Go through all the enum constants and build a map of
			// LayoutVersion <-> Set of all supported features in that LayoutVersion
			foreach (LayoutVersion.LayoutFeature f in features)
			{
				LayoutVersion.FeatureInfo info = f.GetInfo();
				ICollection<LayoutVersion.LayoutFeature> ancestorSet = map[info.GetAncestorLayoutVersion
					()];
				if (ancestorSet == null)
				{
					// Empty set
					ancestorSet = new TreeSet<LayoutVersion.LayoutFeature>(new LayoutVersion.LayoutFeatureComparator
						());
					map[info.GetAncestorLayoutVersion()] = ancestorSet;
				}
				ICollection<LayoutVersion.LayoutFeature> featureSet = new TreeSet<LayoutVersion.LayoutFeature
					>(ancestorSet);
				if (info.GetSpecialFeatures() != null)
				{
					foreach (LayoutVersion.LayoutFeature specialFeature in info.GetSpecialFeatures())
					{
						featureSet.AddItem(specialFeature);
					}
				}
				featureSet.AddItem(f);
				map[info.GetLayoutVersion()] = featureSet;
			}
		}

		/// <summary>
		/// Gets formatted string that describes
		/// <see cref="LayoutVersion"/>
		/// information.
		/// </summary>
		public virtual string GetString(IDictionary<int, ICollection<LayoutVersion.LayoutFeature
			>> map, LayoutVersion.LayoutFeature[] values)
		{
			StringBuilder buf = new StringBuilder();
			buf.Append("Feature List:\n");
			foreach (LayoutVersion.LayoutFeature f in values)
			{
				LayoutVersion.FeatureInfo info = f.GetInfo();
				buf.Append(f).Append(" introduced in layout version ").Append(info.GetLayoutVersion
					()).Append(" (").Append(info.GetDescription()).Append(")\n");
			}
			buf.Append("\n\nLayoutVersion and supported features:\n");
			foreach (LayoutVersion.LayoutFeature f_1 in values)
			{
				LayoutVersion.FeatureInfo info = f_1.GetInfo();
				buf.Append(info.GetLayoutVersion()).Append(": ").Append(map[info.GetLayoutVersion
					()]).Append("\n");
			}
			return buf.ToString();
		}

		/// <summary>Returns true if a given feature is supported in the given layout version
		/// 	</summary>
		/// <param name="map">layout feature map</param>
		/// <param name="f">Feature</param>
		/// <param name="lv">LayoutVersion</param>
		/// <returns>
		/// true if
		/// <paramref name="f"/>
		/// is supported in layout version
		/// <paramref name="lv"/>
		/// </returns>
		public static bool Supports(IDictionary<int, ICollection<LayoutVersion.LayoutFeature
			>> map, LayoutVersion.LayoutFeature f, int lv)
		{
			ICollection<LayoutVersion.LayoutFeature> set = map[lv];
			return set != null && set.Contains(f);
		}

		/// <summary>Get the current layout version</summary>
		public static int GetCurrentLayoutVersion(LayoutVersion.LayoutFeature[] features)
		{
			return GetLastNonReservedFeature(features).GetInfo().GetLayoutVersion();
		}

		internal static LayoutVersion.LayoutFeature GetLastNonReservedFeature(LayoutVersion.LayoutFeature
			[] features)
		{
			for (int i = features.Length - 1; i >= 0; i--)
			{
				LayoutVersion.FeatureInfo info = features[i].GetInfo();
				if (!info.IsReservedForOldRelease())
				{
					return features[i];
				}
			}
			throw new Exception("All layout versions are reserved.");
		}
	}
}
