using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class NameNodeLayoutVersion
	{
		/// <summary>Build layout version and corresponding feature matrix</summary>
		public static readonly IDictionary<int, ICollection<LayoutVersion.LayoutFeature>>
			 Features = new Dictionary<int, ICollection<LayoutVersion.LayoutFeature>>();

		public static readonly int CurrentLayoutVersion = LayoutVersion.GetCurrentLayoutVersion
			(NameNodeLayoutVersion.Feature.Values());

		static NameNodeLayoutVersion()
		{
			LayoutVersion.UpdateMap(Features, LayoutVersion.Feature.Values());
			LayoutVersion.UpdateMap(Features, NameNodeLayoutVersion.Feature.Values());
		}

		public static ICollection<LayoutVersion.LayoutFeature> GetFeatures(int lv)
		{
			return Features[lv];
		}

		public static bool Supports(LayoutVersion.LayoutFeature f, int lv)
		{
			return LayoutVersion.Supports(Features, f, lv);
		}

		/// <summary>Enums for features that change the layout version.</summary>
		/// <remarks>
		/// Enums for features that change the layout version.
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
			public static readonly NameNodeLayoutVersion.Feature RollingUpgrade = new NameNodeLayoutVersion.Feature
				(-55, -53, "Support rolling upgrade", false);

			public static readonly NameNodeLayoutVersion.Feature EditlogLength = new NameNodeLayoutVersion.Feature
				(-56, "Add length field to every edit log op");

			public static readonly NameNodeLayoutVersion.Feature Xattrs = new NameNodeLayoutVersion.Feature
				(-57, "Extended attributes");

			public static readonly NameNodeLayoutVersion.Feature CreateOverwrite = new NameNodeLayoutVersion.Feature
				(-58, "Use single editlog record for " + "creating file with overwrite");

			public static readonly NameNodeLayoutVersion.Feature XattrsNamespaceExt = new NameNodeLayoutVersion.Feature
				(-59, "Increase number of xattr namespaces");

			public static readonly NameNodeLayoutVersion.Feature BlockStoragePolicy = new NameNodeLayoutVersion.Feature
				(-60, "Block Storage policy");

			public static readonly NameNodeLayoutVersion.Feature Truncate = new NameNodeLayoutVersion.Feature
				(-61, "Truncate");

			public static readonly NameNodeLayoutVersion.Feature AppendNewBlock = new NameNodeLayoutVersion.Feature
				(-62, "Support appending to new block");

			public static readonly NameNodeLayoutVersion.Feature QuotaByStorageType = new NameNodeLayoutVersion.Feature
				(-63, "Support quota for specific storage types");

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
			/// NameNode feature that is added at layout version
			/// <c>ancestoryLV</c>
			/// .
			/// </summary>
			/// <param name="lv">new layout version with the addition of this feature</param>
			/// <param name="ancestorLV">layout version from which the new lv is derived from.</param>
			/// <param name="description">description of the feature</param>
			/// <param name="reserved">
			/// true when this is a layout version reserved for previous
			/// versions
			/// </param>
			/// <param name="features">set of features that are to be enabled for this version</param>
			internal Feature(int lv, int ancestorLV, string description, bool reserved, params 
				NameNodeLayoutVersion.Feature[] features)
			{
				NameNodeLayoutVersion.Feature.info = new LayoutVersion.FeatureInfo(lv, ancestorLV
					, description, reserved, features);
			}

			public LayoutVersion.FeatureInfo GetInfo()
			{
				return NameNodeLayoutVersion.Feature.info;
			}
		}
	}
}
