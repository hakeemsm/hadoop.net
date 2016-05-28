using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class DataNodeLayoutVersion
	{
		/// <summary>Build layout version and corresponding feature matrix</summary>
		public static readonly IDictionary<int, ICollection<LayoutVersion.LayoutFeature>>
			 Features = new Dictionary<int, ICollection<LayoutVersion.LayoutFeature>>();

		public static readonly int CurrentLayoutVersion = LayoutVersion.GetCurrentLayoutVersion
			(DataNodeLayoutVersion.Feature.Values());

		static DataNodeLayoutVersion()
		{
			LayoutVersion.UpdateMap(Features, LayoutVersion.Feature.Values());
			LayoutVersion.UpdateMap(Features, DataNodeLayoutVersion.Feature.Values());
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
			public static readonly DataNodeLayoutVersion.Feature FirstLayout = new DataNodeLayoutVersion.Feature
				(-55, -53, "First datanode layout", false);

			public static readonly DataNodeLayoutVersion.Feature BlockidBasedLayout = new DataNodeLayoutVersion.Feature
				(-56, "The block ID of a finalized block uniquely determines its position " + "in the directory structure"
				);

			private readonly LayoutVersion.FeatureInfo info;

			/// <summary>
			/// DataNodeFeature that is added at layout version
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
			/// DataNode feature that is added at layout version
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
				DataNodeLayoutVersion.Feature[] features)
			{
				DataNodeLayoutVersion.Feature.info = new LayoutVersion.FeatureInfo(lv, ancestorLV
					, description, reserved, features);
			}

			public LayoutVersion.FeatureInfo GetInfo()
			{
				return DataNodeLayoutVersion.Feature.info;
			}
		}
	}
}
