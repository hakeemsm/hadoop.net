using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// Test for
	/// <see cref="LayoutVersion"/>
	/// </summary>
	public class TestLayoutVersion
	{
		public static readonly LayoutVersion.LayoutFeature LastNonReservedCommonFeature;

		public static readonly LayoutVersion.LayoutFeature LastCommonFeature;

		static TestLayoutVersion()
		{
			LayoutVersion.Feature[] features = LayoutVersion.Feature.Values();
			LastCommonFeature = features[features.Length - 1];
			LastNonReservedCommonFeature = LayoutVersion.GetLastNonReservedFeature(features);
		}

		/// <summary>
		/// Tests to make sure a given layout version supports all the
		/// features from the ancestor
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestFeaturesFromAncestorSupported()
		{
			foreach (LayoutVersion.LayoutFeature f in LayoutVersion.Feature.Values())
			{
				ValidateFeatureList(f);
			}
		}

		/// <summary>Test to make sure 0.20.203 supports delegation token</summary>
		[NUnit.Framework.Test]
		public virtual void TestRelease203()
		{
			NUnit.Framework.Assert.IsTrue(NameNodeLayoutVersion.Supports(LayoutVersion.Feature
				.DelegationToken, LayoutVersion.Feature.ReservedRel20203.GetInfo().GetLayoutVersion
				()));
		}

		/// <summary>Test to make sure 0.20.204 supports delegation token</summary>
		[NUnit.Framework.Test]
		public virtual void TestRelease204()
		{
			NUnit.Framework.Assert.IsTrue(NameNodeLayoutVersion.Supports(LayoutVersion.Feature
				.DelegationToken, LayoutVersion.Feature.ReservedRel20204.GetInfo().GetLayoutVersion
				()));
		}

		/// <summary>Test to make sure release 1.2.0 support CONCAT</summary>
		[NUnit.Framework.Test]
		public virtual void TestRelease1_2_0()
		{
			NUnit.Framework.Assert.IsTrue(NameNodeLayoutVersion.Supports(LayoutVersion.Feature
				.Concat, LayoutVersion.Feature.ReservedRel120.GetInfo().GetLayoutVersion()));
		}

		/// <summary>Test to make sure NameNode.Feature support previous features</summary>
		[NUnit.Framework.Test]
		public virtual void TestNameNodeFeature()
		{
			LayoutVersion.LayoutFeature first = NameNodeLayoutVersion.Feature.RollingUpgrade;
			NUnit.Framework.Assert.IsTrue(NameNodeLayoutVersion.Supports(LastNonReservedCommonFeature
				, first.GetInfo().GetLayoutVersion()));
			NUnit.Framework.Assert.AreEqual(LastCommonFeature.GetInfo().GetLayoutVersion() - 
				1, first.GetInfo().GetLayoutVersion());
		}

		/// <summary>Test to make sure DataNode.Feature support previous features</summary>
		[NUnit.Framework.Test]
		public virtual void TestDataNodeFeature()
		{
			LayoutVersion.LayoutFeature first = DataNodeLayoutVersion.Feature.FirstLayout;
			NUnit.Framework.Assert.IsTrue(DataNodeLayoutVersion.Supports(LastNonReservedCommonFeature
				, first.GetInfo().GetLayoutVersion()));
			NUnit.Framework.Assert.AreEqual(LastCommonFeature.GetInfo().GetLayoutVersion() - 
				1, first.GetInfo().GetLayoutVersion());
		}

		/// <summary>
		/// Given feature
		/// <paramref name="f"/>
		/// , ensures the layout version of that feature
		/// supports all the features supported by it's ancestor.
		/// </summary>
		private void ValidateFeatureList(LayoutVersion.LayoutFeature f)
		{
			LayoutVersion.FeatureInfo info = f.GetInfo();
			int lv = info.GetLayoutVersion();
			int ancestorLV = info.GetAncestorLayoutVersion();
			ICollection<LayoutVersion.LayoutFeature> ancestorSet = NameNodeLayoutVersion.GetFeatures
				(ancestorLV);
			NUnit.Framework.Assert.IsNotNull(ancestorSet);
			foreach (LayoutVersion.LayoutFeature feature in ancestorSet)
			{
				NUnit.Framework.Assert.IsTrue("LV " + lv + " does nto support " + feature + " supported by the ancestor LV "
					 + info.GetAncestorLayoutVersion(), NameNodeLayoutVersion.Supports(feature, lv));
			}
		}

		/// <summary>
		/// When a LayoutVersion support SNAPSHOT, it must support
		/// FSIMAGE_NAME_OPTIMIZATION.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestSNAPSHOT()
		{
			foreach (LayoutVersion.Feature f in LayoutVersion.Feature.Values())
			{
				int version = f.GetInfo().GetLayoutVersion();
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Snapshot, version))
				{
					NUnit.Framework.Assert.IsTrue(NameNodeLayoutVersion.Supports(LayoutVersion.Feature
						.FsimageNameOptimization, version));
				}
			}
		}
	}
}
