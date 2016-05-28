using System.Collections.Generic;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// This class tests various upgrade cases from earlier versions to current
	/// version with and without clusterid.
	/// </summary>
	public class TestStartupOptionUpgrade
	{
		private Configuration conf;

		private HdfsServerConstants.StartupOption startOpt;

		private int layoutVersion;

		internal NNStorage storage;

		[Parameterized.Parameters]
		public static ICollection<object[]> StartOption()
		{
			object[][] @params = new object[][] { new object[] { HdfsServerConstants.StartupOption
				.Upgrade }, new object[] { HdfsServerConstants.StartupOption.Upgradeonly } };
			return Arrays.AsList(@params);
		}

		public TestStartupOptionUpgrade(HdfsServerConstants.StartupOption startOption)
			: base()
		{
			this.startOpt = startOption;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new HdfsConfiguration();
			startOpt.SetClusterId(null);
			storage = new NNStorage(conf, Sharpen.Collections.EmptyList<URI>(), Sharpen.Collections
				.EmptyList<URI>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			conf = null;
			startOpt = null;
		}

		/// <summary>
		/// Tests the upgrade from version 0.20.204 to Federation version Test without
		/// clusterid the case: -upgrade
		/// Expected to generate clusterid
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartupOptUpgradeFrom204()
		{
			layoutVersion = LayoutVersion.Feature.ReservedRel20204.GetInfo().GetLayoutVersion
				();
			storage.ProcessStartupOptionsForUpgrade(startOpt, layoutVersion);
			NUnit.Framework.Assert.IsTrue("Clusterid should start with CID", storage.GetClusterID
				().StartsWith("CID"));
		}

		/// <summary>
		/// Tests the upgrade from version 0.22 to Federation version Test with
		/// clusterid case: -upgrade -clusterid <cid>
		/// Expected to reuse user given clusterid
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartupOptUpgradeFrom22WithCID()
		{
			startOpt.SetClusterId("cid");
			layoutVersion = LayoutVersion.Feature.ReservedRel22.GetInfo().GetLayoutVersion();
			storage.ProcessStartupOptionsForUpgrade(startOpt, layoutVersion);
			NUnit.Framework.Assert.AreEqual("Clusterid should match with the given clusterid"
				, "cid", storage.GetClusterID());
		}

		/// <summary>
		/// Tests the upgrade from one version of Federation to another Federation
		/// version Test without clusterid case: -upgrade
		/// Expected to reuse existing clusterid
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartupOptUpgradeFromFederation()
		{
			// Test assumes clusterid already exists, set the clusterid
			storage.SetClusterID("currentcid");
			layoutVersion = LayoutVersion.Feature.Federation.GetInfo().GetLayoutVersion();
			storage.ProcessStartupOptionsForUpgrade(startOpt, layoutVersion);
			NUnit.Framework.Assert.AreEqual("Clusterid should match with the existing one", "currentcid"
				, storage.GetClusterID());
		}

		/// <summary>
		/// Tests the upgrade from one version of Federation to another Federation
		/// version Test with wrong clusterid case: -upgrade -clusterid <cid>
		/// Expected to reuse existing clusterid and ignore user given clusterid
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartupOptUpgradeFromFederationWithWrongCID()
		{
			startOpt.SetClusterId("wrong-cid");
			storage.SetClusterID("currentcid");
			layoutVersion = LayoutVersion.Feature.Federation.GetInfo().GetLayoutVersion();
			storage.ProcessStartupOptionsForUpgrade(startOpt, layoutVersion);
			NUnit.Framework.Assert.AreEqual("Clusterid should match with the existing one", "currentcid"
				, storage.GetClusterID());
		}

		/// <summary>
		/// Tests the upgrade from one version of Federation to another Federation
		/// version Test with correct clusterid case: -upgrade -clusterid <cid>
		/// Expected to reuse existing clusterid and ignore user given clusterid
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartupOptUpgradeFromFederationWithCID()
		{
			startOpt.SetClusterId("currentcid");
			storage.SetClusterID("currentcid");
			layoutVersion = LayoutVersion.Feature.Federation.GetInfo().GetLayoutVersion();
			storage.ProcessStartupOptionsForUpgrade(startOpt, layoutVersion);
			NUnit.Framework.Assert.AreEqual("Clusterid should match with the existing one", "currentcid"
				, storage.GetClusterID());
		}
	}
}
