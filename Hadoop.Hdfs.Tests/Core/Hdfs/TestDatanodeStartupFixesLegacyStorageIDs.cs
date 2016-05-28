using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// The test verifies that legacy storage IDs in older DataNode
	/// images are replaced with UUID-based storage IDs.
	/// </summary>
	/// <remarks>
	/// The test verifies that legacy storage IDs in older DataNode
	/// images are replaced with UUID-based storage IDs. The startup may
	/// or may not involve a Datanode Layout upgrade. Each test case uses
	/// the following resource files.
	/// 1. testCaseName.tgz - NN and DN directories corresponding
	/// to a specific layout version.
	/// 2. testCaseName.txt - Text file listing the checksum of each file
	/// in the cluster and overall checksum. See
	/// TestUpgradeFromImage for the file format.
	/// If any test case is renamed then the corresponding resource files must
	/// also be renamed.
	/// </remarks>
	public class TestDatanodeStartupFixesLegacyStorageIDs
	{
		/// <summary>
		/// Perform a upgrade using the test image corresponding to
		/// testCaseName.
		/// </summary>
		/// <param name="testCaseName"/>
		/// <param name="expectedStorageId">
		/// if null, then the upgrade generates a new
		/// unique storage ID.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		private static void RunLayoutUpgradeTest(string testCaseName, string expectedStorageId
			)
		{
			TestDFSUpgradeFromImage upgrade = new TestDFSUpgradeFromImage();
			upgrade.UnpackStorage(testCaseName + ".tgz", testCaseName + ".txt");
			Configuration conf = new Configuration(TestDFSUpgradeFromImage.upgradeConf);
			InitStorageDirs(conf, testCaseName);
			UpgradeAndVerify(upgrade, conf, new _ClusterVerifier_70(expectedStorageId));
		}

		private sealed class _ClusterVerifier_70 : TestDFSUpgradeFromImage.ClusterVerifier
		{
			public _ClusterVerifier_70(string expectedStorageId)
			{
				this.expectedStorageId = expectedStorageId;
			}

			/// <exception cref="System.IO.IOException"/>
			public void VerifyClusterPostUpgrade(MiniDFSCluster cluster)
			{
				// Verify that a GUID-based storage ID was generated.
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				StorageReport[] reports = cluster.GetDataNodes()[0].GetFSDataset().GetStorageReports
					(bpid);
				Assert.AssertThat(reports.Length, IS.Is(1));
				string storageID = reports[0].GetStorage().GetStorageID();
				NUnit.Framework.Assert.IsTrue(DatanodeStorage.IsValidStorageId(storageID));
				if (expectedStorageId != null)
				{
					Assert.AssertThat(storageID, IS.Is(expectedStorageId));
				}
			}

			private readonly string expectedStorageId;
		}

		private static void InitStorageDirs(Configuration conf, string testName)
		{
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, Runtime.GetProperty("test.build.data"
				) + FilePath.separator + testName + FilePath.separator + "dfs" + FilePath.separator
				 + "data");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Runtime.GetProperty("test.build.data"
				) + FilePath.separator + testName + FilePath.separator + "dfs" + FilePath.separator
				 + "name");
		}

		/// <exception cref="System.IO.IOException"/>
		private static void UpgradeAndVerify(TestDFSUpgradeFromImage upgrade, Configuration
			 conf, TestDFSUpgradeFromImage.ClusterVerifier verifier)
		{
			upgrade.UpgradeAndVerify(new MiniDFSCluster.Builder(conf).NumDataNodes(1).ManageDataDfsDirs
				(false).ManageNameDfsDirs(false), verifier);
		}

		/// <summary>
		/// Upgrade from 2.2 (no storage IDs per volume) correctly generates
		/// GUID-based storage IDs.
		/// </summary>
		/// <remarks>
		/// Upgrade from 2.2 (no storage IDs per volume) correctly generates
		/// GUID-based storage IDs. Test case for HDFS-7575.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUpgradeFrom22FixesStorageIDs()
		{
			RunLayoutUpgradeTest(GenericTestUtils.GetMethodName(), null);
		}

		/// <summary>
		/// Startup from a 2.6-layout that has legacy storage IDs correctly
		/// generates new storage IDs.
		/// </summary>
		/// <remarks>
		/// Startup from a 2.6-layout that has legacy storage IDs correctly
		/// generates new storage IDs.
		/// Test case for HDFS-7575.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUpgradeFrom22via26FixesStorageIDs()
		{
			RunLayoutUpgradeTest(GenericTestUtils.GetMethodName(), null);
		}

		/// <summary>
		/// Startup from a 2.6-layout that already has unique storage IDs does
		/// not regenerate the storage IDs.
		/// </summary>
		/// <remarks>
		/// Startup from a 2.6-layout that already has unique storage IDs does
		/// not regenerate the storage IDs.
		/// Test case for HDFS-7575.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUpgradeFrom26PreservesStorageIDs()
		{
			// StorageId present in the image testUpgradeFrom26PreservesStorageId.tgz
			RunLayoutUpgradeTest(GenericTestUtils.GetMethodName(), "DS-a0e39cfa-930f-4abd-813c-e22b59223774"
				);
		}
	}
}
