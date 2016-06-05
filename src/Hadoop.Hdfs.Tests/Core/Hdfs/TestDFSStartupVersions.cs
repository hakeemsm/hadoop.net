using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This test ensures the appropriate response (successful or failure) from
	/// a Datanode when the system is started with differing version combinations.
	/// </summary>
	public class TestDFSStartupVersions
	{
		private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestDFSStartupVersions"
			);

		private MiniDFSCluster cluster = null;

		/// <summary>Writes an INFO log message containing the parameters.</summary>
		internal virtual void Log(string label, HdfsServerConstants.NodeType nodeType, int
			 testCase, TestDFSStartupVersions.StorageData sd)
		{
			string testCaseLine = string.Empty;
			if (testCase != null)
			{
				testCaseLine = " testCase=" + testCase;
			}
			Log.Info("============================================================");
			Log.Info("***TEST*** " + label + ":" + testCaseLine + " nodeType=" + nodeType + " layoutVersion="
				 + sd.storageInfo.GetLayoutVersion() + " namespaceID=" + sd.storageInfo.GetNamespaceID
				() + " fsscTime=" + sd.storageInfo.GetCTime() + " clusterID=" + sd.storageInfo.GetClusterID
				() + " BlockPoolID=" + sd.blockPoolId);
		}

		/// <summary>Class used for initializing version information for tests</summary>
		private class StorageData
		{
			private readonly StorageInfo storageInfo;

			private readonly string blockPoolId;

			internal StorageData(int layoutVersion, int namespaceId, string clusterId, long cTime
				, string bpid)
			{
				storageInfo = new StorageInfo(layoutVersion, namespaceId, clusterId, cTime, HdfsServerConstants.NodeType
					.DataNode);
				blockPoolId = bpid;
			}
		}

		/// <summary>Initialize the versions array.</summary>
		/// <remarks>
		/// Initialize the versions array.  This array stores all combinations
		/// of cross product:
		/// {oldLayoutVersion,currentLayoutVersion,futureLayoutVersion} X
		/// {currentNamespaceId,incorrectNamespaceId} X
		/// {pastFsscTime,currentFsscTime,futureFsscTime}
		/// </remarks>
		/// <exception cref="System.Exception"/>
		private TestDFSStartupVersions.StorageData[] InitializeVersions()
		{
			int layoutVersionOld = Storage.LastUpgradableLayoutVersion;
			int layoutVersionCur = HdfsConstants.DatanodeLayoutVersion;
			int layoutVersionNew = int.MinValue;
			int namespaceIdCur = UpgradeUtilities.GetCurrentNamespaceID(null);
			int namespaceIdOld = int.MinValue;
			long fsscTimeOld = long.MinValue;
			long fsscTimeCur = UpgradeUtilities.GetCurrentFsscTime(null);
			long fsscTimeNew = long.MaxValue;
			string clusterID = "testClusterID";
			string invalidClusterID = "testClusterID";
			string bpid = UpgradeUtilities.GetCurrentBlockPoolID(null);
			string invalidBpid = "invalidBpid";
			return new TestDFSStartupVersions.StorageData[] { new TestDFSStartupVersions.StorageData
				(layoutVersionOld, namespaceIdCur, clusterID, fsscTimeOld, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionOld, namespaceIdCur, clusterID, fsscTimeCur, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionOld, namespaceIdCur, clusterID, fsscTimeNew, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionOld, namespaceIdOld, clusterID, fsscTimeOld, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionOld, namespaceIdOld, clusterID, fsscTimeCur, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionOld, namespaceIdOld, clusterID, fsscTimeNew, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionCur, namespaceIdCur, clusterID, fsscTimeOld, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionCur, namespaceIdCur, clusterID, fsscTimeCur, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionCur, namespaceIdCur, clusterID, fsscTimeNew, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionCur, namespaceIdOld, clusterID, fsscTimeOld, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionCur, namespaceIdOld, clusterID, fsscTimeCur, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionCur, namespaceIdOld, clusterID, fsscTimeNew, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionNew, namespaceIdCur, clusterID, fsscTimeOld, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionNew, namespaceIdCur, clusterID, fsscTimeCur, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionNew, namespaceIdCur, clusterID, fsscTimeNew, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionNew, namespaceIdOld, clusterID, fsscTimeOld, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionNew, namespaceIdOld, clusterID, fsscTimeCur, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionNew, namespaceIdOld, clusterID, fsscTimeNew, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionCur, namespaceIdCur, invalidClusterID, fsscTimeCur, bpid), new TestDFSStartupVersions.StorageData
				(layoutVersionCur, namespaceIdCur, clusterID, fsscTimeCur, invalidBpid) };
		}

		// 0
		// 1
		// 2
		// 3
		// 4
		// 5
		// 6
		// 7
		// 8
		// 9
		// 10
		// 11
		// 12
		// 13
		// 14
		// 15
		// 16
		// 17
		// Test with invalid clusterId
		// 18
		// Test with invalid block pool Id
		// 19
		/// <summary>
		/// Determines if the given Namenode version and Datanode version
		/// are compatible with each other.
		/// </summary>
		/// <remarks>
		/// Determines if the given Namenode version and Datanode version
		/// are compatible with each other. Compatibility in this case mean
		/// that the Namenode and Datanode will successfully start up and
		/// will work together. The rules for compatibility,
		/// taken from the DFS Upgrade Design, are as follows:
		/// <pre>
		/// <ol>
		/// <li>Check 0: Datanode namespaceID != Namenode namespaceID the startup fails
		/// </li>
		/// <li>Check 1: Datanode clusterID != Namenode clusterID the startup fails
		/// </li>
		/// <li>Check 2: Datanode blockPoolID != Namenode blockPoolID the startup fails
		/// </li>
		/// <li>Check 3: The data-node does regular startup (no matter which options
		/// it is started with) if
		/// softwareLV == storedLV AND
		/// DataNode.FSSCTime == NameNode.FSSCTime
		/// </li>
		/// <li>Check 4: The data-node performs an upgrade if it is started without any
		/// options and
		/// |softwareLV| &gt; |storedLV| OR
		/// (softwareLV == storedLV AND
		/// DataNode.FSSCTime &lt; NameNode.FSSCTime)
		/// </li>
		/// <li>NOT TESTED: The data-node rolls back if it is started with
		/// the -rollback option and
		/// |softwareLV| &gt;= |previous.storedLV| AND
		/// DataNode.previous.FSSCTime &lt;= NameNode.FSSCTime
		/// </li>
		/// <li>Check 5: In all other cases the startup fails.</li>
		/// </ol>
		/// </pre>
		/// </remarks>
		internal virtual bool IsVersionCompatible(TestDFSStartupVersions.StorageData namenodeSd
			, TestDFSStartupVersions.StorageData datanodeSd)
		{
			StorageInfo namenodeVer = namenodeSd.storageInfo;
			StorageInfo datanodeVer = datanodeSd.storageInfo;
			// check #0
			if (namenodeVer.GetNamespaceID() != datanodeVer.GetNamespaceID())
			{
				Log.Info("namespaceIDs are not equal: isVersionCompatible=false");
				return false;
			}
			// check #1
			if (!namenodeVer.GetClusterID().Equals(datanodeVer.GetClusterID()))
			{
				Log.Info("clusterIDs are not equal: isVersionCompatible=false");
				return false;
			}
			// check #2
			if (!namenodeSd.blockPoolId.Equals(datanodeSd.blockPoolId))
			{
				Log.Info("blockPoolIDs are not equal: isVersionCompatible=false");
				return false;
			}
			// check #3
			int softwareLV = HdfsConstants.DatanodeLayoutVersion;
			int storedLV = datanodeVer.GetLayoutVersion();
			if (softwareLV == storedLV && datanodeVer.GetCTime() == namenodeVer.GetCTime())
			{
				Log.Info("layoutVersions and cTimes are equal: isVersionCompatible=true");
				return true;
			}
			// check #4
			long absSoftwareLV = Math.Abs((long)softwareLV);
			long absStoredLV = Math.Abs((long)storedLV);
			if (absSoftwareLV > absStoredLV || (softwareLV == storedLV && datanodeVer.GetCTime
				() < namenodeVer.GetCTime()))
			{
				Log.Info("softwareLayoutVersion is newer OR namenode cTime is newer: isVersionCompatible=true"
					);
				return true;
			}
			// check #5
			Log.Info("default case: isVersionCompatible=false");
			return false;
		}

		/// <summary>
		/// This test ensures the appropriate response (successful or failure) from
		/// a Datanode when the system is started with differing version combinations.
		/// </summary>
		/// <remarks>
		/// This test ensures the appropriate response (successful or failure) from
		/// a Datanode when the system is started with differing version combinations.
		/// <pre>
		/// For each 3-tuple in the cross product
		/// ({oldLayoutVersion,currentLayoutVersion,futureLayoutVersion},
		/// {currentNamespaceId,incorrectNamespaceId},
		/// {pastFsscTime,currentFsscTime,futureFsscTime})
		/// 1. Startup Namenode with version file containing
		/// (currentLayoutVersion,currentNamespaceId,currentFsscTime)
		/// 2. Attempt to startup Datanode with version file containing
		/// this iterations version 3-tuple
		/// </pre>
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestVersions()
		{
			UpgradeUtilities.Initialize();
			Configuration conf = UpgradeUtilities.InitializeStorageStateConf(1, new HdfsConfiguration
				());
			TestDFSStartupVersions.StorageData[] versions = InitializeVersions();
			UpgradeUtilities.CreateNameNodeStorageDirs(conf.GetStrings(DFSConfigKeys.DfsNamenodeNameDirKey
				), "current");
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).ManageDataDfsDirs
				(false).ManageNameDfsDirs(false).StartupOption(HdfsServerConstants.StartupOption
				.Regular).Build();
			TestDFSStartupVersions.StorageData nameNodeVersion = new TestDFSStartupVersions.StorageData
				(HdfsConstants.NamenodeLayoutVersion, UpgradeUtilities.GetCurrentNamespaceID(cluster
				), UpgradeUtilities.GetCurrentClusterID(cluster), UpgradeUtilities.GetCurrentFsscTime
				(cluster), UpgradeUtilities.GetCurrentBlockPoolID(cluster));
			Log("NameNode version info", HdfsServerConstants.NodeType.NameNode, null, nameNodeVersion
				);
			string bpid = UpgradeUtilities.GetCurrentBlockPoolID(cluster);
			for (int i = 0; i < versions.Length; i++)
			{
				FilePath[] storage = UpgradeUtilities.CreateDataNodeStorageDirs(conf.GetStrings(DFSConfigKeys
					.DfsDatanodeDataDirKey), "current");
				Log("DataNode version info", HdfsServerConstants.NodeType.DataNode, i, versions[i
					]);
				UpgradeUtilities.CreateDataNodeVersionFile(storage, versions[i].storageInfo, bpid
					, versions[i].blockPoolId);
				try
				{
					cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Regular, 
						null);
				}
				catch (Exception)
				{
				}
				// Ignore.  The asserts below will check for problems.
				// ignore.printStackTrace();
				NUnit.Framework.Assert.IsTrue(cluster.GetNameNode() != null);
				NUnit.Framework.Assert.AreEqual(IsVersionCompatible(nameNodeVersion, versions[i])
					, cluster.IsDataNodeUp());
				cluster.ShutdownDataNodes();
			}
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDown()
		{
			Log.Info("Shutting down MiniDFSCluster");
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new TestDFSStartupVersions().TestVersions();
		}
	}
}
