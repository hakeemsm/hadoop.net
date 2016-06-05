using System;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This test ensures the appropriate response (successful or failure) from
	/// the system when the system is upgraded under various storage state and
	/// version conditions.
	/// </summary>
	public class TestDFSUpgrade
	{
		private const int ExpectedTxid = 61;

		private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog(typeof(
			TestDFSUpgrade).FullName);

		private Configuration conf;

		private int testCounter = 0;

		private MiniDFSCluster cluster = null;

		// TODO: Avoid hard-coding expected_txid. The test should be more robust.
		/// <summary>Writes an INFO log message containing the parameters.</summary>
		internal virtual void Log(string label, int numDirs)
		{
			Log.Info("============================================================");
			Log.Info("***TEST " + (testCounter++) + "*** " + label + ":" + " numDirs=" + numDirs
				);
		}

		/// <summary>For namenode, Verify that the current and previous directories exist.</summary>
		/// <remarks>
		/// For namenode, Verify that the current and previous directories exist.
		/// Verify that previous hasn't been modified by comparing the checksum of all
		/// its files with their original checksum. It is assumed that the
		/// server has recovered and upgraded.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckNameNode(string[] baseDirs, long imageTxId)
		{
			foreach (string baseDir in baseDirs)
			{
				Log.Info("Checking namenode directory " + baseDir);
				Log.Info("==== Contents ====:\n  " + Joiner.On("  \n").Join(new FilePath(baseDir, 
					"current").List()));
				Log.Info("==================");
				GenericTestUtils.AssertExists(new FilePath(baseDir, "current"));
				GenericTestUtils.AssertExists(new FilePath(baseDir, "current/VERSION"));
				GenericTestUtils.AssertExists(new FilePath(baseDir, "current/" + NNStorage.GetInProgressEditsFileName
					(imageTxId + 1)));
				GenericTestUtils.AssertExists(new FilePath(baseDir, "current/" + NNStorage.GetImageFileName
					(imageTxId)));
				GenericTestUtils.AssertExists(new FilePath(baseDir, "current/seen_txid"));
				FilePath previous = new FilePath(baseDir, "previous");
				GenericTestUtils.AssertExists(previous);
				NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
					.NameNode, previous, false), UpgradeUtilities.ChecksumMasterNameNodeContents());
			}
		}

		/// <summary>
		/// For datanode, for a block pool, verify that the current and previous
		/// directories exist.
		/// </summary>
		/// <remarks>
		/// For datanode, for a block pool, verify that the current and previous
		/// directories exist. Verify that previous hasn't been modified by comparing
		/// the checksum of all its files with their original checksum. It
		/// is assumed that the server has recovered and upgraded.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckDataNode(string[] baseDirs, string bpid)
		{
			for (int i = 0; i < baseDirs.Length; i++)
			{
				FilePath current = new FilePath(baseDirs[i], "current/" + bpid + "/current");
				NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
					.DataNode, current, false), UpgradeUtilities.ChecksumMasterDataNodeContents());
				// block files are placed under <sd>/current/<bpid>/current/finalized
				FilePath currentFinalized = MiniDFSCluster.GetFinalizedDir(new FilePath(baseDirs[
					i]), bpid);
				NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
					.DataNode, currentFinalized, true), UpgradeUtilities.ChecksumMasterBlockPoolFinalizedContents
					());
				FilePath previous = new FilePath(baseDirs[i], "current/" + bpid + "/previous");
				NUnit.Framework.Assert.IsTrue(previous.IsDirectory());
				NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
					.DataNode, previous, false), UpgradeUtilities.ChecksumMasterDataNodeContents());
				FilePath previousFinalized = new FilePath(baseDirs[i], "current/" + bpid + "/previous"
					 + "/finalized");
				NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
					.DataNode, previousFinalized, true), UpgradeUtilities.ChecksumMasterBlockPoolFinalizedContents
					());
			}
		}

		/// <summary>Attempts to start a NameNode with the given operation.</summary>
		/// <remarks>
		/// Attempts to start a NameNode with the given operation.  Starting
		/// the NameNode should throw an exception.
		/// </remarks>
		internal virtual void StartNameNodeShouldFail(HdfsServerConstants.StartupOption operation
			)
		{
			StartNameNodeShouldFail(operation, null, null);
		}

		/// <summary>Attempts to start a NameNode with the given operation.</summary>
		/// <remarks>
		/// Attempts to start a NameNode with the given operation.  Starting
		/// the NameNode should throw an exception.
		/// </remarks>
		/// <param name="operation">- NameNode startup operation</param>
		/// <param name="exceptionClass">
		/// - if non-null, will check that the caught exception
		/// is assignment-compatible with exceptionClass
		/// </param>
		/// <param name="messagePattern">
		/// - if non-null, will check that a substring of the
		/// message from the caught exception matches this pattern, via the
		/// <see cref="Matcher#find()"/>
		/// method.
		/// </param>
		internal virtual void StartNameNodeShouldFail(HdfsServerConstants.StartupOption operation
			, Type exceptionClass, Sharpen.Pattern messagePattern)
		{
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).StartupOption(operation
					).Format(false).ManageDataDfsDirs(false).ManageNameDfsDirs(false).Build();
				// should fail
				NUnit.Framework.Assert.Fail("NameNode should have failed to start");
			}
			catch (Exception e)
			{
				// expect exception
				if (exceptionClass != null)
				{
					NUnit.Framework.Assert.IsTrue("Caught exception is not of expected class " + exceptionClass
						.Name + ": " + StringUtils.StringifyException(e), exceptionClass.IsInstanceOfType
						(e));
				}
				if (messagePattern != null)
				{
					NUnit.Framework.Assert.IsTrue("Caught exception message string does not match expected pattern \""
						 + messagePattern.Pattern() + "\" : " + StringUtils.StringifyException(e), messagePattern
						.Matcher(e.Message).Find());
				}
				Log.Info("Successfully detected expected NameNode startup failure.");
			}
		}

		/// <summary>Attempts to start a DataNode with the given operation.</summary>
		/// <remarks>
		/// Attempts to start a DataNode with the given operation. Starting
		/// the given block pool should fail.
		/// </remarks>
		/// <param name="operation">startup option</param>
		/// <param name="bpid">block pool Id that should fail to start</param>
		/// <exception cref="System.IO.IOException"></exception>
		internal virtual void StartBlockPoolShouldFail(HdfsServerConstants.StartupOption 
			operation, string bpid)
		{
			cluster.StartDataNodes(conf, 1, false, operation, null);
			// should fail
			NUnit.Framework.Assert.IsFalse("Block pool " + bpid + " should have failed to start"
				, cluster.GetDataNodes()[0].IsBPServiceAlive(bpid));
		}

		/// <summary>
		/// Create an instance of a newly configured cluster for testing that does
		/// not manage its own directories or files
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private MiniDFSCluster CreateCluster()
		{
			return new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).ManageDataDfsDirs
				(false).ManageNameDfsDirs(false).StartupOption(HdfsServerConstants.StartupOption
				.Upgrade).Build();
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Initialize()
		{
			UpgradeUtilities.Initialize();
		}

		/// <summary>
		/// This test attempts to upgrade the NameNode and DataNode under
		/// a number of valid and invalid conditions.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestUpgrade()
		{
			FilePath[] baseDirs;
			StorageInfo storageInfo = null;
			for (int numDirs = 1; numDirs <= 2; numDirs++)
			{
				conf = new HdfsConfiguration();
				conf = UpgradeUtilities.InitializeStorageStateConf(numDirs, conf);
				string[] nameNodeDirs = conf.GetStrings(DFSConfigKeys.DfsNamenodeNameDirKey);
				string[] dataNodeDirs = conf.GetStrings(DFSConfigKeys.DfsDatanodeDataDirKey);
				conf.SetBoolean(DFSConfigKeys.DfsDatanodeDuplicateReplicaDeletion, false);
				Log("Normal NameNode upgrade", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				cluster = CreateCluster();
				// make sure that rolling upgrade cannot be started
				try
				{
					DistributedFileSystem dfs = cluster.GetFileSystem();
					dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
					dfs.RollingUpgrade(HdfsConstants.RollingUpgradeAction.Prepare);
					NUnit.Framework.Assert.Fail();
				}
				catch (RemoteException re)
				{
					NUnit.Framework.Assert.AreEqual(typeof(InconsistentFSStateException).FullName, re
						.GetClassName());
					Log.Info("The exception is expected.", re);
				}
				CheckNameNode(nameNodeDirs, ExpectedTxid);
				if (numDirs > 1)
				{
					TestParallelImageWrite.CheckImages(cluster.GetNamesystem(), numDirs);
				}
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("Normal DataNode upgrade", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				cluster = CreateCluster();
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Regular, 
					null);
				CheckDataNode(dataNodeDirs, UpgradeUtilities.GetCurrentBlockPoolID(null));
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
				Log("NameNode upgrade with existing previous dir", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				StartNameNodeShouldFail(HdfsServerConstants.StartupOption.Upgrade);
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("DataNode upgrade with existing previous dir", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				cluster = CreateCluster();
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "previous");
				cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Regular, 
					null);
				CheckDataNode(dataNodeDirs, UpgradeUtilities.GetCurrentBlockPoolID(null));
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
				Log("DataNode upgrade with future stored layout version in current", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				cluster = CreateCluster();
				baseDirs = UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				storageInfo = new StorageInfo(int.MinValue, UpgradeUtilities.GetCurrentNamespaceID
					(cluster), UpgradeUtilities.GetCurrentClusterID(cluster), UpgradeUtilities.GetCurrentFsscTime
					(cluster), HdfsServerConstants.NodeType.DataNode);
				UpgradeUtilities.CreateDataNodeVersionFile(baseDirs, storageInfo, UpgradeUtilities
					.GetCurrentBlockPoolID(cluster));
				StartBlockPoolShouldFail(HdfsServerConstants.StartupOption.Regular, UpgradeUtilities
					.GetCurrentBlockPoolID(null));
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
				Log("DataNode upgrade with newer fsscTime in current", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				cluster = CreateCluster();
				baseDirs = UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				storageInfo = new StorageInfo(HdfsConstants.DatanodeLayoutVersion, UpgradeUtilities
					.GetCurrentNamespaceID(cluster), UpgradeUtilities.GetCurrentClusterID(cluster), 
					long.MaxValue, HdfsServerConstants.NodeType.DataNode);
				UpgradeUtilities.CreateDataNodeVersionFile(baseDirs, storageInfo, UpgradeUtilities
					.GetCurrentBlockPoolID(cluster));
				// Ensure corresponding block pool failed to initialized
				StartBlockPoolShouldFail(HdfsServerConstants.StartupOption.Regular, UpgradeUtilities
					.GetCurrentBlockPoolID(null));
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
				Log("NameNode upgrade with no edits file", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				DeleteStorageFilesWithPrefix(nameNodeDirs, "edits_");
				StartNameNodeShouldFail(HdfsServerConstants.StartupOption.Upgrade);
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("NameNode upgrade with no image file", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				DeleteStorageFilesWithPrefix(nameNodeDirs, "fsimage_");
				StartNameNodeShouldFail(HdfsServerConstants.StartupOption.Upgrade);
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("NameNode upgrade with corrupt version file", numDirs);
				baseDirs = UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				foreach (FilePath f in baseDirs)
				{
					UpgradeUtilities.CorruptFile(new FilePath(f, "VERSION"), Sharpen.Runtime.GetBytesForString
						("layoutVersion", Charsets.Utf8), Sharpen.Runtime.GetBytesForString("xxxxxxxxxxxxx"
						, Charsets.Utf8));
				}
				StartNameNodeShouldFail(HdfsServerConstants.StartupOption.Upgrade);
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("NameNode upgrade with old layout version in current", numDirs);
				baseDirs = UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				storageInfo = new StorageInfo(Storage.LastUpgradableLayoutVersion + 1, UpgradeUtilities
					.GetCurrentNamespaceID(null), UpgradeUtilities.GetCurrentClusterID(null), UpgradeUtilities
					.GetCurrentFsscTime(null), HdfsServerConstants.NodeType.NameNode);
				UpgradeUtilities.CreateNameNodeVersionFile(conf, baseDirs, storageInfo, UpgradeUtilities
					.GetCurrentBlockPoolID(cluster));
				StartNameNodeShouldFail(HdfsServerConstants.StartupOption.Upgrade);
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("NameNode upgrade with future layout version in current", numDirs);
				baseDirs = UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				storageInfo = new StorageInfo(int.MinValue, UpgradeUtilities.GetCurrentNamespaceID
					(null), UpgradeUtilities.GetCurrentClusterID(null), UpgradeUtilities.GetCurrentFsscTime
					(null), HdfsServerConstants.NodeType.NameNode);
				UpgradeUtilities.CreateNameNodeVersionFile(conf, baseDirs, storageInfo, UpgradeUtilities
					.GetCurrentBlockPoolID(cluster));
				StartNameNodeShouldFail(HdfsServerConstants.StartupOption.Upgrade);
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
			}
			// end numDir loop
			// One more check: normal NN upgrade with 4 directories, concurrent write
			int numDirs_1 = 4;
			{
				conf = new HdfsConfiguration();
				conf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
				conf.SetBoolean(DFSConfigKeys.DfsDatanodeDuplicateReplicaDeletion, false);
				conf = UpgradeUtilities.InitializeStorageStateConf(numDirs_1, conf);
				string[] nameNodeDirs = conf.GetStrings(DFSConfigKeys.DfsNamenodeNameDirKey);
				Log("Normal NameNode upgrade", numDirs_1);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				cluster = CreateCluster();
				// make sure that rolling upgrade cannot be started
				try
				{
					DistributedFileSystem dfs = cluster.GetFileSystem();
					dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
					dfs.RollingUpgrade(HdfsConstants.RollingUpgradeAction.Prepare);
					NUnit.Framework.Assert.Fail();
				}
				catch (RemoteException re)
				{
					NUnit.Framework.Assert.AreEqual(typeof(InconsistentFSStateException).FullName, re
						.GetClassName());
					Log.Info("The exception is expected.", re);
				}
				CheckNameNode(nameNodeDirs, ExpectedTxid);
				TestParallelImageWrite.CheckImages(cluster.GetNamesystem(), numDirs_1);
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
			}
		}

		/*
		* Stand-alone test to detect failure of one SD during parallel upgrade.
		* At this time, can only be done with manual hack of {@link FSImage.doUpgrade()}
		*/
		/// <exception cref="System.Exception"/>
		[Ignore]
		public virtual void TestUpgrade4()
		{
			int numDirs = 4;
			conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
			conf.SetBoolean(DFSConfigKeys.DfsDatanodeDuplicateReplicaDeletion, false);
			conf = UpgradeUtilities.InitializeStorageStateConf(numDirs, conf);
			string[] nameNodeDirs = conf.GetStrings(DFSConfigKeys.DfsNamenodeNameDirKey);
			Log("NameNode upgrade with one bad storage dir", numDirs);
			UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
			try
			{
				// assert("storage dir has been prepared for failure before reaching this point");
				StartNameNodeShouldFail(HdfsServerConstants.StartupOption.Upgrade, typeof(IOException
					), Sharpen.Pattern.Compile("failed in 1 storage"));
			}
			finally
			{
				// assert("storage dir shall be returned to normal state before exiting");
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
			}
		}

		/// <exception cref="System.Exception"/>
		private void DeleteStorageFilesWithPrefix(string[] nameNodeDirs, string prefix)
		{
			foreach (string baseDirStr in nameNodeDirs)
			{
				FilePath baseDir = new FilePath(baseDirStr);
				FilePath currentDir = new FilePath(baseDir, "current");
				foreach (FilePath f in currentDir.ListFiles())
				{
					if (f.GetName().StartsWith(prefix))
					{
						NUnit.Framework.Assert.IsTrue("Deleting " + f, f.Delete());
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUpgradeFromPreUpgradeLVFails()
		{
			// Upgrade from versions prior to Storage#LAST_UPGRADABLE_LAYOUT_VERSION
			// is not allowed
			Storage.CheckVersionUpgradable(Storage.LastPreUpgradeLayoutVersion + 1);
			NUnit.Framework.Assert.Fail("Expected IOException is not thrown");
		}

		[Ignore]
		public virtual void Test203LayoutVersion()
		{
			foreach (int lv in Storage.LayoutVersions203)
			{
				NUnit.Framework.Assert.IsTrue(Storage.Is203LayoutVersion(lv));
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestDFSUpgrade t = new TestDFSUpgrade();
			TestDFSUpgrade.Initialize();
			t.TestUpgrade();
		}
	}
}
