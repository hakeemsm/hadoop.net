using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This test ensures the appropriate response (successful or failure) from
	/// the system when the system is rolled back under various storage state and
	/// version conditions.
	/// </summary>
	public class TestDFSRollback
	{
		private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestDFSRollback"
			);

		private Configuration conf;

		private int testCounter = 0;

		private MiniDFSCluster cluster = null;

		/// <summary>Writes an INFO log message containing the parameters.</summary>
		internal virtual void Log(string label, int numDirs)
		{
			Log.Info("============================================================");
			Log.Info("***TEST " + (testCounter++) + "*** " + label + ":" + " numDirs=" + numDirs
				);
		}

		/// <summary>Verify that the new current directory is the old previous.</summary>
		/// <remarks>
		/// Verify that the new current directory is the old previous.
		/// It is assumed that the server has recovered and rolled back.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		internal virtual void CheckResult(HdfsServerConstants.NodeType nodeType, string[]
			 baseDirs)
		{
			IList<FilePath> curDirs = Lists.NewArrayList();
			foreach (string baseDir in baseDirs)
			{
				FilePath curDir = new FilePath(baseDir, "current");
				curDirs.AddItem(curDir);
				switch (nodeType)
				{
					case HdfsServerConstants.NodeType.NameNode:
					{
						FSImageTestUtil.AssertReasonableNameCurrentDir(curDir);
						break;
					}

					case HdfsServerConstants.NodeType.DataNode:
					{
						NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(nodeType, curDir
							, false), UpgradeUtilities.ChecksumMasterDataNodeContents());
						break;
					}
				}
			}
			FSImageTestUtil.AssertParallelFilesAreIdentical(curDirs, Sharpen.Collections.EmptySet
				<string>());
			for (int i = 0; i < baseDirs.Length; i++)
			{
				NUnit.Framework.Assert.IsFalse(new FilePath(baseDirs[i], "previous").IsDirectory(
					));
			}
		}

		/// <summary>Attempts to start a NameNode with the given operation.</summary>
		/// <remarks>
		/// Attempts to start a NameNode with the given operation.  Starting
		/// the NameNode should throw an exception.
		/// </remarks>
		internal virtual void StartNameNodeShouldFail(string searchString)
		{
			try
			{
				NameNode.DoRollback(conf, false);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).ManageDataDfsDirs
					(false).ManageNameDfsDirs(false).Build();
				// should fail
				throw new Exception("NameNode should have failed to start");
			}
			catch (Exception expected)
			{
				if (!expected.Message.Contains(searchString))
				{
					NUnit.Framework.Assert.Fail("Expected substring '" + searchString + "' in exception "
						 + "but got: " + StringUtils.StringifyException(expected));
				}
			}
		}

		// expected
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
		/// This test attempts to rollback the NameNode and DataNode under
		/// a number of valid and invalid conditions.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRollback()
		{
			FilePath[] baseDirs;
			UpgradeUtilities.Initialize();
			StorageInfo storageInfo = null;
			for (int numDirs = 1; numDirs <= 2; numDirs++)
			{
				conf = new HdfsConfiguration();
				conf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
				conf = UpgradeUtilities.InitializeStorageStateConf(numDirs, conf);
				string[] nameNodeDirs = conf.GetStrings(DFSConfigKeys.DfsNamenodeNameDirKey);
				string[] dataNodeDirs = conf.GetStrings(DFSConfigKeys.DfsDatanodeDataDirKey);
				Log("Normal NameNode rollback", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				NameNode.DoRollback(conf, false);
				CheckResult(HdfsServerConstants.NodeType.NameNode, nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("Normal DataNode rollback", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				NameNode.DoRollback(conf, false);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).ManageDataDfsDirs
					(false).ManageNameDfsDirs(false).DnStartupOption(HdfsServerConstants.StartupOption
					.Rollback).Build();
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "previous");
				cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Rollback
					, null);
				CheckResult(HdfsServerConstants.NodeType.DataNode, dataNodeDirs);
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
				Log("Normal BlockPool rollback", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				NameNode.DoRollback(conf, false);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).ManageDataDfsDirs
					(false).ManageNameDfsDirs(false).DnStartupOption(HdfsServerConstants.StartupOption
					.Rollback).Build();
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				UpgradeUtilities.CreateBlockPoolStorageDirs(dataNodeDirs, "current", UpgradeUtilities
					.GetCurrentBlockPoolID(cluster));
				// Create a previous snapshot for the blockpool
				UpgradeUtilities.CreateBlockPoolStorageDirs(dataNodeDirs, "previous", UpgradeUtilities
					.GetCurrentBlockPoolID(cluster));
				// Put newer layout version in current.
				storageInfo = new StorageInfo(HdfsConstants.DatanodeLayoutVersion - 1, UpgradeUtilities
					.GetCurrentNamespaceID(cluster), UpgradeUtilities.GetCurrentClusterID(cluster), 
					UpgradeUtilities.GetCurrentFsscTime(cluster), HdfsServerConstants.NodeType.DataNode
					);
				// Overwrite VERSION file in the current directory of
				// volume directories and block pool slice directories
				// with a layout version from future.
				FilePath[] dataCurrentDirs = new FilePath[dataNodeDirs.Length];
				for (int i = 0; i < dataNodeDirs.Length; i++)
				{
					dataCurrentDirs[i] = new FilePath((new Path(dataNodeDirs[i] + "/current")).ToString
						());
				}
				UpgradeUtilities.CreateDataNodeVersionFile(dataCurrentDirs, storageInfo, UpgradeUtilities
					.GetCurrentBlockPoolID(cluster));
				cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Rollback
					, null);
				NUnit.Framework.Assert.IsTrue(cluster.IsDataNodeUp());
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
				Log("NameNode rollback without existing previous dir", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				StartNameNodeShouldFail("None of the storage directories contain previous fs state"
					);
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("DataNode rollback without existing previous dir", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).ManageDataDfsDirs
					(false).ManageNameDfsDirs(false).StartupOption(HdfsServerConstants.StartupOption
					.Upgrade).Build();
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Rollback
					, null);
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
				Log("DataNode rollback with future stored layout version in previous", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				NameNode.DoRollback(conf, false);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).ManageDataDfsDirs
					(false).ManageNameDfsDirs(false).DnStartupOption(HdfsServerConstants.StartupOption
					.Rollback).Build();
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				baseDirs = UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "previous");
				storageInfo = new StorageInfo(int.MinValue, UpgradeUtilities.GetCurrentNamespaceID
					(cluster), UpgradeUtilities.GetCurrentClusterID(cluster), UpgradeUtilities.GetCurrentFsscTime
					(cluster), HdfsServerConstants.NodeType.DataNode);
				UpgradeUtilities.CreateDataNodeVersionFile(baseDirs, storageInfo, UpgradeUtilities
					.GetCurrentBlockPoolID(cluster));
				StartBlockPoolShouldFail(HdfsServerConstants.StartupOption.Rollback, cluster.GetNamesystem
					().GetBlockPoolId());
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
				Log("DataNode rollback with newer fsscTime in previous", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				NameNode.DoRollback(conf, false);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).ManageDataDfsDirs
					(false).ManageNameDfsDirs(false).DnStartupOption(HdfsServerConstants.StartupOption
					.Rollback).Build();
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				baseDirs = UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "previous");
				storageInfo = new StorageInfo(HdfsConstants.DatanodeLayoutVersion, UpgradeUtilities
					.GetCurrentNamespaceID(cluster), UpgradeUtilities.GetCurrentClusterID(cluster), 
					long.MaxValue, HdfsServerConstants.NodeType.DataNode);
				UpgradeUtilities.CreateDataNodeVersionFile(baseDirs, storageInfo, UpgradeUtilities
					.GetCurrentBlockPoolID(cluster));
				StartBlockPoolShouldFail(HdfsServerConstants.StartupOption.Rollback, cluster.GetNamesystem
					().GetBlockPoolId());
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
				Log("NameNode rollback with no edits file", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				baseDirs = UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				DeleteMatchingFiles(baseDirs, "edits.*");
				StartNameNodeShouldFail("Gap in transactions");
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("NameNode rollback with no image file", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				baseDirs = UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				DeleteMatchingFiles(baseDirs, "fsimage_.*");
				StartNameNodeShouldFail("No valid image files found");
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("NameNode rollback with corrupt version file", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				baseDirs = UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				foreach (FilePath f in baseDirs)
				{
					UpgradeUtilities.CorruptFile(new FilePath(f, "VERSION"), Sharpen.Runtime.GetBytesForString
						("layoutVersion", Charsets.Utf8), Sharpen.Runtime.GetBytesForString("xxxxxxxxxxxxx"
						, Charsets.Utf8));
				}
				StartNameNodeShouldFail("file VERSION has layoutVersion missing");
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				Log("NameNode rollback with old layout version in previous", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				baseDirs = UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				storageInfo = new StorageInfo(1, UpgradeUtilities.GetCurrentNamespaceID(null), UpgradeUtilities
					.GetCurrentClusterID(null), UpgradeUtilities.GetCurrentFsscTime(null), HdfsServerConstants.NodeType
					.NameNode);
				UpgradeUtilities.CreateNameNodeVersionFile(conf, baseDirs, storageInfo, UpgradeUtilities
					.GetCurrentBlockPoolID(cluster));
				StartNameNodeShouldFail("Cannot rollback to storage version 1 using this version"
					);
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
			}
		}

		// end numDir loop
		private void DeleteMatchingFiles(FilePath[] baseDirs, string regex)
		{
			foreach (FilePath baseDir in baseDirs)
			{
				foreach (FilePath f in baseDir.ListFiles())
				{
					if (f.GetName().Matches(regex))
					{
						f.Delete();
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
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
			new TestDFSRollback().TestRollback();
		}
	}
}
