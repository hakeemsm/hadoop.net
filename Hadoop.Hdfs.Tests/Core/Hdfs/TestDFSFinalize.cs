using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This test ensures the appropriate response from the system when
	/// the system is finalized.
	/// </summary>
	public class TestDFSFinalize
	{
		private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestDFSFinalize"
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

		/// <summary>
		/// Verify that the current directory exists and that the previous directory
		/// does not exist.
		/// </summary>
		/// <remarks>
		/// Verify that the current directory exists and that the previous directory
		/// does not exist.  Verify that current hasn't been modified by comparing
		/// the checksum of all it's containing files with their original checksum.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		internal static void CheckResult(string[] nameNodeDirs, string[] dataNodeDirs, string
			 bpid)
		{
			IList<FilePath> dirs = Lists.NewArrayList();
			for (int i = 0; i < nameNodeDirs.Length; i++)
			{
				FilePath curDir = new FilePath(nameNodeDirs[i], "current");
				dirs.AddItem(curDir);
				FSImageTestUtil.AssertReasonableNameCurrentDir(curDir);
			}
			FSImageTestUtil.AssertParallelFilesAreIdentical(dirs, Sharpen.Collections.EmptySet
				<string>());
			FilePath[] dnCurDirs = new FilePath[dataNodeDirs.Length];
			for (int i_1 = 0; i_1 < dataNodeDirs.Length; i_1++)
			{
				dnCurDirs[i_1] = new FilePath(dataNodeDirs[i_1], "current");
				NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
					.DataNode, dnCurDirs[i_1], false), UpgradeUtilities.ChecksumMasterDataNodeContents
					());
			}
			for (int i_2 = 0; i_2 < nameNodeDirs.Length; i_2++)
			{
				NUnit.Framework.Assert.IsFalse(new FilePath(nameNodeDirs[i_2], "previous").IsDirectory
					());
			}
			if (bpid == null)
			{
				for (int i_3 = 0; i_3 < dataNodeDirs.Length; i_3++)
				{
					NUnit.Framework.Assert.IsFalse(new FilePath(dataNodeDirs[i_3], "previous").IsDirectory
						());
				}
			}
			else
			{
				for (int i_3 = 0; i_3 < dataNodeDirs.Length; i_3++)
				{
					FilePath bpRoot = BlockPoolSliceStorage.GetBpRoot(bpid, dnCurDirs[i_3]);
					NUnit.Framework.Assert.IsFalse(new FilePath(bpRoot, "previous").IsDirectory());
					FilePath bpCurFinalizeDir = new FilePath(bpRoot, "current/" + DataStorage.StorageDirFinalized
						);
					NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
						.DataNode, bpCurFinalizeDir, true), UpgradeUtilities.ChecksumMasterBlockPoolFinalizedContents
						());
				}
			}
		}

		/// <summary>This test attempts to finalize the NameNode and DataNode.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFinalize()
		{
			UpgradeUtilities.Initialize();
			for (int numDirs = 1; numDirs <= 2; numDirs++)
			{
				/* This test requires that "current" directory not change after
				* the upgrade. Actually it is ok for those contents to change.
				* For now disabling block verification so that the contents are
				* not changed.
				* Disable duplicate replica deletion as the test intentionally
				* mirrors the contents of storage directories.
				*/
				conf = new HdfsConfiguration();
				conf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
				conf.SetBoolean(DFSConfigKeys.DfsDatanodeDuplicateReplicaDeletion, false);
				conf = UpgradeUtilities.InitializeStorageStateConf(numDirs, conf);
				string[] nameNodeDirs = conf.GetStrings(DFSConfigKeys.DfsNamenodeNameDirKey);
				string[] dataNodeDirs = conf.GetStrings(DFSConfigKeys.DfsDatanodeDataDirKey);
				Log("Finalize NN & DN with existing previous dir", numDirs);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "previous");
				cluster = new MiniDFSCluster.Builder(conf).Format(false).ManageDataDfsDirs(false)
					.ManageNameDfsDirs(false).StartupOption(HdfsServerConstants.StartupOption.Regular
					).Build();
				cluster.FinalizeCluster(conf);
				cluster.TriggerBlockReports();
				// 1 second should be enough for asynchronous DN finalize
				Sharpen.Thread.Sleep(1000);
				CheckResult(nameNodeDirs, dataNodeDirs, null);
				Log("Finalize NN & DN without existing previous dir", numDirs);
				cluster.FinalizeCluster(conf);
				cluster.TriggerBlockReports();
				// 1 second should be enough for asynchronous DN finalize
				Sharpen.Thread.Sleep(1000);
				CheckResult(nameNodeDirs, dataNodeDirs, null);
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
				Log("Finalize NN & BP with existing previous dir", numDirs);
				string bpid = UpgradeUtilities.GetCurrentBlockPoolID(cluster);
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "current");
				UpgradeUtilities.CreateNameNodeStorageDirs(nameNodeDirs, "previous");
				UpgradeUtilities.CreateDataNodeStorageDirs(dataNodeDirs, "current");
				UpgradeUtilities.CreateBlockPoolStorageDirs(dataNodeDirs, "current", bpid);
				UpgradeUtilities.CreateBlockPoolStorageDirs(dataNodeDirs, "previous", bpid);
				cluster = new MiniDFSCluster.Builder(conf).Format(false).ManageDataDfsDirs(false)
					.ManageNameDfsDirs(false).StartupOption(HdfsServerConstants.StartupOption.Regular
					).Build();
				cluster.FinalizeCluster(conf);
				cluster.TriggerBlockReports();
				// 1 second should be enough for asynchronous BP finalize
				Sharpen.Thread.Sleep(1000);
				CheckResult(nameNodeDirs, dataNodeDirs, bpid);
				Log("Finalize NN & BP without existing previous dir", numDirs);
				cluster.FinalizeCluster(conf);
				cluster.TriggerBlockReports();
				// 1 second should be enough for asynchronous BP finalize
				Sharpen.Thread.Sleep(1000);
				CheckResult(nameNodeDirs, dataNodeDirs, bpid);
				cluster.Shutdown();
				UpgradeUtilities.CreateEmptyDirs(nameNodeDirs);
				UpgradeUtilities.CreateEmptyDirs(dataNodeDirs);
			}
		}

		// end numDir loop
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
			new TestDFSFinalize().TestFinalize();
		}
	}
}
