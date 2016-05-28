using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Functional tests for NNStorageRetentionManager.</summary>
	/// <remarks>
	/// Functional tests for NNStorageRetentionManager. This differs from
	/// <see cref="TestNNStorageRetentionManager"/>
	/// in that the other test suite
	/// is only unit/mock-based tests whereas this suite starts miniclusters,
	/// etc.
	/// </remarks>
	public class TestNNStorageRetentionFunctional
	{
		private static readonly FilePath TestRootDir = new FilePath(MiniDFSCluster.GetBaseDirectory
			());

		private static readonly Log Log = LogFactory.GetLog(typeof(TestNNStorageRetentionFunctional
			));

		/// <summary>
		/// Test case where two directories are configured as NAME_AND_EDITS
		/// and one of them fails to save storage.
		/// </summary>
		/// <remarks>
		/// Test case where two directories are configured as NAME_AND_EDITS
		/// and one of them fails to save storage. Since the edits and image
		/// failure states are decoupled, the failure of image saving should
		/// not prevent the purging of logs from that dir.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPurgingWithNameEditsDirAfterFailure()
		{
			MiniDFSCluster cluster = null;
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsNamenodeNumExtraEditsRetainedKey, 0);
			FilePath sd0 = new FilePath(TestRootDir, "nn0");
			FilePath sd1 = new FilePath(TestRootDir, "nn1");
			FilePath cd0 = new FilePath(sd0, "current");
			FilePath cd1 = new FilePath(sd1, "current");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Joiner.On(",").Join(sd0, sd1));
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).ManageNameDfsDirs(false
					).Format(true).Build();
				NameNode nn = cluster.GetNameNode();
				DoSaveNamespace(nn);
				Log.Info("After first save, images 0 and 2 should exist in both dirs");
				GenericTestUtils.AssertGlobEquals(cd0, "fsimage_\\d*", NNStorage.GetImageFileName
					(0), NNStorage.GetImageFileName(2));
				GenericTestUtils.AssertGlobEquals(cd1, "fsimage_\\d*", NNStorage.GetImageFileName
					(0), NNStorage.GetImageFileName(2));
				GenericTestUtils.AssertGlobEquals(cd0, "edits_.*", NNStorage.GetFinalizedEditsFileName
					(1, 2), NNStorage.GetInProgressEditsFileName(3));
				GenericTestUtils.AssertGlobEquals(cd1, "edits_.*", NNStorage.GetFinalizedEditsFileName
					(1, 2), NNStorage.GetInProgressEditsFileName(3));
				DoSaveNamespace(nn);
				Log.Info("After second save, image 0 should be purged, " + "and image 4 should exist in both."
					);
				GenericTestUtils.AssertGlobEquals(cd0, "fsimage_\\d*", NNStorage.GetImageFileName
					(2), NNStorage.GetImageFileName(4));
				GenericTestUtils.AssertGlobEquals(cd1, "fsimage_\\d*", NNStorage.GetImageFileName
					(2), NNStorage.GetImageFileName(4));
				GenericTestUtils.AssertGlobEquals(cd0, "edits_.*", NNStorage.GetFinalizedEditsFileName
					(3, 4), NNStorage.GetInProgressEditsFileName(5));
				GenericTestUtils.AssertGlobEquals(cd1, "edits_.*", NNStorage.GetFinalizedEditsFileName
					(3, 4), NNStorage.GetInProgressEditsFileName(5));
				Log.Info("Failing first storage dir by chmodding it");
				NUnit.Framework.Assert.AreEqual(0, FileUtil.Chmod(cd0.GetAbsolutePath(), "000"));
				DoSaveNamespace(nn);
				Log.Info("Restoring accessibility of first storage dir");
				NUnit.Framework.Assert.AreEqual(0, FileUtil.Chmod(cd0.GetAbsolutePath(), "755"));
				Log.Info("nothing should have been purged in first storage dir");
				GenericTestUtils.AssertGlobEquals(cd0, "fsimage_\\d*", NNStorage.GetImageFileName
					(2), NNStorage.GetImageFileName(4));
				GenericTestUtils.AssertGlobEquals(cd0, "edits_.*", NNStorage.GetFinalizedEditsFileName
					(3, 4), NNStorage.GetInProgressEditsFileName(5));
				Log.Info("fsimage_2 should be purged in second storage dir");
				GenericTestUtils.AssertGlobEquals(cd1, "fsimage_\\d*", NNStorage.GetImageFileName
					(4), NNStorage.GetImageFileName(6));
				GenericTestUtils.AssertGlobEquals(cd1, "edits_.*", NNStorage.GetFinalizedEditsFileName
					(5, 6), NNStorage.GetInProgressEditsFileName(7));
				Log.Info("On next save, we should purge logs from the failed dir," + " but not images, since the image directory is in failed state."
					);
				DoSaveNamespace(nn);
				GenericTestUtils.AssertGlobEquals(cd1, "fsimage_\\d*", NNStorage.GetImageFileName
					(6), NNStorage.GetImageFileName(8));
				GenericTestUtils.AssertGlobEquals(cd1, "edits_.*", NNStorage.GetFinalizedEditsFileName
					(7, 8), NNStorage.GetInProgressEditsFileName(9));
				GenericTestUtils.AssertGlobEquals(cd0, "fsimage_\\d*", NNStorage.GetImageFileName
					(2), NNStorage.GetImageFileName(4));
				GenericTestUtils.AssertGlobEquals(cd0, "edits_.*", NNStorage.GetInProgressEditsFileName
					(9));
			}
			finally
			{
				FileUtil.Chmod(cd0.GetAbsolutePath(), "755");
				Log.Info("Shutting down...");
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void DoSaveNamespace(NameNode nn)
		{
			Log.Info("Saving namespace...");
			nn.GetRpcServer().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, false);
			nn.GetRpcServer().SaveNamespace();
			nn.GetRpcServer().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, false);
		}
	}
}
