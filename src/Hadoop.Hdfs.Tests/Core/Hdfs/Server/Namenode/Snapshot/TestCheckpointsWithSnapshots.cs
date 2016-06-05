using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	public class TestCheckpointsWithSnapshots
	{
		private static readonly Path TestPath = new Path("/foo");

		private static readonly Configuration conf = new HdfsConfiguration();

		static TestCheckpointsWithSnapshots()
		{
			conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
		}

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			FileUtil.FullyDeleteContents(new FilePath(MiniDFSCluster.GetBaseDirectory()));
		}

		/// <summary>
		/// Regression test for HDFS-5433 - "When reloading fsimage during
		/// checkpointing, we should clear existing snapshottable directories"
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckpoint()
		{
			MiniDFSCluster cluster = null;
			SecondaryNameNode secondary = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				secondary = new SecondaryNameNode(conf);
				SnapshotManager nnSnapshotManager = cluster.GetNamesystem().GetSnapshotManager();
				SnapshotManager secondarySnapshotManager = secondary.GetFSNamesystem().GetSnapshotManager
					();
				FileSystem fs = cluster.GetFileSystem();
				HdfsAdmin admin = new HdfsAdmin(FileSystem.GetDefaultUri(conf), conf);
				NUnit.Framework.Assert.AreEqual(0, nnSnapshotManager.GetNumSnapshots());
				NUnit.Framework.Assert.AreEqual(0, nnSnapshotManager.GetNumSnapshottableDirs());
				NUnit.Framework.Assert.AreEqual(0, secondarySnapshotManager.GetNumSnapshots());
				NUnit.Framework.Assert.AreEqual(0, secondarySnapshotManager.GetNumSnapshottableDirs
					());
				// 1. Create a snapshottable directory foo on the NN.
				fs.Mkdirs(TestPath);
				admin.AllowSnapshot(TestPath);
				NUnit.Framework.Assert.AreEqual(0, nnSnapshotManager.GetNumSnapshots());
				NUnit.Framework.Assert.AreEqual(1, nnSnapshotManager.GetNumSnapshottableDirs());
				// 2. Create a snapshot of the dir foo. This will be referenced both in
				// the SnapshotManager as well as in the file system tree. The snapshot
				// count will go up to 1.
				Path snapshotPath = fs.CreateSnapshot(TestPath);
				NUnit.Framework.Assert.AreEqual(1, nnSnapshotManager.GetNumSnapshots());
				NUnit.Framework.Assert.AreEqual(1, nnSnapshotManager.GetNumSnapshottableDirs());
				// 3. Start up a 2NN and have it do a checkpoint. It will have foo and its
				// snapshot in its list of snapshottable dirs referenced from the
				// SnapshotManager, as well as in the file system tree.
				secondary.DoCheckpoint();
				NUnit.Framework.Assert.AreEqual(1, secondarySnapshotManager.GetNumSnapshots());
				NUnit.Framework.Assert.AreEqual(1, secondarySnapshotManager.GetNumSnapshottableDirs
					());
				// 4. Disallow snapshots on and delete foo on the NN. The snapshot count
				// will go down to 0 and the snapshottable dir will be removed from the fs
				// tree.
				fs.DeleteSnapshot(TestPath, snapshotPath.GetName());
				admin.DisallowSnapshot(TestPath);
				NUnit.Framework.Assert.AreEqual(0, nnSnapshotManager.GetNumSnapshots());
				NUnit.Framework.Assert.AreEqual(0, nnSnapshotManager.GetNumSnapshottableDirs());
				// 5. Have the NN do a saveNamespace, writing out a new fsimage with
				// snapshot count 0.
				NameNodeAdapter.EnterSafeMode(cluster.GetNameNode(), false);
				NameNodeAdapter.SaveNamespace(cluster.GetNameNode());
				NameNodeAdapter.LeaveSafeMode(cluster.GetNameNode());
				// 6. Have the still-running 2NN do a checkpoint. It will notice that the
				// fsimage has changed on the NN and redownload/reload from that image.
				// This will replace all INodes in the file system tree as well as reset
				// the snapshot counter to 0 in the SnapshotManager. However, it will not
				// clear the list of snapshottable dirs referenced from the
				// SnapshotManager. When it writes out an fsimage, the 2NN will write out
				// 0 for the snapshot count, but still serialize the snapshottable dir
				// referenced in the SnapshotManager even though it no longer appears in
				// the file system tree. The NN will not be able to start up with this.
				secondary.DoCheckpoint();
				NUnit.Framework.Assert.AreEqual(0, secondarySnapshotManager.GetNumSnapshots());
				NUnit.Framework.Assert.AreEqual(0, secondarySnapshotManager.GetNumSnapshottableDirs
					());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				if (secondary != null)
				{
					secondary.Shutdown();
				}
			}
		}
	}
}
