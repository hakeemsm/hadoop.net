using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	public class TestSnapshottableDirListing
	{
		internal const long seed = 0;

		internal const short Replication = 3;

		internal const long Blocksize = 1024;

		private readonly Path root = new Path("/");

		private readonly Path dir1 = new Path("/TestSnapshot1");

		private readonly Path dir2 = new Path("/TestSnapshot2");

		internal Configuration conf;

		internal MiniDFSCluster cluster;

		internal FSNamesystem fsn;

		internal DistributedFileSystem hdfs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
			hdfs.Mkdirs(dir1);
			hdfs.Mkdirs(dir2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test listing all the snapshottable directories</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestListSnapshottableDir()
		{
			cluster.GetNamesystem().GetSnapshotManager().SetAllowNestedSnapshots(true);
			// Initially there is no snapshottable directories in the system
			SnapshottableDirectoryStatus[] dirs = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.IsNull(dirs);
			// Make root as snapshottable
			Path root = new Path("/");
			hdfs.AllowSnapshot(root);
			dirs = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(1, dirs.Length);
			NUnit.Framework.Assert.AreEqual(string.Empty, dirs[0].GetDirStatus().GetLocalName
				());
			NUnit.Framework.Assert.AreEqual(root, dirs[0].GetFullPath());
			// Make root non-snaphsottable
			hdfs.DisallowSnapshot(root);
			dirs = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.IsNull(dirs);
			// Make dir1 as snapshottable
			hdfs.AllowSnapshot(dir1);
			dirs = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(1, dirs.Length);
			NUnit.Framework.Assert.AreEqual(dir1.GetName(), dirs[0].GetDirStatus().GetLocalName
				());
			NUnit.Framework.Assert.AreEqual(dir1, dirs[0].GetFullPath());
			// There is no snapshot for dir1 yet
			NUnit.Framework.Assert.AreEqual(0, dirs[0].GetSnapshotNumber());
			// Make dir2 as snapshottable
			hdfs.AllowSnapshot(dir2);
			dirs = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(2, dirs.Length);
			NUnit.Framework.Assert.AreEqual(dir1.GetName(), dirs[0].GetDirStatus().GetLocalName
				());
			NUnit.Framework.Assert.AreEqual(dir1, dirs[0].GetFullPath());
			NUnit.Framework.Assert.AreEqual(dir2.GetName(), dirs[1].GetDirStatus().GetLocalName
				());
			NUnit.Framework.Assert.AreEqual(dir2, dirs[1].GetFullPath());
			// There is no snapshot for dir2 yet
			NUnit.Framework.Assert.AreEqual(0, dirs[1].GetSnapshotNumber());
			// Create dir3
			Path dir3 = new Path("/TestSnapshot3");
			hdfs.Mkdirs(dir3);
			// Rename dir3 to dir2
			hdfs.Rename(dir3, dir2, Options.Rename.Overwrite);
			// Now we only have one snapshottable dir: dir1
			dirs = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(1, dirs.Length);
			NUnit.Framework.Assert.AreEqual(dir1, dirs[0].GetFullPath());
			// Make dir2 snapshottable again
			hdfs.AllowSnapshot(dir2);
			// Create a snapshot for dir2
			hdfs.CreateSnapshot(dir2, "s1");
			hdfs.CreateSnapshot(dir2, "s2");
			dirs = hdfs.GetSnapshottableDirListing();
			// There are now 2 snapshots for dir2
			NUnit.Framework.Assert.AreEqual(dir2, dirs[1].GetFullPath());
			NUnit.Framework.Assert.AreEqual(2, dirs[1].GetSnapshotNumber());
			// Create sub-dirs under dir1
			Path sub1 = new Path(dir1, "sub1");
			Path file1 = new Path(sub1, "file1");
			Path sub2 = new Path(dir1, "sub2");
			Path file2 = new Path(sub2, "file2");
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file2, Blocksize, Replication, seed);
			// Make sub1 and sub2 snapshottable
			hdfs.AllowSnapshot(sub1);
			hdfs.AllowSnapshot(sub2);
			dirs = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(4, dirs.Length);
			NUnit.Framework.Assert.AreEqual(dir1, dirs[0].GetFullPath());
			NUnit.Framework.Assert.AreEqual(dir2, dirs[1].GetFullPath());
			NUnit.Framework.Assert.AreEqual(sub1, dirs[2].GetFullPath());
			NUnit.Framework.Assert.AreEqual(sub2, dirs[3].GetFullPath());
			// reset sub1
			hdfs.DisallowSnapshot(sub1);
			dirs = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(3, dirs.Length);
			NUnit.Framework.Assert.AreEqual(dir1, dirs[0].GetFullPath());
			NUnit.Framework.Assert.AreEqual(dir2, dirs[1].GetFullPath());
			NUnit.Framework.Assert.AreEqual(sub2, dirs[2].GetFullPath());
			// Remove dir1, both dir1 and sub2 will be removed
			hdfs.Delete(dir1, true);
			dirs = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(1, dirs.Length);
			NUnit.Framework.Assert.AreEqual(dir2.GetName(), dirs[0].GetDirStatus().GetLocalName
				());
			NUnit.Framework.Assert.AreEqual(dir2, dirs[0].GetFullPath());
		}

		/// <summary>
		/// Test the listing with different user names to make sure only directories
		/// that are owned by the user are listed.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestListWithDifferentUser()
		{
			cluster.GetNamesystem().GetSnapshotManager().SetAllowNestedSnapshots(true);
			// first make dir1 and dir2 snapshottable
			hdfs.AllowSnapshot(dir1);
			hdfs.AllowSnapshot(dir2);
			hdfs.SetPermission(root, FsPermission.ValueOf("-rwxrwxrwx"));
			// create two dirs and make them snapshottable under the name of user1
			UserGroupInformation ugi1 = UserGroupInformation.CreateUserForTesting("user1", new 
				string[] { "group1" });
			DistributedFileSystem fs1 = (DistributedFileSystem)DFSTestUtil.GetFileSystemAs(ugi1
				, conf);
			Path dir1_user1 = new Path("/dir1_user1");
			Path dir2_user1 = new Path("/dir2_user1");
			fs1.Mkdirs(dir1_user1);
			fs1.Mkdirs(dir2_user1);
			hdfs.AllowSnapshot(dir1_user1);
			hdfs.AllowSnapshot(dir2_user1);
			// user2
			UserGroupInformation ugi2 = UserGroupInformation.CreateUserForTesting("user2", new 
				string[] { "group2" });
			DistributedFileSystem fs2 = (DistributedFileSystem)DFSTestUtil.GetFileSystemAs(ugi2
				, conf);
			Path dir_user2 = new Path("/dir_user2");
			Path subdir_user2 = new Path(dir_user2, "subdir");
			fs2.Mkdirs(dir_user2);
			fs2.Mkdirs(subdir_user2);
			hdfs.AllowSnapshot(dir_user2);
			hdfs.AllowSnapshot(subdir_user2);
			// super user
			string supergroup = conf.Get(DFSConfigKeys.DfsPermissionsSuperusergroupKey, DFSConfigKeys
				.DfsPermissionsSuperusergroupDefault);
			UserGroupInformation superUgi = UserGroupInformation.CreateUserForTesting("superuser"
				, new string[] { supergroup });
			DistributedFileSystem fs3 = (DistributedFileSystem)DFSTestUtil.GetFileSystemAs(superUgi
				, conf);
			// list the snapshottable dirs for superuser
			SnapshottableDirectoryStatus[] dirs = fs3.GetSnapshottableDirListing();
			// 6 snapshottable dirs: dir1, dir2, dir1_user1, dir2_user1, dir_user2, and
			// subdir_user2
			NUnit.Framework.Assert.AreEqual(6, dirs.Length);
			// list the snapshottable dirs for user1
			dirs = fs1.GetSnapshottableDirListing();
			// 2 dirs owned by user1: dir1_user1 and dir2_user1
			NUnit.Framework.Assert.AreEqual(2, dirs.Length);
			NUnit.Framework.Assert.AreEqual(dir1_user1, dirs[0].GetFullPath());
			NUnit.Framework.Assert.AreEqual(dir2_user1, dirs[1].GetFullPath());
			// list the snapshottable dirs for user2
			dirs = fs2.GetSnapshottableDirListing();
			// 2 dirs owned by user2: dir_user2 and subdir_user2
			NUnit.Framework.Assert.AreEqual(2, dirs.Length);
			NUnit.Framework.Assert.AreEqual(dir_user2, dirs[0].GetFullPath());
			NUnit.Framework.Assert.AreEqual(subdir_user2, dirs[1].GetFullPath());
		}
	}
}
