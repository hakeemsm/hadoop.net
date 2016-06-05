using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class includes end-to-end tests for snapshot related FsShell and
	/// DFSAdmin commands.
	/// </summary>
	public class TestSnapshotCommands
	{
		private static Configuration conf;

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem fs;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void ClusterSetUp()
		{
			conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void ClusterShutdown()
		{
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			fs.Mkdirs(new Path("/sub1"));
			fs.AllowSnapshot(new Path("/sub1"));
			fs.Mkdirs(new Path("/sub1/sub1sub1"));
			fs.Mkdirs(new Path("/sub1/sub1sub2"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (fs.Exists(new Path("/sub1")))
			{
				if (fs.Exists(new Path("/sub1/.snapshot")))
				{
					foreach (FileStatus st in fs.ListStatus(new Path("/sub1/.snapshot")))
					{
						fs.DeleteSnapshot(new Path("/sub1"), st.GetPath().GetName());
					}
					fs.DisallowSnapshot(new Path("/sub1"));
				}
				fs.Delete(new Path("/sub1"), true);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllowSnapshot()
		{
			// Idempotent test
			DFSTestUtil.DFSAdminRun("-allowSnapshot /sub1", 0, "Allowing snaphot on /sub1 succeeded"
				, conf);
			// allow normal dir success 
			DFSTestUtil.FsShellRun("-mkdir /sub2", conf);
			DFSTestUtil.DFSAdminRun("-allowSnapshot /sub2", 0, "Allowing snaphot on /sub2 succeeded"
				, conf);
			// allow non-exists dir failed
			DFSTestUtil.DFSAdminRun("-allowSnapshot /sub3", -1, null, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateSnapshot()
		{
			// test createSnapshot
			DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn0", 0, "Created snapshot /sub1/.snapshot/sn0"
				, conf);
			DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn0", 1, "there is already a snapshot with the same name \"sn0\""
				, conf);
			DFSTestUtil.FsShellRun("-rmr /sub1/sub1sub2", conf);
			DFSTestUtil.FsShellRun("-mkdir /sub1/sub1sub3", conf);
			DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn1", 0, "Created snapshot /sub1/.snapshot/sn1"
				, conf);
			// check snapshot contents
			DFSTestUtil.FsShellRun("-ls /sub1", 0, "/sub1/sub1sub1", conf);
			DFSTestUtil.FsShellRun("-ls /sub1", 0, "/sub1/sub1sub3", conf);
			DFSTestUtil.FsShellRun("-ls /sub1/.snapshot", 0, "/sub1/.snapshot/sn0", conf);
			DFSTestUtil.FsShellRun("-ls /sub1/.snapshot", 0, "/sub1/.snapshot/sn1", conf);
			DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn0", 0, "/sub1/.snapshot/sn0/sub1sub1"
				, conf);
			DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn0", 0, "/sub1/.snapshot/sn0/sub1sub2"
				, conf);
			DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn1", 0, "/sub1/.snapshot/sn1/sub1sub1"
				, conf);
			DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn1", 0, "/sub1/.snapshot/sn1/sub1sub3"
				, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirUsingReservedName()
		{
			// test can not create dir with reserved name: .snapshot
			DFSTestUtil.FsShellRun("-ls /", conf);
			DFSTestUtil.FsShellRun("-mkdir /.snapshot", 1, "File exists", conf);
			DFSTestUtil.FsShellRun("-mkdir /sub1/.snapshot", 1, "File exists", conf);
			// mkdir -p ignore reserved name check if dir already exists
			DFSTestUtil.FsShellRun("-mkdir -p /sub1/.snapshot", conf);
			DFSTestUtil.FsShellRun("-mkdir -p /sub1/sub1sub1/.snapshot", 1, "mkdir: \".snapshot\" is a reserved name."
				, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameSnapshot()
		{
			DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn.orig", conf);
			DFSTestUtil.FsShellRun("-renameSnapshot /sub1 sn.orig sn.rename", conf);
			DFSTestUtil.FsShellRun("-ls /sub1/.snapshot", 0, "/sub1/.snapshot/sn.rename", conf
				);
			DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn.rename", 0, "/sub1/.snapshot/sn.rename/sub1sub1"
				, conf);
			DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn.rename", 0, "/sub1/.snapshot/sn.rename/sub1sub2"
				, conf);
			//try renaming from a non-existing snapshot
			DFSTestUtil.FsShellRun("-renameSnapshot /sub1 sn.nonexist sn.rename", 1, "renameSnapshot: The snapshot sn.nonexist does not exist for directory /sub1"
				, conf);
			//try renaming to existing snapshots
			DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn.new", conf);
			DFSTestUtil.FsShellRun("-renameSnapshot /sub1 sn.new sn.rename", 1, "renameSnapshot: The snapshot sn.rename already exists for directory /sub1"
				, conf);
			DFSTestUtil.FsShellRun("-renameSnapshot /sub1 sn.rename sn.new", 1, "renameSnapshot: The snapshot sn.new already exists for directory /sub1"
				, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteSnapshot()
		{
			DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn1", conf);
			DFSTestUtil.FsShellRun("-deleteSnapshot /sub1 sn1", conf);
			DFSTestUtil.FsShellRun("-deleteSnapshot /sub1 sn1", 1, "deleteSnapshot: Cannot delete snapshot sn1 from path /sub1: the snapshot does not exist."
				, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDisallowSnapshot()
		{
			DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn1", conf);
			// cannot delete snapshotable dir
			DFSTestUtil.FsShellRun("-rmr /sub1", 1, "The directory /sub1 cannot be deleted since /sub1 is snapshottable and already has snapshots"
				, conf);
			DFSTestUtil.DFSAdminRun("-disallowSnapshot /sub1", -1, "disallowSnapshot: The directory /sub1 has snapshot(s). Please redo the operation after removing all the snapshots."
				, conf);
			DFSTestUtil.FsShellRun("-deleteSnapshot /sub1 sn1", conf);
			DFSTestUtil.DFSAdminRun("-disallowSnapshot /sub1", 0, "Disallowing snaphot on /sub1 succeeded"
				, conf);
			// Idempotent test
			DFSTestUtil.DFSAdminRun("-disallowSnapshot /sub1", 0, "Disallowing snaphot on /sub1 succeeded"
				, conf);
			// now it can be deleted
			DFSTestUtil.FsShellRun("-rmr /sub1", conf);
		}
	}
}
