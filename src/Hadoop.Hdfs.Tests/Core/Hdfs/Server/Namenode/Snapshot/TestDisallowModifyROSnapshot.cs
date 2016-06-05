using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>This class tests snapshot functionality.</summary>
	/// <remarks>
	/// This class tests snapshot functionality. One or multiple snapshots are
	/// created. The snapshotted directory is changed and verification is done to
	/// ensure snapshots remain unchanges.
	/// </remarks>
	public class TestDisallowModifyROSnapshot
	{
		private static readonly Path dir = new Path("/TestSnapshot");

		private static readonly Path sub1 = new Path(dir, "sub1");

		private static readonly Path sub2 = new Path(dir, "sub2");

		protected internal static Configuration conf;

		protected internal static MiniDFSCluster cluster;

		protected internal static FSNamesystem fsn;

		protected internal static DistributedFileSystem fs;

		/// <summary>The list recording all previous snapshots.</summary>
		/// <remarks>
		/// The list recording all previous snapshots. Each element in the array
		/// records a snapshot root.
		/// </remarks>
		protected internal static AList<Path> snapshotList = new AList<Path>();

		internal static Path objInSnapshot = null;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			fs = cluster.GetFileSystem();
			Path path1 = new Path(sub1, "dir1");
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path1));
			Path path2 = new Path(sub2, "dir2");
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path2));
			SnapshotTestHelper.CreateSnapshot(fs, sub1, "testSnapshot");
			objInSnapshot = SnapshotTestHelper.GetSnapshotPath(sub1, "testSnapshot", "dir1");
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetReplication()
		{
			fs.SetReplication(objInSnapshot, (short)1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetPermission()
		{
			fs.SetPermission(objInSnapshot, new FsPermission("777"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetOwner()
		{
			fs.SetOwner(objInSnapshot, "username", "groupname");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRename()
		{
			try
			{
				fs.Rename(objInSnapshot, new Path("/invalid/path"));
				NUnit.Framework.Assert.Fail("Didn't throw SnapshotAccessControlException");
			}
			catch (SnapshotAccessControlException)
			{
			}
			/* Ignored */
			try
			{
				fs.Rename(sub2, objInSnapshot);
				NUnit.Framework.Assert.Fail("Didn't throw SnapshotAccessControlException");
			}
			catch (SnapshotAccessControlException)
			{
			}
			/* Ignored */
			try
			{
				fs.Rename(sub2, objInSnapshot, (Options.Rename)null);
				NUnit.Framework.Assert.Fail("Didn't throw SnapshotAccessControlException");
			}
			catch (SnapshotAccessControlException)
			{
			}
		}

		/* Ignored */
		/// <exception cref="System.Exception"/>
		public virtual void TestDelete()
		{
			fs.Delete(objInSnapshot, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuota()
		{
			fs.SetQuota(objInSnapshot, 100, 100);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetTime()
		{
			fs.SetTimes(objInSnapshot, 100, 100);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreate()
		{
			DFSClient dfsclient = new DFSClient(conf);
			dfsclient.Create(objInSnapshot.ToString(), true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppend()
		{
			fs.Append(objInSnapshot, 65535, null);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMkdir()
		{
			fs.Mkdirs(objInSnapshot, new FsPermission("777"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateSymlink()
		{
			DFSClient dfsclient = new DFSClient(conf);
			dfsclient.CreateSymlink(sub2.ToString(), "/TestSnapshot/sub1/.snapshot", false);
		}
	}
}
