using System.IO;
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
	/// <summary>Testing nested snapshots.</summary>
	public class TestNestedSnapshots
	{
		static TestNestedSnapshots()
		{
			// These tests generate a large number of edits, and repeated edit log
			// flushes can be a bottleneck.
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
		}

		private const long Seed = 0;

		private static readonly Random Random = new Random(Seed);

		private const short Replication = 3;

		private const long Blocksize = 1024;

		private static readonly Configuration conf = new Configuration();

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem hdfs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			hdfs = cluster.GetFileSystem();
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

		/// <summary>
		/// Create a snapshot for /test/foo and create another snapshot for
		/// /test/foo/bar.
		/// </summary>
		/// <remarks>
		/// Create a snapshot for /test/foo and create another snapshot for
		/// /test/foo/bar.  Files created before the snapshots should appear in both
		/// snapshots and the files created after the snapshots should not appear in
		/// any of the snapshots.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestNestedSnapshots()
		{
			cluster.GetNamesystem().GetSnapshotManager().SetAllowNestedSnapshots(true);
			Path foo = new Path("/testNestedSnapshots/foo");
			Path bar = new Path(foo, "bar");
			Path file1 = new Path(bar, "file1");
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, Seed);
			Print("create file " + file1);
			string s1name = "foo-s1";
			Path s1path = SnapshotTestHelper.GetSnapshotRoot(foo, s1name);
			hdfs.AllowSnapshot(foo);
			Print("allow snapshot " + foo);
			hdfs.CreateSnapshot(foo, s1name);
			Print("create snapshot " + s1name);
			string s2name = "bar-s2";
			Path s2path = SnapshotTestHelper.GetSnapshotRoot(bar, s2name);
			hdfs.AllowSnapshot(bar);
			Print("allow snapshot " + bar);
			hdfs.CreateSnapshot(bar, s2name);
			Print("create snapshot " + s2name);
			Path file2 = new Path(bar, "file2");
			DFSTestUtil.CreateFile(hdfs, file2, Blocksize, Replication, Seed);
			Print("create file " + file2);
			AssertFile(s1path, s2path, file1, true, true, true);
			AssertFile(s1path, s2path, file2, true, false, false);
			//test root
			string rootStr = "/";
			Path rootPath = new Path(rootStr);
			hdfs.AllowSnapshot(rootPath);
			Print("allow snapshot " + rootStr);
			Path rootSnapshot = hdfs.CreateSnapshot(rootPath);
			Print("create snapshot " + rootSnapshot);
			hdfs.DeleteSnapshot(rootPath, rootSnapshot.GetName());
			Print("delete snapshot " + rootSnapshot);
			hdfs.DisallowSnapshot(rootPath);
			Print("disallow snapshot " + rootStr);
			//change foo to non-snapshottable
			hdfs.DeleteSnapshot(foo, s1name);
			hdfs.DisallowSnapshot(foo);
			//test disallow nested snapshots
			cluster.GetNamesystem().GetSnapshotManager().SetAllowNestedSnapshots(false);
			try
			{
				hdfs.AllowSnapshot(rootPath);
				NUnit.Framework.Assert.Fail();
			}
			catch (SnapshotException se)
			{
				AssertNestedSnapshotException(se, "subdirectory");
			}
			try
			{
				hdfs.AllowSnapshot(foo);
				NUnit.Framework.Assert.Fail();
			}
			catch (SnapshotException se)
			{
				AssertNestedSnapshotException(se, "subdirectory");
			}
			Path sub1Bar = new Path(bar, "sub1");
			Path sub2Bar = new Path(sub1Bar, "sub2");
			hdfs.Mkdirs(sub2Bar);
			try
			{
				hdfs.AllowSnapshot(sub1Bar);
				NUnit.Framework.Assert.Fail();
			}
			catch (SnapshotException se)
			{
				AssertNestedSnapshotException(se, "ancestor");
			}
			try
			{
				hdfs.AllowSnapshot(sub2Bar);
				NUnit.Framework.Assert.Fail();
			}
			catch (SnapshotException se)
			{
				AssertNestedSnapshotException(se, "ancestor");
			}
		}

		internal static void AssertNestedSnapshotException(SnapshotException se, string substring
			)
		{
			NUnit.Framework.Assert.IsTrue(se.Message.StartsWith("Nested snapshottable directories not allowed"
				));
			NUnit.Framework.Assert.IsTrue(se.Message.Contains(substring));
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		private static void Print(string message)
		{
			SnapshotTestHelper.DumpTree(message, cluster);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void AssertFile(Path s1, Path s2, Path file, params bool[] expected
			)
		{
			Path[] paths = new Path[] { file, new Path(s1, "bar/" + file.GetName()), new Path
				(s2, file.GetName()) };
			NUnit.Framework.Assert.AreEqual(expected.Length, paths.Length);
			for (int i = 0; i < paths.Length; i++)
			{
				bool computed = hdfs.Exists(paths[i]);
				NUnit.Framework.Assert.AreEqual("Failed on " + paths[i], expected[i], computed);
			}
		}

		/// <summary>Test the snapshot limit of a single snapshottable directory.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotLimit()
		{
			int step = 1000;
			string dirStr = "/testSnapshotLimit/dir";
			Path dir = new Path(dirStr);
			hdfs.Mkdirs(dir, new FsPermission((short)0x1ff));
			hdfs.AllowSnapshot(dir);
			int s = 0;
			for (; s < DirectorySnapshottableFeature.SnapshotLimit; s++)
			{
				string snapshotName = "s" + s;
				hdfs.CreateSnapshot(dir, snapshotName);
				//create a file occasionally 
				if (s % step == 0)
				{
					Path file = new Path(dirStr, "f" + s);
					DFSTestUtil.CreateFile(hdfs, file, Blocksize, Replication, Seed);
				}
			}
			try
			{
				hdfs.CreateSnapshot(dir, "s" + s);
				NUnit.Framework.Assert.Fail("Expected to fail to create snapshot, but didn't.");
			}
			catch (IOException ioe)
			{
				SnapshotTestHelper.Log.Info("The exception is expected.", ioe);
			}
			for (int f = 0; f < DirectorySnapshottableFeature.SnapshotLimit; f += step)
			{
				string file = "f" + f;
				s = Random.Next(step);
				for (; s < DirectorySnapshottableFeature.SnapshotLimit; s += Random.Next(step))
				{
					Path p = SnapshotTestHelper.GetSnapshotPath(dir, "s" + s, file);
					//the file #f exists in snapshot #s iff s > f.
					NUnit.Framework.Assert.AreEqual(s > f, hdfs.Exists(p));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotName()
		{
			string dirStr = "/testSnapshotWithQuota/dir";
			Path dir = new Path(dirStr);
			hdfs.Mkdirs(dir, new FsPermission((short)0x1ff));
			hdfs.AllowSnapshot(dir);
			// set namespace quota
			int NsQuota = 6;
			hdfs.SetQuota(dir, NsQuota, HdfsConstants.QuotaDontSet);
			// create object to use up the quota.
			Path foo = new Path(dir, "foo");
			Path f1 = new Path(foo, "f1");
			DFSTestUtil.CreateFile(hdfs, f1, Blocksize, Replication, Seed);
			{
				//create a snapshot with default snapshot name
				Path snapshotPath = hdfs.CreateSnapshot(dir);
				//check snapshot path and the default snapshot name
				string snapshotName = snapshotPath.GetName();
				NUnit.Framework.Assert.IsTrue("snapshotName=" + snapshotName, Sharpen.Pattern.Matches
					("s\\d\\d\\d\\d\\d\\d\\d\\d-\\d\\d\\d\\d\\d\\d\\.\\d\\d\\d", snapshotName));
				Path parent = snapshotPath.GetParent();
				NUnit.Framework.Assert.AreEqual(HdfsConstants.DotSnapshotDir, parent.GetName());
				NUnit.Framework.Assert.AreEqual(dir, parent.GetParent());
			}
		}

		/// <summary>
		/// Test
		/// <see cref="Snapshot.IdComparator"/>
		/// .
		/// </summary>
		public virtual void TestIdCmp()
		{
			PermissionStatus perm = PermissionStatus.CreateImmutable("user", "group", FsPermission
				.CreateImmutable((short)0));
			INodeDirectory snapshottable = new INodeDirectory(0, DFSUtil.String2Bytes("foo"), 
				perm, 0L);
			snapshottable.AddSnapshottableFeature();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot[] snapshots = new Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				[] { new Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot(1, "s1", snapshottable
				), new Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot(1, "s1", snapshottable
				), new Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot(2, "s2", snapshottable
				), new Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot(2, "s2", snapshottable
				) };
			NUnit.Framework.Assert.AreEqual(0, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.IdComparator.Compare(null, null));
			foreach (Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s in snapshots)
			{
				NUnit.Framework.Assert.IsTrue(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.IdComparator.Compare(null, s) > 0);
				NUnit.Framework.Assert.IsTrue(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.IdComparator.Compare(s, null) < 0);
				foreach (Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot t in snapshots)
				{
					int expected = string.CompareOrdinal(s.GetRoot().GetLocalName(), t.GetRoot().GetLocalName
						());
					int computed = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.IdComparator
						.Compare(s, t);
					NUnit.Framework.Assert.AreEqual(expected > 0, computed > 0);
					NUnit.Framework.Assert.AreEqual(expected == 0, computed == 0);
					NUnit.Framework.Assert.AreEqual(expected < 0, computed < 0);
				}
			}
		}

		/// <summary>
		/// When we have nested snapshottable directories and if we try to reset the
		/// snapshottable descendant back to an regular directory, we need to replace
		/// the snapshottable descendant with an INodeDirectoryWithSnapshot
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDisallowNestedSnapshottableDir()
		{
			cluster.GetNamesystem().GetSnapshotManager().SetAllowNestedSnapshots(true);
			Path dir = new Path("/dir");
			Path sub = new Path(dir, "sub");
			hdfs.Mkdirs(sub);
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s1");
			Path file = new Path(sub, "file");
			DFSTestUtil.CreateFile(hdfs, file, Blocksize, Replication, Seed);
			FSDirectory fsdir = cluster.GetNamesystem().GetFSDirectory();
			INode subNode = fsdir.GetINode(sub.ToString());
			NUnit.Framework.Assert.IsTrue(subNode.AsDirectory().IsWithSnapshot());
			hdfs.AllowSnapshot(sub);
			subNode = fsdir.GetINode(sub.ToString());
			NUnit.Framework.Assert.IsTrue(subNode.IsDirectory() && subNode.AsDirectory().IsSnapshottable
				());
			hdfs.DisallowSnapshot(sub);
			subNode = fsdir.GetINode(sub.ToString());
			NUnit.Framework.Assert.IsTrue(subNode.AsDirectory().IsWithSnapshot());
		}
	}
}
