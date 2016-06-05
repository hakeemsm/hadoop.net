using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Test;
using Org.Mockito;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Testing rename with snapshots.</summary>
	public class TestRenameWithSnapshots
	{
		static TestRenameWithSnapshots()
		{
			SnapshotTestHelper.DisableLogs();
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(TestRenameWithSnapshots
			));

		private const long Seed = 0;

		private const short Repl = 3;

		private const short Repl1 = 2;

		private const short Repl2 = 1;

		private const long Blocksize = 1024;

		private static readonly Configuration conf = new Configuration();

		private static MiniDFSCluster cluster;

		private static FSNamesystem fsn;

		private static FSDirectory fsdir;

		private static DistributedFileSystem hdfs;

		private static readonly string testDir = Runtime.GetProperty("test.build.data", "build/test/data"
			);

		private static readonly Path dir = new Path("/testRenameWithSnapshots");

		private static readonly Path sub1 = new Path(dir, "sub1");

		private static readonly Path file1 = new Path(sub1, "file1");

		private static readonly Path file2 = new Path(sub1, "file2");

		private static readonly Path file3 = new Path(sub1, "file3");

		private const string snap1 = "snap1";

		private const string snap2 = "snap2";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, Blocksize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Repl).Format(true).Build(
				);
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			fsdir = fsn.GetFSDirectory();
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

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFromSDir2NonSDir()
		{
			string dirStr = "/testRenameWithSnapshot";
			string abcStr = dirStr + "/abc";
			Path abc = new Path(abcStr);
			hdfs.Mkdirs(abc, new FsPermission((short)0x1ff));
			hdfs.AllowSnapshot(abc);
			Path foo = new Path(abc, "foo");
			DFSTestUtil.CreateFile(hdfs, foo, Blocksize, Repl, Seed);
			hdfs.CreateSnapshot(abc, "s0");
			try
			{
				hdfs.Rename(abc, new Path(dirStr, "tmp"));
				NUnit.Framework.Assert.Fail("Expect exception since " + abc + " is snapshottable and already has snapshots"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains(abcStr + " is snapshottable and already has snapshots"
					, e);
			}
			string xyzStr = dirStr + "/xyz";
			Path xyz = new Path(xyzStr);
			hdfs.Mkdirs(xyz, new FsPermission((short)0x1ff));
			Path bar = new Path(xyz, "bar");
			hdfs.Rename(foo, bar);
			INode fooRef = fsdir.GetINode(SnapshotTestHelper.GetSnapshotPath(abc, "s0", "foo"
				).ToString());
			NUnit.Framework.Assert.IsTrue(fooRef.IsReference());
			NUnit.Framework.Assert.IsTrue(fooRef.AsReference() is INodeReference.WithName);
			INodeReference.WithCount withCount = (INodeReference.WithCount)fooRef.AsReference
				().GetReferredINode();
			NUnit.Framework.Assert.AreEqual(2, withCount.GetReferenceCount());
			INode barRef = fsdir.GetINode(bar.ToString());
			NUnit.Framework.Assert.IsTrue(barRef.IsReference());
			NUnit.Framework.Assert.AreSame(withCount, barRef.AsReference().GetReferredINode()
				);
			hdfs.Delete(bar, false);
			NUnit.Framework.Assert.AreEqual(1, withCount.GetReferenceCount());
		}

		private static bool ExistsInDiffReport(IList<SnapshotDiffReport.DiffReportEntry> 
			entries, SnapshotDiffReport.DiffType type, string sourcePath, string targetPath)
		{
			foreach (SnapshotDiffReport.DiffReportEntry entry in entries)
			{
				if (entry.Equals(new SnapshotDiffReport.DiffReportEntry(type, DFSUtil.String2Bytes
					(sourcePath), targetPath == null ? null : DFSUtil.String2Bytes(targetPath))))
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Rename a file under a snapshottable directory, file does not exist
		/// in a snapshot.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFileNotInSnapshot()
		{
			hdfs.Mkdirs(sub1);
			hdfs.AllowSnapshot(sub1);
			hdfs.CreateSnapshot(sub1, snap1);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Repl, Seed);
			hdfs.Rename(file1, file2);
			// Query the diff report and make sure it looks as expected.
			SnapshotDiffReport diffReport = hdfs.GetSnapshotDiffReport(sub1, snap1, string.Empty
				);
			IList<SnapshotDiffReport.DiffReportEntry> entries = diffReport.GetDiffList();
			NUnit.Framework.Assert.IsTrue(entries.Count == 2);
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Modify, string.Empty, null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Create, file2.GetName(), null));
		}

		/// <summary>
		/// Rename a file under a snapshottable directory, file exists
		/// in a snapshot.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameFileInSnapshot()
		{
			hdfs.Mkdirs(sub1);
			hdfs.AllowSnapshot(sub1);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Repl, Seed);
			hdfs.CreateSnapshot(sub1, snap1);
			hdfs.Rename(file1, file2);
			// Query the diff report and make sure it looks as expected.
			SnapshotDiffReport diffReport = hdfs.GetSnapshotDiffReport(sub1, snap1, string.Empty
				);
			System.Console.Out.WriteLine("DiffList is " + diffReport.ToString());
			IList<SnapshotDiffReport.DiffReportEntry> entries = diffReport.GetDiffList();
			NUnit.Framework.Assert.IsTrue(entries.Count == 2);
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Modify, string.Empty, null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Rename, file1.GetName(), file2.GetName()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameTwiceInSnapshot()
		{
			hdfs.Mkdirs(sub1);
			hdfs.AllowSnapshot(sub1);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Repl, Seed);
			hdfs.CreateSnapshot(sub1, snap1);
			hdfs.Rename(file1, file2);
			hdfs.CreateSnapshot(sub1, snap2);
			hdfs.Rename(file2, file3);
			SnapshotDiffReport diffReport;
			// Query the diff report and make sure it looks as expected.
			diffReport = hdfs.GetSnapshotDiffReport(sub1, snap1, snap2);
			Log.Info("DiffList is " + diffReport.ToString());
			IList<SnapshotDiffReport.DiffReportEntry> entries = diffReport.GetDiffList();
			NUnit.Framework.Assert.IsTrue(entries.Count == 2);
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Modify, string.Empty, null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Rename, file1.GetName(), file2.GetName()));
			diffReport = hdfs.GetSnapshotDiffReport(sub1, snap2, string.Empty);
			Log.Info("DiffList is " + diffReport.ToString());
			entries = diffReport.GetDiffList();
			NUnit.Framework.Assert.IsTrue(entries.Count == 2);
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Modify, string.Empty, null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Rename, file2.GetName(), file3.GetName()));
			diffReport = hdfs.GetSnapshotDiffReport(sub1, snap1, string.Empty);
			Log.Info("DiffList is " + diffReport.ToString());
			entries = diffReport.GetDiffList();
			NUnit.Framework.Assert.IsTrue(entries.Count == 2);
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Modify, string.Empty, null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Rename, file1.GetName(), file3.GetName()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFileInSubDirOfDirWithSnapshot()
		{
			Path sub2 = new Path(sub1, "sub2");
			Path sub2file1 = new Path(sub2, "sub2file1");
			Path sub2file2 = new Path(sub2, "sub2file2");
			string sub1snap1 = "sub1snap1";
			hdfs.Mkdirs(sub1);
			hdfs.Mkdirs(sub2);
			DFSTestUtil.CreateFile(hdfs, sub2file1, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, sub1snap1);
			// Rename the file in the subdirectory.
			hdfs.Rename(sub2file1, sub2file2);
			// Query the diff report and make sure it looks as expected.
			SnapshotDiffReport diffReport = hdfs.GetSnapshotDiffReport(sub1, sub1snap1, string.Empty
				);
			Log.Info("DiffList is \n\"" + diffReport.ToString() + "\"");
			IList<SnapshotDiffReport.DiffReportEntry> entries = diffReport.GetDiffList();
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Modify, sub2.GetName(), null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Rename, sub2.GetName() + "/" + sub2file1.GetName(), sub2.GetName() + "/" + sub2file2
				.GetName()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRenameDirectoryInSnapshot()
		{
			Path sub2 = new Path(sub1, "sub2");
			Path sub3 = new Path(sub1, "sub3");
			Path sub2file1 = new Path(sub2, "sub2file1");
			string sub1snap1 = "sub1snap1";
			hdfs.Mkdirs(sub1);
			hdfs.Mkdirs(sub2);
			DFSTestUtil.CreateFile(hdfs, sub2file1, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, sub1snap1);
			// First rename the sub-directory.
			hdfs.Rename(sub2, sub3);
			// Query the diff report and make sure it looks as expected.
			SnapshotDiffReport diffReport = hdfs.GetSnapshotDiffReport(sub1, sub1snap1, string.Empty
				);
			Log.Info("DiffList is \n\"" + diffReport.ToString() + "\"");
			IList<SnapshotDiffReport.DiffReportEntry> entries = diffReport.GetDiffList();
			NUnit.Framework.Assert.AreEqual(2, entries.Count);
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Modify, string.Empty, null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Rename, sub2.GetName(), sub3.GetName()));
		}

		/// <summary>
		/// After the following steps:
		/// <pre>
		/// 1.
		/// </summary>
		/// <remarks>
		/// After the following steps:
		/// <pre>
		/// 1. Take snapshot s1 on /dir1 at time t1.
		/// 2. Take snapshot s2 on /dir2 at time t2.
		/// 3. Modify the subtree of /dir2/foo/ to make it a dir with snapshots.
		/// 4. Take snapshot s3 on /dir1 at time t3.
		/// 5. Rename /dir2/foo/ to /dir1/foo/.
		/// </pre>
		/// When changes happening on foo, the diff should be recorded in snapshot s2.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestRenameDirAcrossSnapshottableDirs()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			Path foo = new Path(sdir2, "foo");
			Path bar = new Path(foo, "bar");
			Path bar2 = new Path(foo, "bar2");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			DFSTestUtil.CreateFile(hdfs, bar2, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			hdfs.SetReplication(bar2, Repl1);
			hdfs.Delete(bar, true);
			hdfs.CreateSnapshot(sdir1, "s3");
			Path newfoo = new Path(sdir1, "foo");
			hdfs.Rename(foo, newfoo);
			// still can visit the snapshot copy of bar through 
			// /dir2/.snapshot/s2/foo/bar
			Path snapshotBar = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo/bar");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(snapshotBar));
			// delete bar2
			Path newBar2 = new Path(newfoo, "bar2");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(newBar2));
			hdfs.Delete(newBar2, true);
			// /dir2/.snapshot/s2/foo/bar2 should still work
			Path bar2_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo/bar2");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar2_s2));
			FileStatus status = hdfs.GetFileStatus(bar2_s2);
			NUnit.Framework.Assert.AreEqual(Repl, status.GetReplication());
			Path bar2_s3 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s3", "foo/bar2");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s3));
		}

		/// <summary>Rename a single file across snapshottable dirs.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFileAcrossSnapshottableDirs()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			Path foo = new Path(sdir2, "foo");
			DFSTestUtil.CreateFile(hdfs, foo, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			hdfs.CreateSnapshot(sdir1, "s3");
			Path newfoo = new Path(sdir1, "foo");
			hdfs.Rename(foo, newfoo);
			// change the replication factor of foo
			hdfs.SetReplication(newfoo, Repl1);
			// /dir2/.snapshot/s2/foo should still work
			Path foo_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(foo_s2));
			FileStatus status = hdfs.GetFileStatus(foo_s2);
			NUnit.Framework.Assert.AreEqual(Repl, status.GetReplication());
			Path foo_s3 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s3", "foo");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(foo_s3));
			INodeDirectory sdir2Node = fsdir.GetINode(sdir2.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s2 = sdir2Node.GetSnapshot
				(DFSUtil.String2Bytes("s2"));
			INodeFile sfoo = fsdir.GetINode(newfoo.ToString()).AsFile();
			NUnit.Framework.Assert.AreEqual(s2.GetId(), sfoo.GetDiffs().GetLastSnapshotId());
		}

		/// <summary>Test renaming a dir and then delete snapshots.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirAndDeleteSnapshot_1()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			Path foo = new Path(sdir2, "foo");
			Path bar = new Path(foo, "bar");
			Path bar2 = new Path(foo, "bar2");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			DFSTestUtil.CreateFile(hdfs, bar2, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			hdfs.CreateSnapshot(sdir1, "s3");
			Path newfoo = new Path(sdir1, "foo");
			hdfs.Rename(foo, newfoo);
			Path newbar = new Path(newfoo, bar.GetName());
			Path newbar2 = new Path(newfoo, bar2.GetName());
			Path newbar3 = new Path(newfoo, "bar3");
			DFSTestUtil.CreateFile(hdfs, newbar3, Blocksize, Repl, Seed);
			hdfs.CreateSnapshot(sdir1, "s4");
			hdfs.Delete(newbar, true);
			hdfs.Delete(newbar3, true);
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(newbar3));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar));
			Path bar_s4 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s4", "foo/bar");
			Path bar3_s4 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s4", "foo/bar3");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s4));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar3_s4));
			hdfs.CreateSnapshot(sdir1, "s5");
			hdfs.Delete(newbar2, true);
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2));
			Path bar2_s5 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s5", "foo/bar2");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar2_s5));
			// delete snapshot s5. The diff of s5 should be combined to s4
			hdfs.DeleteSnapshot(sdir1, "s5");
			RestartClusterAndCheckImage(true);
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s5));
			Path bar2_s4 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s4", "foo/bar2");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar2_s4));
			// delete snapshot s4. The diff of s4 should be combined to s2 instead of
			// s3.
			hdfs.DeleteSnapshot(sdir1, "s4");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar_s4));
			Path bar_s3 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s3", "foo/bar");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar_s3));
			bar_s3 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s3", "foo/bar");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar_s3));
			Path bar_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo/bar");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s2));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s4));
			Path bar2_s3 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s3", "foo/bar2");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s3));
			bar2_s3 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s3", "foo/bar2");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s3));
			Path bar2_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo/bar2");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar2_s2));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar3_s4));
			Path bar3_s3 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s3", "foo/bar3");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar3_s3));
			bar3_s3 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s3", "foo/bar3");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar3_s3));
			Path bar3_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo/bar3");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar3_s2));
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// delete snapshot s2.
			hdfs.DeleteSnapshot(sdir2, "s2");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar_s2));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s2));
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			hdfs.DeleteSnapshot(sdir1, "s3");
			RestartClusterAndCheckImage(true);
			hdfs.DeleteSnapshot(sdir1, "s1");
			RestartClusterAndCheckImage(true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RestartClusterAndCheckImage(bool compareQuota)
		{
			FilePath fsnBefore = new FilePath(testDir, "dumptree_before");
			FilePath fsnMiddle = new FilePath(testDir, "dumptree_middle");
			FilePath fsnAfter = new FilePath(testDir, "dumptree_after");
			SnapshotTestHelper.DumpTree2File(fsdir, fsnBefore);
			cluster.Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(Repl).Build
				();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			fsdir = fsn.GetFSDirectory();
			hdfs = cluster.GetFileSystem();
			// later check fsnMiddle to see if the edit log is applied correctly 
			SnapshotTestHelper.DumpTree2File(fsdir, fsnMiddle);
			// save namespace and restart cluster
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			hdfs.SaveNamespace();
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			cluster.Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(Repl).Build
				();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			fsdir = fsn.GetFSDirectory();
			hdfs = cluster.GetFileSystem();
			// dump the namespace loaded from fsimage
			SnapshotTestHelper.DumpTree2File(fsdir, fsnAfter);
			SnapshotTestHelper.CompareDumpedTreeInFile(fsnBefore, fsnMiddle, compareQuota);
			SnapshotTestHelper.CompareDumpedTreeInFile(fsnBefore, fsnAfter, compareQuota);
		}

		/// <summary>Test renaming a file and then delete snapshots.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameFileAndDeleteSnapshot()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			Path foo = new Path(sdir2, "foo");
			DFSTestUtil.CreateFile(hdfs, foo, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			hdfs.CreateSnapshot(sdir1, "s3");
			Path newfoo = new Path(sdir1, "foo");
			hdfs.Rename(foo, newfoo);
			hdfs.SetReplication(newfoo, Repl1);
			hdfs.CreateSnapshot(sdir1, "s4");
			hdfs.SetReplication(newfoo, Repl2);
			FileStatus status = hdfs.GetFileStatus(newfoo);
			NUnit.Framework.Assert.AreEqual(Repl2, status.GetReplication());
			Path foo_s4 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s4", "foo");
			status = hdfs.GetFileStatus(foo_s4);
			NUnit.Framework.Assert.AreEqual(Repl1, status.GetReplication());
			hdfs.CreateSnapshot(sdir1, "s5");
			Path foo_s5 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s5", "foo");
			status = hdfs.GetFileStatus(foo_s5);
			NUnit.Framework.Assert.AreEqual(Repl2, status.GetReplication());
			// delete snapshot s5.
			hdfs.DeleteSnapshot(sdir1, "s5");
			RestartClusterAndCheckImage(true);
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(foo_s5));
			status = hdfs.GetFileStatus(foo_s4);
			NUnit.Framework.Assert.AreEqual(Repl1, status.GetReplication());
			// delete snapshot s4.
			hdfs.DeleteSnapshot(sdir1, "s4");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(foo_s4));
			Path foo_s3 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s3", "foo");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(foo_s3));
			foo_s3 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s3", "foo");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(foo_s3));
			Path foo_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(foo_s2));
			status = hdfs.GetFileStatus(foo_s2);
			NUnit.Framework.Assert.AreEqual(Repl, status.GetReplication());
			INodeFile snode = fsdir.GetINode(newfoo.ToString()).AsFile();
			NUnit.Framework.Assert.AreEqual(1, snode.GetDiffs().AsList().Count);
			INodeDirectory sdir2Node = fsdir.GetINode(sdir2.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s2 = sdir2Node.GetSnapshot
				(DFSUtil.String2Bytes("s2"));
			NUnit.Framework.Assert.AreEqual(s2.GetId(), snode.GetDiffs().GetLastSnapshotId());
			// restart cluster
			RestartClusterAndCheckImage(true);
			// delete snapshot s2.
			hdfs.DeleteSnapshot(sdir2, "s2");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(foo_s2));
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			hdfs.DeleteSnapshot(sdir1, "s3");
			RestartClusterAndCheckImage(true);
			hdfs.DeleteSnapshot(sdir1, "s1");
			RestartClusterAndCheckImage(true);
		}

		/// <summary>
		/// Test rename a dir and a file multiple times across snapshottable
		/// directories: /dir1/foo -&gt; /dir2/foo -&gt; /dir3/foo -&gt; /dir2/foo -&gt; /dir1/foo
		/// Only create snapshots in the beginning (before the rename).
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameMoreThanOnceAcrossSnapDirs()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			Path sdir3 = new Path("/dir3");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			hdfs.Mkdirs(sdir3);
			Path foo_dir1 = new Path(sdir1, "foo");
			Path bar1_dir1 = new Path(foo_dir1, "bar1");
			Path bar2_dir1 = new Path(sdir1, "bar");
			DFSTestUtil.CreateFile(hdfs, bar1_dir1, Blocksize, Repl, Seed);
			DFSTestUtil.CreateFile(hdfs, bar2_dir1, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir3, "s3");
			// 1. /dir1/foo -> /dir2/foo, /dir1/bar -> /dir2/bar
			Path foo_dir2 = new Path(sdir2, "foo");
			hdfs.Rename(foo_dir1, foo_dir2);
			Path bar2_dir2 = new Path(sdir2, "bar");
			hdfs.Rename(bar2_dir1, bar2_dir2);
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// modification on /dir2/foo and /dir2/bar
			Path bar1_dir2 = new Path(foo_dir2, "bar1");
			hdfs.SetReplication(bar1_dir2, Repl1);
			hdfs.SetReplication(bar2_dir2, Repl1);
			// check
			Path bar1_s1 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1", "foo/bar1");
			Path bar2_s1 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1", "bar");
			Path bar1_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo/bar1");
			Path bar2_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "bar");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s1));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar2_s1));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar1_s2));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s2));
			FileStatus statusBar1 = hdfs.GetFileStatus(bar1_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar1.GetReplication());
			statusBar1 = hdfs.GetFileStatus(bar1_dir2);
			NUnit.Framework.Assert.AreEqual(Repl1, statusBar1.GetReplication());
			FileStatus statusBar2 = hdfs.GetFileStatus(bar2_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar2.GetReplication());
			statusBar2 = hdfs.GetFileStatus(bar2_dir2);
			NUnit.Framework.Assert.AreEqual(Repl1, statusBar2.GetReplication());
			// 2. /dir2/foo -> /dir3/foo, /dir2/bar -> /dir3/bar
			Path foo_dir3 = new Path(sdir3, "foo");
			hdfs.Rename(foo_dir2, foo_dir3);
			Path bar2_dir3 = new Path(sdir3, "bar");
			hdfs.Rename(bar2_dir2, bar2_dir3);
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// modification on /dir3/foo and /dir3/bar
			Path bar1_dir3 = new Path(foo_dir3, "bar1");
			hdfs.SetReplication(bar1_dir3, Repl2);
			hdfs.SetReplication(bar2_dir3, Repl2);
			// check
			Path bar1_s3 = SnapshotTestHelper.GetSnapshotPath(sdir3, "s3", "foo/bar1");
			Path bar2_s3 = SnapshotTestHelper.GetSnapshotPath(sdir3, "s3", "bar");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s1));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar2_s1));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar1_s2));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s2));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar1_s3));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s3));
			statusBar1 = hdfs.GetFileStatus(bar1_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar1.GetReplication());
			statusBar1 = hdfs.GetFileStatus(bar1_dir3);
			NUnit.Framework.Assert.AreEqual(Repl2, statusBar1.GetReplication());
			statusBar2 = hdfs.GetFileStatus(bar2_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar2.GetReplication());
			statusBar2 = hdfs.GetFileStatus(bar2_dir3);
			NUnit.Framework.Assert.AreEqual(Repl2, statusBar2.GetReplication());
			// 3. /dir3/foo -> /dir2/foo, /dir3/bar -> /dir2/bar
			hdfs.Rename(foo_dir3, foo_dir2);
			hdfs.Rename(bar2_dir3, bar2_dir2);
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// modification on /dir2/foo
			hdfs.SetReplication(bar1_dir2, Repl);
			hdfs.SetReplication(bar2_dir2, Repl);
			// check
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s1));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar2_s1));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar1_s2));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s2));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar1_s3));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s3));
			statusBar1 = hdfs.GetFileStatus(bar1_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar1.GetReplication());
			statusBar1 = hdfs.GetFileStatus(bar1_dir2);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar1.GetReplication());
			statusBar2 = hdfs.GetFileStatus(bar2_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar2.GetReplication());
			statusBar2 = hdfs.GetFileStatus(bar2_dir2);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar2.GetReplication());
			// 4. /dir2/foo -> /dir1/foo, /dir2/bar -> /dir1/bar
			hdfs.Rename(foo_dir2, foo_dir1);
			hdfs.Rename(bar2_dir2, bar2_dir1);
			// check the internal details
			INodeReference fooRef = fsdir.GetINode4Write(foo_dir1.ToString()).AsReference();
			INodeReference.WithCount fooWithCount = (INodeReference.WithCount)fooRef.GetReferredINode
				();
			// only 2 references: one in deleted list of sdir1, one in created list of
			// sdir1
			NUnit.Framework.Assert.AreEqual(2, fooWithCount.GetReferenceCount());
			INodeDirectory foo = fooWithCount.AsDirectory();
			NUnit.Framework.Assert.AreEqual(1, foo.GetDiffs().AsList().Count);
			INodeDirectory sdir1Node = fsdir.GetINode(sdir1.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s1 = sdir1Node.GetSnapshot
				(DFSUtil.String2Bytes("s1"));
			NUnit.Framework.Assert.AreEqual(s1.GetId(), foo.GetDirectoryWithSnapshotFeature()
				.GetLastSnapshotId());
			INodeFile bar1 = fsdir.GetINode4Write(bar1_dir1.ToString()).AsFile();
			NUnit.Framework.Assert.AreEqual(1, bar1.GetDiffs().AsList().Count);
			NUnit.Framework.Assert.AreEqual(s1.GetId(), bar1.GetDiffs().GetLastSnapshotId());
			INodeReference barRef = fsdir.GetINode4Write(bar2_dir1.ToString()).AsReference();
			INodeReference.WithCount barWithCount = (INodeReference.WithCount)barRef.GetReferredINode
				();
			NUnit.Framework.Assert.AreEqual(2, barWithCount.GetReferenceCount());
			INodeFile bar = barWithCount.AsFile();
			NUnit.Framework.Assert.AreEqual(1, bar.GetDiffs().AsList().Count);
			NUnit.Framework.Assert.AreEqual(s1.GetId(), bar.GetDiffs().GetLastSnapshotId());
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// delete foo
			hdfs.Delete(foo_dir1, true);
			hdfs.Delete(bar2_dir1, true);
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// check
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s1));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar2_s1));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar1_s2));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s2));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar1_s3));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s3));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(foo_dir1));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar1_dir1));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_dir1));
			statusBar1 = hdfs.GetFileStatus(bar1_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar1.GetReplication());
			statusBar2 = hdfs.GetFileStatus(bar2_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar2.GetReplication());
			Path foo_s1 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1", "foo");
			fooRef = fsdir.GetINode(foo_s1.ToString()).AsReference();
			fooWithCount = (INodeReference.WithCount)fooRef.GetReferredINode();
			NUnit.Framework.Assert.AreEqual(1, fooWithCount.GetReferenceCount());
			barRef = fsdir.GetINode(bar2_s1.ToString()).AsReference();
			barWithCount = (INodeReference.WithCount)barRef.GetReferredINode();
			NUnit.Framework.Assert.AreEqual(1, barWithCount.GetReferenceCount());
		}

		/// <summary>
		/// Test rename a dir multiple times across snapshottable directories:
		/// /dir1/foo -&gt; /dir2/foo -&gt; /dir3/foo -&gt; /dir2/foo -&gt; /dir1/foo
		/// Create snapshots after each rename.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameMoreThanOnceAcrossSnapDirs_2()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			Path sdir3 = new Path("/dir3");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			hdfs.Mkdirs(sdir3);
			Path foo_dir1 = new Path(sdir1, "foo");
			Path bar1_dir1 = new Path(foo_dir1, "bar1");
			Path bar_dir1 = new Path(sdir1, "bar");
			DFSTestUtil.CreateFile(hdfs, bar1_dir1, Blocksize, Repl, Seed);
			DFSTestUtil.CreateFile(hdfs, bar_dir1, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir3, "s3");
			// 1. /dir1/foo -> /dir2/foo, /dir1/bar -> /dir2/bar
			Path foo_dir2 = new Path(sdir2, "foo");
			hdfs.Rename(foo_dir1, foo_dir2);
			Path bar_dir2 = new Path(sdir2, "bar");
			hdfs.Rename(bar_dir1, bar_dir2);
			// modification on /dir2/foo and /dir2/bar
			Path bar1_dir2 = new Path(foo_dir2, "bar1");
			hdfs.SetReplication(bar1_dir2, Repl1);
			hdfs.SetReplication(bar_dir2, Repl1);
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// create snapshots
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s11");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s22");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir3, "s33");
			// 2. /dir2/foo -> /dir3/foo
			Path foo_dir3 = new Path(sdir3, "foo");
			hdfs.Rename(foo_dir2, foo_dir3);
			Path bar_dir3 = new Path(sdir3, "bar");
			hdfs.Rename(bar_dir2, bar_dir3);
			// modification on /dir3/foo
			Path bar1_dir3 = new Path(foo_dir3, "bar1");
			hdfs.SetReplication(bar1_dir3, Repl2);
			hdfs.SetReplication(bar_dir3, Repl2);
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// create snapshots
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s111");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s222");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir3, "s333");
			// check
			Path bar1_s1 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1", "foo/bar1");
			Path bar1_s22 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s22", "foo/bar1");
			Path bar1_s333 = SnapshotTestHelper.GetSnapshotPath(sdir3, "s333", "foo/bar1");
			Path bar_s1 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1", "bar");
			Path bar_s22 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s22", "bar");
			Path bar_s333 = SnapshotTestHelper.GetSnapshotPath(sdir3, "s333", "bar");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s1));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s22));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s333));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s1));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s22));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s333));
			FileStatus statusBar1 = hdfs.GetFileStatus(bar1_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar1.GetReplication());
			statusBar1 = hdfs.GetFileStatus(bar1_dir3);
			NUnit.Framework.Assert.AreEqual(Repl2, statusBar1.GetReplication());
			statusBar1 = hdfs.GetFileStatus(bar1_s22);
			NUnit.Framework.Assert.AreEqual(Repl1, statusBar1.GetReplication());
			statusBar1 = hdfs.GetFileStatus(bar1_s333);
			NUnit.Framework.Assert.AreEqual(Repl2, statusBar1.GetReplication());
			FileStatus statusBar = hdfs.GetFileStatus(bar_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar.GetReplication());
			statusBar = hdfs.GetFileStatus(bar_dir3);
			NUnit.Framework.Assert.AreEqual(Repl2, statusBar.GetReplication());
			statusBar = hdfs.GetFileStatus(bar_s22);
			NUnit.Framework.Assert.AreEqual(Repl1, statusBar.GetReplication());
			statusBar = hdfs.GetFileStatus(bar_s333);
			NUnit.Framework.Assert.AreEqual(Repl2, statusBar.GetReplication());
			// 3. /dir3/foo -> /dir2/foo
			hdfs.Rename(foo_dir3, foo_dir2);
			hdfs.Rename(bar_dir3, bar_dir2);
			// modification on /dir2/foo
			hdfs.SetReplication(bar1_dir2, Repl);
			hdfs.SetReplication(bar_dir2, Repl);
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// create snapshots
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1111");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2222");
			// check
			Path bar1_s2222 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2222", "foo/bar1");
			Path bar_s2222 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2222", "bar");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s1));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s22));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s333));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s2222));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s1));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s22));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s333));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s2222));
			statusBar1 = hdfs.GetFileStatus(bar1_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar1.GetReplication());
			statusBar1 = hdfs.GetFileStatus(bar1_dir2);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar1.GetReplication());
			statusBar1 = hdfs.GetFileStatus(bar1_s22);
			NUnit.Framework.Assert.AreEqual(Repl1, statusBar1.GetReplication());
			statusBar1 = hdfs.GetFileStatus(bar1_s333);
			NUnit.Framework.Assert.AreEqual(Repl2, statusBar1.GetReplication());
			statusBar1 = hdfs.GetFileStatus(bar1_s2222);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar1.GetReplication());
			statusBar = hdfs.GetFileStatus(bar_s1);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar.GetReplication());
			statusBar = hdfs.GetFileStatus(bar_dir2);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar.GetReplication());
			statusBar = hdfs.GetFileStatus(bar_s22);
			NUnit.Framework.Assert.AreEqual(Repl1, statusBar.GetReplication());
			statusBar = hdfs.GetFileStatus(bar_s333);
			NUnit.Framework.Assert.AreEqual(Repl2, statusBar.GetReplication());
			statusBar = hdfs.GetFileStatus(bar_s2222);
			NUnit.Framework.Assert.AreEqual(Repl, statusBar.GetReplication());
			// 4. /dir2/foo -> /dir1/foo
			hdfs.Rename(foo_dir2, foo_dir1);
			hdfs.Rename(bar_dir2, bar_dir1);
			// check the internal details
			INodeDirectory sdir1Node = fsdir.GetINode(sdir1.ToString()).AsDirectory();
			INodeDirectory sdir2Node = fsdir.GetINode(sdir2.ToString()).AsDirectory();
			INodeDirectory sdir3Node = fsdir.GetINode(sdir3.ToString()).AsDirectory();
			INodeReference fooRef = fsdir.GetINode4Write(foo_dir1.ToString()).AsReference();
			INodeReference.WithCount fooWithCount = (INodeReference.WithCount)fooRef.GetReferredINode
				();
			// 5 references: s1, s22, s333, s2222, current tree of sdir1
			NUnit.Framework.Assert.AreEqual(5, fooWithCount.GetReferenceCount());
			INodeDirectory foo = fooWithCount.AsDirectory();
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> fooDiffs = foo.GetDiffs().AsList
				();
			NUnit.Framework.Assert.AreEqual(4, fooDiffs.Count);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s2222 = sdir2Node.GetSnapshot
				(DFSUtil.String2Bytes("s2222"));
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s333 = sdir3Node.GetSnapshot
				(DFSUtil.String2Bytes("s333"));
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s22 = sdir2Node.GetSnapshot
				(DFSUtil.String2Bytes("s22"));
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s1 = sdir1Node.GetSnapshot
				(DFSUtil.String2Bytes("s1"));
			NUnit.Framework.Assert.AreEqual(s2222.GetId(), fooDiffs[3].GetSnapshotId());
			NUnit.Framework.Assert.AreEqual(s333.GetId(), fooDiffs[2].GetSnapshotId());
			NUnit.Framework.Assert.AreEqual(s22.GetId(), fooDiffs[1].GetSnapshotId());
			NUnit.Framework.Assert.AreEqual(s1.GetId(), fooDiffs[0].GetSnapshotId());
			INodeFile bar1 = fsdir.GetINode4Write(bar1_dir1.ToString()).AsFile();
			IList<FileDiff> bar1Diffs = bar1.GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(3, bar1Diffs.Count);
			NUnit.Framework.Assert.AreEqual(s333.GetId(), bar1Diffs[2].GetSnapshotId());
			NUnit.Framework.Assert.AreEqual(s22.GetId(), bar1Diffs[1].GetSnapshotId());
			NUnit.Framework.Assert.AreEqual(s1.GetId(), bar1Diffs[0].GetSnapshotId());
			INodeReference barRef = fsdir.GetINode4Write(bar_dir1.ToString()).AsReference();
			INodeReference.WithCount barWithCount = (INodeReference.WithCount)barRef.GetReferredINode
				();
			// 5 references: s1, s22, s333, s2222, current tree of sdir1
			NUnit.Framework.Assert.AreEqual(5, barWithCount.GetReferenceCount());
			INodeFile bar = barWithCount.AsFile();
			IList<FileDiff> barDiffs = bar.GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(4, barDiffs.Count);
			NUnit.Framework.Assert.AreEqual(s2222.GetId(), barDiffs[3].GetSnapshotId());
			NUnit.Framework.Assert.AreEqual(s333.GetId(), barDiffs[2].GetSnapshotId());
			NUnit.Framework.Assert.AreEqual(s22.GetId(), barDiffs[1].GetSnapshotId());
			NUnit.Framework.Assert.AreEqual(s1.GetId(), barDiffs[0].GetSnapshotId());
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// delete foo
			hdfs.Delete(foo_dir1, true);
			hdfs.Delete(bar_dir1, true);
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// check
			Path bar1_s1111 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1111", "foo/bar1");
			Path bar_s1111 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1111", "bar");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s1));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s22));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s333));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar1_s2222));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar1_s1111));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s1));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s22));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s333));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s2222));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar_s1111));
			Path foo_s2222 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2222", "foo");
			fooRef = fsdir.GetINode(foo_s2222.ToString()).AsReference();
			fooWithCount = (INodeReference.WithCount)fooRef.GetReferredINode();
			NUnit.Framework.Assert.AreEqual(4, fooWithCount.GetReferenceCount());
			foo = fooWithCount.AsDirectory();
			fooDiffs = foo.GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(4, fooDiffs.Count);
			NUnit.Framework.Assert.AreEqual(s2222.GetId(), fooDiffs[3].GetSnapshotId());
			bar1Diffs = bar1.GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(3, bar1Diffs.Count);
			NUnit.Framework.Assert.AreEqual(s333.GetId(), bar1Diffs[2].GetSnapshotId());
			barRef = fsdir.GetINode(bar_s2222.ToString()).AsReference();
			barWithCount = (INodeReference.WithCount)barRef.GetReferredINode();
			NUnit.Framework.Assert.AreEqual(4, barWithCount.GetReferenceCount());
			bar = barWithCount.AsFile();
			barDiffs = bar.GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(4, barDiffs.Count);
			NUnit.Framework.Assert.AreEqual(s2222.GetId(), barDiffs[3].GetSnapshotId());
		}

		/// <summary>Test rename from a non-snapshottable dir to a snapshottable dir</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRenameFromNonSDir2SDir()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, snap1);
			Path newfoo = new Path(sdir2, "foo");
			hdfs.Rename(foo, newfoo);
			INode fooNode = fsdir.GetINode4Write(newfoo.ToString());
			NUnit.Framework.Assert.IsTrue(fooNode is INodeDirectory);
		}

		/// <summary>
		/// Test rename where the src/dst directories are both snapshottable
		/// directories without snapshots.
		/// </summary>
		/// <remarks>
		/// Test rename where the src/dst directories are both snapshottable
		/// directories without snapshots. In such case we need to update the
		/// snapshottable dir list in SnapshotManager.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestRenameAndUpdateSnapshottableDirs()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(sdir2, "bar");
			hdfs.Mkdirs(foo);
			hdfs.Mkdirs(bar);
			hdfs.AllowSnapshot(foo);
			SnapshotTestHelper.CreateSnapshot(hdfs, bar, snap1);
			NUnit.Framework.Assert.AreEqual(2, fsn.GetSnapshottableDirListing().Length);
			INodeDirectory fooNode = fsdir.GetINode4Write(foo.ToString()).AsDirectory();
			long fooId = fooNode.GetId();
			try
			{
				hdfs.Rename(foo, bar, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Expect exception since " + bar + " is snapshottable and already has snapshots"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains(bar.ToString() + " is snapshottable and already has snapshots"
					, e);
			}
			hdfs.DeleteSnapshot(bar, snap1);
			hdfs.Rename(foo, bar, Options.Rename.Overwrite);
			SnapshottableDirectoryStatus[] dirs = fsn.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(1, dirs.Length);
			NUnit.Framework.Assert.AreEqual(bar, dirs[0].GetFullPath());
			NUnit.Framework.Assert.AreEqual(fooId, dirs[0].GetDirStatus().GetFileId());
		}

		/// <summary>After rename, delete the snapshot in src</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirAndDeleteSnapshot_2()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			Path foo = new Path(sdir2, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s3");
			Path newfoo = new Path(sdir1, "foo");
			hdfs.Rename(foo, newfoo);
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			Path bar2 = new Path(newfoo, "bar2");
			DFSTestUtil.CreateFile(hdfs, bar2, Blocksize, Repl, Seed);
			hdfs.CreateSnapshot(sdir1, "s4");
			hdfs.Delete(newfoo, true);
			Path bar2_s4 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s4", "foo/bar2");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar2_s4));
			Path bar_s4 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s4", "foo/bar");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s4));
			// delete snapshot s4. The diff of s4 should be combined to s3
			hdfs.DeleteSnapshot(sdir1, "s4");
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			Path bar_s3 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s3", "foo/bar");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar_s3));
			bar_s3 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s3", "foo/bar");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s3));
			Path bar2_s3 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s3", "foo/bar2");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s3));
			bar2_s3 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s3", "foo/bar2");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar2_s3));
			// delete snapshot s3
			hdfs.DeleteSnapshot(sdir2, "s3");
			Path bar_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo/bar");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar_s2));
			// check internal details
			INodeDirectory sdir2Node = fsdir.GetINode(sdir2.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s2 = sdir2Node.GetSnapshot
				(DFSUtil.String2Bytes("s2"));
			Path foo_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo");
			INodeReference fooRef = fsdir.GetINode(foo_s2.ToString()).AsReference();
			NUnit.Framework.Assert.IsTrue(fooRef is INodeReference.WithName);
			INodeReference.WithCount fooWC = (INodeReference.WithCount)fooRef.GetReferredINode
				();
			NUnit.Framework.Assert.AreEqual(1, fooWC.GetReferenceCount());
			INodeDirectory fooDir = fooWC.GetReferredINode().AsDirectory();
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffs = fooDir.GetDiffs().AsList
				();
			NUnit.Framework.Assert.AreEqual(1, diffs.Count);
			NUnit.Framework.Assert.AreEqual(s2.GetId(), diffs[0].GetSnapshotId());
			// restart the cluster and check fsimage
			RestartClusterAndCheckImage(true);
			// delete snapshot s2.
			hdfs.DeleteSnapshot(sdir2, "s2");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(bar_s2));
			RestartClusterAndCheckImage(true);
			// make sure the whole referred subtree has been destroyed
			QuotaCounts q = fsdir.GetRoot().GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			NUnit.Framework.Assert.AreEqual(3, q.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(0, q.GetStorageSpace());
			hdfs.DeleteSnapshot(sdir1, "s1");
			RestartClusterAndCheckImage(true);
			q = fsdir.GetRoot().GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			NUnit.Framework.Assert.AreEqual(3, q.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(0, q.GetStorageSpace());
		}

		/// <summary>Rename a file and then append the same file.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameAndAppend()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			Path foo = new Path(sdir1, "foo");
			DFSTestUtil.CreateFile(hdfs, foo, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, snap1);
			Path foo2 = new Path(sdir2, "foo");
			hdfs.Rename(foo, foo2);
			INode fooRef = fsdir.GetINode4Write(foo2.ToString());
			NUnit.Framework.Assert.IsTrue(fooRef is INodeReference.DstReference);
			FSDataOutputStream @out = hdfs.Append(foo2);
			try
			{
				byte[] content = new byte[1024];
				(new Random()).NextBytes(content);
				@out.Write(content);
				fooRef = fsdir.GetINode4Write(foo2.ToString());
				NUnit.Framework.Assert.IsTrue(fooRef is INodeReference.DstReference);
				INodeFile fooNode = fooRef.AsFile();
				NUnit.Framework.Assert.IsTrue(fooNode.IsWithSnapshot());
				NUnit.Framework.Assert.IsTrue(fooNode.IsUnderConstruction());
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
			}
			fooRef = fsdir.GetINode4Write(foo2.ToString());
			NUnit.Framework.Assert.IsTrue(fooRef is INodeReference.DstReference);
			INodeFile fooNode_1 = fooRef.AsFile();
			NUnit.Framework.Assert.IsTrue(fooNode_1.IsWithSnapshot());
			NUnit.Framework.Assert.IsFalse(fooNode_1.IsUnderConstruction());
			RestartClusterAndCheckImage(true);
		}

		/// <summary>Test the undo section of rename.</summary>
		/// <remarks>
		/// Test the undo section of rename. Before the rename, we create the renamed
		/// file/dir before taking the snapshot.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameUndo_1()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			Path dir2file = new Path(sdir2, "file");
			DFSTestUtil.CreateFile(hdfs, dir2file, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			INodeDirectory dir2 = fsdir.GetINode4Write(sdir2.ToString()).AsDirectory();
			INodeDirectory mockDir2 = Org.Mockito.Mockito.Spy(dir2);
			Org.Mockito.Mockito.DoReturn(false).When(mockDir2).AddChild((INode)Matchers.AnyObject
				(), Matchers.AnyBoolean(), Org.Mockito.Mockito.AnyInt());
			INodeDirectory root = fsdir.GetINode4Write("/").AsDirectory();
			root.ReplaceChild(dir2, mockDir2, fsdir.GetINodeMap());
			Path newfoo = new Path(sdir2, "foo");
			bool result = hdfs.Rename(foo, newfoo);
			NUnit.Framework.Assert.IsFalse(result);
			// check the current internal details
			INodeDirectory dir1Node = fsdir.GetINode4Write(sdir1.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s1 = dir1Node.GetSnapshot
				(DFSUtil.String2Bytes("s1"));
			ReadOnlyList<INode> dir1Children = dir1Node.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			NUnit.Framework.Assert.AreEqual(1, dir1Children.Size());
			NUnit.Framework.Assert.AreEqual(foo.GetName(), dir1Children.Get(0).GetLocalName()
				);
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> dir1Diffs = dir1Node.GetDiffs()
				.AsList();
			NUnit.Framework.Assert.AreEqual(1, dir1Diffs.Count);
			NUnit.Framework.Assert.AreEqual(s1.GetId(), dir1Diffs[0].GetSnapshotId());
			// after the undo of rename, both the created and deleted list of sdir1
			// should be empty
			DirectoryWithSnapshotFeature.ChildrenDiff childrenDiff = dir1Diffs[0].GetChildrenDiff
				();
			NUnit.Framework.Assert.AreEqual(0, childrenDiff.GetList(Diff.ListType.Deleted).Count
				);
			NUnit.Framework.Assert.AreEqual(0, childrenDiff.GetList(Diff.ListType.Created).Count
				);
			INode fooNode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fooNode.IsDirectory() && fooNode.AsDirectory().IsWithSnapshot
				());
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> fooDiffs = fooNode.AsDirectory(
				).GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(1, fooDiffs.Count);
			NUnit.Framework.Assert.AreEqual(s1.GetId(), fooDiffs[0].GetSnapshotId());
			Path foo_s1 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1", "foo");
			INode fooNode_s1 = fsdir.GetINode(foo_s1.ToString());
			NUnit.Framework.Assert.IsTrue(fooNode_s1 == fooNode);
			// check sdir2
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(newfoo));
			INodeDirectory dir2Node = fsdir.GetINode4Write(sdir2.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsFalse(dir2Node.IsWithSnapshot());
			ReadOnlyList<INode> dir2Children = dir2Node.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			NUnit.Framework.Assert.AreEqual(1, dir2Children.Size());
			NUnit.Framework.Assert.AreEqual(dir2file.GetName(), dir2Children.Get(0).GetLocalName
				());
		}

		/// <summary>Test the undo section of rename.</summary>
		/// <remarks>
		/// Test the undo section of rename. Before the rename, we create the renamed
		/// file/dir after taking the snapshot.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameUndo_2()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			Path dir2file = new Path(sdir2, "file");
			DFSTestUtil.CreateFile(hdfs, dir2file, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			// create foo after taking snapshot
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			INodeDirectory dir2 = fsdir.GetINode4Write(sdir2.ToString()).AsDirectory();
			INodeDirectory mockDir2 = Org.Mockito.Mockito.Spy(dir2);
			Org.Mockito.Mockito.DoReturn(false).When(mockDir2).AddChild((INode)Matchers.AnyObject
				(), Matchers.AnyBoolean(), Org.Mockito.Mockito.AnyInt());
			INodeDirectory root = fsdir.GetINode4Write("/").AsDirectory();
			root.ReplaceChild(dir2, mockDir2, fsdir.GetINodeMap());
			Path newfoo = new Path(sdir2, "foo");
			bool result = hdfs.Rename(foo, newfoo);
			NUnit.Framework.Assert.IsFalse(result);
			// check the current internal details
			INodeDirectory dir1Node = fsdir.GetINode4Write(sdir1.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s1 = dir1Node.GetSnapshot
				(DFSUtil.String2Bytes("s1"));
			ReadOnlyList<INode> dir1Children = dir1Node.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			NUnit.Framework.Assert.AreEqual(1, dir1Children.Size());
			NUnit.Framework.Assert.AreEqual(foo.GetName(), dir1Children.Get(0).GetLocalName()
				);
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> dir1Diffs = dir1Node.GetDiffs()
				.AsList();
			NUnit.Framework.Assert.AreEqual(1, dir1Diffs.Count);
			NUnit.Framework.Assert.AreEqual(s1.GetId(), dir1Diffs[0].GetSnapshotId());
			// after the undo of rename, the created list of sdir1 should contain 
			// 1 element
			DirectoryWithSnapshotFeature.ChildrenDiff childrenDiff = dir1Diffs[0].GetChildrenDiff
				();
			NUnit.Framework.Assert.AreEqual(0, childrenDiff.GetList(Diff.ListType.Deleted).Count
				);
			NUnit.Framework.Assert.AreEqual(1, childrenDiff.GetList(Diff.ListType.Created).Count
				);
			INode fooNode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fooNode is INodeDirectory);
			NUnit.Framework.Assert.IsTrue(childrenDiff.GetList(Diff.ListType.Created)[0] == fooNode
				);
			Path foo_s1 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1", "foo");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(foo_s1));
			// check sdir2
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(newfoo));
			INodeDirectory dir2Node = fsdir.GetINode4Write(sdir2.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsFalse(dir2Node.IsWithSnapshot());
			ReadOnlyList<INode> dir2Children = dir2Node.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			NUnit.Framework.Assert.AreEqual(1, dir2Children.Size());
			NUnit.Framework.Assert.AreEqual(dir2file.GetName(), dir2Children.Get(0).GetLocalName
				());
		}

		/// <summary>Test the undo section of the second-time rename.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameUndo_3()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			Path sdir3 = new Path("/dir3");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			hdfs.Mkdirs(sdir3);
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			INodeDirectory dir3 = fsdir.GetINode4Write(sdir3.ToString()).AsDirectory();
			INodeDirectory mockDir3 = Org.Mockito.Mockito.Spy(dir3);
			Org.Mockito.Mockito.DoReturn(false).When(mockDir3).AddChild((INode)Matchers.AnyObject
				(), Matchers.AnyBoolean(), Org.Mockito.Mockito.AnyInt());
			INodeDirectory root = fsdir.GetINode4Write("/").AsDirectory();
			root.ReplaceChild(dir3, mockDir3, fsdir.GetINodeMap());
			Path foo_dir2 = new Path(sdir2, "foo2");
			Path foo_dir3 = new Path(sdir3, "foo3");
			hdfs.Rename(foo, foo_dir2);
			bool result = hdfs.Rename(foo_dir2, foo_dir3);
			NUnit.Framework.Assert.IsFalse(result);
			// check the current internal details
			INodeDirectory dir1Node = fsdir.GetINode4Write(sdir1.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s1 = dir1Node.GetSnapshot
				(DFSUtil.String2Bytes("s1"));
			INodeDirectory dir2Node = fsdir.GetINode4Write(sdir2.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s2 = dir2Node.GetSnapshot
				(DFSUtil.String2Bytes("s2"));
			ReadOnlyList<INode> dir2Children = dir2Node.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			NUnit.Framework.Assert.AreEqual(1, dir2Children.Size());
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> dir2Diffs = dir2Node.GetDiffs()
				.AsList();
			NUnit.Framework.Assert.AreEqual(1, dir2Diffs.Count);
			NUnit.Framework.Assert.AreEqual(s2.GetId(), dir2Diffs[0].GetSnapshotId());
			DirectoryWithSnapshotFeature.ChildrenDiff childrenDiff = dir2Diffs[0].GetChildrenDiff
				();
			NUnit.Framework.Assert.AreEqual(0, childrenDiff.GetList(Diff.ListType.Deleted).Count
				);
			NUnit.Framework.Assert.AreEqual(1, childrenDiff.GetList(Diff.ListType.Created).Count
				);
			Path foo_s2 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s2", "foo2");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(foo_s2));
			INode fooNode = fsdir.GetINode4Write(foo_dir2.ToString());
			NUnit.Framework.Assert.IsTrue(childrenDiff.GetList(Diff.ListType.Created)[0] == fooNode
				);
			NUnit.Framework.Assert.IsTrue(fooNode is INodeReference.DstReference);
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> fooDiffs = fooNode.AsDirectory(
				).GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(1, fooDiffs.Count);
			NUnit.Framework.Assert.AreEqual(s1.GetId(), fooDiffs[0].GetSnapshotId());
			// create snapshot on sdir2 and rename again
			hdfs.CreateSnapshot(sdir2, "s3");
			result = hdfs.Rename(foo_dir2, foo_dir3);
			NUnit.Framework.Assert.IsFalse(result);
			// check internal details again
			dir2Node = fsdir.GetINode4Write(sdir2.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s3 = dir2Node.GetSnapshot
				(DFSUtil.String2Bytes("s3"));
			fooNode = fsdir.GetINode4Write(foo_dir2.ToString());
			dir2Children = dir2Node.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			NUnit.Framework.Assert.AreEqual(1, dir2Children.Size());
			dir2Diffs = dir2Node.GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(2, dir2Diffs.Count);
			NUnit.Framework.Assert.AreEqual(s2.GetId(), dir2Diffs[0].GetSnapshotId());
			NUnit.Framework.Assert.AreEqual(s3.GetId(), dir2Diffs[1].GetSnapshotId());
			childrenDiff = dir2Diffs[0].GetChildrenDiff();
			NUnit.Framework.Assert.AreEqual(0, childrenDiff.GetList(Diff.ListType.Deleted).Count
				);
			NUnit.Framework.Assert.AreEqual(1, childrenDiff.GetList(Diff.ListType.Created).Count
				);
			NUnit.Framework.Assert.IsTrue(childrenDiff.GetList(Diff.ListType.Created)[0] == fooNode
				);
			childrenDiff = dir2Diffs[1].GetChildrenDiff();
			NUnit.Framework.Assert.AreEqual(0, childrenDiff.GetList(Diff.ListType.Deleted).Count
				);
			NUnit.Framework.Assert.AreEqual(0, childrenDiff.GetList(Diff.ListType.Created).Count
				);
			Path foo_s3 = SnapshotTestHelper.GetSnapshotPath(sdir2, "s3", "foo2");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(foo_s2));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(foo_s3));
			NUnit.Framework.Assert.IsTrue(fooNode is INodeReference.DstReference);
			fooDiffs = fooNode.AsDirectory().GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(2, fooDiffs.Count);
			NUnit.Framework.Assert.AreEqual(s1.GetId(), fooDiffs[0].GetSnapshotId());
			NUnit.Framework.Assert.AreEqual(s3.GetId(), fooDiffs[1].GetSnapshotId());
		}

		/// <summary>Test undo where dst node being overwritten is a reference node</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameUndo_4()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			Path sdir3 = new Path("/dir3");
			hdfs.Mkdirs(sdir1);
			hdfs.Mkdirs(sdir2);
			hdfs.Mkdirs(sdir3);
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			Path foo2 = new Path(sdir2, "foo2");
			hdfs.Mkdirs(foo2);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			// rename foo2 to foo3, so that foo3 will be a reference node
			Path foo3 = new Path(sdir3, "foo3");
			hdfs.Rename(foo2, foo3);
			INode foo3Node = fsdir.GetINode4Write(foo3.ToString());
			NUnit.Framework.Assert.IsTrue(foo3Node.IsReference());
			INodeDirectory dir3 = fsdir.GetINode4Write(sdir3.ToString()).AsDirectory();
			INodeDirectory mockDir3 = Org.Mockito.Mockito.Spy(dir3);
			// fail the rename but succeed in undo
			Org.Mockito.Mockito.DoReturn(false).When(mockDir3).AddChild((INode)Org.Mockito.Mockito
				.IsNull(), Matchers.AnyBoolean(), Org.Mockito.Mockito.AnyInt());
			Org.Mockito.Mockito.When(mockDir3.AddChild((INode)Org.Mockito.Mockito.IsNotNull()
				, Matchers.AnyBoolean(), Org.Mockito.Mockito.AnyInt())).ThenReturn(false).ThenCallRealMethod
				();
			INodeDirectory root = fsdir.GetINode4Write("/").AsDirectory();
			root.ReplaceChild(dir3, mockDir3, fsdir.GetINodeMap());
			foo3Node.SetParent(mockDir3);
			try
			{
				hdfs.Rename(foo, foo3, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("the rename from " + foo + " to " + foo3 + " should fail"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("rename from " + foo + " to " + foo3 + " failed."
					, e);
			}
			// make sure the undo is correct
			INode foo3Node_undo = fsdir.GetINode4Write(foo3.ToString());
			NUnit.Framework.Assert.AreSame(foo3Node, foo3Node_undo);
			INodeReference.WithCount foo3_wc = (INodeReference.WithCount)foo3Node.AsReference
				().GetReferredINode();
			NUnit.Framework.Assert.AreEqual(2, foo3_wc.GetReferenceCount());
			NUnit.Framework.Assert.AreSame(foo3Node, foo3_wc.GetParentReference());
		}

		/// <summary>
		/// Test rename while the rename operation will exceed the quota in the dst
		/// tree.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameUndo_5()
		{
			Path test = new Path("/test");
			Path dir1 = new Path(test, "dir1");
			Path dir2 = new Path(test, "dir2");
			Path subdir2 = new Path(dir2, "subdir2");
			hdfs.Mkdirs(dir1);
			hdfs.Mkdirs(subdir2);
			Path foo = new Path(dir1, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, dir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, dir2, "s2");
			// set ns quota of dir2 to 4, so the current remaining is 2 (already has
			// dir2, and subdir2)
			hdfs.SetQuota(dir2, 4, long.MaxValue - 1);
			Path foo2 = new Path(subdir2, foo.GetName());
			FSDirectory fsdir2 = Org.Mockito.Mockito.Spy(fsdir);
			Org.Mockito.Mockito.DoThrow(new NSQuotaExceededException("fake exception")).When(
				fsdir2).AddLastINode((INodesInPath)Org.Mockito.Mockito.AnyObject(), (INode)Org.Mockito.Mockito
				.AnyObject(), Org.Mockito.Mockito.AnyBoolean());
			Whitebox.SetInternalState(fsn, "dir", fsdir2);
			// rename /test/dir1/foo to /test/dir2/subdir2/foo. 
			// FSDirectory#verifyQuota4Rename will pass since the remaining quota is 2.
			// However, the rename operation will fail since we let addLastINode throw
			// NSQuotaExceededException
			bool rename = hdfs.Rename(foo, foo2);
			NUnit.Framework.Assert.IsFalse(rename);
			// check the undo
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(foo));
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar));
			INodeDirectory dir1Node = fsdir2.GetINode4Write(dir1.ToString()).AsDirectory();
			IList<INode> childrenList = ReadOnlyList.Util.AsList(dir1Node.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId));
			NUnit.Framework.Assert.AreEqual(1, childrenList.Count);
			INode fooNode = childrenList[0];
			NUnit.Framework.Assert.IsTrue(fooNode.AsDirectory().IsWithSnapshot());
			INode barNode = fsdir2.GetINode4Write(bar.ToString());
			NUnit.Framework.Assert.IsTrue(barNode.GetType() == typeof(INodeFile));
			NUnit.Framework.Assert.AreSame(fooNode, barNode.GetParent());
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = dir1Node.GetDiffs().
				AsList();
			NUnit.Framework.Assert.AreEqual(1, diffList.Count);
			DirectoryWithSnapshotFeature.DirectoryDiff diff = diffList[0];
			NUnit.Framework.Assert.IsTrue(diff.GetChildrenDiff().GetList(Diff.ListType.Created
				).IsEmpty());
			NUnit.Framework.Assert.IsTrue(diff.GetChildrenDiff().GetList(Diff.ListType.Deleted
				).IsEmpty());
			// check dir2
			INodeDirectory dir2Node = fsdir2.GetINode4Write(dir2.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(dir2Node.IsSnapshottable());
			QuotaCounts counts = dir2Node.ComputeQuotaUsage(fsdir.GetBlockStoragePolicySuite(
				));
			NUnit.Framework.Assert.AreEqual(2, counts.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(0, counts.GetStorageSpace());
			childrenList = ReadOnlyList.Util.AsList(dir2Node.AsDirectory().GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId));
			NUnit.Framework.Assert.AreEqual(1, childrenList.Count);
			INode subdir2Node = childrenList[0];
			NUnit.Framework.Assert.AreSame(dir2Node, subdir2Node.GetParent());
			NUnit.Framework.Assert.AreSame(subdir2Node, fsdir2.GetINode4Write(subdir2.ToString
				()));
			diffList = dir2Node.GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(1, diffList.Count);
			diff = diffList[0];
			NUnit.Framework.Assert.IsTrue(diff.GetChildrenDiff().GetList(Diff.ListType.Created
				).IsEmpty());
			NUnit.Framework.Assert.IsTrue(diff.GetChildrenDiff().GetList(Diff.ListType.Deleted
				).IsEmpty());
		}

		/// <summary>Test the rename undo when removing dst node fails</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameUndo_6()
		{
			Path test = new Path("/test");
			Path dir1 = new Path(test, "dir1");
			Path dir2 = new Path(test, "dir2");
			Path sub_dir2 = new Path(dir2, "subdir");
			Path subsub_dir2 = new Path(sub_dir2, "subdir");
			hdfs.Mkdirs(dir1);
			hdfs.Mkdirs(subsub_dir2);
			Path foo = new Path(dir1, "foo");
			hdfs.Mkdirs(foo);
			SnapshotTestHelper.CreateSnapshot(hdfs, dir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, dir2, "s2");
			// set ns quota of dir2 to 4, so the current remaining is 1 (already has
			// dir2, sub_dir2, and subsub_dir2)
			hdfs.SetQuota(dir2, 4, long.MaxValue - 1);
			FSDirectory fsdir2 = Org.Mockito.Mockito.Spy(fsdir);
			Org.Mockito.Mockito.DoThrow(new RuntimeException("fake exception")).When(fsdir2).
				RemoveLastINode((INodesInPath)Org.Mockito.Mockito.AnyObject());
			Whitebox.SetInternalState(fsn, "dir", fsdir2);
			// rename /test/dir1/foo to /test/dir2/sub_dir2/subsub_dir2. 
			// FSDirectory#verifyQuota4Rename will pass since foo only be counted 
			// as 1 in NS quota. However, the rename operation will fail when removing
			// subsub_dir2.
			try
			{
				hdfs.Rename(foo, subsub_dir2, Options.Rename.Overwrite);
				NUnit.Framework.Assert.Fail("Expect QuotaExceedException");
			}
			catch (Exception e)
			{
				string msg = "fake exception";
				GenericTestUtils.AssertExceptionContains(msg, e);
			}
			// check the undo
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(foo));
			INodeDirectory dir1Node = fsdir2.GetINode4Write(dir1.ToString()).AsDirectory();
			IList<INode> childrenList = ReadOnlyList.Util.AsList(dir1Node.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId));
			NUnit.Framework.Assert.AreEqual(1, childrenList.Count);
			INode fooNode = childrenList[0];
			NUnit.Framework.Assert.IsTrue(fooNode.AsDirectory().IsWithSnapshot());
			NUnit.Framework.Assert.AreSame(dir1Node, fooNode.GetParent());
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = dir1Node.GetDiffs().
				AsList();
			NUnit.Framework.Assert.AreEqual(1, diffList.Count);
			DirectoryWithSnapshotFeature.DirectoryDiff diff = diffList[0];
			NUnit.Framework.Assert.IsTrue(diff.GetChildrenDiff().GetList(Diff.ListType.Created
				).IsEmpty());
			NUnit.Framework.Assert.IsTrue(diff.GetChildrenDiff().GetList(Diff.ListType.Deleted
				).IsEmpty());
			// check dir2
			INodeDirectory dir2Node = fsdir2.GetINode4Write(dir2.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(dir2Node.IsSnapshottable());
			QuotaCounts counts = dir2Node.ComputeQuotaUsage(fsdir.GetBlockStoragePolicySuite(
				));
			NUnit.Framework.Assert.AreEqual(3, counts.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(0, counts.GetStorageSpace());
			childrenList = ReadOnlyList.Util.AsList(dir2Node.AsDirectory().GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId));
			NUnit.Framework.Assert.AreEqual(1, childrenList.Count);
			INode subdir2Node = childrenList[0];
			NUnit.Framework.Assert.AreSame(dir2Node, subdir2Node.GetParent());
			NUnit.Framework.Assert.AreSame(subdir2Node, fsdir2.GetINode4Write(sub_dir2.ToString
				()));
			INode subsubdir2Node = fsdir2.GetINode4Write(subsub_dir2.ToString());
			NUnit.Framework.Assert.IsTrue(subsubdir2Node.GetType() == typeof(INodeDirectory));
			NUnit.Framework.Assert.AreSame(subdir2Node, subsubdir2Node.GetParent());
			diffList = (dir2Node).GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(1, diffList.Count);
			diff = diffList[0];
			NUnit.Framework.Assert.IsTrue(diff.GetChildrenDiff().GetList(Diff.ListType.Created
				).IsEmpty());
			NUnit.Framework.Assert.IsTrue(diff.GetChildrenDiff().GetList(Diff.ListType.Deleted
				).IsEmpty());
		}

		/// <summary>Test rename to an invalid name (xxx/.snapshot)</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameUndo_7()
		{
			Path root = new Path("/");
			Path foo = new Path(root, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			// create a snapshot on root
			SnapshotTestHelper.CreateSnapshot(hdfs, root, snap1);
			// rename bar to /foo/.snapshot which is invalid
			Path invalid = new Path(foo, HdfsConstants.DotSnapshotDir);
			try
			{
				hdfs.Rename(bar, invalid);
				NUnit.Framework.Assert.Fail("expect exception since invalid name is used for rename"
					);
			}
			catch (Exception e)
			{
				GenericTestUtils.AssertExceptionContains("\"" + HdfsConstants.DotSnapshotDir + "\" is a reserved name"
					, e);
			}
			// check
			INodeDirectory rootNode = fsdir.GetINode4Write(root.ToString()).AsDirectory();
			INodeDirectory fooNode = fsdir.GetINode4Write(foo.ToString()).AsDirectory();
			ReadOnlyList<INode> children = fooNode.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			NUnit.Framework.Assert.AreEqual(1, children.Size());
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = fooNode.GetDiffs().AsList
				();
			NUnit.Framework.Assert.AreEqual(1, diffList.Count);
			DirectoryWithSnapshotFeature.DirectoryDiff diff = diffList[0];
			// this diff is generated while renaming
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s1 = rootNode.GetSnapshot
				(DFSUtil.String2Bytes(snap1));
			NUnit.Framework.Assert.AreEqual(s1.GetId(), diff.GetSnapshotId());
			// after undo, the diff should be empty
			NUnit.Framework.Assert.IsTrue(diff.GetChildrenDiff().GetList(Diff.ListType.Deleted
				).IsEmpty());
			NUnit.Framework.Assert.IsTrue(diff.GetChildrenDiff().GetList(Diff.ListType.Created
				).IsEmpty());
			// bar was converted to filewithsnapshot while renaming
			INodeFile barNode = fsdir.GetINode4Write(bar.ToString()).AsFile();
			NUnit.Framework.Assert.AreSame(barNode, children.Get(0));
			NUnit.Framework.Assert.AreSame(fooNode, barNode.GetParent());
			IList<FileDiff> barDiffList = barNode.GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(1, barDiffList.Count);
			FileDiff barDiff = barDiffList[0];
			NUnit.Framework.Assert.AreEqual(s1.GetId(), barDiff.GetSnapshotId());
			// restart cluster multiple times to make sure the fsimage and edits log are
			// correct. Note that when loading fsimage, foo and bar will be converted 
			// back to normal INodeDirectory and INodeFile since they do not store any 
			// snapshot data
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			hdfs.SaveNamespace();
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			cluster.Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(Repl).Build
				();
			cluster.WaitActive();
			RestartClusterAndCheckImage(true);
		}

		/// <summary>Test the rename undo when quota of dst tree is exceeded after rename.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameExceedQuota()
		{
			Path test = new Path("/test");
			Path dir1 = new Path(test, "dir1");
			Path dir2 = new Path(test, "dir2");
			Path sub_dir2 = new Path(dir2, "subdir");
			Path subfile_dir2 = new Path(sub_dir2, "subfile");
			hdfs.Mkdirs(dir1);
			DFSTestUtil.CreateFile(hdfs, subfile_dir2, Blocksize, Repl, Seed);
			Path foo = new Path(dir1, "foo");
			DFSTestUtil.CreateFile(hdfs, foo, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, dir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, dir2, "s2");
			// set ns quota of dir2 to 4, so the current remaining is 1 (already has
			// dir2, sub_dir2, subfile_dir2, and s2)
			hdfs.SetQuota(dir2, 5, long.MaxValue - 1);
			// rename /test/dir1/foo to /test/dir2/sub_dir2/subfile_dir2. 
			// FSDirectory#verifyQuota4Rename will pass since foo only be counted 
			// as 1 in NS quota. The rename operation will succeed while the real quota 
			// of dir2 will become 7 (dir2, s2 in dir2, sub_dir2, s2 in sub_dir2,
			// subfile_dir2 in deleted list, new subfile, s1 in new subfile).
			hdfs.Rename(foo, subfile_dir2, Options.Rename.Overwrite);
			// check dir2
			INode dir2Node = fsdir.GetINode4Write(dir2.ToString());
			NUnit.Framework.Assert.IsTrue(dir2Node.AsDirectory().IsSnapshottable());
			QuotaCounts counts = dir2Node.ComputeQuotaUsage(fsdir.GetBlockStoragePolicySuite(
				));
			NUnit.Framework.Assert.AreEqual(4, counts.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(Blocksize * Repl * 2, counts.GetStorageSpace());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRename2PreDescendant()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			hdfs.Mkdirs(bar);
			hdfs.Mkdirs(sdir2);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, snap1);
			// /dir1/foo/bar -> /dir2/bar
			Path bar2 = new Path(sdir2, "bar");
			hdfs.Rename(bar, bar2);
			// /dir1/foo -> /dir2/bar/foo
			Path foo2 = new Path(bar2, "foo");
			hdfs.Rename(foo, foo2);
			RestartClusterAndCheckImage(true);
			// delete snap1
			hdfs.DeleteSnapshot(sdir1, snap1);
			RestartClusterAndCheckImage(true);
		}

		/// <summary>move a directory to its prior descendant</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRename2PreDescendant_2()
		{
			Path root = new Path("/");
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			Path file1InBar = new Path(bar, "file1");
			Path file2InBar = new Path(bar, "file2");
			hdfs.Mkdirs(bar);
			hdfs.Mkdirs(sdir2);
			DFSTestUtil.CreateFile(hdfs, file1InBar, Blocksize, Repl, Seed);
			DFSTestUtil.CreateFile(hdfs, file2InBar, Blocksize, Repl, Seed);
			hdfs.SetQuota(sdir1, long.MaxValue - 1, long.MaxValue - 1);
			hdfs.SetQuota(sdir2, long.MaxValue - 1, long.MaxValue - 1);
			hdfs.SetQuota(foo, long.MaxValue - 1, long.MaxValue - 1);
			hdfs.SetQuota(bar, long.MaxValue - 1, long.MaxValue - 1);
			// create snapshot on root
			SnapshotTestHelper.CreateSnapshot(hdfs, root, snap1);
			// delete file1InBar
			hdfs.Delete(file1InBar, true);
			// create another snapshot on root
			SnapshotTestHelper.CreateSnapshot(hdfs, root, snap2);
			// delete file2InBar
			hdfs.Delete(file2InBar, true);
			// /dir1/foo/bar -> /dir2/bar
			Path bar2 = new Path(sdir2, "bar2");
			hdfs.Rename(bar, bar2);
			// /dir1/foo -> /dir2/bar/foo
			Path foo2 = new Path(bar2, "foo2");
			hdfs.Rename(foo, foo2);
			RestartClusterAndCheckImage(true);
			// delete snapshot snap2
			hdfs.DeleteSnapshot(root, snap2);
			// after deleteing snap2, the WithName node "bar", which originally was 
			// stored in the deleted list of "foo" for snap2, is moved to its deleted 
			// list for snap1. In that case, it will not be counted when calculating 
			// quota for "foo". However, we do not update this quota usage change while 
			// deleting snap2.
			RestartClusterAndCheckImage(false);
		}

		/// <summary>move a directory to its prior descedant</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRename2PreDescendant_3()
		{
			Path root = new Path("/");
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			Path fileInBar = new Path(bar, "file");
			hdfs.Mkdirs(bar);
			hdfs.Mkdirs(sdir2);
			DFSTestUtil.CreateFile(hdfs, fileInBar, Blocksize, Repl, Seed);
			hdfs.SetQuota(sdir1, long.MaxValue - 1, long.MaxValue - 1);
			hdfs.SetQuota(sdir2, long.MaxValue - 1, long.MaxValue - 1);
			hdfs.SetQuota(foo, long.MaxValue - 1, long.MaxValue - 1);
			hdfs.SetQuota(bar, long.MaxValue - 1, long.MaxValue - 1);
			// create snapshot on root
			SnapshotTestHelper.CreateSnapshot(hdfs, root, snap1);
			// delete fileInBar
			hdfs.Delete(fileInBar, true);
			// create another snapshot on root
			SnapshotTestHelper.CreateSnapshot(hdfs, root, snap2);
			// /dir1/foo/bar -> /dir2/bar
			Path bar2 = new Path(sdir2, "bar2");
			hdfs.Rename(bar, bar2);
			// /dir1/foo -> /dir2/bar/foo
			Path foo2 = new Path(bar2, "foo2");
			hdfs.Rename(foo, foo2);
			RestartClusterAndCheckImage(true);
			// delete snapshot snap1
			hdfs.DeleteSnapshot(root, snap1);
			RestartClusterAndCheckImage(true);
		}

		/// <summary>
		/// After the following operations:
		/// Rename a dir -&gt; create a snapshot s on dst tree -&gt; delete the renamed dir
		/// -&gt; delete snapshot s on dst tree
		/// Make sure we destroy everything created after the rename under the renamed
		/// dir.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirAndDeleteSnapshot_3()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			hdfs.Mkdirs(sdir2);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			Path foo2 = new Path(sdir2, "foo");
			hdfs.Rename(foo, foo2);
			// create two new files under foo2
			Path bar2 = new Path(foo2, "bar2");
			DFSTestUtil.CreateFile(hdfs, bar2, Blocksize, Repl, Seed);
			Path bar3 = new Path(foo2, "bar3");
			DFSTestUtil.CreateFile(hdfs, bar3, Blocksize, Repl, Seed);
			// create a new snapshot on sdir2
			hdfs.CreateSnapshot(sdir2, "s3");
			// delete foo2
			hdfs.Delete(foo2, true);
			// delete s3
			hdfs.DeleteSnapshot(sdir2, "s3");
			// check
			INodeDirectory dir1Node = fsdir.GetINode4Write(sdir1.ToString()).AsDirectory();
			QuotaCounts q1 = dir1Node.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			NUnit.Framework.Assert.AreEqual(3, q1.GetNameSpace());
			INodeDirectory dir2Node = fsdir.GetINode4Write(sdir2.ToString()).AsDirectory();
			QuotaCounts q2 = dir2Node.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			NUnit.Framework.Assert.AreEqual(1, q2.GetNameSpace());
			Path foo_s1 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1", foo.GetName());
			INode fooRef = fsdir.GetINode(foo_s1.ToString());
			NUnit.Framework.Assert.IsTrue(fooRef is INodeReference.WithName);
			INodeReference.WithCount wc = (INodeReference.WithCount)fooRef.AsReference().GetReferredINode
				();
			NUnit.Framework.Assert.AreEqual(1, wc.GetReferenceCount());
			INodeDirectory fooNode = wc.GetReferredINode().AsDirectory();
			ReadOnlyList<INode> children = fooNode.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			NUnit.Framework.Assert.AreEqual(1, children.Size());
			NUnit.Framework.Assert.AreEqual(bar.GetName(), children.Get(0).GetLocalName());
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = fooNode.GetDiffs().AsList
				();
			NUnit.Framework.Assert.AreEqual(1, diffList.Count);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s1 = dir1Node.GetSnapshot
				(DFSUtil.String2Bytes("s1"));
			NUnit.Framework.Assert.AreEqual(s1.GetId(), diffList[0].GetSnapshotId());
			DirectoryWithSnapshotFeature.ChildrenDiff diff = diffList[0].GetChildrenDiff();
			NUnit.Framework.Assert.AreEqual(0, diff.GetList(Diff.ListType.Created).Count);
			NUnit.Framework.Assert.AreEqual(0, diff.GetList(Diff.ListType.Deleted).Count);
			RestartClusterAndCheckImage(true);
		}

		/// <summary>
		/// After the following operations:
		/// Rename a dir -&gt; create a snapshot s on dst tree -&gt; rename the renamed dir
		/// again -&gt; delete snapshot s on dst tree
		/// Make sure we only delete the snapshot s under the renamed dir.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirAndDeleteSnapshot_4()
		{
			Path sdir1 = new Path("/dir1");
			Path sdir2 = new Path("/dir2");
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			hdfs.Mkdirs(sdir2);
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sdir2, "s2");
			Path foo2 = new Path(sdir2, "foo");
			hdfs.Rename(foo, foo2);
			// create two new files under foo2
			Path bar2 = new Path(foo2, "bar2");
			DFSTestUtil.CreateFile(hdfs, bar2, Blocksize, Repl, Seed);
			Path bar3 = new Path(foo2, "bar3");
			DFSTestUtil.CreateFile(hdfs, bar3, Blocksize, Repl, Seed);
			// create a new snapshot on sdir2
			hdfs.CreateSnapshot(sdir2, "s3");
			// rename foo2 again
			hdfs.Rename(foo2, foo);
			// delete snapshot s3
			hdfs.DeleteSnapshot(sdir2, "s3");
			// check
			INodeDirectory dir1Node = fsdir.GetINode4Write(sdir1.ToString()).AsDirectory();
			// sdir1 + s1 + foo_s1 (foo) + foo (foo + s1 + bar~bar3)
			QuotaCounts q1 = dir1Node.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			NUnit.Framework.Assert.AreEqual(7, q1.GetNameSpace());
			INodeDirectory dir2Node = fsdir.GetINode4Write(sdir2.ToString()).AsDirectory();
			QuotaCounts q2 = dir2Node.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			NUnit.Framework.Assert.AreEqual(1, q2.GetNameSpace());
			Path foo_s1 = SnapshotTestHelper.GetSnapshotPath(sdir1, "s1", foo.GetName());
			INode fooRef = fsdir.GetINode(foo_s1.ToString());
			NUnit.Framework.Assert.IsTrue(fooRef is INodeReference.WithName);
			INodeReference.WithCount wc = (INodeReference.WithCount)fooRef.AsReference().GetReferredINode
				();
			NUnit.Framework.Assert.AreEqual(2, wc.GetReferenceCount());
			INodeDirectory fooNode = wc.GetReferredINode().AsDirectory();
			ReadOnlyList<INode> children = fooNode.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			NUnit.Framework.Assert.AreEqual(3, children.Size());
			NUnit.Framework.Assert.AreEqual(bar.GetName(), children.Get(0).GetLocalName());
			NUnit.Framework.Assert.AreEqual(bar2.GetName(), children.Get(1).GetLocalName());
			NUnit.Framework.Assert.AreEqual(bar3.GetName(), children.Get(2).GetLocalName());
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = fooNode.GetDiffs().AsList
				();
			NUnit.Framework.Assert.AreEqual(1, diffList.Count);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s1 = dir1Node.GetSnapshot
				(DFSUtil.String2Bytes("s1"));
			NUnit.Framework.Assert.AreEqual(s1.GetId(), diffList[0].GetSnapshotId());
			DirectoryWithSnapshotFeature.ChildrenDiff diff = diffList[0].GetChildrenDiff();
			// bar2 and bar3 in the created list
			NUnit.Framework.Assert.AreEqual(2, diff.GetList(Diff.ListType.Created).Count);
			NUnit.Framework.Assert.AreEqual(0, diff.GetList(Diff.ListType.Deleted).Count);
			INode fooRef2 = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fooRef2 is INodeReference.DstReference);
			INodeReference.WithCount wc2 = (INodeReference.WithCount)fooRef2.AsReference().GetReferredINode
				();
			NUnit.Framework.Assert.AreSame(wc, wc2);
			NUnit.Framework.Assert.AreSame(fooRef2, wc.GetParentReference());
			RestartClusterAndCheckImage(true);
		}

		/// <summary>
		/// This test demonstrates that
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeDirectory.RemoveChild(Org.Apache.Hadoop.Hdfs.Server.Namenode.INode)
		/// 	"/>
		/// and
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeDirectory.AddChild(Org.Apache.Hadoop.Hdfs.Server.Namenode.INode)
		/// 	"/>
		/// should use
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INode.IsInLatestSnapshot(int)"/
		/// 	>
		/// to check if the
		/// added/removed child should be recorded in snapshots.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirAndDeleteSnapshot_5()
		{
			Path dir1 = new Path("/dir1");
			Path dir2 = new Path("/dir2");
			Path dir3 = new Path("/dir3");
			hdfs.Mkdirs(dir1);
			hdfs.Mkdirs(dir2);
			hdfs.Mkdirs(dir3);
			Path foo = new Path(dir1, "foo");
			hdfs.Mkdirs(foo);
			SnapshotTestHelper.CreateSnapshot(hdfs, dir1, "s1");
			Path bar = new Path(foo, "bar");
			// create file bar, and foo will become an INodeDirectory with snapshot
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			// delete snapshot s1. now foo is not in any snapshot
			hdfs.DeleteSnapshot(dir1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, dir2, "s2");
			// rename /dir1/foo to /dir2/foo
			Path foo2 = new Path(dir2, foo.GetName());
			hdfs.Rename(foo, foo2);
			// rename /dir2/foo/bar to /dir3/foo/bar
			Path bar2 = new Path(dir2, "foo/bar");
			Path bar3 = new Path(dir3, "bar");
			hdfs.Rename(bar2, bar3);
			// delete /dir2/foo. Since it is not in any snapshot, we will call its 
			// destroy function. If we do not use isInLatestSnapshot in removeChild and
			// addChild methods in INodeDirectory (with snapshot), the file bar will be 
			// stored in the deleted list of foo, and will be destroyed.
			hdfs.Delete(foo2, true);
			// check if /dir3/bar still exists
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(bar3));
			INodeFile barNode = (INodeFile)fsdir.GetINode4Write(bar3.ToString());
			NUnit.Framework.Assert.AreSame(fsdir.GetINode4Write(dir3.ToString()), barNode.GetParent
				());
		}

		/// <summary>Rename and deletion snapshot under the same the snapshottable directory.
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirAndDeleteSnapshot_6()
		{
			Path test = new Path("/test");
			Path dir1 = new Path(test, "dir1");
			Path dir2 = new Path(test, "dir2");
			hdfs.Mkdirs(dir1);
			hdfs.Mkdirs(dir2);
			Path foo = new Path(dir2, "foo");
			Path bar = new Path(foo, "bar");
			Path file = new Path(bar, "file");
			DFSTestUtil.CreateFile(hdfs, file, Blocksize, Repl, Seed);
			// take a snapshot on /test
			SnapshotTestHelper.CreateSnapshot(hdfs, test, "s0");
			// delete /test/dir2/foo/bar/file after snapshot s0, so that there is a 
			// snapshot copy recorded in bar
			hdfs.Delete(file, true);
			// rename foo from dir2 to dir1
			Path newfoo = new Path(dir1, foo.GetName());
			hdfs.Rename(foo, newfoo);
			Path foo_s0 = SnapshotTestHelper.GetSnapshotPath(test, "s0", "dir2/foo");
			NUnit.Framework.Assert.IsTrue("the snapshot path " + foo_s0 + " should exist", hdfs
				.Exists(foo_s0));
			// delete snapshot s0. The deletion will first go down through dir1, and 
			// find foo in the created list of dir1. Then it will use null as the prior
			// snapshot and continue the snapshot deletion process in the subtree of 
			// foo. We need to make sure the snapshot s0 can be deleted cleanly in the
			// foo subtree.
			hdfs.DeleteSnapshot(test, "s0");
			// check the internal
			NUnit.Framework.Assert.IsFalse("after deleting s0, " + foo_s0 + " should not exist"
				, hdfs.Exists(foo_s0));
			INodeDirectory dir2Node = fsdir.GetINode4Write(dir2.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue("the diff list of " + dir2 + " should be empty after deleting s0"
				, dir2Node.GetDiffs().AsList().IsEmpty());
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(newfoo));
			INode fooRefNode = fsdir.GetINode4Write(newfoo.ToString());
			NUnit.Framework.Assert.IsTrue(fooRefNode is INodeReference.DstReference);
			INodeDirectory fooNode = fooRefNode.AsDirectory();
			// fooNode should be still INodeDirectory (With Snapshot) since we call
			// recordModification before the rename
			NUnit.Framework.Assert.IsTrue(fooNode.IsWithSnapshot());
			NUnit.Framework.Assert.IsTrue(fooNode.GetDiffs().AsList().IsEmpty());
			INodeDirectory barNode = fooNode.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId).Get(0).AsDirectory();
			// bar should also be INodeDirectory (With Snapshot), and both of its diff 
			// list and children list are empty 
			NUnit.Framework.Assert.IsTrue(barNode.GetDiffs().AsList().IsEmpty());
			NUnit.Framework.Assert.IsTrue(barNode.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId).IsEmpty());
			RestartClusterAndCheckImage(true);
		}

		/// <summary>Unit test for HDFS-4842.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameDirAndDeleteSnapshot_7()
		{
			fsn.GetSnapshotManager().SetAllowNestedSnapshots(true);
			Path test = new Path("/test");
			Path dir1 = new Path(test, "dir1");
			Path dir2 = new Path(test, "dir2");
			hdfs.Mkdirs(dir1);
			hdfs.Mkdirs(dir2);
			Path foo = new Path(dir2, "foo");
			Path bar = new Path(foo, "bar");
			Path file = new Path(bar, "file");
			DFSTestUtil.CreateFile(hdfs, file, Blocksize, Repl, Seed);
			// take a snapshot s0 and s1 on /test
			SnapshotTestHelper.CreateSnapshot(hdfs, test, "s0");
			SnapshotTestHelper.CreateSnapshot(hdfs, test, "s1");
			// delete file so we have a snapshot copy for s1 in bar
			hdfs.Delete(file, true);
			// create another snapshot on dir2
			SnapshotTestHelper.CreateSnapshot(hdfs, dir2, "s2");
			// rename foo from dir2 to dir1
			Path newfoo = new Path(dir1, foo.GetName());
			hdfs.Rename(foo, newfoo);
			// delete snapshot s1
			hdfs.DeleteSnapshot(test, "s1");
			// make sure the snapshot copy of file in s1 is merged to s0. For 
			// HDFS-4842, we need to make sure that we do not wrongly use s2 as the
			// prior snapshot of s1.
			Path file_s2 = SnapshotTestHelper.GetSnapshotPath(dir2, "s2", "foo/bar/file");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(file_s2));
			Path file_s0 = SnapshotTestHelper.GetSnapshotPath(test, "s0", "dir2/foo/bar/file"
				);
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(file_s0));
			// check dir1: foo should be in the created list of s0
			INodeDirectory dir1Node = fsdir.GetINode4Write(dir1.ToString()).AsDirectory();
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> dir1DiffList = dir1Node.GetDiffs
				().AsList();
			NUnit.Framework.Assert.AreEqual(1, dir1DiffList.Count);
			IList<INode> dList = dir1DiffList[0].GetChildrenDiff().GetList(Diff.ListType.Deleted
				);
			NUnit.Framework.Assert.IsTrue(dList.IsEmpty());
			IList<INode> cList = dir1DiffList[0].GetChildrenDiff().GetList(Diff.ListType.Created
				);
			NUnit.Framework.Assert.AreEqual(1, cList.Count);
			INode cNode = cList[0];
			INode fooNode = fsdir.GetINode4Write(newfoo.ToString());
			NUnit.Framework.Assert.AreSame(cNode, fooNode);
			// check foo and its subtree
			Path newbar = new Path(newfoo, bar.GetName());
			INodeDirectory barNode = fsdir.GetINode4Write(newbar.ToString()).AsDirectory();
			NUnit.Framework.Assert.AreSame(fooNode.AsDirectory(), barNode.GetParent());
			// bar should only have a snapshot diff for s0
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> barDiffList = barNode.GetDiffs(
				).AsList();
			NUnit.Framework.Assert.AreEqual(1, barDiffList.Count);
			DirectoryWithSnapshotFeature.DirectoryDiff diff = barDiffList[0];
			INodeDirectory testNode = fsdir.GetINode4Write(test.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s0 = testNode.GetSnapshot
				(DFSUtil.String2Bytes("s0"));
			NUnit.Framework.Assert.AreEqual(s0.GetId(), diff.GetSnapshotId());
			// and file should be stored in the deleted list of this snapshot diff
			NUnit.Framework.Assert.AreEqual("file", diff.GetChildrenDiff().GetList(Diff.ListType
				.Deleted)[0].GetLocalName());
			// check dir2: a WithName instance for foo should be in the deleted list
			// of the snapshot diff for s2
			INodeDirectory dir2Node = fsdir.GetINode4Write(dir2.ToString()).AsDirectory();
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> dir2DiffList = dir2Node.GetDiffs
				().AsList();
			// dir2Node should contain 1 snapshot diffs for s2
			NUnit.Framework.Assert.AreEqual(1, dir2DiffList.Count);
			dList = dir2DiffList[0].GetChildrenDiff().GetList(Diff.ListType.Deleted);
			NUnit.Framework.Assert.AreEqual(1, dList.Count);
			Path foo_s2 = SnapshotTestHelper.GetSnapshotPath(dir2, "s2", foo.GetName());
			INodeReference.WithName fooNode_s2 = (INodeReference.WithName)fsdir.GetINode(foo_s2
				.ToString());
			NUnit.Framework.Assert.AreSame(dList[0], fooNode_s2);
			NUnit.Framework.Assert.AreSame(fooNode.AsReference().GetReferredINode(), fooNode_s2
				.GetReferredINode());
			RestartClusterAndCheckImage(true);
		}

		/// <summary>
		/// Make sure we clean the whole subtree under a DstReference node after
		/// deleting a snapshot.
		/// </summary>
		/// <remarks>
		/// Make sure we clean the whole subtree under a DstReference node after
		/// deleting a snapshot.
		/// see HDFS-5476.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCleanDstReference()
		{
			Path test = new Path("/test");
			Path foo = new Path(test, "foo");
			Path bar = new Path(foo, "bar");
			hdfs.Mkdirs(bar);
			SnapshotTestHelper.CreateSnapshot(hdfs, test, "s0");
			// create file after s0 so that the file should not be included in s0
			Path fileInBar = new Path(bar, "file");
			DFSTestUtil.CreateFile(hdfs, fileInBar, Blocksize, Repl, Seed);
			// rename foo --> foo2
			Path foo2 = new Path(test, "foo2");
			hdfs.Rename(foo, foo2);
			// create snapshot s1, note the file is included in s1
			hdfs.CreateSnapshot(test, "s1");
			// delete bar and foo2
			hdfs.Delete(new Path(foo2, "bar"), true);
			hdfs.Delete(foo2, true);
			Path sfileInBar = SnapshotTestHelper.GetSnapshotPath(test, "s1", "foo2/bar/file");
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(sfileInBar));
			hdfs.DeleteSnapshot(test, "s1");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(sfileInBar));
			RestartClusterAndCheckImage(true);
			// make sure the file under bar is deleted 
			Path barInS0 = SnapshotTestHelper.GetSnapshotPath(test, "s0", "foo/bar");
			INodeDirectory barNode = fsdir.GetINode(barInS0.ToString()).AsDirectory();
			NUnit.Framework.Assert.AreEqual(0, barNode.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId).Size());
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = barNode.GetDiffs().AsList
				();
			NUnit.Framework.Assert.AreEqual(1, diffList.Count);
			DirectoryWithSnapshotFeature.DirectoryDiff diff = diffList[0];
			NUnit.Framework.Assert.AreEqual(0, diff.GetChildrenDiff().GetList(Diff.ListType.Deleted
				).Count);
			NUnit.Framework.Assert.AreEqual(0, diff.GetChildrenDiff().GetList(Diff.ListType.Created
				).Count);
		}

		/// <summary>
		/// Rename of the underconstruction file in snapshot should not fail NN restart
		/// after checkpoint.
		/// </summary>
		/// <remarks>
		/// Rename of the underconstruction file in snapshot should not fail NN restart
		/// after checkpoint. Unit test for HDFS-5425.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameUCFileInSnapshot()
		{
			Path test = new Path("/test");
			Path foo = new Path(test, "foo");
			Path bar = new Path(foo, "bar");
			hdfs.Mkdirs(foo);
			// create a file and keep it as underconstruction.
			hdfs.Create(bar);
			SnapshotTestHelper.CreateSnapshot(hdfs, test, "s0");
			// rename bar --> bar2
			Path bar2 = new Path(foo, "bar2");
			hdfs.Rename(bar, bar2);
			// save namespace and restart
			RestartClusterAndCheckImage(true);
		}

		/// <summary>
		/// Similar with testRenameUCFileInSnapshot, but do renaming first and then
		/// append file without closing it.
		/// </summary>
		/// <remarks>
		/// Similar with testRenameUCFileInSnapshot, but do renaming first and then
		/// append file without closing it. Unit test for HDFS-5425.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendFileAfterRenameInSnapshot()
		{
			Path test = new Path("/test");
			Path foo = new Path(test, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Repl, Seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, test, "s0");
			// rename bar --> bar2
			Path bar2 = new Path(foo, "bar2");
			hdfs.Rename(bar, bar2);
			// append file and keep it as underconstruction.
			FSDataOutputStream @out = hdfs.Append(bar2);
			@out.WriteByte(0);
			((DFSOutputStream)@out.GetWrappedStream()).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag
				.UpdateLength));
			// save namespace and restart
			RestartClusterAndCheckImage(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameWithOverWrite()
		{
			Path root = new Path("/");
			Path foo = new Path(root, "foo");
			Path file1InFoo = new Path(foo, "file1");
			Path file2InFoo = new Path(foo, "file2");
			Path file3InFoo = new Path(foo, "file3");
			DFSTestUtil.CreateFile(hdfs, file1InFoo, 1L, Repl, Seed);
			DFSTestUtil.CreateFile(hdfs, file2InFoo, 1L, Repl, Seed);
			DFSTestUtil.CreateFile(hdfs, file3InFoo, 1L, Repl, Seed);
			Path bar = new Path(root, "bar");
			hdfs.Mkdirs(bar);
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s0");
			// move file1 from foo to bar
			Path fileInBar = new Path(bar, "file1");
			hdfs.Rename(file1InFoo, fileInBar);
			// rename bar to newDir
			Path newDir = new Path(root, "newDir");
			hdfs.Rename(bar, newDir);
			// move file2 from foo to newDir
			Path file2InNewDir = new Path(newDir, "file2");
			hdfs.Rename(file2InFoo, file2InNewDir);
			// move file3 from foo to newDir and rename it to file1, this will overwrite
			// the original file1
			Path file1InNewDir = new Path(newDir, "file1");
			hdfs.Rename(file3InFoo, file1InNewDir, Options.Rename.Overwrite);
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s1");
			SnapshotDiffReport report = hdfs.GetSnapshotDiffReport(root, "s0", "s1");
			Log.Info("DiffList is \n\"" + report.ToString() + "\"");
			IList<SnapshotDiffReport.DiffReportEntry> entries = report.GetDiffList();
			NUnit.Framework.Assert.AreEqual(7, entries.Count);
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Modify, string.Empty, null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Modify, foo.GetName(), null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Modify, bar.GetName(), null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Delete, "foo/file1", null));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Rename, "bar", "newDir"));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Rename, "foo/file2", "newDir/file2"));
			NUnit.Framework.Assert.IsTrue(ExistsInDiffReport(entries, SnapshotDiffReport.DiffType
				.Rename, "foo/file3", "newDir/file1"));
		}
	}
}
