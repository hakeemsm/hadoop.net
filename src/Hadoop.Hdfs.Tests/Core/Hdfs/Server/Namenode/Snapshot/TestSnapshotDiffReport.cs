using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Tests snapshot deletion.</summary>
	public class TestSnapshotDiffReport
	{
		protected internal const long seed = 0;

		protected internal const short Replication = 3;

		protected internal const short Replication1 = 2;

		protected internal const long Blocksize = 1024;

		public const int Snapshotnumber = 10;

		private readonly Path dir = new Path("/TestSnapshot");

		private readonly Path sub1;

		protected internal Configuration conf;

		protected internal MiniDFSCluster cluster;

		protected internal DistributedFileSystem hdfs;

		private readonly Dictionary<Path, int> snapshotNumberMap = new Dictionary<Path, int
			>();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Format(true)
				.Build();
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

		private string GenSnapshotName(Path snapshotDir)
		{
			int sNum = -1;
			if (snapshotNumberMap.Contains(snapshotDir))
			{
				sNum = snapshotNumberMap[snapshotDir];
			}
			snapshotNumberMap[snapshotDir] = ++sNum;
			return "s" + sNum;
		}

		/// <summary>
		/// Create/modify/delete files under a given directory, also create snapshots
		/// of directories.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void ModifyAndCreateSnapshot(Path modifyDir, Path[] snapshotDirs)
		{
			Path file10 = new Path(modifyDir, "file10");
			Path file11 = new Path(modifyDir, "file11");
			Path file12 = new Path(modifyDir, "file12");
			Path file13 = new Path(modifyDir, "file13");
			Path link13 = new Path(modifyDir, "link13");
			Path file14 = new Path(modifyDir, "file14");
			Path file15 = new Path(modifyDir, "file15");
			DFSTestUtil.CreateFile(hdfs, file10, Blocksize, Replication1, seed);
			DFSTestUtil.CreateFile(hdfs, file11, Blocksize, Replication1, seed);
			DFSTestUtil.CreateFile(hdfs, file12, Blocksize, Replication1, seed);
			DFSTestUtil.CreateFile(hdfs, file13, Blocksize, Replication1, seed);
			// create link13
			hdfs.CreateSymlink(file13, link13, false);
			// create snapshot
			foreach (Path snapshotDir in snapshotDirs)
			{
				hdfs.AllowSnapshot(snapshotDir);
				hdfs.CreateSnapshot(snapshotDir, GenSnapshotName(snapshotDir));
			}
			// delete file11
			hdfs.Delete(file11, true);
			// modify file12
			hdfs.SetReplication(file12, Replication);
			// modify file13
			hdfs.SetReplication(file13, Replication);
			// delete link13
			hdfs.Delete(link13, false);
			// create file14
			DFSTestUtil.CreateFile(hdfs, file14, Blocksize, Replication, seed);
			// create file15
			DFSTestUtil.CreateFile(hdfs, file15, Blocksize, Replication, seed);
			// create snapshot
			foreach (Path snapshotDir_1 in snapshotDirs)
			{
				hdfs.CreateSnapshot(snapshotDir_1, GenSnapshotName(snapshotDir_1));
			}
			// create file11 again
			DFSTestUtil.CreateFile(hdfs, file11, Blocksize, Replication, seed);
			// delete file12
			hdfs.Delete(file12, true);
			// modify file13
			hdfs.SetReplication(file13, (short)(Replication - 2));
			// create link13 again
			hdfs.CreateSymlink(file13, link13, false);
			// delete file14
			hdfs.Delete(file14, true);
			// modify file15
			hdfs.SetReplication(file15, (short)(Replication - 1));
			// create snapshot
			foreach (Path snapshotDir_2 in snapshotDirs)
			{
				hdfs.CreateSnapshot(snapshotDir_2, GenSnapshotName(snapshotDir_2));
			}
			// modify file10
			hdfs.SetReplication(file10, (short)(Replication + 1));
		}

		/// <summary>check the correctness of the diff reports</summary>
		/// <exception cref="System.IO.IOException"/>
		private void VerifyDiffReport(Path dir, string from, string to, params SnapshotDiffReport.DiffReportEntry
			[] entries)
		{
			SnapshotDiffReport report = hdfs.GetSnapshotDiffReport(dir, from, to);
			// reverse the order of from and to
			SnapshotDiffReport inverseReport = hdfs.GetSnapshotDiffReport(dir, to, from);
			System.Console.Out.WriteLine(report.ToString());
			System.Console.Out.WriteLine(inverseReport.ToString() + "\n");
			NUnit.Framework.Assert.AreEqual(entries.Length, report.GetDiffList().Count);
			NUnit.Framework.Assert.AreEqual(entries.Length, inverseReport.GetDiffList().Count
				);
			foreach (SnapshotDiffReport.DiffReportEntry entry in entries)
			{
				if (entry.GetType() == SnapshotDiffReport.DiffType.Modify)
				{
					NUnit.Framework.Assert.IsTrue(report.GetDiffList().Contains(entry));
					NUnit.Framework.Assert.IsTrue(inverseReport.GetDiffList().Contains(entry));
				}
				else
				{
					if (entry.GetType() == SnapshotDiffReport.DiffType.Delete)
					{
						NUnit.Framework.Assert.IsTrue(report.GetDiffList().Contains(entry));
						NUnit.Framework.Assert.IsTrue(inverseReport.GetDiffList().Contains(new SnapshotDiffReport.DiffReportEntry
							(SnapshotDiffReport.DiffType.Create, entry.GetSourcePath())));
					}
					else
					{
						if (entry.GetType() == SnapshotDiffReport.DiffType.Create)
						{
							NUnit.Framework.Assert.IsTrue(report.GetDiffList().Contains(entry));
							NUnit.Framework.Assert.IsTrue(inverseReport.GetDiffList().Contains(new SnapshotDiffReport.DiffReportEntry
								(SnapshotDiffReport.DiffType.Delete, entry.GetSourcePath())));
						}
					}
				}
			}
		}

		/// <summary>Test the computation and representation of diff between snapshots</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDiffReport()
		{
			cluster.GetNamesystem().GetSnapshotManager().SetAllowNestedSnapshots(true);
			Path subsub1 = new Path(sub1, "subsub1");
			Path subsubsub1 = new Path(subsub1, "subsubsub1");
			hdfs.Mkdirs(subsubsub1);
			ModifyAndCreateSnapshot(sub1, new Path[] { sub1, subsubsub1 });
			ModifyAndCreateSnapshot(subsubsub1, new Path[] { sub1, subsubsub1 });
			try
			{
				hdfs.GetSnapshotDiffReport(subsub1, "s1", "s2");
				NUnit.Framework.Assert.Fail("Expect exception when getting snapshot diff report: "
					 + subsub1 + " is not a snapshottable directory.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Directory is not a snapshottable directory: "
					 + subsub1, e);
			}
			string invalidName = "invalid";
			try
			{
				hdfs.GetSnapshotDiffReport(sub1, invalidName, invalidName);
				NUnit.Framework.Assert.Fail("Expect exception when providing invalid snapshot name for diff report"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot find the snapshot of directory "
					 + sub1 + " with name " + invalidName, e);
			}
			// diff between the same snapshot
			SnapshotDiffReport report = hdfs.GetSnapshotDiffReport(sub1, "s0", "s0");
			System.Console.Out.WriteLine(report);
			NUnit.Framework.Assert.AreEqual(0, report.GetDiffList().Count);
			report = hdfs.GetSnapshotDiffReport(sub1, string.Empty, string.Empty);
			System.Console.Out.WriteLine(report);
			NUnit.Framework.Assert.AreEqual(0, report.GetDiffList().Count);
			report = hdfs.GetSnapshotDiffReport(subsubsub1, "s0", "s2");
			System.Console.Out.WriteLine(report);
			NUnit.Framework.Assert.AreEqual(0, report.GetDiffList().Count);
			// test path with scheme also works
			report = hdfs.GetSnapshotDiffReport(hdfs.MakeQualified(subsubsub1), "s0", "s2");
			System.Console.Out.WriteLine(report);
			NUnit.Framework.Assert.AreEqual(0, report.GetDiffList().Count);
			VerifyDiffReport(sub1, "s0", "s2", new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes(string.Empty)), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("file15")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("file12")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("file11")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("file11")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("file13")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("link13")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("link13")));
			VerifyDiffReport(sub1, "s0", "s5", new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes(string.Empty)), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("file15")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("file12")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("file10")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("file11")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("file11")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("file13")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("link13")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("link13")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("subsub1/subsubsub1"))
				, new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file10")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("subsub1/subsubsub1/file11"
				)), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file13")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("subsub1/subsubsub1/link13"
				)), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file15")));
			VerifyDiffReport(sub1, "s2", "s5", new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes("file10")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("subsub1/subsubsub1"))
				, new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file10")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("subsub1/subsubsub1/file11"
				)), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file13")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("subsub1/subsubsub1/link13"
				)), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file15")));
			VerifyDiffReport(sub1, "s3", string.Empty, new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("subsub1/subsubsub1"))
				, new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file15")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("subsub1/subsubsub1/file12"
				)), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Modify, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file10")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("subsub1/subsubsub1/file11"
				)), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file11")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("subsub1/subsubsub1/file13"
				)), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/link13")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("subsub1/subsubsub1/link13"
				)));
		}

		/// <summary>Make changes under a sub-directory, then delete the sub-directory.</summary>
		/// <remarks>
		/// Make changes under a sub-directory, then delete the sub-directory. Make
		/// sure the diff report computation correctly retrieve the diff from the
		/// deleted sub-directory.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDiffReport2()
		{
			Path subsub1 = new Path(sub1, "subsub1");
			Path subsubsub1 = new Path(subsub1, "subsubsub1");
			hdfs.Mkdirs(subsubsub1);
			ModifyAndCreateSnapshot(subsubsub1, new Path[] { sub1 });
			// delete subsub1
			hdfs.Delete(subsub1, true);
			// check diff report between s0 and s2
			VerifyDiffReport(sub1, "s0", "s2", new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes("subsub1/subsubsub1")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("subsub1/subsubsub1/file15"
				)), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Delete, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file12")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("subsub1/subsubsub1/file11"
				)), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/file11")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("subsub1/subsubsub1/file13"
				)), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Create, DFSUtil
				.String2Bytes("subsub1/subsubsub1/link13")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("subsub1/subsubsub1/link13"
				)));
			// check diff report between s0 and the current status
			VerifyDiffReport(sub1, "s0", string.Empty, new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes(string.Empty)), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("subsub1")));
		}

		/// <summary>Rename a directory to its prior descendant, and verify the diff report.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDiffReportWithRename()
		{
			Path root = new Path("/");
			Path sdir1 = new Path(root, "dir1");
			Path sdir2 = new Path(root, "dir2");
			Path foo = new Path(sdir1, "foo");
			Path bar = new Path(foo, "bar");
			hdfs.Mkdirs(bar);
			hdfs.Mkdirs(sdir2);
			// create snapshot on root
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s1");
			// /dir1/foo/bar -> /dir2/bar
			Path bar2 = new Path(sdir2, "bar");
			hdfs.Rename(bar, bar2);
			// /dir1/foo -> /dir2/bar/foo
			Path foo2 = new Path(bar2, "foo");
			hdfs.Rename(foo, foo2);
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s2");
			// let's delete /dir2 to make things more complicated
			hdfs.Delete(sdir2, true);
			VerifyDiffReport(root, "s1", "s2", new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes(string.Empty)), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("dir1")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Rename, DFSUtil.String2Bytes("dir1/foo"), DFSUtil.String2Bytes
				("dir2/bar/foo")), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes("dir2")), new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes("dir1/foo/bar")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("dir1/foo")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Rename, DFSUtil.String2Bytes("dir1/foo/bar"), DFSUtil
				.String2Bytes("dir2/bar")));
		}

		/// <summary>
		/// Rename a file/dir outside of the snapshottable dir should be reported as
		/// deleted.
		/// </summary>
		/// <remarks>
		/// Rename a file/dir outside of the snapshottable dir should be reported as
		/// deleted. Rename a file/dir from outside should be reported as created.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDiffReportWithRenameOutside()
		{
			Path root = new Path("/");
			Path dir1 = new Path(root, "dir1");
			Path dir2 = new Path(root, "dir2");
			Path foo = new Path(dir1, "foo");
			Path fileInFoo = new Path(foo, "file");
			Path bar = new Path(dir2, "bar");
			Path fileInBar = new Path(bar, "file");
			DFSTestUtil.CreateFile(hdfs, fileInFoo, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, fileInBar, Blocksize, Replication, seed);
			// create snapshot on /dir1
			SnapshotTestHelper.CreateSnapshot(hdfs, dir1, "s0");
			// move bar into dir1
			Path newBar = new Path(dir1, "newBar");
			hdfs.Rename(bar, newBar);
			// move foo out of dir1 into dir2
			Path newFoo = new Path(dir2, "new");
			hdfs.Rename(foo, newFoo);
			SnapshotTestHelper.CreateSnapshot(hdfs, dir1, "s1");
			VerifyDiffReport(dir1, "s0", "s1", new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes(string.Empty)), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes(newBar.GetName())), new 
				SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes
				(foo.GetName())));
		}

		/// <summary>
		/// Renaming a file/dir then delete the ancestor dir of the rename target
		/// should be reported as deleted.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDiffReportWithRenameAndDelete()
		{
			Path root = new Path("/");
			Path dir1 = new Path(root, "dir1");
			Path dir2 = new Path(root, "dir2");
			Path foo = new Path(dir1, "foo");
			Path fileInFoo = new Path(foo, "file");
			Path bar = new Path(dir2, "bar");
			Path fileInBar = new Path(bar, "file");
			DFSTestUtil.CreateFile(hdfs, fileInFoo, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, fileInBar, Blocksize, Replication, seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s0");
			hdfs.Rename(fileInFoo, fileInBar, Options.Rename.Overwrite);
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s1");
			VerifyDiffReport(root, "s0", "s1", new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes(string.Empty)), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("dir1/foo")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("dir2/bar")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("dir2/bar/file")), new 
				SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.Rename, DFSUtil.String2Bytes
				("dir1/foo/file"), DFSUtil.String2Bytes("dir2/bar/file")));
			// delete bar
			hdfs.Delete(bar, true);
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s2");
			VerifyDiffReport(root, "s0", "s2", new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes(string.Empty)), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("dir1/foo")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("dir2")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("dir2/bar")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Delete, DFSUtil.String2Bytes("dir1/foo/file")));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDiffReportWithRenameToNewDir()
		{
			Path root = new Path("/");
			Path foo = new Path(root, "foo");
			Path fileInFoo = new Path(foo, "file");
			DFSTestUtil.CreateFile(hdfs, fileInFoo, Blocksize, Replication, seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s0");
			Path bar = new Path(root, "bar");
			hdfs.Mkdirs(bar);
			Path fileInBar = new Path(bar, "file");
			hdfs.Rename(fileInFoo, fileInBar);
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s1");
			VerifyDiffReport(root, "s0", "s1", new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes(string.Empty)), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("foo")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Create, DFSUtil.String2Bytes("bar")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Rename, DFSUtil.String2Bytes("foo/file"), DFSUtil.String2Bytes
				("bar/file")));
		}

		/// <summary>Rename a file and then append some data to it</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDiffReportWithRenameAndAppend()
		{
			Path root = new Path("/");
			Path foo = new Path(root, "foo");
			DFSTestUtil.CreateFile(hdfs, foo, Blocksize, Replication, seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s0");
			Path bar = new Path(root, "bar");
			hdfs.Rename(foo, bar);
			DFSTestUtil.AppendFile(hdfs, bar, 10);
			// append 10 bytes
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s1");
			// we always put modification on the file before rename
			VerifyDiffReport(root, "s0", "s1", new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType
				.Modify, DFSUtil.String2Bytes(string.Empty)), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("foo")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Rename, DFSUtil.String2Bytes("foo"), DFSUtil.String2Bytes
				("bar")));
		}

		/// <summary>
		/// Nested renamed dir/file and the withNameList in the WithCount node of the
		/// parental directory is empty due to snapshot deletion.
		/// </summary>
		/// <remarks>
		/// Nested renamed dir/file and the withNameList in the WithCount node of the
		/// parental directory is empty due to snapshot deletion. See HDFS-6996 for
		/// details.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDiffReportWithRenameAndSnapshotDeletion()
		{
			Path root = new Path("/");
			Path foo = new Path(root, "foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Replication, seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s0");
			// rename /foo to /foo2
			Path foo2 = new Path(root, "foo2");
			hdfs.Rename(foo, foo2);
			// now /foo/bar becomes /foo2/bar
			Path bar2 = new Path(foo2, "bar");
			// delete snapshot s0 so that the withNameList inside of the WithCount node
			// of foo becomes empty
			hdfs.DeleteSnapshot(root, "s0");
			// create snapshot s1 and rename bar again
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s1");
			Path bar3 = new Path(foo2, "bar-new");
			hdfs.Rename(bar2, bar3);
			// we always put modification on the file before rename
			VerifyDiffReport(root, "s1", string.Empty, new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes(string.Empty)), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Modify, DFSUtil.String2Bytes("foo2")), new SnapshotDiffReport.DiffReportEntry
				(SnapshotDiffReport.DiffType.Rename, DFSUtil.String2Bytes("foo2/bar"), DFSUtil.String2Bytes
				("foo2/bar-new")));
		}

		public TestSnapshotDiffReport()
		{
			sub1 = new Path(dir, "sub1");
		}
	}
}
