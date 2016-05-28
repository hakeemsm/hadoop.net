using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Test for renaming snapshot</summary>
	public class TestSnapshotRename
	{
		internal const long seed = 0;

		internal const short Replication = 3;

		internal const long Blocksize = 1024;

		private readonly Path dir = new Path("/TestSnapshot");

		private readonly Path sub1;

		private readonly Path file1;

		internal Configuration conf;

		internal MiniDFSCluster cluster;

		internal FSNamesystem fsn;

		internal DistributedFileSystem hdfs;

		internal FSDirectory fsdir;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
			fsdir = fsn.GetFSDirectory();
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

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		/// <summary>Check the correctness of snapshot list within snapshottable dir</summary>
		private void CheckSnapshotList(INodeDirectory srcRoot, string[] sortedNames, string
			[] names)
		{
			NUnit.Framework.Assert.IsTrue(srcRoot.IsSnapshottable());
			ReadOnlyList<Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot> listByName
				 = srcRoot.GetDirectorySnapshottableFeature().GetSnapshotList();
			NUnit.Framework.Assert.AreEqual(sortedNames.Length, listByName.Size());
			for (int i = 0; i < listByName.Size(); i++)
			{
				NUnit.Framework.Assert.AreEqual(sortedNames[i], listByName.Get(i).GetRoot().GetLocalName
					());
			}
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> listByTime = srcRoot.GetDiffs()
				.AsList();
			NUnit.Framework.Assert.AreEqual(names.Length, listByTime.Count);
			for (int i_1 = 0; i_1 < listByTime.Count; i_1++)
			{
				Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s = srcRoot.GetDirectorySnapshottableFeature
					().GetSnapshotById(listByTime[i_1].GetSnapshotId());
				NUnit.Framework.Assert.AreEqual(names[i_1], s.GetRoot().GetLocalName());
			}
		}

		/// <summary>
		/// Rename snapshot(s), and check the correctness of the snapshot list within
		/// <see cref="INodeDirectorySnapshottable"/>
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotList()
		{
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			// Create three snapshots for sub1
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, "s2");
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, "s3");
			// Rename s3 to s22
			hdfs.RenameSnapshot(sub1, "s3", "s22");
			// Check the snapshots list
			INodeDirectory srcRoot = fsdir.GetINode(sub1.ToString()).AsDirectory();
			CheckSnapshotList(srcRoot, new string[] { "s1", "s2", "s22" }, new string[] { "s1"
				, "s2", "s22" });
			// Rename s1 to s4
			hdfs.RenameSnapshot(sub1, "s1", "s4");
			CheckSnapshotList(srcRoot, new string[] { "s2", "s22", "s4" }, new string[] { "s4"
				, "s2", "s22" });
			// Rename s22 to s0
			hdfs.RenameSnapshot(sub1, "s22", "s0");
			CheckSnapshotList(srcRoot, new string[] { "s0", "s2", "s4" }, new string[] { "s4"
				, "s2", "s0" });
		}

		/// <summary>Test FileStatus of snapshot file before/after rename</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotRename()
		{
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			// Create snapshot for sub1
			Path snapshotRoot = SnapshotTestHelper.CreateSnapshot(hdfs, sub1, "s1");
			Path ssPath = new Path(snapshotRoot, file1.GetName());
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(ssPath));
			FileStatus statusBeforeRename = hdfs.GetFileStatus(ssPath);
			// Rename the snapshot
			hdfs.RenameSnapshot(sub1, "s1", "s2");
			// <sub1>/.snapshot/s1/file1 should no longer exist
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(ssPath));
			snapshotRoot = SnapshotTestHelper.GetSnapshotRoot(sub1, "s2");
			ssPath = new Path(snapshotRoot, file1.GetName());
			// Instead, <sub1>/.snapshot/s2/file1 should exist
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(ssPath));
			FileStatus statusAfterRename = hdfs.GetFileStatus(ssPath);
			// FileStatus of the snapshot should not change except the path
			NUnit.Framework.Assert.IsFalse(statusBeforeRename.Equals(statusAfterRename));
			statusBeforeRename.SetPath(statusAfterRename.GetPath());
			NUnit.Framework.Assert.AreEqual(statusBeforeRename.ToString(), statusAfterRename.
				ToString());
		}

		/// <summary>Test rename a non-existing snapshot</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRenameNonExistingSnapshot()
		{
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			// Create snapshot for sub1
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, "s1");
			exception.Expect(typeof(SnapshotException));
			string error = "The snapshot wrongName does not exist for directory " + sub1.ToString
				();
			exception.ExpectMessage(error);
			hdfs.RenameSnapshot(sub1, "wrongName", "s2");
		}

		/// <summary>Test rename a snapshot to another existing snapshot</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRenameToExistingSnapshot()
		{
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			// Create snapshots for sub1
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, "s1");
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, "s2");
			exception.Expect(typeof(SnapshotException));
			string error = "The snapshot s2 already exists for directory " + sub1.ToString();
			exception.ExpectMessage(error);
			hdfs.RenameSnapshot(sub1, "s1", "s2");
		}

		/// <summary>Test renaming a snapshot with illegal name</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameWithIllegalName()
		{
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			// Create snapshots for sub1
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, "s1");
			string name1 = HdfsConstants.DotSnapshotDir;
			try
			{
				hdfs.RenameSnapshot(sub1, "s1", name1);
				NUnit.Framework.Assert.Fail("Exception expected when an illegal name is given for rename"
					);
			}
			catch (RemoteException e)
			{
				string errorMsg = "\"" + HdfsConstants.DotSnapshotDir + "\" is a reserved name.";
				GenericTestUtils.AssertExceptionContains(errorMsg, e);
			}
			string errorMsg_1 = "Snapshot name cannot contain \"" + Path.Separator + "\"";
			string[] badNames = new string[] { "foo" + Path.Separator, Path.Separator + "foo"
				, Path.Separator, "foo" + Path.Separator + "bar" };
			foreach (string badName in badNames)
			{
				try
				{
					hdfs.RenameSnapshot(sub1, "s1", badName);
					NUnit.Framework.Assert.Fail("Exception expected when an illegal name is given");
				}
				catch (RemoteException e)
				{
					GenericTestUtils.AssertExceptionContains(errorMsg_1, e);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameSnapshotCommandWithIllegalArguments()
		{
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			TextWriter psOut = new TextWriter(@out);
			Runtime.SetOut(psOut);
			Runtime.SetErr(psOut);
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			string[] argv1 = new string[] { "-renameSnapshot", "/tmp", "s1" };
			int val = shell.Run(argv1);
			NUnit.Framework.Assert.IsTrue(val == -1);
			NUnit.Framework.Assert.IsTrue(@out.ToString().Contains(argv1[0] + ": Incorrect number of arguments."
				));
			@out.Reset();
			string[] argv2 = new string[] { "-renameSnapshot", "/tmp", "s1", "s2", "s3" };
			val = shell.Run(argv2);
			NUnit.Framework.Assert.IsTrue(val == -1);
			NUnit.Framework.Assert.IsTrue(@out.ToString().Contains(argv2[0] + ": Incorrect number of arguments."
				));
			psOut.Close();
			@out.Close();
		}

		public TestSnapshotRename()
		{
			sub1 = new Path(dir, "sub1");
			file1 = new Path(sub1, "file1");
		}
	}
}
