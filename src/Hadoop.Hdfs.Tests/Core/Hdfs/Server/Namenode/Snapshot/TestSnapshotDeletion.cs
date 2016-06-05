using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Tests snapshot deletion.</summary>
	public class TestSnapshotDeletion
	{
		protected internal const long seed = 0;

		protected internal const short Replication = 3;

		protected internal const short Replication1 = 2;

		protected internal const long Blocksize = 1024;

		private readonly Path dir = new Path("/TestSnapshot");

		private readonly Path sub;

		private readonly Path subsub;

		protected internal Configuration conf;

		protected internal MiniDFSCluster cluster;

		protected internal FSNamesystem fsn;

		protected internal FSDirectory fsdir;

		protected internal BlockManager blockmanager;

		protected internal DistributedFileSystem hdfs;

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Format(true)
				.Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			fsdir = fsn.GetFSDirectory();
			blockmanager = fsn.GetBlockManager();
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

		/// <summary>Deleting snapshottable directory with snapshots must fail.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteDirectoryWithSnapshot()
		{
			Path file0 = new Path(sub, "file0");
			Path file1 = new Path(sub, "file1");
			DFSTestUtil.CreateFile(hdfs, file0, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			// Allow snapshot for sub1, and create snapshot for it
			hdfs.AllowSnapshot(sub);
			hdfs.CreateSnapshot(sub, "s1");
			// Deleting a snapshottable dir with snapshots should fail
			exception.Expect(typeof(RemoteException));
			string error = "The directory " + sub.ToString() + " cannot be deleted since " + 
				sub.ToString() + " is snapshottable and already has snapshots";
			exception.ExpectMessage(error);
			hdfs.Delete(sub, true);
		}

		/// <summary>
		/// Test applying editlog of operation which deletes a snapshottable directory
		/// without snapshots.
		/// </summary>
		/// <remarks>
		/// Test applying editlog of operation which deletes a snapshottable directory
		/// without snapshots. The snapshottable dir list in snapshot manager should be
		/// updated.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestApplyEditLogForDeletion()
		{
			Path foo = new Path("/foo");
			Path bar1 = new Path(foo, "bar1");
			Path bar2 = new Path(foo, "bar2");
			hdfs.Mkdirs(bar1);
			hdfs.Mkdirs(bar2);
			// allow snapshots on bar1 and bar2
			hdfs.AllowSnapshot(bar1);
			hdfs.AllowSnapshot(bar2);
			NUnit.Framework.Assert.AreEqual(2, cluster.GetNamesystem().GetSnapshotManager().GetNumSnapshottableDirs
				());
			NUnit.Framework.Assert.AreEqual(2, cluster.GetNamesystem().GetSnapshotManager().GetSnapshottableDirs
				().Length);
			// delete /foo
			hdfs.Delete(foo, true);
			cluster.RestartNameNode(0);
			// the snapshottable dir list in snapshot manager should be empty
			NUnit.Framework.Assert.AreEqual(0, cluster.GetNamesystem().GetSnapshotManager().GetNumSnapshottableDirs
				());
			NUnit.Framework.Assert.AreEqual(0, cluster.GetNamesystem().GetSnapshotManager().GetSnapshottableDirs
				().Length);
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			hdfs.SaveNamespace();
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			cluster.RestartNameNode(0);
		}

		/// <summary>Deleting directory with snapshottable descendant with snapshots must fail.
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteDirectoryWithSnapshot2()
		{
			Path file0 = new Path(sub, "file0");
			Path file1 = new Path(sub, "file1");
			DFSTestUtil.CreateFile(hdfs, file0, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			Path subfile1 = new Path(subsub, "file0");
			Path subfile2 = new Path(subsub, "file1");
			DFSTestUtil.CreateFile(hdfs, subfile1, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, subfile2, Blocksize, Replication, seed);
			// Allow snapshot for subsub1, and create snapshot for it
			hdfs.AllowSnapshot(subsub);
			hdfs.CreateSnapshot(subsub, "s1");
			// Deleting dir while its descedant subsub1 having snapshots should fail
			exception.Expect(typeof(RemoteException));
			string error = subsub.ToString() + " is snapshottable and already has snapshots";
			exception.ExpectMessage(error);
			hdfs.Delete(dir, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private static INodeDirectory GetDir(FSDirectory fsdir, Path dir)
		{
			string dirStr = dir.ToString();
			return INodeDirectory.ValueOf(fsdir.GetINode(dirStr), dirStr);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckQuotaUsageComputation(Path dirPath, long expectedNs, long expectedDs
			)
		{
			INodeDirectory dirNode = GetDir(fsdir, dirPath);
			NUnit.Framework.Assert.IsTrue(dirNode.IsQuotaSet());
			QuotaCounts q = dirNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			NUnit.Framework.Assert.AreEqual(dirNode.DumpTreeRecursively().ToString(), expectedNs
				, q.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(dirNode.DumpTreeRecursively().ToString(), expectedDs
				, q.GetStorageSpace());
			QuotaCounts counts = new QuotaCounts.Builder().Build();
			dirNode.ComputeQuotaUsage(fsdir.GetBlockStoragePolicySuite(), counts, false);
			NUnit.Framework.Assert.AreEqual(dirNode.DumpTreeRecursively().ToString(), expectedNs
				, counts.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(dirNode.DumpTreeRecursively().ToString(), expectedDs
				, counts.GetStorageSpace());
		}

		/// <summary>
		/// Test deleting a directory which is a descendant of a snapshottable
		/// directory.
		/// </summary>
		/// <remarks>
		/// Test deleting a directory which is a descendant of a snapshottable
		/// directory. In the test we need to cover the following cases:
		/// <pre>
		/// 1. Delete current INodeFile/INodeDirectory without taking any snapshot.
		/// 2. Delete current INodeFile/INodeDirectory while snapshots have been taken
		/// on ancestor(s).
		/// 3. Delete current INodeFileWithSnapshot.
		/// 4. Delete current INodeDirectoryWithSnapshot.
		/// </pre>
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteCurrentFileDirectory()
		{
			// create a folder which will be deleted before taking snapshots
			Path deleteDir = new Path(subsub, "deleteDir");
			Path deleteFile = new Path(deleteDir, "deleteFile");
			// create a directory that we will not change during the whole process.
			Path noChangeDirParent = new Path(sub, "noChangeDirParent");
			Path noChangeDir = new Path(noChangeDirParent, "noChangeDir");
			// create a file that we will not change in the future
			Path noChangeFile = new Path(noChangeDir, "noChangeFile");
			DFSTestUtil.CreateFile(hdfs, deleteFile, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, noChangeFile, Blocksize, Replication, seed);
			// we will change this file's metadata in the future
			Path metaChangeFile1 = new Path(subsub, "metaChangeFile1");
			DFSTestUtil.CreateFile(hdfs, metaChangeFile1, Blocksize, Replication, seed);
			// another file, created under noChangeDir, whose metadata will be changed
			Path metaChangeFile2 = new Path(noChangeDir, "metaChangeFile2");
			DFSTestUtil.CreateFile(hdfs, metaChangeFile2, Blocksize, Replication, seed);
			// Case 1: delete deleteDir before taking snapshots
			hdfs.Delete(deleteDir, true);
			// create snapshot s0
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s0");
			// after creating snapshot s0, create a directory tempdir under dir and then
			// delete dir immediately
			Path tempDir = new Path(dir, "tempdir");
			Path tempFile = new Path(tempDir, "tempfile");
			DFSTestUtil.CreateFile(hdfs, tempFile, Blocksize, Replication, seed);
			INodeFile temp = TestSnapshotBlocksMap.AssertBlockCollection(tempFile.ToString(), 
				1, fsdir, blockmanager);
			BlockInfoContiguous[] blocks = temp.GetBlocks();
			hdfs.Delete(tempDir, true);
			// check dir's quota usage
			CheckQuotaUsageComputation(dir, 8, Blocksize * Replication * 3);
			// check blocks of tempFile
			foreach (BlockInfoContiguous b in blocks)
			{
				NUnit.Framework.Assert.IsNull(blockmanager.GetBlockCollection(b));
			}
			// make a change: create a new file under subsub
			Path newFileAfterS0 = new Path(subsub, "newFile");
			DFSTestUtil.CreateFile(hdfs, newFileAfterS0, Blocksize, Replication, seed);
			// further change: change the replicator factor of metaChangeFile
			hdfs.SetReplication(metaChangeFile1, Replication1);
			hdfs.SetReplication(metaChangeFile2, Replication1);
			// create snapshot s1
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s1");
			// check dir's quota usage
			CheckQuotaUsageComputation(dir, 9L, Blocksize * Replication * 4);
			// get two snapshots for later use
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot0 = fsdir.GetINode
				(dir.ToString()).AsDirectory().GetSnapshot(DFSUtil.String2Bytes("s0"));
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot1 = fsdir.GetINode
				(dir.ToString()).AsDirectory().GetSnapshot(DFSUtil.String2Bytes("s1"));
			// Case 2 + Case 3: delete noChangeDirParent, noChangeFile, and
			// metaChangeFile2. Note that when we directly delete a directory, the 
			// directory will be converted to an INodeDirectoryWithSnapshot. To make
			// sure the deletion goes through an INodeDirectory, we delete the parent
			// of noChangeDir
			hdfs.Delete(noChangeDirParent, true);
			// while deletion, we add a diff for metaChangeFile2 as its snapshot copy
			// for s1, we also add diffs for both sub and noChangeDirParent
			CheckQuotaUsageComputation(dir, 9L, Blocksize * Replication * 4);
			// check the snapshot copy of noChangeDir 
			Path snapshotNoChangeDir = SnapshotTestHelper.GetSnapshotPath(dir, "s1", sub.GetName
				() + "/" + noChangeDirParent.GetName() + "/" + noChangeDir.GetName());
			INodeDirectory snapshotNode = (INodeDirectory)fsdir.GetINode(snapshotNoChangeDir.
				ToString());
			// should still be an INodeDirectory
			NUnit.Framework.Assert.AreEqual(typeof(INodeDirectory), snapshotNode.GetType());
			ReadOnlyList<INode> children = snapshotNode.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			// check 2 children: noChangeFile and metaChangeFile2
			NUnit.Framework.Assert.AreEqual(2, children.Size());
			INode noChangeFileSCopy = children.Get(1);
			NUnit.Framework.Assert.AreEqual(noChangeFile.GetName(), noChangeFileSCopy.GetLocalName
				());
			NUnit.Framework.Assert.AreEqual(typeof(INodeFile), noChangeFileSCopy.GetType());
			TestSnapshotBlocksMap.AssertBlockCollection(new Path(snapshotNoChangeDir, noChangeFileSCopy
				.GetLocalName()).ToString(), 1, fsdir, blockmanager);
			INodeFile metaChangeFile2SCopy = children.Get(0).AsFile();
			NUnit.Framework.Assert.AreEqual(metaChangeFile2.GetName(), metaChangeFile2SCopy.GetLocalName
				());
			NUnit.Framework.Assert.IsTrue(metaChangeFile2SCopy.IsWithSnapshot());
			NUnit.Framework.Assert.IsFalse(metaChangeFile2SCopy.IsUnderConstruction());
			TestSnapshotBlocksMap.AssertBlockCollection(new Path(snapshotNoChangeDir, metaChangeFile2SCopy
				.GetLocalName()).ToString(), 1, fsdir, blockmanager);
			// check the replication factor of metaChangeFile2SCopy
			NUnit.Framework.Assert.AreEqual(Replication1, metaChangeFile2SCopy.GetFileReplication
				(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId));
			NUnit.Framework.Assert.AreEqual(Replication1, metaChangeFile2SCopy.GetFileReplication
				(snapshot1.GetId()));
			NUnit.Framework.Assert.AreEqual(Replication, metaChangeFile2SCopy.GetFileReplication
				(snapshot0.GetId()));
			// Case 4: delete directory sub
			// before deleting sub, we first create a new file under sub
			Path newFile = new Path(sub, "newFile");
			DFSTestUtil.CreateFile(hdfs, newFile, Blocksize, Replication, seed);
			INodeFile newFileNode = TestSnapshotBlocksMap.AssertBlockCollection(newFile.ToString
				(), 1, fsdir, blockmanager);
			blocks = newFileNode.GetBlocks();
			CheckQuotaUsageComputation(dir, 10L, Blocksize * Replication * 5);
			hdfs.Delete(sub, true);
			// while deletion, we add diff for subsub and metaChangeFile1, and remove
			// newFile
			CheckQuotaUsageComputation(dir, 9L, Blocksize * Replication * 4);
			foreach (BlockInfoContiguous b_1 in blocks)
			{
				NUnit.Framework.Assert.IsNull(blockmanager.GetBlockCollection(b_1));
			}
			// make sure the whole subtree of sub is stored correctly in snapshot
			Path snapshotSub = SnapshotTestHelper.GetSnapshotPath(dir, "s1", sub.GetName());
			INodeDirectory snapshotNode4Sub = fsdir.GetINode(snapshotSub.ToString()).AsDirectory
				();
			NUnit.Framework.Assert.IsTrue(snapshotNode4Sub.IsWithSnapshot());
			// the snapshot copy of sub has only one child subsub.
			// newFile should have been destroyed
			NUnit.Framework.Assert.AreEqual(1, snapshotNode4Sub.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId).Size());
			// but should have two children, subsub and noChangeDir, when s1 was taken  
			NUnit.Framework.Assert.AreEqual(2, snapshotNode4Sub.GetChildrenList(snapshot1.GetId
				()).Size());
			// check the snapshot copy of subsub, which is contained in the subtree of
			// sub's snapshot copy
			INode snapshotNode4Subsub = snapshotNode4Sub.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId).Get(0);
			NUnit.Framework.Assert.IsTrue(snapshotNode4Subsub.AsDirectory().IsWithSnapshot());
			NUnit.Framework.Assert.IsTrue(snapshotNode4Sub == snapshotNode4Subsub.GetParent()
				);
			// check the children of subsub
			INodeDirectory snapshotSubsubDir = (INodeDirectory)snapshotNode4Subsub;
			children = snapshotSubsubDir.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			NUnit.Framework.Assert.AreEqual(2, children.Size());
			NUnit.Framework.Assert.AreEqual(children.Get(0).GetLocalName(), metaChangeFile1.GetName
				());
			NUnit.Framework.Assert.AreEqual(children.Get(1).GetLocalName(), newFileAfterS0.GetName
				());
			// only one child before snapshot s0 
			children = snapshotSubsubDir.GetChildrenList(snapshot0.GetId());
			NUnit.Framework.Assert.AreEqual(1, children.Size());
			INode child = children.Get(0);
			NUnit.Framework.Assert.AreEqual(child.GetLocalName(), metaChangeFile1.GetName());
			// check snapshot copy of metaChangeFile1
			INodeFile metaChangeFile1SCopy = child.AsFile();
			NUnit.Framework.Assert.IsTrue(metaChangeFile1SCopy.IsWithSnapshot());
			NUnit.Framework.Assert.IsFalse(metaChangeFile1SCopy.IsUnderConstruction());
			NUnit.Framework.Assert.AreEqual(Replication1, metaChangeFile1SCopy.GetFileReplication
				(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId));
			NUnit.Framework.Assert.AreEqual(Replication1, metaChangeFile1SCopy.GetFileReplication
				(snapshot1.GetId()));
			NUnit.Framework.Assert.AreEqual(Replication, metaChangeFile1SCopy.GetFileReplication
				(snapshot0.GetId()));
		}

		/// <summary>Test deleting the earliest (first) snapshot.</summary>
		/// <remarks>
		/// Test deleting the earliest (first) snapshot. In this simplest scenario, the
		/// snapshots are taken on the same directory, and we do not need to combine
		/// snapshot diffs.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteEarliestSnapshot1()
		{
			// create files under sub
			Path file0 = new Path(sub, "file0");
			Path file1 = new Path(sub, "file1");
			DFSTestUtil.CreateFile(hdfs, file0, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			string snapshotName = "s1";
			try
			{
				hdfs.DeleteSnapshot(sub, snapshotName);
				NUnit.Framework.Assert.Fail("SnapshotException expected: " + sub.ToString() + " is not snapshottable yet"
					);
			}
			catch (Exception e)
			{
				GenericTestUtils.AssertExceptionContains("Directory is not a snapshottable directory: "
					 + sub, e);
			}
			// make sub snapshottable
			hdfs.AllowSnapshot(sub);
			try
			{
				hdfs.DeleteSnapshot(sub, snapshotName);
				NUnit.Framework.Assert.Fail("SnapshotException expected: snapshot " + snapshotName
					 + " does not exist for " + sub.ToString());
			}
			catch (Exception e)
			{
				GenericTestUtils.AssertExceptionContains("Cannot delete snapshot " + snapshotName
					 + " from path " + sub.ToString() + ": the snapshot does not exist.", e);
			}
			// create snapshot s1 for sub
			SnapshotTestHelper.CreateSnapshot(hdfs, sub, snapshotName);
			// check quota usage computation
			CheckQuotaUsageComputation(sub, 3, Blocksize * Replication * 2);
			// delete s1
			hdfs.DeleteSnapshot(sub, snapshotName);
			CheckQuotaUsageComputation(sub, 3, Blocksize * Replication * 2);
			// now we can create a snapshot with the same name
			hdfs.CreateSnapshot(sub, snapshotName);
			CheckQuotaUsageComputation(sub, 3, Blocksize * Replication * 2);
			// create a new file under sub
			Path newFile = new Path(sub, "newFile");
			DFSTestUtil.CreateFile(hdfs, newFile, Blocksize, Replication, seed);
			// create another snapshot s2
			string snapshotName2 = "s2";
			hdfs.CreateSnapshot(sub, snapshotName2);
			CheckQuotaUsageComputation(sub, 4, Blocksize * Replication * 3);
			// Get the filestatus of sub under snapshot s2
			Path ss = SnapshotTestHelper.GetSnapshotPath(sub, snapshotName2, "newFile");
			FileStatus statusBeforeDeletion = hdfs.GetFileStatus(ss);
			// delete s1
			hdfs.DeleteSnapshot(sub, snapshotName);
			CheckQuotaUsageComputation(sub, 4, Blocksize * Replication * 3);
			FileStatus statusAfterDeletion = hdfs.GetFileStatus(ss);
			System.Console.Out.WriteLine("Before deletion: " + statusBeforeDeletion.ToString(
				) + "\n" + "After deletion: " + statusAfterDeletion.ToString());
			NUnit.Framework.Assert.AreEqual(statusBeforeDeletion.ToString(), statusAfterDeletion
				.ToString());
		}

		/// <summary>Test deleting the earliest (first) snapshot.</summary>
		/// <remarks>
		/// Test deleting the earliest (first) snapshot. In this more complicated
		/// scenario, the snapshots are taken across directories.
		/// <pre>
		/// The test covers the following scenarios:
		/// 1. delete the first diff in the diff list of a directory
		/// 2. delete the first diff in the diff list of a file
		/// </pre>
		/// Also, the recursive cleanTree process should cover both INodeFile and
		/// INodeDirectory.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteEarliestSnapshot2()
		{
			Path noChangeDir = new Path(sub, "noChangeDir");
			Path noChangeFile = new Path(noChangeDir, "noChangeFile");
			Path metaChangeFile = new Path(noChangeDir, "metaChangeFile");
			Path metaChangeDir = new Path(noChangeDir, "metaChangeDir");
			Path toDeleteFile = new Path(metaChangeDir, "toDeleteFile");
			DFSTestUtil.CreateFile(hdfs, noChangeFile, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, metaChangeFile, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, toDeleteFile, Blocksize, Replication, seed);
			INodeFile toDeleteFileNode = TestSnapshotBlocksMap.AssertBlockCollection(toDeleteFile
				.ToString(), 1, fsdir, blockmanager);
			BlockInfoContiguous[] blocks = toDeleteFileNode.GetBlocks();
			// create snapshot s0 on dir
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s0");
			CheckQuotaUsageComputation(dir, 7, 3 * Blocksize * Replication);
			// delete /TestSnapshot/sub/noChangeDir/metaChangeDir/toDeleteFile
			hdfs.Delete(toDeleteFile, true);
			// the deletion adds diff of toDeleteFile and metaChangeDir
			CheckQuotaUsageComputation(dir, 7, 3 * Blocksize * Replication);
			// change metadata of /TestSnapshot/sub/noChangeDir/metaChangeDir and
			// /TestSnapshot/sub/noChangeDir/metaChangeFile
			hdfs.SetReplication(metaChangeFile, Replication1);
			hdfs.SetOwner(metaChangeDir, "unknown", "unknown");
			CheckQuotaUsageComputation(dir, 7, 3 * Blocksize * Replication);
			// create snapshot s1 on dir
			hdfs.CreateSnapshot(dir, "s1");
			CheckQuotaUsageComputation(dir, 7, 3 * Blocksize * Replication);
			// delete snapshot s0
			hdfs.DeleteSnapshot(dir, "s0");
			// namespace: remove toDeleteFile and its diff, metaChangeFile's diff, 
			// metaChangeDir's diff, dir's diff. diskspace: remove toDeleteFile, and 
			// metaChangeFile's replication factor decreases
			CheckQuotaUsageComputation(dir, 6, 2 * Blocksize * Replication - Blocksize);
			foreach (BlockInfoContiguous b in blocks)
			{
				NUnit.Framework.Assert.IsNull(blockmanager.GetBlockCollection(b));
			}
			// check 1. there is no snapshot s0
			INodeDirectory dirNode = fsdir.GetINode(dir.ToString()).AsDirectory();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot0 = dirNode.GetSnapshot
				(DFSUtil.String2Bytes("s0"));
			NUnit.Framework.Assert.IsNull(snapshot0);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot snapshot1 = dirNode.GetSnapshot
				(DFSUtil.String2Bytes("s1"));
			DirectoryWithSnapshotFeature.DirectoryDiffList diffList = dirNode.GetDiffs();
			NUnit.Framework.Assert.AreEqual(1, diffList.AsList().Count);
			NUnit.Framework.Assert.AreEqual(snapshot1.GetId(), diffList.GetLast().GetSnapshotId
				());
			diffList = fsdir.GetINode(metaChangeDir.ToString()).AsDirectory().GetDiffs();
			NUnit.Framework.Assert.AreEqual(0, diffList.AsList().Count);
			// check 2. noChangeDir and noChangeFile are still there
			INodeDirectory noChangeDirNode = (INodeDirectory)fsdir.GetINode(noChangeDir.ToString
				());
			NUnit.Framework.Assert.AreEqual(typeof(INodeDirectory), noChangeDirNode.GetType()
				);
			INodeFile noChangeFileNode = (INodeFile)fsdir.GetINode(noChangeFile.ToString());
			NUnit.Framework.Assert.AreEqual(typeof(INodeFile), noChangeFileNode.GetType());
			TestSnapshotBlocksMap.AssertBlockCollection(noChangeFile.ToString(), 1, fsdir, blockmanager
				);
			// check 3: current metadata of metaChangeFile and metaChangeDir
			FileStatus status = hdfs.GetFileStatus(metaChangeDir);
			NUnit.Framework.Assert.AreEqual("unknown", status.GetOwner());
			NUnit.Framework.Assert.AreEqual("unknown", status.GetGroup());
			status = hdfs.GetFileStatus(metaChangeFile);
			NUnit.Framework.Assert.AreEqual(Replication1, status.GetReplication());
			TestSnapshotBlocksMap.AssertBlockCollection(metaChangeFile.ToString(), 1, fsdir, 
				blockmanager);
			// check 4: no snapshot copy for toDeleteFile
			try
			{
				status = hdfs.GetFileStatus(toDeleteFile);
				NUnit.Framework.Assert.Fail("should throw FileNotFoundException");
			}
			catch (FileNotFoundException e)
			{
				GenericTestUtils.AssertExceptionContains("File does not exist: " + toDeleteFile.ToString
					(), e);
			}
			Path toDeleteFileInSnapshot = SnapshotTestHelper.GetSnapshotPath(dir, "s0", Sharpen.Runtime.Substring
				(toDeleteFile.ToString(), dir.ToString().Length));
			try
			{
				status = hdfs.GetFileStatus(toDeleteFileInSnapshot);
				NUnit.Framework.Assert.Fail("should throw FileNotFoundException");
			}
			catch (FileNotFoundException e)
			{
				GenericTestUtils.AssertExceptionContains("File does not exist: " + toDeleteFileInSnapshot
					.ToString(), e);
			}
		}

		/// <summary>
		/// Delete a snapshot that is taken before a directory deletion,
		/// directory diff list should be combined correctly.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteSnapshot1()
		{
			Path root = new Path("/");
			Path dir = new Path("/dir1");
			Path file1 = new Path(dir, "file1");
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			hdfs.AllowSnapshot(root);
			hdfs.CreateSnapshot(root, "s1");
			Path file2 = new Path(dir, "file2");
			DFSTestUtil.CreateFile(hdfs, file2, Blocksize, Replication, seed);
			hdfs.CreateSnapshot(root, "s2");
			// delete file
			hdfs.Delete(file1, true);
			hdfs.Delete(file2, true);
			// delete directory
			NUnit.Framework.Assert.IsTrue(hdfs.Delete(dir, false));
			// delete second snapshot
			hdfs.DeleteSnapshot(root, "s2");
			NameNodeAdapter.EnterSafeMode(cluster.GetNameNode(), false);
			NameNodeAdapter.SaveNamespace(cluster.GetNameNode());
			// restart NN
			cluster.RestartNameNodes();
		}

		/// <summary>
		/// Delete a snapshot that is taken before a directory deletion (recursively),
		/// directory diff list should be combined correctly.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteSnapshot2()
		{
			Path root = new Path("/");
			Path dir = new Path("/dir1");
			Path file1 = new Path(dir, "file1");
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			hdfs.AllowSnapshot(root);
			hdfs.CreateSnapshot(root, "s1");
			Path file2 = new Path(dir, "file2");
			DFSTestUtil.CreateFile(hdfs, file2, Blocksize, Replication, seed);
			INodeFile file2Node = fsdir.GetINode(file2.ToString()).AsFile();
			long file2NodeId = file2Node.GetId();
			hdfs.CreateSnapshot(root, "s2");
			// delete directory recursively
			NUnit.Framework.Assert.IsTrue(hdfs.Delete(dir, true));
			NUnit.Framework.Assert.IsNotNull(fsdir.GetInode(file2NodeId));
			// delete second snapshot
			hdfs.DeleteSnapshot(root, "s2");
			NUnit.Framework.Assert.IsTrue(fsdir.GetInode(file2NodeId) == null);
			NameNodeAdapter.EnterSafeMode(cluster.GetNameNode(), false);
			NameNodeAdapter.SaveNamespace(cluster.GetNameNode());
			// restart NN
			cluster.RestartNameNodes();
		}

		/// <summary>
		/// Test deleting snapshots in a more complicated scenario: need to combine
		/// snapshot diffs, but no need to handle diffs distributed in a dir tree
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCombineSnapshotDiff1()
		{
			TestCombineSnapshotDiffImpl(sub, string.Empty, 1);
		}

		/// <summary>
		/// Test deleting snapshots in more complicated scenarios (snapshot diffs are
		/// distributed in the directory sub-tree)
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCombineSnapshotDiff2()
		{
			TestCombineSnapshotDiffImpl(sub, "subsub1/subsubsub1/", 3);
		}

		/// <summary>
		/// When combine two snapshots, make sure files/directories created after the
		/// prior snapshot get destroyed.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCombineSnapshotDiff3()
		{
			// create initial dir and subdir
			Path dir = new Path("/dir");
			Path subDir1 = new Path(dir, "subdir1");
			Path subDir2 = new Path(dir, "subdir2");
			hdfs.Mkdirs(subDir2);
			Path subsubDir = new Path(subDir1, "subsubdir");
			hdfs.Mkdirs(subsubDir);
			// take snapshots on subdir and dir
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s1");
			// create new dir under initial dir
			Path newDir = new Path(subsubDir, "newdir");
			Path newFile = new Path(newDir, "newfile");
			DFSTestUtil.CreateFile(hdfs, newFile, Blocksize, Replication, seed);
			Path newFile2 = new Path(subDir2, "newfile");
			DFSTestUtil.CreateFile(hdfs, newFile2, Blocksize, Replication, seed);
			// create another snapshot
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s2");
			CheckQuotaUsageComputation(dir, 7, Blocksize * 2 * Replication);
			// delete subsubdir and subDir2
			hdfs.Delete(subsubDir, true);
			hdfs.Delete(subDir2, true);
			// add diff of s2 to subDir1, subsubDir, and subDir2
			CheckQuotaUsageComputation(dir, 7, Blocksize * 2 * Replication);
			// delete snapshot s2
			hdfs.DeleteSnapshot(dir, "s2");
			// delete s2 diff in dir, subDir2, and subsubDir. Delete newFile, newDir,
			// and newFile2. Rename s2 diff to s1 for subDir1 
			CheckQuotaUsageComputation(dir, 4, 0);
			// Check rename of snapshot diff in subDir1
			Path subdir1_s1 = SnapshotTestHelper.GetSnapshotPath(dir, "s1", subDir1.GetName()
				);
			Path subdir1_s2 = SnapshotTestHelper.GetSnapshotPath(dir, "s2", subDir1.GetName()
				);
			NUnit.Framework.Assert.IsTrue(hdfs.Exists(subdir1_s1));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(subdir1_s2));
		}

		/// <summary>Test snapshot deletion</summary>
		/// <param name="snapshotRoot">The dir where the snapshots are created</param>
		/// <param name="modDirStr">
		/// The snapshotRoot itself or one of its sub-directory,
		/// where the modifications happen. It is represented as a relative
		/// path to the snapshotRoot.
		/// </param>
		/// <exception cref="System.Exception"/>
		private void TestCombineSnapshotDiffImpl(Path snapshotRoot, string modDirStr, int
			 dirNodeNum)
		{
			Path modDir = modDirStr.IsEmpty() ? snapshotRoot : new Path(snapshotRoot, modDirStr
				);
			Path file10 = new Path(modDir, "file10");
			Path file11 = new Path(modDir, "file11");
			Path file12 = new Path(modDir, "file12");
			Path file13 = new Path(modDir, "file13");
			Path file14 = new Path(modDir, "file14");
			Path file15 = new Path(modDir, "file15");
			DFSTestUtil.CreateFile(hdfs, file10, Blocksize, Replication1, seed);
			DFSTestUtil.CreateFile(hdfs, file11, Blocksize, Replication1, seed);
			DFSTestUtil.CreateFile(hdfs, file12, Blocksize, Replication1, seed);
			DFSTestUtil.CreateFile(hdfs, file13, Blocksize, Replication1, seed);
			// create snapshot s1 for snapshotRoot
			SnapshotTestHelper.CreateSnapshot(hdfs, snapshotRoot, "s1");
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 4, 8 * Blocksize);
			// delete file11
			hdfs.Delete(file11, true);
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 4, 8 * Blocksize);
			// modify file12
			hdfs.SetReplication(file12, Replication);
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 4, 9 * Blocksize);
			// modify file13
			hdfs.SetReplication(file13, Replication);
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 4, 10 * Blocksize);
			// create file14
			DFSTestUtil.CreateFile(hdfs, file14, Blocksize, Replication, seed);
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 5, 13 * Blocksize);
			// create file15
			DFSTestUtil.CreateFile(hdfs, file15, Blocksize, Replication, seed);
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 6, 16 * Blocksize);
			// create snapshot s2 for snapshotRoot
			hdfs.CreateSnapshot(snapshotRoot, "s2");
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 6, 16 * Blocksize);
			// create file11 again: (0, d) + (c, 0)
			DFSTestUtil.CreateFile(hdfs, file11, Blocksize, Replication, seed);
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 7, 19 * Blocksize);
			// delete file12
			hdfs.Delete(file12, true);
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 7, 19 * Blocksize);
			// modify file13
			hdfs.SetReplication(file13, (short)(Replication - 2));
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 7, 19 * Blocksize);
			// delete file14: (c, 0) + (0, d)
			hdfs.Delete(file14, true);
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 7, 19 * Blocksize);
			// modify file15
			hdfs.SetReplication(file15, Replication1);
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 7, 19 * Blocksize);
			// create snapshot s3 for snapshotRoot
			hdfs.CreateSnapshot(snapshotRoot, "s3");
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 7, 19 * Blocksize);
			// modify file10, to check if the posterior diff was set correctly
			hdfs.SetReplication(file10, Replication);
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 7, 20 * Blocksize);
			Path file10_s1 = SnapshotTestHelper.GetSnapshotPath(snapshotRoot, "s1", modDirStr
				 + "file10");
			Path file11_s1 = SnapshotTestHelper.GetSnapshotPath(snapshotRoot, "s1", modDirStr
				 + "file11");
			Path file12_s1 = SnapshotTestHelper.GetSnapshotPath(snapshotRoot, "s1", modDirStr
				 + "file12");
			Path file13_s1 = SnapshotTestHelper.GetSnapshotPath(snapshotRoot, "s1", modDirStr
				 + "file13");
			Path file14_s2 = SnapshotTestHelper.GetSnapshotPath(snapshotRoot, "s2", modDirStr
				 + "file14");
			Path file15_s2 = SnapshotTestHelper.GetSnapshotPath(snapshotRoot, "s2", modDirStr
				 + "file15");
			FileStatus statusBeforeDeletion10 = hdfs.GetFileStatus(file10_s1);
			FileStatus statusBeforeDeletion11 = hdfs.GetFileStatus(file11_s1);
			FileStatus statusBeforeDeletion12 = hdfs.GetFileStatus(file12_s1);
			FileStatus statusBeforeDeletion13 = hdfs.GetFileStatus(file13_s1);
			INodeFile file14Node = TestSnapshotBlocksMap.AssertBlockCollection(file14_s2.ToString
				(), 1, fsdir, blockmanager);
			BlockInfoContiguous[] blocks_14 = file14Node.GetBlocks();
			TestSnapshotBlocksMap.AssertBlockCollection(file15_s2.ToString(), 1, fsdir, blockmanager
				);
			// delete s2, in which process we need to combine the diff in s2 to s1
			hdfs.DeleteSnapshot(snapshotRoot, "s2");
			CheckQuotaUsageComputation(snapshotRoot, dirNodeNum + 6, 14 * Blocksize);
			// check the correctness of s1
			FileStatus statusAfterDeletion10 = hdfs.GetFileStatus(file10_s1);
			FileStatus statusAfterDeletion11 = hdfs.GetFileStatus(file11_s1);
			FileStatus statusAfterDeletion12 = hdfs.GetFileStatus(file12_s1);
			FileStatus statusAfterDeletion13 = hdfs.GetFileStatus(file13_s1);
			NUnit.Framework.Assert.AreEqual(statusBeforeDeletion10.ToString(), statusAfterDeletion10
				.ToString());
			NUnit.Framework.Assert.AreEqual(statusBeforeDeletion11.ToString(), statusAfterDeletion11
				.ToString());
			NUnit.Framework.Assert.AreEqual(statusBeforeDeletion12.ToString(), statusAfterDeletion12
				.ToString());
			NUnit.Framework.Assert.AreEqual(statusBeforeDeletion13.ToString(), statusAfterDeletion13
				.ToString());
			TestSnapshotBlocksMap.AssertBlockCollection(file10_s1.ToString(), 1, fsdir, blockmanager
				);
			TestSnapshotBlocksMap.AssertBlockCollection(file11_s1.ToString(), 1, fsdir, blockmanager
				);
			TestSnapshotBlocksMap.AssertBlockCollection(file12_s1.ToString(), 1, fsdir, blockmanager
				);
			TestSnapshotBlocksMap.AssertBlockCollection(file13_s1.ToString(), 1, fsdir, blockmanager
				);
			// make sure file14 and file15 are not included in s1
			Path file14_s1 = SnapshotTestHelper.GetSnapshotPath(snapshotRoot, "s1", modDirStr
				 + "file14");
			Path file15_s1 = SnapshotTestHelper.GetSnapshotPath(snapshotRoot, "s1", modDirStr
				 + "file15");
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(file14_s1));
			NUnit.Framework.Assert.IsFalse(hdfs.Exists(file15_s1));
			foreach (BlockInfoContiguous b in blocks_14)
			{
				NUnit.Framework.Assert.IsNull(blockmanager.GetBlockCollection(b));
			}
			INodeFile nodeFile13 = (INodeFile)fsdir.GetINode(file13.ToString());
			NUnit.Framework.Assert.AreEqual(Replication1, nodeFile13.GetBlockReplication());
			TestSnapshotBlocksMap.AssertBlockCollection(file13.ToString(), 1, fsdir, blockmanager
				);
			INodeFile nodeFile12 = (INodeFile)fsdir.GetINode(file12_s1.ToString());
			NUnit.Framework.Assert.AreEqual(Replication1, nodeFile12.GetBlockReplication());
		}

		/// <summary>Test deleting snapshots with modification on the metadata of directory</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteSnapshotWithDirModification()
		{
			Path file = new Path(sub, "file");
			DFSTestUtil.CreateFile(hdfs, file, Blocksize, Replication, seed);
			hdfs.SetOwner(sub, "user1", "group1");
			// create snapshot s1 for sub1, and change the metadata of sub1
			SnapshotTestHelper.CreateSnapshot(hdfs, sub, "s1");
			CheckQuotaUsageComputation(sub, 2, Blocksize * 3);
			hdfs.SetOwner(sub, "user2", "group2");
			CheckQuotaUsageComputation(sub, 2, Blocksize * 3);
			// create snapshot s2 for sub1, but do not modify sub1 afterwards
			hdfs.CreateSnapshot(sub, "s2");
			CheckQuotaUsageComputation(sub, 2, Blocksize * 3);
			// create snapshot s3 for sub1, and change the metadata of sub1
			hdfs.CreateSnapshot(sub, "s3");
			CheckQuotaUsageComputation(sub, 2, Blocksize * 3);
			hdfs.SetOwner(sub, "user3", "group3");
			CheckQuotaUsageComputation(sub, 2, Blocksize * 3);
			// delete snapshot s3
			hdfs.DeleteSnapshot(sub, "s3");
			CheckQuotaUsageComputation(sub, 2, Blocksize * 3);
			// check sub1's metadata in snapshot s2
			FileStatus statusOfS2 = hdfs.GetFileStatus(new Path(sub, HdfsConstants.DotSnapshotDir
				 + "/s2"));
			NUnit.Framework.Assert.AreEqual("user2", statusOfS2.GetOwner());
			NUnit.Framework.Assert.AreEqual("group2", statusOfS2.GetGroup());
			// delete snapshot s2
			hdfs.DeleteSnapshot(sub, "s2");
			CheckQuotaUsageComputation(sub, 2, Blocksize * 3);
			// check sub1's metadata in snapshot s1
			FileStatus statusOfS1 = hdfs.GetFileStatus(new Path(sub, HdfsConstants.DotSnapshotDir
				 + "/s1"));
			NUnit.Framework.Assert.AreEqual("user1", statusOfS1.GetOwner());
			NUnit.Framework.Assert.AreEqual("group1", statusOfS1.GetGroup());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteSnapshotWithPermissionsDisabled()
		{
			cluster.Shutdown();
			Configuration newConf = new Configuration(conf);
			newConf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, false);
			cluster = new MiniDFSCluster.Builder(newConf).NumDataNodes(0).Build();
			cluster.WaitActive();
			hdfs = cluster.GetFileSystem();
			Path path = new Path("/dir");
			hdfs.Mkdirs(path);
			hdfs.AllowSnapshot(path);
			hdfs.Mkdirs(new Path(path, "/test"));
			hdfs.CreateSnapshot(path, "s1");
			UserGroupInformation anotherUser = UserGroupInformation.CreateRemoteUser("anotheruser"
				);
			anotherUser.DoAs(new _PrivilegedAction_912(this, path));
		}

		private sealed class _PrivilegedAction_912 : PrivilegedAction<object>
		{
			public _PrivilegedAction_912(TestSnapshotDeletion _enclosing, Path path)
			{
				this._enclosing = _enclosing;
				this.path = path;
			}

			public object Run()
			{
				DistributedFileSystem anotherUserFS = null;
				try
				{
					anotherUserFS = this._enclosing.cluster.GetFileSystem();
					anotherUserFS.DeleteSnapshot(path, "s1");
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.Fail("Failed to delete snapshot : " + e.GetLocalizedMessage
						());
				}
				finally
				{
					IOUtils.CloseStream(anotherUserFS);
				}
				return null;
			}

			private readonly TestSnapshotDeletion _enclosing;

			private readonly Path path;
		}

		/// <summary>
		/// A test covering the case where the snapshot diff to be deleted is renamed
		/// to its previous snapshot.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRenameSnapshotDiff()
		{
			cluster.GetNamesystem().GetSnapshotManager().SetAllowNestedSnapshots(true);
			Path subFile0 = new Path(sub, "file0");
			Path subsubFile0 = new Path(subsub, "file0");
			DFSTestUtil.CreateFile(hdfs, subFile0, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, subsubFile0, Blocksize, Replication, seed);
			hdfs.SetOwner(subsub, "owner", "group");
			// create snapshot s0 on sub
			SnapshotTestHelper.CreateSnapshot(hdfs, sub, "s0");
			CheckQuotaUsageComputation(sub, 4, Blocksize * 6);
			// make some changes on both sub and subsub
			Path subFile1 = new Path(sub, "file1");
			Path subsubFile1 = new Path(subsub, "file1");
			DFSTestUtil.CreateFile(hdfs, subFile1, Blocksize, Replication1, seed);
			DFSTestUtil.CreateFile(hdfs, subsubFile1, Blocksize, Replication, seed);
			CheckQuotaUsageComputation(sub, 6, Blocksize * 11);
			// create snapshot s1 on sub
			SnapshotTestHelper.CreateSnapshot(hdfs, sub, "s1");
			CheckQuotaUsageComputation(sub, 6, Blocksize * 11);
			// create snapshot s2 on dir
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s2");
			CheckQuotaUsageComputation(dir, 7, Blocksize * 11);
			CheckQuotaUsageComputation(sub, 6, Blocksize * 11);
			// make changes on subsub and subsubFile1
			hdfs.SetOwner(subsub, "unknown", "unknown");
			hdfs.SetReplication(subsubFile1, Replication1);
			CheckQuotaUsageComputation(dir, 7, Blocksize * 11);
			CheckQuotaUsageComputation(sub, 6, Blocksize * 11);
			// make changes on sub
			hdfs.Delete(subFile1, true);
			CheckQuotaUsageComputation(new Path("/"), 8, Blocksize * 11);
			CheckQuotaUsageComputation(dir, 7, Blocksize * 11);
			CheckQuotaUsageComputation(sub, 6, Blocksize * 11);
			Path subsubSnapshotCopy = SnapshotTestHelper.GetSnapshotPath(dir, "s2", sub.GetName
				() + Path.Separator + subsub.GetName());
			Path subsubFile1SCopy = SnapshotTestHelper.GetSnapshotPath(dir, "s2", sub.GetName
				() + Path.Separator + subsub.GetName() + Path.Separator + subsubFile1.GetName());
			Path subFile1SCopy = SnapshotTestHelper.GetSnapshotPath(dir, "s2", sub.GetName() 
				+ Path.Separator + subFile1.GetName());
			FileStatus subsubStatus = hdfs.GetFileStatus(subsubSnapshotCopy);
			NUnit.Framework.Assert.AreEqual("owner", subsubStatus.GetOwner());
			NUnit.Framework.Assert.AreEqual("group", subsubStatus.GetGroup());
			FileStatus subsubFile1Status = hdfs.GetFileStatus(subsubFile1SCopy);
			NUnit.Framework.Assert.AreEqual(Replication, subsubFile1Status.GetReplication());
			FileStatus subFile1Status = hdfs.GetFileStatus(subFile1SCopy);
			NUnit.Framework.Assert.AreEqual(Replication1, subFile1Status.GetReplication());
			// delete snapshot s2
			hdfs.DeleteSnapshot(dir, "s2");
			CheckQuotaUsageComputation(new Path("/"), 8, Blocksize * 11);
			CheckQuotaUsageComputation(dir, 7, Blocksize * 11);
			CheckQuotaUsageComputation(sub, 6, Blocksize * 11);
			// no snapshot copy for s2
			try
			{
				hdfs.GetFileStatus(subsubSnapshotCopy);
				NUnit.Framework.Assert.Fail("should throw FileNotFoundException");
			}
			catch (FileNotFoundException e)
			{
				GenericTestUtils.AssertExceptionContains("File does not exist: " + subsubSnapshotCopy
					.ToString(), e);
			}
			try
			{
				hdfs.GetFileStatus(subsubFile1SCopy);
				NUnit.Framework.Assert.Fail("should throw FileNotFoundException");
			}
			catch (FileNotFoundException e)
			{
				GenericTestUtils.AssertExceptionContains("File does not exist: " + subsubFile1SCopy
					.ToString(), e);
			}
			try
			{
				hdfs.GetFileStatus(subFile1SCopy);
				NUnit.Framework.Assert.Fail("should throw FileNotFoundException");
			}
			catch (FileNotFoundException e)
			{
				GenericTestUtils.AssertExceptionContains("File does not exist: " + subFile1SCopy.
					ToString(), e);
			}
			// the snapshot copy of s2 should now be renamed to s1 under sub
			subsubSnapshotCopy = SnapshotTestHelper.GetSnapshotPath(sub, "s1", subsub.GetName
				());
			subsubFile1SCopy = SnapshotTestHelper.GetSnapshotPath(sub, "s1", subsub.GetName()
				 + Path.Separator + subsubFile1.GetName());
			subFile1SCopy = SnapshotTestHelper.GetSnapshotPath(sub, "s1", subFile1.GetName());
			subsubStatus = hdfs.GetFileStatus(subsubSnapshotCopy);
			NUnit.Framework.Assert.AreEqual("owner", subsubStatus.GetOwner());
			NUnit.Framework.Assert.AreEqual("group", subsubStatus.GetGroup());
			subsubFile1Status = hdfs.GetFileStatus(subsubFile1SCopy);
			NUnit.Framework.Assert.AreEqual(Replication, subsubFile1Status.GetReplication());
			// also subFile1's snapshot copy should have been moved to diff of s1 as 
			// combination
			subFile1Status = hdfs.GetFileStatus(subFile1SCopy);
			NUnit.Framework.Assert.AreEqual(Replication1, subFile1Status.GetReplication());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteSnapshotCommandWithIllegalArguments()
		{
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			TextWriter psOut = new TextWriter(@out);
			Runtime.SetOut(psOut);
			Runtime.SetErr(psOut);
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			string[] argv1 = new string[] { "-deleteSnapshot", "/tmp" };
			int val = shell.Run(argv1);
			NUnit.Framework.Assert.IsTrue(val == -1);
			NUnit.Framework.Assert.IsTrue(@out.ToString().Contains(argv1[0] + ": Incorrect number of arguments."
				));
			@out.Reset();
			string[] argv2 = new string[] { "-deleteSnapshot", "/tmp", "s1", "s2" };
			val = shell.Run(argv2);
			NUnit.Framework.Assert.IsTrue(val == -1);
			NUnit.Framework.Assert.IsTrue(@out.ToString().Contains(argv2[0] + ": Incorrect number of arguments."
				));
			psOut.Close();
			@out.Close();
		}

		/*
		* OP_DELETE_SNAPSHOT edits op was not decrementing the safemode threshold on
		* restart in HA mode. HDFS-5504
		*/
		/// <exception cref="System.Exception"/>
		public virtual void TestHANNRestartAfterSnapshotDeletion()
		{
			hdfs.Close();
			cluster.Shutdown();
			conf = new Configuration();
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(1).Build();
			cluster.TransitionToActive(0);
			// stop the standby namenode
			NameNode snn = cluster.GetNameNode(1);
			snn.Stop();
			hdfs = (DistributedFileSystem)HATestUtil.ConfigureFailoverFs(cluster, conf);
			Path dir = new Path("/dir");
			Path subDir = new Path(dir, "sub");
			hdfs.Mkdirs(dir);
			hdfs.AllowSnapshot(dir);
			for (int i = 0; i < 5; i++)
			{
				DFSTestUtil.CreateFile(hdfs, new Path(subDir, string.Empty + i), 100, (short)1, 1024L
					);
			}
			// take snapshot
			hdfs.CreateSnapshot(dir, "s0");
			// delete the subdir
			hdfs.Delete(subDir, true);
			// roll the edit log
			NameNode ann = cluster.GetNameNode(0);
			ann.GetRpcServer().RollEditLog();
			hdfs.DeleteSnapshot(dir, "s0");
			// wait for the blocks deletion at namenode
			Sharpen.Thread.Sleep(2000);
			NameNodeAdapter.AbortEditLogs(ann);
			cluster.RestartNameNode(0, false);
			cluster.TransitionToActive(0);
			// wait till the cluster becomes active
			cluster.WaitClusterUp();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCorrectNumberOfBlocksAfterRestart()
		{
			Path foo = new Path("/foo");
			Path bar = new Path(foo, "bar");
			Path file = new Path(foo, "file");
			string snapshotName = "ss0";
			DFSTestUtil.CreateFile(hdfs, file, Blocksize, Replication, seed);
			hdfs.Mkdirs(bar);
			hdfs.SetQuota(foo, long.MaxValue - 1, long.MaxValue - 1);
			hdfs.SetQuota(bar, long.MaxValue - 1, long.MaxValue - 1);
			hdfs.AllowSnapshot(foo);
			hdfs.CreateSnapshot(foo, snapshotName);
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			hdfs.SaveNamespace();
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			hdfs.DeleteSnapshot(foo, snapshotName);
			hdfs.Delete(bar, true);
			hdfs.Delete(foo, true);
			long numberOfBlocks = cluster.GetNamesystem().GetBlocksTotal();
			cluster.RestartNameNode(0);
			NUnit.Framework.Assert.AreEqual(numberOfBlocks, cluster.GetNamesystem().GetBlocksTotal
				());
		}

		public TestSnapshotDeletion()
		{
			sub = new Path(dir, "sub1");
			subsub = new Path(sub, "subsub1");
		}
	}
}
