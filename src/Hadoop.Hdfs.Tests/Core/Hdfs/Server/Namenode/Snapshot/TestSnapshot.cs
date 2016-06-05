using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.IO.Output;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>This class tests snapshot functionality.</summary>
	/// <remarks>
	/// This class tests snapshot functionality. One or multiple snapshots are
	/// created. The snapshotted directory is changed and verification is done to
	/// ensure snapshots remain unchanges.
	/// </remarks>
	public class TestSnapshot
	{
		private static readonly long seed;

		private static readonly Random random;

		static TestSnapshot()
		{
			seed = Time.Now();
			random = new Random(seed);
			System.Console.Out.WriteLine("Random seed: " + seed);
		}

		protected internal const short Replication = 3;

		protected internal const int Blocksize = 1024;

		/// <summary>The number of times snapshots are created for a snapshottable directory</summary>
		public const int SnapshotIterationNumber = 20;

		/// <summary>Height of directory tree used for testing</summary>
		public const int DirectoryTreeLevel = 5;

		protected internal Configuration conf;

		protected internal static MiniDFSCluster cluster;

		protected internal static FSNamesystem fsn;

		protected internal static FSDirectory fsdir;

		protected internal DistributedFileSystem hdfs;

		private static readonly string testDir = Runtime.GetProperty("test.build.data", "build/test/data"
			);

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		/// <summary>The list recording all previous snapshots.</summary>
		/// <remarks>
		/// The list recording all previous snapshots. Each element in the array
		/// records a snapshot root.
		/// </remarks>
		protected internal static readonly AList<Path> snapshotList = new AList<Path>();

		/// <summary>
		/// Check
		/// <see cref="TestDirectoryTree"/>
		/// </summary>
		private SnapshotTestHelper.TestDirectoryTree dirTree;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, Blocksize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			fsdir = fsn.GetFSDirectory();
			hdfs = cluster.GetFileSystem();
			dirTree = new SnapshotTestHelper.TestDirectoryTree(DirectoryTreeLevel, hdfs);
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

		internal static int modificationCount = 0;

		/// <summary>Make changes (modification, deletion, creation) to the current files/dir.
		/// 	</summary>
		/// <remarks>
		/// Make changes (modification, deletion, creation) to the current files/dir.
		/// Then check if the previous snapshots are still correct.
		/// </remarks>
		/// <param name="modifications">Modifications that to be applied to the current dir.</param>
		/// <exception cref="System.Exception"/>
		private void ModifyCurrentDirAndCheckSnapshots(TestSnapshot.Modification[] modifications
			)
		{
			foreach (TestSnapshot.Modification modification in modifications)
			{
				System.Console.Out.WriteLine(++modificationCount + ") " + modification);
				modification.LoadSnapshots();
				modification.Modify();
				modification.CheckSnapshots();
			}
		}

		/// <summary>Create two snapshots in each iteration.</summary>
		/// <remarks>
		/// Create two snapshots in each iteration. Each time we will create a snapshot
		/// for the top node, then randomly pick a dir in the tree and create
		/// snapshot for it.
		/// Finally check the snapshots are created correctly.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		protected internal virtual SnapshotTestHelper.TestDirectoryTree.Node[] CreateSnapshots
			()
		{
			SnapshotTestHelper.TestDirectoryTree.Node[] nodes = new SnapshotTestHelper.TestDirectoryTree.Node
				[2];
			// Each time we will create a snapshot for the top level dir
			Path root = SnapshotTestHelper.CreateSnapshot(hdfs, dirTree.topNode.nodePath, NextSnapshotName
				());
			snapshotList.AddItem(root);
			nodes[0] = dirTree.topNode;
			SnapshotTestHelper.CheckSnapshotCreation(hdfs, root, nodes[0].nodePath);
			// Then randomly pick one dir from the tree (cannot be the top node) and
			// create snapshot for it
			AList<SnapshotTestHelper.TestDirectoryTree.Node> excludedList = new AList<SnapshotTestHelper.TestDirectoryTree.Node
				>();
			excludedList.AddItem(nodes[0]);
			nodes[1] = dirTree.GetRandomDirNode(random, excludedList);
			root = SnapshotTestHelper.CreateSnapshot(hdfs, nodes[1].nodePath, NextSnapshotName
				());
			snapshotList.AddItem(root);
			SnapshotTestHelper.CheckSnapshotCreation(hdfs, root, nodes[1].nodePath);
			return nodes;
		}

		private FilePath GetDumpTreeFile(string dir, string suffix)
		{
			return new FilePath(dir, string.Format("dumptree_%s", suffix));
		}

		/// <summary>Restart the cluster to check edit log applying and fsimage saving/loading
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		private void CheckFSImage()
		{
			FilePath fsnBefore = GetDumpTreeFile(testDir, "before");
			FilePath fsnMiddle = GetDumpTreeFile(testDir, "middle");
			FilePath fsnAfter = GetDumpTreeFile(testDir, "after");
			SnapshotTestHelper.DumpTree2File(fsdir, fsnBefore);
			cluster.Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(Replication
				).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
			// later check fsnMiddle to see if the edit log is applied correctly 
			SnapshotTestHelper.DumpTree2File(fsdir, fsnMiddle);
			// save namespace and restart cluster
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			hdfs.SaveNamespace();
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			cluster.Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(Replication
				).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
			// dump the namespace loaded from fsimage
			SnapshotTestHelper.DumpTree2File(fsdir, fsnAfter);
			SnapshotTestHelper.CompareDumpedTreeInFile(fsnBefore, fsnMiddle, true);
			SnapshotTestHelper.CompareDumpedTreeInFile(fsnBefore, fsnAfter, true);
		}

		/// <summary>
		/// Main test, where we will go in the following loop:
		/// <pre>
		/// Create snapshot and check the creation &lt;--+
		/// -&gt; Change the current/live files/dir         |
		/// -&gt; Check previous snapshots -----------------+
		/// </pre>
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSnapshot()
		{
			try
			{
				RunTestSnapshot(SnapshotIterationNumber);
			}
			catch (Exception t)
			{
				SnapshotTestHelper.Log.Info("FAILED", t);
				SnapshotTestHelper.DumpTree("FAILED", cluster);
				throw;
			}
		}

		/// <summary>
		/// Test if the OfflineImageViewerPB can correctly parse a fsimage containing
		/// snapshots
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOfflineImageViewer()
		{
			RunTestSnapshot(1);
			// retrieve the fsimage. Note that we already save namespace to fsimage at
			// the end of each iteration of runTestSnapshot.
			FilePath originalFsimage = FSImageTestUtil.FindLatestImageFile(FSImageTestUtil.GetFSImage
				(cluster.GetNameNode()).GetStorage().GetStorageDir(0));
			NUnit.Framework.Assert.IsNotNull("Didn't generate or can't find fsimage", originalFsimage
				);
			TextWriter o = new TextWriter(NullOutputStream.NullOutputStream);
			PBImageXmlWriter v = new PBImageXmlWriter(new Configuration(), o);
			v.Visit(new RandomAccessFile(originalFsimage, "r"));
		}

		/// <exception cref="System.Exception"/>
		private void RunTestSnapshot(int iteration)
		{
			for (int i = 0; i < iteration; i++)
			{
				// create snapshot and check the creation
				cluster.GetNamesystem().GetSnapshotManager().SetAllowNestedSnapshots(true);
				SnapshotTestHelper.TestDirectoryTree.Node[] ssNodes = CreateSnapshots();
				// prepare the modifications for the snapshotted dirs
				// we cover the following directories: top, new, and a random
				AList<SnapshotTestHelper.TestDirectoryTree.Node> excludedList = new AList<SnapshotTestHelper.TestDirectoryTree.Node
					>();
				SnapshotTestHelper.TestDirectoryTree.Node[] modNodes = new SnapshotTestHelper.TestDirectoryTree.Node
					[ssNodes.Length + 1];
				for (int n = 0; n < ssNodes.Length; n++)
				{
					modNodes[n] = ssNodes[n];
					excludedList.AddItem(ssNodes[n]);
				}
				modNodes[modNodes.Length - 1] = dirTree.GetRandomDirNode(random, excludedList);
				TestSnapshot.Modification[] mods = PrepareModifications(modNodes);
				// make changes to the directories/files
				ModifyCurrentDirAndCheckSnapshots(mods);
				// also update the metadata of directories
				SnapshotTestHelper.TestDirectoryTree.Node chmodDir = dirTree.GetRandomDirNode(random
					, null);
				TestSnapshot.Modification chmod = new TestSnapshot.FileChangePermission(chmodDir.
					nodePath, hdfs, GenRandomPermission());
				string[] userGroup = GenRandomOwner();
				SnapshotTestHelper.TestDirectoryTree.Node chownDir = dirTree.GetRandomDirNode(random
					, Arrays.AsList(chmodDir));
				TestSnapshot.Modification chown = new TestSnapshot.FileChown(chownDir.nodePath, hdfs
					, userGroup[0], userGroup[1]);
				ModifyCurrentDirAndCheckSnapshots(new TestSnapshot.Modification[] { chmod, chown }
					);
				// check fsimage saving/loading
				CheckFSImage();
			}
		}

		/// <summary>
		/// A simple test that updates a sub-directory of a snapshottable directory
		/// with snapshots
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestUpdateDirectory()
		{
			Path dir = new Path("/dir");
			Path sub = new Path(dir, "sub");
			Path subFile = new Path(sub, "file");
			DFSTestUtil.CreateFile(hdfs, subFile, Blocksize, Replication, seed);
			FileStatus oldStatus = hdfs.GetFileStatus(sub);
			hdfs.AllowSnapshot(dir);
			hdfs.CreateSnapshot(dir, "s1");
			hdfs.SetTimes(sub, 100L, 100L);
			Path snapshotPath = SnapshotTestHelper.GetSnapshotPath(dir, "s1", "sub");
			FileStatus snapshotStatus = hdfs.GetFileStatus(snapshotPath);
			NUnit.Framework.Assert.AreEqual(oldStatus.GetModificationTime(), snapshotStatus.GetModificationTime
				());
			NUnit.Framework.Assert.AreEqual(oldStatus.GetAccessTime(), snapshotStatus.GetAccessTime
				());
		}

		/// <summary>Test creating a snapshot with illegal name</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreateSnapshotWithIllegalName()
		{
			Path dir = new Path("/dir");
			hdfs.Mkdirs(dir);
			string name1 = HdfsConstants.DotSnapshotDir;
			try
			{
				hdfs.CreateSnapshot(dir, name1);
				NUnit.Framework.Assert.Fail("Exception expected when an illegal name is given");
			}
			catch (RemoteException e)
			{
				string errorMsg = "Invalid path name Invalid snapshot name: " + name1;
				GenericTestUtils.AssertExceptionContains(errorMsg, e);
			}
			string[] badNames = new string[] { "foo" + Path.Separator, Path.Separator + "foo"
				, Path.Separator, "foo" + Path.Separator + "bar" };
			foreach (string badName in badNames)
			{
				try
				{
					hdfs.CreateSnapshot(dir, badName);
					NUnit.Framework.Assert.Fail("Exception expected when an illegal name is given");
				}
				catch (RemoteException e)
				{
					string errorMsg = "Invalid path name Invalid snapshot name: " + badName;
					GenericTestUtils.AssertExceptionContains(errorMsg, e);
				}
			}
		}

		/// <summary>Creating snapshots for a directory that is not snapshottable must fail.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshottableDirectory()
		{
			Path dir = new Path("/TestSnapshot/sub");
			Path file0 = new Path(dir, "file0");
			Path file1 = new Path(dir, "file1");
			DFSTestUtil.CreateFile(hdfs, file0, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			try
			{
				hdfs.CreateSnapshot(dir, "s1");
				NUnit.Framework.Assert.Fail("Exception expected: " + dir + " is not snapshottable"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Directory is not a snapshottable directory: "
					 + dir, e);
			}
			try
			{
				hdfs.DeleteSnapshot(dir, "s1");
				NUnit.Framework.Assert.Fail("Exception expected: " + dir + " is not a snapshottale dir"
					);
			}
			catch (Exception e)
			{
				GenericTestUtils.AssertExceptionContains("Directory is not a snapshottable directory: "
					 + dir, e);
			}
			try
			{
				hdfs.RenameSnapshot(dir, "s1", "s2");
				NUnit.Framework.Assert.Fail("Exception expected: " + dir + " is not a snapshottale dir"
					);
			}
			catch (Exception e)
			{
				GenericTestUtils.AssertExceptionContains("Directory is not a snapshottable directory: "
					 + dir, e);
			}
		}

		/// <summary>
		/// Test multiple calls of allowSnapshot and disallowSnapshot, to make sure
		/// they are idempotent
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllowAndDisallowSnapshot()
		{
			Path dir = new Path("/dir");
			Path file0 = new Path(dir, "file0");
			Path file1 = new Path(dir, "file1");
			DFSTestUtil.CreateFile(hdfs, file0, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file1, Blocksize, Replication, seed);
			INodeDirectory dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsFalse(dirNode.IsSnapshottable());
			hdfs.AllowSnapshot(dir);
			dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(dirNode.IsSnapshottable());
			// call allowSnapshot again
			hdfs.AllowSnapshot(dir);
			dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(dirNode.IsSnapshottable());
			// disallowSnapshot on dir
			hdfs.DisallowSnapshot(dir);
			dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsFalse(dirNode.IsSnapshottable());
			// do it again
			hdfs.DisallowSnapshot(dir);
			dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsFalse(dirNode.IsSnapshottable());
			// same process on root
			Path root = new Path("/");
			INodeDirectory rootNode = fsdir.GetINode4Write(root.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(rootNode.IsSnapshottable());
			// root is snapshottable dir, but with 0 snapshot quota
			NUnit.Framework.Assert.AreEqual(0, rootNode.GetDirectorySnapshottableFeature().GetSnapshotQuota
				());
			hdfs.AllowSnapshot(root);
			rootNode = fsdir.GetINode4Write(root.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(rootNode.IsSnapshottable());
			NUnit.Framework.Assert.AreEqual(DirectorySnapshottableFeature.SnapshotLimit, rootNode
				.GetDirectorySnapshottableFeature().GetSnapshotQuota());
			// call allowSnapshot again
			hdfs.AllowSnapshot(root);
			rootNode = fsdir.GetINode4Write(root.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(rootNode.IsSnapshottable());
			NUnit.Framework.Assert.AreEqual(DirectorySnapshottableFeature.SnapshotLimit, rootNode
				.GetDirectorySnapshottableFeature().GetSnapshotQuota());
			// disallowSnapshot on dir
			hdfs.DisallowSnapshot(root);
			rootNode = fsdir.GetINode4Write(root.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(rootNode.IsSnapshottable());
			NUnit.Framework.Assert.AreEqual(0, rootNode.GetDirectorySnapshottableFeature().GetSnapshotQuota
				());
			// do it again
			hdfs.DisallowSnapshot(root);
			rootNode = fsdir.GetINode4Write(root.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(rootNode.IsSnapshottable());
			NUnit.Framework.Assert.AreEqual(0, rootNode.GetDirectorySnapshottableFeature().GetSnapshotQuota
				());
		}

		/// <summary>Prepare a list of modifications.</summary>
		/// <remarks>
		/// Prepare a list of modifications. A modification may be a file creation,
		/// file deletion, or a modification operation such as appending to an existing
		/// file.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		private TestSnapshot.Modification[] PrepareModifications(SnapshotTestHelper.TestDirectoryTree.Node
			[] nodes)
		{
			AList<TestSnapshot.Modification> mList = new AList<TestSnapshot.Modification>();
			foreach (SnapshotTestHelper.TestDirectoryTree.Node node in nodes)
			{
				// If the node does not have files in it, create files
				if (node.fileList == null)
				{
					node.InitFileList(hdfs, node.nodePath.GetName(), Blocksize, Replication, seed, 6);
				}
				//
				// Modification iterations are as follows:
				// Iteration 0 - create:fileList[5], delete:fileList[0],
				//               append:fileList[1], chmod:fileList[2], 
				//               chown:fileList[3],  change_replication:fileList[4].
				//               Set nullFileIndex to 0
				//
				// Iteration 1 - create:fileList[0], delete:fileList[1],
				//               append:fileList[2], chmod:fileList[3], 
				//               chown:fileList[4],  change_replication:fileList[5]
				//               Set nullFileIndex to 1
				// 
				// Iteration 2 - create:fileList[1], delete:fileList[2],
				//               append:fileList[3], chmod:fileList[4], 
				//               chown:fileList[5],  change_replication:fileList[6]
				//               Set nullFileIndex to 2
				// ...
				//
				TestSnapshot.Modification create = new TestSnapshot.FileCreation(node.fileList[node
					.nullFileIndex], hdfs, Blocksize);
				TestSnapshot.Modification delete = new TestSnapshot.FileDeletion(node.fileList[(node
					.nullFileIndex + 1) % node.fileList.Count], hdfs);
				Path f = node.fileList[(node.nullFileIndex + 2) % node.fileList.Count];
				TestSnapshot.Modification append = new TestSnapshot.FileAppend(f, hdfs, Blocksize
					);
				TestSnapshot.FileAppendNotClose appendNotClose = new TestSnapshot.FileAppendNotClose
					(f, hdfs, Blocksize);
				TestSnapshot.Modification appendClose = new TestSnapshot.FileAppendClose(f, hdfs, 
					Blocksize, appendNotClose);
				TestSnapshot.Modification chmod = new TestSnapshot.FileChangePermission(node.fileList
					[(node.nullFileIndex + 3) % node.fileList.Count], hdfs, GenRandomPermission());
				string[] userGroup = GenRandomOwner();
				TestSnapshot.Modification chown = new TestSnapshot.FileChown(node.fileList[(node.
					nullFileIndex + 4) % node.fileList.Count], hdfs, userGroup[0], userGroup[1]);
				TestSnapshot.Modification replication = new TestSnapshot.FileChangeReplication(node
					.fileList[(node.nullFileIndex + 5) % node.fileList.Count], hdfs, (short)(random.
					Next(Replication) + 1));
				node.nullFileIndex = (node.nullFileIndex + 1) % node.fileList.Count;
				TestSnapshot.Modification dirChange = new TestSnapshot.DirCreationOrDeletion(this
					, node.nodePath, hdfs, node, random.NextBoolean());
				// dir rename
				SnapshotTestHelper.TestDirectoryTree.Node dstParent = dirTree.GetRandomDirNode(random
					, Arrays.AsList(nodes));
				TestSnapshot.Modification dirRename = new TestSnapshot.DirRename(this, node.nodePath
					, hdfs, node, dstParent);
				mList.AddItem(create);
				mList.AddItem(delete);
				mList.AddItem(append);
				mList.AddItem(appendNotClose);
				mList.AddItem(appendClose);
				mList.AddItem(chmod);
				mList.AddItem(chown);
				mList.AddItem(replication);
				mList.AddItem(dirChange);
				mList.AddItem(dirRename);
			}
			return Sharpen.Collections.ToArray(mList, new TestSnapshot.Modification[mList.Count
				]);
		}

		/// <returns>A random FsPermission</returns>
		private FsPermission GenRandomPermission()
		{
			// randomly select between "rwx" and "rw-"
			FsAction u = random.NextBoolean() ? FsAction.All : FsAction.ReadWrite;
			FsAction g = random.NextBoolean() ? FsAction.All : FsAction.ReadWrite;
			FsAction o = random.NextBoolean() ? FsAction.All : FsAction.ReadWrite;
			return new FsPermission(u, g, o);
		}

		/// <returns>
		/// A string array containing two string: the first string indicates
		/// the owner, and the other indicates the group
		/// </returns>
		private string[] GenRandomOwner()
		{
			string[] userGroup = new string[] { "dr.who", "unknown" };
			return userGroup;
		}

		private static int snapshotCount = 0;

		/// <returns>The next snapshot name</returns>
		internal static string NextSnapshotName()
		{
			return string.Format("s-%d", ++snapshotCount);
		}

		/// <summary>Base class to present changes applied to current file/dir.</summary>
		/// <remarks>
		/// Base class to present changes applied to current file/dir. A modification
		/// can be file creation, deletion, or other modifications such as appending on
		/// an existing file. Three abstract methods need to be implemented by
		/// subclasses: loadSnapshots() captures the states of snapshots before the
		/// modification, modify() applies the modification to the current directory,
		/// and checkSnapshots() verifies the snapshots do not change after the
		/// modification.
		/// </remarks>
		internal abstract class Modification
		{
			protected internal readonly Path file;

			protected internal readonly FileSystem fs;

			internal readonly string type;

			internal Modification(Path file, FileSystem fs, string type)
			{
				this.file = file;
				this.fs = fs;
				this.type = type;
			}

			/// <exception cref="System.Exception"/>
			internal abstract void LoadSnapshots();

			/// <exception cref="System.Exception"/>
			internal abstract void Modify();

			/// <exception cref="System.Exception"/>
			internal abstract void CheckSnapshots();

			public override string ToString()
			{
				return GetType().Name + ":" + type + ":" + file;
			}
		}

		/// <summary>Modifications that change the file status.</summary>
		/// <remarks>
		/// Modifications that change the file status. We check the FileStatus of
		/// snapshot files before/after the modification.
		/// </remarks>
		internal abstract class FileStatusChange : TestSnapshot.Modification
		{
			protected internal readonly Dictionary<Path, FileStatus> statusMap;

			internal FileStatusChange(Path file, FileSystem fs, string type)
				: base(file, fs, type)
			{
				statusMap = new Dictionary<Path, FileStatus>();
			}

			/// <exception cref="System.Exception"/>
			internal override void LoadSnapshots()
			{
				foreach (Path snapshotRoot in snapshotList)
				{
					Path snapshotFile = SnapshotTestHelper.GetSnapshotFile(snapshotRoot, file);
					if (snapshotFile != null)
					{
						if (fs.Exists(snapshotFile))
						{
							FileStatus status = fs.GetFileStatus(snapshotFile);
							statusMap[snapshotFile] = status;
						}
						else
						{
							statusMap[snapshotFile] = null;
						}
					}
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void CheckSnapshots()
			{
				foreach (Path snapshotFile in statusMap.Keys)
				{
					FileStatus currentStatus = fs.Exists(snapshotFile) ? fs.GetFileStatus(snapshotFile
						) : null;
					FileStatus originalStatus = statusMap[snapshotFile];
					NUnit.Framework.Assert.AreEqual(currentStatus, originalStatus);
					if (currentStatus != null)
					{
						string s = null;
						if (!currentStatus.ToString().Equals(originalStatus.ToString()))
						{
							s = "FAILED: " + GetType().Name + ": file=" + file + ", snapshotFile" + snapshotFile
								 + "\n\n currentStatus = " + currentStatus + "\noriginalStatus = " + originalStatus
								 + "\n\nfile        : " + fsdir.GetINode(file.ToString()).ToDetailString() + "\n\nsnapshotFile: "
								 + fsdir.GetINode(snapshotFile.ToString()).ToDetailString();
							SnapshotTestHelper.DumpTree(s, cluster);
						}
						NUnit.Framework.Assert.AreEqual(s, currentStatus.ToString(), originalStatus.ToString
							());
					}
				}
			}
		}

		/// <summary>Change the file permission</summary>
		internal class FileChangePermission : TestSnapshot.FileStatusChange
		{
			private readonly FsPermission newPermission;

			internal FileChangePermission(Path file, FileSystem fs, FsPermission newPermission
				)
				: base(file, fs, "chmod")
			{
				this.newPermission = newPermission;
			}

			/// <exception cref="System.Exception"/>
			internal override void Modify()
			{
				NUnit.Framework.Assert.IsTrue(fs.Exists(file));
				fs.SetPermission(file, newPermission);
			}
		}

		/// <summary>Change the replication factor of file</summary>
		internal class FileChangeReplication : TestSnapshot.FileStatusChange
		{
			private readonly short newReplication;

			internal FileChangeReplication(Path file, FileSystem fs, short replication)
				: base(file, fs, "replication")
			{
				this.newReplication = replication;
			}

			/// <exception cref="System.Exception"/>
			internal override void Modify()
			{
				NUnit.Framework.Assert.IsTrue(fs.Exists(file));
				fs.SetReplication(file, newReplication);
			}
		}

		/// <summary>Change the owner:group of a file</summary>
		internal class FileChown : TestSnapshot.FileStatusChange
		{
			private readonly string newUser;

			private readonly string newGroup;

			internal FileChown(Path file, FileSystem fs, string user, string group)
				: base(file, fs, "chown")
			{
				this.newUser = user;
				this.newGroup = group;
			}

			/// <exception cref="System.Exception"/>
			internal override void Modify()
			{
				NUnit.Framework.Assert.IsTrue(fs.Exists(file));
				fs.SetOwner(file, newUser, newGroup);
			}
		}

		/// <summary>Appending a specified length to an existing file</summary>
		internal class FileAppend : TestSnapshot.Modification
		{
			internal readonly int appendLen;

			private readonly Dictionary<Path, long> snapshotFileLengthMap;

			internal FileAppend(Path file, FileSystem fs, int len)
				: base(file, fs, "append")
			{
				this.appendLen = len;
				this.snapshotFileLengthMap = new Dictionary<Path, long>();
			}

			/// <exception cref="System.Exception"/>
			internal override void LoadSnapshots()
			{
				foreach (Path snapshotRoot in snapshotList)
				{
					Path snapshotFile = SnapshotTestHelper.GetSnapshotFile(snapshotRoot, file);
					if (snapshotFile != null)
					{
						long snapshotFileLen = fs.Exists(snapshotFile) ? fs.GetFileStatus(snapshotFile).GetLen
							() : -1L;
						snapshotFileLengthMap[snapshotFile] = snapshotFileLen;
					}
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Modify()
			{
				NUnit.Framework.Assert.IsTrue(fs.Exists(file));
				DFSTestUtil.AppendFile(fs, file, appendLen);
			}

			/// <exception cref="System.Exception"/>
			internal override void CheckSnapshots()
			{
				byte[] buffer = new byte[32];
				foreach (Path snapshotFile in snapshotFileLengthMap.Keys)
				{
					long currentSnapshotFileLen = fs.Exists(snapshotFile) ? fs.GetFileStatus(snapshotFile
						).GetLen() : -1L;
					long originalSnapshotFileLen = snapshotFileLengthMap[snapshotFile];
					string s = null;
					if (currentSnapshotFileLen != originalSnapshotFileLen)
					{
						s = "FAILED: " + GetType().Name + ": file=" + file + ", snapshotFile" + snapshotFile
							 + "\n\n currentSnapshotFileLen = " + currentSnapshotFileLen + "\noriginalSnapshotFileLen = "
							 + originalSnapshotFileLen + "\n\nfile        : " + fsdir.GetINode(file.ToString
							()).ToDetailString() + "\n\nsnapshotFile: " + fsdir.GetINode(snapshotFile.ToString
							()).ToDetailString();
						SnapshotTestHelper.DumpTree(s, cluster);
					}
					NUnit.Framework.Assert.AreEqual(s, originalSnapshotFileLen, currentSnapshotFileLen
						);
					// Read the snapshot file out of the boundary
					if (currentSnapshotFileLen != -1L && !(this is TestSnapshot.FileAppendNotClose))
					{
						FSDataInputStream input = fs.Open(snapshotFile);
						int readLen = input.Read(currentSnapshotFileLen, buffer, 0, 1);
						if (readLen != -1)
						{
							s = "FAILED: " + GetType().Name + ": file=" + file + ", snapshotFile" + snapshotFile
								 + "\n\n currentSnapshotFileLen = " + currentSnapshotFileLen + "\n                readLen = "
								 + readLen + "\n\nfile        : " + fsdir.GetINode(file.ToString()).ToDetailString
								() + "\n\nsnapshotFile: " + fsdir.GetINode(snapshotFile.ToString()).ToDetailString
								();
							SnapshotTestHelper.DumpTree(s, cluster);
						}
						NUnit.Framework.Assert.AreEqual(s, -1, readLen);
						input.Close();
					}
				}
			}
		}

		/// <summary>Appending a specified length to an existing file but not close the file</summary>
		internal class FileAppendNotClose : TestSnapshot.FileAppend
		{
			internal HdfsDataOutputStream @out;

			internal FileAppendNotClose(Path file, FileSystem fs, int len)
				: base(file, fs, len)
			{
			}

			/// <exception cref="System.Exception"/>
			internal override void Modify()
			{
				NUnit.Framework.Assert.IsTrue(fs.Exists(file));
				byte[] toAppend = new byte[appendLen];
				random.NextBytes(toAppend);
				@out = (HdfsDataOutputStream)fs.Append(file);
				@out.Write(toAppend);
				@out.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
			}
		}

		/// <summary>Appending a specified length to an existing file</summary>
		internal class FileAppendClose : TestSnapshot.FileAppend
		{
			internal readonly TestSnapshot.FileAppendNotClose fileAppendNotClose;

			internal FileAppendClose(Path file, FileSystem fs, int len, TestSnapshot.FileAppendNotClose
				 fileAppendNotClose)
				: base(file, fs, len)
			{
				this.fileAppendNotClose = fileAppendNotClose;
			}

			/// <exception cref="System.Exception"/>
			internal override void Modify()
			{
				NUnit.Framework.Assert.IsTrue(fs.Exists(file));
				byte[] toAppend = new byte[appendLen];
				random.NextBytes(toAppend);
				fileAppendNotClose.@out.Write(toAppend);
				fileAppendNotClose.@out.Close();
			}
		}

		/// <summary>New file creation</summary>
		internal class FileCreation : TestSnapshot.Modification
		{
			internal readonly int fileLen;

			private readonly Dictionary<Path, FileStatus> fileStatusMap;

			internal FileCreation(Path file, FileSystem fs, int len)
				: base(file, fs, "creation")
			{
				System.Diagnostics.Debug.Assert(len >= 0);
				this.fileLen = len;
				fileStatusMap = new Dictionary<Path, FileStatus>();
			}

			/// <exception cref="System.Exception"/>
			internal override void LoadSnapshots()
			{
				foreach (Path snapshotRoot in snapshotList)
				{
					Path snapshotFile = SnapshotTestHelper.GetSnapshotFile(snapshotRoot, file);
					if (snapshotFile != null)
					{
						FileStatus status = fs.Exists(snapshotFile) ? fs.GetFileStatus(snapshotFile) : null;
						fileStatusMap[snapshotFile] = status;
					}
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Modify()
			{
				DFSTestUtil.CreateFile(fs, file, fileLen, Replication, seed);
			}

			/// <exception cref="System.Exception"/>
			internal override void CheckSnapshots()
			{
				foreach (Path snapshotRoot in snapshotList)
				{
					Path snapshotFile = SnapshotTestHelper.GetSnapshotFile(snapshotRoot, file);
					if (snapshotFile != null)
					{
						bool computed = fs.Exists(snapshotFile);
						bool expected = fileStatusMap[snapshotFile] != null;
						NUnit.Framework.Assert.AreEqual(expected, computed);
						if (computed)
						{
							FileStatus currentSnapshotStatus = fs.GetFileStatus(snapshotFile);
							FileStatus originalStatus = fileStatusMap[snapshotFile];
							// We compare the string because it contains all the information,
							// while FileStatus#equals only compares the path
							NUnit.Framework.Assert.AreEqual(currentSnapshotStatus.ToString(), originalStatus.
								ToString());
						}
					}
				}
			}
		}

		/// <summary>File deletion</summary>
		internal class FileDeletion : TestSnapshot.Modification
		{
			private readonly Dictionary<Path, bool> snapshotFileExistenceMap;

			internal FileDeletion(Path file, FileSystem fs)
				: base(file, fs, "deletion")
			{
				snapshotFileExistenceMap = new Dictionary<Path, bool>();
			}

			/// <exception cref="System.Exception"/>
			internal override void LoadSnapshots()
			{
				foreach (Path snapshotRoot in snapshotList)
				{
					bool existence = SnapshotTestHelper.GetSnapshotFile(snapshotRoot, file) != null;
					snapshotFileExistenceMap[snapshotRoot] = existence;
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Modify()
			{
				fs.Delete(file, true);
			}

			/// <exception cref="System.Exception"/>
			internal override void CheckSnapshots()
			{
				foreach (Path snapshotRoot in snapshotList)
				{
					bool currentSnapshotFileExist = SnapshotTestHelper.GetSnapshotFile(snapshotRoot, 
						file) != null;
					bool originalSnapshotFileExist = snapshotFileExistenceMap[snapshotRoot];
					NUnit.Framework.Assert.AreEqual(currentSnapshotFileExist, originalSnapshotFileExist
						);
				}
			}
		}

		/// <summary>Directory creation or deletion.</summary>
		internal class DirCreationOrDeletion : TestSnapshot.Modification
		{
			private readonly SnapshotTestHelper.TestDirectoryTree.Node node;

			private readonly bool isCreation;

			private readonly Path changedPath;

			private readonly Dictionary<Path, FileStatus> statusMap;

			internal DirCreationOrDeletion(TestSnapshot _enclosing, Path file, FileSystem fs, 
				SnapshotTestHelper.TestDirectoryTree.Node node, bool isCreation)
				: base(file, fs, "dircreation")
			{
				this._enclosing = _enclosing;
				this.node = node;
				// If the node's nonSnapshotChildren is empty, we still need to create
				// sub-directories
				this.isCreation = isCreation || node.nonSnapshotChildren.IsEmpty();
				if (this.isCreation)
				{
					// Generate the path for the dir to be created
					this.changedPath = new Path(node.nodePath, "sub" + node.nonSnapshotChildren.Count
						);
				}
				else
				{
					// If deletion, we delete the current last dir in nonSnapshotChildren
					this.changedPath = node.nonSnapshotChildren[node.nonSnapshotChildren.Count - 1].nodePath;
				}
				this.statusMap = new Dictionary<Path, FileStatus>();
			}

			/// <exception cref="System.Exception"/>
			internal override void LoadSnapshots()
			{
				foreach (Path snapshotRoot in TestSnapshot.snapshotList)
				{
					Path snapshotDir = SnapshotTestHelper.GetSnapshotFile(snapshotRoot, this.changedPath
						);
					if (snapshotDir != null)
					{
						FileStatus status = this.fs.Exists(snapshotDir) ? this.fs.GetFileStatus(snapshotDir
							) : null;
						this.statusMap[snapshotDir] = status;
						// In each non-snapshottable directory, we also create a file. Thus
						// here we also need to check the file's status before/after taking
						// snapshots
						Path snapshotFile = new Path(snapshotDir, "file0");
						status = this.fs.Exists(snapshotFile) ? this.fs.GetFileStatus(snapshotFile) : null;
						this.statusMap[snapshotFile] = status;
					}
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Modify()
			{
				if (this.isCreation)
				{
					// creation
					SnapshotTestHelper.TestDirectoryTree.Node newChild = new SnapshotTestHelper.TestDirectoryTree.Node
						(this.changedPath, this.node.level + 1, this.node, this._enclosing.hdfs);
					// create file under the new non-snapshottable directory
					newChild.InitFileList(this._enclosing.hdfs, this.node.nodePath.GetName(), TestSnapshot
						.Blocksize, TestSnapshot.Replication, TestSnapshot.seed, 2);
					this.node.nonSnapshotChildren.AddItem(newChild);
				}
				else
				{
					// deletion
					SnapshotTestHelper.TestDirectoryTree.Node childToDelete = this.node.nonSnapshotChildren
						.Remove(this.node.nonSnapshotChildren.Count - 1);
					this._enclosing.hdfs.Delete(childToDelete.nodePath, true);
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void CheckSnapshots()
			{
				foreach (Path snapshot in this.statusMap.Keys)
				{
					FileStatus currentStatus = this.fs.Exists(snapshot) ? this.fs.GetFileStatus(snapshot
						) : null;
					FileStatus originalStatus = this.statusMap[snapshot];
					NUnit.Framework.Assert.AreEqual(currentStatus, originalStatus);
					if (currentStatus != null)
					{
						NUnit.Framework.Assert.AreEqual(currentStatus.ToString(), originalStatus.ToString
							());
					}
				}
			}

			private readonly TestSnapshot _enclosing;
		}

		/// <summary>Directory creation or deletion.</summary>
		internal class DirRename : TestSnapshot.Modification
		{
			private readonly SnapshotTestHelper.TestDirectoryTree.Node srcParent;

			private readonly SnapshotTestHelper.TestDirectoryTree.Node dstParent;

			private readonly Path srcPath;

			private readonly Path dstPath;

			private readonly Dictionary<Path, FileStatus> statusMap;

			/// <exception cref="System.Exception"/>
			internal DirRename(TestSnapshot _enclosing, Path file, FileSystem fs, SnapshotTestHelper.TestDirectoryTree.Node
				 src, SnapshotTestHelper.TestDirectoryTree.Node dst)
				: base(file, fs, "dirrename")
			{
				this._enclosing = _enclosing;
				this.srcParent = src;
				this.dstParent = dst;
				this.dstPath = new Path(this.dstParent.nodePath, "sub" + this.dstParent.nonSnapshotChildren
					.Count);
				// If the srcParent's nonSnapshotChildren is empty, we need to create
				// sub-directories
				if (this.srcParent.nonSnapshotChildren.IsEmpty())
				{
					this.srcPath = new Path(this.srcParent.nodePath, "sub" + this.srcParent.nonSnapshotChildren
						.Count);
					// creation
					SnapshotTestHelper.TestDirectoryTree.Node newChild = new SnapshotTestHelper.TestDirectoryTree.Node
						(this.srcPath, this.srcParent.level + 1, this.srcParent, this._enclosing.hdfs);
					// create file under the new non-snapshottable directory
					newChild.InitFileList(this._enclosing.hdfs, this.srcParent.nodePath.GetName(), TestSnapshot
						.Blocksize, TestSnapshot.Replication, TestSnapshot.seed, 2);
					this.srcParent.nonSnapshotChildren.AddItem(newChild);
				}
				else
				{
					this.srcPath = new Path(this.srcParent.nodePath, "sub" + (this.srcParent.nonSnapshotChildren
						.Count - 1));
				}
				this.statusMap = new Dictionary<Path, FileStatus>();
			}

			/// <exception cref="System.Exception"/>
			internal override void LoadSnapshots()
			{
				foreach (Path snapshotRoot in TestSnapshot.snapshotList)
				{
					Path snapshotDir = SnapshotTestHelper.GetSnapshotFile(snapshotRoot, this.srcPath);
					if (snapshotDir != null)
					{
						FileStatus status = this.fs.Exists(snapshotDir) ? this.fs.GetFileStatus(snapshotDir
							) : null;
						this.statusMap[snapshotDir] = status;
						// In each non-snapshottable directory, we also create a file. Thus
						// here we also need to check the file's status before/after taking
						// snapshots
						Path snapshotFile = new Path(snapshotDir, "file0");
						status = this.fs.Exists(snapshotFile) ? this.fs.GetFileStatus(snapshotFile) : null;
						this.statusMap[snapshotFile] = status;
					}
				}
			}

			/// <exception cref="System.Exception"/>
			internal override void Modify()
			{
				this._enclosing.hdfs.Rename(this.srcPath, this.dstPath);
				SnapshotTestHelper.TestDirectoryTree.Node newDstChild = new SnapshotTestHelper.TestDirectoryTree.Node
					(this.dstPath, this.dstParent.level + 1, this.dstParent, this._enclosing.hdfs);
				this.dstParent.nonSnapshotChildren.AddItem(newDstChild);
			}

			/// <exception cref="System.Exception"/>
			internal override void CheckSnapshots()
			{
				foreach (Path snapshot in this.statusMap.Keys)
				{
					FileStatus currentStatus = this.fs.Exists(snapshot) ? this.fs.GetFileStatus(snapshot
						) : null;
					FileStatus originalStatus = this.statusMap[snapshot];
					NUnit.Framework.Assert.AreEqual(currentStatus, originalStatus);
					if (currentStatus != null)
					{
						NUnit.Framework.Assert.AreEqual(currentStatus.ToString(), originalStatus.ToString
							());
					}
				}
			}

			private readonly TestSnapshot _enclosing;
		}
	}
}
