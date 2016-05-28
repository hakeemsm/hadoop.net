using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Test FSImage save/load when Snapshot is supported</summary>
	public class TestFSImageWithSnapshot
	{
		internal const long seed = 0;

		internal const short Replication = 3;

		internal const int Blocksize = 1024;

		internal const long txid = 1;

		private readonly Path dir = new Path("/TestSnapshot");

		private static readonly string testDir = Runtime.GetProperty("test.build.data", "build/test/data"
			);

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

		/// <summary>Create a temp fsimage file for testing.</summary>
		/// <param name="dir">The directory where the fsimage file resides</param>
		/// <param name="imageTxId">The transaction id of the fsimage</param>
		/// <returns>The file of the image file</returns>
		private FilePath GetImageFile(string dir, long imageTxId)
		{
			return new FilePath(dir, string.Format("%s_%019d", NNStorage.NameNodeFile.Image, 
				imageTxId));
		}

		/// <summary>Create a temp file for dumping the fsdir</summary>
		/// <param name="dir">directory for the temp file</param>
		/// <param name="suffix">suffix of of the temp file</param>
		/// <returns>the temp file</returns>
		private FilePath GetDumpTreeFile(string dir, string suffix)
		{
			return new FilePath(dir, string.Format("dumpTree_%s", suffix));
		}

		/// <summary>Dump the fsdir tree to a temp file</summary>
		/// <param name="fileSuffix">suffix of the temp file for dumping</param>
		/// <returns>the temp file</returns>
		/// <exception cref="System.IO.IOException"/>
		private FilePath DumpTree2File(string fileSuffix)
		{
			FilePath file = GetDumpTreeFile(testDir, fileSuffix);
			SnapshotTestHelper.DumpTree2File(fsn.GetFSDirectory(), file);
			return file;
		}

		/// <summary>Append a file without closing the output stream</summary>
		/// <exception cref="System.IO.IOException"/>
		private HdfsDataOutputStream AppendFileWithoutClosing(Path file, int length)
		{
			byte[] toAppend = new byte[length];
			Random random = new Random();
			random.NextBytes(toAppend);
			HdfsDataOutputStream @out = (HdfsDataOutputStream)hdfs.Append(file);
			@out.Write(toAppend);
			return @out;
		}

		/// <summary>Save the fsimage to a temp file</summary>
		/// <exception cref="System.IO.IOException"/>
		private FilePath SaveFSImageToTempFile()
		{
			SaveNamespaceContext context = new SaveNamespaceContext(fsn, txid, new Canceler()
				);
			FSImageFormatProtobuf.Saver saver = new FSImageFormatProtobuf.Saver(context);
			FSImageCompression compression = FSImageCompression.CreateCompression(conf);
			FilePath imageFile = GetImageFile(testDir, txid);
			fsn.ReadLock();
			try
			{
				saver.Save(imageFile, compression);
			}
			finally
			{
				fsn.ReadUnlock();
			}
			return imageFile;
		}

		/// <summary>Load the fsimage from a temp file</summary>
		/// <exception cref="System.IO.IOException"/>
		private void LoadFSImageFromTempFile(FilePath imageFile)
		{
			FSImageFormat.LoaderDelegator loader = FSImageFormat.NewLoader(conf, fsn);
			fsn.WriteLock();
			fsn.GetFSDirectory().WriteLock();
			try
			{
				loader.Load(imageFile, false);
				FSImage.UpdateCountForQuota(fsn.GetBlockManager().GetStoragePolicySuite(), INodeDirectory
					.ValueOf(fsn.GetFSDirectory().GetINode("/"), "/"));
			}
			finally
			{
				fsn.GetFSDirectory().WriteUnlock();
				fsn.WriteUnlock();
			}
		}

		/// <summary>Test when there is snapshot taken on root</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSnapshotOnRoot()
		{
			Path root = new Path("/");
			hdfs.AllowSnapshot(root);
			hdfs.CreateSnapshot(root, "s1");
			cluster.Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(Replication
				).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
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
			INodeDirectory rootNode = fsn.dir.GetINode4Write(root.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue("The children list of root should be empty", rootNode
				.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId
				).IsEmpty());
			// one snapshot on root: s1
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = rootNode.GetDiffs().
				AsList();
			NUnit.Framework.Assert.AreEqual(1, diffList.Count);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s1 = rootNode.GetSnapshot
				(DFSUtil.String2Bytes("s1"));
			NUnit.Framework.Assert.AreEqual(s1.GetId(), diffList[0].GetSnapshotId());
			// check SnapshotManager's snapshottable directory list
			NUnit.Framework.Assert.AreEqual(1, fsn.GetSnapshotManager().GetNumSnapshottableDirs
				());
			SnapshottableDirectoryStatus[] sdirs = fsn.GetSnapshotManager().GetSnapshottableDirListing
				(null);
			NUnit.Framework.Assert.AreEqual(root, sdirs[0].GetFullPath());
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
		}

		/// <summary>
		/// Testing steps:
		/// <pre>
		/// 1.
		/// </summary>
		/// <remarks>
		/// Testing steps:
		/// <pre>
		/// 1. Creating/modifying directories/files while snapshots are being taken.
		/// 2. Dump the FSDirectory tree of the namesystem.
		/// 3. Save the namesystem to a temp file (FSImage saving).
		/// 4. Restart the cluster and format the namesystem.
		/// 5. Load the namesystem from the temp file (FSImage loading).
		/// 6. Dump the FSDirectory again and compare the two dumped string.
		/// </pre>
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSaveLoadImage()
		{
			int s = 0;
			// make changes to the namesystem
			hdfs.Mkdirs(dir);
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s" + ++s);
			Path sub1 = new Path(dir, "sub1");
			hdfs.Mkdirs(sub1);
			hdfs.SetPermission(sub1, new FsPermission((short)0x1ff));
			Path sub11 = new Path(sub1, "sub11");
			hdfs.Mkdirs(sub11);
			CheckImage(s);
			hdfs.CreateSnapshot(dir, "s" + ++s);
			Path sub1file1 = new Path(sub1, "sub1file1");
			Path sub1file2 = new Path(sub1, "sub1file2");
			DFSTestUtil.CreateFile(hdfs, sub1file1, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, sub1file2, Blocksize, Replication, seed);
			CheckImage(s);
			hdfs.CreateSnapshot(dir, "s" + ++s);
			Path sub2 = new Path(dir, "sub2");
			Path sub2file1 = new Path(sub2, "sub2file1");
			Path sub2file2 = new Path(sub2, "sub2file2");
			DFSTestUtil.CreateFile(hdfs, sub2file1, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, sub2file2, Blocksize, Replication, seed);
			CheckImage(s);
			hdfs.CreateSnapshot(dir, "s" + ++s);
			hdfs.SetReplication(sub1file1, (short)(Replication - 1));
			hdfs.Delete(sub1file2, true);
			hdfs.SetOwner(sub2, "dr.who", "unknown");
			hdfs.Delete(sub2file1, true);
			CheckImage(s);
			hdfs.CreateSnapshot(dir, "s" + ++s);
			Path sub1_sub2file2 = new Path(sub1, "sub2file2");
			hdfs.Rename(sub2file2, sub1_sub2file2);
			hdfs.Rename(sub1file1, sub2file1);
			CheckImage(s);
			hdfs.Rename(sub2file1, sub2file2);
			CheckImage(s);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckImage(int s)
		{
			string name = "s" + s;
			// dump the fsdir tree
			FilePath fsnBefore = DumpTree2File(name + "_before");
			// save the namesystem to a temp file
			FilePath imageFile = SaveFSImageToTempFile();
			long numSdirBefore = fsn.GetNumSnapshottableDirs();
			long numSnapshotBefore = fsn.GetNumSnapshots();
			SnapshottableDirectoryStatus[] dirBefore = hdfs.GetSnapshottableDirListing();
			// shutdown the cluster
			cluster.Shutdown();
			// dump the fsdir tree
			FilePath fsnBetween = DumpTree2File(name + "_between");
			SnapshotTestHelper.CompareDumpedTreeInFile(fsnBefore, fsnBetween, true);
			// restart the cluster, and format the cluster
			cluster = new MiniDFSCluster.Builder(conf).Format(true).NumDataNodes(Replication)
				.Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
			// load the namesystem from the temp file
			LoadFSImageFromTempFile(imageFile);
			// dump the fsdir tree again
			FilePath fsnAfter = DumpTree2File(name + "_after");
			// compare two dumped tree
			SnapshotTestHelper.CompareDumpedTreeInFile(fsnBefore, fsnAfter, true);
			long numSdirAfter = fsn.GetNumSnapshottableDirs();
			long numSnapshotAfter = fsn.GetNumSnapshots();
			SnapshottableDirectoryStatus[] dirAfter = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(numSdirBefore, numSdirAfter);
			NUnit.Framework.Assert.AreEqual(numSnapshotBefore, numSnapshotAfter);
			NUnit.Framework.Assert.AreEqual(dirBefore.Length, dirAfter.Length);
			IList<string> pathListBefore = new AList<string>();
			foreach (SnapshottableDirectoryStatus sBefore in dirBefore)
			{
				pathListBefore.AddItem(sBefore.GetFullPath().ToString());
			}
			foreach (SnapshottableDirectoryStatus sAfter in dirAfter)
			{
				NUnit.Framework.Assert.IsTrue(pathListBefore.Contains(sAfter.GetFullPath().ToString
					()));
			}
		}

		/// <summary>Test the fsimage saving/loading while file appending.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSaveLoadImageWithAppending()
		{
			Path sub1 = new Path(dir, "sub1");
			Path sub1file1 = new Path(sub1, "sub1file1");
			Path sub1file2 = new Path(sub1, "sub1file2");
			DFSTestUtil.CreateFile(hdfs, sub1file1, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, sub1file2, Blocksize, Replication, seed);
			// 1. create snapshot s0
			hdfs.AllowSnapshot(dir);
			hdfs.CreateSnapshot(dir, "s0");
			// 2. create snapshot s1 before appending sub1file1 finishes
			HdfsDataOutputStream @out = AppendFileWithoutClosing(sub1file1, Blocksize);
			@out.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
			// also append sub1file2
			DFSTestUtil.AppendFile(hdfs, sub1file2, Blocksize);
			hdfs.CreateSnapshot(dir, "s1");
			@out.Close();
			// 3. create snapshot s2 before appending finishes
			@out = AppendFileWithoutClosing(sub1file1, Blocksize);
			@out.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
			hdfs.CreateSnapshot(dir, "s2");
			@out.Close();
			// 4. save fsimage before appending finishes
			@out = AppendFileWithoutClosing(sub1file1, Blocksize);
			@out.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
			// dump fsdir
			FilePath fsnBefore = DumpTree2File("before");
			// save the namesystem to a temp file
			FilePath imageFile = SaveFSImageToTempFile();
			// 5. load fsimage and compare
			// first restart the cluster, and format the cluster
			@out.Close();
			cluster.Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).Format(true).NumDataNodes(Replication)
				.Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
			// then load the fsimage
			LoadFSImageFromTempFile(imageFile);
			// dump the fsdir tree again
			FilePath fsnAfter = DumpTree2File("after");
			// compare two dumped tree
			SnapshotTestHelper.CompareDumpedTreeInFile(fsnBefore, fsnAfter, true);
		}

		/// <summary>Test the fsimage loading while there is file under construction.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestLoadImageWithAppending()
		{
			Path sub1 = new Path(dir, "sub1");
			Path sub1file1 = new Path(sub1, "sub1file1");
			Path sub1file2 = new Path(sub1, "sub1file2");
			DFSTestUtil.CreateFile(hdfs, sub1file1, Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, sub1file2, Blocksize, Replication, seed);
			hdfs.AllowSnapshot(dir);
			hdfs.CreateSnapshot(dir, "s0");
			HdfsDataOutputStream @out = AppendFileWithoutClosing(sub1file1, Blocksize);
			@out.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
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
		}

		/// <summary>
		/// Test fsimage loading when 1) there is an empty file loaded from fsimage,
		/// and 2) there is later an append operation to be applied from edit log.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestLoadImageWithEmptyFile()
		{
			// create an empty file
			Path file = new Path(dir, "file");
			FSDataOutputStream @out = hdfs.Create(file);
			@out.Close();
			// save namespace
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			hdfs.SaveNamespace();
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			// append to the empty file
			@out = hdfs.Append(file);
			@out.Write(1);
			@out.Close();
			// restart cluster
			cluster.Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(Replication
				).Build();
			cluster.WaitActive();
			hdfs = cluster.GetFileSystem();
			FileStatus status = hdfs.GetFileStatus(file);
			NUnit.Framework.Assert.AreEqual(1, status.GetLen());
		}

		/// <summary>Testing a special case with snapshots.</summary>
		/// <remarks>
		/// Testing a special case with snapshots. When the following steps happen:
		/// <pre>
		/// 1. Take snapshot s1 on dir.
		/// 2. Create new dir and files under subsubDir, which is descendant of dir.
		/// 3. Take snapshot s2 on dir.
		/// 4. Delete subsubDir.
		/// 5. Delete snapshot s2.
		/// </pre>
		/// When we merge the diff from s2 to s1 (since we deleted s2), we need to make
		/// sure all the files/dirs created after s1 should be destroyed. Otherwise
		/// we may save these files/dirs to the fsimage, and cause FileNotFound
		/// Exception while loading fsimage.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestSaveLoadImageAfterSnapshotDeletion()
		{
			// create initial dir and subdir
			Path dir = new Path("/dir");
			Path subDir = new Path(dir, "subdir");
			Path subsubDir = new Path(subDir, "subsubdir");
			hdfs.Mkdirs(subsubDir);
			// take snapshots on subdir and dir
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s1");
			// create new dir under initial dir
			Path newDir = new Path(subsubDir, "newdir");
			Path newFile = new Path(newDir, "newfile");
			hdfs.Mkdirs(newDir);
			DFSTestUtil.CreateFile(hdfs, newFile, Blocksize, Replication, seed);
			// create another snapshot
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s2");
			// delete subsubdir
			hdfs.Delete(subsubDir, true);
			// delete snapshot s2
			hdfs.DeleteSnapshot(dir, "s2");
			// restart cluster
			cluster.Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Format(false
				).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
			// save namespace to fsimage
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			hdfs.SaveNamespace();
			hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			cluster.Shutdown();
			cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(Replication
				).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			hdfs = cluster.GetFileSystem();
		}

		public TestFSImageWithSnapshot()
		{
			{
				SnapshotTestHelper.DisableLogs();
				((Log4JLogger)INode.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
