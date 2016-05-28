using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Test cases for snapshot-related information in blocksMap.</summary>
	public class TestSnapshotBlocksMap
	{
		private const long seed = 0;

		private const short Replication = 3;

		private const int Blocksize = 1024;

		private readonly Path dir = new Path("/TestSnapshot");

		private readonly Path sub1;

		protected internal Configuration conf;

		protected internal MiniDFSCluster cluster;

		protected internal FSNamesystem fsn;

		internal FSDirectory fsdir;

		internal BlockManager blockmanager;

		protected internal DistributedFileSystem hdfs;

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

		/// <exception cref="System.Exception"/>
		internal virtual void AssertAllNull(INodeFile inode, Path path, string[] snapshots
			)
		{
			NUnit.Framework.Assert.IsNull(inode.GetBlocks());
			AssertINodeNull(path.ToString());
			AssertINodeNullInSnapshots(path, snapshots);
		}

		/// <exception cref="System.Exception"/>
		internal virtual void AssertINodeNull(string path)
		{
			NUnit.Framework.Assert.IsNull(fsdir.GetINode(path));
		}

		/// <exception cref="System.Exception"/>
		internal virtual void AssertINodeNullInSnapshots(Path path, params string[] snapshots
			)
		{
			foreach (string s in snapshots)
			{
				AssertINodeNull(SnapshotTestHelper.GetSnapshotPath(path.GetParent(), s, path.GetName
					()).ToString());
			}
		}

		/// <exception cref="System.Exception"/>
		internal static INodeFile AssertBlockCollection(string path, int numBlocks, FSDirectory
			 dir, BlockManager blkManager)
		{
			INodeFile file = INodeFile.ValueOf(dir.GetINode(path), path);
			NUnit.Framework.Assert.AreEqual(numBlocks, file.GetBlocks().Length);
			foreach (BlockInfoContiguous b in file.GetBlocks())
			{
				AssertBlockCollection(blkManager, file, b);
			}
			return file;
		}

		internal static void AssertBlockCollection(BlockManager blkManager, INodeFile file
			, BlockInfoContiguous b)
		{
			NUnit.Framework.Assert.AreSame(b, blkManager.GetStoredBlock(b));
			NUnit.Framework.Assert.AreSame(file, blkManager.GetBlockCollection(b));
			NUnit.Framework.Assert.AreSame(file, b.GetBlockCollection());
		}

		/// <summary>Test deleting a file with snapshots.</summary>
		/// <remarks>
		/// Test deleting a file with snapshots. Need to check the blocksMap to make
		/// sure the corresponding record is updated correctly.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeletionWithSnapshots()
		{
			Path file0 = new Path(sub1, "file0");
			Path file1 = new Path(sub1, "file1");
			Path sub2 = new Path(sub1, "sub2");
			Path file2 = new Path(sub2, "file2");
			Path file3 = new Path(sub1, "file3");
			Path file4 = new Path(sub1, "file4");
			Path file5 = new Path(sub1, "file5");
			// Create file under sub1
			DFSTestUtil.CreateFile(hdfs, file0, 4 * Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file1, 2 * Blocksize, Replication, seed);
			DFSTestUtil.CreateFile(hdfs, file2, 3 * Blocksize, Replication, seed);
			{
				// Normal deletion
				INodeFile f2 = AssertBlockCollection(file2.ToString(), 3, fsdir, blockmanager);
				BlockInfoContiguous[] blocks = f2.GetBlocks();
				hdfs.Delete(sub2, true);
				// The INode should have been removed from the blocksMap
				foreach (BlockInfoContiguous b in blocks)
				{
					NUnit.Framework.Assert.IsNull(blockmanager.GetBlockCollection(b));
				}
			}
			// Create snapshots for sub1
			string[] snapshots = new string[] { "s0", "s1", "s2" };
			DFSTestUtil.CreateFile(hdfs, file3, 5 * Blocksize, Replication, seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, snapshots[0]);
			DFSTestUtil.CreateFile(hdfs, file4, 1 * Blocksize, Replication, seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, snapshots[1]);
			DFSTestUtil.CreateFile(hdfs, file5, 7 * Blocksize, Replication, seed);
			SnapshotTestHelper.CreateSnapshot(hdfs, sub1, snapshots[2]);
			{
				// set replication so that the inode should be replaced for snapshots
				INodeFile f1 = AssertBlockCollection(file1.ToString(), 2, fsdir, blockmanager);
				NUnit.Framework.Assert.AreSame(typeof(INodeFile), f1.GetType());
				hdfs.SetReplication(file1, (short)2);
				f1 = AssertBlockCollection(file1.ToString(), 2, fsdir, blockmanager);
				NUnit.Framework.Assert.IsTrue(f1.IsWithSnapshot());
				NUnit.Framework.Assert.IsFalse(f1.IsUnderConstruction());
			}
			// Check the block information for file0
			INodeFile f0 = AssertBlockCollection(file0.ToString(), 4, fsdir, blockmanager);
			BlockInfoContiguous[] blocks0 = f0.GetBlocks();
			// Also check the block information for snapshot of file0
			Path snapshotFile0 = SnapshotTestHelper.GetSnapshotPath(sub1, "s0", file0.GetName
				());
			AssertBlockCollection(snapshotFile0.ToString(), 4, fsdir, blockmanager);
			// Delete file0
			hdfs.Delete(file0, true);
			// Make sure the blocks of file0 is still in blocksMap
			foreach (BlockInfoContiguous b_1 in blocks0)
			{
				NUnit.Framework.Assert.IsNotNull(blockmanager.GetBlockCollection(b_1));
			}
			AssertBlockCollection(snapshotFile0.ToString(), 4, fsdir, blockmanager);
			// Compare the INode in the blocksMap with INodes for snapshots
			string s1f0 = SnapshotTestHelper.GetSnapshotPath(sub1, "s1", file0.GetName()).ToString
				();
			AssertBlockCollection(s1f0, 4, fsdir, blockmanager);
			// Delete snapshot s1
			hdfs.DeleteSnapshot(sub1, "s1");
			// Make sure the first block of file0 is still in blocksMap
			foreach (BlockInfoContiguous b_2 in blocks0)
			{
				NUnit.Framework.Assert.IsNotNull(blockmanager.GetBlockCollection(b_2));
			}
			AssertBlockCollection(snapshotFile0.ToString(), 4, fsdir, blockmanager);
			try
			{
				INodeFile.ValueOf(fsdir.GetINode(s1f0), s1f0);
				NUnit.Framework.Assert.Fail("Expect FileNotFoundException when identifying the INode in a deleted Snapshot"
					);
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("File does not exist: " + s1f0, e);
			}
		}

		/*
		* Try to read the files inside snapshot but deleted in original place after
		* restarting post checkpoint. refer HDFS-5427
		*/
		/// <exception cref="System.Exception"/>
		public virtual void TestReadSnapshotFileWithCheckpoint()
		{
			Path foo = new Path("/foo");
			hdfs.Mkdirs(foo);
			hdfs.AllowSnapshot(foo);
			Path bar = new Path("/foo/bar");
			DFSTestUtil.CreateFile(hdfs, bar, 100, (short)2, 100024L);
			hdfs.CreateSnapshot(foo, "s1");
			NUnit.Framework.Assert.IsTrue(hdfs.Delete(bar, true));
			// checkpoint
			NameNode nameNode = cluster.GetNameNode();
			NameNodeAdapter.EnterSafeMode(nameNode, false);
			NameNodeAdapter.SaveNamespace(nameNode);
			NameNodeAdapter.LeaveSafeMode(nameNode);
			// restart namenode to load snapshot files from fsimage
			cluster.RestartNameNode(true);
			string snapshotPath = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.GetSnapshotPath
				(foo.ToString(), "s1/bar");
			DFSTestUtil.ReadFile(hdfs, new Path(snapshotPath));
		}

		/*
		* Try to read the files inside snapshot but renamed to different file and
		* deleted after restarting post checkpoint. refer HDFS-5427
		*/
		/// <exception cref="System.Exception"/>
		public virtual void TestReadRenamedSnapshotFileWithCheckpoint()
		{
			Path foo = new Path("/foo");
			Path foo2 = new Path("/foo2");
			hdfs.Mkdirs(foo);
			hdfs.Mkdirs(foo2);
			hdfs.AllowSnapshot(foo);
			hdfs.AllowSnapshot(foo2);
			Path bar = new Path(foo, "bar");
			Path bar2 = new Path(foo2, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, 100, (short)2, 100024L);
			hdfs.CreateSnapshot(foo, "s1");
			// rename to another snapshottable directory and take snapshot
			NUnit.Framework.Assert.IsTrue(hdfs.Rename(bar, bar2));
			hdfs.CreateSnapshot(foo2, "s2");
			// delete the original renamed file to make sure blocks are not updated by
			// the original file
			NUnit.Framework.Assert.IsTrue(hdfs.Delete(bar2, true));
			// checkpoint
			NameNode nameNode = cluster.GetNameNode();
			NameNodeAdapter.EnterSafeMode(nameNode, false);
			NameNodeAdapter.SaveNamespace(nameNode);
			NameNodeAdapter.LeaveSafeMode(nameNode);
			// restart namenode to load snapshot files from fsimage
			cluster.RestartNameNode(true);
			// file in first snapshot
			string barSnapshotPath = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotPath(foo.ToString(), "s1/bar");
			DFSTestUtil.ReadFile(hdfs, new Path(barSnapshotPath));
			// file in second snapshot after rename+delete
			string bar2SnapshotPath = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotPath(foo2.ToString(), "s2/bar");
			DFSTestUtil.ReadFile(hdfs, new Path(bar2SnapshotPath));
		}

		/// <summary>Make sure we delete 0-sized block when deleting an INodeFileUCWithSnapshot
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletionWithZeroSizeBlock()
		{
			Path foo = new Path("/foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Replication, 0L);
			SnapshotTestHelper.CreateSnapshot(hdfs, foo, "s0");
			hdfs.Append(bar);
			INodeFile barNode = fsdir.GetINode4Write(bar.ToString()).AsFile();
			BlockInfoContiguous[] blks = barNode.GetBlocks();
			NUnit.Framework.Assert.AreEqual(1, blks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize, blks[0].GetNumBytes());
			ExtendedBlock previous = new ExtendedBlock(fsn.GetBlockPoolId(), blks[0]);
			cluster.GetNameNodeRpc().AddBlock(bar.ToString(), hdfs.GetClient().GetClientName(
				), previous, null, barNode.GetId(), null);
			SnapshotTestHelper.CreateSnapshot(hdfs, foo, "s1");
			barNode = fsdir.GetINode4Write(bar.ToString()).AsFile();
			blks = barNode.GetBlocks();
			NUnit.Framework.Assert.AreEqual(2, blks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize, blks[0].GetNumBytes());
			NUnit.Framework.Assert.AreEqual(0, blks[1].GetNumBytes());
			hdfs.Delete(bar, true);
			Path sbar = SnapshotTestHelper.GetSnapshotPath(foo, "s1", bar.GetName());
			barNode = fsdir.GetINode(sbar.ToString()).AsFile();
			blks = barNode.GetBlocks();
			NUnit.Framework.Assert.AreEqual(1, blks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize, blks[0].GetNumBytes());
		}

		/// <summary>Make sure we delete 0-sized block when deleting an under-construction file
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletionWithZeroSizeBlock2()
		{
			Path foo = new Path("/foo");
			Path subDir = new Path(foo, "sub");
			Path bar = new Path(subDir, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Replication, 0L);
			hdfs.Append(bar);
			INodeFile barNode = fsdir.GetINode4Write(bar.ToString()).AsFile();
			BlockInfoContiguous[] blks = barNode.GetBlocks();
			NUnit.Framework.Assert.AreEqual(1, blks.Length);
			ExtendedBlock previous = new ExtendedBlock(fsn.GetBlockPoolId(), blks[0]);
			cluster.GetNameNodeRpc().AddBlock(bar.ToString(), hdfs.GetClient().GetClientName(
				), previous, null, barNode.GetId(), null);
			SnapshotTestHelper.CreateSnapshot(hdfs, foo, "s1");
			barNode = fsdir.GetINode4Write(bar.ToString()).AsFile();
			blks = barNode.GetBlocks();
			NUnit.Framework.Assert.AreEqual(2, blks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize, blks[0].GetNumBytes());
			NUnit.Framework.Assert.AreEqual(0, blks[1].GetNumBytes());
			hdfs.Delete(subDir, true);
			Path sbar = SnapshotTestHelper.GetSnapshotPath(foo, "s1", "sub/bar");
			barNode = fsdir.GetINode(sbar.ToString()).AsFile();
			blks = barNode.GetBlocks();
			NUnit.Framework.Assert.AreEqual(1, blks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize, blks[0].GetNumBytes());
		}

		/// <summary>1.</summary>
		/// <remarks>
		/// 1. rename under-construction file with 0-sized blocks after snapshot.
		/// 2. delete the renamed directory.
		/// make sure we delete the 0-sized block.
		/// see HDFS-5476.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletionWithZeroSizeBlock3()
		{
			Path foo = new Path("/foo");
			Path subDir = new Path(foo, "sub");
			Path bar = new Path(subDir, "bar");
			DFSTestUtil.CreateFile(hdfs, bar, Blocksize, Replication, 0L);
			hdfs.Append(bar);
			INodeFile barNode = fsdir.GetINode4Write(bar.ToString()).AsFile();
			BlockInfoContiguous[] blks = barNode.GetBlocks();
			NUnit.Framework.Assert.AreEqual(1, blks.Length);
			ExtendedBlock previous = new ExtendedBlock(fsn.GetBlockPoolId(), blks[0]);
			cluster.GetNameNodeRpc().AddBlock(bar.ToString(), hdfs.GetClient().GetClientName(
				), previous, null, barNode.GetId(), null);
			SnapshotTestHelper.CreateSnapshot(hdfs, foo, "s1");
			// rename bar
			Path bar2 = new Path(subDir, "bar2");
			hdfs.Rename(bar, bar2);
			INodeFile bar2Node = fsdir.GetINode4Write(bar2.ToString()).AsFile();
			blks = bar2Node.GetBlocks();
			NUnit.Framework.Assert.AreEqual(2, blks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize, blks[0].GetNumBytes());
			NUnit.Framework.Assert.AreEqual(0, blks[1].GetNumBytes());
			// delete subDir
			hdfs.Delete(subDir, true);
			Path sbar = SnapshotTestHelper.GetSnapshotPath(foo, "s1", "sub/bar");
			barNode = fsdir.GetINode(sbar.ToString()).AsFile();
			blks = barNode.GetBlocks();
			NUnit.Framework.Assert.AreEqual(1, blks.Length);
			NUnit.Framework.Assert.AreEqual(Blocksize, blks[0].GetNumBytes());
		}

		/// <summary>
		/// Make sure that a delete of a non-zero-length file which results in a
		/// zero-length file in a snapshot works.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletionOfLaterBlocksWithZeroSizeFirstBlock()
		{
			Path foo = new Path("/foo");
			Path bar = new Path(foo, "bar");
			byte[] testData = Sharpen.Runtime.GetBytesForString("foo bar baz");
			// Create a zero-length file.
			DFSTestUtil.CreateFile(hdfs, bar, 0, Replication, 0L);
			NUnit.Framework.Assert.AreEqual(0, fsdir.GetINode4Write(bar.ToString()).AsFile().
				GetBlocks().Length);
			// Create a snapshot that includes that file.
			SnapshotTestHelper.CreateSnapshot(hdfs, foo, "s0");
			// Extend that file.
			FSDataOutputStream @out = hdfs.Append(bar);
			@out.Write(testData);
			@out.Close();
			INodeFile barNode = fsdir.GetINode4Write(bar.ToString()).AsFile();
			BlockInfoContiguous[] blks = barNode.GetBlocks();
			NUnit.Framework.Assert.AreEqual(1, blks.Length);
			NUnit.Framework.Assert.AreEqual(testData.Length, blks[0].GetNumBytes());
			// Delete the file.
			hdfs.Delete(bar, true);
			// Now make sure that the NN can still save an fsimage successfully.
			cluster.GetNameNode().GetRpcServer().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter
				, false);
			cluster.GetNameNode().GetRpcServer().SaveNamespace();
		}

		public TestSnapshotBlocksMap()
		{
			sub1 = new Path(dir, "sub1");
		}
	}
}
