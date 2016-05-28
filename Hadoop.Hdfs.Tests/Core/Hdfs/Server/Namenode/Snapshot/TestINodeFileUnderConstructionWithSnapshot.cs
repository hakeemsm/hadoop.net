using System.Collections.Generic;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Test snapshot functionalities while file appending.</summary>
	public class TestINodeFileUnderConstructionWithSnapshot
	{
		internal const long seed = 0;

		internal const short Replication = 3;

		internal const int Blocksize = 1024;

		private readonly Path dir = new Path("/TestSnapshot");

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
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, Blocksize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			fsdir = fsn.GetFSDirectory();
			hdfs = cluster.GetFileSystem();
			hdfs.Mkdirs(dir);
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

		/// <summary>Test snapshot after file appending</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotAfterAppending()
		{
			Path file = new Path(dir, "file");
			// 1. create snapshot --> create file --> append
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s0");
			DFSTestUtil.CreateFile(hdfs, file, Blocksize, Replication, seed);
			DFSTestUtil.AppendFile(hdfs, file, Blocksize);
			INodeFile fileNode = (INodeFile)fsdir.GetINode(file.ToString());
			// 2. create snapshot --> modify the file --> append
			hdfs.CreateSnapshot(dir, "s1");
			hdfs.SetReplication(file, (short)(Replication - 1));
			DFSTestUtil.AppendFile(hdfs, file, Blocksize);
			// check corresponding inodes
			fileNode = (INodeFile)fsdir.GetINode(file.ToString());
			NUnit.Framework.Assert.AreEqual(Replication - 1, fileNode.GetFileReplication());
			NUnit.Framework.Assert.AreEqual(Blocksize * 3, fileNode.ComputeFileSize());
			// 3. create snapshot --> append
			hdfs.CreateSnapshot(dir, "s2");
			DFSTestUtil.AppendFile(hdfs, file, Blocksize);
			// check corresponding inodes
			fileNode = (INodeFile)fsdir.GetINode(file.ToString());
			NUnit.Framework.Assert.AreEqual(Replication - 1, fileNode.GetFileReplication());
			NUnit.Framework.Assert.AreEqual(Blocksize * 4, fileNode.ComputeFileSize());
		}

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

		/// <summary>
		/// Test snapshot during file appending, before the corresponding
		/// <see cref="Org.Apache.Hadoop.FS.FSDataOutputStream"/>
		/// instance closes.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotWhileAppending()
		{
			Path file = new Path(dir, "file");
			DFSTestUtil.CreateFile(hdfs, file, Blocksize, Replication, seed);
			// 1. append without closing stream --> create snapshot
			HdfsDataOutputStream @out = AppendFileWithoutClosing(file, Blocksize);
			@out.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s0");
			@out.Close();
			// check: an INodeFileUnderConstructionWithSnapshot should be stored into s0's
			// deleted list, with size BLOCKSIZE*2
			INodeFile fileNode = (INodeFile)fsdir.GetINode(file.ToString());
			NUnit.Framework.Assert.AreEqual(Blocksize * 2, fileNode.ComputeFileSize());
			INodeDirectory dirNode = fsdir.GetINode(dir.ToString()).AsDirectory();
			DirectoryWithSnapshotFeature.DirectoryDiff last = dirNode.GetDiffs().GetLast();
			// 2. append without closing stream
			@out = AppendFileWithoutClosing(file, Blocksize);
			@out.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
			// re-check nodeInDeleted_S0
			dirNode = fsdir.GetINode(dir.ToString()).AsDirectory();
			NUnit.Framework.Assert.AreEqual(Blocksize * 2, fileNode.ComputeFileSize(last.GetSnapshotId
				()));
			// 3. take snapshot --> close stream
			hdfs.CreateSnapshot(dir, "s1");
			@out.Close();
			// check: an INodeFileUnderConstructionWithSnapshot with size BLOCKSIZE*3 should
			// have been stored in s1's deleted list
			fileNode = (INodeFile)fsdir.GetINode(file.ToString());
			dirNode = fsdir.GetINode(dir.ToString()).AsDirectory();
			last = dirNode.GetDiffs().GetLast();
			NUnit.Framework.Assert.IsTrue(fileNode.IsWithSnapshot());
			NUnit.Framework.Assert.AreEqual(Blocksize * 3, fileNode.ComputeFileSize(last.GetSnapshotId
				()));
			// 4. modify file --> append without closing stream --> take snapshot -->
			// close stream
			hdfs.SetReplication(file, (short)(Replication - 1));
			@out = AppendFileWithoutClosing(file, Blocksize);
			hdfs.CreateSnapshot(dir, "s2");
			@out.Close();
			// re-check the size of nodeInDeleted_S1
			NUnit.Framework.Assert.AreEqual(Blocksize * 3, fileNode.ComputeFileSize(last.GetSnapshotId
				()));
		}

		/// <summary>call DFSClient#callGetBlockLocations(...) for snapshot file.</summary>
		/// <remarks>
		/// call DFSClient#callGetBlockLocations(...) for snapshot file. Make sure only
		/// blocks within the size range are returned.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetBlockLocations()
		{
			Path root = new Path("/");
			Path file = new Path("/file");
			DFSTestUtil.CreateFile(hdfs, file, Blocksize, Replication, seed);
			// take a snapshot on root
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s1");
			Path fileInSnapshot = SnapshotTestHelper.GetSnapshotPath(root, "s1", file.GetName
				());
			FileStatus status = hdfs.GetFileStatus(fileInSnapshot);
			// make sure we record the size for the file
			NUnit.Framework.Assert.AreEqual(Blocksize, status.GetLen());
			// append data to file
			DFSTestUtil.AppendFile(hdfs, file, Blocksize - 1);
			status = hdfs.GetFileStatus(fileInSnapshot);
			// the size of snapshot file should still be BLOCKSIZE
			NUnit.Framework.Assert.AreEqual(Blocksize, status.GetLen());
			// the size of the file should be (2 * BLOCKSIZE - 1)
			status = hdfs.GetFileStatus(file);
			NUnit.Framework.Assert.AreEqual(Blocksize * 2 - 1, status.GetLen());
			// call DFSClient#callGetBlockLocations for the file in snapshot
			LocatedBlocks blocks = DFSClientAdapter.CallGetBlockLocations(cluster.GetNameNodeRpc
				(), fileInSnapshot.ToString(), 0, long.MaxValue);
			IList<LocatedBlock> blockList = blocks.GetLocatedBlocks();
			// should be only one block
			NUnit.Framework.Assert.AreEqual(Blocksize, blocks.GetFileLength());
			NUnit.Framework.Assert.AreEqual(1, blockList.Count);
			// check the last block
			LocatedBlock lastBlock = blocks.GetLastLocatedBlock();
			NUnit.Framework.Assert.AreEqual(0, lastBlock.GetStartOffset());
			NUnit.Framework.Assert.AreEqual(Blocksize, lastBlock.GetBlockSize());
			// take another snapshot
			SnapshotTestHelper.CreateSnapshot(hdfs, root, "s2");
			Path fileInSnapshot2 = SnapshotTestHelper.GetSnapshotPath(root, "s2", file.GetName
				());
			// append data to file without closing
			HdfsDataOutputStream @out = AppendFileWithoutClosing(file, Blocksize);
			@out.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
			status = hdfs.GetFileStatus(fileInSnapshot2);
			// the size of snapshot file should be BLOCKSIZE*2-1
			NUnit.Framework.Assert.AreEqual(Blocksize * 2 - 1, status.GetLen());
			// the size of the file should be (3 * BLOCKSIZE - 1)
			status = hdfs.GetFileStatus(file);
			NUnit.Framework.Assert.AreEqual(Blocksize * 3 - 1, status.GetLen());
			blocks = DFSClientAdapter.CallGetBlockLocations(cluster.GetNameNodeRpc(), fileInSnapshot2
				.ToString(), 0, long.MaxValue);
			NUnit.Framework.Assert.IsFalse(blocks.IsUnderConstruction());
			NUnit.Framework.Assert.IsTrue(blocks.IsLastBlockComplete());
			blockList = blocks.GetLocatedBlocks();
			// should be 2 blocks
			NUnit.Framework.Assert.AreEqual(Blocksize * 2 - 1, blocks.GetFileLength());
			NUnit.Framework.Assert.AreEqual(2, blockList.Count);
			// check the last block
			lastBlock = blocks.GetLastLocatedBlock();
			NUnit.Framework.Assert.AreEqual(Blocksize, lastBlock.GetStartOffset());
			NUnit.Framework.Assert.AreEqual(Blocksize, lastBlock.GetBlockSize());
			blocks = DFSClientAdapter.CallGetBlockLocations(cluster.GetNameNodeRpc(), fileInSnapshot2
				.ToString(), Blocksize, 0);
			blockList = blocks.GetLocatedBlocks();
			NUnit.Framework.Assert.AreEqual(1, blockList.Count);
			// check blocks for file being written
			blocks = DFSClientAdapter.CallGetBlockLocations(cluster.GetNameNodeRpc(), file.ToString
				(), 0, long.MaxValue);
			blockList = blocks.GetLocatedBlocks();
			NUnit.Framework.Assert.AreEqual(3, blockList.Count);
			NUnit.Framework.Assert.IsTrue(blocks.IsUnderConstruction());
			NUnit.Framework.Assert.IsFalse(blocks.IsLastBlockComplete());
			lastBlock = blocks.GetLastLocatedBlock();
			NUnit.Framework.Assert.AreEqual(Blocksize * 2, lastBlock.GetStartOffset());
			NUnit.Framework.Assert.AreEqual(Blocksize - 1, lastBlock.GetBlockSize());
			@out.Close();
		}

		public TestINodeFileUnderConstructionWithSnapshot()
		{
			{
				((Log4JLogger)INode.Log).GetLogger().SetLevel(Level.All);
				SnapshotTestHelper.DisableLogs();
			}
		}
	}
}
