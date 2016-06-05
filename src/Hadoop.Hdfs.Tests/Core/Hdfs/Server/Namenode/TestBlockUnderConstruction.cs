using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestBlockUnderConstruction
	{
		internal const string BaseDir = "/test/TestBlockUnderConstruction";

		internal const int BlockSize = 8192;

		internal const int NumBlocks = 5;

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem hdfs;

		// same as TestFileCreation.blocksize
		// number of blocks to write
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			Configuration conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
			hdfs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (hdfs != null)
			{
				hdfs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void WriteFile(Path file, FSDataOutputStream stm, int size)
		{
			long blocksBefore = stm.GetPos() / BlockSize;
			TestFileCreation.WriteFile(stm, BlockSize);
			// need to make sure the full block is completely flushed to the DataNodes
			// (see FSOutputSummer#flush)
			stm.Flush();
			int blocksAfter = 0;
			// wait until the block is allocated by DataStreamer
			BlockLocation[] locatedBlocks;
			while (blocksAfter <= blocksBefore)
			{
				locatedBlocks = DFSClientAdapter.GetDFSClient(hdfs).GetBlockLocations(file.ToString
					(), 0L, BlockSize * NumBlocks);
				blocksAfter = locatedBlocks == null ? 0 : locatedBlocks.Length;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyFileBlocks(string file, bool isFileOpen)
		{
			FSNamesystem ns = cluster.GetNamesystem();
			INodeFile inode = INodeFile.ValueOf(ns.dir.GetINode(file), file);
			NUnit.Framework.Assert.IsTrue("File " + inode.ToString() + " isUnderConstruction = "
				 + inode.IsUnderConstruction() + " expected to be " + isFileOpen, inode.IsUnderConstruction
				() == isFileOpen);
			BlockInfoContiguous[] blocks = inode.GetBlocks();
			NUnit.Framework.Assert.IsTrue("File does not have blocks: " + inode.ToString(), blocks
				 != null && blocks.Length > 0);
			int idx = 0;
			BlockInfoContiguous curBlock;
			// all blocks but the last two should be regular blocks
			for (; idx < blocks.Length - 2; idx++)
			{
				curBlock = blocks[idx];
				NUnit.Framework.Assert.IsTrue("Block is not complete: " + curBlock, curBlock.IsComplete
					());
				NUnit.Framework.Assert.IsTrue("Block is not in BlocksMap: " + curBlock, ns.GetBlockManager
					().GetStoredBlock(curBlock) == curBlock);
			}
			// the penultimate block is either complete or
			// committed if the file is not closed
			if (idx > 0)
			{
				curBlock = blocks[idx - 1];
				// penultimate block
				NUnit.Framework.Assert.IsTrue("Block " + curBlock + " isUnderConstruction = " + inode
					.IsUnderConstruction() + " expected to be " + isFileOpen, (isFileOpen && curBlock
					.IsComplete()) || (!isFileOpen && !curBlock.IsComplete() == (curBlock.GetBlockUCState
					() == HdfsServerConstants.BlockUCState.Committed)));
				NUnit.Framework.Assert.IsTrue("Block is not in BlocksMap: " + curBlock, ns.GetBlockManager
					().GetStoredBlock(curBlock) == curBlock);
			}
			// The last block is complete if the file is closed.
			// If the file is open, the last block may be complete or not. 
			curBlock = blocks[idx];
			// last block
			if (!isFileOpen)
			{
				NUnit.Framework.Assert.IsTrue("Block " + curBlock + ", isFileOpen = " + isFileOpen
					, curBlock.IsComplete());
			}
			NUnit.Framework.Assert.IsTrue("Block is not in BlocksMap: " + curBlock, ns.GetBlockManager
				().GetStoredBlock(curBlock) == curBlock);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockCreation()
		{
			Path file1 = new Path(BaseDir, "file1.dat");
			FSDataOutputStream @out = TestFileCreation.CreateFile(hdfs, file1, 3);
			for (int idx = 0; idx < NumBlocks; idx++)
			{
				// write one block
				WriteFile(file1, @out, BlockSize);
				// verify consistency
				VerifyFileBlocks(file1.ToString(), true);
			}
			// close file
			@out.Close();
			// verify consistency
			VerifyFileBlocks(file1.ToString(), false);
		}

		/// <summary>Test NameNode.getBlockLocations(..) on reading un-closed files.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetBlockLocations()
		{
			NamenodeProtocols namenode = cluster.GetNameNodeRpc();
			Path p = new Path(BaseDir, "file2.dat");
			string src = p.ToString();
			FSDataOutputStream @out = TestFileCreation.CreateFile(hdfs, p, 3);
			// write a half block
			int len = (int)(((uint)BlockSize) >> 1);
			WriteFile(p, @out, len);
			for (int i = 1; i < NumBlocks; )
			{
				// verify consistency
				LocatedBlocks lb = namenode.GetBlockLocations(src, 0, len);
				IList<LocatedBlock> blocks = lb.GetLocatedBlocks();
				NUnit.Framework.Assert.AreEqual(i, blocks.Count);
				Block b = blocks[blocks.Count - 1].GetBlock().GetLocalBlock();
				NUnit.Framework.Assert.IsTrue(b is BlockInfoContiguousUnderConstruction);
				if (++i < NumBlocks)
				{
					// write one more block
					WriteFile(p, @out, BlockSize);
					len += BlockSize;
				}
			}
			// close file
			@out.Close();
		}
	}
}
