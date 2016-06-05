using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>This class tests that methods in DatanodeDescriptor</summary>
	public class TestDatanodeDescriptor
	{
		/// <summary>Test that getInvalidateBlocks observes the maxlimit.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetInvalidateBlocks()
		{
			int MaxBlocks = 10;
			int RemainingBlocks = 2;
			int MaxLimit = MaxBlocks - RemainingBlocks;
			DatanodeDescriptor dd = DFSTestUtil.GetLocalDatanodeDescriptor();
			AList<Block> blockList = new AList<Block>(MaxBlocks);
			for (int i = 0; i < MaxBlocks; i++)
			{
				blockList.AddItem(new Block(i, 0, GenerationStamp.LastReservedStamp));
			}
			dd.AddBlocksToBeInvalidated(blockList);
			Block[] bc = dd.GetInvalidateBlocks(MaxLimit);
			NUnit.Framework.Assert.AreEqual(bc.Length, MaxLimit);
			bc = dd.GetInvalidateBlocks(MaxLimit);
			NUnit.Framework.Assert.AreEqual(bc.Length, RemainingBlocks);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlocksCounter()
		{
			DatanodeDescriptor dd = BlockManagerTestUtil.GetLocalDatanodeDescriptor(true);
			NUnit.Framework.Assert.AreEqual(0, dd.NumBlocks());
			BlockInfoContiguous blk = new BlockInfoContiguous(new Block(1L), (short)1);
			BlockInfoContiguous blk1 = new BlockInfoContiguous(new Block(2L), (short)2);
			DatanodeStorageInfo[] storages = dd.GetStorageInfos();
			NUnit.Framework.Assert.IsTrue(storages.Length > 0);
			// add first block
			NUnit.Framework.Assert.IsTrue(storages[0].AddBlock(blk) == DatanodeStorageInfo.AddBlockResult
				.Added);
			NUnit.Framework.Assert.AreEqual(1, dd.NumBlocks());
			// remove a non-existent block
			NUnit.Framework.Assert.IsFalse(dd.RemoveBlock(blk1));
			NUnit.Framework.Assert.AreEqual(1, dd.NumBlocks());
			// add an existent block
			NUnit.Framework.Assert.IsFalse(storages[0].AddBlock(blk) == DatanodeStorageInfo.AddBlockResult
				.Added);
			NUnit.Framework.Assert.AreEqual(1, dd.NumBlocks());
			// add second block
			NUnit.Framework.Assert.IsTrue(storages[0].AddBlock(blk1) == DatanodeStorageInfo.AddBlockResult
				.Added);
			NUnit.Framework.Assert.AreEqual(2, dd.NumBlocks());
			// remove first block
			NUnit.Framework.Assert.IsTrue(dd.RemoveBlock(blk));
			NUnit.Framework.Assert.AreEqual(1, dd.NumBlocks());
			// remove second block
			NUnit.Framework.Assert.IsTrue(dd.RemoveBlock(blk1));
			NUnit.Framework.Assert.AreEqual(0, dd.NumBlocks());
		}
	}
}
