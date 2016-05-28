using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>This class provides tests for BlockInfo class, which is used in BlocksMap.
	/// 	</summary>
	/// <remarks>
	/// This class provides tests for BlockInfo class, which is used in BlocksMap.
	/// The test covers BlockList.listMoveToHead, used for faster block report
	/// processing in DatanodeDescriptor.reportDiff.
	/// </remarks>
	public class TestBlockInfo
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestBlockInfo"
			);

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddStorage()
		{
			BlockInfoContiguous blockInfo = new BlockInfoContiguous((short)3);
			DatanodeStorageInfo storage = DFSTestUtil.CreateDatanodeStorageInfo("storageID", 
				"127.0.0.1");
			bool added = blockInfo.AddStorage(storage);
			NUnit.Framework.Assert.IsTrue(added);
			NUnit.Framework.Assert.AreEqual(storage, blockInfo.GetStorageInfo(0));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceStorage()
		{
			// Create two dummy storages.
			DatanodeStorageInfo storage1 = DFSTestUtil.CreateDatanodeStorageInfo("storageID1"
				, "127.0.0.1");
			DatanodeStorageInfo storage2 = new DatanodeStorageInfo(storage1.GetDatanodeDescriptor
				(), new DatanodeStorage("storageID2"));
			int NumBlocks = 10;
			BlockInfoContiguous[] blockInfos = new BlockInfoContiguous[NumBlocks];
			// Create a few dummy blocks and add them to the first storage.
			for (int i = 0; i < NumBlocks; ++i)
			{
				blockInfos[i] = new BlockInfoContiguous((short)3);
				storage1.AddBlock(blockInfos[i]);
			}
			// Try to move one of the blocks to a different storage.
			bool added = storage2.AddBlock(blockInfos[NumBlocks / 2]) == DatanodeStorageInfo.AddBlockResult
				.Added;
			Assert.AssertThat(added, IS.Is(false));
			Assert.AssertThat(blockInfos[NumBlocks / 2].GetStorageInfo(0), IS.Is(storage2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockListMoveToHead()
		{
			Log.Info("BlockInfo moveToHead tests...");
			int MaxBlocks = 10;
			DatanodeStorageInfo dd = DFSTestUtil.CreateDatanodeStorageInfo("s1", "1.1.1.1");
			AList<Block> blockList = new AList<Block>(MaxBlocks);
			AList<BlockInfoContiguous> blockInfoList = new AList<BlockInfoContiguous>();
			int headIndex;
			int curIndex;
			Log.Info("Building block list...");
			for (int i = 0; i < MaxBlocks; i++)
			{
				blockList.AddItem(new Block(i, 0, GenerationStamp.LastReservedStamp));
				blockInfoList.AddItem(new BlockInfoContiguous(blockList[i], (short)3));
				dd.AddBlock(blockInfoList[i]);
				// index of the datanode should be 0
				NUnit.Framework.Assert.AreEqual("Find datanode should be 0", 0, blockInfoList[i].
					FindStorageInfo(dd));
			}
			// list length should be equal to the number of blocks we inserted
			Log.Info("Checking list length...");
			NUnit.Framework.Assert.AreEqual("Length should be MAX_BLOCK", MaxBlocks, dd.NumBlocks
				());
			IEnumerator<BlockInfoContiguous> it = dd.GetBlockIterator();
			int len = 0;
			while (it.HasNext())
			{
				it.Next();
				len++;
			}
			NUnit.Framework.Assert.AreEqual("There should be MAX_BLOCK blockInfo's", MaxBlocks
				, len);
			headIndex = dd.GetBlockListHeadForTesting().FindStorageInfo(dd);
			Log.Info("Moving each block to the head of the list...");
			for (int i_1 = 0; i_1 < MaxBlocks; i_1++)
			{
				curIndex = blockInfoList[i_1].FindStorageInfo(dd);
				headIndex = dd.MoveBlockToHead(blockInfoList[i_1], curIndex, headIndex);
				// the moved element must be at the head of the list
				NUnit.Framework.Assert.AreEqual("Block should be at the head of the list now.", blockInfoList
					[i_1], dd.GetBlockListHeadForTesting());
			}
			// move head of the list to the head - this should not change the list
			Log.Info("Moving head to the head...");
			BlockInfoContiguous temp = dd.GetBlockListHeadForTesting();
			curIndex = 0;
			headIndex = 0;
			dd.MoveBlockToHead(temp, curIndex, headIndex);
			NUnit.Framework.Assert.AreEqual("Moving head to the head of the list shopuld not change the list"
				, temp, dd.GetBlockListHeadForTesting());
			// check all elements of the list against the original blockInfoList
			Log.Info("Checking elements of the list...");
			temp = dd.GetBlockListHeadForTesting();
			NUnit.Framework.Assert.IsNotNull("Head should not be null", temp);
			int c = MaxBlocks - 1;
			while (temp != null)
			{
				NUnit.Framework.Assert.AreEqual("Expected element is not on the list", blockInfoList
					[c--], temp);
				temp = temp.GetNext(0);
			}
			Log.Info("Moving random blocks to the head of the list...");
			headIndex = dd.GetBlockListHeadForTesting().FindStorageInfo(dd);
			Random rand = new Random();
			for (int i_2 = 0; i_2 < MaxBlocks; i_2++)
			{
				int j = rand.Next(MaxBlocks);
				curIndex = blockInfoList[j].FindStorageInfo(dd);
				headIndex = dd.MoveBlockToHead(blockInfoList[j], curIndex, headIndex);
				// the moved element must be at the head of the list
				NUnit.Framework.Assert.AreEqual("Block should be at the head of the list now.", blockInfoList
					[j], dd.GetBlockListHeadForTesting());
			}
		}
	}
}
