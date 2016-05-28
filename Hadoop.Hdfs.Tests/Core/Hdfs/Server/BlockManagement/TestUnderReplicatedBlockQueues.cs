using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestUnderReplicatedBlockQueues
	{
		/// <summary>
		/// Test that adding blocks with different replication counts puts them
		/// into different queues
		/// </summary>
		/// <exception cref="System.Exception">if something goes wrong</exception>
		[NUnit.Framework.Test]
		public virtual void TestBlockPriorities()
		{
			UnderReplicatedBlocks queues = new UnderReplicatedBlocks();
			Block block1 = new Block(1);
			Block block2 = new Block(2);
			Block block_very_under_replicated = new Block(3);
			Block block_corrupt = new Block(4);
			Block block_corrupt_repl_one = new Block(5);
			//add a block with a single entry
			AssertAdded(queues, block1, 1, 0, 3);
			NUnit.Framework.Assert.AreEqual(1, queues.GetUnderReplicatedBlockCount());
			NUnit.Framework.Assert.AreEqual(1, queues.Size());
			AssertInLevel(queues, block1, UnderReplicatedBlocks.QueueHighestPriority);
			//repeated additions fail
			NUnit.Framework.Assert.IsFalse(queues.Add(block1, 1, 0, 3));
			//add a second block with two replicas
			AssertAdded(queues, block2, 2, 0, 3);
			NUnit.Framework.Assert.AreEqual(2, queues.GetUnderReplicatedBlockCount());
			NUnit.Framework.Assert.AreEqual(2, queues.Size());
			AssertInLevel(queues, block2, UnderReplicatedBlocks.QueueUnderReplicated);
			//now try to add a block that is corrupt
			AssertAdded(queues, block_corrupt, 0, 0, 3);
			NUnit.Framework.Assert.AreEqual(3, queues.Size());
			NUnit.Framework.Assert.AreEqual(2, queues.GetUnderReplicatedBlockCount());
			NUnit.Framework.Assert.AreEqual(1, queues.GetCorruptBlockSize());
			AssertInLevel(queues, block_corrupt, UnderReplicatedBlocks.QueueWithCorruptBlocks
				);
			//insert a very under-replicated block
			AssertAdded(queues, block_very_under_replicated, 4, 0, 25);
			AssertInLevel(queues, block_very_under_replicated, UnderReplicatedBlocks.QueueVeryUnderReplicated
				);
			//insert a corrupt block with replication factor 1
			AssertAdded(queues, block_corrupt_repl_one, 0, 0, 1);
			NUnit.Framework.Assert.AreEqual(2, queues.GetCorruptBlockSize());
			NUnit.Framework.Assert.AreEqual(1, queues.GetCorruptReplOneBlockSize());
			queues.Update(block_corrupt_repl_one, 0, 0, 3, 0, 2);
			NUnit.Framework.Assert.AreEqual(0, queues.GetCorruptReplOneBlockSize());
			queues.Update(block_corrupt, 0, 0, 1, 0, -2);
			NUnit.Framework.Assert.AreEqual(1, queues.GetCorruptReplOneBlockSize());
			queues.Update(block_very_under_replicated, 0, 0, 1, -4, -24);
			NUnit.Framework.Assert.AreEqual(2, queues.GetCorruptReplOneBlockSize());
		}

		private void AssertAdded(UnderReplicatedBlocks queues, Block block, int curReplicas
			, int decomissionedReplicas, int expectedReplicas)
		{
			NUnit.Framework.Assert.IsTrue("Failed to add " + block, queues.Add(block, curReplicas
				, decomissionedReplicas, expectedReplicas));
		}

		/// <summary>Determine whether or not a block is in a level without changing the API.
		/// 	</summary>
		/// <remarks>
		/// Determine whether or not a block is in a level without changing the API.
		/// Instead get the per-level iterator and run though it looking for a match.
		/// If the block is not found, an assertion is thrown.
		/// This is inefficient, but this is only a test case.
		/// </remarks>
		/// <param name="queues">queues to scan</param>
		/// <param name="block">block to look for</param>
		/// <param name="level">level to select</param>
		private void AssertInLevel(UnderReplicatedBlocks queues, Block block, int level)
		{
			UnderReplicatedBlocks.BlockIterator bi = queues.Iterator(level);
			while (bi.HasNext())
			{
				Block next = bi.Next();
				if (block.Equals(next))
				{
					return;
				}
			}
			NUnit.Framework.Assert.Fail("Block " + block + " not found in level " + level);
		}
	}
}
