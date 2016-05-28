using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// Tests the sequential block ID generation mechanism and block ID
	/// collision handling.
	/// </summary>
	public class TestSequentialBlockId
	{
		private static readonly Log Log = LogFactory.GetLog("TestSequentialBlockId");

		internal readonly int BlockSize = 1024;

		internal readonly int IoSize;

		internal readonly short Replication = 1;

		internal readonly long Seed = 0;

		/// <summary>Test that block IDs are generated sequentially.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockIdGeneration()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				// Create a file that is 10 blocks long.
				Path path = new Path("testBlockIdGeneration.dat");
				DFSTestUtil.CreateFile(fs, path, IoSize, BlockSize * 10, BlockSize, Replication, 
					Seed);
				IList<LocatedBlock> blocks = DFSTestUtil.GetAllBlocks(fs, path);
				Log.Info("Block0 id is " + blocks[0].GetBlock().GetBlockId());
				long nextBlockExpectedId = blocks[0].GetBlock().GetBlockId() + 1;
				// Ensure that the block IDs are sequentially increasing.
				for (int i = 1; i < blocks.Count; ++i)
				{
					long nextBlockId = blocks[i].GetBlock().GetBlockId();
					Log.Info("Block" + i + " id is " + nextBlockId);
					Assert.AssertThat(nextBlockId, CoreMatchers.Is(nextBlockExpectedId));
					++nextBlockExpectedId;
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test that collisions in the block ID space are handled gracefully.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTriggerBlockIdCollision()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				FSNamesystem fsn = cluster.GetNamesystem();
				int blockCount = 10;
				// Create a file with a few blocks to rev up the global block ID
				// counter.
				Path path1 = new Path("testBlockIdCollisionDetection_file1.dat");
				DFSTestUtil.CreateFile(fs, path1, IoSize, BlockSize * blockCount, BlockSize, Replication
					, Seed);
				IList<LocatedBlock> blocks1 = DFSTestUtil.GetAllBlocks(fs, path1);
				// Rewind the block ID counter in the name system object. This will result
				// in block ID collisions when we try to allocate new blocks.
				SequentialBlockIdGenerator blockIdGenerator = fsn.GetBlockIdManager().GetBlockIdGenerator
					();
				blockIdGenerator.SetCurrentValue(blockIdGenerator.GetCurrentValue() - 5);
				// Trigger collisions by creating a new file.
				Path path2 = new Path("testBlockIdCollisionDetection_file2.dat");
				DFSTestUtil.CreateFile(fs, path2, IoSize, BlockSize * blockCount, BlockSize, Replication
					, Seed);
				IList<LocatedBlock> blocks2 = DFSTestUtil.GetAllBlocks(fs, path2);
				Assert.AssertThat(blocks2.Count, CoreMatchers.Is(blockCount));
				// Make sure that file2 block IDs start immediately after file1
				Assert.AssertThat(blocks2[0].GetBlock().GetBlockId(), CoreMatchers.Is(blocks1[9].
					GetBlock().GetBlockId() + 1));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test that the block type (legacy or not) can be correctly detected
		/// based on its generation stamp.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockTypeDetection()
		{
			// Setup a mock object and stub out a few routines to
			// retrieve the generation stamp counters.
			BlockIdManager bid = Org.Mockito.Mockito.Mock<BlockIdManager>();
			long maxGenStampForLegacyBlocks = 10000;
			Org.Mockito.Mockito.When(bid.GetGenerationStampV1Limit()).ThenReturn(maxGenStampForLegacyBlocks
				);
			Block legacyBlock = Org.Mockito.Mockito.Spy(new Block());
			Org.Mockito.Mockito.When(legacyBlock.GetGenerationStamp()).ThenReturn(maxGenStampForLegacyBlocks
				 / 2);
			Block newBlock = Org.Mockito.Mockito.Spy(new Block());
			Org.Mockito.Mockito.When(newBlock.GetGenerationStamp()).ThenReturn(maxGenStampForLegacyBlocks
				 + 1);
			// Make sure that isLegacyBlock() can correctly detect
			// legacy and new blocks.
			Org.Mockito.Mockito.When(bid.IsLegacyBlock(Any<Block>())).ThenCallRealMethod();
			Assert.AssertThat(bid.IsLegacyBlock(legacyBlock), CoreMatchers.Is(true));
			Assert.AssertThat(bid.IsLegacyBlock(newBlock), CoreMatchers.Is(false));
		}

		/// <summary>
		/// Test that the generation stamp for legacy and new blocks is updated
		/// as expected.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGenerationStampUpdate()
		{
			// Setup a mock object and stub out a few routines to
			// retrieve the generation stamp counters.
			BlockIdManager bid = Org.Mockito.Mockito.Mock<BlockIdManager>();
			long nextGenerationStampV1 = 5000;
			long nextGenerationStampV2 = 20000;
			Org.Mockito.Mockito.When(bid.GetNextGenerationStampV1()).ThenReturn(nextGenerationStampV1
				);
			Org.Mockito.Mockito.When(bid.GetNextGenerationStampV2()).ThenReturn(nextGenerationStampV2
				);
			// Make sure that the generation stamp is set correctly for both
			// kinds of blocks.
			Org.Mockito.Mockito.When(bid.NextGenerationStamp(AnyBoolean())).ThenCallRealMethod
				();
			Assert.AssertThat(bid.NextGenerationStamp(true), CoreMatchers.Is(nextGenerationStampV1
				));
			Assert.AssertThat(bid.NextGenerationStamp(false), CoreMatchers.Is(nextGenerationStampV2
				));
		}

		public TestSequentialBlockId()
		{
			IoSize = BlockSize;
		}
	}
}
