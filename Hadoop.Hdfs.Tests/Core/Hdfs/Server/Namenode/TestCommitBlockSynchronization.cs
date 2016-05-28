using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Verify that TestCommitBlockSynchronization is idempotent.</summary>
	public class TestCommitBlockSynchronization
	{
		private const long blockId = 100;

		private const long length = 200;

		private const long genStamp = 300;

		/// <exception cref="System.IO.IOException"/>
		private FSNamesystem MakeNameSystemSpy(Block block, INodeFile file)
		{
			Configuration conf = new Configuration();
			FSImage image = new FSImage(conf);
			DatanodeStorageInfo[] targets = new DatanodeStorageInfo[] {  };
			FSNamesystem namesystem = new FSNamesystem(conf, image);
			namesystem.SetImageLoaded(true);
			// set file's parent as root and put the file to inodeMap, so
			// FSNamesystem's isFileDeleted() method will return false on this file
			if (file.GetParent() == null)
			{
				INodeDirectory mparent = Org.Mockito.Mockito.Mock<INodeDirectory>();
				INodeDirectory parent = new INodeDirectory(mparent.GetId(), new byte[0], mparent.
					GetPermissionStatus(), mparent.GetAccessTime());
				parent.SetLocalName(new byte[0]);
				parent.AddChild(file);
				file.SetParent(parent);
			}
			namesystem.dir.GetINodeMap().Put(file);
			FSNamesystem namesystemSpy = Org.Mockito.Mockito.Spy(namesystem);
			BlockInfoContiguousUnderConstruction blockInfo = new BlockInfoContiguousUnderConstruction
				(block, (short)1, HdfsServerConstants.BlockUCState.UnderConstruction, targets);
			blockInfo.SetBlockCollection(file);
			blockInfo.SetGenerationStamp(genStamp);
			blockInfo.InitializeBlockRecovery(genStamp);
			Org.Mockito.Mockito.DoReturn(true).When(file).RemoveLastBlock(Matchers.Any<Block>
				());
			Org.Mockito.Mockito.DoReturn(true).When(file).IsUnderConstruction();
			Org.Mockito.Mockito.DoReturn(new BlockInfoContiguous[1]).When(file).GetBlocks();
			Org.Mockito.Mockito.DoReturn(blockInfo).When(namesystemSpy).GetStoredBlock(Matchers.Any
				<Block>());
			Org.Mockito.Mockito.DoReturn(blockInfo).When(file).GetLastBlock();
			Org.Mockito.Mockito.DoReturn(string.Empty).When(namesystemSpy).CloseFileCommitBlocks
				(Matchers.Any<INodeFile>(), Matchers.Any<BlockInfoContiguous>());
			Org.Mockito.Mockito.DoReturn(Org.Mockito.Mockito.Mock<FSEditLog>()).When(namesystemSpy
				).GetEditLog();
			return namesystemSpy;
		}

		private INodeFile MockFileUnderConstruction()
		{
			INodeFile file = Org.Mockito.Mockito.Mock<INodeFile>();
			return file;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCommitBlockSynchronization()
		{
			INodeFile file = MockFileUnderConstruction();
			Block block = new Block(blockId, length, genStamp);
			FSNamesystem namesystemSpy = MakeNameSystemSpy(block, file);
			DatanodeID[] newTargets = new DatanodeID[0];
			ExtendedBlock lastBlock = new ExtendedBlock();
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, false, false
				, newTargets, null);
			// Repeat the call to make sure it does not throw
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, false, false
				, newTargets, null);
			// Simulate 'completing' the block.
			BlockInfoContiguous completedBlockInfo = new BlockInfoContiguous(block, (short)1);
			completedBlockInfo.SetBlockCollection(file);
			completedBlockInfo.SetGenerationStamp(genStamp);
			Org.Mockito.Mockito.DoReturn(completedBlockInfo).When(namesystemSpy).GetStoredBlock
				(Matchers.Any<Block>());
			Org.Mockito.Mockito.DoReturn(completedBlockInfo).When(file).GetLastBlock();
			// Repeat the call to make sure it does not throw
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, false, false
				, newTargets, null);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCommitBlockSynchronization2()
		{
			INodeFile file = MockFileUnderConstruction();
			Block block = new Block(blockId, length, genStamp);
			FSNamesystem namesystemSpy = MakeNameSystemSpy(block, file);
			DatanodeID[] newTargets = new DatanodeID[0];
			ExtendedBlock lastBlock = new ExtendedBlock();
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, false, false
				, newTargets, null);
			// Make sure the call fails if the generation stamp does not match
			// the block recovery ID.
			try
			{
				namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp - 1, length, false, 
					false, newTargets, null);
				NUnit.Framework.Assert.Fail("Failed to get expected IOException on generation stamp/"
					 + "recovery ID mismatch");
			}
			catch (IOException)
			{
			}
		}

		// Expected exception.
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCommitBlockSynchronizationWithDelete()
		{
			INodeFile file = MockFileUnderConstruction();
			Block block = new Block(blockId, length, genStamp);
			FSNamesystem namesystemSpy = MakeNameSystemSpy(block, file);
			DatanodeID[] newTargets = new DatanodeID[0];
			ExtendedBlock lastBlock = new ExtendedBlock();
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, false, true
				, newTargets, null);
			// Simulate removing the last block from the file.
			Org.Mockito.Mockito.DoReturn(false).When(file).RemoveLastBlock(Matchers.Any<Block
				>());
			// Repeat the call to make sure it does not throw
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, false, true
				, newTargets, null);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCommitBlockSynchronizationWithClose()
		{
			INodeFile file = MockFileUnderConstruction();
			Block block = new Block(blockId, length, genStamp);
			FSNamesystem namesystemSpy = MakeNameSystemSpy(block, file);
			DatanodeID[] newTargets = new DatanodeID[0];
			ExtendedBlock lastBlock = new ExtendedBlock();
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, true, false
				, newTargets, null);
			// Repeat the call to make sure it returns true
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, true, false
				, newTargets, null);
			BlockInfoContiguous completedBlockInfo = new BlockInfoContiguous(block, (short)1);
			completedBlockInfo.SetBlockCollection(file);
			completedBlockInfo.SetGenerationStamp(genStamp);
			Org.Mockito.Mockito.DoReturn(completedBlockInfo).When(namesystemSpy).GetStoredBlock
				(Matchers.Any<Block>());
			Org.Mockito.Mockito.DoReturn(completedBlockInfo).When(file).GetLastBlock();
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, true, false
				, newTargets, null);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCommitBlockSynchronizationWithCloseAndNonExistantTarget()
		{
			INodeFile file = MockFileUnderConstruction();
			Block block = new Block(blockId, length, genStamp);
			FSNamesystem namesystemSpy = MakeNameSystemSpy(block, file);
			DatanodeID[] newTargets = new DatanodeID[] { new DatanodeID("0.0.0.0", "nonexistantHost"
				, "1", 0, 0, 0, 0) };
			ExtendedBlock lastBlock = new ExtendedBlock();
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, true, false
				, newTargets, null);
			// Repeat the call to make sure it returns true
			namesystemSpy.CommitBlockSynchronization(lastBlock, genStamp, length, true, false
				, newTargets, null);
		}
	}
}
