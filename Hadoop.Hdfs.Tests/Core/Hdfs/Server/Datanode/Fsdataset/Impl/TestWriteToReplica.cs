using System;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>Test if FSDataset#append, writeToRbw, and writeToTmp</summary>
	public class TestWriteToReplica
	{
		private const int Finalized = 0;

		private const int Temporary = 1;

		private const int Rbw = 2;

		private const int Rwr = 3;

		private const int Rur = 4;

		private const int NonExistent = 5;

		// test close
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClose()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).Build
				();
			try
			{
				cluster.WaitActive();
				DataNode dn = cluster.GetDataNodes()[0];
				FsDatasetImpl dataSet = (FsDatasetImpl)DataNodeTestUtils.GetFSDataset(dn);
				// set up replicasMap
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				ExtendedBlock[] blocks = Setup(bpid, dataSet);
				// test close
				TestClose(dataSet, blocks);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		// test append
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppend()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).Build
				();
			try
			{
				cluster.WaitActive();
				DataNode dn = cluster.GetDataNodes()[0];
				FsDatasetImpl dataSet = (FsDatasetImpl)DataNodeTestUtils.GetFSDataset(dn);
				// set up replicasMap
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				ExtendedBlock[] blocks = Setup(bpid, dataSet);
				// test append
				TestAppend(bpid, dataSet, blocks);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		// test writeToRbw
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteToRbw()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).Build
				();
			try
			{
				cluster.WaitActive();
				DataNode dn = cluster.GetDataNodes()[0];
				FsDatasetImpl dataSet = (FsDatasetImpl)DataNodeTestUtils.GetFSDataset(dn);
				// set up replicasMap
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				ExtendedBlock[] blocks = Setup(bpid, dataSet);
				// test writeToRbw
				TestWriteToRbw(dataSet, blocks);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		// test writeToTemporary
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteToTemporary()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).Build
				();
			try
			{
				cluster.WaitActive();
				DataNode dn = cluster.GetDataNodes()[0];
				FsDatasetImpl dataSet = (FsDatasetImpl)DataNodeTestUtils.GetFSDataset(dn);
				// set up replicasMap
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				ExtendedBlock[] blocks = Setup(bpid, dataSet);
				// test writeToTemporary
				TestWriteToTemporary(dataSet, blocks);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Generate testing environment and return a collection of blocks
		/// on which to run the tests.
		/// </summary>
		/// <param name="bpid">Block pool ID to generate blocks for</param>
		/// <param name="dataSet">Namespace in which to insert blocks</param>
		/// <returns>Contrived blocks for further testing.</returns>
		/// <exception cref="System.IO.IOException"/>
		private ExtendedBlock[] Setup(string bpid, FsDatasetImpl dataSet)
		{
			// setup replicas map
			ExtendedBlock[] blocks = new ExtendedBlock[] { new ExtendedBlock(bpid, 1, 1, 2001
				), new ExtendedBlock(bpid, 2, 1, 2002), new ExtendedBlock(bpid, 3, 1, 2003), new 
				ExtendedBlock(bpid, 4, 1, 2004), new ExtendedBlock(bpid, 5, 1, 2005), new ExtendedBlock
				(bpid, 6, 1, 2006) };
			ReplicaMap replicasMap = dataSet.volumeMap;
			FsVolumeImpl vol = (FsVolumeImpl)dataSet.volumes.GetNextVolume(StorageType.Default
				, 0).GetVolume();
			ReplicaInfo replicaInfo = new FinalizedReplica(blocks[Finalized].GetLocalBlock(), 
				vol, vol.GetCurrentDir().GetParentFile());
			replicasMap.Add(bpid, replicaInfo);
			replicaInfo.GetBlockFile().CreateNewFile();
			replicaInfo.GetMetaFile().CreateNewFile();
			replicasMap.Add(bpid, new ReplicaInPipeline(blocks[Temporary].GetBlockId(), blocks
				[Temporary].GetGenerationStamp(), vol, vol.CreateTmpFile(bpid, blocks[Temporary]
				.GetLocalBlock()).GetParentFile(), 0));
			replicaInfo = new ReplicaBeingWritten(blocks[Rbw].GetLocalBlock(), vol, vol.CreateRbwFile
				(bpid, blocks[Rbw].GetLocalBlock()).GetParentFile(), null);
			replicasMap.Add(bpid, replicaInfo);
			replicaInfo.GetBlockFile().CreateNewFile();
			replicaInfo.GetMetaFile().CreateNewFile();
			replicasMap.Add(bpid, new ReplicaWaitingToBeRecovered(blocks[Rwr].GetLocalBlock()
				, vol, vol.CreateRbwFile(bpid, blocks[Rwr].GetLocalBlock()).GetParentFile()));
			replicasMap.Add(bpid, new ReplicaUnderRecovery(new FinalizedReplica(blocks[Rur].GetLocalBlock
				(), vol, vol.GetCurrentDir().GetParentFile()), 2007));
			return blocks;
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestAppend(string bpid, FsDatasetImpl dataSet, ExtendedBlock[] blocks
			)
		{
			long newGS = blocks[Finalized].GetGenerationStamp() + 1;
			FsVolumeImpl v = (FsVolumeImpl)dataSet.volumeMap.Get(bpid, blocks[Finalized].GetLocalBlock
				()).GetVolume();
			long available = v.GetCapacity() - v.GetDfsUsed();
			long expectedLen = blocks[Finalized].GetNumBytes();
			try
			{
				v.DecDfsUsed(bpid, -available);
				blocks[Finalized].SetNumBytes(expectedLen + 100);
				dataSet.Append(blocks[Finalized], newGS, expectedLen);
				NUnit.Framework.Assert.Fail("Should not have space to append to an RWR replica" +
					 blocks[Rwr]);
			}
			catch (DiskChecker.DiskOutOfSpaceException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("Insufficient space for appending to "
					));
			}
			v.DecDfsUsed(bpid, available);
			blocks[Finalized].SetNumBytes(expectedLen);
			newGS = blocks[Rbw].GetGenerationStamp() + 1;
			dataSet.Append(blocks[Finalized], newGS, blocks[Finalized].GetNumBytes());
			// successful
			blocks[Finalized].SetGenerationStamp(newGS);
			try
			{
				dataSet.Append(blocks[Temporary], blocks[Temporary].GetGenerationStamp() + 1, blocks
					[Temporary].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have appended to a temporary replica " + 
					blocks[Temporary]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.AreEqual(ReplicaNotFoundException.UnfinalizedReplica + blocks
					[Temporary], e.Message);
			}
			try
			{
				dataSet.Append(blocks[Rbw], blocks[Rbw].GetGenerationStamp() + 1, blocks[Rbw].GetNumBytes
					());
				NUnit.Framework.Assert.Fail("Should not have appended to an RBW replica" + blocks
					[Rbw]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.AreEqual(ReplicaNotFoundException.UnfinalizedReplica + blocks
					[Rbw], e.Message);
			}
			try
			{
				dataSet.Append(blocks[Rwr], blocks[Rwr].GetGenerationStamp() + 1, blocks[Rbw].GetNumBytes
					());
				NUnit.Framework.Assert.Fail("Should not have appended to an RWR replica" + blocks
					[Rwr]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.AreEqual(ReplicaNotFoundException.UnfinalizedReplica + blocks
					[Rwr], e.Message);
			}
			try
			{
				dataSet.Append(blocks[Rur], blocks[Rur].GetGenerationStamp() + 1, blocks[Rur].GetNumBytes
					());
				NUnit.Framework.Assert.Fail("Should not have appended to an RUR replica" + blocks
					[Rur]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.AreEqual(ReplicaNotFoundException.UnfinalizedReplica + blocks
					[Rur], e.Message);
			}
			try
			{
				dataSet.Append(blocks[NonExistent], blocks[NonExistent].GetGenerationStamp(), blocks
					[NonExistent].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have appended to a non-existent replica "
					 + blocks[NonExistent]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.AreEqual(ReplicaNotFoundException.NonExistentReplica + blocks
					[NonExistent], e.Message);
			}
			newGS = blocks[Finalized].GetGenerationStamp() + 1;
			dataSet.RecoverAppend(blocks[Finalized], newGS, blocks[Finalized].GetNumBytes());
			// successful
			blocks[Finalized].SetGenerationStamp(newGS);
			try
			{
				dataSet.RecoverAppend(blocks[Temporary], blocks[Temporary].GetGenerationStamp() +
					 1, blocks[Temporary].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have appended to a temporary replica " + 
					blocks[Temporary]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.UnfinalizedAndNonrbwReplica
					));
			}
			newGS = blocks[Rbw].GetGenerationStamp() + 1;
			dataSet.RecoverAppend(blocks[Rbw], newGS, blocks[Rbw].GetNumBytes());
			blocks[Rbw].SetGenerationStamp(newGS);
			try
			{
				dataSet.RecoverAppend(blocks[Rwr], blocks[Rwr].GetGenerationStamp() + 1, blocks[Rbw
					].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have appended to an RWR replica" + blocks
					[Rwr]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.UnfinalizedAndNonrbwReplica
					));
			}
			try
			{
				dataSet.RecoverAppend(blocks[Rur], blocks[Rur].GetGenerationStamp() + 1, blocks[Rur
					].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have appended to an RUR replica" + blocks
					[Rur]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.UnfinalizedAndNonrbwReplica
					));
			}
			try
			{
				dataSet.RecoverAppend(blocks[NonExistent], blocks[NonExistent].GetGenerationStamp
					(), blocks[NonExistent].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have appended to a non-existent replica "
					 + blocks[NonExistent]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.NonExistentReplica
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestClose(FsDatasetImpl dataSet, ExtendedBlock[] blocks)
		{
			long newGS = blocks[Finalized].GetGenerationStamp() + 1;
			dataSet.RecoverClose(blocks[Finalized], newGS, blocks[Finalized].GetNumBytes());
			// successful
			blocks[Finalized].SetGenerationStamp(newGS);
			try
			{
				dataSet.RecoverClose(blocks[Temporary], blocks[Temporary].GetGenerationStamp() + 
					1, blocks[Temporary].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have recovered close a temporary replica "
					 + blocks[Temporary]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.UnfinalizedAndNonrbwReplica
					));
			}
			newGS = blocks[Rbw].GetGenerationStamp() + 1;
			dataSet.RecoverClose(blocks[Rbw], newGS, blocks[Rbw].GetNumBytes());
			blocks[Rbw].SetGenerationStamp(newGS);
			try
			{
				dataSet.RecoverClose(blocks[Rwr], blocks[Rwr].GetGenerationStamp() + 1, blocks[Rbw
					].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have recovered close an RWR replica" + blocks
					[Rwr]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.UnfinalizedAndNonrbwReplica
					));
			}
			try
			{
				dataSet.RecoverClose(blocks[Rur], blocks[Rur].GetGenerationStamp() + 1, blocks[Rur
					].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have recovered close an RUR replica" + blocks
					[Rur]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.UnfinalizedAndNonrbwReplica
					));
			}
			try
			{
				dataSet.RecoverClose(blocks[NonExistent], blocks[NonExistent].GetGenerationStamp(
					), blocks[NonExistent].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have recovered close a non-existent replica "
					 + blocks[NonExistent]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.NonExistentReplica
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestWriteToRbw(FsDatasetImpl dataSet, ExtendedBlock[] blocks)
		{
			try
			{
				dataSet.RecoverRbw(blocks[Finalized], blocks[Finalized].GetGenerationStamp() + 1, 
					0L, blocks[Finalized].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have recovered a finalized replica " + blocks
					[Finalized]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.NonRbwReplica
					));
			}
			try
			{
				dataSet.CreateRbw(StorageType.Default, blocks[Finalized], false);
				NUnit.Framework.Assert.Fail("Should not have created a replica that's already " +
					 "finalized " + blocks[Finalized]);
			}
			catch (ReplicaAlreadyExistsException)
			{
			}
			try
			{
				dataSet.RecoverRbw(blocks[Temporary], blocks[Temporary].GetGenerationStamp() + 1, 
					0L, blocks[Temporary].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have recovered a temporary replica " + blocks
					[Temporary]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.NonRbwReplica
					));
			}
			try
			{
				dataSet.CreateRbw(StorageType.Default, blocks[Temporary], false);
				NUnit.Framework.Assert.Fail("Should not have created a replica that had created as "
					 + "temporary " + blocks[Temporary]);
			}
			catch (ReplicaAlreadyExistsException)
			{
			}
			dataSet.RecoverRbw(blocks[Rbw], blocks[Rbw].GetGenerationStamp() + 1, 0L, blocks[
				Rbw].GetNumBytes());
			// expect to be successful
			try
			{
				dataSet.CreateRbw(StorageType.Default, blocks[Rbw], false);
				NUnit.Framework.Assert.Fail("Should not have created a replica that had created as RBW "
					 + blocks[Rbw]);
			}
			catch (ReplicaAlreadyExistsException)
			{
			}
			try
			{
				dataSet.RecoverRbw(blocks[Rwr], blocks[Rwr].GetGenerationStamp() + 1, 0L, blocks[
					Rwr].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have recovered a RWR replica " + blocks[Rwr
					]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.NonRbwReplica
					));
			}
			try
			{
				dataSet.CreateRbw(StorageType.Default, blocks[Rwr], false);
				NUnit.Framework.Assert.Fail("Should not have created a replica that was waiting to be "
					 + "recovered " + blocks[Rwr]);
			}
			catch (ReplicaAlreadyExistsException)
			{
			}
			try
			{
				dataSet.RecoverRbw(blocks[Rur], blocks[Rur].GetGenerationStamp() + 1, 0L, blocks[
					Rur].GetNumBytes());
				NUnit.Framework.Assert.Fail("Should not have recovered a RUR replica " + blocks[Rur
					]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith(ReplicaNotFoundException.NonRbwReplica
					));
			}
			try
			{
				dataSet.CreateRbw(StorageType.Default, blocks[Rur], false);
				NUnit.Framework.Assert.Fail("Should not have created a replica that was under recovery "
					 + blocks[Rur]);
			}
			catch (ReplicaAlreadyExistsException)
			{
			}
			try
			{
				dataSet.RecoverRbw(blocks[NonExistent], blocks[NonExistent].GetGenerationStamp() 
					+ 1, 0L, blocks[NonExistent].GetNumBytes());
				NUnit.Framework.Assert.Fail("Cannot recover a non-existent replica " + blocks[NonExistent
					]);
			}
			catch (ReplicaNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(ReplicaNotFoundException.NonExistentReplica
					));
			}
			dataSet.CreateRbw(StorageType.Default, blocks[NonExistent], false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestWriteToTemporary(FsDatasetImpl dataSet, ExtendedBlock[] blocks)
		{
			try
			{
				dataSet.CreateTemporary(StorageType.Default, blocks[Finalized]);
				NUnit.Framework.Assert.Fail("Should not have created a temporary replica that was "
					 + "finalized " + blocks[Finalized]);
			}
			catch (ReplicaAlreadyExistsException)
			{
			}
			try
			{
				dataSet.CreateTemporary(StorageType.Default, blocks[Temporary]);
				NUnit.Framework.Assert.Fail("Should not have created a replica that had created as"
					 + "temporary " + blocks[Temporary]);
			}
			catch (ReplicaAlreadyExistsException)
			{
			}
			try
			{
				dataSet.CreateTemporary(StorageType.Default, blocks[Rbw]);
				NUnit.Framework.Assert.Fail("Should not have created a replica that had created as RBW "
					 + blocks[Rbw]);
			}
			catch (ReplicaAlreadyExistsException)
			{
			}
			try
			{
				dataSet.CreateTemporary(StorageType.Default, blocks[Rwr]);
				NUnit.Framework.Assert.Fail("Should not have created a replica that was waiting to be "
					 + "recovered " + blocks[Rwr]);
			}
			catch (ReplicaAlreadyExistsException)
			{
			}
			try
			{
				dataSet.CreateTemporary(StorageType.Default, blocks[Rur]);
				NUnit.Framework.Assert.Fail("Should not have created a replica that was under recovery "
					 + blocks[Rur]);
			}
			catch (ReplicaAlreadyExistsException)
			{
			}
			dataSet.CreateTemporary(StorageType.Default, blocks[NonExistent]);
			try
			{
				dataSet.CreateTemporary(StorageType.Default, blocks[NonExistent]);
				NUnit.Framework.Assert.Fail("Should not have created a replica that had already been "
					 + "created " + blocks[NonExistent]);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(blocks[NonExistent].GetBlockName
					()));
				NUnit.Framework.Assert.IsTrue(e is ReplicaAlreadyExistsException);
			}
			long newGenStamp = blocks[NonExistent].GetGenerationStamp() * 10;
			blocks[NonExistent].SetGenerationStamp(newGenStamp);
			try
			{
				ReplicaInPipelineInterface replicaInfo = dataSet.CreateTemporary(StorageType.Default
					, blocks[NonExistent]).GetReplica();
				NUnit.Framework.Assert.IsTrue(replicaInfo.GetGenerationStamp() == newGenStamp);
				NUnit.Framework.Assert.IsTrue(replicaInfo.GetBlockId() == blocks[NonExistent].GetBlockId
					());
			}
			catch (ReplicaAlreadyExistsException)
			{
				NUnit.Framework.Assert.Fail("createRbw() Should have removed the block with the older "
					 + "genstamp and replaced it with the newer one: " + blocks[NonExistent]);
			}
		}
	}
}
