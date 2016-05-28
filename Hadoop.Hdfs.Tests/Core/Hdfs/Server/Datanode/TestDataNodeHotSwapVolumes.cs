using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Hamcrest;
using Org.Hamcrest.Core;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestDataNodeHotSwapVolumes
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDataNodeHotSwapVolumes
			));

		private const int BlockSize = 512;

		private MiniDFSCluster cluster;

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		private void StartDFSCluster(int numNameNodes, int numDataNodes)
		{
			Shutdown();
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			/*
			* Lower the DN heartbeat, DF rate, and recheck interval to one second
			* so state about failures and datanode death propagates faster.
			*/
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsDfIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			/* Allow 1 volume failure */
			conf.SetInt(DFSConfigKeys.DfsDatanodeFailedVolumesToleratedKey, 1);
			MiniDFSNNTopology nnTopology = MiniDFSNNTopology.SimpleFederatedTopology(numNameNodes
				);
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(nnTopology).NumDataNodes(numDataNodes
				).Build();
			cluster.WaitActive();
		}

		private void Shutdown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private void CreateFile(Path path, int numBlocks)
		{
			short replicateFactor = 1;
			CreateFile(path, numBlocks, replicateFactor);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private void CreateFile(Path path, int numBlocks, short replicateFactor)
		{
			CreateFile(0, path, numBlocks, replicateFactor);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private void CreateFile(int fsIdx, Path path, int numBlocks)
		{
			short replicateFactor = 1;
			CreateFile(fsIdx, path, numBlocks, replicateFactor);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		private void CreateFile(int fsIdx, Path path, int numBlocks, short replicateFactor
			)
		{
			int seed = 0;
			DistributedFileSystem fs = cluster.GetFileSystem(fsIdx);
			DFSTestUtil.CreateFile(fs, path, BlockSize * numBlocks, replicateFactor, seed);
			DFSTestUtil.WaitReplication(fs, path, replicateFactor);
		}

		/// <summary>Verify whether a file has enough content.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static void VerifyFileLength(FileSystem fs, Path path, int numBlocks)
		{
			FileStatus status = fs.GetFileStatus(path);
			NUnit.Framework.Assert.AreEqual(numBlocks * BlockSize, status.GetLen());
		}

		/// <summary>Return the number of replicas for a given block in the file.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static int GetNumReplicas(FileSystem fs, Path file, int blockIdx)
		{
			BlockLocation[] locs = fs.GetFileBlockLocations(file, 0, long.MaxValue);
			return locs.Length < blockIdx + 1 ? 0 : locs[blockIdx].GetNames().Length;
		}

		/// <summary>Wait the block to have the exact number of replicas as expected.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		private static void WaitReplication(FileSystem fs, Path file, int blockIdx, int numReplicas
			)
		{
			int attempts = 50;
			// Wait 5 seconds.
			while (attempts > 0)
			{
				int actualReplicas = GetNumReplicas(fs, file, blockIdx);
				if (actualReplicas == numReplicas)
				{
					return;
				}
				System.Console.Out.Printf("Block %d of file %s has %d replicas (desired %d).\n", 
					blockIdx, file.ToString(), actualReplicas, numReplicas);
				Sharpen.Thread.Sleep(100);
				attempts--;
			}
			throw new TimeoutException("Timed out waiting the " + blockIdx + "-th block" + " of "
				 + file + " to have " + numReplicas + " replicas.");
		}

		/// <summary>Parses data dirs from DataNode's configuration.</summary>
		private static IList<string> GetDataDirs(DataNode datanode)
		{
			return new AList<string>(datanode.GetConf().GetTrimmedStringCollection(DFSConfigKeys
				.DfsDatanodeDataDirKey));
		}

		/// <summary>Force the DataNode to report missing blocks immediately.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static void TriggerDeleteReport(DataNode datanode)
		{
			datanode.ScheduleAllBlockReport(0);
			DataNodeTestUtils.TriggerDeletionReport(datanode);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestParseChangedVolumes()
		{
			StartDFSCluster(1, 1);
			DataNode dn = cluster.GetDataNodes()[0];
			Configuration conf = dn.GetConf();
			string oldPaths = conf.Get(DFSConfigKeys.DfsDatanodeDataDirKey);
			IList<StorageLocation> oldLocations = new AList<StorageLocation>();
			foreach (string path in oldPaths.Split(","))
			{
				oldLocations.AddItem(StorageLocation.Parse(path));
			}
			NUnit.Framework.Assert.IsFalse(oldLocations.IsEmpty());
			string newPaths = oldLocations[0].GetFile().GetAbsolutePath() + ",/foo/path1,/foo/path2";
			DataNode.ChangedVolumes changedVolumes = dn.ParseChangedVolumes(newPaths);
			IList<StorageLocation> newVolumes = changedVolumes.newLocations;
			NUnit.Framework.Assert.AreEqual(2, newVolumes.Count);
			NUnit.Framework.Assert.AreEqual(new FilePath("/foo/path1").GetAbsolutePath(), newVolumes
				[0].GetFile().GetAbsolutePath());
			NUnit.Framework.Assert.AreEqual(new FilePath("/foo/path2").GetAbsolutePath(), newVolumes
				[1].GetFile().GetAbsolutePath());
			IList<StorageLocation> removedVolumes = changedVolumes.deactivateLocations;
			NUnit.Framework.Assert.AreEqual(1, removedVolumes.Count);
			NUnit.Framework.Assert.AreEqual(oldLocations[1].GetFile(), removedVolumes[0].GetFile
				());
			NUnit.Framework.Assert.AreEqual(1, changedVolumes.unchangedLocations.Count);
			NUnit.Framework.Assert.AreEqual(oldLocations[0].GetFile(), changedVolumes.unchangedLocations
				[0].GetFile());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestParseChangedVolumesFailures()
		{
			StartDFSCluster(1, 1);
			DataNode dn = cluster.GetDataNodes()[0];
			try
			{
				dn.ParseChangedVolumes(string.Empty);
				NUnit.Framework.Assert.Fail("Should throw IOException: empty inputs.");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("No directory is specified.", e);
			}
		}

		/// <summary>Add volumes to the first DataNode.</summary>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		/// <exception cref="System.IO.IOException"/>
		private void AddVolumes(int numNewVolumes)
		{
			FilePath dataDir = new FilePath(cluster.GetDataDirectory());
			DataNode dn = cluster.GetDataNodes()[0];
			// First DataNode.
			Configuration conf = dn.GetConf();
			string oldDataDir = conf.Get(DFSConfigKeys.DfsDatanodeDataDirKey);
			IList<FilePath> newVolumeDirs = new AList<FilePath>();
			StringBuilder newDataDirBuf = new StringBuilder(oldDataDir);
			int startIdx = oldDataDir.Split(",").Length + 1;
			// Find the first available (non-taken) directory name for data volume.
			while (true)
			{
				FilePath volumeDir = new FilePath(dataDir, "data" + startIdx);
				if (!volumeDir.Exists())
				{
					break;
				}
				startIdx++;
			}
			for (int i = startIdx; i < startIdx + numNewVolumes; i++)
			{
				FilePath volumeDir = new FilePath(dataDir, "data" + i.ToString());
				newVolumeDirs.AddItem(volumeDir);
				volumeDir.Mkdirs();
				newDataDirBuf.Append(",");
				newDataDirBuf.Append(StorageLocation.Parse(volumeDir.ToString()).ToString());
			}
			string newDataDir = newDataDirBuf.ToString();
			dn.ReconfigurePropertyImpl(DFSConfigKeys.DfsDatanodeDataDirKey, newDataDir);
			// Verify the configuration value is appropriately set.
			string[] effectiveDataDirs = conf.Get(DFSConfigKeys.DfsDatanodeDataDirKey).Split(
				",");
			string[] expectDataDirs = newDataDir.Split(",");
			NUnit.Framework.Assert.AreEqual(expectDataDirs.Length, effectiveDataDirs.Length);
			for (int i_1 = 0; i_1 < expectDataDirs.Length; i_1++)
			{
				StorageLocation expectLocation = StorageLocation.Parse(expectDataDirs[i_1]);
				StorageLocation effectiveLocation = StorageLocation.Parse(effectiveDataDirs[i_1]);
				NUnit.Framework.Assert.AreEqual(expectLocation.GetStorageType(), effectiveLocation
					.GetStorageType());
				NUnit.Framework.Assert.AreEqual(expectLocation.GetFile().GetCanonicalFile(), effectiveLocation
					.GetFile().GetCanonicalFile());
			}
			// Check that all newly created volumes are appropriately formatted.
			foreach (FilePath volumeDir_1 in newVolumeDirs)
			{
				FilePath curDir = new FilePath(volumeDir_1, "current");
				NUnit.Framework.Assert.IsTrue(curDir.Exists());
				NUnit.Framework.Assert.IsTrue(curDir.IsDirectory());
			}
		}

		private IList<IList<int>> GetNumBlocksReport(int namesystemIdx)
		{
			IList<IList<int>> results = new AList<IList<int>>();
			string bpid = cluster.GetNamesystem(namesystemIdx).GetBlockPoolId();
			IList<IDictionary<DatanodeStorage, BlockListAsLongs>> blockReports = cluster.GetAllBlockReports
				(bpid);
			foreach (IDictionary<DatanodeStorage, BlockListAsLongs> datanodeReport in blockReports)
			{
				IList<int> numBlocksPerDN = new AList<int>();
				foreach (BlockListAsLongs blocks in datanodeReport.Values)
				{
					numBlocksPerDN.AddItem(blocks.GetNumberOfBlocks());
				}
				results.AddItem(numBlocksPerDN);
			}
			return results;
		}

		/// <summary>Test adding one volume on a running MiniDFSCluster with only one NameNode.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		public virtual void TestAddOneNewVolume()
		{
			StartDFSCluster(1, 1);
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			int numBlocks = 10;
			AddVolumes(1);
			Path testFile = new Path("/test");
			CreateFile(testFile, numBlocks);
			IList<IDictionary<DatanodeStorage, BlockListAsLongs>> blockReports = cluster.GetAllBlockReports
				(bpid);
			NUnit.Framework.Assert.AreEqual(1, blockReports.Count);
			// 1 DataNode
			NUnit.Framework.Assert.AreEqual(3, blockReports[0].Count);
			// 3 volumes
			// FSVolumeList uses Round-Robin block chooser by default. Thus the new
			// blocks should be evenly located in all volumes.
			int minNumBlocks = int.MaxValue;
			int maxNumBlocks = int.MinValue;
			foreach (BlockListAsLongs blockList in blockReports[0].Values)
			{
				minNumBlocks = Math.Min(minNumBlocks, blockList.GetNumberOfBlocks());
				maxNumBlocks = Math.Max(maxNumBlocks, blockList.GetNumberOfBlocks());
			}
			NUnit.Framework.Assert.IsTrue(Math.Abs(maxNumBlocks - maxNumBlocks) <= 1);
			VerifyFileLength(cluster.GetFileSystem(), testFile, numBlocks);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		public virtual void TestAddVolumesDuringWrite()
		{
			StartDFSCluster(1, 1);
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			Path testFile = new Path("/test");
			CreateFile(testFile, 4);
			// Each volume has 2 blocks.
			AddVolumes(2);
			// Continue to write the same file, thus the new volumes will have blocks.
			DFSTestUtil.AppendFile(cluster.GetFileSystem(), testFile, BlockSize * 8);
			VerifyFileLength(cluster.GetFileSystem(), testFile, 8 + 4);
			// After appending data, there should be [2, 2, 4, 4] blocks in each volume
			// respectively.
			IList<int> expectedNumBlocks = Arrays.AsList(2, 2, 4, 4);
			IList<IDictionary<DatanodeStorage, BlockListAsLongs>> blockReports = cluster.GetAllBlockReports
				(bpid);
			NUnit.Framework.Assert.AreEqual(1, blockReports.Count);
			// 1 DataNode
			NUnit.Framework.Assert.AreEqual(4, blockReports[0].Count);
			// 4 volumes
			IDictionary<DatanodeStorage, BlockListAsLongs> dnReport = blockReports[0];
			IList<int> actualNumBlocks = new AList<int>();
			foreach (BlockListAsLongs blockList in dnReport.Values)
			{
				actualNumBlocks.AddItem(blockList.GetNumberOfBlocks());
			}
			actualNumBlocks.Sort();
			NUnit.Framework.Assert.AreEqual(expectedNumBlocks, actualNumBlocks);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		public virtual void TestAddVolumesToFederationNN()
		{
			// Starts a Cluster with 2 NameNode and 3 DataNodes. Each DataNode has 2
			// volumes.
			int numNameNodes = 2;
			int numDataNodes = 1;
			StartDFSCluster(numNameNodes, numDataNodes);
			Path testFile = new Path("/test");
			// Create a file on the first namespace with 4 blocks.
			CreateFile(0, testFile, 4);
			// Create a file on the second namespace with 4 blocks.
			CreateFile(1, testFile, 4);
			// Add 2 volumes to the first DataNode.
			int numNewVolumes = 2;
			AddVolumes(numNewVolumes);
			// Append to the file on the first namespace.
			DFSTestUtil.AppendFile(cluster.GetFileSystem(0), testFile, BlockSize * 8);
			IList<IList<int>> actualNumBlocks = GetNumBlocksReport(0);
			NUnit.Framework.Assert.AreEqual(cluster.GetDataNodes().Count, actualNumBlocks.Count
				);
			IList<int> blocksOnFirstDN = actualNumBlocks[0];
			blocksOnFirstDN.Sort();
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(2, 2, 4, 4), blocksOnFirstDN);
			// Verify the second namespace also has the new volumes and they are empty.
			actualNumBlocks = GetNumBlocksReport(1);
			NUnit.Framework.Assert.AreEqual(4, actualNumBlocks[0].Count);
			NUnit.Framework.Assert.AreEqual(numNewVolumes, Sharpen.Collections.Frequency(actualNumBlocks
				[0], 0));
		}

		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRemoveOneVolume()
		{
			StartDFSCluster(1, 1);
			short replFactor = 1;
			Path testFile = new Path("/test");
			CreateFile(testFile, 10, replFactor);
			DataNode dn = cluster.GetDataNodes()[0];
			ICollection<string> oldDirs = GetDataDirs(dn);
			string newDirs = oldDirs.GetEnumerator().Next();
			// Keep the first volume.
			dn.ReconfigurePropertyImpl(DFSConfigKeys.DfsDatanodeDataDirKey, newDirs);
			AssertFileLocksReleased(new AList<string>(oldDirs).SubList(1, oldDirs.Count));
			dn.ScheduleAllBlockReport(0);
			try
			{
				DFSTestUtil.ReadFile(cluster.GetFileSystem(), testFile);
				NUnit.Framework.Assert.Fail("Expect to throw BlockMissingException.");
			}
			catch (BlockMissingException e)
			{
				GenericTestUtils.AssertExceptionContains("Could not obtain block", e);
			}
			Path newFile = new Path("/newFile");
			CreateFile(newFile, 6);
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			IList<IDictionary<DatanodeStorage, BlockListAsLongs>> blockReports = cluster.GetAllBlockReports
				(bpid);
			NUnit.Framework.Assert.AreEqual((int)replFactor, blockReports.Count);
			BlockListAsLongs blocksForVolume1 = blockReports[0].Values.GetEnumerator().Next();
			// The first volume has half of the testFile and full of newFile.
			NUnit.Framework.Assert.AreEqual(10 / 2 + 6, blocksForVolume1.GetNumberOfBlocks());
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		public virtual void TestReplicatingAfterRemoveVolume()
		{
			StartDFSCluster(1, 2);
			FileSystem fs = cluster.GetFileSystem();
			short replFactor = 2;
			Path testFile = new Path("/test");
			CreateFile(testFile, 4, replFactor);
			DataNode dn = cluster.GetDataNodes()[0];
			ICollection<string> oldDirs = GetDataDirs(dn);
			string newDirs = oldDirs.GetEnumerator().Next();
			// Keep the first volume.
			dn.ReconfigurePropertyImpl(DFSConfigKeys.DfsDatanodeDataDirKey, newDirs);
			AssertFileLocksReleased(new AList<string>(oldDirs).SubList(1, oldDirs.Count));
			TriggerDeleteReport(dn);
			WaitReplication(fs, testFile, 1, 1);
			DFSTestUtil.WaitReplication(fs, testFile, replFactor);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAddVolumeFailures()
		{
			StartDFSCluster(1, 1);
			string dataDir = cluster.GetDataDirectory();
			DataNode dn = cluster.GetDataNodes()[0];
			IList<string> newDirs = Lists.NewArrayList();
			int NumNewDirs = 4;
			for (int i = 0; i < NumNewDirs; i++)
			{
				FilePath newVolume = new FilePath(dataDir, "new_vol" + i);
				newDirs.AddItem(newVolume.ToString());
				if (i % 2 == 0)
				{
					// Make addVolume() fail.
					newVolume.CreateNewFile();
				}
			}
			string newValue = dn.GetConf().Get(DFSConfigKeys.DfsDatanodeDataDirKey) + "," + Joiner
				.On(",").Join(newDirs);
			try
			{
				dn.ReconfigurePropertyImpl(DFSConfigKeys.DfsDatanodeDataDirKey, newValue);
				NUnit.Framework.Assert.Fail("Expect to throw IOException.");
			}
			catch (ReconfigurationException e)
			{
				string errorMessage = e.InnerException.Message;
				string[] messages = errorMessage.Split("\\r?\\n");
				NUnit.Framework.Assert.AreEqual(2, messages.Length);
				Assert.AssertThat(messages[0], CoreMatchers.ContainsString("new_vol0"));
				Assert.AssertThat(messages[1], CoreMatchers.ContainsString("new_vol2"));
			}
			// Make sure that vol0 and vol2's metadata are not left in memory.
			FsDatasetSpi<object> dataset = dn.GetFSDataset();
			foreach (FsVolumeSpi volume in dataset.GetVolumes())
			{
				Assert.AssertThat(volume.GetBasePath(), IS.Is(CoreMatchers.Not(CoreMatchers.AnyOf
					(IS.Is(newDirs[0]), IS.Is(newDirs[2])))));
			}
			DataStorage storage = dn.GetStorage();
			for (int i_1 = 0; i_1 < storage.GetNumStorageDirs(); i_1++)
			{
				Storage.StorageDirectory sd = storage.GetStorageDir(i_1);
				Assert.AssertThat(sd.GetRoot().ToString(), IS.Is(CoreMatchers.Not(CoreMatchers.AnyOf
					(IS.Is(newDirs[0]), IS.Is(newDirs[2])))));
			}
			// The newly effective conf does not have vol0 and vol2.
			string[] effectiveVolumes = dn.GetConf().Get(DFSConfigKeys.DfsDatanodeDataDirKey)
				.Split(",");
			NUnit.Framework.Assert.AreEqual(4, effectiveVolumes.Length);
			foreach (string ev in effectiveVolumes)
			{
				Assert.AssertThat(StorageLocation.Parse(ev).GetFile().GetCanonicalPath(), IS.Is(CoreMatchers.Not
					(CoreMatchers.AnyOf(IS.Is(newDirs[0]), IS.Is(newDirs[2])))));
			}
		}

		/// <summary>
		/// Asserts that the storage lock file in each given directory has been
		/// released.
		/// </summary>
		/// <remarks>
		/// Asserts that the storage lock file in each given directory has been
		/// released.  This method works by trying to acquire the lock file itself.  If
		/// locking fails here, then the main code must have failed to release it.
		/// </remarks>
		/// <param name="dirs">every storage directory to check</param>
		/// <exception cref="System.IO.IOException">if there is an unexpected I/O error</exception>
		private static void AssertFileLocksReleased(ICollection<string> dirs)
		{
			foreach (string dir in dirs)
			{
				try
				{
					FsDatasetTestUtil.AssertFileLockReleased(dir);
				}
				catch (IOException e)
				{
					Log.Warn(e);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.BrokenBarrierException"/>
		public virtual void TestRemoveVolumeBeingWritten()
		{
			// test against removing volumes on the different DataNode on the pipeline.
			for (int i = 0; i < 3; i++)
			{
				TestRemoveVolumeBeingWrittenForDatanode(i);
			}
		}

		/// <summary>
		/// Test the case that remove a data volume on a particular DataNode when the
		/// volume is actively being written.
		/// </summary>
		/// <param name="dataNodeIdx">the index of the DataNode to remove a volume.</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.BrokenBarrierException"/>
		private void TestRemoveVolumeBeingWrittenForDatanode(int dataNodeIdx)
		{
			// Starts DFS cluster with 3 DataNodes to form a pipeline.
			StartDFSCluster(1, 3);
			short Replication = 3;
			DataNode dn = cluster.GetDataNodes()[dataNodeIdx];
			FileSystem fs = cluster.GetFileSystem();
			Path testFile = new Path("/test");
			long lastTimeDiskErrorCheck = dn.GetLastDiskErrorCheck();
			FSDataOutputStream @out = fs.Create(testFile, Replication);
			Random rb = new Random(0);
			byte[] writeBuf = new byte[BlockSize / 2];
			// half of the block.
			rb.NextBytes(writeBuf);
			@out.Write(writeBuf);
			@out.Hflush();
			// Make FsDatasetSpi#finalizeBlock a time-consuming operation. So if the
			// BlockReceiver releases volume reference before finalizeBlock(), the blocks
			// on the volume will be removed, and finalizeBlock() throws IOE.
			FsDatasetSpi<FsVolumeSpi> data = dn.data;
			dn.data = Org.Mockito.Mockito.Spy(data);
			Org.Mockito.Mockito.DoAnswer(new _Answer_599(data)).When(dn.data).FinalizeBlock(Matchers.Any
				<ExtendedBlock>());
			// Bypass the argument to FsDatasetImpl#finalizeBlock to verify that
			// the block is not removed, since the volume reference should not
			// be released at this point.
			CyclicBarrier barrier = new CyclicBarrier(2);
			IList<string> oldDirs = GetDataDirs(dn);
			string newDirs = oldDirs[1];
			// Remove the first volume.
			IList<Exception> exceptions = new AList<Exception>();
			Sharpen.Thread reconfigThread = new _Thread_616(barrier, dn, newDirs, exceptions);
			reconfigThread.Start();
			barrier.Await();
			rb.NextBytes(writeBuf);
			@out.Write(writeBuf);
			@out.Hflush();
			@out.Close();
			reconfigThread.Join();
			// Verify the file has sufficient replications.
			DFSTestUtil.WaitReplication(fs, testFile, Replication);
			// Read the content back
			byte[] content = DFSTestUtil.ReadFileBuffer(fs, testFile);
			NUnit.Framework.Assert.AreEqual(BlockSize, content.Length);
			// If an IOException thrown from BlockReceiver#run, it triggers
			// DataNode#checkDiskError(). So we can test whether checkDiskError() is called,
			// to see whether there is IOException in BlockReceiver#run().
			NUnit.Framework.Assert.AreEqual(lastTimeDiskErrorCheck, dn.GetLastDiskErrorCheck(
				));
			if (!exceptions.IsEmpty())
			{
				throw new IOException(exceptions[0].InnerException);
			}
		}

		private sealed class _Answer_599 : Answer<object>
		{
			public _Answer_599(FsDatasetSpi<FsVolumeSpi> data)
			{
				this.data = data;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				Sharpen.Thread.Sleep(1000);
				data.FinalizeBlock((ExtendedBlock)invocation.GetArguments()[0]);
				return null;
			}

			private readonly FsDatasetSpi<FsVolumeSpi> data;
		}

		private sealed class _Thread_616 : Sharpen.Thread
		{
			public _Thread_616(CyclicBarrier barrier, DataNode dn, string newDirs, IList<Exception
				> exceptions)
			{
				this.barrier = barrier;
				this.dn = dn;
				this.newDirs = newDirs;
				this.exceptions = exceptions;
			}

			public override void Run()
			{
				try
				{
					barrier.Await();
					dn.ReconfigurePropertyImpl(DFSConfigKeys.DfsDatanodeDataDirKey, newDirs);
				}
				catch (Exception e)
				{
					exceptions.AddItem(e);
				}
			}

			private readonly CyclicBarrier barrier;

			private readonly DataNode dn;

			private readonly string newDirs;

			private readonly IList<Exception> exceptions;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		public virtual void TestAddBackRemovedVolume()
		{
			StartDFSCluster(1, 2);
			// Create some data on every volume.
			CreateFile(new Path("/test"), 32);
			DataNode dn = cluster.GetDataNodes()[0];
			Configuration conf = dn.GetConf();
			string oldDataDir = conf.Get(DFSConfigKeys.DfsDatanodeDataDirKey);
			string keepDataDir = oldDataDir.Split(",")[0];
			string removeDataDir = oldDataDir.Split(",")[1];
			dn.ReconfigurePropertyImpl(DFSConfigKeys.DfsDatanodeDataDirKey, keepDataDir);
			for (int i = 0; i < cluster.GetNumNameNodes(); i++)
			{
				string bpid = cluster.GetNamesystem(i).GetBlockPoolId();
				BlockPoolSliceStorage bpsStorage = dn.GetStorage().GetBPStorage(bpid);
				// Make sure that there is no block pool level storage under removeDataDir.
				for (int j = 0; j < bpsStorage.GetNumStorageDirs(); j++)
				{
					Storage.StorageDirectory sd = bpsStorage.GetStorageDir(j);
					NUnit.Framework.Assert.IsFalse(sd.GetRoot().GetAbsolutePath().StartsWith(new FilePath
						(removeDataDir).GetAbsolutePath()));
				}
				NUnit.Framework.Assert.AreEqual(dn.GetStorage().GetBPStorage(bpid).GetNumStorageDirs
					(), 1);
			}
			// Bring the removed directory back. It only successes if all metadata about
			// this directory were removed from the previous step.
			dn.ReconfigurePropertyImpl(DFSConfigKeys.DfsDatanodeDataDirKey, oldDataDir);
		}

		/// <summary>Get the FsVolume on the given basePath</summary>
		private FsVolumeImpl GetVolume(DataNode dn, FilePath basePath)
		{
			foreach (FsVolumeSpi vol in dn.GetFSDataset().GetVolumes())
			{
				if (vol.GetBasePath().Equals(basePath.GetPath()))
				{
					return (FsVolumeImpl)vol;
				}
			}
			return null;
		}

		/// <summary>
		/// Verify that
		/// <see cref="DataNode#checkDiskErrors()"/>
		/// removes all metadata in
		/// DataNode upon a volume failure. Thus we can run reconfig on the same
		/// configuration to reload the new volume on the same directory as the failed one.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		public virtual void TestDirectlyReloadAfterCheckDiskError()
		{
			StartDFSCluster(1, 2);
			CreateFile(new Path("/test"), 32, (short)2);
			DataNode dn = cluster.GetDataNodes()[0];
			string oldDataDir = dn.GetConf().Get(DFSConfigKeys.DfsDatanodeDataDirKey);
			FilePath dirToFail = new FilePath(cluster.GetDataDirectory(), "data1");
			FsVolumeImpl failedVolume = GetVolume(dn, dirToFail);
			NUnit.Framework.Assert.IsTrue("No FsVolume was found for " + dirToFail, failedVolume
				 != null);
			long used = failedVolume.GetDfsUsed();
			DataNodeTestUtils.InjectDataDirFailure(dirToFail);
			// Call and wait DataNode to detect disk failure.
			long lastDiskErrorCheck = dn.GetLastDiskErrorCheck();
			dn.CheckDiskErrorAsync();
			while (dn.GetLastDiskErrorCheck() == lastDiskErrorCheck)
			{
				Sharpen.Thread.Sleep(100);
			}
			CreateFile(new Path("/test1"), 32, (short)2);
			NUnit.Framework.Assert.AreEqual(used, failedVolume.GetDfsUsed());
			DataNodeTestUtils.RestoreDataDirFromFailure(dirToFail);
			dn.ReconfigurePropertyImpl(DFSConfigKeys.DfsDatanodeDataDirKey, oldDataDir);
			CreateFile(new Path("/test2"), 32, (short)2);
			FsVolumeImpl restoredVolume = GetVolume(dn, dirToFail);
			NUnit.Framework.Assert.IsTrue(restoredVolume != null);
			NUnit.Framework.Assert.IsTrue(restoredVolume != failedVolume);
			// More data has been written to this volume.
			NUnit.Framework.Assert.IsTrue(restoredVolume.GetDfsUsed() > used);
		}

		/// <summary>Test that a full block report is sent after hot swapping volumes</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		public virtual void TestFullBlockReportAfterRemovingVolumes()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			// Similar to TestTriggerBlockReport, set a really long value for
			// dfs.heartbeat.interval, so that incremental block reports and heartbeats
			// won't be sent during this test unless they're triggered
			// manually.
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10800000L);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1080L);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			cluster.WaitActive();
			DataNode dn = cluster.GetDataNodes()[0];
			DatanodeProtocolClientSideTranslatorPB spy = DataNodeTestUtils.SpyOnBposToNN(dn, 
				cluster.GetNameNode());
			// Remove a data dir from datanode
			FilePath dataDirToKeep = new FilePath(cluster.GetDataDirectory(), "data1");
			dn.ReconfigurePropertyImpl(DFSConfigKeys.DfsDatanodeDataDirKey, dataDirToKeep.ToString
				());
			// We should get 1 full report
			Org.Mockito.Mockito.Verify(spy, Org.Mockito.Mockito.Timeout(60000).Times(1)).BlockReport
				(Matchers.Any<DatanodeRegistration>(), Matchers.AnyString(), Matchers.Any<StorageBlockReport
				[]>(), Matchers.Any<BlockReportContext>());
		}
	}
}
