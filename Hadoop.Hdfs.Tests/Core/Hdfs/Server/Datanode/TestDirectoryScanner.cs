using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Hamcrest;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Tests
	/// <see cref="DirectoryScanner"/>
	/// handling of differences
	/// between blocks on the disk and block in memory.
	/// </summary>
	public class TestDirectoryScanner
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.TestDirectoryScanner
			));

		private static readonly Configuration Conf = new HdfsConfiguration();

		private const int DefaultGenStamp = 9999;

		private MiniDFSCluster cluster;

		private string bpid;

		private DFSClient client;

		private FsDatasetSpi<FsVolumeSpi> fds = null;

		private DirectoryScanner scanner = null;

		private readonly Random rand = new Random();

		private readonly Random r = new Random();

		private const int BlockLength = 100;

		static TestDirectoryScanner()
		{
			Conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockLength);
			Conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 1);
			Conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
		}

		/// <summary>create a file with a length of <code>fileLen</code></summary>
		/// <exception cref="System.IO.IOException"/>
		private IList<LocatedBlock> CreateFile(string fileNamePrefix, long fileLen, bool 
			isLazyPersist)
		{
			FileSystem fs = cluster.GetFileSystem();
			Path filePath = new Path("/" + fileNamePrefix + ".dat");
			DFSTestUtil.CreateFile(fs, filePath, isLazyPersist, 1024, fileLen, BlockLength, (
				short)1, r.NextLong(), false);
			return client.GetLocatedBlocks(filePath.ToString(), 0, fileLen).GetLocatedBlocks(
				);
		}

		/// <summary>Truncate a block file</summary>
		/// <exception cref="System.IO.IOException"/>
		private long TruncateBlockFile()
		{
			lock (fds)
			{
				foreach (ReplicaInfo b in FsDatasetTestUtil.GetReplicas(fds, bpid))
				{
					FilePath f = b.GetBlockFile();
					FilePath mf = b.GetMetaFile();
					// Truncate a block file that has a corresponding metadata file
					if (f.Exists() && f.Length() != 0 && mf.Exists())
					{
						FileOutputStream s = null;
						FileChannel channel = null;
						try
						{
							s = new FileOutputStream(f);
							channel = s.GetChannel();
							channel.Truncate(0);
							Log.Info("Truncated block file " + f.GetAbsolutePath());
							return b.GetBlockId();
						}
						finally
						{
							IOUtils.Cleanup(Log, channel, s);
						}
					}
				}
			}
			return 0;
		}

		/// <summary>Delete a block file</summary>
		private long DeleteBlockFile()
		{
			lock (fds)
			{
				foreach (ReplicaInfo b in FsDatasetTestUtil.GetReplicas(fds, bpid))
				{
					FilePath f = b.GetBlockFile();
					FilePath mf = b.GetMetaFile();
					// Delete a block file that has corresponding metadata file
					if (f.Exists() && mf.Exists() && f.Delete())
					{
						Log.Info("Deleting block file " + f.GetAbsolutePath());
						return b.GetBlockId();
					}
				}
			}
			return 0;
		}

		/// <summary>Delete block meta file</summary>
		private long DeleteMetaFile()
		{
			lock (fds)
			{
				foreach (ReplicaInfo b in FsDatasetTestUtil.GetReplicas(fds, bpid))
				{
					FilePath file = b.GetMetaFile();
					// Delete a metadata file
					if (file.Exists() && file.Delete())
					{
						Log.Info("Deleting metadata file " + file.GetAbsolutePath());
						return b.GetBlockId();
					}
				}
			}
			return 0;
		}

		/// <summary>Duplicate the given block on all volumes.</summary>
		/// <param name="blockId"/>
		/// <exception cref="System.IO.IOException"/>
		private void DuplicateBlock(long blockId)
		{
			lock (fds)
			{
				ReplicaInfo b = FsDatasetTestUtil.FetchReplicaInfo(fds, bpid, blockId);
				foreach (FsVolumeSpi v in fds.GetVolumes())
				{
					if (v.GetStorageID().Equals(b.GetVolume().GetStorageID()))
					{
						continue;
					}
					// Volume without a copy of the block. Make a copy now.
					FilePath sourceBlock = b.GetBlockFile();
					FilePath sourceMeta = b.GetMetaFile();
					string sourceRoot = b.GetVolume().GetBasePath();
					string destRoot = v.GetBasePath();
					string relativeBlockPath = new FilePath(sourceRoot).ToURI().Relativize(sourceBlock
						.ToURI()).GetPath();
					string relativeMetaPath = new FilePath(sourceRoot).ToURI().Relativize(sourceMeta.
						ToURI()).GetPath();
					FilePath destBlock = new FilePath(destRoot, relativeBlockPath);
					FilePath destMeta = new FilePath(destRoot, relativeMetaPath);
					destBlock.GetParentFile().Mkdirs();
					FileUtils.CopyFile(sourceBlock, destBlock);
					FileUtils.CopyFile(sourceMeta, destMeta);
					if (destBlock.Exists() && destMeta.Exists())
					{
						Log.Info("Copied " + sourceBlock + " ==> " + destBlock);
						Log.Info("Copied " + sourceMeta + " ==> " + destMeta);
					}
				}
			}
		}

		/// <summary>Get a random blockId that is not used already</summary>
		private long GetFreeBlockId()
		{
			long id = rand.NextLong();
			while (true)
			{
				id = rand.NextLong();
				if (FsDatasetTestUtil.FetchReplicaInfo(fds, bpid, id) == null)
				{
					break;
				}
			}
			return id;
		}

		private string GetBlockFile(long id)
		{
			return Block.BlockFilePrefix + id;
		}

		private string GetMetaFile(long id)
		{
			return Block.BlockFilePrefix + id + "_" + DefaultGenStamp + Block.MetadataExtension;
		}

		/// <summary>Create a block file in a random volume</summary>
		/// <exception cref="System.IO.IOException"/>
		private long CreateBlockFile()
		{
			IList<FsVolumeSpi> volumes = fds.GetVolumes();
			int index = rand.Next(volumes.Count - 1);
			long id = GetFreeBlockId();
			FilePath finalizedDir = volumes[index].GetFinalizedDir(bpid);
			FilePath file = new FilePath(finalizedDir, GetBlockFile(id));
			if (file.CreateNewFile())
			{
				Log.Info("Created block file " + file.GetName());
			}
			return id;
		}

		/// <summary>Create a metafile in a random volume</summary>
		/// <exception cref="System.IO.IOException"/>
		private long CreateMetaFile()
		{
			IList<FsVolumeSpi> volumes = fds.GetVolumes();
			int index = rand.Next(volumes.Count - 1);
			long id = GetFreeBlockId();
			FilePath finalizedDir = volumes[index].GetFinalizedDir(bpid);
			FilePath file = new FilePath(finalizedDir, GetMetaFile(id));
			if (file.CreateNewFile())
			{
				Log.Info("Created metafile " + file.GetName());
			}
			return id;
		}

		/// <summary>Create block file and corresponding metafile in a rondom volume</summary>
		/// <exception cref="System.IO.IOException"/>
		private long CreateBlockMetaFile()
		{
			IList<FsVolumeSpi> volumes = fds.GetVolumes();
			int index = rand.Next(volumes.Count - 1);
			long id = GetFreeBlockId();
			FilePath finalizedDir = volumes[index].GetFinalizedDir(bpid);
			FilePath file = new FilePath(finalizedDir, GetBlockFile(id));
			if (file.CreateNewFile())
			{
				Log.Info("Created block file " + file.GetName());
				// Create files with same prefix as block file but extension names
				// such that during sorting, these files appear around meta file
				// to test how DirectoryScanner handles extraneous files
				string name1 = file.GetAbsolutePath() + ".l";
				string name2 = file.GetAbsolutePath() + ".n";
				file = new FilePath(name1);
				if (file.CreateNewFile())
				{
					Log.Info("Created extraneous file " + name1);
				}
				file = new FilePath(name2);
				if (file.CreateNewFile())
				{
					Log.Info("Created extraneous file " + name2);
				}
				file = new FilePath(finalizedDir, GetMetaFile(id));
				if (file.CreateNewFile())
				{
					Log.Info("Created metafile " + file.GetName());
				}
			}
			return id;
		}

		/// <exception cref="System.IO.IOException"/>
		private void Scan(long totalBlocks, int diffsize, long missingMetaFile, long missingBlockFile
			, long missingMemoryBlocks, long mismatchBlocks)
		{
			Scan(totalBlocks, diffsize, missingMetaFile, missingBlockFile, missingMemoryBlocks
				, mismatchBlocks, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		private void Scan(long totalBlocks, int diffsize, long missingMetaFile, long missingBlockFile
			, long missingMemoryBlocks, long mismatchBlocks, long duplicateBlocks)
		{
			scanner.Reconcile();
			NUnit.Framework.Assert.IsTrue(scanner.diffs.Contains(bpid));
			List<DirectoryScanner.ScanInfo> diff = scanner.diffs[bpid];
			NUnit.Framework.Assert.IsTrue(scanner.stats.Contains(bpid));
			DirectoryScanner.Stats stats = scanner.stats[bpid];
			NUnit.Framework.Assert.AreEqual(diffsize, diff.Count);
			NUnit.Framework.Assert.AreEqual(totalBlocks, stats.totalBlocks);
			NUnit.Framework.Assert.AreEqual(missingMetaFile, stats.missingMetaFile);
			NUnit.Framework.Assert.AreEqual(missingBlockFile, stats.missingBlockFile);
			NUnit.Framework.Assert.AreEqual(missingMemoryBlocks, stats.missingMemoryBlocks);
			NUnit.Framework.Assert.AreEqual(mismatchBlocks, stats.mismatchBlocks);
			NUnit.Framework.Assert.AreEqual(duplicateBlocks, stats.duplicateBlocks);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRetainBlockOnPersistentStorage()
		{
			cluster = new MiniDFSCluster.Builder(Conf).StorageTypes(new StorageType[] { StorageType
				.RamDisk, StorageType.Default }).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				DataNode dataNode = cluster.GetDataNodes()[0];
				bpid = cluster.GetNamesystem().GetBlockPoolId();
				fds = DataNodeTestUtils.GetFSDataset(cluster.GetDataNodes()[0]);
				client = cluster.GetFileSystem().GetClient();
				scanner = new DirectoryScanner(dataNode, fds, Conf);
				scanner.SetRetainDiffs(true);
				FsDatasetTestUtil.StopLazyWriter(cluster.GetDataNodes()[0]);
				// Add a file with 1 block
				IList<LocatedBlock> blocks = CreateFile(GenericTestUtils.GetMethodName(), BlockLength
					, false);
				// Ensure no difference between volumeMap and disk.
				Scan(1, 0, 0, 0, 0, 0);
				// Make a copy of the block on RAM_DISK and ensure that it is
				// picked up by the scanner.
				DuplicateBlock(blocks[0].GetBlock().GetBlockId());
				Scan(2, 1, 0, 0, 0, 0, 1);
				VerifyStorageType(blocks[0].GetBlock().GetBlockId(), false);
				Scan(1, 0, 0, 0, 0, 0);
			}
			finally
			{
				if (scanner != null)
				{
					scanner.Shutdown();
					scanner = null;
				}
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteBlockOnTransientStorage()
		{
			cluster = new MiniDFSCluster.Builder(Conf).StorageTypes(new StorageType[] { StorageType
				.RamDisk, StorageType.Default }).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				bpid = cluster.GetNamesystem().GetBlockPoolId();
				DataNode dataNode = cluster.GetDataNodes()[0];
				fds = DataNodeTestUtils.GetFSDataset(cluster.GetDataNodes()[0]);
				client = cluster.GetFileSystem().GetClient();
				scanner = new DirectoryScanner(dataNode, fds, Conf);
				scanner.SetRetainDiffs(true);
				FsDatasetTestUtil.StopLazyWriter(cluster.GetDataNodes()[0]);
				// Create a file file on RAM_DISK
				IList<LocatedBlock> blocks = CreateFile(GenericTestUtils.GetMethodName(), BlockLength
					, true);
				// Ensure no difference between volumeMap and disk.
				Scan(1, 0, 0, 0, 0, 0);
				// Make a copy of the block on DEFAULT storage and ensure that it is
				// picked up by the scanner.
				DuplicateBlock(blocks[0].GetBlock().GetBlockId());
				Scan(2, 1, 0, 0, 0, 0, 1);
				// Ensure that the copy on RAM_DISK was deleted.
				VerifyStorageType(blocks[0].GetBlock().GetBlockId(), false);
				Scan(1, 0, 0, 0, 0, 0);
			}
			finally
			{
				if (scanner != null)
				{
					scanner.Shutdown();
					scanner = null;
				}
				cluster.Shutdown();
				cluster = null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDirectoryScanner()
		{
			// Run the test with and without parallel scanning
			for (int parallelism = 1; parallelism < 3; parallelism++)
			{
				RunTest(parallelism);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void RunTest(int parallelism)
		{
			cluster = new MiniDFSCluster.Builder(Conf).Build();
			try
			{
				cluster.WaitActive();
				bpid = cluster.GetNamesystem().GetBlockPoolId();
				fds = DataNodeTestUtils.GetFSDataset(cluster.GetDataNodes()[0]);
				client = cluster.GetFileSystem().GetClient();
				Conf.SetInt(DFSConfigKeys.DfsDatanodeDirectoryscanThreadsKey, parallelism);
				DataNode dataNode = cluster.GetDataNodes()[0];
				scanner = new DirectoryScanner(dataNode, fds, Conf);
				scanner.SetRetainDiffs(true);
				// Add files with 100 blocks
				CreateFile(GenericTestUtils.GetMethodName(), BlockLength * 100, false);
				long totalBlocks = 100;
				// Test1: No difference between volumeMap and disk
				Scan(100, 0, 0, 0, 0, 0);
				// Test2: block metafile is missing
				long blockId = DeleteMetaFile();
				Scan(totalBlocks, 1, 1, 0, 0, 1);
				VerifyGenStamp(blockId, GenerationStamp.GrandfatherGenerationStamp);
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test3: block file is missing
				blockId = DeleteBlockFile();
				Scan(totalBlocks, 1, 0, 1, 0, 0);
				totalBlocks--;
				VerifyDeletion(blockId);
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test4: A block file exists for which there is no metafile and
				// a block in memory
				blockId = CreateBlockFile();
				totalBlocks++;
				Scan(totalBlocks, 1, 1, 0, 1, 0);
				VerifyAddition(blockId, GenerationStamp.GrandfatherGenerationStamp, 0);
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test5: A metafile exists for which there is no block file and
				// a block in memory
				blockId = CreateMetaFile();
				Scan(totalBlocks + 1, 1, 0, 1, 1, 0);
				FilePath metafile = new FilePath(GetMetaFile(blockId));
				NUnit.Framework.Assert.IsTrue(!metafile.Exists());
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test6: A block file and metafile exists for which there is no block in
				// memory
				blockId = CreateBlockMetaFile();
				totalBlocks++;
				Scan(totalBlocks, 1, 0, 0, 1, 0);
				VerifyAddition(blockId, DefaultGenStamp, 0);
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test7: Delete bunch of metafiles
				for (int i = 0; i < 10; i++)
				{
					blockId = DeleteMetaFile();
				}
				Scan(totalBlocks, 10, 10, 0, 0, 10);
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test8: Delete bunch of block files
				for (int i_1 = 0; i_1 < 10; i_1++)
				{
					blockId = DeleteBlockFile();
				}
				Scan(totalBlocks, 10, 0, 10, 0, 0);
				totalBlocks -= 10;
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test9: create a bunch of blocks files
				for (int i_2 = 0; i_2 < 10; i_2++)
				{
					blockId = CreateBlockFile();
				}
				totalBlocks += 10;
				Scan(totalBlocks, 10, 10, 0, 10, 0);
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test10: create a bunch of metafiles
				for (int i_3 = 0; i_3 < 10; i_3++)
				{
					blockId = CreateMetaFile();
				}
				Scan(totalBlocks + 10, 10, 0, 10, 10, 0);
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test11: create a bunch block files and meta files
				for (int i_4 = 0; i_4 < 10; i_4++)
				{
					blockId = CreateBlockMetaFile();
				}
				totalBlocks += 10;
				Scan(totalBlocks, 10, 0, 0, 10, 0);
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test12: truncate block files to test block length mismatch
				for (int i_5 = 0; i_5 < 10; i_5++)
				{
					TruncateBlockFile();
				}
				Scan(totalBlocks, 10, 0, 0, 0, 10);
				Scan(totalBlocks, 0, 0, 0, 0, 0);
				// Test13: all the conditions combined
				CreateMetaFile();
				CreateBlockFile();
				CreateBlockMetaFile();
				DeleteMetaFile();
				DeleteBlockFile();
				TruncateBlockFile();
				Scan(totalBlocks + 3, 6, 2, 2, 3, 2);
				Scan(totalBlocks + 1, 0, 0, 0, 0, 0);
				// Test14: validate clean shutdown of DirectoryScanner
				////assertTrue(scanner.getRunStatus()); //assumes "real" FSDataset, not sim
				scanner.Shutdown();
				NUnit.Framework.Assert.IsFalse(scanner.GetRunStatus());
			}
			finally
			{
				if (scanner != null)
				{
					scanner.Shutdown();
					scanner = null;
				}
				cluster.Shutdown();
			}
		}

		private void VerifyAddition(long blockId, long genStamp, long size)
		{
			ReplicaInfo replicainfo;
			replicainfo = FsDatasetTestUtil.FetchReplicaInfo(fds, bpid, blockId);
			NUnit.Framework.Assert.IsNotNull(replicainfo);
			// Added block has the same file as the one created by the test
			FilePath file = new FilePath(GetBlockFile(blockId));
			NUnit.Framework.Assert.AreEqual(file.GetName(), FsDatasetTestUtil.GetFile(fds, bpid
				, blockId).GetName());
			// Generation stamp is same as that of created file
			NUnit.Framework.Assert.AreEqual(genStamp, replicainfo.GetGenerationStamp());
			// File size matches
			NUnit.Framework.Assert.AreEqual(size, replicainfo.GetNumBytes());
		}

		private void VerifyDeletion(long blockId)
		{
			// Ensure block does not exist in memory
			NUnit.Framework.Assert.IsNull(FsDatasetTestUtil.FetchReplicaInfo(fds, bpid, blockId
				));
		}

		private void VerifyGenStamp(long blockId, long genStamp)
		{
			ReplicaInfo memBlock;
			memBlock = FsDatasetTestUtil.FetchReplicaInfo(fds, bpid, blockId);
			NUnit.Framework.Assert.IsNotNull(memBlock);
			NUnit.Framework.Assert.AreEqual(genStamp, memBlock.GetGenerationStamp());
		}

		private void VerifyStorageType(long blockId, bool expectTransient)
		{
			ReplicaInfo memBlock;
			memBlock = FsDatasetTestUtil.FetchReplicaInfo(fds, bpid, blockId);
			NUnit.Framework.Assert.IsNotNull(memBlock);
			MatcherAssert.AssertThat(memBlock.GetVolume().IsTransientStorage(), IS.Is(expectTransient
				));
		}

		private class TestFsVolumeSpi : FsVolumeSpi
		{
			public override string[] GetBlockPoolList()
			{
				return new string[0];
			}

			/// <exception cref="Sharpen.ClosedChannelException"/>
			public override FsVolumeReference ObtainReference()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long GetAvailable()
			{
				return 0;
			}

			public override string GetBasePath()
			{
				return (new FilePath("/base")).GetAbsolutePath();
			}

			/// <exception cref="System.IO.IOException"/>
			public override string GetPath(string bpid)
			{
				return (new FilePath("/base/current/" + bpid)).GetAbsolutePath();
			}

			/// <exception cref="System.IO.IOException"/>
			public override FilePath GetFinalizedDir(string bpid)
			{
				return new FilePath("/base/current/" + bpid + "/finalized");
			}

			public override StorageType GetStorageType()
			{
				return StorageType.Default;
			}

			public override string GetStorageID()
			{
				return string.Empty;
			}

			public override void ReserveSpaceForRbw(long bytesToReserve)
			{
			}

			public override void ReleaseReservedSpace(long bytesToRelease)
			{
			}

			public override bool IsTransientStorage()
			{
				return false;
			}

			public override FsVolumeSpi.BlockIterator NewBlockIterator(string bpid, string name
				)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override FsVolumeSpi.BlockIterator LoadBlockIterator(string bpid, string name
				)
			{
				throw new NotSupportedException();
			}

			public override FsDatasetSpi GetDataset()
			{
				throw new NotSupportedException();
			}
		}

		private static readonly TestDirectoryScanner.TestFsVolumeSpi TestVolume = new TestDirectoryScanner.TestFsVolumeSpi
			();

		private const string Bpid1 = "BP-783049782-127.0.0.1-1370971773491";

		private const string Bpid2 = "BP-367845636-127.0.0.1-5895645674231";

		/// <exception cref="System.Exception"/>
		internal virtual void TestScanInfoObject(long blockId, FilePath blockFile, FilePath
			 metaFile)
		{
			DirectoryScanner.ScanInfo scanInfo = new DirectoryScanner.ScanInfo(blockId, blockFile
				, metaFile, TestVolume);
			NUnit.Framework.Assert.AreEqual(blockId, scanInfo.GetBlockId());
			if (blockFile != null)
			{
				NUnit.Framework.Assert.AreEqual(blockFile.GetAbsolutePath(), scanInfo.GetBlockFile
					().GetAbsolutePath());
			}
			else
			{
				NUnit.Framework.Assert.IsNull(scanInfo.GetBlockFile());
			}
			if (metaFile != null)
			{
				NUnit.Framework.Assert.AreEqual(metaFile.GetAbsolutePath(), scanInfo.GetMetaFile(
					).GetAbsolutePath());
			}
			else
			{
				NUnit.Framework.Assert.IsNull(scanInfo.GetMetaFile());
			}
			NUnit.Framework.Assert.AreEqual(TestVolume, scanInfo.GetVolume());
		}

		/// <exception cref="System.Exception"/>
		internal virtual void TestScanInfoObject(long blockId)
		{
			DirectoryScanner.ScanInfo scanInfo = new DirectoryScanner.ScanInfo(blockId, null, 
				null, null);
			NUnit.Framework.Assert.AreEqual(blockId, scanInfo.GetBlockId());
			NUnit.Framework.Assert.IsNull(scanInfo.GetBlockFile());
			NUnit.Framework.Assert.IsNull(scanInfo.GetMetaFile());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestScanInfo()
		{
			TestScanInfoObject(123, new FilePath(TestVolume.GetFinalizedDir(Bpid1).GetAbsolutePath
				(), "blk_123"), new FilePath(TestVolume.GetFinalizedDir(Bpid1).GetAbsolutePath()
				, "blk_123__1001.meta"));
			TestScanInfoObject(464, new FilePath(TestVolume.GetFinalizedDir(Bpid1).GetAbsolutePath
				(), "blk_123"), null);
			TestScanInfoObject(523, null, new FilePath(TestVolume.GetFinalizedDir(Bpid1).GetAbsolutePath
				(), "blk_123__1009.meta"));
			TestScanInfoObject(789, null, null);
			TestScanInfoObject(456);
			TestScanInfoObject(123, new FilePath(TestVolume.GetFinalizedDir(Bpid2).GetAbsolutePath
				(), "blk_567"), new FilePath(TestVolume.GetFinalizedDir(Bpid2).GetAbsolutePath()
				, "blk_567__1004.meta"));
		}
	}
}
