using System;
using System.IO;
using Com.Google.Common.Util.Concurrent;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	public class TestLazyPersistFiles : LazyPersistTestCase
	{
		private const byte LazyPersistPolicyId = unchecked((byte)15);

		private const int ThreadpoolSize = 10;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPolicyNotSetByDefault()
		{
			StartUpCluster(false, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, 0, false);
			// Stat the file and check that the LAZY_PERSIST policy is not
			// returned back.
			HdfsFileStatus status = client.GetFileInfo(path.ToString());
			Assert.AssertThat(status.GetStoragePolicy(), IsNot.Not(LazyPersistPolicyId));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPolicyPropagation()
		{
			StartUpCluster(false, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, 0, true);
			// Stat the file and check that the lazyPersist flag is returned back.
			HdfsFileStatus status = client.GetFileInfo(path.ToString());
			Assert.AssertThat(status.GetStoragePolicy(), IS.Is(LazyPersistPolicyId));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPolicyPersistenceInEditLog()
		{
			StartUpCluster(false, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, 0, true);
			cluster.RestartNameNode(true);
			// Stat the file and check that the lazyPersist flag is returned back.
			HdfsFileStatus status = client.GetFileInfo(path.ToString());
			Assert.AssertThat(status.GetStoragePolicy(), IS.Is(LazyPersistPolicyId));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPolicyPersistenceInFsImage()
		{
			StartUpCluster(false, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, 0, true);
			// checkpoint
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			fs.SaveNamespace();
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			cluster.RestartNameNode(true);
			// Stat the file and check that the lazyPersist flag is returned back.
			HdfsFileStatus status = client.GetFileInfo(path.ToString());
			Assert.AssertThat(status.GetStoragePolicy(), IS.Is(LazyPersistPolicyId));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPlacementOnRamDisk()
		{
			StartUpCluster(true, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, BlockSize, true);
			EnsureFileReplicasOnStorageType(path, StorageType.RamDisk);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPlacementOnSizeLimitedRamDisk()
		{
			StartUpCluster(true, 3);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".01.dat");
			Path path2 = new Path("/" + MethodName + ".02.dat");
			MakeTestFile(path1, BlockSize, true);
			MakeTestFile(path2, BlockSize, true);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			EnsureFileReplicasOnStorageType(path2, StorageType.RamDisk);
		}

		/// <summary>
		/// Client tries to write LAZY_PERSIST to same DN with no RamDisk configured
		/// Write should default to disk.
		/// </summary>
		/// <remarks>
		/// Client tries to write LAZY_PERSIST to same DN with no RamDisk configured
		/// Write should default to disk. No error.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFallbackToDisk()
		{
			StartUpCluster(false, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, BlockSize, true);
			EnsureFileReplicasOnStorageType(path, StorageType.Default);
		}

		/// <summary>File can not fit in RamDisk even with eviction</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFallbackToDiskFull()
		{
			StartUpCluster(false, 0);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, BlockSize, true);
			EnsureFileReplicasOnStorageType(path, StorageType.Default);
			VerifyRamDiskJMXMetric("RamDiskBlocksWriteFallback", 1);
		}

		/// <summary>File partially fit in RamDisk after eviction.</summary>
		/// <remarks>
		/// File partially fit in RamDisk after eviction.
		/// RamDisk can fit 2 blocks. Write a file with 5 blocks.
		/// Expect 2 or less blocks are on RamDisk and 3 or more on disk.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFallbackToDiskPartial()
		{
			StartUpCluster(true, 2);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, BlockSize * 5, true);
			// Sleep for a short time to allow the lazy writer thread to do its job
			Sharpen.Thread.Sleep(6 * LazyWriterIntervalSec * 1000);
			TriggerBlockReport();
			int numBlocksOnRamDisk = 0;
			int numBlocksOnDisk = 0;
			long fileLength = client.GetFileInfo(path.ToString()).GetLen();
			LocatedBlocks locatedBlocks = client.GetLocatedBlocks(path.ToString(), 0, fileLength
				);
			foreach (LocatedBlock locatedBlock in locatedBlocks.GetLocatedBlocks())
			{
				if (locatedBlock.GetStorageTypes()[0] == StorageType.RamDisk)
				{
					numBlocksOnRamDisk++;
				}
				else
				{
					if (locatedBlock.GetStorageTypes()[0] == StorageType.Default)
					{
						numBlocksOnDisk++;
					}
				}
			}
			// Since eviction is asynchronous, depending on the timing of eviction
			// wrt writes, we may get 2 or less blocks on RAM disk.
			System.Diagnostics.Debug.Assert((numBlocksOnRamDisk <= 2));
			System.Diagnostics.Debug.Assert((numBlocksOnDisk >= 3));
		}

		/// <summary>
		/// If the only available storage is RAM_DISK and the LAZY_PERSIST flag is not
		/// specified, then block placement should fail.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRamDiskNotChosenByDefault()
		{
			StartUpCluster(true, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			try
			{
				MakeTestFile(path, BlockSize, false);
				NUnit.Framework.Assert.Fail("Block placement to RAM_DISK should have failed without lazyPersist flag"
					);
			}
			catch (Exception t)
			{
				Log.Info("Got expected exception ", t);
			}
		}

		/// <summary>Append to lazy persist file is denied.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendIsDenied()
		{
			StartUpCluster(true, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, BlockSize, true);
			try
			{
				client.Append(path.ToString(), BufferLength, EnumSet.Of(CreateFlag.Append), null, 
					null).Close();
				NUnit.Framework.Assert.Fail("Append to LazyPersist file did not fail as expected"
					);
			}
			catch (Exception t)
			{
				Log.Info("Got expected exception ", t);
			}
		}

		/// <summary>Truncate to lazy persist file is denied.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncateIsDenied()
		{
			StartUpCluster(true, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, BlockSize, true);
			try
			{
				client.Truncate(path.ToString(), BlockSize / 2);
				NUnit.Framework.Assert.Fail("Truncate to LazyPersist file did not fail as expected"
					);
			}
			catch (Exception t)
			{
				Log.Info("Got expected exception ", t);
			}
		}

		/// <summary>
		/// If one or more replicas of a lazyPersist file are lost, then the file
		/// must be discarded by the NN, instead of being kept around as a
		/// 'corrupt' file.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLazyPersistFilesAreDiscarded()
		{
			StartUpCluster(true, 2);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".01.dat");
			MakeTestFile(path1, BlockSize, true);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			// Stop the DataNode and sleep for the time it takes the NN to
			// detect the DN as being dead.
			cluster.ShutdownDataNodes();
			Sharpen.Thread.Sleep(30000L);
			Assert.AssertThat(cluster.GetNamesystem().GetNumDeadDataNodes(), IS.Is(1));
			// Next, wait for the replication monitor to mark the file as corrupt
			Sharpen.Thread.Sleep(2 * DfsNamenodeReplicationIntervalDefault * 1000);
			// Wait for the LazyPersistFileScrubber to run
			Sharpen.Thread.Sleep(2 * LazyWriteFileScrubberIntervalSec * 1000);
			// Ensure that path1 does not exist anymore, whereas path2 does.
			System.Diagnostics.Debug.Assert((!fs.Exists(path1)));
			// We should have zero blocks that needs replication i.e. the one
			// belonging to path2.
			Assert.AssertThat(cluster.GetNameNode().GetNamesystem().GetBlockManager().GetUnderReplicatedBlocksCount
				(), IS.Is(0L));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLazyPersistBlocksAreSaved()
		{
			StartUpCluster(true, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			// Create a test file
			MakeTestFile(path, BlockSize * 10, true);
			LocatedBlocks locatedBlocks = EnsureFileReplicasOnStorageType(path, StorageType.RamDisk
				);
			// Sleep for a short time to allow the lazy writer thread to do its job
			Sharpen.Thread.Sleep(6 * LazyWriterIntervalSec * 1000);
			Log.Info("Verifying copy was saved to lazyPersist/");
			// Make sure that there is a saved copy of the replica on persistent
			// storage.
			EnsureLazyPersistBlocksAreSaved(locatedBlocks);
		}

		/// <summary>RamDisk eviction after lazy persist to disk.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRamDiskEviction()
		{
			StartUpCluster(true, 1 + EvictionLowWatermark);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".01.dat");
			Path path2 = new Path("/" + MethodName + ".02.dat");
			int Seed = unchecked((int)(0xFADED));
			MakeRandomTestFile(path1, BlockSize, true, Seed);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			// Sleep for a short time to allow the lazy writer thread to do its job.
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			// Create another file with a replica on RAM_DISK.
			MakeTestFile(path2, BlockSize, true);
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			TriggerBlockReport();
			// Ensure the first file was evicted to disk, the second is still on
			// RAM_DISK.
			EnsureFileReplicasOnStorageType(path2, StorageType.RamDisk);
			EnsureFileReplicasOnStorageType(path1, StorageType.Default);
			VerifyRamDiskJMXMetric("RamDiskBlocksEvicted", 1);
			VerifyRamDiskJMXMetric("RamDiskBlocksEvictedWithoutRead", 1);
		}

		/// <summary>
		/// RamDisk eviction should not happen on blocks that are not yet
		/// persisted on disk.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRamDiskEvictionBeforePersist()
		{
			StartUpCluster(true, 1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".01.dat");
			Path path2 = new Path("/" + MethodName + ".02.dat");
			int Seed = 0XFADED;
			// Stop lazy writer to ensure block for path1 is not persisted to disk.
			FsDatasetTestUtil.StopLazyWriter(cluster.GetDataNodes()[0]);
			MakeRandomTestFile(path1, BlockSize, true, Seed);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			// Create second file with a replica on RAM_DISK.
			MakeTestFile(path2, BlockSize, true);
			// Eviction should not happen for block of the first file that is not
			// persisted yet.
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			EnsureFileReplicasOnStorageType(path2, StorageType.Default);
			System.Diagnostics.Debug.Assert((fs.Exists(path1)));
			System.Diagnostics.Debug.Assert((fs.Exists(path2)));
			NUnit.Framework.Assert.IsTrue(VerifyReadRandomFile(path1, BlockSize, Seed));
		}

		/// <summary>Validates lazy persisted blocks are evicted from RAM_DISK based on LRU.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRamDiskEvictionIsLru()
		{
			int NumPaths = 5;
			StartUpCluster(true, NumPaths + EvictionLowWatermark);
			string MethodName = GenericTestUtils.GetMethodName();
			Path[] paths = new Path[NumPaths * 2];
			for (int i = 0; i < paths.Length; i++)
			{
				paths[i] = new Path("/" + MethodName + "." + i + ".dat");
			}
			for (int i_1 = 0; i_1 < NumPaths; i_1++)
			{
				MakeTestFile(paths[i_1], BlockSize, true);
			}
			// Sleep for a short time to allow the lazy writer thread to do its job.
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			for (int i_2 = 0; i_2 < NumPaths; ++i_2)
			{
				EnsureFileReplicasOnStorageType(paths[i_2], StorageType.RamDisk);
			}
			// Open the files for read in a random order.
			AList<int> indexes = new AList<int>(NumPaths);
			for (int i_3 = 0; i_3 < NumPaths; ++i_3)
			{
				indexes.AddItem(i_3);
			}
			Collections.Shuffle(indexes);
			for (int i_4 = 0; i_4 < NumPaths; ++i_4)
			{
				Log.Info("Touching file " + paths[indexes[i_4]]);
				DFSTestUtil.ReadFile(fs, paths[indexes[i_4]]);
			}
			// Create an equal number of new files ensuring that the previous
			// files are evicted in the same order they were read.
			for (int i_5 = 0; i_5 < NumPaths; ++i_5)
			{
				MakeTestFile(paths[i_5 + NumPaths], BlockSize, true);
				TriggerBlockReport();
				Sharpen.Thread.Sleep(3000);
				EnsureFileReplicasOnStorageType(paths[i_5 + NumPaths], StorageType.RamDisk);
				EnsureFileReplicasOnStorageType(paths[indexes[i_5]], StorageType.Default);
				for (int j = i_5 + 1; j < NumPaths; ++j)
				{
					EnsureFileReplicasOnStorageType(paths[indexes[j]], StorageType.RamDisk);
				}
			}
			VerifyRamDiskJMXMetric("RamDiskBlocksWrite", NumPaths * 2);
			VerifyRamDiskJMXMetric("RamDiskBlocksWriteFallback", 0);
			VerifyRamDiskJMXMetric("RamDiskBytesWrite", BlockSize * NumPaths * 2);
			VerifyRamDiskJMXMetric("RamDiskBlocksReadHits", NumPaths);
			VerifyRamDiskJMXMetric("RamDiskBlocksEvicted", NumPaths);
			VerifyRamDiskJMXMetric("RamDiskBlocksEvictedWithoutRead", 0);
			VerifyRamDiskJMXMetric("RamDiskBlocksDeletedBeforeLazyPersisted", 0);
		}

		/// <summary>Delete lazy-persist file that has not been persisted to disk.</summary>
		/// <remarks>
		/// Delete lazy-persist file that has not been persisted to disk.
		/// Memory is freed up and file is gone.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteBeforePersist()
		{
			StartUpCluster(true, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			FsDatasetTestUtil.StopLazyWriter(cluster.GetDataNodes()[0]);
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, BlockSize, true);
			LocatedBlocks locatedBlocks = EnsureFileReplicasOnStorageType(path, StorageType.RamDisk
				);
			// Delete before persist
			client.Delete(path.ToString(), false);
			NUnit.Framework.Assert.IsFalse(fs.Exists(path));
			Assert.AssertThat(VerifyDeletedBlocks(locatedBlocks), IS.Is(true));
			VerifyRamDiskJMXMetric("RamDiskBlocksDeletedBeforeLazyPersisted", 1);
		}

		/// <summary>
		/// Delete lazy-persist file that has been persisted to disk
		/// Both memory blocks and disk blocks are deleted.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteAfterPersist()
		{
			StartUpCluster(true, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			MakeTestFile(path, BlockSize, true);
			LocatedBlocks locatedBlocks = EnsureFileReplicasOnStorageType(path, StorageType.RamDisk
				);
			// Sleep for a short time to allow the lazy writer thread to do its job
			Sharpen.Thread.Sleep(6 * LazyWriterIntervalSec * 1000);
			// Delete after persist
			client.Delete(path.ToString(), false);
			NUnit.Framework.Assert.IsFalse(fs.Exists(path));
			Assert.AssertThat(VerifyDeletedBlocks(locatedBlocks), IS.Is(true));
			VerifyRamDiskJMXMetric("RamDiskBlocksLazyPersisted", 1);
			VerifyRamDiskJMXMetric("RamDiskBytesLazyPersisted", BlockSize);
		}

		/// <summary>RAM_DISK used/free space</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDfsUsageCreateDelete()
		{
			StartUpCluster(true, 4);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path = new Path("/" + MethodName + ".dat");
			// Get the usage before write BLOCK_SIZE
			long usedBeforeCreate = fs.GetUsed();
			MakeTestFile(path, BlockSize, true);
			long usedAfterCreate = fs.GetUsed();
			Assert.AssertThat(usedAfterCreate, IS.Is((long)BlockSize));
			// Sleep for a short time to allow the lazy writer thread to do its job
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			long usedAfterPersist = fs.GetUsed();
			Assert.AssertThat(usedAfterPersist, IS.Is((long)BlockSize));
			// Delete after persist
			client.Delete(path.ToString(), false);
			long usedAfterDelete = fs.GetUsed();
			Assert.AssertThat(usedBeforeCreate, IS.Is(usedAfterDelete));
		}

		/// <summary>Concurrent read from the same node and verify the contents.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConcurrentRead()
		{
			StartUpCluster(true, 2);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".dat");
			int Seed = unchecked((int)(0xFADED));
			int NumTasks = 5;
			MakeRandomTestFile(path1, BlockSize, true, Seed);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			//Read from multiple clients
			CountDownLatch latch = new CountDownLatch(NumTasks);
			AtomicBoolean testFailed = new AtomicBoolean(false);
			Runnable readerRunnable = new _Runnable_564(this, path1, Seed, testFailed, latch);
			Sharpen.Thread[] threads = new Sharpen.Thread[NumTasks];
			for (int i = 0; i < NumTasks; i++)
			{
				threads[i] = new Sharpen.Thread(readerRunnable);
				threads[i].Start();
			}
			Sharpen.Thread.Sleep(500);
			for (int i_1 = 0; i_1 < NumTasks; i_1++)
			{
				Uninterruptibles.JoinUninterruptibly(threads[i_1]);
			}
			NUnit.Framework.Assert.IsFalse(testFailed.Get());
		}

		private sealed class _Runnable_564 : Runnable
		{
			public _Runnable_564(TestLazyPersistFiles _enclosing, Path path1, int Seed, AtomicBoolean
				 testFailed, CountDownLatch latch)
			{
				this._enclosing = _enclosing;
				this.path1 = path1;
				this.Seed = Seed;
				this.testFailed = testFailed;
				this.latch = latch;
			}

			public void Run()
			{
				try
				{
					NUnit.Framework.Assert.IsTrue(this._enclosing.VerifyReadRandomFile(path1, LazyPersistTestCase
						.BlockSize, Seed));
				}
				catch (Exception e)
				{
					LazyPersistTestCase.Log.Error("readerRunnable error", e);
					testFailed.Set(true);
				}
				finally
				{
					latch.CountDown();
				}
			}

			private readonly TestLazyPersistFiles _enclosing;

			private readonly Path path1;

			private readonly int Seed;

			private readonly AtomicBoolean testFailed;

			private readonly CountDownLatch latch;
		}

		/// <summary>
		/// Concurrent write with eviction
		/// RAM_DISK can hold 9 replicas
		/// 4 threads each write 5 replicas
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConcurrentWrites()
		{
			StartUpCluster(true, 9);
			string MethodName = GenericTestUtils.GetMethodName();
			int Seed = unchecked((int)(0xFADED));
			int NumWriters = 4;
			int NumWriterPaths = 5;
			Path[][] paths = new Path[][] { new Path[NumWriterPaths], new Path[NumWriterPaths
				], new Path[NumWriterPaths], new Path[NumWriterPaths] };
			for (int i = 0; i < NumWriters; i++)
			{
				paths[i] = new Path[NumWriterPaths];
				for (int j = 0; j < NumWriterPaths; j++)
				{
					paths[i][j] = new Path("/" + MethodName + ".Writer" + i + ".File." + j + ".dat");
				}
			}
			CountDownLatch latch = new CountDownLatch(NumWriters);
			AtomicBoolean testFailed = new AtomicBoolean(false);
			ExecutorService executor = Executors.NewFixedThreadPool(ThreadpoolSize);
			for (int i_1 = 0; i_1 < NumWriters; i_1++)
			{
				Runnable writer = new TestLazyPersistFiles.WriterRunnable(this, i_1, paths[i_1], 
					Seed, latch, testFailed);
				executor.Execute(writer);
			}
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			TriggerBlockReport();
			// Stop executor from adding new tasks to finish existing threads in queue
			latch.Await();
			Assert.AssertThat(testFailed.Get(), IS.Is(false));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDnRestartWithSavedReplicas()
		{
			StartUpCluster(true, -1);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".01.dat");
			MakeTestFile(path1, BlockSize, true);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			// Sleep for a short time to allow the lazy writer thread to do its job.
			// However the block replica should not be evicted from RAM_DISK yet.
			Sharpen.Thread.Sleep(3 * LazyWriterIntervalSec * 1000);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			Log.Info("Restarting the DataNode");
			cluster.RestartDataNode(0, true);
			cluster.WaitActive();
			TriggerBlockReport();
			// Ensure that the replica is now on persistent storage.
			EnsureFileReplicasOnStorageType(path1, StorageType.Default);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDnRestartWithUnsavedReplicas()
		{
			StartUpCluster(true, 1);
			FsDatasetTestUtil.StopLazyWriter(cluster.GetDataNodes()[0]);
			string MethodName = GenericTestUtils.GetMethodName();
			Path path1 = new Path("/" + MethodName + ".01.dat");
			MakeTestFile(path1, BlockSize, true);
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
			Log.Info("Restarting the DataNode");
			cluster.RestartDataNode(0, true);
			cluster.WaitActive();
			// Ensure that the replica is still on transient storage.
			EnsureFileReplicasOnStorageType(path1, StorageType.RamDisk);
		}

		internal class WriterRunnable : Runnable
		{
			private readonly int id;

			private readonly Path[] paths;

			private readonly int seed;

			private CountDownLatch latch;

			private AtomicBoolean bFail;

			public WriterRunnable(TestLazyPersistFiles _enclosing, int threadIndex, Path[] paths
				, int seed, CountDownLatch latch, AtomicBoolean bFail)
			{
				this._enclosing = _enclosing;
				this.id = threadIndex;
				this.paths = paths;
				this.seed = seed;
				this.latch = latch;
				this.bFail = bFail;
				System.Console.Out.WriteLine("Creating Writer: " + this.id);
			}

			public virtual void Run()
			{
				System.Console.Out.WriteLine("Writer " + this.id + " starting... ");
				int i = 0;
				try
				{
					for (i = 0; i < this.paths.Length; i++)
					{
						this._enclosing.MakeRandomTestFile(this.paths[i], LazyPersistTestCase.BlockSize, 
							true, this.seed);
					}
				}
				catch (IOException e)
				{
					// eviction may faiL when all blocks are not persisted yet.
					// ensureFileReplicasOnStorageType(paths[i], RAM_DISK);
					this.bFail.Set(true);
					LazyPersistTestCase.Log.Error("Writer exception: writer id:" + this.id + " testfile: "
						 + this.paths[i].ToString() + " " + e);
				}
				finally
				{
					this.latch.CountDown();
				}
			}

			private readonly TestLazyPersistFiles _enclosing;
		}
	}
}
