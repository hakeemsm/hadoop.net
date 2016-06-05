using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestBlockScanner
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestBlockScanner
			));

		[SetUp]
		public virtual void Before()
		{
			BlockScanner.Conf.allowUnitTestSettings = true;
			GenericTestUtils.SetLogLevel(BlockScanner.Log, Level.All);
			GenericTestUtils.SetLogLevel(VolumeScanner.Log, Level.All);
			GenericTestUtils.SetLogLevel(FsVolumeImpl.Log, Level.All);
		}

		private static void DisableBlockScanner(Configuration conf)
		{
			conf.SetLong(DFSConfigKeys.DfsBlockScannerVolumeBytesPerSecond, 0L);
		}

		private class TestContext : IDisposable
		{
			internal readonly int numNameServices;

			internal readonly MiniDFSCluster cluster;

			internal readonly DistributedFileSystem[] dfs;

			internal readonly string[] bpids;

			internal readonly DataNode datanode;

			internal readonly BlockScanner blockScanner;

			internal readonly FsDatasetSpi<FsVolumeSpi> data;

			internal readonly IList<FsVolumeSpi> volumes;

			/// <exception cref="System.Exception"/>
			internal TestContext(Configuration conf, int numNameServices)
			{
				this.numNameServices = numNameServices;
				MiniDFSCluster.Builder bld = new MiniDFSCluster.Builder(conf).NumDataNodes(1).StoragesPerDatanode
					(1);
				if (numNameServices > 1)
				{
					bld.NnTopology(MiniDFSNNTopology.SimpleFederatedTopology(numNameServices));
				}
				cluster = bld.Build();
				cluster.WaitActive();
				dfs = new DistributedFileSystem[numNameServices];
				for (int i = 0; i < numNameServices; i++)
				{
					dfs[i] = cluster.GetFileSystem(i);
				}
				bpids = new string[numNameServices];
				for (int i_1 = 0; i_1 < numNameServices; i_1++)
				{
					bpids[i_1] = cluster.GetNamesystem(i_1).GetBlockPoolId();
				}
				datanode = cluster.GetDataNodes()[0];
				blockScanner = datanode.GetBlockScanner();
				for (int i_2 = 0; i_2 < numNameServices; i_2++)
				{
					dfs[i_2].Mkdirs(new Path("/test"));
				}
				data = datanode.GetFSDataset();
				volumes = data.GetVolumes();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				if (cluster != null)
				{
					for (int i = 0; i < numNameServices; i++)
					{
						dfs[i].Delete(new Path("/test"), true);
					}
					cluster.Shutdown();
				}
			}

			/// <exception cref="System.Exception"/>
			public virtual void CreateFiles(int nsIdx, int numFiles, int length)
			{
				for (int blockIdx = 0; blockIdx < numFiles; blockIdx++)
				{
					DFSTestUtil.CreateFile(dfs[nsIdx], GetPath(blockIdx), length, (short)1, 123L);
				}
			}

			public virtual Path GetPath(int fileIdx)
			{
				return new Path("/test/" + fileIdx);
			}

			/// <exception cref="System.Exception"/>
			public virtual ExtendedBlock GetFileBlock(int nsIdx, int fileIdx)
			{
				return DFSTestUtil.GetFirstBlock(dfs[nsIdx], GetPath(fileIdx));
			}
		}

		/// <summary>
		/// Test iterating through a bunch of blocks in a volume using a volume
		/// iterator.<p/>
		/// We will rewind the iterator when about halfway through the blocks.
		/// </summary>
		/// <param name="numFiles">The number of files to create.</param>
		/// <param name="maxStaleness">The maximum staleness to allow with the iterator.</param>
		/// <exception cref="System.Exception"/>
		private void TestVolumeIteratorImpl(int numFiles, long maxStaleness)
		{
			Configuration conf = new Configuration();
			DisableBlockScanner(conf);
			TestBlockScanner.TestContext ctx = new TestBlockScanner.TestContext(conf, 1);
			ctx.CreateFiles(0, numFiles, 1);
			NUnit.Framework.Assert.AreEqual(1, ctx.volumes.Count);
			FsVolumeSpi volume = ctx.volumes[0];
			ExtendedBlock savedBlock = null;
			ExtendedBlock loadedBlock = null;
			bool testedRewind = false;
			bool testedSave = false;
			bool testedLoad = false;
			int blocksProcessed = 0;
			int savedBlocksProcessed = 0;
			try
			{
				BPOfferService[] bpos = ctx.datanode.GetAllBpOs();
				NUnit.Framework.Assert.AreEqual(1, bpos.Length);
				FsVolumeSpi.BlockIterator iter = volume.NewBlockIterator(ctx.bpids[0], "test");
				NUnit.Framework.Assert.AreEqual(ctx.bpids[0], iter.GetBlockPoolId());
				iter.SetMaxStalenessMs(maxStaleness);
				while (true)
				{
					HashSet<ExtendedBlock> blocks = new HashSet<ExtendedBlock>();
					for (int blockIdx = 0; blockIdx < numFiles; blockIdx++)
					{
						blocks.AddItem(ctx.GetFileBlock(0, blockIdx));
					}
					while (true)
					{
						ExtendedBlock block = iter.NextBlock();
						if (block == null)
						{
							break;
						}
						blocksProcessed++;
						Log.Info("BlockIterator for {} found block {}, blocksProcessed = {}", volume, block
							, blocksProcessed);
						if (testedSave && (savedBlock == null))
						{
							savedBlock = block;
						}
						if (testedLoad && (loadedBlock == null))
						{
							loadedBlock = block;
							// The block that we get back right after loading the iterator
							// should be the same block we got back right after saving
							// the iterator.
							NUnit.Framework.Assert.AreEqual(savedBlock, loadedBlock);
						}
						bool blockRemoved = blocks.Remove(block);
						NUnit.Framework.Assert.IsTrue("Found unknown block " + block, blockRemoved);
						if (blocksProcessed > (numFiles / 3))
						{
							if (!testedSave)
							{
								Log.Info("Processed {} blocks out of {}.  Saving iterator.", blocksProcessed, numFiles
									);
								iter.Save();
								testedSave = true;
								savedBlocksProcessed = blocksProcessed;
							}
						}
						if (blocksProcessed > (numFiles / 2))
						{
							if (!testedRewind)
							{
								Log.Info("Processed {} blocks out of {}.  Rewinding iterator.", blocksProcessed, 
									numFiles);
								iter.Rewind();
								break;
							}
						}
						if (blocksProcessed > ((2 * numFiles) / 3))
						{
							if (!testedLoad)
							{
								Log.Info("Processed {} blocks out of {}.  Loading iterator.", blocksProcessed, numFiles
									);
								iter = volume.LoadBlockIterator(ctx.bpids[0], "test");
								iter.SetMaxStalenessMs(maxStaleness);
								break;
							}
						}
					}
					if (!testedRewind)
					{
						testedRewind = true;
						blocksProcessed = 0;
						Log.Info("Starting again at the beginning...");
						continue;
					}
					if (!testedLoad)
					{
						testedLoad = true;
						blocksProcessed = savedBlocksProcessed;
						Log.Info("Starting again at the load point...");
						continue;
					}
					NUnit.Framework.Assert.AreEqual(numFiles, blocksProcessed);
					break;
				}
			}
			finally
			{
				ctx.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestVolumeIteratorWithoutCaching()
		{
			TestVolumeIteratorImpl(5, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestVolumeIteratorWithCaching()
		{
			TestVolumeIteratorImpl(600, 100);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDisableVolumeScanner()
		{
			Configuration conf = new Configuration();
			DisableBlockScanner(conf);
			TestBlockScanner.TestContext ctx = new TestBlockScanner.TestContext(conf, 1);
			try
			{
				NUnit.Framework.Assert.IsFalse(ctx.datanode.GetBlockScanner().IsEnabled());
			}
			finally
			{
				ctx.Close();
			}
		}

		public class TestScanResultHandler : VolumeScanner.ScanResultHandler
		{
			internal class Info
			{
				internal bool shouldRun = false;

				internal readonly ICollection<ExtendedBlock> badBlocks = new HashSet<ExtendedBlock
					>();

				internal readonly ICollection<ExtendedBlock> goodBlocks = new HashSet<ExtendedBlock
					>();

				internal long blocksScanned = 0;

				internal Semaphore sem = null;

				public override string ToString()
				{
					StringBuilder bld = new StringBuilder();
					bld.Append("ScanResultHandler.Info{");
					bld.Append("shouldRun=").Append(shouldRun).Append(", ");
					bld.Append("blocksScanned=").Append(blocksScanned).Append(", ");
					bld.Append("sem#availablePermits=").Append(sem.AvailablePermits()).Append(", ");
					bld.Append("badBlocks=").Append(badBlocks).Append(", ");
					bld.Append("goodBlocks=").Append(goodBlocks);
					bld.Append("}");
					return bld.ToString();
				}
			}

			private VolumeScanner scanner;

			internal static readonly ConcurrentHashMap<string, TestBlockScanner.TestScanResultHandler.Info
				> infos = new ConcurrentHashMap<string, TestBlockScanner.TestScanResultHandler.Info
				>();

			internal static TestBlockScanner.TestScanResultHandler.Info GetInfo(FsVolumeSpi volume
				)
			{
				TestBlockScanner.TestScanResultHandler.Info newInfo = new TestBlockScanner.TestScanResultHandler.Info
					();
				TestBlockScanner.TestScanResultHandler.Info prevInfo = infos.PutIfAbsent(volume.GetStorageID
					(), newInfo);
				return prevInfo == null ? newInfo : prevInfo;
			}

			public override void Setup(VolumeScanner scanner)
			{
				this.scanner = scanner;
				TestBlockScanner.TestScanResultHandler.Info info = GetInfo(scanner.volume);
				Log.Info("about to start scanning.");
				lock (info)
				{
					while (!info.shouldRun)
					{
						try
						{
							Sharpen.Runtime.Wait(info);
						}
						catch (Exception)
						{
						}
					}
				}
				Log.Info("starting scanning.");
			}

			public override void Handle(ExtendedBlock block, IOException e)
			{
				Log.Info("handling block {} (exception {})", block, e);
				TestBlockScanner.TestScanResultHandler.Info info = GetInfo(scanner.volume);
				Semaphore sem;
				lock (info)
				{
					sem = info.sem;
				}
				if (sem != null)
				{
					try
					{
						sem.WaitOne();
					}
					catch (Exception)
					{
						throw new RuntimeException("interrupted");
					}
				}
				lock (info)
				{
					if (!info.shouldRun)
					{
						throw new RuntimeException("stopping volumescanner thread.");
					}
					if (e == null)
					{
						info.goodBlocks.AddItem(block);
					}
					else
					{
						info.badBlocks.AddItem(block);
					}
					info.blocksScanned++;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestScanAllBlocksImpl(bool rescan)
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockScannerVolumeBytesPerSecond, 1048576L);
			if (rescan)
			{
				conf.SetLong(BlockScanner.Conf.InternalDfsDatanodeScanPeriodMs, 100L);
			}
			else
			{
				conf.SetLong(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, 100L);
			}
			conf.Set(BlockScanner.Conf.InternalVolumeScannerScanResultHandler, typeof(TestBlockScanner.TestScanResultHandler
				).FullName);
			TestBlockScanner.TestContext ctx = new TestBlockScanner.TestContext(conf, 1);
			int NumExpectedBlocks = 10;
			ctx.CreateFiles(0, NumExpectedBlocks, 1);
			ICollection<ExtendedBlock> expectedBlocks = new HashSet<ExtendedBlock>();
			for (int i = 0; i < NumExpectedBlocks; i++)
			{
				expectedBlocks.AddItem(ctx.GetFileBlock(0, i));
			}
			TestBlockScanner.TestScanResultHandler.Info info = TestBlockScanner.TestScanResultHandler
				.GetInfo(ctx.volumes[0]);
			lock (info)
			{
				info.shouldRun = true;
				Sharpen.Runtime.Notify(info);
			}
			GenericTestUtils.WaitFor(new _Supplier_367(ctx, expectedBlocks, rescan, NumExpectedBlocks
				), 10, 60000);
			if (!rescan)
			{
				lock (info)
				{
					NUnit.Framework.Assert.AreEqual(NumExpectedBlocks, info.blocksScanned);
				}
				VolumeScanner.Statistics stats = ctx.blockScanner.GetVolumeStats(ctx.volumes[0].GetStorageID
					());
				NUnit.Framework.Assert.AreEqual(5 * NumExpectedBlocks, stats.bytesScannedInPastHour
					);
				NUnit.Framework.Assert.AreEqual(NumExpectedBlocks, stats.blocksScannedSinceRestart
					);
				NUnit.Framework.Assert.AreEqual(NumExpectedBlocks, stats.blocksScannedInCurrentPeriod
					);
				NUnit.Framework.Assert.AreEqual(0, stats.scanErrorsSinceRestart);
				NUnit.Framework.Assert.AreEqual(1, stats.scansSinceRestart);
			}
			ctx.Close();
		}

		private sealed class _Supplier_367 : Supplier<bool>
		{
			public _Supplier_367(TestBlockScanner.TestContext ctx, ICollection<ExtendedBlock>
				 expectedBlocks, bool rescan, int NumExpectedBlocks)
			{
				this.ctx = ctx;
				this.expectedBlocks = expectedBlocks;
				this.rescan = rescan;
				this.NumExpectedBlocks = NumExpectedBlocks;
			}

			public bool Get()
			{
				TestBlockScanner.TestScanResultHandler.Info info = TestBlockScanner.TestScanResultHandler
					.GetInfo(ctx.volumes[0]);
				int numFoundBlocks = 0;
				StringBuilder foundBlocksBld = new StringBuilder();
				string prefix = string.Empty;
				lock (info)
				{
					foreach (ExtendedBlock block in info.goodBlocks)
					{
						NUnit.Framework.Assert.IsTrue(expectedBlocks.Contains(block));
						numFoundBlocks++;
						foundBlocksBld.Append(prefix).Append(block);
						prefix = ", ";
					}
					TestBlockScanner.Log.Info("numFoundBlocks = {}.  blocksScanned = {}. Found blocks {}"
						, numFoundBlocks, info.blocksScanned, foundBlocksBld.ToString());
					if (rescan)
					{
						return (numFoundBlocks == NumExpectedBlocks) && (info.blocksScanned >= 2 * NumExpectedBlocks
							);
					}
					else
					{
						return numFoundBlocks == NumExpectedBlocks;
					}
				}
			}

			private readonly TestBlockScanner.TestContext ctx;

			private readonly ICollection<ExtendedBlock> expectedBlocks;

			private readonly bool rescan;

			private readonly int NumExpectedBlocks;
		}

		/// <summary>Test scanning all blocks.</summary>
		/// <remarks>
		/// Test scanning all blocks.  Set the scan period high enough that
		/// we shouldn't rescan any block during this test.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestScanAllBlocksNoRescan()
		{
			TestScanAllBlocksImpl(false);
		}

		/// <summary>Test scanning all blocks.</summary>
		/// <remarks>
		/// Test scanning all blocks.  Set the scan period high enough that
		/// we should rescan all blocks at least twice during this test.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestScanAllBlocksWithRescan()
		{
			TestScanAllBlocksImpl(true);
		}

		/// <summary>Test that we don't scan too many blocks per second.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestScanRateLimit()
		{
			Configuration conf = new Configuration();
			// Limit scan bytes per second dramatically
			conf.SetLong(DFSConfigKeys.DfsBlockScannerVolumeBytesPerSecond, 4096L);
			// Scan continuously
			conf.SetLong(BlockScanner.Conf.InternalDfsDatanodeScanPeriodMs, 1L);
			conf.Set(BlockScanner.Conf.InternalVolumeScannerScanResultHandler, typeof(TestBlockScanner.TestScanResultHandler
				).FullName);
			TestBlockScanner.TestContext ctx = new TestBlockScanner.TestContext(conf, 1);
			int NumExpectedBlocks = 5;
			ctx.CreateFiles(0, NumExpectedBlocks, 4096);
			TestBlockScanner.TestScanResultHandler.Info info = TestBlockScanner.TestScanResultHandler
				.GetInfo(ctx.volumes[0]);
			long startMs = Time.MonotonicNow();
			lock (info)
			{
				info.shouldRun = true;
				Sharpen.Runtime.Notify(info);
			}
			GenericTestUtils.WaitFor(new _Supplier_448(info), 1, 30000);
			Sharpen.Thread.Sleep(2000);
			lock (info)
			{
				long endMs = Time.MonotonicNow();
				// Should scan no more than one block a second.
				long seconds = ((endMs + 999 - startMs) / 1000);
				long maxBlocksScanned = seconds * 1;
				NUnit.Framework.Assert.IsTrue("The number of blocks scanned is too large.  Scanned "
					 + info.blocksScanned + " blocks; only expected to scan at most " + maxBlocksScanned
					 + " in " + seconds + " seconds.", info.blocksScanned <= maxBlocksScanned);
			}
			ctx.Close();
		}

		private sealed class _Supplier_448 : Supplier<bool>
		{
			public _Supplier_448(TestBlockScanner.TestScanResultHandler.Info info)
			{
				this.info = info;
			}

			public bool Get()
			{
				lock (info)
				{
					return info.blocksScanned > 0;
				}
			}

			private readonly TestBlockScanner.TestScanResultHandler.Info info;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCorruptBlockHandling()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, 100L);
			conf.Set(BlockScanner.Conf.InternalVolumeScannerScanResultHandler, typeof(TestBlockScanner.TestScanResultHandler
				).FullName);
			TestBlockScanner.TestContext ctx = new TestBlockScanner.TestContext(conf, 1);
			int NumExpectedBlocks = 5;
			int CorruptIndex = 3;
			ctx.CreateFiles(0, NumExpectedBlocks, 4);
			ExtendedBlock badBlock = ctx.GetFileBlock(0, CorruptIndex);
			ctx.cluster.CorruptBlockOnDataNodes(badBlock);
			TestBlockScanner.TestScanResultHandler.Info info = TestBlockScanner.TestScanResultHandler
				.GetInfo(ctx.volumes[0]);
			lock (info)
			{
				info.shouldRun = true;
				Sharpen.Runtime.Notify(info);
			}
			GenericTestUtils.WaitFor(new _Supplier_488(info, NumExpectedBlocks), 3, 30000);
			lock (info)
			{
				NUnit.Framework.Assert.IsTrue(info.badBlocks.Contains(badBlock));
				for (int i = 0; i < NumExpectedBlocks; i++)
				{
					if (i != CorruptIndex)
					{
						ExtendedBlock block = ctx.GetFileBlock(0, i);
						NUnit.Framework.Assert.IsTrue(info.goodBlocks.Contains(block));
					}
				}
			}
			ctx.Close();
		}

		private sealed class _Supplier_488 : Supplier<bool>
		{
			public _Supplier_488(TestBlockScanner.TestScanResultHandler.Info info, int NumExpectedBlocks
				)
			{
				this.info = info;
				this.NumExpectedBlocks = NumExpectedBlocks;
			}

			public bool Get()
			{
				lock (info)
				{
					return info.blocksScanned == NumExpectedBlocks;
				}
			}

			private readonly TestBlockScanner.TestScanResultHandler.Info info;

			private readonly int NumExpectedBlocks;
		}

		/// <summary>
		/// Test that we save the scan cursor when shutting down the datanode, and
		/// restart scanning from there when the datanode is restarted.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDatanodeCursor()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, 100L);
			conf.Set(BlockScanner.Conf.InternalVolumeScannerScanResultHandler, typeof(TestBlockScanner.TestScanResultHandler
				).FullName);
			conf.SetLong(BlockScanner.Conf.InternalDfsBlockScannerCursorSaveIntervalMs, 0L);
			TestBlockScanner.TestContext ctx = new TestBlockScanner.TestContext(conf, 1);
			int NumExpectedBlocks = 10;
			ctx.CreateFiles(0, NumExpectedBlocks, 1);
			TestBlockScanner.TestScanResultHandler.Info info = TestBlockScanner.TestScanResultHandler
				.GetInfo(ctx.volumes[0]);
			lock (info)
			{
				info.sem = Sharpen.Extensions.CreateSemaphore(5);
				info.shouldRun = true;
				Sharpen.Runtime.Notify(info);
			}
			// Scan the first 5 blocks
			GenericTestUtils.WaitFor(new _Supplier_530(info), 3, 30000);
			lock (info)
			{
				NUnit.Framework.Assert.AreEqual(5, info.goodBlocks.Count);
				NUnit.Framework.Assert.AreEqual(5, info.blocksScanned);
				info.shouldRun = false;
			}
			ctx.datanode.Shutdown();
			string vPath = ctx.volumes[0].GetBasePath();
			FilePath cursorPath = new FilePath(new FilePath(new FilePath(vPath, "current"), ctx
				.bpids[0]), "scanner.cursor");
			NUnit.Framework.Assert.IsTrue("Failed to find cursor save file in " + cursorPath.
				GetAbsolutePath(), cursorPath.Exists());
			ICollection<ExtendedBlock> prevGoodBlocks = new HashSet<ExtendedBlock>();
			lock (info)
			{
				info.sem = Sharpen.Extensions.CreateSemaphore(4);
				Sharpen.Collections.AddAll(prevGoodBlocks, info.goodBlocks);
				info.goodBlocks.Clear();
			}
			// The block that we were scanning when we shut down the DN won't get
			// recorded.
			// After restarting the datanode, we should scan the next 4 blocks.
			ctx.cluster.RestartDataNode(0);
			lock (info)
			{
				info.shouldRun = true;
				Sharpen.Runtime.Notify(info);
			}
			GenericTestUtils.WaitFor(new _Supplier_564(info), 3, 30000);
			lock (info)
			{
				NUnit.Framework.Assert.AreEqual(4, info.goodBlocks.Count);
				Sharpen.Collections.AddAll(info.goodBlocks, prevGoodBlocks);
				NUnit.Framework.Assert.AreEqual(9, info.goodBlocks.Count);
				NUnit.Framework.Assert.AreEqual(9, info.blocksScanned);
			}
			ctx.datanode.Shutdown();
			// After restarting the datanode, we should not scan any more blocks.
			// This is because we reached the end of the block pool earlier, and
			// the scan period is much, much longer than the test time.
			lock (info)
			{
				info.sem = null;
				info.shouldRun = false;
				info.goodBlocks.Clear();
			}
			ctx.cluster.RestartDataNode(0);
			lock (info)
			{
				info.shouldRun = true;
				Sharpen.Runtime.Notify(info);
			}
			Sharpen.Thread.Sleep(3000);
			lock (info)
			{
				NUnit.Framework.Assert.IsTrue(info.goodBlocks.IsEmpty());
			}
			ctx.Close();
		}

		private sealed class _Supplier_530 : Supplier<bool>
		{
			public _Supplier_530(TestBlockScanner.TestScanResultHandler.Info info)
			{
				this.info = info;
			}

			public bool Get()
			{
				lock (info)
				{
					return info.blocksScanned == 5;
				}
			}

			private readonly TestBlockScanner.TestScanResultHandler.Info info;
		}

		private sealed class _Supplier_564 : Supplier<bool>
		{
			public _Supplier_564(TestBlockScanner.TestScanResultHandler.Info info)
			{
				this.info = info;
			}

			public bool Get()
			{
				lock (info)
				{
					if (info.blocksScanned != 9)
					{
						TestBlockScanner.Log.Info("Waiting for blocksScanned to reach 9.  It is at {}", info
							.blocksScanned);
					}
					return info.blocksScanned == 9;
				}
			}

			private readonly TestBlockScanner.TestScanResultHandler.Info info;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMultipleBlockPoolScanning()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, 100L);
			conf.Set(BlockScanner.Conf.InternalVolumeScannerScanResultHandler, typeof(TestBlockScanner.TestScanResultHandler
				).FullName);
			TestBlockScanner.TestContext ctx = new TestBlockScanner.TestContext(conf, 3);
			// We scan 5 bytes per file (1 byte in file, 4 bytes of checksum)
			int BytesScannedPerFile = 5;
			int[] NumFiles = new int[] { 1, 5, 10 };
			int TotalFiles = 0;
			for (int i = 0; i < NumFiles.Length; i++)
			{
				TotalFiles += NumFiles[i];
			}
			ctx.CreateFiles(0, NumFiles[0], 1);
			ctx.CreateFiles(0, NumFiles[1], 1);
			ctx.CreateFiles(0, NumFiles[2], 1);
			// start scanning
			TestBlockScanner.TestScanResultHandler.Info info = TestBlockScanner.TestScanResultHandler
				.GetInfo(ctx.volumes[0]);
			lock (info)
			{
				info.shouldRun = true;
				Sharpen.Runtime.Notify(info);
			}
			// Wait for all the block pools to be scanned.
			GenericTestUtils.WaitFor(new _Supplier_632(info, ctx), 3, 30000);
			VolumeScanner.Statistics stats = ctx.blockScanner.GetVolumeStats(ctx.volumes[0].GetStorageID
				());
			NUnit.Framework.Assert.AreEqual(TotalFiles, stats.blocksScannedSinceRestart);
			NUnit.Framework.Assert.AreEqual(BytesScannedPerFile * TotalFiles, stats.bytesScannedInPastHour
				);
			ctx.Close();
		}

		private sealed class _Supplier_632 : Supplier<bool>
		{
			public _Supplier_632(TestBlockScanner.TestScanResultHandler.Info info, TestBlockScanner.TestContext
				 ctx)
			{
				this.info = info;
				this.ctx = ctx;
			}

			public bool Get()
			{
				lock (info)
				{
					VolumeScanner.Statistics stats = ctx.blockScanner.GetVolumeStats(ctx.volumes[0].GetStorageID
						());
					if (stats.scansSinceRestart < 3)
					{
						TestBlockScanner.Log.Info("Waiting for scansSinceRestart to reach 3 (it is {})", 
							stats.scansSinceRestart);
						return false;
					}
					if (!stats.eof)
					{
						TestBlockScanner.Log.Info("Waiting for eof.");
						return false;
					}
					return true;
				}
			}

			private readonly TestBlockScanner.TestScanResultHandler.Info info;

			private readonly TestBlockScanner.TestContext ctx;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNextSorted()
		{
			IList<string> arr = new List<string>();
			arr.AddItem("1");
			arr.AddItem("3");
			arr.AddItem("5");
			arr.AddItem("7");
			NUnit.Framework.Assert.AreEqual("3", FsVolumeImpl.NextSorted(arr, "2"));
			NUnit.Framework.Assert.AreEqual("3", FsVolumeImpl.NextSorted(arr, "1"));
			NUnit.Framework.Assert.AreEqual("1", FsVolumeImpl.NextSorted(arr, string.Empty));
			NUnit.Framework.Assert.AreEqual("1", FsVolumeImpl.NextSorted(arr, null));
			NUnit.Framework.Assert.AreEqual(null, FsVolumeImpl.NextSorted(arr, "9"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCalculateNeededBytesPerSec()
		{
			// If we didn't check anything the last hour, we should scan now.
			NUnit.Framework.Assert.IsTrue(VolumeScanner.CalculateShouldScan("test", 100, 0, 0
				, 60));
			// If, on average, we checked 101 bytes/s checked during the last hour,
			// stop checking now.
			NUnit.Framework.Assert.IsFalse(VolumeScanner.CalculateShouldScan("test", 100, 101
				 * 3600, 1000, 5000));
			// Target is 1 byte / s, but we didn't scan anything in the last minute.
			// Should scan now.
			NUnit.Framework.Assert.IsTrue(VolumeScanner.CalculateShouldScan("test", 1, 3540, 
				0, 60));
			// Target is 1000000 byte / s, but we didn't scan anything in the last
			// minute.  Should scan now.
			NUnit.Framework.Assert.IsTrue(VolumeScanner.CalculateShouldScan("test", 100000L, 
				354000000L, 0, 60));
			NUnit.Framework.Assert.IsFalse(VolumeScanner.CalculateShouldScan("test", 100000L, 
				365000000L, 0, 60));
		}

		/// <summary>
		/// Test that we can mark certain blocks as suspect, and get them quickly
		/// rescanned that way.
		/// </summary>
		/// <remarks>
		/// Test that we can mark certain blocks as suspect, and get them quickly
		/// rescanned that way.  See HDFS-7686 and HDFS-7548.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestMarkSuspectBlock()
		{
			Configuration conf = new Configuration();
			// Set a really long scan period.
			conf.SetLong(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, 100L);
			conf.Set(BlockScanner.Conf.InternalVolumeScannerScanResultHandler, typeof(TestBlockScanner.TestScanResultHandler
				).FullName);
			conf.SetLong(BlockScanner.Conf.InternalDfsBlockScannerCursorSaveIntervalMs, 0L);
			TestBlockScanner.TestContext ctx = new TestBlockScanner.TestContext(conf, 1);
			int NumExpectedBlocks = 10;
			ctx.CreateFiles(0, NumExpectedBlocks, 1);
			TestBlockScanner.TestScanResultHandler.Info info = TestBlockScanner.TestScanResultHandler
				.GetInfo(ctx.volumes[0]);
			string storageID = ctx.datanode.GetFSDataset().GetVolumes()[0].GetStorageID();
			lock (info)
			{
				info.sem = Sharpen.Extensions.CreateSemaphore(4);
				info.shouldRun = true;
				Sharpen.Runtime.Notify(info);
			}
			// Scan the first 4 blocks
			Log.Info("Waiting for the first 4 blocks to be scanned.");
			GenericTestUtils.WaitFor(new _Supplier_725(info), 50, 30000);
			// We should have scanned 4 blocks
			lock (info)
			{
				NUnit.Framework.Assert.AreEqual("Expected 4 good blocks.", 4, info.goodBlocks.Count
					);
				info.goodBlocks.Clear();
				NUnit.Framework.Assert.AreEqual("Expected 4 blocksScanned", 4, info.blocksScanned
					);
				NUnit.Framework.Assert.AreEqual("Did not expect bad blocks.", 0, info.badBlocks.Count
					);
				info.blocksScanned = 0;
			}
			ExtendedBlock first = ctx.GetFileBlock(0, 0);
			ctx.datanode.GetBlockScanner().MarkSuspectBlock(storageID, first);
			// When we increment the semaphore, the TestScanResultHandler will finish
			// adding the block that it was scanning previously (the 5th block).
			// We increment the semaphore twice so that the handler will also
			// get a chance to see the suspect block which we just requested the
			// VolumeScanner to process.
			info.sem.Release(2);
			Log.Info("Waiting for 2 more blocks to be scanned.");
			GenericTestUtils.WaitFor(new _Supplier_758(info), 50, 30000);
			lock (info)
			{
				NUnit.Framework.Assert.IsTrue("Expected block " + first + " to have been scanned."
					, info.goodBlocks.Contains(first));
				NUnit.Framework.Assert.AreEqual(2, info.goodBlocks.Count);
				info.goodBlocks.Clear();
				NUnit.Framework.Assert.AreEqual("Did not expect bad blocks.", 0, info.badBlocks.Count
					);
				NUnit.Framework.Assert.AreEqual(2, info.blocksScanned);
				info.blocksScanned = 0;
			}
			// Re-mark the same block as suspect.
			ctx.datanode.GetBlockScanner().MarkSuspectBlock(storageID, first);
			info.sem.Release(10);
			Log.Info("Waiting for 5 more blocks to be scanned.");
			GenericTestUtils.WaitFor(new _Supplier_788(info), 50, 30000);
			lock (info)
			{
				NUnit.Framework.Assert.AreEqual(5, info.goodBlocks.Count);
				NUnit.Framework.Assert.AreEqual(0, info.badBlocks.Count);
				NUnit.Framework.Assert.AreEqual(5, info.blocksScanned);
				// We should not have rescanned the "suspect block",
				// because it was recently rescanned by the suspect block system.
				// This is a test of the "suspect block" rate limiting.
				NUnit.Framework.Assert.IsFalse("We should not " + "have rescanned block " + first
					 + ", because it should have been " + "in recentSuspectBlocks.", info.goodBlocks
					.Contains(first));
				info.blocksScanned = 0;
			}
		}

		private sealed class _Supplier_725 : Supplier<bool>
		{
			public _Supplier_725(TestBlockScanner.TestScanResultHandler.Info info)
			{
				this.info = info;
			}

			public bool Get()
			{
				lock (info)
				{
					if (info.blocksScanned >= 4)
					{
						TestBlockScanner.Log.Info("info = {}.  blockScanned has now reached 4.", info);
						return true;
					}
					else
					{
						TestBlockScanner.Log.Info("info = {}.  Waiting for blockScanned to reach 4.", info
							);
						return false;
					}
				}
			}

			private readonly TestBlockScanner.TestScanResultHandler.Info info;
		}

		private sealed class _Supplier_758 : Supplier<bool>
		{
			public _Supplier_758(TestBlockScanner.TestScanResultHandler.Info info)
			{
				this.info = info;
			}

			public bool Get()
			{
				lock (info)
				{
					if (info.blocksScanned >= 2)
					{
						TestBlockScanner.Log.Info("info = {}.  blockScanned has now reached 2.", info);
						return true;
					}
					else
					{
						TestBlockScanner.Log.Info("info = {}.  Waiting for blockScanned to reach 2.", info
							);
						return false;
					}
				}
			}

			private readonly TestBlockScanner.TestScanResultHandler.Info info;
		}

		private sealed class _Supplier_788 : Supplier<bool>
		{
			public _Supplier_788(TestBlockScanner.TestScanResultHandler.Info info)
			{
				this.info = info;
			}

			public bool Get()
			{
				lock (info)
				{
					if (info.blocksScanned >= 5)
					{
						TestBlockScanner.Log.Info("info = {}.  blockScanned has now reached 5.", info);
						return true;
					}
					else
					{
						TestBlockScanner.Log.Info("info = {}.  Waiting for blockScanned to reach 5.", info
							);
						return false;
					}
				}
			}

			private readonly TestBlockScanner.TestScanResultHandler.Info info;
		}
	}
}
