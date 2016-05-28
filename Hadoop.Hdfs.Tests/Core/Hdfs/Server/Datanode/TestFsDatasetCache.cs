using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestFsDatasetCache
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestFsDatasetCache));

		internal const long CacheCapacity = 64 * 1024;

		private static readonly long PageSize = NativeIO.POSIX.GetCacheManipulator().GetOperatingSystemPageSize
			();

		private static readonly long BlockSize = PageSize;

		private static Configuration conf;

		private static MiniDFSCluster cluster = null;

		private static FileSystem fs;

		private static NameNode nn;

		private static FSImage fsImage;

		private static DataNode dn;

		private static FsDatasetSpi<object> fsd;

		private static DatanodeProtocolClientSideTranslatorPB spyNN;

		private static readonly FsDatasetCache.PageRounder rounder = new FsDatasetCache.PageRounder
			();

		private static NativeIO.POSIX.CacheManipulator prevCacheManipulator;

		static TestFsDatasetCache()
		{
			// Most Linux installs allow a default of 64KB locked memory
			// mlock always locks the entire page. So we don't need to deal with this
			// rounding, use the OS page size for the block size.
			LogManager.GetLogger(typeof(FsDatasetCache)).SetLevel(Level.Debug);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsNamenodePathBasedCacheRefreshIntervalMs, 100);
			conf.SetLong(DFSConfigKeys.DfsCachereportIntervalMsecKey, 500);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetLong(DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, CacheCapacity);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			prevCacheManipulator = NativeIO.POSIX.GetCacheManipulator();
			NativeIO.POSIX.SetCacheManipulator(new NativeIO.POSIX.NoMlockCacheManipulator());
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			nn = cluster.GetNameNode();
			fsImage = nn.GetFSImage();
			dn = cluster.GetDataNodes()[0];
			fsd = dn.GetFSDataset();
			spyNN = DataNodeTestUtils.SpyOnBposToNN(dn, nn);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			// Verify that each test uncached whatever it cached.  This cleanup is
			// required so that file descriptors are not leaked across tests.
			DFSTestUtil.VerifyExpectedCacheUsage(0, 0, fsd);
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			// Restore the original CacheManipulator
			NativeIO.POSIX.SetCacheManipulator(prevCacheManipulator);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void SetHeartbeatResponse(DatanodeCommand[] cmds)
		{
			NNHAStatusHeartbeat ha = new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState
				.Active, fsImage.GetLastAppliedOrWrittenTxId());
			HeartbeatResponse response = new HeartbeatResponse(cmds, ha, null);
			Org.Mockito.Mockito.DoReturn(response).When(spyNN).SendHeartbeat((DatanodeRegistration
				)Matchers.Any(), (StorageReport[])Matchers.Any(), Matchers.AnyLong(), Matchers.AnyLong
				(), Matchers.AnyInt(), Matchers.AnyInt(), Matchers.AnyInt(), (VolumeFailureSummary
				)Matchers.Any());
		}

		private static DatanodeCommand[] CacheBlock(HdfsBlockLocation loc)
		{
			return CacheBlocks(new HdfsBlockLocation[] { loc });
		}

		private static DatanodeCommand[] CacheBlocks(HdfsBlockLocation[] locs)
		{
			return new DatanodeCommand[] { GetResponse(locs, DatanodeProtocol.DnaCache) };
		}

		private static DatanodeCommand[] UncacheBlock(HdfsBlockLocation loc)
		{
			return UncacheBlocks(new HdfsBlockLocation[] { loc });
		}

		private static DatanodeCommand[] UncacheBlocks(HdfsBlockLocation[] locs)
		{
			return new DatanodeCommand[] { GetResponse(locs, DatanodeProtocol.DnaUncache) };
		}

		/// <summary>Creates a cache or uncache DatanodeCommand from an array of locations</summary>
		private static DatanodeCommand GetResponse(HdfsBlockLocation[] locs, int action)
		{
			string bpid = locs[0].GetLocatedBlock().GetBlock().GetBlockPoolId();
			long[] blocks = new long[locs.Length];
			for (int i = 0; i < locs.Length; i++)
			{
				blocks[i] = locs[i].GetLocatedBlock().GetBlock().GetBlockId();
			}
			return new BlockIdCommand(action, bpid, blocks);
		}

		/// <exception cref="System.Exception"/>
		private static long[] GetBlockSizes(HdfsBlockLocation[] locs)
		{
			long[] sizes = new long[locs.Length];
			for (int i = 0; i < locs.Length; i++)
			{
				HdfsBlockLocation loc = locs[i];
				string bpid = loc.GetLocatedBlock().GetBlock().GetBlockPoolId();
				Block block = loc.GetLocatedBlock().GetBlock().GetLocalBlock();
				ExtendedBlock extBlock = new ExtendedBlock(bpid, block);
				FileInputStream blockInputStream = null;
				FileChannel blockChannel = null;
				try
				{
					blockInputStream = (FileInputStream)fsd.GetBlockInputStream(extBlock, 0);
					blockChannel = blockInputStream.GetChannel();
					sizes[i] = blockChannel.Size();
				}
				finally
				{
					IOUtils.Cleanup(Log, blockChannel, blockInputStream);
				}
			}
			return sizes;
		}

		/// <exception cref="System.Exception"/>
		private void TestCacheAndUncacheBlock()
		{
			Log.Info("beginning testCacheAndUncacheBlock");
			int NumBlocks = 5;
			DFSTestUtil.VerifyExpectedCacheUsage(0, 0, fsd);
			NUnit.Framework.Assert.AreEqual(0, fsd.GetNumBlocksCached());
			// Write a test file
			Path testFile = new Path("/testCacheBlock");
			long testFileLen = BlockSize * NumBlocks;
			DFSTestUtil.CreateFile(fs, testFile, testFileLen, (short)1, unchecked((long)(0xABBAl
				)));
			// Get the details of the written file
			HdfsBlockLocation[] locs = (HdfsBlockLocation[])fs.GetFileBlockLocations(testFile
				, 0, testFileLen);
			NUnit.Framework.Assert.AreEqual("Unexpected number of blocks", NumBlocks, locs.Length
				);
			long[] blockSizes = GetBlockSizes(locs);
			// Check initial state
			long cacheCapacity = fsd.GetCacheCapacity();
			long cacheUsed = fsd.GetCacheUsed();
			long current = 0;
			NUnit.Framework.Assert.AreEqual("Unexpected cache capacity", CacheCapacity, cacheCapacity
				);
			NUnit.Framework.Assert.AreEqual("Unexpected amount of cache used", current, cacheUsed
				);
			MetricsRecordBuilder dnMetrics;
			long numCacheCommands = 0;
			long numUncacheCommands = 0;
			// Cache each block in succession, checking each time
			for (int i = 0; i < NumBlocks; i++)
			{
				SetHeartbeatResponse(CacheBlock(locs[i]));
				current = DFSTestUtil.VerifyExpectedCacheUsage(current + blockSizes[i], i + 1, fsd
					);
				dnMetrics = MetricsAsserts.GetMetrics(dn.GetMetrics().Name());
				long cmds = MetricsAsserts.GetLongCounter("BlocksCached", dnMetrics);
				NUnit.Framework.Assert.IsTrue("Expected more cache requests from the NN (" + cmds
					 + " <= " + numCacheCommands + ")", cmds > numCacheCommands);
				numCacheCommands = cmds;
			}
			// Uncache each block in succession, again checking each time
			for (int i_1 = 0; i_1 < NumBlocks; i_1++)
			{
				SetHeartbeatResponse(UncacheBlock(locs[i_1]));
				current = DFSTestUtil.VerifyExpectedCacheUsage(current - blockSizes[i_1], NumBlocks
					 - 1 - i_1, fsd);
				dnMetrics = MetricsAsserts.GetMetrics(dn.GetMetrics().Name());
				long cmds = MetricsAsserts.GetLongCounter("BlocksUncached", dnMetrics);
				NUnit.Framework.Assert.IsTrue("Expected more uncache requests from the NN", cmds 
					> numUncacheCommands);
				numUncacheCommands = cmds;
			}
			Log.Info("finishing testCacheAndUncacheBlock");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCacheAndUncacheBlockSimple()
		{
			TestCacheAndUncacheBlock();
		}

		/// <summary>
		/// Run testCacheAndUncacheBlock with some failures injected into the mlock
		/// call.
		/// </summary>
		/// <remarks>
		/// Run testCacheAndUncacheBlock with some failures injected into the mlock
		/// call.  This tests the ability of the NameNode to resend commands.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestCacheAndUncacheBlockWithRetries()
		{
			// We don't have to save the previous cacheManipulator
			// because it will be reinstalled by the @After function.
			NativeIO.POSIX.SetCacheManipulator(new _NoMlockCacheManipulator_294());
			// mlock succeeds the second time.
			TestCacheAndUncacheBlock();
		}

		private sealed class _NoMlockCacheManipulator_294 : NativeIO.POSIX.NoMlockCacheManipulator
		{
			public _NoMlockCacheManipulator_294()
			{
				this.seenIdentifiers = new HashSet<string>();
			}

			private readonly ICollection<string> seenIdentifiers;

			/// <exception cref="System.IO.IOException"/>
			public override void Mlock(string identifier, ByteBuffer mmap, long length)
			{
				if (this.seenIdentifiers.Contains(identifier))
				{
					TestFsDatasetCache.Log.Info("mlocking " + identifier);
					return;
				}
				this.seenIdentifiers.AddItem(identifier);
				throw new IOException("injecting IOException during mlock of " + identifier);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFilesExceedMaxLockedMemory()
		{
			Log.Info("beginning testFilesExceedMaxLockedMemory");
			// Create some test files that will exceed total cache capacity
			int numFiles = 5;
			long fileSize = CacheCapacity / (numFiles - 1);
			Path[] testFiles = new Path[numFiles];
			HdfsBlockLocation[][] fileLocs = new HdfsBlockLocation[numFiles][];
			long[] fileSizes = new long[numFiles];
			for (int i = 0; i < numFiles; i++)
			{
				testFiles[i] = new Path("/testFilesExceedMaxLockedMemory-" + i);
				DFSTestUtil.CreateFile(fs, testFiles[i], fileSize, (short)1, unchecked((long)(0xDFAl
					)));
				fileLocs[i] = (HdfsBlockLocation[])fs.GetFileBlockLocations(testFiles[i], 0, fileSize
					);
				// Get the file size (sum of blocks)
				long[] sizes = GetBlockSizes(fileLocs[i]);
				for (int j = 0; j < sizes.Length; j++)
				{
					fileSizes[i] += sizes[j];
				}
			}
			// Cache the first n-1 files
			long total = 0;
			DFSTestUtil.VerifyExpectedCacheUsage(0, 0, fsd);
			for (int i_1 = 0; i_1 < numFiles - 1; i_1++)
			{
				SetHeartbeatResponse(CacheBlocks(fileLocs[i_1]));
				total = DFSTestUtil.VerifyExpectedCacheUsage(rounder.Round(total + fileSizes[i_1]
					), 4 * (i_1 + 1), fsd);
			}
			// nth file should hit a capacity exception
			LogVerificationAppender appender = new LogVerificationAppender();
			Logger logger = Logger.GetRootLogger();
			logger.AddAppender(appender);
			SetHeartbeatResponse(CacheBlocks(fileLocs[numFiles - 1]));
			GenericTestUtils.WaitFor(new _Supplier_351(appender), 500, 30000);
			// Also check the metrics for the failure
			NUnit.Framework.Assert.IsTrue("Expected more than 0 failed cache attempts", fsd.GetNumBlocksFailedToCache
				() > 0);
			// Uncache the n-1 files
			int curCachedBlocks = 16;
			for (int i_2 = 0; i_2 < numFiles - 1; i_2++)
			{
				SetHeartbeatResponse(UncacheBlocks(fileLocs[i_2]));
				long uncachedBytes = rounder.Round(fileSizes[i_2]);
				total -= uncachedBytes;
				curCachedBlocks -= uncachedBytes / BlockSize;
				DFSTestUtil.VerifyExpectedCacheUsage(total, curCachedBlocks, fsd);
			}
			Log.Info("finishing testFilesExceedMaxLockedMemory");
		}

		private sealed class _Supplier_351 : Supplier<bool>
		{
			public _Supplier_351(LogVerificationAppender appender)
			{
				this.appender = appender;
			}

			public bool Get()
			{
				int lines = appender.CountLinesWithMessage("more bytes in the cache: " + DFSConfigKeys
					.DfsDatanodeMaxLockedMemoryKey);
				return lines > 0;
			}

			private readonly LogVerificationAppender appender;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUncachingBlocksBeforeCachingFinishes()
		{
			Log.Info("beginning testUncachingBlocksBeforeCachingFinishes");
			int NumBlocks = 5;
			DFSTestUtil.VerifyExpectedCacheUsage(0, 0, fsd);
			// Write a test file
			Path testFile = new Path("/testCacheBlock");
			long testFileLen = BlockSize * NumBlocks;
			DFSTestUtil.CreateFile(fs, testFile, testFileLen, (short)1, unchecked((long)(0xABBAl
				)));
			// Get the details of the written file
			HdfsBlockLocation[] locs = (HdfsBlockLocation[])fs.GetFileBlockLocations(testFile
				, 0, testFileLen);
			NUnit.Framework.Assert.AreEqual("Unexpected number of blocks", NumBlocks, locs.Length
				);
			long[] blockSizes = GetBlockSizes(locs);
			// Check initial state
			long cacheCapacity = fsd.GetCacheCapacity();
			long cacheUsed = fsd.GetCacheUsed();
			long current = 0;
			NUnit.Framework.Assert.AreEqual("Unexpected cache capacity", CacheCapacity, cacheCapacity
				);
			NUnit.Framework.Assert.AreEqual("Unexpected amount of cache used", current, cacheUsed
				);
			NativeIO.POSIX.SetCacheManipulator(new _NoMlockCacheManipulator_401());
			// Starting caching each block in succession.  The usedBytes amount
			// should increase, even though caching doesn't complete on any of them.
			for (int i = 0; i < NumBlocks; i++)
			{
				SetHeartbeatResponse(CacheBlock(locs[i]));
				current = DFSTestUtil.VerifyExpectedCacheUsage(current + blockSizes[i], i + 1, fsd
					);
			}
			SetHeartbeatResponse(new DatanodeCommand[] { GetResponse(locs, DatanodeProtocol.DnaUncache
				) });
			// wait until all caching jobs are finished cancelling.
			current = DFSTestUtil.VerifyExpectedCacheUsage(0, 0, fsd);
			Log.Info("finishing testUncachingBlocksBeforeCachingFinishes");
		}

		private sealed class _NoMlockCacheManipulator_401 : NativeIO.POSIX.NoMlockCacheManipulator
		{
			public _NoMlockCacheManipulator_401()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Mlock(string identifier, ByteBuffer mmap, long length)
			{
				TestFsDatasetCache.Log.Info("An mlock operation is starting on " + identifier);
				try
				{
					Sharpen.Thread.Sleep(3000);
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUncacheUnknownBlock()
		{
			// Create a file
			Path fileName = new Path("/testUncacheUnknownBlock");
			int fileLen = 4096;
			DFSTestUtil.CreateFile(fs, fileName, fileLen, (short)1, unchecked((int)(0xFDFD)));
			HdfsBlockLocation[] locs = (HdfsBlockLocation[])fs.GetFileBlockLocations(fileName
				, 0, fileLen);
			// Try to uncache it without caching it first
			SetHeartbeatResponse(UncacheBlocks(locs));
			GenericTestUtils.WaitFor(new _Supplier_442(), 100, 10000);
		}

		private sealed class _Supplier_442 : Supplier<bool>
		{
			public _Supplier_442()
			{
			}

			public bool Get()
			{
				return TestFsDatasetCache.fsd.GetNumBlocksFailedToUncache() > 0;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPageRounder()
		{
			// Write a small file
			Path fileName = new Path("/testPageRounder");
			int smallBlocks = 512;
			// This should be smaller than the page size
			NUnit.Framework.Assert.IsTrue("Page size should be greater than smallBlocks!", PageSize
				 > smallBlocks);
			int numBlocks = 5;
			int fileLen = smallBlocks * numBlocks;
			FSDataOutputStream @out = fs.Create(fileName, false, 4096, (short)1, smallBlocks);
			@out.Write(new byte[fileLen]);
			@out.Close();
			HdfsBlockLocation[] locs = (HdfsBlockLocation[])fs.GetFileBlockLocations(fileName
				, 0, fileLen);
			// Cache the file and check the sizes match the page size
			SetHeartbeatResponse(CacheBlocks(locs));
			DFSTestUtil.VerifyExpectedCacheUsage(PageSize * numBlocks, numBlocks, fsd);
			// Uncache and check that it decrements by the page size too
			SetHeartbeatResponse(UncacheBlocks(locs));
			DFSTestUtil.VerifyExpectedCacheUsage(0, 0, fsd);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUncacheQuiesces()
		{
			// Create a file
			Path fileName = new Path("/testUncacheQuiesces");
			int fileLen = 4096;
			DFSTestUtil.CreateFile(fs, fileName, fileLen, (short)1, unchecked((int)(0xFDFD)));
			// Cache it
			DistributedFileSystem dfs = cluster.GetFileSystem();
			dfs.AddCachePool(new CachePoolInfo("pool"));
			dfs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPool("pool").SetPath(fileName
				).SetReplication((short)3).Build());
			GenericTestUtils.WaitFor(new _Supplier_484(), 1000, 30000);
			// Uncache it
			dfs.RemoveCacheDirective(1);
			GenericTestUtils.WaitFor(new _Supplier_495(), 1000, 30000);
			// Make sure that no additional messages were sent
			Sharpen.Thread.Sleep(10000);
			MetricsRecordBuilder dnMetrics = MetricsAsserts.GetMetrics(dn.GetMetrics().Name()
				);
			MetricsAsserts.AssertCounter("BlocksCached", 1l, dnMetrics);
			MetricsAsserts.AssertCounter("BlocksUncached", 1l, dnMetrics);
		}

		private sealed class _Supplier_484 : Supplier<bool>
		{
			public _Supplier_484()
			{
			}

			public bool Get()
			{
				MetricsRecordBuilder dnMetrics = MetricsAsserts.GetMetrics(TestFsDatasetCache.dn.
					GetMetrics().Name());
				long blocksCached = MetricsAsserts.GetLongCounter("BlocksCached", dnMetrics);
				return blocksCached > 0;
			}
		}

		private sealed class _Supplier_495 : Supplier<bool>
		{
			public _Supplier_495()
			{
			}

			public bool Get()
			{
				MetricsRecordBuilder dnMetrics = MetricsAsserts.GetMetrics(TestFsDatasetCache.dn.
					GetMetrics().Name());
				long blocksUncached = MetricsAsserts.GetLongCounter("BlocksUncached", dnMetrics);
				return blocksUncached > 0;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReCacheAfterUncache()
		{
			int TotalBlocksPerCache = Ints.CheckedCast(CacheCapacity / BlockSize);
			BlockReaderTestUtil.EnableHdfsCachingTracing();
			NUnit.Framework.Assert.AreEqual(0, CacheCapacity % BlockSize);
			// Create a small file
			Path SmallFile = new Path("/smallFile");
			DFSTestUtil.CreateFile(fs, SmallFile, BlockSize, (short)1, unchecked((int)(0xcafe
				)));
			// Create a file that will take up the whole cache
			Path BigFile = new Path("/bigFile");
			DFSTestUtil.CreateFile(fs, BigFile, TotalBlocksPerCache * BlockSize, (short)1, unchecked(
				(int)(0xbeef)));
			DistributedFileSystem dfs = cluster.GetFileSystem();
			dfs.AddCachePool(new CachePoolInfo("pool"));
			long bigCacheDirectiveId = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder()
				.SetPool("pool").SetPath(BigFile).SetReplication((short)1).Build());
			GenericTestUtils.WaitFor(new _Supplier_532(TotalBlocksPerCache), 1000, 30000);
			// Try to cache a smaller file.  It should fail.
			long shortCacheDirectiveId = dfs.AddCacheDirective(new CacheDirectiveInfo.Builder
				().SetPool("pool").SetPath(SmallFile).SetReplication((short)1).Build());
			Sharpen.Thread.Sleep(10000);
			MetricsRecordBuilder dnMetrics = MetricsAsserts.GetMetrics(dn.GetMetrics().Name()
				);
			NUnit.Framework.Assert.AreEqual(TotalBlocksPerCache, MetricsAsserts.GetLongCounter
				("BlocksCached", dnMetrics));
			// Uncache the big file and verify that the small file can now be
			// cached (regression test for HDFS-6107)
			dfs.RemoveCacheDirective(bigCacheDirectiveId);
			GenericTestUtils.WaitFor(new _Supplier_560(dfs, shortCacheDirectiveId), 1000, 30000
				);
			dfs.RemoveCacheDirective(shortCacheDirectiveId);
		}

		private sealed class _Supplier_532 : Supplier<bool>
		{
			public _Supplier_532(int TotalBlocksPerCache)
			{
				this.TotalBlocksPerCache = TotalBlocksPerCache;
			}

			public bool Get()
			{
				MetricsRecordBuilder dnMetrics = MetricsAsserts.GetMetrics(TestFsDatasetCache.dn.
					GetMetrics().Name());
				long blocksCached = MetricsAsserts.GetLongCounter("BlocksCached", dnMetrics);
				if (blocksCached != TotalBlocksPerCache)
				{
					TestFsDatasetCache.Log.Info("waiting for " + TotalBlocksPerCache + " to " + "be cached.   Right now only "
						 + blocksCached + " blocks are cached.");
					return false;
				}
				TestFsDatasetCache.Log.Info(TotalBlocksPerCache + " blocks are now cached.");
				return true;
			}

			private readonly int TotalBlocksPerCache;
		}

		private sealed class _Supplier_560 : Supplier<bool>
		{
			public _Supplier_560(DistributedFileSystem dfs, long shortCacheDirectiveId)
			{
				this.dfs = dfs;
				this.shortCacheDirectiveId = shortCacheDirectiveId;
			}

			public bool Get()
			{
				RemoteIterator<CacheDirectiveEntry> iter;
				try
				{
					iter = dfs.ListCacheDirectives(new CacheDirectiveInfo.Builder().Build());
					CacheDirectiveEntry entry;
					do
					{
						entry = iter.Next();
					}
					while (entry.GetInfo().GetId() != shortCacheDirectiveId);
					if (entry.GetStats().GetFilesCached() != 1)
					{
						TestFsDatasetCache.Log.Info("waiting for directive " + shortCacheDirectiveId + " to be cached.  stats = "
							 + entry.GetStats());
						return false;
					}
					TestFsDatasetCache.Log.Info("directive " + shortCacheDirectiveId + " has been cached."
						);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.Fail("unexpected exception" + e.ToString());
				}
				return true;
			}

			private readonly DistributedFileSystem dfs;

			private readonly long shortCacheDirectiveId;
		}
	}
}
