using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Lang.Mutable;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This class tests if EnhancedByteBufferAccess works correctly.</summary>
	public class TestEnhancedByteBufferAccess
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestEnhancedByteBufferAccess
			).FullName);

		private static TemporarySocketDirectory sockDir;

		private static NativeIO.POSIX.CacheManipulator prevCacheManipulator;

		[BeforeClass]
		public static void Init()
		{
			sockDir = new TemporarySocketDirectory();
			DomainSocket.DisableBindPathValidation();
			prevCacheManipulator = NativeIO.POSIX.GetCacheManipulator();
			NativeIO.POSIX.SetCacheManipulator(new _CacheManipulator_94());
		}

		private sealed class _CacheManipulator_94 : NativeIO.POSIX.CacheManipulator
		{
			public _CacheManipulator_94()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Mlock(string identifier, ByteBuffer mmap, long length)
			{
				TestEnhancedByteBufferAccess.Log.Info("mlocking " + identifier);
			}
		}

		[AfterClass]
		public static void Teardown()
		{
			// Restore the original CacheManipulator
			NativeIO.POSIX.SetCacheManipulator(prevCacheManipulator);
		}

		private static byte[] ByteBufferToArray(ByteBuffer buf)
		{
			byte[] resultArray = new byte[buf.Remaining()];
			buf.Get(resultArray);
			buf.Flip();
			return resultArray;
		}

		private static readonly int BlockSize = (int)NativeIO.POSIX.GetCacheManipulator()
			.GetOperatingSystemPageSize();

		public static HdfsConfiguration InitZeroCopyTest()
		{
			Assume.AssumeTrue(NativeIO.IsAvailable());
			Assume.AssumeTrue(SystemUtils.IsOsUnix);
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsClientMmapCacheSize, 3);
			conf.SetLong(DFSConfigKeys.DfsClientMmapCacheTimeoutMs, 100);
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), "TestRequestMmapAccess._PORT.sock"
				).GetAbsolutePath());
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, true);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetLong(DFSConfigKeys.DfsCachereportIntervalMsecKey, 1000);
			conf.SetLong(DFSConfigKeys.DfsNamenodePathBasedCacheRefreshIntervalMs, 1000);
			return conf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZeroCopyReads()
		{
			HdfsConfiguration conf = InitZeroCopyTest();
			MiniDFSCluster cluster = null;
			Path TestPath = new Path("/a");
			FSDataInputStream fsIn = null;
			int TestFileLength = 3 * BlockSize;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestFileLength, (short)1, 7567L);
				try
				{
					DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail("unexpected InterruptedException during " + "waitReplication: "
						 + e);
				}
				catch (TimeoutException e)
				{
					NUnit.Framework.Assert.Fail("unexpected TimeoutException during " + "waitReplication: "
						 + e);
				}
				fsIn = fs.Open(TestPath);
				byte[] original = new byte[TestFileLength];
				IOUtils.ReadFully(fsIn, original, 0, TestFileLength);
				fsIn.Close();
				fsIn = fs.Open(TestPath);
				ByteBuffer result = fsIn.Read(null, BlockSize, EnumSet.Of(ReadOption.SkipChecksums
					));
				NUnit.Framework.Assert.AreEqual(BlockSize, result.Remaining());
				HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
				NUnit.Framework.Assert.AreEqual(BlockSize, dfsIn.GetReadStatistics().GetTotalBytesRead
					());
				NUnit.Framework.Assert.AreEqual(BlockSize, dfsIn.GetReadStatistics().GetTotalZeroCopyBytesRead
					());
				Assert.AssertArrayEquals(Arrays.CopyOfRange(original, 0, BlockSize), ByteBufferToArray
					(result));
				fsIn.ReleaseBuffer(result);
			}
			finally
			{
				if (fsIn != null)
				{
					fsIn.Close();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortZeroCopyReads()
		{
			HdfsConfiguration conf = InitZeroCopyTest();
			MiniDFSCluster cluster = null;
			Path TestPath = new Path("/a");
			FSDataInputStream fsIn = null;
			int TestFileLength = 3 * BlockSize;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestFileLength, (short)1, 7567L);
				try
				{
					DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail("unexpected InterruptedException during " + "waitReplication: "
						 + e);
				}
				catch (TimeoutException e)
				{
					NUnit.Framework.Assert.Fail("unexpected TimeoutException during " + "waitReplication: "
						 + e);
				}
				fsIn = fs.Open(TestPath);
				byte[] original = new byte[TestFileLength];
				IOUtils.ReadFully(fsIn, original, 0, TestFileLength);
				fsIn.Close();
				fsIn = fs.Open(TestPath);
				// Try to read (2 * ${BLOCK_SIZE}), but only get ${BLOCK_SIZE} because of the block size.
				HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
				ByteBuffer result = dfsIn.Read(null, 2 * BlockSize, EnumSet.Of(ReadOption.SkipChecksums
					));
				NUnit.Framework.Assert.AreEqual(BlockSize, result.Remaining());
				NUnit.Framework.Assert.AreEqual(BlockSize, dfsIn.GetReadStatistics().GetTotalBytesRead
					());
				NUnit.Framework.Assert.AreEqual(BlockSize, dfsIn.GetReadStatistics().GetTotalZeroCopyBytesRead
					());
				Assert.AssertArrayEquals(Arrays.CopyOfRange(original, 0, BlockSize), ByteBufferToArray
					(result));
				dfsIn.ReleaseBuffer(result);
				// Try to read (1 + ${BLOCK_SIZE}), but only get ${BLOCK_SIZE} because of the block size.
				result = dfsIn.Read(null, 1 + BlockSize, EnumSet.Of(ReadOption.SkipChecksums));
				NUnit.Framework.Assert.AreEqual(BlockSize, result.Remaining());
				Assert.AssertArrayEquals(Arrays.CopyOfRange(original, BlockSize, 2 * BlockSize), 
					ByteBufferToArray(result));
				dfsIn.ReleaseBuffer(result);
			}
			finally
			{
				if (fsIn != null)
				{
					fsIn.Close();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZeroCopyReadsNoFallback()
		{
			HdfsConfiguration conf = InitZeroCopyTest();
			MiniDFSCluster cluster = null;
			Path TestPath = new Path("/a");
			FSDataInputStream fsIn = null;
			int TestFileLength = 3 * BlockSize;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestFileLength, (short)1, 7567L);
				try
				{
					DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail("unexpected InterruptedException during " + "waitReplication: "
						 + e);
				}
				catch (TimeoutException e)
				{
					NUnit.Framework.Assert.Fail("unexpected TimeoutException during " + "waitReplication: "
						 + e);
				}
				fsIn = fs.Open(TestPath);
				byte[] original = new byte[TestFileLength];
				IOUtils.ReadFully(fsIn, original, 0, TestFileLength);
				fsIn.Close();
				fsIn = fs.Open(TestPath);
				HdfsDataInputStream dfsIn = (HdfsDataInputStream)fsIn;
				ByteBuffer result;
				try
				{
					result = dfsIn.Read(null, BlockSize + 1, EnumSet.NoneOf<ReadOption>());
					NUnit.Framework.Assert.Fail("expected UnsupportedOperationException");
				}
				catch (NotSupportedException)
				{
				}
				// expected
				result = dfsIn.Read(null, BlockSize, EnumSet.Of(ReadOption.SkipChecksums));
				NUnit.Framework.Assert.AreEqual(BlockSize, result.Remaining());
				NUnit.Framework.Assert.AreEqual(BlockSize, dfsIn.GetReadStatistics().GetTotalBytesRead
					());
				NUnit.Framework.Assert.AreEqual(BlockSize, dfsIn.GetReadStatistics().GetTotalZeroCopyBytesRead
					());
				Assert.AssertArrayEquals(Arrays.CopyOfRange(original, 0, BlockSize), ByteBufferToArray
					(result));
			}
			finally
			{
				if (fsIn != null)
				{
					fsIn.Close();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private class CountingVisitor : ShortCircuitCache.CacheVisitor
		{
			private readonly int expectedNumOutstandingMmaps;

			private readonly int expectedNumReplicas;

			private readonly int expectedNumEvictable;

			private readonly int expectedNumMmapedEvictable;

			internal CountingVisitor(int expectedNumOutstandingMmaps, int expectedNumReplicas
				, int expectedNumEvictable, int expectedNumMmapedEvictable)
			{
				this.expectedNumOutstandingMmaps = expectedNumOutstandingMmaps;
				this.expectedNumReplicas = expectedNumReplicas;
				this.expectedNumEvictable = expectedNumEvictable;
				this.expectedNumMmapedEvictable = expectedNumMmapedEvictable;
			}

			public virtual void Visit(int numOutstandingMmaps, IDictionary<ExtendedBlockId, ShortCircuitReplica
				> replicas, IDictionary<ExtendedBlockId, SecretManager.InvalidToken> failedLoads
				, IDictionary<long, ShortCircuitReplica> evictable, IDictionary<long, ShortCircuitReplica
				> evictableMmapped)
			{
				if (expectedNumOutstandingMmaps >= 0)
				{
					NUnit.Framework.Assert.AreEqual(expectedNumOutstandingMmaps, numOutstandingMmaps);
				}
				if (expectedNumReplicas >= 0)
				{
					NUnit.Framework.Assert.AreEqual(expectedNumReplicas, replicas.Count);
				}
				if (expectedNumEvictable >= 0)
				{
					NUnit.Framework.Assert.AreEqual(expectedNumEvictable, evictable.Count);
				}
				if (expectedNumMmapedEvictable >= 0)
				{
					NUnit.Framework.Assert.AreEqual(expectedNumMmapedEvictable, evictableMmapped.Count
						);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZeroCopyMmapCache()
		{
			HdfsConfiguration conf = InitZeroCopyTest();
			MiniDFSCluster cluster = null;
			Path TestPath = new Path("/a");
			int TestFileLength = 5 * BlockSize;
			int RandomSeed = 23453;
			string Context = "testZeroCopyMmapCacheContext";
			FSDataInputStream fsIn = null;
			ByteBuffer[] results = new ByteBuffer[] { null, null, null, null };
			DistributedFileSystem fs = null;
			conf.Set(DFSConfigKeys.DfsClientContext, Context);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			DFSTestUtil.CreateFile(fs, TestPath, TestFileLength, (short)1, RandomSeed);
			try
			{
				DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("unexpected InterruptedException during " + "waitReplication: "
					 + e);
			}
			catch (TimeoutException e)
			{
				NUnit.Framework.Assert.Fail("unexpected TimeoutException during " + "waitReplication: "
					 + e);
			}
			fsIn = fs.Open(TestPath);
			byte[] original = new byte[TestFileLength];
			IOUtils.ReadFully(fsIn, original, 0, TestFileLength);
			fsIn.Close();
			fsIn = fs.Open(TestPath);
			ShortCircuitCache cache = ClientContext.Get(Context, new DFSClient.Conf(conf)).GetShortCircuitCache
				();
			cache.Accept(new TestEnhancedByteBufferAccess.CountingVisitor(0, 5, 5, 0));
			results[0] = fsIn.Read(null, BlockSize, EnumSet.Of(ReadOption.SkipChecksums));
			fsIn.Seek(0);
			results[1] = fsIn.Read(null, BlockSize, EnumSet.Of(ReadOption.SkipChecksums));
			// The mmap should be of the first block of the file.
			ExtendedBlock firstBlock = DFSTestUtil.GetFirstBlock(fs, TestPath);
			cache.Accept(new _CacheVisitor_373(firstBlock));
			// The replica should not yet be evictable, since we have it open.
			// Read more blocks.
			results[2] = fsIn.Read(null, BlockSize, EnumSet.Of(ReadOption.SkipChecksums));
			results[3] = fsIn.Read(null, BlockSize, EnumSet.Of(ReadOption.SkipChecksums));
			// we should have 3 mmaps, 1 evictable
			cache.Accept(new TestEnhancedByteBufferAccess.CountingVisitor(3, 5, 2, 0));
			// After we close the cursors, the mmaps should be evictable for 
			// a brief period of time.  Then, they should be closed (we're 
			// using a very quick timeout)
			foreach (ByteBuffer buffer in results)
			{
				if (buffer != null)
				{
					fsIn.ReleaseBuffer(buffer);
				}
			}
			fsIn.Close();
			GenericTestUtils.WaitFor(new _Supplier_407(cache), 10, 60000);
			cache.Accept(new TestEnhancedByteBufferAccess.CountingVisitor(0, -1, -1, -1));
			fs.Close();
			cluster.Shutdown();
		}

		private sealed class _CacheVisitor_373 : ShortCircuitCache.CacheVisitor
		{
			public _CacheVisitor_373(ExtendedBlock firstBlock)
			{
				this.firstBlock = firstBlock;
			}

			public void Visit(int numOutstandingMmaps, IDictionary<ExtendedBlockId, ShortCircuitReplica
				> replicas, IDictionary<ExtendedBlockId, SecretManager.InvalidToken> failedLoads
				, IDictionary<long, ShortCircuitReplica> evictable, IDictionary<long, ShortCircuitReplica
				> evictableMmapped)
			{
				ShortCircuitReplica replica = replicas[new ExtendedBlockId(firstBlock.GetBlockId(
					), firstBlock.GetBlockPoolId())];
				NUnit.Framework.Assert.IsNotNull(replica);
				NUnit.Framework.Assert.IsTrue(replica.HasMmap());
				NUnit.Framework.Assert.IsNull(replica.GetEvictableTimeNs());
			}

			private readonly ExtendedBlock firstBlock;
		}

		private sealed class _Supplier_407 : Supplier<bool>
		{
			public _Supplier_407(ShortCircuitCache cache)
			{
				this.cache = cache;
			}

			public bool Get()
			{
				MutableBoolean finished = new MutableBoolean(false);
				cache.Accept(new _CacheVisitor_410(finished));
				return finished.BooleanValue();
			}

			private sealed class _CacheVisitor_410 : ShortCircuitCache.CacheVisitor
			{
				public _CacheVisitor_410(MutableBoolean finished)
				{
					this.finished = finished;
				}

				public void Visit(int numOutstandingMmaps, IDictionary<ExtendedBlockId, ShortCircuitReplica
					> replicas, IDictionary<ExtendedBlockId, SecretManager.InvalidToken> failedLoads
					, IDictionary<long, ShortCircuitReplica> evictable, IDictionary<long, ShortCircuitReplica
					> evictableMmapped)
				{
					finished.SetValue(evictableMmapped.IsEmpty());
				}

				private readonly MutableBoolean finished;
			}

			private readonly ShortCircuitCache cache;
		}

		/// <summary>Test HDFS fallback reads.</summary>
		/// <remarks>
		/// Test HDFS fallback reads.  HDFS streams support the ByteBufferReadable
		/// interface.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHdfsFallbackReads()
		{
			HdfsConfiguration conf = InitZeroCopyTest();
			MiniDFSCluster cluster = null;
			Path TestPath = new Path("/a");
			int TestFileLength = 16385;
			int RandomSeed = 23453;
			FSDataInputStream fsIn = null;
			DistributedFileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestFileLength, (short)1, RandomSeed);
				try
				{
					DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail("unexpected InterruptedException during " + "waitReplication: "
						 + e);
				}
				catch (TimeoutException e)
				{
					NUnit.Framework.Assert.Fail("unexpected TimeoutException during " + "waitReplication: "
						 + e);
				}
				fsIn = fs.Open(TestPath);
				byte[] original = new byte[TestFileLength];
				IOUtils.ReadFully(fsIn, original, 0, TestFileLength);
				fsIn.Close();
				fsIn = fs.Open(TestPath);
				TestFallbackImpl(fsIn, original);
			}
			finally
			{
				if (fsIn != null)
				{
					fsIn.Close();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private class RestrictedAllocatingByteBufferPool : ByteBufferPool
		{
			private readonly bool direct;

			internal RestrictedAllocatingByteBufferPool(bool direct)
			{
				this.direct = direct;
			}

			public virtual ByteBuffer GetBuffer(bool direct, int length)
			{
				Preconditions.CheckArgument(this.direct == direct);
				return direct ? ByteBuffer.AllocateDirect(length) : ByteBuffer.Allocate(length);
			}

			public virtual void PutBuffer(ByteBuffer buffer)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		private static void TestFallbackImpl(InputStream stream, byte[] original)
		{
			TestEnhancedByteBufferAccess.RestrictedAllocatingByteBufferPool bufferPool = new 
				TestEnhancedByteBufferAccess.RestrictedAllocatingByteBufferPool(stream is ByteBufferReadable
				);
			ByteBuffer result = ByteBufferUtil.FallbackRead(stream, bufferPool, 10);
			NUnit.Framework.Assert.AreEqual(10, result.Remaining());
			Assert.AssertArrayEquals(Arrays.CopyOfRange(original, 0, 10), ByteBufferToArray(result
				));
			result = ByteBufferUtil.FallbackRead(stream, bufferPool, 5000);
			NUnit.Framework.Assert.AreEqual(5000, result.Remaining());
			Assert.AssertArrayEquals(Arrays.CopyOfRange(original, 10, 5010), ByteBufferToArray
				(result));
			result = ByteBufferUtil.FallbackRead(stream, bufferPool, 9999999);
			NUnit.Framework.Assert.AreEqual(11375, result.Remaining());
			Assert.AssertArrayEquals(Arrays.CopyOfRange(original, 5010, 16385), ByteBufferToArray
				(result));
			result = ByteBufferUtil.FallbackRead(stream, bufferPool, 10);
			NUnit.Framework.Assert.IsNull(result);
		}

		/// <summary>
		/// Test the
		/// <see cref="ByteBufferUtil.FallbackRead(System.IO.InputStream, Org.Apache.Hadoop.IO.ByteBufferPool, int)
		/// 	"/>
		/// function directly.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFallbackRead()
		{
			HdfsConfiguration conf = InitZeroCopyTest();
			MiniDFSCluster cluster = null;
			Path TestPath = new Path("/a");
			int TestFileLength = 16385;
			int RandomSeed = 23453;
			FSDataInputStream fsIn = null;
			DistributedFileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestFileLength, (short)1, RandomSeed);
				try
				{
					DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail("unexpected InterruptedException during " + "waitReplication: "
						 + e);
				}
				catch (TimeoutException e)
				{
					NUnit.Framework.Assert.Fail("unexpected TimeoutException during " + "waitReplication: "
						 + e);
				}
				fsIn = fs.Open(TestPath);
				byte[] original = new byte[TestFileLength];
				IOUtils.ReadFully(fsIn, original, 0, TestFileLength);
				fsIn.Close();
				fsIn = fs.Open(TestPath);
				TestFallbackImpl(fsIn, original);
			}
			finally
			{
				if (fsIn != null)
				{
					fsIn.Close();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test fallback reads on a stream which does not support the
		/// ByteBufferReadable * interface.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIndirectFallbackReads()
		{
			FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data", "build/test/data"
				));
			string TestPath = TestDir + FilePath.separator + "indirectFallbackTestFile";
			int TestFileLength = 16385;
			int RandomSeed = 23453;
			FileOutputStream fos = null;
			FileInputStream fis = null;
			try
			{
				fos = new FileOutputStream(TestPath);
				Random random = new Random(RandomSeed);
				byte[] original = new byte[TestFileLength];
				random.NextBytes(original);
				fos.Write(original);
				fos.Close();
				fos = null;
				fis = new FileInputStream(TestPath);
				TestFallbackImpl(fis, original);
			}
			finally
			{
				IOUtils.Cleanup(Log, fos, fis);
				new FilePath(TestPath).Delete();
			}
		}

		/// <summary>
		/// Test that we can zero-copy read cached data even without disabling
		/// checksums.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestZeroCopyReadOfCachedData()
		{
			BlockReaderTestUtil.EnableShortCircuitShmTracing();
			BlockReaderTestUtil.EnableBlockReaderFactoryTracing();
			BlockReaderTestUtil.EnableHdfsCachingTracing();
			int TestFileLength = BlockSize;
			Path TestPath = new Path("/a");
			int RandomSeed = 23453;
			HdfsConfiguration conf = InitZeroCopyTest();
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, false);
			string Context = "testZeroCopyReadOfCachedData";
			conf.Set(DFSConfigKeys.DfsClientContext, Context);
			conf.SetLong(DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, DFSTestUtil.RoundUpToMultiple
				(TestFileLength, (int)NativeIO.POSIX.GetCacheManipulator().GetOperatingSystemPageSize
				()));
			MiniDFSCluster cluster = null;
			ByteBuffer result = null;
			ByteBuffer result2 = null;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			FsDatasetSpi<object> fsd = cluster.GetDataNodes()[0].GetFSDataset();
			DistributedFileSystem fs = cluster.GetFileSystem();
			DFSTestUtil.CreateFile(fs, TestPath, TestFileLength, (short)1, RandomSeed);
			DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
			byte[] original = DFSTestUtil.CalculateFileContentsFromSeed(RandomSeed, TestFileLength
				);
			// Prior to caching, the file can't be read via zero-copy
			FSDataInputStream fsIn = fs.Open(TestPath);
			try
			{
				result = fsIn.Read(null, TestFileLength / 2, EnumSet.NoneOf<ReadOption>());
				NUnit.Framework.Assert.Fail("expected UnsupportedOperationException");
			}
			catch (NotSupportedException)
			{
			}
			// expected
			// Cache the file
			fs.AddCachePool(new CachePoolInfo("pool1"));
			long directiveId = fs.AddCacheDirective(new CacheDirectiveInfo.Builder().SetPath(
				TestPath).SetReplication((short)1).SetPool("pool1").Build());
			int numBlocks = (int)Math.Ceil((double)TestFileLength / BlockSize);
			DFSTestUtil.VerifyExpectedCacheUsage(DFSTestUtil.RoundUpToMultiple(TestFileLength
				, BlockSize), numBlocks, cluster.GetDataNodes()[0].GetFSDataset());
			try
			{
				result = fsIn.Read(null, TestFileLength, EnumSet.NoneOf<ReadOption>());
			}
			catch (NotSupportedException)
			{
				NUnit.Framework.Assert.Fail("expected to be able to read cached file via zero-copy"
					);
			}
			Assert.AssertArrayEquals(Arrays.CopyOfRange(original, 0, BlockSize), ByteBufferToArray
				(result));
			// Test that files opened after the cache operation has finished
			// still get the benefits of zero-copy (regression test for HDFS-6086)
			FSDataInputStream fsIn2 = fs.Open(TestPath);
			try
			{
				result2 = fsIn2.Read(null, TestFileLength, EnumSet.NoneOf<ReadOption>());
			}
			catch (NotSupportedException)
			{
				NUnit.Framework.Assert.Fail("expected to be able to read cached file via zero-copy"
					);
			}
			Assert.AssertArrayEquals(Arrays.CopyOfRange(original, 0, BlockSize), ByteBufferToArray
				(result2));
			fsIn2.ReleaseBuffer(result2);
			fsIn2.Close();
			// check that the replica is anchored 
			ExtendedBlock firstBlock = DFSTestUtil.GetFirstBlock(fs, TestPath);
			ShortCircuitCache cache = ClientContext.Get(Context, new DFSClient.Conf(conf)).GetShortCircuitCache
				();
			WaitForReplicaAnchorStatus(cache, firstBlock, true, true, 1);
			// Uncache the replica
			fs.RemoveCacheDirective(directiveId);
			WaitForReplicaAnchorStatus(cache, firstBlock, false, true, 1);
			fsIn.ReleaseBuffer(result);
			WaitForReplicaAnchorStatus(cache, firstBlock, false, false, 1);
			DFSTestUtil.VerifyExpectedCacheUsage(0, 0, fsd);
			fsIn.Close();
			fs.Close();
			cluster.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		private void WaitForReplicaAnchorStatus(ShortCircuitCache cache, ExtendedBlock block
			, bool expectedIsAnchorable, bool expectedIsAnchored, int expectedOutstandingMmaps
			)
		{
			GenericTestUtils.WaitFor(new _Supplier_683(cache, expectedOutstandingMmaps, block
				, expectedIsAnchorable, expectedIsAnchored), 10, 60000);
		}

		private sealed class _Supplier_683 : Supplier<bool>
		{
			public _Supplier_683(ShortCircuitCache cache, int expectedOutstandingMmaps, ExtendedBlock
				 block, bool expectedIsAnchorable, bool expectedIsAnchored)
			{
				this.cache = cache;
				this.expectedOutstandingMmaps = expectedOutstandingMmaps;
				this.block = block;
				this.expectedIsAnchorable = expectedIsAnchorable;
				this.expectedIsAnchored = expectedIsAnchored;
			}

			public bool Get()
			{
				MutableBoolean result = new MutableBoolean(false);
				cache.Accept(new _CacheVisitor_687(expectedOutstandingMmaps, block, expectedIsAnchorable
					, expectedIsAnchored, result));
				return result.ToBoolean();
			}

			private sealed class _CacheVisitor_687 : ShortCircuitCache.CacheVisitor
			{
				public _CacheVisitor_687(int expectedOutstandingMmaps, ExtendedBlock block, bool 
					expectedIsAnchorable, bool expectedIsAnchored, MutableBoolean result)
				{
					this.expectedOutstandingMmaps = expectedOutstandingMmaps;
					this.block = block;
					this.expectedIsAnchorable = expectedIsAnchorable;
					this.expectedIsAnchored = expectedIsAnchored;
					this.result = result;
				}

				public void Visit(int numOutstandingMmaps, IDictionary<ExtendedBlockId, ShortCircuitReplica
					> replicas, IDictionary<ExtendedBlockId, SecretManager.InvalidToken> failedLoads
					, IDictionary<long, ShortCircuitReplica> evictable, IDictionary<long, ShortCircuitReplica
					> evictableMmapped)
				{
					NUnit.Framework.Assert.AreEqual(expectedOutstandingMmaps, numOutstandingMmaps);
					ShortCircuitReplica replica = replicas[ExtendedBlockId.FromExtendedBlock(block)];
					NUnit.Framework.Assert.IsNotNull(replica);
					ShortCircuitShm.Slot slot = replica.GetSlot();
					if ((expectedIsAnchorable != slot.IsAnchorable()) || (expectedIsAnchored != slot.
						IsAnchored()))
					{
						TestEnhancedByteBufferAccess.Log.Info("replica " + replica + " has isAnchorable = "
							 + slot.IsAnchorable() + ", isAnchored = " + slot.IsAnchored() + ".  Waiting for isAnchorable = "
							 + expectedIsAnchorable + ", isAnchored = " + expectedIsAnchored);
						return;
					}
					result.SetValue(true);
				}

				private readonly int expectedOutstandingMmaps;

				private readonly ExtendedBlock block;

				private readonly bool expectedIsAnchorable;

				private readonly bool expectedIsAnchored;

				private readonly MutableBoolean result;
			}

			private readonly ShortCircuitCache cache;

			private readonly int expectedOutstandingMmaps;

			private readonly ExtendedBlock block;

			private readonly bool expectedIsAnchorable;

			private readonly bool expectedIsAnchored;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClientMmapDisable()
		{
			HdfsConfiguration conf = InitZeroCopyTest();
			conf.SetBoolean(DFSConfigKeys.DfsClientMmapEnabled, false);
			MiniDFSCluster cluster = null;
			Path TestPath = new Path("/a");
			int TestFileLength = 16385;
			int RandomSeed = 23453;
			string Context = "testClientMmapDisable";
			FSDataInputStream fsIn = null;
			DistributedFileSystem fs = null;
			conf.Set(DFSConfigKeys.DfsClientContext, Context);
			try
			{
				// With DFS_CLIENT_MMAP_ENABLED set to false, we should not do memory
				// mapped reads.
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestFileLength, (short)1, RandomSeed);
				DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				fsIn = fs.Open(TestPath);
				try
				{
					fsIn.Read(null, 1, EnumSet.Of(ReadOption.SkipChecksums));
					NUnit.Framework.Assert.Fail("expected zero-copy read to fail when client mmaps " 
						+ "were disabled.");
				}
				catch (NotSupportedException)
				{
				}
			}
			finally
			{
				if (fsIn != null)
				{
					fsIn.Close();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			fsIn = null;
			fs = null;
			cluster = null;
			try
			{
				// Now try again with DFS_CLIENT_MMAP_CACHE_SIZE == 0.  It should work.
				conf.SetBoolean(DFSConfigKeys.DfsClientMmapEnabled, true);
				conf.SetInt(DFSConfigKeys.DfsClientMmapCacheSize, 0);
				conf.Set(DFSConfigKeys.DfsClientContext, Context + ".1");
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestFileLength, (short)1, RandomSeed);
				DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				fsIn = fs.Open(TestPath);
				ByteBuffer buf = fsIn.Read(null, 1, EnumSet.Of(ReadOption.SkipChecksums));
				fsIn.ReleaseBuffer(buf);
				// Test EOF behavior
				IOUtils.SkipFully(fsIn, TestFileLength - 1);
				buf = fsIn.Read(null, 1, EnumSet.Of(ReadOption.SkipChecksums));
				NUnit.Framework.Assert.AreEqual(null, buf);
			}
			finally
			{
				if (fsIn != null)
				{
					fsIn.Close();
				}
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Test2GBMmapLimit()
		{
			Assume.AssumeTrue(BlockReaderTestUtil.ShouldTestLargeFiles());
			HdfsConfiguration conf = InitZeroCopyTest();
			long TestFileLength = 2469605888L;
			conf.Set(DFSConfigKeys.DfsChecksumTypeKey, "NULL");
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, TestFileLength);
			MiniDFSCluster cluster = null;
			Path TestPath = new Path("/a");
			string Context = "test2GBMmapLimit";
			conf.Set(DFSConfigKeys.DfsClientContext, Context);
			FSDataInputStream fsIn = null;
			FSDataInputStream fsIn2 = null;
			ByteBuffer buf1 = null;
			ByteBuffer buf2 = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				DistributedFileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestFileLength, (short)1, unchecked((int)(0xB
					)));
				DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				fsIn = fs.Open(TestPath);
				buf1 = fsIn.Read(null, 1, EnumSet.Of(ReadOption.SkipChecksums));
				NUnit.Framework.Assert.AreEqual(1, buf1.Remaining());
				fsIn.ReleaseBuffer(buf1);
				buf1 = null;
				fsIn.Seek(2147483640L);
				buf1 = fsIn.Read(null, 1024, EnumSet.Of(ReadOption.SkipChecksums));
				NUnit.Framework.Assert.AreEqual(7, buf1.Remaining());
				NUnit.Framework.Assert.AreEqual(int.MaxValue, buf1.Limit());
				fsIn.ReleaseBuffer(buf1);
				buf1 = null;
				NUnit.Framework.Assert.AreEqual(2147483647L, fsIn.GetPos());
				try
				{
					buf1 = fsIn.Read(null, 1024, EnumSet.Of(ReadOption.SkipChecksums));
					NUnit.Framework.Assert.Fail("expected UnsupportedOperationException");
				}
				catch (NotSupportedException)
				{
				}
				// expected; can't read past 2GB boundary.
				fsIn.Close();
				fsIn = null;
				// Now create another file with normal-sized blocks, and verify we
				// can read past 2GB
				Path TestPath2 = new Path("/b");
				conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 268435456L);
				DFSTestUtil.CreateFile(fs, TestPath2, 1024 * 1024, TestFileLength, 268435456L, (short
					)1, unchecked((int)(0xA)));
				fsIn2 = fs.Open(TestPath2);
				fsIn2.Seek(2147483640L);
				buf2 = fsIn2.Read(null, 1024, EnumSet.Of(ReadOption.SkipChecksums));
				NUnit.Framework.Assert.AreEqual(8, buf2.Remaining());
				NUnit.Framework.Assert.AreEqual(2147483648L, fsIn2.GetPos());
				fsIn2.ReleaseBuffer(buf2);
				buf2 = null;
				buf2 = fsIn2.Read(null, 1024, EnumSet.Of(ReadOption.SkipChecksums));
				NUnit.Framework.Assert.AreEqual(1024, buf2.Remaining());
				NUnit.Framework.Assert.AreEqual(2147484672L, fsIn2.GetPos());
				fsIn2.ReleaseBuffer(buf2);
				buf2 = null;
			}
			finally
			{
				if (buf1 != null)
				{
					fsIn.ReleaseBuffer(buf1);
				}
				if (buf2 != null)
				{
					fsIn2.ReleaseBuffer(buf2);
				}
				IOUtils.Cleanup(null, fsIn, fsIn2);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
