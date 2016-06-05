using System;
using System.Collections.Generic;
using System.Threading;
using Com.Google.Common.Util.Concurrent;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.Net.Unix;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestBlockReaderFactory
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestBlockReaderFactory
			));

		[SetUp]
		public virtual void Init()
		{
			DomainSocket.DisableBindPathValidation();
			Assume.AssumeThat(DomainSocket.GetLoadingFailureReason(), CoreMatchers.EqualTo(null
				));
		}

		[TearDown]
		public virtual void Cleanup()
		{
			DFSInputStream.tcpReadsDisabledForTesting = false;
			BlockReaderFactory.createShortCircuitReplicaInfoCallback = null;
		}

		public static Configuration CreateShortCircuitConf(string testName, TemporarySocketDirectory
			 sockDir)
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsClientContext, testName);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 4096);
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), testName
				 + "._PORT").GetAbsolutePath());
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, false);
			conf.SetBoolean(DFSConfigKeys.DfsClientDomainSocketDataTraffic, false);
			return conf;
		}

		/// <summary>
		/// If we have a UNIX domain socket configured,
		/// and we have dfs.client.domain.socket.data.traffic set to true,
		/// and short-circuit access fails, we should still be able to pass
		/// data traffic over the UNIX domain socket.
		/// </summary>
		/// <remarks>
		/// If we have a UNIX domain socket configured,
		/// and we have dfs.client.domain.socket.data.traffic set to true,
		/// and short-circuit access fails, we should still be able to pass
		/// data traffic over the UNIX domain socket.  Test this.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestFallbackFromShortCircuitToUnixDomainTraffic()
		{
			DFSInputStream.tcpReadsDisabledForTesting = true;
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			// The server is NOT configured with short-circuit local reads;
			// the client is.  Both support UNIX domain reads.
			Configuration clientConf = CreateShortCircuitConf("testFallbackFromShortCircuitToUnixDomainTraffic"
				, sockDir);
			clientConf.Set(DFSConfigKeys.DfsClientContext, "testFallbackFromShortCircuitToUnixDomainTraffic_clientContext"
				);
			clientConf.SetBoolean(DFSConfigKeys.DfsClientDomainSocketDataTraffic, true);
			Configuration serverConf = new Configuration(clientConf);
			serverConf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, false);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(serverConf).NumDataNodes(1).Build
				();
			cluster.WaitActive();
			FileSystem dfs = FileSystem.Get(cluster.GetURI(0), clientConf);
			string TestFile = "/test_file";
			int TestFileLen = 8193;
			int Seed = unchecked((int)(0xFADED));
			DFSTestUtil.CreateFile(dfs, new Path(TestFile), TestFileLen, (short)1, Seed);
			byte[] contents = DFSTestUtil.ReadFileBuffer(dfs, new Path(TestFile));
			byte[] expected = DFSTestUtil.CalculateFileContentsFromSeed(Seed, TestFileLen);
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(contents, expected));
			cluster.Shutdown();
			sockDir.Close();
		}

		/// <summary>
		/// Test the case where we have multiple threads waiting on the
		/// ShortCircuitCache delivering a certain ShortCircuitReplica.
		/// </summary>
		/// <remarks>
		/// Test the case where we have multiple threads waiting on the
		/// ShortCircuitCache delivering a certain ShortCircuitReplica.
		/// In this case, there should only be one call to
		/// createShortCircuitReplicaInfo.  This one replica should be shared
		/// by all threads.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestMultipleWaitersOnShortCircuitCache()
		{
			CountDownLatch latch = new CountDownLatch(1);
			AtomicBoolean creationIsBlocked = new AtomicBoolean(true);
			AtomicBoolean testFailed = new AtomicBoolean(false);
			DFSInputStream.tcpReadsDisabledForTesting = true;
			BlockReaderFactory.createShortCircuitReplicaInfoCallback = new _ShortCircuitReplicaCreator_146
				(latch, creationIsBlocked);
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration conf = CreateShortCircuitConf("testMultipleWaitersOnShortCircuitCache"
				, sockDir);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			string TestFile = "/test_file";
			int TestFileLen = 4000;
			int Seed = unchecked((int)(0xFADED));
			int NumThreads = 10;
			DFSTestUtil.CreateFile(dfs, new Path(TestFile), TestFileLen, (short)1, Seed);
			Runnable readerRunnable = new _Runnable_170(dfs, TestFile, creationIsBlocked, Seed
				, TestFileLen, testFailed);
			Sharpen.Thread[] threads = new Sharpen.Thread[NumThreads];
			for (int i = 0; i < NumThreads; i++)
			{
				threads[i] = new Sharpen.Thread(readerRunnable);
				threads[i].Start();
			}
			Sharpen.Thread.Sleep(500);
			latch.CountDown();
			for (int i_1 = 0; i_1 < NumThreads; i_1++)
			{
				Uninterruptibles.JoinUninterruptibly(threads[i_1]);
			}
			cluster.Shutdown();
			sockDir.Close();
			NUnit.Framework.Assert.IsFalse(testFailed.Get());
		}

		private sealed class _ShortCircuitReplicaCreator_146 : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			public _ShortCircuitReplicaCreator_146(CountDownLatch latch, AtomicBoolean creationIsBlocked
				)
			{
				this.latch = latch;
				this.creationIsBlocked = creationIsBlocked;
			}

			public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				Uninterruptibles.AwaitUninterruptibly(latch);
				if (!creationIsBlocked.CompareAndSet(true, false))
				{
					NUnit.Framework.Assert.Fail("there were multiple calls to " + "createShortCircuitReplicaInfo.  Only one was expected."
						);
				}
				return null;
			}

			private readonly CountDownLatch latch;

			private readonly AtomicBoolean creationIsBlocked;
		}

		private sealed class _Runnable_170 : Runnable
		{
			public _Runnable_170(DistributedFileSystem dfs, string TestFile, AtomicBoolean creationIsBlocked
				, int Seed, int TestFileLen, AtomicBoolean testFailed)
			{
				this.dfs = dfs;
				this.TestFile = TestFile;
				this.creationIsBlocked = creationIsBlocked;
				this.Seed = Seed;
				this.TestFileLen = TestFileLen;
				this.testFailed = testFailed;
			}

			public void Run()
			{
				try
				{
					byte[] contents = DFSTestUtil.ReadFileBuffer(dfs, new Path(TestFile));
					NUnit.Framework.Assert.IsFalse(creationIsBlocked.Get());
					byte[] expected = DFSTestUtil.CalculateFileContentsFromSeed(Seed, TestFileLen);
					NUnit.Framework.Assert.IsTrue(Arrays.Equals(contents, expected));
				}
				catch (Exception e)
				{
					TestBlockReaderFactory.Log.Error("readerRunnable error", e);
					testFailed.Set(true);
				}
			}

			private readonly DistributedFileSystem dfs;

			private readonly string TestFile;

			private readonly AtomicBoolean creationIsBlocked;

			private readonly int Seed;

			private readonly int TestFileLen;

			private readonly AtomicBoolean testFailed;
		}

		/// <summary>
		/// Test the case where we have a failure to complete a short circuit read
		/// that occurs, and then later on, we have a success.
		/// </summary>
		/// <remarks>
		/// Test the case where we have a failure to complete a short circuit read
		/// that occurs, and then later on, we have a success.
		/// Any thread waiting on a cache load should receive the failure (if it
		/// occurs);  however, the failure result should not be cached.  We want
		/// to be able to retry later and succeed.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestShortCircuitCacheTemporaryFailure()
		{
			BlockReaderTestUtil.EnableBlockReaderFactoryTracing();
			AtomicBoolean replicaCreationShouldFail = new AtomicBoolean(true);
			AtomicBoolean testFailed = new AtomicBoolean(false);
			DFSInputStream.tcpReadsDisabledForTesting = true;
			BlockReaderFactory.createShortCircuitReplicaInfoCallback = new _ShortCircuitReplicaCreator_215
				(replicaCreationShouldFail);
			// Insert a short delay to increase the chance that one client
			// thread waits for the other client thread's failure via
			// a condition variable.
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration conf = CreateShortCircuitConf("testShortCircuitCacheTemporaryFailure"
				, sockDir);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			string TestFile = "/test_file";
			int TestFileLen = 4000;
			int NumThreads = 2;
			int Seed = unchecked((int)(0xFADED));
			CountDownLatch gotFailureLatch = new CountDownLatch(NumThreads);
			CountDownLatch shouldRetryLatch = new CountDownLatch(1);
			DFSTestUtil.CreateFile(dfs, new Path(TestFile), TestFileLen, (short)1, Seed);
			Runnable readerRunnable = new _Runnable_243(cluster, TestFile, TestFileLen, gotFailureLatch
				, shouldRetryLatch, testFailed);
			// First time should fail.
			// first block
			// keep findbugs happy
			// Second time should succeed.
			Sharpen.Thread[] threads = new Sharpen.Thread[NumThreads];
			for (int i = 0; i < NumThreads; i++)
			{
				threads[i] = new Sharpen.Thread(readerRunnable);
				threads[i].Start();
			}
			gotFailureLatch.Await();
			replicaCreationShouldFail.Set(false);
			shouldRetryLatch.CountDown();
			for (int i_1 = 0; i_1 < NumThreads; i_1++)
			{
				Uninterruptibles.JoinUninterruptibly(threads[i_1]);
			}
			cluster.Shutdown();
			sockDir.Close();
			NUnit.Framework.Assert.IsFalse(testFailed.Get());
		}

		private sealed class _ShortCircuitReplicaCreator_215 : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			public _ShortCircuitReplicaCreator_215(AtomicBoolean replicaCreationShouldFail)
			{
				this.replicaCreationShouldFail = replicaCreationShouldFail;
			}

			public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				if (replicaCreationShouldFail.Get())
				{
					Uninterruptibles.SleepUninterruptibly(2, TimeUnit.Seconds);
					return new ShortCircuitReplicaInfo();
				}
				return null;
			}

			private readonly AtomicBoolean replicaCreationShouldFail;
		}

		private sealed class _Runnable_243 : Runnable
		{
			public _Runnable_243(MiniDFSCluster cluster, string TestFile, int TestFileLen, CountDownLatch
				 gotFailureLatch, CountDownLatch shouldRetryLatch, AtomicBoolean testFailed)
			{
				this.cluster = cluster;
				this.TestFile = TestFile;
				this.TestFileLen = TestFileLen;
				this.gotFailureLatch = gotFailureLatch;
				this.shouldRetryLatch = shouldRetryLatch;
				this.testFailed = testFailed;
			}

			public void Run()
			{
				try
				{
					IList<LocatedBlock> locatedBlocks = cluster.GetNameNode().GetRpcServer().GetBlockLocations
						(TestFile, 0, TestFileLen).GetLocatedBlocks();
					LocatedBlock lblock = locatedBlocks[0];
					BlockReader blockReader = null;
					try
					{
						blockReader = BlockReaderTestUtil.GetBlockReader(cluster, lblock, 0, TestFileLen);
						NUnit.Framework.Assert.Fail("expected getBlockReader to fail the first time.");
					}
					catch (Exception t)
					{
						NUnit.Framework.Assert.IsTrue("expected to see 'TCP reads were disabled " + "for testing' in exception "
							 + t, t.Message.Contains("TCP reads were disabled for testing"));
					}
					finally
					{
						if (blockReader != null)
						{
							blockReader.Close();
						}
					}
					gotFailureLatch.CountDown();
					shouldRetryLatch.Await();
					try
					{
						blockReader = BlockReaderTestUtil.GetBlockReader(cluster, lblock, 0, TestFileLen);
					}
					catch (Exception t)
					{
						TestBlockReaderFactory.Log.Error("error trying to retrieve a block reader " + "the second time."
							, t);
						throw;
					}
					finally
					{
						if (blockReader != null)
						{
							blockReader.Close();
						}
					}
				}
				catch (Exception t)
				{
					TestBlockReaderFactory.Log.Error("getBlockReader failure", t);
					testFailed.Set(true);
				}
			}

			private readonly MiniDFSCluster cluster;

			private readonly string TestFile;

			private readonly int TestFileLen;

			private readonly CountDownLatch gotFailureLatch;

			private readonly CountDownLatch shouldRetryLatch;

			private readonly AtomicBoolean testFailed;
		}

		/// <summary>
		/// Test that a client which supports short-circuit reads using
		/// shared memory can fall back to not using shared memory when
		/// the server doesn't support it.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitReadFromServerWithoutShm()
		{
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration clientConf = CreateShortCircuitConf("testShortCircuitReadFromServerWithoutShm"
				, sockDir);
			Configuration serverConf = new Configuration(clientConf);
			serverConf.SetInt(DFSConfigKeys.DfsShortCircuitSharedMemoryWatcherInterruptCheckMs
				, 0);
			DFSInputStream.tcpReadsDisabledForTesting = true;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(serverConf).NumDataNodes(1).Build
				();
			cluster.WaitActive();
			clientConf.Set(DFSConfigKeys.DfsClientContext, "testShortCircuitReadFromServerWithoutShm_clientContext"
				);
			DistributedFileSystem fs = (DistributedFileSystem)FileSystem.Get(cluster.GetURI(0
				), clientConf);
			string TestFile = "/test_file";
			int TestFileLen = 4000;
			int Seed = unchecked((int)(0xFADEC));
			DFSTestUtil.CreateFile(fs, new Path(TestFile), TestFileLen, (short)1, Seed);
			byte[] contents = DFSTestUtil.ReadFileBuffer(fs, new Path(TestFile));
			byte[] expected = DFSTestUtil.CalculateFileContentsFromSeed(Seed, TestFileLen);
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(contents, expected));
			ShortCircuitCache cache = fs.dfs.GetClientContext().GetShortCircuitCache();
			DatanodeInfo datanode = new DatanodeInfo(cluster.GetDataNodes()[0].GetDatanodeId(
				));
			cache.GetDfsClientShmManager().Visit(new _Visitor_334(datanode));
			cluster.Shutdown();
			sockDir.Close();
		}

		private sealed class _Visitor_334 : DfsClientShmManager.Visitor
		{
			public _Visitor_334(DatanodeInfo datanode)
			{
				this.datanode = datanode;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Visit(Dictionary<DatanodeInfo, DfsClientShmManager.PerDatanodeVisitorInfo
				> info)
			{
				NUnit.Framework.Assert.AreEqual(1, info.Count);
				DfsClientShmManager.PerDatanodeVisitorInfo vinfo = info[datanode];
				NUnit.Framework.Assert.IsTrue(vinfo.disabled);
				NUnit.Framework.Assert.AreEqual(0, vinfo.full.Count);
				NUnit.Framework.Assert.AreEqual(0, vinfo.notFull.Count);
			}

			private readonly DatanodeInfo datanode;
		}

		/// <summary>
		/// Test that a client which does not support short-circuit reads using
		/// shared memory can talk with a server which supports it.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitReadFromClientWithoutShm()
		{
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration clientConf = CreateShortCircuitConf("testShortCircuitReadWithoutShm"
				, sockDir);
			Configuration serverConf = new Configuration(clientConf);
			DFSInputStream.tcpReadsDisabledForTesting = true;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(serverConf).NumDataNodes(1).Build
				();
			cluster.WaitActive();
			clientConf.SetInt(DFSConfigKeys.DfsShortCircuitSharedMemoryWatcherInterruptCheckMs
				, 0);
			clientConf.Set(DFSConfigKeys.DfsClientContext, "testShortCircuitReadFromClientWithoutShm_clientContext"
				);
			DistributedFileSystem fs = (DistributedFileSystem)FileSystem.Get(cluster.GetURI(0
				), clientConf);
			string TestFile = "/test_file";
			int TestFileLen = 4000;
			int Seed = unchecked((int)(0xFADEC));
			DFSTestUtil.CreateFile(fs, new Path(TestFile), TestFileLen, (short)1, Seed);
			byte[] contents = DFSTestUtil.ReadFileBuffer(fs, new Path(TestFile));
			byte[] expected = DFSTestUtil.CalculateFileContentsFromSeed(Seed, TestFileLen);
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(contents, expected));
			ShortCircuitCache cache = fs.dfs.GetClientContext().GetShortCircuitCache();
			NUnit.Framework.Assert.AreEqual(null, cache.GetDfsClientShmManager());
			cluster.Shutdown();
			sockDir.Close();
		}

		/// <summary>Test shutting down the ShortCircuitCache while there are things in it.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortCircuitCacheShutdown()
		{
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration conf = CreateShortCircuitConf("testShortCircuitCacheShutdown", sockDir
				);
			conf.Set(DFSConfigKeys.DfsClientContext, "testShortCircuitCacheShutdown");
			Configuration serverConf = new Configuration(conf);
			DFSInputStream.tcpReadsDisabledForTesting = true;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(serverConf).NumDataNodes(1).Build
				();
			cluster.WaitActive();
			DistributedFileSystem fs = (DistributedFileSystem)FileSystem.Get(cluster.GetURI(0
				), conf);
			string TestFile = "/test_file";
			int TestFileLen = 4000;
			int Seed = unchecked((int)(0xFADEC));
			DFSTestUtil.CreateFile(fs, new Path(TestFile), TestFileLen, (short)1, Seed);
			byte[] contents = DFSTestUtil.ReadFileBuffer(fs, new Path(TestFile));
			byte[] expected = DFSTestUtil.CalculateFileContentsFromSeed(Seed, TestFileLen);
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(contents, expected));
			ShortCircuitCache cache = fs.dfs.GetClientContext().GetShortCircuitCache();
			cache.Close();
			NUnit.Framework.Assert.IsTrue(cache.GetDfsClientShmManager().GetDomainSocketWatcher
				().IsClosed());
			cluster.Shutdown();
			sockDir.Close();
		}

		/// <summary>
		/// When an InterruptedException is sent to a thread calling
		/// FileChannel#read, the FileChannel is immediately closed and the
		/// thread gets an exception.
		/// </summary>
		/// <remarks>
		/// When an InterruptedException is sent to a thread calling
		/// FileChannel#read, the FileChannel is immediately closed and the
		/// thread gets an exception.  This effectively means that we might have
		/// someone asynchronously calling close() on the file descriptors we use
		/// in BlockReaderLocal.  So when unreferencing a ShortCircuitReplica in
		/// ShortCircuitCache#unref, we should check if the FileChannel objects
		/// are still open.  If not, we should purge the replica to avoid giving
		/// it out to any future readers.
		/// This is a regression test for HDFS-6227: Short circuit read failed
		/// due to ClosedChannelException.
		/// Note that you may still get ClosedChannelException errors if two threads
		/// are reading from the same replica and an InterruptedException is delivered
		/// to one of them.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestPurgingClosedReplicas()
		{
			BlockReaderTestUtil.EnableBlockReaderFactoryTracing();
			AtomicInteger replicasCreated = new AtomicInteger(0);
			AtomicBoolean testFailed = new AtomicBoolean(false);
			DFSInputStream.tcpReadsDisabledForTesting = true;
			BlockReaderFactory.createShortCircuitReplicaInfoCallback = new _ShortCircuitReplicaCreator_443
				(replicasCreated);
			TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
			Configuration conf = CreateShortCircuitConf("testPurgingClosedReplicas", sockDir);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			string TestFile = "/test_file";
			int TestFileLen = 4095;
			int Seed = unchecked((int)(0xFADE0));
			DistributedFileSystem fs = (DistributedFileSystem)FileSystem.Get(cluster.GetURI(0
				), conf);
			DFSTestUtil.CreateFile(fs, new Path(TestFile), TestFileLen, (short)1, Seed);
			Semaphore sem = Sharpen.Extensions.CreateSemaphore(0);
			IList<LocatedBlock> locatedBlocks = cluster.GetNameNode().GetRpcServer().GetBlockLocations
				(TestFile, 0, TestFileLen).GetLocatedBlocks();
			LocatedBlock lblock = locatedBlocks[0];
			// first block
			byte[] buf = new byte[TestFileLen];
			Runnable readerRunnable = new _Runnable_471(cluster, lblock, TestFileLen, sem, buf
				, testFailed);
			Sharpen.Thread thread = new Sharpen.Thread(readerRunnable);
			thread.Start();
			// While the thread is reading, send it interrupts.
			// These should trigger a ClosedChannelException.
			while (thread.IsAlive())
			{
				sem.AcquireUninterruptibly();
				thread.Interrupt();
				sem.Release();
			}
			NUnit.Framework.Assert.IsFalse(testFailed.Get());
			// We should be able to read from the file without
			// getting a ClosedChannelException.
			BlockReader blockReader = null;
			try
			{
				blockReader = BlockReaderTestUtil.GetBlockReader(cluster, lblock, 0, TestFileLen);
				blockReader.ReadFully(buf, 0, TestFileLen);
			}
			finally
			{
				if (blockReader != null)
				{
					blockReader.Close();
				}
			}
			byte[] expected = DFSTestUtil.CalculateFileContentsFromSeed(Seed, TestFileLen);
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(buf, expected));
			// Another ShortCircuitReplica object should have been created.
			NUnit.Framework.Assert.AreEqual(2, replicasCreated.Get());
			dfs.Close();
			cluster.Shutdown();
			sockDir.Close();
		}

		private sealed class _ShortCircuitReplicaCreator_443 : ShortCircuitCache.ShortCircuitReplicaCreator
		{
			public _ShortCircuitReplicaCreator_443(AtomicInteger replicasCreated)
			{
				this.replicasCreated = replicasCreated;
			}

			public ShortCircuitReplicaInfo CreateShortCircuitReplicaInfo()
			{
				replicasCreated.IncrementAndGet();
				return null;
			}

			private readonly AtomicInteger replicasCreated;
		}

		private sealed class _Runnable_471 : Runnable
		{
			public _Runnable_471(MiniDFSCluster cluster, LocatedBlock lblock, int TestFileLen
				, Semaphore sem, byte[] buf, AtomicBoolean testFailed)
			{
				this.cluster = cluster;
				this.lblock = lblock;
				this.TestFileLen = TestFileLen;
				this.sem = sem;
				this.buf = buf;
				this.testFailed = testFailed;
			}

			public void Run()
			{
				try
				{
					while (true)
					{
						BlockReader blockReader = null;
						try
						{
							blockReader = BlockReaderTestUtil.GetBlockReader(cluster, lblock, 0, TestFileLen);
							sem.Release();
							try
							{
								blockReader.ReadAll(buf, 0, TestFileLen);
							}
							finally
							{
								sem.AcquireUninterruptibly();
							}
						}
						catch (ClosedByInterruptException e)
						{
							TestBlockReaderFactory.Log.Info("got the expected ClosedByInterruptException", e);
							sem.Release();
							break;
						}
						finally
						{
							if (blockReader != null)
							{
								blockReader.Close();
							}
						}
						TestBlockReaderFactory.Log.Info("read another " + TestFileLen + " bytes.");
					}
				}
				catch (Exception t)
				{
					TestBlockReaderFactory.Log.Error("getBlockReader failure", t);
					testFailed.Set(true);
					sem.Release();
				}
			}

			private readonly MiniDFSCluster cluster;

			private readonly LocatedBlock lblock;

			private readonly int TestFileLen;

			private readonly Semaphore sem;

			private readonly byte[] buf;

			private readonly AtomicBoolean testFailed;
		}
	}
}
