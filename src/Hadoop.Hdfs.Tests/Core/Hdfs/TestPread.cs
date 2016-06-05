using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Log4j;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests the DFS positional read functionality in a single node
	/// mini-cluster.
	/// </summary>
	public class TestPread
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 4096;

		internal bool simulatedStorage;

		internal bool isHedgedRead;

		[SetUp]
		public virtual void Setup()
		{
			simulatedStorage = false;
			isHedgedRead = false;
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name)
		{
			int replication = 3;
			// We need > 1 blocks to test out the hedged reads.
			// create and write a file that contains three blocks of data
			DataOutputStream stm = fileSys.Create(name, true, 4096, (short)replication, blockSize
				);
			// test empty file open and read
			stm.Close();
			FSDataInputStream @in = fileSys.Open(name);
			byte[] buffer = new byte[12 * blockSize];
			@in.ReadFully(0, buffer, 0, 0);
			IOException res = null;
			try
			{
				// read beyond the end of the file
				@in.ReadFully(0, buffer, 0, 1);
			}
			catch (IOException e)
			{
				// should throw an exception
				res = e;
			}
			NUnit.Framework.Assert.IsTrue("Error reading beyond file boundary.", res != null);
			@in.Close();
			if (!fileSys.Delete(name, true))
			{
				NUnit.Framework.Assert.IsTrue("Cannot delete file", false);
			}
			// now create the real file
			DFSTestUtil.CreateFile(fileSys, name, 12 * blockSize, 12 * blockSize, blockSize, 
				(short)replication, seed);
		}

		private void CheckAndEraseData(byte[] actual, int from, byte[] expected, string message
			)
		{
			for (int idx = 0; idx < actual.Length; idx++)
			{
				NUnit.Framework.Assert.AreEqual(message + " byte " + (from + idx) + " differs. expected "
					 + expected[from + idx] + " actual " + actual[idx], actual[idx], expected[from +
					 idx]);
				actual[idx] = 0;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoPread(FSDataInputStream stm, long position, byte[] buffer, int offset
			, int length)
		{
			int nread = 0;
			long totalRead = 0;
			DFSInputStream dfstm = null;
			if (stm.GetWrappedStream() is DFSInputStream)
			{
				dfstm = (DFSInputStream)(stm.GetWrappedStream());
				totalRead = dfstm.GetReadStatistics().GetTotalBytesRead();
			}
			while (nread < length)
			{
				int nbytes = stm.Read(position + nread, buffer, offset + nread, length - nread);
				NUnit.Framework.Assert.IsTrue("Error in pread", nbytes > 0);
				nread += nbytes;
			}
			if (dfstm != null)
			{
				if (isHedgedRead)
				{
					NUnit.Framework.Assert.IsTrue("Expected read statistic to be incremented", length
						 <= dfstm.GetReadStatistics().GetTotalBytesRead() - totalRead);
				}
				else
				{
					NUnit.Framework.Assert.AreEqual("Expected read statistic to be incremented", length
						, dfstm.GetReadStatistics().GetTotalBytesRead() - totalRead);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void PReadFile(FileSystem fileSys, Path name)
		{
			FSDataInputStream stm = fileSys.Open(name);
			byte[] expected = new byte[12 * blockSize];
			if (simulatedStorage)
			{
				for (int i = 0; i < expected.Length; i++)
				{
					expected[i] = SimulatedFSDataset.DefaultDatabyte;
				}
			}
			else
			{
				Random rand = new Random(seed);
				rand.NextBytes(expected);
			}
			// do a sanity check. Read first 4K bytes
			byte[] actual = new byte[4096];
			stm.ReadFully(actual);
			CheckAndEraseData(actual, 0, expected, "Read Sanity Test");
			// now do a pread for the first 8K bytes
			actual = new byte[8192];
			DoPread(stm, 0L, actual, 0, 8192);
			CheckAndEraseData(actual, 0, expected, "Pread Test 1");
			// Now check to see if the normal read returns 4K-8K byte range
			actual = new byte[4096];
			stm.ReadFully(actual);
			CheckAndEraseData(actual, 4096, expected, "Pread Test 2");
			// Now see if we can cross a single block boundary successfully
			// read 4K bytes from blockSize - 2K offset
			stm.ReadFully(blockSize - 2048, actual, 0, 4096);
			CheckAndEraseData(actual, (blockSize - 2048), expected, "Pread Test 3");
			// now see if we can cross two block boundaries successfully
			// read blockSize + 4K bytes from blockSize - 2K offset
			actual = new byte[blockSize + 4096];
			stm.ReadFully(blockSize - 2048, actual);
			CheckAndEraseData(actual, (blockSize - 2048), expected, "Pread Test 4");
			// now see if we can cross two block boundaries that are not cached
			// read blockSize + 4K bytes from 10*blockSize - 2K offset
			actual = new byte[blockSize + 4096];
			stm.ReadFully(10 * blockSize - 2048, actual);
			CheckAndEraseData(actual, (10 * blockSize - 2048), expected, "Pread Test 5");
			// now check that even after all these preads, we can still read
			// bytes 8K-12K
			actual = new byte[4096];
			stm.ReadFully(actual);
			CheckAndEraseData(actual, 8192, expected, "Pread Test 6");
			// done
			stm.Close();
			// check block location caching
			stm = fileSys.Open(name);
			stm.ReadFully(1, actual, 0, 4096);
			stm.ReadFully(4 * blockSize, actual, 0, 4096);
			stm.ReadFully(7 * blockSize, actual, 0, 4096);
			actual = new byte[3 * 4096];
			stm.ReadFully(0 * blockSize, actual, 0, 3 * 4096);
			CheckAndEraseData(actual, 0, expected, "Pread Test 7");
			actual = new byte[8 * 4096];
			stm.ReadFully(3 * blockSize, actual, 0, 8 * 4096);
			CheckAndEraseData(actual, 3 * blockSize, expected, "Pread Test 8");
			// read the tail
			stm.ReadFully(11 * blockSize + blockSize / 2, actual, 0, blockSize / 2);
			IOException res = null;
			try
			{
				// read beyond the end of the file
				stm.ReadFully(11 * blockSize + blockSize / 2, actual, 0, blockSize);
			}
			catch (IOException e)
			{
				// should throw an exception
				res = e;
			}
			NUnit.Framework.Assert.IsTrue("Error reading beyond file boundary.", res != null);
			stm.Close();
		}

		// test pread can survive datanode restarts
		/// <exception cref="System.IO.IOException"/>
		private void DatanodeRestartTest(MiniDFSCluster cluster, FileSystem fileSys, Path
			 name)
		{
			// skip this test if using simulated storage since simulated blocks
			// don't survive datanode restarts.
			if (simulatedStorage)
			{
				return;
			}
			int numBlocks = 1;
			NUnit.Framework.Assert.IsTrue(numBlocks <= DFSConfigKeys.DfsClientMaxBlockAcquireFailuresDefault
				);
			byte[] expected = new byte[numBlocks * blockSize];
			Random rand = new Random(seed);
			rand.NextBytes(expected);
			byte[] actual = new byte[numBlocks * blockSize];
			FSDataInputStream stm = fileSys.Open(name);
			// read a block and get block locations cached as a result
			stm.ReadFully(0, actual);
			CheckAndEraseData(actual, 0, expected, "Pread Datanode Restart Setup");
			// restart all datanodes. it is expected that they will
			// restart on different ports, hence, cached block locations
			// will no longer work.
			NUnit.Framework.Assert.IsTrue(cluster.RestartDataNodes());
			cluster.WaitActive();
			// verify the block can be read again using the same InputStream 
			// (via re-fetching of block locations from namenode). there is a 
			// 3 sec sleep in chooseDataNode(), which can be shortened for 
			// this test if configurable.
			stm.ReadFully(0, actual);
			CheckAndEraseData(actual, 0, expected, "Pread Datanode Restart Test");
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			NUnit.Framework.Assert.IsTrue(fileSys.Delete(name, true));
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		private Callable<Void> GetPReadFileCallable(FileSystem fileSys, Path file)
		{
			return new _Callable_239(this, fileSys, file);
		}

		private sealed class _Callable_239 : Callable<Void>
		{
			public _Callable_239(TestPread _enclosing, FileSystem fileSys, Path file)
			{
				this._enclosing = _enclosing;
				this.fileSys = fileSys;
				this.file = file;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				this._enclosing.PReadFile(fileSys, file);
				return null;
			}

			private readonly TestPread _enclosing;

			private readonly FileSystem fileSys;

			private readonly Path file;
		}

		/// <summary>Tests positional read in DFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPreadDFS()
		{
			Configuration conf = new Configuration();
			DfsPreadTest(conf, false, true);
			// normal pread
			DfsPreadTest(conf, true, true);
		}

		// trigger read code path without
		// transferTo.
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPreadDFSNoChecksum()
		{
			Configuration conf = new Configuration();
			((Log4JLogger)DataTransferProtocol.Log).GetLogger().SetLevel(Level.All);
			DfsPreadTest(conf, false, false);
			DfsPreadTest(conf, true, false);
		}

		/// <summary>Tests positional read in DFS, with hedged reads enabled.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHedgedPreadDFSBasic()
		{
			isHedgedRead = true;
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsDfsclientHedgedReadThreadpoolSize, 5);
			conf.SetLong(DFSConfigKeys.DfsDfsclientHedgedReadThresholdMillis, 1);
			DfsPreadTest(conf, false, true);
			// normal pread
			DfsPreadTest(conf, true, true);
		}

		// trigger read code path without
		// transferTo.
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHedgedReadLoopTooManyTimes()
		{
			Configuration conf = new Configuration();
			int numHedgedReadPoolThreads = 5;
			int hedgedReadTimeoutMillis = 50;
			conf.SetInt(DFSConfigKeys.DfsDfsclientHedgedReadThreadpoolSize, numHedgedReadPoolThreads
				);
			conf.SetLong(DFSConfigKeys.DfsDfsclientHedgedReadThresholdMillis, hedgedReadTimeoutMillis
				);
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 0);
			// Set up the InjectionHandler
			DFSClientFaultInjector.instance = Org.Mockito.Mockito.Mock<DFSClientFaultInjector
				>();
			DFSClientFaultInjector injector = DFSClientFaultInjector.instance;
			int sleepMs = 100;
			Org.Mockito.Mockito.DoAnswer(new _Answer_296(hedgedReadTimeoutMillis, sleepMs)).When
				(injector).FetchFromDatanodeException();
			Org.Mockito.Mockito.DoAnswer(new _Answer_309(sleepMs)).When(injector).ReadFromDatanodeDelay
				();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Format(
				true).Build();
			DistributedFileSystem fileSys = cluster.GetFileSystem();
			DFSClient dfsClient = fileSys.GetClient();
			FSDataOutputStream output = null;
			DFSInputStream input = null;
			string filename = "/hedgedReadMaxOut.dat";
			try
			{
				Path file = new Path(filename);
				output = fileSys.Create(file, (short)2);
				byte[] data = new byte[64 * 1024];
				output.Write(data);
				output.Flush();
				output.Write(data);
				output.Flush();
				output.Write(data);
				output.Flush();
				output.Close();
				byte[] buffer = new byte[64 * 1024];
				input = dfsClient.Open(filename);
				input.Read(0, buffer, 0, 1024);
				input.Close();
				NUnit.Framework.Assert.AreEqual(3, input.GetHedgedReadOpsLoopNumForTesting());
			}
			catch (BlockMissingException)
			{
				NUnit.Framework.Assert.IsTrue(false);
			}
			finally
			{
				Org.Mockito.Mockito.Reset(injector);
				IOUtils.Cleanup(null, input);
				IOUtils.Cleanup(null, output);
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		private sealed class _Answer_296 : Answer<Void>
		{
			public _Answer_296(int hedgedReadTimeoutMillis, int sleepMs)
			{
				this.hedgedReadTimeoutMillis = hedgedReadTimeoutMillis;
				this.sleepMs = sleepMs;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				if (true)
				{
					Sharpen.Thread.Sleep(hedgedReadTimeoutMillis + sleepMs);
					if (DFSClientFaultInjector.exceptionNum.CompareAndSet(0, 1))
					{
						System.Console.Out.WriteLine("-------------- throw Checksum Exception");
						throw new ChecksumException("ChecksumException test", 100);
					}
				}
				return null;
			}

			private readonly int hedgedReadTimeoutMillis;

			private readonly int sleepMs;
		}

		private sealed class _Answer_309 : Answer<Void>
		{
			public _Answer_309(int sleepMs)
			{
				this.sleepMs = sleepMs;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				if (true)
				{
					Sharpen.Thread.Sleep(sleepMs * 2);
				}
				return null;
			}

			private readonly int sleepMs;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.ExecutionException"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxOutHedgedReadPool()
		{
			isHedgedRead = true;
			Configuration conf = new Configuration();
			int numHedgedReadPoolThreads = 5;
			int initialHedgedReadTimeoutMillis = 50000;
			int fixedSleepIntervalMillis = 50;
			conf.SetInt(DFSConfigKeys.DfsDfsclientHedgedReadThreadpoolSize, numHedgedReadPoolThreads
				);
			conf.SetLong(DFSConfigKeys.DfsDfsclientHedgedReadThresholdMillis, initialHedgedReadTimeoutMillis
				);
			// Set up the InjectionHandler
			DFSClientFaultInjector.instance = Org.Mockito.Mockito.Mock<DFSClientFaultInjector
				>();
			DFSClientFaultInjector injector = DFSClientFaultInjector.instance;
			// make preads sleep for 50ms
			Org.Mockito.Mockito.DoAnswer(new _Answer_372(fixedSleepIntervalMillis)).When(injector
				).StartFetchFromDatanode();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Format(
				true).Build();
			DistributedFileSystem fileSys = cluster.GetFileSystem();
			DFSClient dfsClient = fileSys.GetClient();
			DFSHedgedReadMetrics metrics = dfsClient.GetHedgedReadMetrics();
			// Metrics instance is static, so we need to reset counts from prior tests.
			metrics.hedgedReadOps.Set(0);
			metrics.hedgedReadOpsWin.Set(0);
			metrics.hedgedReadOpsInCurThread.Set(0);
			try
			{
				Path file1 = new Path("hedgedReadMaxOut.dat");
				WriteFile(fileSys, file1);
				// Basic test. Reads complete within timeout. Assert that there were no
				// hedged reads.
				PReadFile(fileSys, file1);
				// assert that there were no hedged reads. 50ms + delta < 500ms
				NUnit.Framework.Assert.IsTrue(metrics.GetHedgedReadOps() == 0);
				NUnit.Framework.Assert.IsTrue(metrics.GetHedgedReadOpsInCurThread() == 0);
				/*
				* Reads take longer than timeout. But, only one thread reading. Assert
				* that there were hedged reads. But, none of the reads had to run in the
				* current thread.
				*/
				dfsClient.SetHedgedReadTimeout(50);
				// 50ms
				PReadFile(fileSys, file1);
				// assert that there were hedged reads
				NUnit.Framework.Assert.IsTrue(metrics.GetHedgedReadOps() > 0);
				NUnit.Framework.Assert.IsTrue(metrics.GetHedgedReadOpsInCurThread() == 0);
				/*
				* Multiple threads reading. Reads take longer than timeout. Assert that
				* there were hedged reads. And that reads had to run in the current
				* thread.
				*/
				int factor = 10;
				int numHedgedReads = numHedgedReadPoolThreads * factor;
				long initialReadOpsValue = metrics.GetHedgedReadOps();
				ExecutorService executor = Executors.NewFixedThreadPool(numHedgedReads);
				AList<Future<Void>> futures = new AList<Future<Void>>();
				for (int i = 0; i < numHedgedReads; i++)
				{
					futures.AddItem(executor.Submit(GetPReadFileCallable(fileSys, file1)));
				}
				for (int i_1 = 0; i_1 < numHedgedReads; i_1++)
				{
					futures[i_1].Get();
				}
				NUnit.Framework.Assert.IsTrue(metrics.GetHedgedReadOps() > initialReadOpsValue);
				NUnit.Framework.Assert.IsTrue(metrics.GetHedgedReadOpsInCurThread() > 0);
				CleanupFile(fileSys, file1);
				executor.Shutdown();
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
				Org.Mockito.Mockito.Reset(injector);
			}
		}

		private sealed class _Answer_372 : Answer<Void>
		{
			public _Answer_372(int fixedSleepIntervalMillis)
			{
				this.fixedSleepIntervalMillis = fixedSleepIntervalMillis;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				Sharpen.Thread.Sleep(fixedSleepIntervalMillis);
				return null;
			}

			private readonly int fixedSleepIntervalMillis;
		}

		/// <exception cref="System.IO.IOException"/>
		private void DfsPreadTest(Configuration conf, bool disableTransferTo, bool verifyChecksum
			)
		{
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 4096);
			conf.SetLong(DFSConfigKeys.DfsClientReadPrefetchSizeKey, 4096);
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 0);
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			if (disableTransferTo)
			{
				conf.SetBoolean("dfs.datanode.transferTo.allowed", false);
			}
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			FileSystem fileSys = cluster.GetFileSystem();
			fileSys.SetVerifyChecksum(verifyChecksum);
			try
			{
				Path file1 = new Path("preadtest.dat");
				WriteFile(fileSys, file1);
				PReadFile(fileSys, file1);
				DatanodeRestartTest(cluster, fileSys, file1);
				CleanupFile(fileSys, file1);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPreadDFSSimulated()
		{
			simulatedStorage = true;
			TestPreadDFS();
		}

		/// <summary>Tests positional read in LocalFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPreadLocalFS()
		{
			Configuration conf = new HdfsConfiguration();
			FileSystem fileSys = FileSystem.GetLocal(conf);
			try
			{
				Path file1 = new Path("build/test/data", "preadtest.dat");
				WriteFile(fileSys, file1);
				PReadFile(fileSys, file1);
				CleanupFile(fileSys, file1);
			}
			finally
			{
				fileSys.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new TestPread().TestPreadDFS();
		}
	}
}
