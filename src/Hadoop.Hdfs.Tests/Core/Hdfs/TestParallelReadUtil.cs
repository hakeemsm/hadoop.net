using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// Driver class for testing the use of DFSInputStream by multiple concurrent
	/// readers, using the different read APIs.
	/// </summary>
	/// <remarks>
	/// Driver class for testing the use of DFSInputStream by multiple concurrent
	/// readers, using the different read APIs.
	/// This class is marked as @Ignore so that junit doesn't try to execute the
	/// tests in here directly.  They are executed from subclasses.
	/// </remarks>
	public class TestParallelReadUtil
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestParallelReadUtil)
			);

		internal static BlockReaderTestUtil util = null;

		internal static DFSClient dfsClient = null;

		internal const int FileSizeK = 256;

		internal static Random rand = null;

		internal const int DefaultReplicationFactor = 2;

		protected internal bool verifyChecksums = true;

		static TestParallelReadUtil()
		{
			// The client-trace log ends up causing a lot of blocking threads
			// in this when it's being used as a performance benchmark.
			LogManager.GetLogger(typeof(DataNode).FullName + ".clienttrace").SetLevel(Level.Warn
				);
		}

		private class TestFileInfo
		{
			public DFSInputStream dis;

			public Path filepath;

			public byte[] authenticData;

			internal TestFileInfo(TestParallelReadUtil _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestParallelReadUtil _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public static void SetupCluster(int replicationFactor, HdfsConfiguration conf)
		{
			util = new BlockReaderTestUtil(replicationFactor, conf);
			dfsClient = util.GetDFSClient();
			long seed = Time.Now();
			Log.Info("Random seed: " + seed);
			rand = new Random(seed);
		}

		/// <summary>Providers of this interface implement two different read APIs.</summary>
		/// <remarks>
		/// Providers of this interface implement two different read APIs. Instances of
		/// this interface are shared across all ReadWorkerThreads, so should be stateless.
		/// </remarks>
		internal interface ReadWorkerHelper
		{
			/// <exception cref="System.IO.IOException"/>
			int Read(DFSInputStream dis, byte[] target, int startOff, int len);

			/// <exception cref="System.IO.IOException"/>
			int PRead(DFSInputStream dis, byte[] target, int startOff, int len);
		}

		/// <summary>Uses read(ByteBuffer...) style APIs</summary>
		internal class DirectReadWorkerHelper : TestParallelReadUtil.ReadWorkerHelper
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual int Read(DFSInputStream dis, byte[] target, int startOff, int len)
			{
				ByteBuffer bb = ByteBuffer.AllocateDirect(target.Length);
				int cnt = 0;
				lock (dis)
				{
					dis.Seek(startOff);
					while (cnt < len)
					{
						int read = dis.Read(bb);
						if (read == -1)
						{
							return read;
						}
						cnt += read;
					}
				}
				bb.Clear();
				bb.Get(target);
				return cnt;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int PRead(DFSInputStream dis, byte[] target, int startOff, int len
				)
			{
				// No pRead for bb read path
				return Read(dis, target, startOff, len);
			}
		}

		/// <summary>Uses the read(byte[]...) style APIs</summary>
		internal class CopyingReadWorkerHelper : TestParallelReadUtil.ReadWorkerHelper
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual int Read(DFSInputStream dis, byte[] target, int startOff, int len)
			{
				int cnt = 0;
				lock (dis)
				{
					dis.Seek(startOff);
					while (cnt < len)
					{
						int read = dis.Read(target, cnt, len - cnt);
						if (read == -1)
						{
							return read;
						}
						cnt += read;
					}
				}
				return cnt;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int PRead(DFSInputStream dis, byte[] target, int startOff, int len
				)
			{
				int cnt = 0;
				while (cnt < len)
				{
					int read = dis.Read(startOff, target, cnt, len - cnt);
					if (read == -1)
					{
						return read;
					}
					cnt += read;
				}
				return cnt;
			}
		}

		/// <summary>Uses a mix of both copying</summary>
		internal class MixedWorkloadHelper : TestParallelReadUtil.ReadWorkerHelper
		{
			private readonly TestParallelReadUtil.DirectReadWorkerHelper bb = new TestParallelReadUtil.DirectReadWorkerHelper
				();

			private readonly TestParallelReadUtil.CopyingReadWorkerHelper copy = new TestParallelReadUtil.CopyingReadWorkerHelper
				();

			private readonly double CopyingProbability = 0.5;

			/// <exception cref="System.IO.IOException"/>
			public virtual int Read(DFSInputStream dis, byte[] target, int startOff, int len)
			{
				double p = rand.NextDouble();
				if (p > CopyingProbability)
				{
					return bb.Read(dis, target, startOff, len);
				}
				else
				{
					return copy.Read(dis, target, startOff, len);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int PRead(DFSInputStream dis, byte[] target, int startOff, int len
				)
			{
				double p = rand.NextDouble();
				if (p > CopyingProbability)
				{
					return bb.PRead(dis, target, startOff, len);
				}
				else
				{
					return copy.PRead(dis, target, startOff, len);
				}
			}
		}

		/// <summary>A worker to do one "unit" of read.</summary>
		internal class ReadWorker : Sharpen.Thread
		{
			public const int NIterations = 1024;

			private const double ProportionNonPositionalRead = 0.10;

			private readonly TestParallelReadUtil.TestFileInfo testInfo;

			private readonly long fileSize;

			private long bytesRead;

			private bool error;

			private readonly TestParallelReadUtil.ReadWorkerHelper helper;

			internal ReadWorker(TestParallelReadUtil.TestFileInfo testInfo, int id, TestParallelReadUtil.ReadWorkerHelper
				 helper)
				: base("ReadWorker-" + id + "-" + testInfo.filepath.ToString())
			{
				this.testInfo = testInfo;
				this.helper = helper;
				fileSize = testInfo.dis.GetFileLength();
				NUnit.Framework.Assert.AreEqual(fileSize, testInfo.authenticData.Length);
				bytesRead = 0;
				error = false;
			}

			/// <summary>Randomly do one of (1) Small read; and (2) Large Pread.</summary>
			public override void Run()
			{
				for (int i = 0; i < NIterations; ++i)
				{
					int startOff = rand.Next((int)fileSize);
					int len = 0;
					try
					{
						double p = rand.NextDouble();
						if (p < ProportionNonPositionalRead)
						{
							// Do a small regular read. Very likely this will leave unread
							// data on the socket and make the socket uncacheable.
							len = Math.Min(rand.Next(64), (int)fileSize - startOff);
							Read(startOff, len);
							bytesRead += len;
						}
						else
						{
							// Do a positional read most of the time.
							len = rand.Next((int)(fileSize - startOff));
							PRead(startOff, len);
							bytesRead += len;
						}
					}
					catch (Exception t)
					{
						Log.Error(GetName() + ": Error while testing read at " + startOff + " length " + 
							len, t);
						error = true;
						NUnit.Framework.Assert.Fail(t.Message);
					}
				}
			}

			public virtual long GetBytesRead()
			{
				return bytesRead;
			}

			/// <summary>Raising error in a thread doesn't seem to fail the test.</summary>
			/// <remarks>
			/// Raising error in a thread doesn't seem to fail the test.
			/// So check afterwards.
			/// </remarks>
			public virtual bool HasError()
			{
				return error;
			}

			internal static int readCount = 0;

			/// <summary>Seek to somewhere random and read.</summary>
			/// <exception cref="System.Exception"/>
			private void Read(int start, int len)
			{
				NUnit.Framework.Assert.IsTrue("Bad args: " + start + " + " + len + " should be <= "
					 + fileSize, start + len <= fileSize);
				readCount++;
				DFSInputStream dis = testInfo.dis;
				byte[] buf = new byte[len];
				helper.Read(dis, buf, start, len);
				VerifyData("Read data corrupted", buf, start, start + len);
			}

			/// <summary>Positional read.</summary>
			/// <exception cref="System.Exception"/>
			private void PRead(int start, int len)
			{
				NUnit.Framework.Assert.IsTrue("Bad args: " + start + " + " + len + " should be <= "
					 + fileSize, start + len <= fileSize);
				DFSInputStream dis = testInfo.dis;
				byte[] buf = new byte[len];
				helper.PRead(dis, buf, start, len);
				VerifyData("Pread data corrupted", buf, start, start + len);
			}

			/// <summary>Verify read data vs authentic data</summary>
			/// <exception cref="System.Exception"/>
			private void VerifyData(string msg, byte[] actual, int start, int end)
			{
				byte[] auth = testInfo.authenticData;
				if (end > auth.Length)
				{
					throw new Exception(msg + ": Actual array (" + end + ") is past the end of authentic data ("
						 + auth.Length + ")");
				}
				int j = start;
				for (int i = 0; i < actual.Length; ++i, ++j)
				{
					if (auth[j] != actual[i])
					{
						throw new Exception(msg + ": Arrays byte " + i + " (at offset " + j + ") differs: expect "
							 + auth[j] + " got " + actual[i]);
					}
				}
			}
		}

		/// <summary>Start the parallel read with the given parameters.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool RunParallelRead(int nFiles, int nWorkerEach, TestParallelReadUtil.ReadWorkerHelper
			 helper)
		{
			TestParallelReadUtil.ReadWorker[] workers = new TestParallelReadUtil.ReadWorker[nFiles
				 * nWorkerEach];
			TestParallelReadUtil.TestFileInfo[] testInfoArr = new TestParallelReadUtil.TestFileInfo
				[nFiles];
			// Prepare the files and workers
			int nWorkers = 0;
			for (int i = 0; i < nFiles; ++i)
			{
				TestParallelReadUtil.TestFileInfo testInfo = new TestParallelReadUtil.TestFileInfo
					(this);
				testInfoArr[i] = testInfo;
				testInfo.filepath = new Path("/TestParallelRead.dat." + i);
				testInfo.authenticData = util.WriteFile(testInfo.filepath, FileSizeK);
				testInfo.dis = dfsClient.Open(testInfo.filepath.ToString(), dfsClient.GetConf().ioBufferSize
					, verifyChecksums);
				for (int j = 0; j < nWorkerEach; ++j)
				{
					workers[nWorkers++] = new TestParallelReadUtil.ReadWorker(testInfo, nWorkers, helper
						);
				}
			}
			// Start the workers and wait
			long starttime = Time.MonotonicNow();
			foreach (TestParallelReadUtil.ReadWorker worker in workers)
			{
				worker.Start();
			}
			foreach (TestParallelReadUtil.ReadWorker worker_1 in workers)
			{
				try
				{
					worker_1.Join();
				}
				catch (Exception)
				{
				}
			}
			long endtime = Time.MonotonicNow();
			// Cleanup
			foreach (TestParallelReadUtil.TestFileInfo testInfo_1 in testInfoArr)
			{
				testInfo_1.dis.Close();
			}
			// Report
			bool res = true;
			long totalRead = 0;
			foreach (TestParallelReadUtil.ReadWorker worker_2 in workers)
			{
				long nread = worker_2.GetBytesRead();
				Log.Info("--- Report: " + worker_2.GetName() + " read " + nread + " B; " + "average "
					 + nread / TestParallelReadUtil.ReadWorker.NIterations + " B per read");
				totalRead += nread;
				if (worker_2.HasError())
				{
					res = false;
				}
			}
			double timeTakenSec = (endtime - starttime) / 1000.0;
			long totalReadKB = totalRead / 1024;
			Log.Info("=== Report: " + nWorkers + " threads read " + totalReadKB + " KB (across "
				 + nFiles + " file(s)) in " + timeTakenSec + "s; average " + totalReadKB / timeTakenSec
				 + " KB/s");
			return res;
		}

		/// <summary>
		/// Runs a standard workload using a helper class which provides the read
		/// implementation to use.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RunTestWorkload(TestParallelReadUtil.ReadWorkerHelper helper)
		{
			if (!RunParallelRead(1, 4, helper))
			{
				NUnit.Framework.Assert.Fail("Check log for errors");
			}
			if (!RunParallelRead(1, 16, helper))
			{
				NUnit.Framework.Assert.Fail("Check log for errors");
			}
			if (!RunParallelRead(2, 4, helper))
			{
				NUnit.Framework.Assert.Fail("Check log for errors");
			}
		}

		/// <exception cref="System.Exception"/>
		public static void TeardownCluster()
		{
			util.Shutdown();
		}

		/// <summary>Do parallel read several times with different number of files and threads.
		/// 	</summary>
		/// <remarks>
		/// Do parallel read several times with different number of files and threads.
		/// Note that while this is the only "test" in a junit sense, we're actually
		/// dispatching a lot more. Failures in the other methods (and other threads)
		/// need to be manually collected, which is inconvenient.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestParallelReadCopying()
		{
			RunTestWorkload(new TestParallelReadUtil.CopyingReadWorkerHelper());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestParallelReadByteBuffer()
		{
			RunTestWorkload(new TestParallelReadUtil.DirectReadWorkerHelper());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestParallelReadMixed()
		{
			RunTestWorkload(new TestParallelReadUtil.MixedWorkloadHelper());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestParallelNoChecksums()
		{
			verifyChecksums = false;
			RunTestWorkload(new TestParallelReadUtil.MixedWorkloadHelper());
		}
	}
}
