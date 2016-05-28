using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests hflushing concurrently from many threads.</summary>
	public class TestMultiThreadedHflush
	{
		internal const int blockSize = 1024 * 1024;

		private const int NumThreads = 10;

		private const int WriteSize = 517;

		private const int NumWritesPerThread = 1000;

		private byte[] toWrite = null;

		private readonly SampleQuantiles quantiles = new SampleQuantiles(new Quantile[] { 
			new Quantile(0.50, 0.050), new Quantile(0.75, 0.025), new Quantile(0.90, 0.010), 
			new Quantile(0.95, 0.005), new Quantile(0.99, 0.001) });

		/*
		* creates a file but does not close it
		*/
		/// <exception cref="System.IO.IOException"/>
		private FSDataOutputStream CreateFile(FileSystem fileSys, Path name, int repl)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blockSize);
			return stm;
		}

		private void InitBuffer(int size)
		{
			long seed = AppendTestUtil.NextLong();
			toWrite = AppendTestUtil.RandomBytes(seed, size);
		}

		private class WriterThread : Sharpen.Thread
		{
			private readonly FSDataOutputStream stm;

			private readonly AtomicReference<Exception> thrown;

			private readonly int numWrites;

			private readonly CountDownLatch countdown;

			public WriterThread(TestMultiThreadedHflush _enclosing, FSDataOutputStream stm, AtomicReference
				<Exception> thrown, CountDownLatch countdown, int numWrites)
			{
				this._enclosing = _enclosing;
				this.stm = stm;
				this.thrown = thrown;
				this.numWrites = numWrites;
				this.countdown = countdown;
			}

			public override void Run()
			{
				try
				{
					this.countdown.Await();
					for (int i = 0; i < this.numWrites && this.thrown.Get() == null; i++)
					{
						this.DoAWrite();
					}
				}
				catch (Exception t)
				{
					this.thrown.CompareAndSet(null, t);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void DoAWrite()
			{
				StopWatch sw = new StopWatch().Start();
				this.stm.Write(this._enclosing.toWrite);
				this.stm.Hflush();
				long micros = sw.Now(TimeUnit.Microseconds);
				this._enclosing.quantiles.Insert(micros);
			}

			private readonly TestMultiThreadedHflush _enclosing;
		}

		/// <summary>Test case where a bunch of threads are both appending and flushing.</summary>
		/// <remarks>
		/// Test case where a bunch of threads are both appending and flushing.
		/// They all finish before the file is closed.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleHflushersRepl1()
		{
			DoTestMultipleHflushers(1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleHflushersRepl3()
		{
			DoTestMultipleHflushers(3);
		}

		/// <exception cref="System.Exception"/>
		private void DoTestMultipleHflushers(int repl)
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(repl).Build
				();
			FileSystem fs = cluster.GetFileSystem();
			Path p = new Path("/multiple-hflushers.dat");
			try
			{
				DoMultithreadedWrites(conf, p, NumThreads, WriteSize, NumWritesPerThread, repl);
				System.Console.Out.WriteLine("Latency quantiles (in microseconds):\n" + quantiles
					);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test case where a bunch of threads are continuously calling hflush() while another
		/// thread appends some data and then closes the file.
		/// </summary>
		/// <remarks>
		/// Test case where a bunch of threads are continuously calling hflush() while another
		/// thread appends some data and then closes the file.
		/// The hflushing threads should eventually catch an IOException stating that the stream
		/// was closed -- and not an NPE or anything like that.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHflushWhileClosing()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			Path p = new Path("/hflush-and-close.dat");
			FSDataOutputStream stm = CreateFile(fs, p, 1);
			AList<Sharpen.Thread> flushers = new AList<Sharpen.Thread>();
			AtomicReference<Exception> thrown = new AtomicReference<Exception>();
			try
			{
				for (int i = 0; i < 10; i++)
				{
					Sharpen.Thread flusher = new _Thread_165(stm, thrown);
					// Expected exception caught. Ignoring.
					flusher.Start();
					flushers.AddItem(flusher);
				}
				// Write some data
				for (int i_1 = 0; i_1 < 10000; i_1++)
				{
					stm.Write(1);
				}
				// Close it while the flushing threads are still flushing
				stm.Close();
				// Wait for the flushers to all die.
				foreach (Sharpen.Thread t in flushers)
				{
					t.Join();
				}
				// They should have all gotten the expected exception, not anything
				// else.
				if (thrown.Get() != null)
				{
					throw thrown.Get();
				}
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		private sealed class _Thread_165 : Sharpen.Thread
		{
			public _Thread_165(FSDataOutputStream stm, AtomicReference<Exception> thrown)
			{
				this.stm = stm;
				this.thrown = thrown;
			}

			public override void Run()
			{
				try
				{
					while (true)
					{
						try
						{
							stm.Hflush();
						}
						catch (ClosedChannelException)
						{
							return;
						}
					}
				}
				catch (Exception t)
				{
					thrown.Set(t);
				}
			}

			private readonly FSDataOutputStream stm;

			private readonly AtomicReference<Exception> thrown;
		}

		/// <exception cref="System.Exception"/>
		public virtual void DoMultithreadedWrites(Configuration conf, Path p, int numThreads
			, int bufferSize, int numWrites, int replication)
		{
			InitBuffer(bufferSize);
			// create a new file.
			FileSystem fs = p.GetFileSystem(conf);
			FSDataOutputStream stm = CreateFile(fs, p, replication);
			System.Console.Out.WriteLine("Created file simpleFlush.dat");
			// There have been a couple issues with flushing empty buffers, so do
			// some empty flushes first.
			stm.Hflush();
			stm.Hflush();
			stm.Write(1);
			stm.Hflush();
			stm.Hflush();
			CountDownLatch countdown = new CountDownLatch(1);
			AList<Sharpen.Thread> threads = new AList<Sharpen.Thread>();
			AtomicReference<Exception> thrown = new AtomicReference<Exception>();
			for (int i = 0; i < numThreads; i++)
			{
				Sharpen.Thread t = new TestMultiThreadedHflush.WriterThread(this, stm, thrown, countdown
					, numWrites);
				threads.AddItem(t);
				t.Start();
			}
			// Start all the threads at the same time for maximum raciness!
			countdown.CountDown();
			foreach (Sharpen.Thread t_1 in threads)
			{
				t_1.Join();
			}
			if (thrown.Get() != null)
			{
				throw new RuntimeException("Deferred", thrown.Get());
			}
			stm.Close();
			System.Console.Out.WriteLine("Closed file.");
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			System.Environment.Exit(ToolRunner.Run(new TestMultiThreadedHflush.CLIBenchmark()
				, args));
		}

		private class CLIBenchmark : Configured, Tool
		{
			/// <exception cref="System.Exception"/>
			public virtual int Run(string[] args)
			{
				if (args.Length != 1)
				{
					System.Console.Error.WriteLine("usage: " + typeof(TestMultiThreadedHflush).Name +
						 " <path to test file> ");
					System.Console.Error.WriteLine("Configurations settable by -D options:\n" + "  num.threads [default 10] - how many threads to run\n"
						 + "  write.size [default 511] - bytes per write\n" + "  num.writes [default 50000] - how many writes to perform"
						);
					System.Environment.Exit(1);
				}
				TestMultiThreadedHflush test = new TestMultiThreadedHflush();
				Configuration conf = GetConf();
				Path p = new Path(args[0]);
				int numThreads = conf.GetInt("num.threads", 10);
				int writeSize = conf.GetInt("write.size", 511);
				int numWrites = conf.GetInt("num.writes", 50000);
				int replication = conf.GetInt(DFSConfigKeys.DfsReplicationKey, DFSConfigKeys.DfsReplicationDefault
					);
				StopWatch sw = new StopWatch().Start();
				test.DoMultithreadedWrites(conf, p, numThreads, writeSize, numWrites, replication
					);
				sw.Stop();
				System.Console.Out.WriteLine("Finished in " + sw.Now(TimeUnit.Milliseconds) + "ms"
					);
				System.Console.Out.WriteLine("Latency quantiles (in microseconds):\n" + test.quantiles
					);
				return 0;
			}
		}
	}
}
