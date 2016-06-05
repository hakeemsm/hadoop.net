using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// Test
	/// <see cref="ByteArrayManager"/>
	/// .
	/// </summary>
	public class TestByteArrayManager
	{
		static TestByteArrayManager()
		{
			((Log4JLogger)LogFactory.GetLog(typeof(ByteArrayManager))).GetLogger().SetLevel(Level
				.All);
		}

		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Util.TestByteArrayManager
			));

		private sealed class _IComparer_59 : IComparer<Future<int>>
		{
			public _IComparer_59()
			{
			}

			public int Compare(Future<int> left, Future<int> right)
			{
				try
				{
					return left.Get() - right.Get();
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		private static readonly IComparer<Future<int>> Cmp = new _IComparer_59();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCounter()
		{
			long countResetTimePeriodMs = 200L;
			ByteArrayManager.Counter c = new ByteArrayManager.Counter(countResetTimePeriodMs);
			int n = DFSUtil.GetRandom().Next(512) + 512;
			IList<Future<int>> futures = new AList<Future<int>>(n);
			ExecutorService pool = Executors.NewFixedThreadPool(32);
			try
			{
				// increment
				for (int i = 0; i < n; i++)
				{
					futures.AddItem(pool.Submit(new _Callable_82(c)));
				}
				// sort and wait for the futures
				futures.Sort(Cmp);
			}
			finally
			{
				pool.Shutdown();
			}
			// check futures
			NUnit.Framework.Assert.AreEqual(n, futures.Count);
			for (int i_1 = 0; i_1 < n; i_1++)
			{
				NUnit.Framework.Assert.AreEqual(i_1 + 1, futures[i_1].Get());
			}
			NUnit.Framework.Assert.AreEqual(n, c.GetCount());
			// test auto-reset
			Sharpen.Thread.Sleep(countResetTimePeriodMs + 100);
			NUnit.Framework.Assert.AreEqual(1, c.Increment());
		}

		private sealed class _Callable_82 : Callable<int>
		{
			public _Callable_82(ByteArrayManager.Counter c)
			{
				this.c = c;
			}

			/// <exception cref="System.Exception"/>
			public int Call()
			{
				return (int)c.Increment();
			}

			private readonly ByteArrayManager.Counter c;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllocateRecycle()
		{
			int countThreshold = 4;
			int countLimit = 8;
			long countResetTimePeriodMs = 200L;
			ByteArrayManager.Impl bam = new ByteArrayManager.Impl(new ByteArrayManager.Conf(countThreshold
				, countLimit, countResetTimePeriodMs));
			ByteArrayManager.CounterMap counters = bam.GetCounters();
			ByteArrayManager.ManagerMap managers = bam.GetManagers();
			int[] uncommonArrays = new int[] { 0, 1, 2, 4, 8, 16, 32, 64 };
			int arrayLength = 1024;
			TestByteArrayManager.Allocator allocator = new TestByteArrayManager.Allocator(bam
				);
			TestByteArrayManager.Recycler recycler = new TestByteArrayManager.Recycler(bam);
			try
			{
				{
					// allocate within threshold
					for (int i = 0; i < countThreshold; i++)
					{
						allocator.Submit(arrayLength);
					}
					WaitForAll(allocator.futures);
					NUnit.Framework.Assert.AreEqual(countThreshold, counters.Get(arrayLength, false).
						GetCount());
					NUnit.Framework.Assert.IsNull(managers.Get(arrayLength, false));
					foreach (int n in uncommonArrays)
					{
						NUnit.Framework.Assert.IsNull(counters.Get(n, false));
						NUnit.Framework.Assert.IsNull(managers.Get(n, false));
					}
				}
				{
					// recycle half of the arrays
					for (int i = 0; i < countThreshold / 2; i++)
					{
						recycler.Submit(RemoveLast(allocator.futures).Get());
					}
					foreach (Future<int> f in recycler.furtures)
					{
						NUnit.Framework.Assert.AreEqual(-1, f.Get());
					}
					recycler.furtures.Clear();
				}
				{
					// allocate one more
					allocator.Submit(arrayLength).Get();
					NUnit.Framework.Assert.AreEqual(countThreshold + 1, counters.Get(arrayLength, false
						).GetCount());
					NUnit.Framework.Assert.IsNotNull(managers.Get(arrayLength, false));
				}
				{
					// recycle the remaining arrays
					int n = allocator.RecycleAll(recycler);
					recycler.Verify(n);
				}
				{
					// allocate until the maximum.
					for (int i = 0; i < countLimit; i++)
					{
						allocator.Submit(arrayLength);
					}
					WaitForAll(allocator.futures);
					// allocate one more should be blocked
					TestByteArrayManager.AllocatorThread t = new TestByteArrayManager.AllocatorThread
						(arrayLength, bam);
					t.Start();
					// check if the thread is waiting, timed wait or runnable.
					for (int i_1 = 0; i_1 < 5; i_1++)
					{
						Sharpen.Thread.Sleep(100);
						Sharpen.Thread.State threadState = t.GetState();
						if (threadState != Sharpen.Thread.State.Runnable && threadState != Sharpen.Thread.State
							.Waiting && threadState != Sharpen.Thread.State.TimedWaiting)
						{
							NUnit.Framework.Assert.Fail("threadState = " + threadState);
						}
					}
					// recycle an array
					recycler.Submit(RemoveLast(allocator.futures).Get());
					NUnit.Framework.Assert.AreEqual(1, RemoveLast(recycler.furtures).Get());
					// check if the thread is unblocked
					Sharpen.Thread.Sleep(100);
					NUnit.Framework.Assert.AreEqual(Sharpen.Thread.State.Terminated, t.GetState());
					// recycle the remaining, the recycle should be full.
					NUnit.Framework.Assert.AreEqual(countLimit - 1, allocator.RecycleAll(recycler));
					recycler.Submit(t.array);
					recycler.Verify(countLimit);
					// recycle one more; it should not increase the free queue size
					NUnit.Framework.Assert.AreEqual(countLimit, bam.Release(new byte[arrayLength]));
				}
			}
			finally
			{
				allocator.pool.Shutdown();
				recycler.pool.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		internal static Future<T> RemoveLast<T>(IList<Future<T>> furtures)
		{
			return Remove(furtures, furtures.Count - 1);
		}

		/// <exception cref="System.Exception"/>
		internal static Future<T> Remove<T>(IList<Future<T>> furtures, int i)
		{
			return furtures.IsEmpty() ? null : furtures.Remove(i);
		}

		/// <exception cref="System.Exception"/>
		internal static void WaitForAll<T>(IList<Future<T>> furtures)
		{
			foreach (Future<T> f in furtures)
			{
				f.Get();
			}
		}

		internal class AllocatorThread : Sharpen.Thread
		{
			private readonly ByteArrayManager bam;

			private readonly int arrayLength;

			private byte[] array;

			internal AllocatorThread(int arrayLength, ByteArrayManager bam)
			{
				this.bam = bam;
				this.arrayLength = arrayLength;
			}

			public override void Run()
			{
				try
				{
					array = bam.NewByteArray(arrayLength);
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}
		}

		internal class Allocator
		{
			private readonly ByteArrayManager bam;

			internal readonly ExecutorService pool = Executors.NewFixedThreadPool(8);

			internal readonly IList<Future<byte[]>> futures = new List<Future<byte[]>>();

			internal Allocator(ByteArrayManager bam)
			{
				this.bam = bam;
			}

			internal virtual Future<byte[]> Submit(int arrayLength)
			{
				Future<byte[]> f = pool.Submit(new _Callable_255(this, arrayLength));
				futures.AddItem(f);
				return f;
			}

			private sealed class _Callable_255 : Callable<byte[]>
			{
				public _Callable_255(Allocator _enclosing, int arrayLength)
				{
					this._enclosing = _enclosing;
					this.arrayLength = arrayLength;
				}

				/// <exception cref="System.Exception"/>
				public byte[] Call()
				{
					byte[] array = this._enclosing.bam.NewByteArray(arrayLength);
					NUnit.Framework.Assert.AreEqual(arrayLength, array.Length);
					return array;
				}

				private readonly Allocator _enclosing;

				private readonly int arrayLength;
			}

			/// <exception cref="System.Exception"/>
			internal virtual int RecycleAll(TestByteArrayManager.Recycler recycler)
			{
				int n = futures.Count;
				foreach (Future<byte[]> f in futures)
				{
					recycler.Submit(f.Get());
				}
				futures.Clear();
				return n;
			}
		}

		internal class Recycler
		{
			private readonly ByteArrayManager bam;

			internal readonly ExecutorService pool = Executors.NewFixedThreadPool(8);

			internal readonly IList<Future<int>> furtures = new List<Future<int>>();

			internal Recycler(ByteArrayManager bam)
			{
				this.bam = bam;
			}

			internal virtual Future<int> Submit(byte[] array)
			{
				Future<int> f = pool.Submit(new _Callable_287(this, array));
				furtures.AddItem(f);
				return f;
			}

			private sealed class _Callable_287 : Callable<int>
			{
				public _Callable_287(Recycler _enclosing, byte[] array)
				{
					this._enclosing = _enclosing;
					this.array = array;
				}

				/// <exception cref="System.Exception"/>
				public int Call()
				{
					return this._enclosing.bam.Release(array);
				}

				private readonly Recycler _enclosing;

				private readonly byte[] array;
			}

			/// <exception cref="System.Exception"/>
			internal virtual void Verify(int expectedSize)
			{
				NUnit.Framework.Assert.AreEqual(expectedSize, furtures.Count);
				furtures.Sort(Cmp);
				for (int i = 0; i < furtures.Count; i++)
				{
					NUnit.Framework.Assert.AreEqual(i + 1, furtures[i].Get());
				}
				furtures.Clear();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestByteArrayManager()
		{
			int countThreshold = 32;
			int countLimit = 64;
			long countResetTimePeriodMs = 1000L;
			ByteArrayManager.Impl bam = new ByteArrayManager.Impl(new ByteArrayManager.Conf(countThreshold
				, countLimit, countResetTimePeriodMs));
			ByteArrayManager.CounterMap counters = bam.GetCounters();
			ByteArrayManager.ManagerMap managers = bam.GetManagers();
			ExecutorService pool = Executors.NewFixedThreadPool(128);
			TestByteArrayManager.Runner[] runners = new TestByteArrayManager.Runner[TestByteArrayManager.Runner
				.NumRunners];
			Sharpen.Thread[] threads = new Sharpen.Thread[runners.Length];
			int num = 1 << 10;
			for (int i = 0; i < runners.Length; i++)
			{
				runners[i] = new TestByteArrayManager.Runner(i, countThreshold, countLimit, pool, 
					i, bam);
				threads[i] = runners[i].Start(num);
			}
			IList<Exception> exceptions = new AList<Exception>();
			Sharpen.Thread randomRecycler = new _Thread_332(runners, exceptions, threads);
			randomRecycler.Start();
			randomRecycler.Join();
			NUnit.Framework.Assert.IsTrue(exceptions.IsEmpty());
			NUnit.Framework.Assert.IsNull(counters.Get(0, false));
			for (int i_1 = 1; i_1 < runners.Length; i_1++)
			{
				if (!runners[i_1].assertionErrors.IsEmpty())
				{
					foreach (Exception e in runners[i_1].assertionErrors)
					{
						Log.Error("AssertionError " + i_1, e);
					}
					NUnit.Framework.Assert.Fail(runners[i_1].assertionErrors.Count + " AssertionError(s)"
						);
				}
				int arrayLength = TestByteArrayManager.Runner.Index2arrayLength(i_1);
				bool exceedCountThreshold = counters.Get(arrayLength, false).GetCount() > countThreshold;
				ByteArrayManager.FixedLengthManager m = managers.Get(arrayLength, false);
				if (exceedCountThreshold)
				{
					NUnit.Framework.Assert.IsNotNull(m);
				}
				else
				{
					NUnit.Framework.Assert.IsNull(m);
				}
			}
		}

		private sealed class _Thread_332 : Sharpen.Thread
		{
			public _Thread_332(TestByteArrayManager.Runner[] runners, IList<Exception> exceptions
				, Sharpen.Thread[] threads)
			{
				this.runners = runners;
				this.exceptions = exceptions;
				this.threads = threads;
			}

			public override void Run()
			{
				TestByteArrayManager.Log.Info("randomRecycler start");
				for (int i = 0; this.ShouldRun(); i++)
				{
					int j = DFSUtil.GetRandom().Next(runners.Length);
					try
					{
						runners[j].Recycle();
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
						exceptions.AddItem(new Exception(this + " has an exception", e));
					}
					if ((i & unchecked((int)(0xFF))) == 0)
					{
						TestByteArrayManager.Log.Info("randomRecycler sleep, i=" + i);
						TestByteArrayManager.SleepMs(100);
					}
				}
				TestByteArrayManager.Log.Info("randomRecycler done");
			}

			internal bool ShouldRun()
			{
				for (int i = 0; i < runners.Length; i++)
				{
					if (threads[i].IsAlive())
					{
						return true;
					}
					if (!runners[i].IsEmpty())
					{
						return true;
					}
				}
				return false;
			}

			private readonly TestByteArrayManager.Runner[] runners;

			private readonly IList<Exception> exceptions;

			private readonly Sharpen.Thread[] threads;
		}

		internal static void SleepMs(long ms)
		{
			try
			{
				Sharpen.Thread.Sleep(ms);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Sleep is interrupted: " + e);
			}
		}

		internal class Runner : Runnable
		{
			internal const int NumRunners = 5;

			internal static int Index2arrayLength(int index)
			{
				return ByteArrayManager.MinArrayLength << (index - 1);
			}

			private readonly ByteArrayManager bam;

			internal readonly int maxArrayLength;

			internal readonly int countThreshold;

			internal readonly int maxArrays;

			internal readonly ExecutorService pool;

			internal readonly IList<Future<byte[]>> arrays = new AList<Future<byte[]>>();

			internal readonly AtomicInteger count = new AtomicInteger();

			internal readonly int p;

			private int n;

			internal readonly IList<Exception> assertionErrors = new AList<Exception>();

			internal Runner(int index, int countThreshold, int maxArrays, ExecutorService pool
				, int p, ByteArrayManager bam)
			{
				this.maxArrayLength = Index2arrayLength(index);
				this.countThreshold = countThreshold;
				this.maxArrays = maxArrays;
				this.pool = pool;
				this.p = p;
				this.bam = bam;
			}

			internal virtual bool IsEmpty()
			{
				lock (arrays)
				{
					return arrays.IsEmpty();
				}
			}

			internal virtual Future<byte[]> SubmitAllocate()
			{
				count.IncrementAndGet();
				Future<byte[]> f = pool.Submit(new _Callable_438(this));
				lock (arrays)
				{
					arrays.AddItem(f);
				}
				return f;
			}

			private sealed class _Callable_438 : Callable<byte[]>
			{
				public _Callable_438(Runner _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				public byte[] Call()
				{
					int lower = this._enclosing.maxArrayLength == ByteArrayManager.MinArrayLength ? 0
						 : this._enclosing.maxArrayLength >> 1;
					int arrayLength = DFSUtil.GetRandom().Next(this._enclosing.maxArrayLength - lower
						) + lower + 1;
					byte[] array = this._enclosing.bam.NewByteArray(arrayLength);
					try
					{
						NUnit.Framework.Assert.AreEqual("arrayLength=" + arrayLength + ", lower=" + lower
							, this._enclosing.maxArrayLength, array.Length);
					}
					catch (Exception e)
					{
						this._enclosing.assertionErrors.AddItem(e);
					}
					return array;
				}

				private readonly Runner _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal virtual Future<byte[]> RemoveFirst()
			{
				lock (arrays)
				{
					return Remove(arrays, 0);
				}
			}

			/// <exception cref="System.Exception"/>
			internal virtual void Recycle()
			{
				Future<byte[]> f = RemoveFirst();
				if (f != null)
				{
					Printf("randomRecycler: ");
					try
					{
						Recycle(f.Get(10, TimeUnit.Milliseconds));
					}
					catch (TimeoutException)
					{
						Recycle(new byte[maxArrayLength]);
						Printf("timeout, new byte[%d]\n", maxArrayLength);
					}
				}
			}

			internal virtual int Recycle(byte[] array)
			{
				return bam.Release(array);
			}

			internal virtual Future<int> SubmitRecycle(byte[] array)
			{
				count.DecrementAndGet();
				Future<int> f = pool.Submit(new _Callable_487(this, array));
				return f;
			}

			private sealed class _Callable_487 : Callable<int>
			{
				public _Callable_487(Runner _enclosing, byte[] array)
				{
					this._enclosing = _enclosing;
					this.array = array;
				}

				/// <exception cref="System.Exception"/>
				public int Call()
				{
					return this._enclosing.Recycle(array);
				}

				private readonly Runner _enclosing;

				private readonly byte[] array;
			}

			public virtual void Run()
			{
				for (int i = 0; i < n; i++)
				{
					bool isAllocate = DFSUtil.GetRandom().Next(NumRunners) < p;
					if (isAllocate)
					{
						SubmitAllocate();
					}
					else
					{
						try
						{
							Future<byte[]> f = RemoveFirst();
							if (f != null)
							{
								SubmitRecycle(f.Get());
							}
						}
						catch (Exception e)
						{
							Sharpen.Runtime.PrintStackTrace(e);
							NUnit.Framework.Assert.Fail(this + " has " + e);
						}
					}
					if ((i & unchecked((int)(0xFF))) == 0)
					{
						SleepMs(100);
					}
				}
			}

			internal virtual Sharpen.Thread Start(int n)
			{
				this.n = n;
				Sharpen.Thread t = new Sharpen.Thread(this);
				t.Start();
				return t;
			}

			public override string ToString()
			{
				return GetType().Name + ": max=" + maxArrayLength + ", count=" + count;
			}
		}

		internal class NewByteArrayWithLimit : ByteArrayManager
		{
			private readonly int maxCount;

			private int count = 0;

			internal NewByteArrayWithLimit(int maxCount)
			{
				this.maxCount = maxCount;
			}

			/// <exception cref="System.Exception"/>
			public override byte[] NewByteArray(int size)
			{
				lock (this)
				{
					for (; count >= maxCount; )
					{
						Sharpen.Runtime.Wait(this);
					}
					count++;
					return new byte[size];
				}
			}

			public override int Release(byte[] array)
			{
				lock (this)
				{
					if (count == maxCount)
					{
						Sharpen.Runtime.NotifyAll(this);
					}
					count--;
					return 0;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			((Log4JLogger)LogFactory.GetLog(typeof(ByteArrayManager))).GetLogger().SetLevel(Level
				.Off);
			int arrayLength = 64 * 1024;
			//64k
			int nThreads = 512;
			int nAllocations = 1 << 15;
			int maxArrays = 1 << 10;
			int nTrials = 5;
			System.Console.Out.WriteLine("arrayLength=" + arrayLength + ", nThreads=" + nThreads
				 + ", nAllocations=" + nAllocations + ", maxArrays=" + maxArrays);
			Random ran = DFSUtil.GetRandom();
			ByteArrayManager[] impls = new ByteArrayManager[] { new ByteArrayManager.NewByteArrayWithoutLimit
				(), new TestByteArrayManager.NewByteArrayWithLimit(maxArrays), new ByteArrayManager.Impl
				(new ByteArrayManager.Conf(DFSConfigKeys.DfsClientWriteByteArrayManagerCountThresholdDefault
				, maxArrays, DFSConfigKeys.DfsClientWriteByteArrayManagerCountResetTimePeriodMsDefault
				)) };
			double[] avg = new double[impls.Length];
			for (int i = 0; i < impls.Length; i++)
			{
				double duration = 0;
				Printf("%26s:", impls[i].GetType().Name);
				for (int j = 0; j < nTrials; j++)
				{
					int[] sleepTime = new int[nAllocations];
					for (int k = 0; k < sleepTime.Length; k++)
					{
						sleepTime[k] = ran.Next(100);
					}
					long elapsed = PerformanceTest(arrayLength, maxArrays, nThreads, sleepTime, impls
						[i]);
					duration += elapsed;
					Printf("%5d, ", elapsed);
				}
				avg[i] = duration / nTrials;
				Printf("avg=%6.3fs", avg[i] / 1000);
				for (int j_1 = 0; j_1 < i; j_1++)
				{
					Printf(" (%6.2f%%)", PercentageDiff(avg[j_1], avg[i]));
				}
				Printf("\n");
			}
		}

		internal static double PercentageDiff(double original, double newValue)
		{
			return (newValue - original) / original * 100;
		}

		internal static void Printf(string format, params object[] args)
		{
			System.Console.Out.Printf(format, args);
			System.Console.Out.Flush();
		}

		/// <exception cref="System.Exception"/>
		internal static long PerformanceTest(int arrayLength, int maxArrays, int nThreads
			, int[] sleepTimeMSs, ByteArrayManager impl)
		{
			ExecutorService pool = Executors.NewFixedThreadPool(nThreads);
			IList<Future<Void>> futures = new AList<Future<Void>>(sleepTimeMSs.Length);
			long startTime = Time.MonotonicNow();
			for (int i = 0; i < sleepTimeMSs.Length; i++)
			{
				long sleepTime = sleepTimeMSs[i];
				futures.AddItem(pool.Submit(new _Callable_628(impl, arrayLength, sleepTime)));
			}
			foreach (Future<Void> f in futures)
			{
				f.Get();
			}
			long endTime = Time.MonotonicNow();
			pool.Shutdown();
			return endTime - startTime;
		}

		private sealed class _Callable_628 : Callable<Void>
		{
			public _Callable_628(ByteArrayManager impl, int arrayLength, long sleepTime)
			{
				this.impl = impl;
				this.arrayLength = arrayLength;
				this.sleepTime = sleepTime;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				byte[] array = impl.NewByteArray(arrayLength);
				TestByteArrayManager.SleepMs(sleepTime);
				impl.Release(array);
				return null;
			}

			private readonly ByteArrayManager impl;

			private readonly int arrayLength;

			private readonly long sleepTime;
		}
	}
}
