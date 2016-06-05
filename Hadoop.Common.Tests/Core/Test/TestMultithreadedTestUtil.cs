using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Test
{
	public class TestMultithreadedTestUtil
	{
		private const string FailMsg = "Inner thread fails an assert";

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNoErrors()
		{
			AtomicInteger threadsRun = new AtomicInteger();
			MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext();
			for (int i = 0; i < 3; i++)
			{
				ctx.AddThread(new _TestingThread_42(threadsRun, ctx));
			}
			Assert.Equal(0, threadsRun.Get());
			ctx.StartThreads();
			long st = Time.Now();
			ctx.WaitFor(30000);
			long et = Time.Now();
			// All threads should have run
			Assert.Equal(3, threadsRun.Get());
			// Test shouldn't have waited the full 30 seconds, since
			// the threads exited faster than that.
			Assert.True("Test took " + (et - st) + "ms", et - st < 5000);
		}

		private sealed class _TestingThread_42 : MultithreadedTestUtil.TestingThread
		{
			public _TestingThread_42(AtomicInteger threadsRun, MultithreadedTestUtil.TestContext
				 baseArg1)
				: base(baseArg1)
			{
				this.threadsRun = threadsRun;
			}

			/// <exception cref="System.Exception"/>
			public override void DoWork()
			{
				threadsRun.IncrementAndGet();
			}

			private readonly AtomicInteger threadsRun;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestThreadFails()
		{
			MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext();
			ctx.AddThread(new _TestingThread_66(ctx));
			ctx.StartThreads();
			long st = Time.Now();
			try
			{
				ctx.WaitFor(30000);
				NUnit.Framework.Assert.Fail("waitFor did not throw");
			}
			catch (RuntimeException rte)
			{
				// expected
				Assert.Equal(FailMsg, rte.InnerException.Message);
			}
			long et = Time.Now();
			// Test shouldn't have waited the full 30 seconds, since
			// the thread throws faster than that
			Assert.True("Test took " + (et - st) + "ms", et - st < 5000);
		}

		private sealed class _TestingThread_66 : MultithreadedTestUtil.TestingThread
		{
			public _TestingThread_66(MultithreadedTestUtil.TestContext baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.Exception"/>
			public override void DoWork()
			{
				NUnit.Framework.Assert.Fail(TestMultithreadedTestUtil.FailMsg);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestThreadThrowsCheckedException()
		{
			MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext();
			ctx.AddThread(new _TestingThread_91(ctx));
			ctx.StartThreads();
			long st = Time.Now();
			try
			{
				ctx.WaitFor(30000);
				NUnit.Framework.Assert.Fail("waitFor did not throw");
			}
			catch (RuntimeException rte)
			{
				// expected
				Assert.Equal("my ioe", rte.InnerException.Message);
			}
			long et = Time.Now();
			// Test shouldn't have waited the full 30 seconds, since
			// the thread throws faster than that
			Assert.True("Test took " + (et - st) + "ms", et - st < 5000);
		}

		private sealed class _TestingThread_91 : MultithreadedTestUtil.TestingThread
		{
			public _TestingThread_91(MultithreadedTestUtil.TestContext baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.Exception"/>
			public override void DoWork()
			{
				throw new IOException("my ioe");
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRepeatingThread()
		{
			AtomicInteger counter = new AtomicInteger();
			MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext();
			ctx.AddThread(new _RepeatingTestThread_118(counter, ctx));
			ctx.StartThreads();
			long st = Time.Now();
			ctx.WaitFor(3000);
			ctx.Stop();
			long et = Time.Now();
			long elapsed = et - st;
			// Test should have waited just about 3 seconds
			Assert.True("Test took " + (et - st) + "ms", Math.Abs(elapsed -
				 3000) < 500);
			// Counter should have been incremented lots of times in 3 full seconds
			Assert.True("Counter value = " + counter.Get(), counter.Get() >
				 1000);
		}

		private sealed class _RepeatingTestThread_118 : MultithreadedTestUtil.RepeatingTestThread
		{
			public _RepeatingTestThread_118(AtomicInteger counter, MultithreadedTestUtil.TestContext
				 baseArg1)
				: base(baseArg1)
			{
				this.counter = counter;
			}

			/// <exception cref="System.Exception"/>
			public override void DoAnAction()
			{
				counter.IncrementAndGet();
			}

			private readonly AtomicInteger counter;
		}
	}
}
