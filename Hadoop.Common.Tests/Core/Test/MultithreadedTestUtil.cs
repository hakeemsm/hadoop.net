using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>A utility to easily test threaded/synchronized code.</summary>
	/// <remarks>
	/// A utility to easily test threaded/synchronized code.
	/// Utility works by letting you add threads that do some work to a
	/// test context object, and then lets you kick them all off to stress test
	/// your parallel code.
	/// Also propagates thread exceptions back to the runner, to let you verify.
	/// An example:
	/// <code>
	/// final AtomicInteger threadsRun = new AtomicInteger();
	/// TestContext ctx = new TestContext();
	/// // Add 3 threads to test.
	/// for (int i = 0; i &lt; 3; i++) {
	/// ctx.addThread(new TestingThread(ctx) {
	/// </remarks>
	/// <Override>
	/// public void doWork() throws Exception {
	/// threadsRun.incrementAndGet();
	/// }
	/// });
	/// }
	/// ctx.startThreads();
	/// // Set a timeout period for threads to complete.
	/// ctx.waitFor(30000);
	/// assertEquals(3, threadsRun.get());
	/// </code>
	/// For repetitive actions, use the
	/// <see cref="MultithreadedTestUtil.RepeatingThread"/>
	/// instead.
	/// (More examples can be found in
	/// <see cref="TestMultithreadedTestUtil"/>
	/// )
	/// </Override>
	public abstract class MultithreadedTestUtil
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(MultithreadedTestUtil));

		/// <summary>TestContext is used to setup the multithreaded test runner.</summary>
		/// <remarks>
		/// TestContext is used to setup the multithreaded test runner.
		/// It lets you add threads, run them, wait upon or stop them.
		/// </remarks>
		public class TestContext
		{
			private Exception err = null;

			private bool stopped = false;

			private ICollection<MultithreadedTestUtil.TestingThread> testThreads = new HashSet
				<MultithreadedTestUtil.TestingThread>();

			private ICollection<MultithreadedTestUtil.TestingThread> finishedThreads = new HashSet
				<MultithreadedTestUtil.TestingThread>();

			/// <summary>Check if the context can run threads.</summary>
			/// <remarks>
			/// Check if the context can run threads.
			/// Can't if its been stopped and contains an error.
			/// </remarks>
			/// <returns>true if it can run, false if it can't.</returns>
			public virtual bool ShouldRun()
			{
				lock (this)
				{
					return !stopped && err == null;
				}
			}

			/// <summary>Add a thread to the context for running.</summary>
			/// <remarks>
			/// Add a thread to the context for running.
			/// Threads can be of type
			/// <see cref="TestingThread"/>
			/// or
			/// <see cref="RepeatingTestThread"/>
			/// or other custom derivatives of the former.
			/// </remarks>
			/// <param name="t">the thread to add for running.</param>
			public virtual void AddThread(MultithreadedTestUtil.TestingThread t)
			{
				testThreads.AddItem(t);
			}

			/// <summary>Starts all test threads that have been added so far.</summary>
			public virtual void StartThreads()
			{
				foreach (MultithreadedTestUtil.TestingThread t in testThreads)
				{
					t.Start();
				}
			}

			/// <summary>Waits for threads to finish or error out.</summary>
			/// <param name="millis">
			/// the number of milliseconds to wait
			/// for threads to complete.
			/// </param>
			/// <exception cref="System.Exception">
			/// if one or more of the threads
			/// have thrown up an error.
			/// </exception>
			public virtual void WaitFor(long millis)
			{
				lock (this)
				{
					long endTime = Time.Now() + millis;
					while (ShouldRun() && finishedThreads.Count < testThreads.Count)
					{
						long left = endTime - Time.Now();
						if (left <= 0)
						{
							break;
						}
						CheckException();
						Sharpen.Runtime.Wait(this, left);
					}
					CheckException();
				}
			}

			/// <summary>
			/// Checks for thread exceptions, and if they've occurred
			/// throws them as RuntimeExceptions in a deferred manner.
			/// </summary>
			/// <exception cref="System.Exception"/>
			public virtual void CheckException()
			{
				lock (this)
				{
					if (err != null)
					{
						throw new RuntimeException("Deferred", err);
					}
				}
			}

			/// <summary>
			/// Called by
			/// <see cref="TestingThread"/>
			/// s to signal
			/// a failed thread.
			/// </summary>
			/// <param name="t">the thread that failed.</param>
			public virtual void ThreadFailed(Exception t)
			{
				lock (this)
				{
					if (err == null)
					{
						err = t;
					}
					Log.Error("Failed!", err);
					Sharpen.Runtime.Notify(this);
				}
			}

			/// <summary>
			/// Called by
			/// <see cref="TestingThread"/>
			/// s to signal
			/// a successful completion.
			/// </summary>
			/// <param name="t">the thread that finished.</param>
			public virtual void ThreadDone(MultithreadedTestUtil.TestingThread t)
			{
				lock (this)
				{
					finishedThreads.AddItem(t);
					Sharpen.Runtime.Notify(this);
				}
			}

			/// <summary>Returns after stopping all threads by joining them back.</summary>
			/// <exception cref="System.Exception">in case a thread terminated with a failure.</exception>
			public virtual void Stop()
			{
				lock (this)
				{
					stopped = true;
				}
				foreach (MultithreadedTestUtil.TestingThread t in testThreads)
				{
					t.Join();
				}
				CheckException();
			}

			public virtual IEnumerable<Sharpen.Thread> GetTestThreads()
			{
				return testThreads;
			}
		}

		/// <summary>
		/// A thread that can be added to a test context, and properly
		/// passes exceptions through.
		/// </summary>
		public abstract class TestingThread : Sharpen.Thread
		{
			protected internal readonly MultithreadedTestUtil.TestContext ctx;

			protected internal bool stopped;

			public TestingThread(MultithreadedTestUtil.TestContext ctx)
			{
				this.ctx = ctx;
			}

			public override void Run()
			{
				try
				{
					DoWork();
				}
				catch (Exception t)
				{
					ctx.ThreadFailed(t);
				}
				ctx.ThreadDone(this);
			}

			/// <summary>User method to add any code to test thread behavior of.</summary>
			/// <exception cref="System.Exception">throw an exception if a failure has occurred.</exception>
			public abstract void DoWork();

			protected internal virtual void StopTestThread()
			{
				this.stopped = true;
			}
		}

		/// <summary>A test thread that performs a repeating operation.</summary>
		public abstract class RepeatingTestThread : MultithreadedTestUtil.TestingThread
		{
			public RepeatingTestThread(MultithreadedTestUtil.TestContext ctx)
				: base(ctx)
			{
			}

			/// <summary>
			/// Repeats a given user action until the context is asked to stop
			/// or meets an error.
			/// </summary>
			/// <exception cref="System.Exception"/>
			public sealed override void DoWork()
			{
				while (ctx.ShouldRun() && !stopped)
				{
					DoAnAction();
				}
			}

			/// <summary>User method for any code to test repeating behavior of (as threads).</summary>
			/// <exception cref="System.Exception">throw an exception if a failure has occured.</exception>
			public abstract void DoAnAction();
		}
	}
}
