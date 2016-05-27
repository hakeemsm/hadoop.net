using System;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestShutdownThreadsHelper
	{
		private sealed class _Runnable_27 : Runnable
		{
			public _Runnable_27()
			{
			}

			public void Run()
			{
				try
				{
					Sharpen.Thread.Sleep(2 * ShutdownThreadsHelper.ShutdownWaitMs);
				}
				catch (Exception)
				{
					System.Console.Out.WriteLine("Thread interrupted");
				}
			}
		}

		private Runnable sampleRunnable = new _Runnable_27();

		public virtual void TestShutdownThread()
		{
			Sharpen.Thread thread = new Sharpen.Thread(sampleRunnable);
			thread.Start();
			bool ret = ShutdownThreadsHelper.ShutdownThread(thread);
			bool isTerminated = !thread.IsAlive();
			NUnit.Framework.Assert.AreEqual("Incorrect return value", ret, isTerminated);
			NUnit.Framework.Assert.IsTrue("Thread is not shutdown", isTerminated);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShutdownThreadPool()
		{
			ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
			executor.Execute(sampleRunnable);
			bool ret = ShutdownThreadsHelper.ShutdownExecutorService(executor);
			bool isTerminated = executor.IsTerminated();
			NUnit.Framework.Assert.AreEqual("Incorrect return value", ret, isTerminated);
			NUnit.Framework.Assert.IsTrue("ExecutorService is not shutdown", isTerminated);
		}
	}
}
