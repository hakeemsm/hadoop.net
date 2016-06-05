using System;


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
					Thread.Sleep(2 * ShutdownThreadsHelper.ShutdownWaitMs);
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
			Thread thread = new Thread(sampleRunnable);
			thread.Start();
			bool ret = ShutdownThreadsHelper.ShutdownThread(thread);
			bool isTerminated = !thread.IsAlive();
			Assert.Equal("Incorrect return value", ret, isTerminated);
			Assert.True("Thread is not shutdown", isTerminated);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestShutdownThreadPool()
		{
			ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
			executor.Execute(sampleRunnable);
			bool ret = ShutdownThreadsHelper.ShutdownExecutorService(executor);
			bool isTerminated = executor.IsTerminated();
			Assert.Equal("Incorrect return value", ret, isTerminated);
			Assert.True("ExecutorService is not shutdown", isTerminated);
		}
	}
}
