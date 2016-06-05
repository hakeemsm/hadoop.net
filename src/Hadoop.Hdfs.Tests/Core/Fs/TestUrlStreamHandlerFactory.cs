using System;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Test of the URL stream handler factory.</summary>
	public class TestUrlStreamHandlerFactory
	{
		private const int Runs = 20;

		private const int Threads = 10;

		private const int Tasks = 200;

		private const int Timeout = 30;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConcurrency()
		{
			for (int i = 0; i < Runs; i++)
			{
				SingleRun();
			}
		}

		/// <exception cref="System.Exception"/>
		private void SingleRun()
		{
			FsUrlStreamHandlerFactory factory = new FsUrlStreamHandlerFactory();
			Random random = new Random();
			ExecutorService executor = Executors.NewFixedThreadPool(Threads);
			AList<Future<object>> futures = new AList<Future<object>>(Tasks);
			for (int i = 0; i < Tasks; i++)
			{
				int aux = i;
				futures.AddItem(executor.Submit(new _Runnable_55(aux, random, factory)));
			}
			executor.Shutdown();
			try
			{
				executor.AwaitTermination(Timeout, TimeUnit.Seconds);
				executor.ShutdownNow();
			}
			catch (Exception)
			{
			}
			// pass
			// check for exceptions
			foreach (Future future in futures)
			{
				if (!future.IsDone())
				{
					break;
				}
				// timed out
				future.Get();
			}
		}

		private sealed class _Runnable_55 : Runnable
		{
			public _Runnable_55(int aux, Random random, FsUrlStreamHandlerFactory factory)
			{
				this.aux = aux;
				this.random = random;
				this.factory = factory;
			}

			public void Run()
			{
				int rand = aux + random.Next(3);
				factory.CreateURLStreamHandler(rand.ToString());
			}

			private readonly int aux;

			private readonly Random random;

			private readonly FsUrlStreamHandlerFactory factory;
		}
	}
}
