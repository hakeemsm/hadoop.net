using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class AsyncDiskService
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.AsyncDiskService
			));

		private const int CoreThreadsPerVolume = 1;

		private const int MaximumThreadsPerVolume = 4;

		private const long ThreadsKeepAliveSeconds = 60;

		private readonly ThreadGroup threadGroup = new ThreadGroup("async disk service");

		private ThreadFactory threadFactory;

		private Dictionary<string, ThreadPoolExecutor> executors = new Dictionary<string, 
			ThreadPoolExecutor>();

		/// <summary>
		/// Create a AsyncDiskServices with a set of volumes (specified by their
		/// root directories).
		/// </summary>
		/// <remarks>
		/// Create a AsyncDiskServices with a set of volumes (specified by their
		/// root directories).
		/// The AsyncDiskServices uses one ThreadPool per volume to do the async
		/// disk operations.
		/// </remarks>
		/// <param name="volumes">The roots of the file system volumes.</param>
		public AsyncDiskService(string[] volumes)
		{
			/*
			* This class is a container of multiple thread pools, each for a volume,
			* so that we can schedule async disk operations easily.
			*
			* Examples of async disk operations are deletion of files.
			* We can move the files to a "TO_BE_DELETED" folder before asychronously
			* deleting it, to make sure the caller can run it faster.
			*/
			// ThreadPool core pool size
			// ThreadPool maximum pool size
			// ThreadPool keep-alive time for threads over core pool size
			threadFactory = new _ThreadFactory_73(this);
			// Create one ThreadPool per volume
			for (int v = 0; v < volumes.Length; v++)
			{
				ThreadPoolExecutor executor = new ThreadPoolExecutor(CoreThreadsPerVolume, MaximumThreadsPerVolume
					, ThreadsKeepAliveSeconds, TimeUnit.Seconds, new LinkedBlockingQueue<Runnable>()
					, threadFactory);
				// This can reduce the number of running threads
				executor.AllowCoreThreadTimeOut(true);
				executors[volumes[v]] = executor;
			}
		}

		private sealed class _ThreadFactory_73 : ThreadFactory
		{
			public _ThreadFactory_73(AsyncDiskService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public Sharpen.Thread NewThread(Runnable r)
			{
				return new Sharpen.Thread(this._enclosing.threadGroup, r);
			}

			private readonly AsyncDiskService _enclosing;
		}

		/// <summary>Execute the task sometime in the future, using ThreadPools.</summary>
		public virtual void Execute(string root, Runnable task)
		{
			lock (this)
			{
				ThreadPoolExecutor executor = executors[root];
				if (executor == null)
				{
					throw new RuntimeException("Cannot find root " + root + " for execution of task "
						 + task);
				}
				else
				{
					executor.Execute(task);
				}
			}
		}

		/// <summary>Gracefully start the shut down of all ThreadPools.</summary>
		public virtual void Shutdown()
		{
			lock (this)
			{
				Log.Info("Shutting down all AsyncDiskService threads...");
				foreach (KeyValuePair<string, ThreadPoolExecutor> e in executors)
				{
					e.Value.Shutdown();
				}
			}
		}

		/// <summary>Wait for the termination of the thread pools.</summary>
		/// <param name="milliseconds">The number of milliseconds to wait</param>
		/// <returns>true if all thread pools are terminated without time limit</returns>
		/// <exception cref="System.Exception"></exception>
		public virtual bool AwaitTermination(long milliseconds)
		{
			lock (this)
			{
				long end = Time.Now() + milliseconds;
				foreach (KeyValuePair<string, ThreadPoolExecutor> e in executors)
				{
					ThreadPoolExecutor executor = e.Value;
					if (!executor.AwaitTermination(Math.Max(end - Time.Now(), 0), TimeUnit.Milliseconds
						))
					{
						Log.Warn("AsyncDiskService awaitTermination timeout.");
						return false;
					}
				}
				Log.Info("All AsyncDiskService threads are terminated.");
				return true;
			}
		}

		/// <summary>Shut down all ThreadPools immediately.</summary>
		public virtual IList<Runnable> ShutdownNow()
		{
			lock (this)
			{
				Log.Info("Shutting down all AsyncDiskService threads immediately...");
				IList<Runnable> list = new AList<Runnable>();
				foreach (KeyValuePair<string, ThreadPoolExecutor> e in executors)
				{
					Sharpen.Collections.AddAll(list, e.Value.ShutdownNow());
				}
				return list;
			}
		}
	}
}
