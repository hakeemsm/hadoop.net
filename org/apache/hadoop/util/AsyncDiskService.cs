using Sharpen;

namespace org.apache.hadoop.util
{
	public class AsyncDiskService
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.AsyncDiskService
			)));

		private const int CORE_THREADS_PER_VOLUME = 1;

		private const int MAXIMUM_THREADS_PER_VOLUME = 4;

		private const long THREADS_KEEP_ALIVE_SECONDS = 60;

		private readonly java.lang.ThreadGroup threadGroup = new java.lang.ThreadGroup("async disk service"
			);

		private java.util.concurrent.ThreadFactory threadFactory;

		private System.Collections.Generic.Dictionary<string, java.util.concurrent.ThreadPoolExecutor
			> executors = new System.Collections.Generic.Dictionary<string, java.util.concurrent.ThreadPoolExecutor
			>();

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
				java.util.concurrent.ThreadPoolExecutor executor = new java.util.concurrent.ThreadPoolExecutor
					(CORE_THREADS_PER_VOLUME, MAXIMUM_THREADS_PER_VOLUME, THREADS_KEEP_ALIVE_SECONDS
					, java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.LinkedBlockingQueue
					<java.lang.Runnable>(), threadFactory);
				// This can reduce the number of running threads
				executor.allowCoreThreadTimeOut(true);
				executors[volumes[v]] = executor;
			}
		}

		private sealed class _ThreadFactory_73 : java.util.concurrent.ThreadFactory
		{
			public _ThreadFactory_73(AsyncDiskService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public java.lang.Thread newThread(java.lang.Runnable r)
			{
				return new java.lang.Thread(this._enclosing.threadGroup, r);
			}

			private readonly AsyncDiskService _enclosing;
		}

		/// <summary>Execute the task sometime in the future, using ThreadPools.</summary>
		public virtual void execute(string root, java.lang.Runnable task)
		{
			lock (this)
			{
				java.util.concurrent.ThreadPoolExecutor executor = executors[root];
				if (executor == null)
				{
					throw new System.Exception("Cannot find root " + root + " for execution of task "
						 + task);
				}
				else
				{
					executor.execute(task);
				}
			}
		}

		/// <summary>Gracefully start the shut down of all ThreadPools.</summary>
		public virtual void shutdown()
		{
			lock (this)
			{
				LOG.info("Shutting down all AsyncDiskService threads...");
				foreach (System.Collections.Generic.KeyValuePair<string, java.util.concurrent.ThreadPoolExecutor
					> e in executors)
				{
					e.Value.shutdown();
				}
			}
		}

		/// <summary>Wait for the termination of the thread pools.</summary>
		/// <param name="milliseconds">The number of milliseconds to wait</param>
		/// <returns>true if all thread pools are terminated without time limit</returns>
		/// <exception cref="System.Exception"></exception>
		public virtual bool awaitTermination(long milliseconds)
		{
			lock (this)
			{
				long end = org.apache.hadoop.util.Time.now() + milliseconds;
				foreach (System.Collections.Generic.KeyValuePair<string, java.util.concurrent.ThreadPoolExecutor
					> e in executors)
				{
					java.util.concurrent.ThreadPoolExecutor executor = e.Value;
					if (!executor.awaitTermination(System.Math.max(end - org.apache.hadoop.util.Time.
						now(), 0), java.util.concurrent.TimeUnit.MILLISECONDS))
					{
						LOG.warn("AsyncDiskService awaitTermination timeout.");
						return false;
					}
				}
				LOG.info("All AsyncDiskService threads are terminated.");
				return true;
			}
		}

		/// <summary>Shut down all ThreadPools immediately.</summary>
		public virtual System.Collections.Generic.IList<java.lang.Runnable> shutdownNow()
		{
			lock (this)
			{
				LOG.info("Shutting down all AsyncDiskService threads immediately...");
				System.Collections.Generic.IList<java.lang.Runnable> list = new System.Collections.Generic.List
					<java.lang.Runnable>();
				foreach (System.Collections.Generic.KeyValuePair<string, java.util.concurrent.ThreadPoolExecutor
					> e in executors)
				{
					Sharpen.Collections.AddAll(list, e.Value.shutdownNow());
				}
				return list;
			}
		}
	}
}
