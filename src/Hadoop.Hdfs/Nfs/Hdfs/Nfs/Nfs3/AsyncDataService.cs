using System;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>This class is a thread pool to easily schedule async data operations.</summary>
	/// <remarks>
	/// This class is a thread pool to easily schedule async data operations. Current
	/// async data operation is write back operation. In the future, we could use it
	/// for readahead operations too.
	/// </remarks>
	public class AsyncDataService
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.AsyncDataService
			));

		private const int CoreThreadsPerVolume = 1;

		private const int MaximumThreadsPerVolume = 4;

		private const long ThreadsKeepAliveSeconds = 60;

		private readonly ThreadGroup threadGroup = new ThreadGroup("async data service");

		private ThreadFactory threadFactory = null;

		private ThreadPoolExecutor executor = null;

		public AsyncDataService()
		{
			// ThreadPool core pool size
			// ThreadPool maximum pool size
			// ThreadPool keep-alive time for threads over core pool size
			threadFactory = new _ThreadFactory_47(this);
			executor = new ThreadPoolExecutor(CoreThreadsPerVolume, MaximumThreadsPerVolume, 
				ThreadsKeepAliveSeconds, TimeUnit.Seconds, new LinkedBlockingQueue<Runnable>(), 
				threadFactory);
			// This can reduce the number of running threads
			executor.AllowCoreThreadTimeOut(true);
		}

		private sealed class _ThreadFactory_47 : ThreadFactory
		{
			public _ThreadFactory_47(AsyncDataService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public Sharpen.Thread NewThread(Runnable r)
			{
				return new Sharpen.Thread(this._enclosing.threadGroup, r);
			}

			private readonly AsyncDataService _enclosing;
		}

		/// <summary>Execute the task sometime in the future.</summary>
		internal virtual void Execute(Runnable task)
		{
			lock (this)
			{
				if (executor == null)
				{
					throw new RuntimeException("AsyncDataService is already shutdown");
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Current active thread number: " + executor.GetActiveCount() + " queue size: "
						 + executor.GetQueue().Count + " scheduled task number: " + executor.GetTaskCount
						());
				}
				executor.Execute(task);
			}
		}

		/// <summary>Gracefully shut down the ThreadPool.</summary>
		/// <remarks>
		/// Gracefully shut down the ThreadPool. Will wait for all data tasks to
		/// finish.
		/// </remarks>
		internal virtual void Shutdown()
		{
			lock (this)
			{
				if (executor == null)
				{
					Log.Warn("AsyncDataService has already shut down.");
				}
				else
				{
					Log.Info("Shutting down all async data service threads...");
					executor.Shutdown();
					// clear the executor so that calling execute again will fail.
					executor = null;
					Log.Info("All async data service threads have been shut down");
				}
			}
		}

		/// <summary>Write the data to HDFS asynchronously</summary>
		internal virtual void WriteAsync(OpenFileCtx openFileCtx)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Scheduling write back task for fileId: " + openFileCtx.GetLatestAttr()
					.GetFileId());
			}
			AsyncDataService.WriteBackTask wbTask = new AsyncDataService.WriteBackTask(openFileCtx
				);
			Execute(wbTask);
		}

		/// <summary>A task to write data back to HDFS for a file.</summary>
		/// <remarks>
		/// A task to write data back to HDFS for a file. Since only one thread can
		/// write to a file, there should only be one task at any time for a file
		/// (in queue or executing), and this should be guaranteed by the caller.
		/// </remarks>
		internal class WriteBackTask : Runnable
		{
			internal OpenFileCtx openFileCtx;

			internal WriteBackTask(OpenFileCtx openFileCtx)
			{
				this.openFileCtx = openFileCtx;
			}

			internal virtual OpenFileCtx GetOpenFileCtx()
			{
				return openFileCtx;
			}

			public override string ToString()
			{
				// Called in AsyncDataService.execute for displaying error messages.
				return "write back data for fileId" + openFileCtx.GetLatestAttr().GetFileId() + " with nextOffset "
					 + openFileCtx.GetNextOffset();
			}

			public virtual void Run()
			{
				try
				{
					openFileCtx.ExecuteWriteBack();
				}
				catch (Exception t)
				{
					Log.Error("Async data service got error: ", t);
				}
			}
		}
	}
}
