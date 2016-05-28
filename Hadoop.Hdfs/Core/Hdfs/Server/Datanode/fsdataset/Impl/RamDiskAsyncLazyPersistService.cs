using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>
	/// This class is a container of multiple thread pools, one for each non-RamDisk
	/// volume with a maximum thread count of 1 so that we can schedule async lazy
	/// persist operations easily with volume arrival and departure handled.
	/// </summary>
	/// <remarks>
	/// This class is a container of multiple thread pools, one for each non-RamDisk
	/// volume with a maximum thread count of 1 so that we can schedule async lazy
	/// persist operations easily with volume arrival and departure handled.
	/// This class and
	/// <see cref="Org.Apache.Hadoop.Util.AsyncDiskService"/>
	/// are similar.
	/// They should be combined.
	/// </remarks>
	internal class RamDiskAsyncLazyPersistService
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.RamDiskAsyncLazyPersistService
			));

		private const int CoreThreadsPerVolume = 1;

		private const int MaximumThreadsPerVolume = 1;

		private const long ThreadsKeepAliveSeconds = 60;

		private readonly DataNode datanode;

		private readonly ThreadGroup threadGroup;

		private IDictionary<FilePath, ThreadPoolExecutor> executors = new Dictionary<FilePath
			, ThreadPoolExecutor>();

		/// <summary>
		/// Create a RamDiskAsyncLazyPersistService with a set of volumes (specified by their
		/// root directories).
		/// </summary>
		/// <remarks>
		/// Create a RamDiskAsyncLazyPersistService with a set of volumes (specified by their
		/// root directories).
		/// The RamDiskAsyncLazyPersistService uses one ThreadPool per volume to do the async
		/// disk operations.
		/// </remarks>
		internal RamDiskAsyncLazyPersistService(DataNode datanode)
		{
			// ThreadPool core pool size
			// ThreadPool maximum pool size
			// ThreadPool keep-alive time for threads over core pool size
			this.datanode = datanode;
			this.threadGroup = new ThreadGroup(GetType().Name);
		}

		private void AddExecutorForVolume(FilePath volume)
		{
			ThreadFactory threadFactory = new _ThreadFactory_71(this, volume);
			ThreadPoolExecutor executor = new ThreadPoolExecutor(CoreThreadsPerVolume, MaximumThreadsPerVolume
				, ThreadsKeepAliveSeconds, TimeUnit.Seconds, new LinkedBlockingQueue<Runnable>()
				, threadFactory);
			// This can reduce the number of running threads
			executor.AllowCoreThreadTimeOut(true);
			executors[volume] = executor;
		}

		private sealed class _ThreadFactory_71 : ThreadFactory
		{
			public _ThreadFactory_71(RamDiskAsyncLazyPersistService _enclosing, FilePath volume
				)
			{
				this._enclosing = _enclosing;
				this.volume = volume;
			}

			public Sharpen.Thread NewThread(Runnable r)
			{
				Sharpen.Thread t = new Sharpen.Thread(this._enclosing.threadGroup, r);
				t.SetName("Async RamDisk lazy persist worker for volume " + volume);
				return t;
			}

			private readonly RamDiskAsyncLazyPersistService _enclosing;

			private readonly FilePath volume;
		}

		/// <summary>Starts AsyncLazyPersistService for a new volume</summary>
		/// <param name="volume">the root of the new data volume.</param>
		internal virtual void AddVolume(FilePath volume)
		{
			lock (this)
			{
				if (executors == null)
				{
					throw new RuntimeException("AsyncLazyPersistService is already shutdown");
				}
				ThreadPoolExecutor executor = executors[volume];
				if (executor != null)
				{
					throw new RuntimeException("Volume " + volume + " is already existed.");
				}
				AddExecutorForVolume(volume);
			}
		}

		/// <summary>Stops AsyncLazyPersistService for a volume.</summary>
		/// <param name="volume">the root of the volume.</param>
		internal virtual void RemoveVolume(FilePath volume)
		{
			lock (this)
			{
				if (executors == null)
				{
					throw new RuntimeException("AsyncDiskService is already shutdown");
				}
				ThreadPoolExecutor executor = executors[volume];
				if (executor == null)
				{
					throw new RuntimeException("Can not find volume " + volume + " to remove.");
				}
				else
				{
					executor.Shutdown();
					Sharpen.Collections.Remove(executors, volume);
				}
			}
		}

		/// <summary>Query if the thread pool exist for the volume</summary>
		/// <param name="volume">the root of a volume</param>
		/// <returns>
		/// true if there is one thread pool for the volume
		/// false otherwise
		/// </returns>
		internal virtual bool QueryVolume(FilePath volume)
		{
			lock (this)
			{
				if (executors == null)
				{
					throw new RuntimeException("AsyncLazyPersistService is already shutdown");
				}
				ThreadPoolExecutor executor = executors[volume];
				return (executor != null);
			}
		}

		/// <summary>Execute the task sometime in the future, using ThreadPools.</summary>
		internal virtual void Execute(FilePath root, Runnable task)
		{
			lock (this)
			{
				if (executors == null)
				{
					throw new RuntimeException("AsyncLazyPersistService is already shutdown");
				}
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

		/// <summary>Gracefully shut down all ThreadPool.</summary>
		/// <remarks>
		/// Gracefully shut down all ThreadPool. Will wait for all lazy persist
		/// tasks to finish.
		/// </remarks>
		internal virtual void Shutdown()
		{
			lock (this)
			{
				if (executors == null)
				{
					Log.Warn("AsyncLazyPersistService has already shut down.");
				}
				else
				{
					Log.Info("Shutting down all async lazy persist service threads");
					foreach (KeyValuePair<FilePath, ThreadPoolExecutor> e in executors)
					{
						e.Value.Shutdown();
					}
					// clear the executor map so that calling execute again will fail.
					executors = null;
					Log.Info("All async lazy persist service threads have been shut down");
				}
			}
		}

		/// <summary>Asynchronously lazy persist the block from the RamDisk to Disk.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SubmitLazyPersistTask(string bpId, long blockId, long genStamp
			, long creationTime, FilePath metaFile, FilePath blockFile, FsVolumeReference target
			)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("LazyWriter schedule async task to persist RamDisk block pool id: " + bpId
					 + " block id: " + blockId);
			}
			FsVolumeImpl volume = (FsVolumeImpl)target.GetVolume();
			FilePath lazyPersistDir = volume.GetLazyPersistDir(bpId);
			if (!lazyPersistDir.Exists() && !lazyPersistDir.Mkdirs())
			{
				FsDatasetImpl.Log.Warn("LazyWriter failed to create " + lazyPersistDir);
				throw new IOException("LazyWriter fail to find or create lazy persist dir: " + lazyPersistDir
					.ToString());
			}
			RamDiskAsyncLazyPersistService.ReplicaLazyPersistTask lazyPersistTask = new RamDiskAsyncLazyPersistService.ReplicaLazyPersistTask
				(this, bpId, blockId, genStamp, creationTime, blockFile, metaFile, target, lazyPersistDir
				);
			Execute(volume.GetCurrentDir(), lazyPersistTask);
		}

		internal class ReplicaLazyPersistTask : Runnable
		{
			internal readonly string bpId;

			internal readonly long blockId;

			internal readonly long genStamp;

			internal readonly long creationTime;

			internal readonly FilePath blockFile;

			internal readonly FilePath metaFile;

			internal readonly FsVolumeReference targetVolume;

			internal readonly FilePath lazyPersistDir;

			internal ReplicaLazyPersistTask(RamDiskAsyncLazyPersistService _enclosing, string
				 bpId, long blockId, long genStamp, long creationTime, FilePath blockFile, FilePath
				 metaFile, FsVolumeReference targetVolume, FilePath lazyPersistDir)
			{
				this._enclosing = _enclosing;
				this.bpId = bpId;
				this.blockId = blockId;
				this.genStamp = genStamp;
				this.creationTime = creationTime;
				this.blockFile = blockFile;
				this.metaFile = metaFile;
				this.targetVolume = targetVolume;
				this.lazyPersistDir = lazyPersistDir;
			}

			public override string ToString()
			{
				// Called in AsyncLazyPersistService.execute for displaying error messages.
				return "LazyWriter async task of persist RamDisk block pool id:" + this.bpId + " block pool id: "
					 + this.blockId + " with block file " + this.blockFile + " and meta file " + this
					.metaFile + " to target volume " + this.targetVolume;
			}

			public virtual void Run()
			{
				bool succeeded = false;
				FsDatasetImpl dataset = (FsDatasetImpl)this._enclosing.datanode.GetFSDataset();
				try
				{
					// No FsDatasetImpl lock for the file copy
					FilePath[] targetFiles = FsDatasetImpl.CopyBlockFiles(this.blockId, this.genStamp
						, this.metaFile, this.blockFile, this.lazyPersistDir, true);
					// Lock FsDataSetImpl during onCompleteLazyPersist callback
					dataset.OnCompleteLazyPersist(this.bpId, this.blockId, this.creationTime, targetFiles
						, (FsVolumeImpl)this.targetVolume.GetVolume());
					succeeded = true;
				}
				catch (Exception e)
				{
					FsDatasetImpl.Log.Warn("LazyWriter failed to async persist RamDisk block pool id: "
						 + this.bpId + "block Id: " + this.blockId, e);
				}
				finally
				{
					if (!succeeded)
					{
						dataset.OnFailLazyPersist(this.bpId, this.blockId);
					}
				}
			}

			private readonly RamDiskAsyncLazyPersistService _enclosing;
		}
	}
}
