using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>
	/// This class is a container of multiple thread pools, each for a volume,
	/// so that we can schedule async disk operations easily.
	/// </summary>
	/// <remarks>
	/// This class is a container of multiple thread pools, each for a volume,
	/// so that we can schedule async disk operations easily.
	/// Examples of async disk operations are deletion of block files.
	/// We don't want to create a new thread for each of the deletion request, and
	/// we don't want to do all deletions in the heartbeat thread since deletion
	/// can be slow, and we don't want to use a single thread pool because that
	/// is inefficient when we have more than 1 volume.  AsyncDiskService is the
	/// solution for these.
	/// Another example of async disk operation is requesting sync_file_range().
	/// This class and
	/// <see cref="Org.Apache.Hadoop.Util.AsyncDiskService"/>
	/// are similar.
	/// They should be combined.
	/// </remarks>
	internal class FsDatasetAsyncDiskService
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.FsDatasetAsyncDiskService
			));

		private const int CoreThreadsPerVolume = 1;

		private const int MaximumThreadsPerVolume = 4;

		private const long ThreadsKeepAliveSeconds = 60;

		private readonly DataNode datanode;

		private readonly FsDatasetImpl fsdatasetImpl;

		private readonly ThreadGroup threadGroup;

		private IDictionary<FilePath, ThreadPoolExecutor> executors = new Dictionary<FilePath
			, ThreadPoolExecutor>();

		private IDictionary<string, ICollection<long>> deletedBlockIds = new Dictionary<string
			, ICollection<long>>();

		private const int MaxDeletedBlocks = 64;

		private int numDeletedBlocks = 0;

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
		internal FsDatasetAsyncDiskService(DataNode datanode, FsDatasetImpl fsdatasetImpl
			)
		{
			// ThreadPool core pool size
			// ThreadPool maximum pool size
			// ThreadPool keep-alive time for threads over core pool size
			this.datanode = datanode;
			this.fsdatasetImpl = fsdatasetImpl;
			this.threadGroup = new ThreadGroup(GetType().Name);
		}

		private void AddExecutorForVolume(FilePath volume)
		{
			ThreadFactory threadFactory = new _ThreadFactory_93(this, volume);
			ThreadPoolExecutor executor = new ThreadPoolExecutor(CoreThreadsPerVolume, MaximumThreadsPerVolume
				, ThreadsKeepAliveSeconds, TimeUnit.Seconds, new LinkedBlockingQueue<Runnable>()
				, threadFactory);
			// This can reduce the number of running threads
			executor.AllowCoreThreadTimeOut(true);
			executors[volume] = executor;
		}

		private sealed class _ThreadFactory_93 : ThreadFactory
		{
			public _ThreadFactory_93(FsDatasetAsyncDiskService _enclosing, FilePath volume)
			{
				this._enclosing = _enclosing;
				this.volume = volume;
				this.counter = 0;
			}

			internal int counter;

			public Sharpen.Thread NewThread(Runnable r)
			{
				int thisIndex;
				lock (this)
				{
					thisIndex = this.counter++;
				}
				Sharpen.Thread t = new Sharpen.Thread(this._enclosing.threadGroup, r);
				t.SetName("Async disk worker #" + thisIndex + " for volume " + volume);
				return t;
			}

			private readonly FsDatasetAsyncDiskService _enclosing;

			private readonly FilePath volume;
		}

		/// <summary>Starts AsyncDiskService for a new volume</summary>
		/// <param name="volume">the root of the new data volume.</param>
		internal virtual void AddVolume(FilePath volume)
		{
			lock (this)
			{
				if (executors == null)
				{
					throw new RuntimeException("AsyncDiskService is already shutdown");
				}
				ThreadPoolExecutor executor = executors[volume];
				if (executor != null)
				{
					throw new RuntimeException("Volume " + volume + " is already existed.");
				}
				AddExecutorForVolume(volume);
			}
		}

		/// <summary>Stops AsyncDiskService for a volume.</summary>
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

		internal virtual long CountPendingDeletions()
		{
			lock (this)
			{
				long count = 0;
				foreach (ThreadPoolExecutor exec in executors.Values)
				{
					count += exec.GetTaskCount() - exec.GetCompletedTaskCount();
				}
				return count;
			}
		}

		/// <summary>Execute the task sometime in the future, using ThreadPools.</summary>
		internal virtual void Execute(FilePath root, Runnable task)
		{
			lock (this)
			{
				if (executors == null)
				{
					throw new RuntimeException("AsyncDiskService is already shutdown");
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
		/// Gracefully shut down all ThreadPool. Will wait for all deletion
		/// tasks to finish.
		/// </remarks>
		internal virtual void Shutdown()
		{
			lock (this)
			{
				if (executors == null)
				{
					Log.Warn("AsyncDiskService has already shut down.");
				}
				else
				{
					Log.Info("Shutting down all async disk service threads");
					foreach (KeyValuePair<FilePath, ThreadPoolExecutor> e in executors)
					{
						e.Value.Shutdown();
					}
					// clear the executor map so that calling execute again will fail.
					executors = null;
					Log.Info("All async disk service threads have been shut down");
				}
			}
		}

		public virtual void SubmitSyncFileRangeRequest(FsVolumeImpl volume, FileDescriptor
			 fd, long offset, long nbytes, int flags)
		{
			Execute(volume.GetCurrentDir(), new _Runnable_199(fd, offset, nbytes, flags));
		}

		private sealed class _Runnable_199 : Runnable
		{
			public _Runnable_199(FileDescriptor fd, long offset, long nbytes, int flags)
			{
				this.fd = fd;
				this.offset = offset;
				this.nbytes = nbytes;
				this.flags = flags;
			}

			public void Run()
			{
				try
				{
					NativeIO.POSIX.SyncFileRangeIfPossible(fd, offset, nbytes, flags);
				}
				catch (NativeIOException e)
				{
					Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.FsDatasetAsyncDiskService.Log
						.Warn("sync_file_range error", e);
				}
			}

			private readonly FileDescriptor fd;

			private readonly long offset;

			private readonly long nbytes;

			private readonly int flags;
		}

		/// <summary>
		/// Delete the block file and meta file from the disk asynchronously, adjust
		/// dfsUsed statistics accordingly.
		/// </summary>
		internal virtual void DeleteAsync(FsVolumeReference volumeRef, FilePath blockFile
			, FilePath metaFile, ExtendedBlock block, string trashDirectory)
		{
			Log.Info("Scheduling " + block.GetLocalBlock() + " file " + blockFile + " for deletion"
				);
			FsDatasetAsyncDiskService.ReplicaFileDeleteTask deletionTask = new FsDatasetAsyncDiskService.ReplicaFileDeleteTask
				(this, volumeRef, blockFile, metaFile, block, trashDirectory);
			Execute(((FsVolumeImpl)volumeRef.GetVolume()).GetCurrentDir(), deletionTask);
		}

		/// <summary>
		/// A task for deleting a block file and its associated meta file, as well
		/// as decrement the dfs usage of the volume.
		/// </summary>
		/// <remarks>
		/// A task for deleting a block file and its associated meta file, as well
		/// as decrement the dfs usage of the volume.
		/// Optionally accepts a trash directory. If one is specified then the files
		/// are moved to trash instead of being deleted. If none is specified then the
		/// files are deleted immediately.
		/// </remarks>
		internal class ReplicaFileDeleteTask : Runnable
		{
			internal readonly FsVolumeReference volumeRef;

			internal readonly FsVolumeImpl volume;

			internal readonly FilePath blockFile;

			internal readonly FilePath metaFile;

			internal readonly ExtendedBlock block;

			internal readonly string trashDirectory;

			internal ReplicaFileDeleteTask(FsDatasetAsyncDiskService _enclosing, FsVolumeReference
				 volumeRef, FilePath blockFile, FilePath metaFile, ExtendedBlock block, string trashDirectory
				)
			{
				this._enclosing = _enclosing;
				this.volumeRef = volumeRef;
				this.volume = (FsVolumeImpl)volumeRef.GetVolume();
				this.blockFile = blockFile;
				this.metaFile = metaFile;
				this.block = block;
				this.trashDirectory = trashDirectory;
			}

			public override string ToString()
			{
				// Called in AsyncDiskService.execute for displaying error messages.
				return "deletion of block " + this.block.GetBlockPoolId() + " " + this.block.GetLocalBlock
					() + " with block file " + this.blockFile + " and meta file " + this.metaFile + 
					" from volume " + this.volume;
			}

			private bool DeleteFiles()
			{
				return this.blockFile.Delete() && (this.metaFile.Delete() || !this.metaFile.Exists
					());
			}

			private bool MoveFiles()
			{
				FilePath trashDirFile = new FilePath(this.trashDirectory);
				if (!trashDirFile.Exists() && !trashDirFile.Mkdirs())
				{
					FsDatasetAsyncDiskService.Log.Error("Failed to create trash directory " + this.trashDirectory
						);
					return false;
				}
				if (FsDatasetAsyncDiskService.Log.IsDebugEnabled())
				{
					FsDatasetAsyncDiskService.Log.Debug("Moving files " + this.blockFile.GetName() + 
						" and " + this.metaFile.GetName() + " to trash.");
				}
				FilePath newBlockFile = new FilePath(this.trashDirectory, this.blockFile.GetName(
					));
				FilePath newMetaFile = new FilePath(this.trashDirectory, this.metaFile.GetName());
				return (this.blockFile.RenameTo(newBlockFile) && this.metaFile.RenameTo(newMetaFile
					));
			}

			public virtual void Run()
			{
				long dfsBytes = this.blockFile.Length() + this.metaFile.Length();
				bool result;
				result = (this.trashDirectory == null) ? this.DeleteFiles() : this.MoveFiles();
				if (!result)
				{
					FsDatasetAsyncDiskService.Log.Warn("Unexpected error trying to " + (this.trashDirectory
						 == null ? "delete" : "move") + " block " + this.block.GetBlockPoolId() + " " + 
						this.block.GetLocalBlock() + " at file " + this.blockFile + ". Ignored.");
				}
				else
				{
					if (this.block.GetLocalBlock().GetNumBytes() != BlockCommand.NoAck)
					{
						this._enclosing.datanode.NotifyNamenodeDeletedBlock(this.block, this.volume.GetStorageID
							());
					}
					this.volume.DecDfsUsed(this.block.GetBlockPoolId(), dfsBytes);
					FsDatasetAsyncDiskService.Log.Info("Deleted " + this.block.GetBlockPoolId() + " "
						 + this.block.GetLocalBlock() + " file " + this.blockFile);
				}
				this._enclosing.UpdateDeletedBlockId(this.block);
				IOUtils.Cleanup(null, this.volumeRef);
			}

			private readonly FsDatasetAsyncDiskService _enclosing;
		}

		private void UpdateDeletedBlockId(ExtendedBlock block)
		{
			lock (this)
			{
				ICollection<long> blockIds = deletedBlockIds[block.GetBlockPoolId()];
				if (blockIds == null)
				{
					blockIds = new HashSet<long>();
					deletedBlockIds[block.GetBlockPoolId()] = blockIds;
				}
				blockIds.AddItem(block.GetBlockId());
				numDeletedBlocks++;
				if (numDeletedBlocks == MaxDeletedBlocks)
				{
					foreach (KeyValuePair<string, ICollection<long>> e in deletedBlockIds)
					{
						string bpid = e.Key;
						ICollection<long> bs = e.Value;
						fsdatasetImpl.RemoveDeletedBlocks(bpid, bs);
						bs.Clear();
					}
					numDeletedBlocks = 0;
				}
			}
		}
	}
}
