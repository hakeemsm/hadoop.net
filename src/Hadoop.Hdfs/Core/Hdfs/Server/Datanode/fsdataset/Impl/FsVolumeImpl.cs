using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson.Annotate;
using Org.Codehaus.Jackson.Map;
using Org.Slf4j;
using Sharpen;
using Sharpen.File;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>The underlying volume used to store replica.</summary>
	/// <remarks>
	/// The underlying volume used to store replica.
	/// It uses the
	/// <see cref="FsDatasetImpl"/>
	/// object for synchronization.
	/// </remarks>
	public class FsVolumeImpl : FsVolumeSpi
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.FsVolumeImpl
			));

		private readonly FsDatasetImpl dataset;

		private readonly string storageID;

		private readonly StorageType storageType;

		private readonly IDictionary<string, BlockPoolSlice> bpSlices = new ConcurrentHashMap
			<string, BlockPoolSlice>();

		private readonly FilePath currentDir;

		private readonly DF usage;

		private readonly long reserved;

		private CloseableReferenceCount reference = new CloseableReferenceCount();

		private AtomicLong reservedForRbw;

		protected internal volatile long configuredCapacity;

		/// <summary>Per-volume worker pool that processes new blocks to cache.</summary>
		/// <remarks>
		/// Per-volume worker pool that processes new blocks to cache.
		/// The maximum number of workers per volume is bounded (configurable via
		/// dfs.datanode.fsdatasetcache.max.threads.per.volume) to limit resource
		/// contention.
		/// </remarks>
		protected internal ThreadPoolExecutor cacheExecutor;

		/// <exception cref="System.IO.IOException"/>
		internal FsVolumeImpl(FsDatasetImpl dataset, string storageID, FilePath currentDir
			, Configuration conf, StorageType storageType)
		{
			// <StorageDirectory>/current
			// Disk space reserved for open blocks.
			// Capacity configured. This is useful when we want to
			// limit the visible capacity for tests. If negative, then we just
			// query from the filesystem.
			this.dataset = dataset;
			this.storageID = storageID;
			this.reserved = conf.GetLong(DFSConfigKeys.DfsDatanodeDuReservedKey, DFSConfigKeys
				.DfsDatanodeDuReservedDefault);
			this.reservedForRbw = new AtomicLong(0L);
			this.currentDir = currentDir;
			FilePath parent = currentDir.GetParentFile();
			this.usage = new DF(parent, conf);
			this.storageType = storageType;
			this.configuredCapacity = -1;
			cacheExecutor = InitializeCacheExecutor(parent);
		}

		protected internal virtual ThreadPoolExecutor InitializeCacheExecutor(FilePath parent
			)
		{
			if (storageType.IsTransient())
			{
				return null;
			}
			if (dataset.datanode == null)
			{
				// FsVolumeImpl is used in test.
				return null;
			}
			int maxNumThreads = dataset.datanode.GetConf().GetInt(DFSConfigKeys.DfsDatanodeFsdatasetcacheMaxThreadsPerVolumeKey
				, DFSConfigKeys.DfsDatanodeFsdatasetcacheMaxThreadsPerVolumeDefault);
			ThreadFactory workerFactory = new ThreadFactoryBuilder().SetDaemon(true).SetNameFormat
				("FsVolumeImplWorker-" + parent.ToString() + "-%d").Build();
			ThreadPoolExecutor executor = new ThreadPoolExecutor(1, maxNumThreads, 60, TimeUnit
				.Seconds, new LinkedBlockingQueue<Runnable>(), workerFactory);
			executor.AllowCoreThreadTimeOut(true);
			return executor;
		}

		private void PrintReferenceTraceInfo(string op)
		{
			StackTraceElement[] stack = Sharpen.Thread.CurrentThread().GetStackTrace();
			foreach (StackTraceElement ste in stack)
			{
				switch (ste.GetMethodName())
				{
					case "getDfsUsed":
					case "getBlockPoolUsed":
					case "getAvailable":
					case "getVolumeMap":
					{
						return;
					}

					default:
					{
						break;
					}
				}
			}
			FsDatasetImpl.Log.Trace("Reference count: " + op + " " + this + ": " + this.reference
				.GetReferenceCount());
			FsDatasetImpl.Log.Trace(Joiner.On("\n").Join(Sharpen.Thread.CurrentThread().GetStackTrace
				()));
		}

		/// <summary>Increase the reference count.</summary>
		/// <remarks>
		/// Increase the reference count. The caller must increase the reference count
		/// before issuing IOs.
		/// </remarks>
		/// <exception cref="System.IO.IOException">if the volume is already closed.</exception>
		/// <exception cref="Sharpen.ClosedChannelException"/>
		private void Reference()
		{
			this.reference.Reference();
			if (FsDatasetImpl.Log.IsTraceEnabled())
			{
				PrintReferenceTraceInfo("incr");
			}
		}

		/// <summary>Decrease the reference count.</summary>
		private void Unreference()
		{
			if (FsDatasetImpl.Log.IsTraceEnabled())
			{
				PrintReferenceTraceInfo("desc");
			}
			if (FsDatasetImpl.Log.IsDebugEnabled())
			{
				if (reference.GetReferenceCount() <= 0)
				{
					FsDatasetImpl.Log.Debug("Decrease reference count <= 0 on " + this + Joiner.On("\n"
						).Join(Sharpen.Thread.CurrentThread().GetStackTrace()));
				}
			}
			CheckReference();
			this.reference.Unreference();
		}

		private class FsVolumeReferenceImpl : FsVolumeReference
		{
			private readonly FsVolumeImpl volume;

			/// <exception cref="Sharpen.ClosedChannelException"/>
			internal FsVolumeReferenceImpl(FsVolumeImpl volume)
			{
				this.volume = volume;
				volume.Reference();
			}

			/// <summary>Decreases the reference count.</summary>
			/// <exception cref="System.IO.IOException">it never throws IOException.</exception>
			public virtual void Close()
			{
				volume.Unreference();
			}

			public virtual FsVolumeSpi GetVolume()
			{
				return this.volume;
			}
		}

		/// <exception cref="Sharpen.ClosedChannelException"/>
		public override FsVolumeReference ObtainReference()
		{
			return new FsVolumeImpl.FsVolumeReferenceImpl(this);
		}

		private void CheckReference()
		{
			Preconditions.CheckState(reference.GetReferenceCount() > 0);
		}

		/// <summary>
		/// Close this volume and wait all other threads to release the reference count
		/// on this volume.
		/// </summary>
		/// <exception cref="System.IO.IOException">if the volume is closed or the waiting is interrupted.
		/// 	</exception>
		internal virtual void CloseAndWait()
		{
			try
			{
				this.reference.SetClosed();
			}
			catch (ClosedChannelException e)
			{
				throw new IOException("The volume has already closed.", e);
			}
			int SleepMillis = 500;
			while (this.reference.GetReferenceCount() > 0)
			{
				if (FsDatasetImpl.Log.IsDebugEnabled())
				{
					FsDatasetImpl.Log.Debug(string.Format("The reference count for %s is %d, wait to be 0."
						, this, reference.GetReferenceCount()));
				}
				try
				{
					Sharpen.Thread.Sleep(SleepMillis);
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
			}
		}

		internal virtual FilePath GetCurrentDir()
		{
			return currentDir;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath GetRbwDir(string bpid)
		{
			return GetBlockPoolSlice(bpid).GetRbwDir();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath GetLazyPersistDir(string bpid)
		{
			return GetBlockPoolSlice(bpid).GetLazypersistDir();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath GetTmpDir(string bpid)
		{
			return GetBlockPoolSlice(bpid).GetTmpDir();
		}

		internal virtual void DecDfsUsed(string bpid, long value)
		{
			lock (dataset)
			{
				BlockPoolSlice bp = bpSlices[bpid];
				if (bp != null)
				{
					bp.DecDfsUsed(value);
				}
			}
		}

		internal virtual void IncDfsUsed(string bpid, long value)
		{
			lock (dataset)
			{
				BlockPoolSlice bp = bpSlices[bpid];
				if (bp != null)
				{
					bp.IncDfsUsed(value);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual long GetDfsUsed()
		{
			long dfsUsed = 0;
			lock (dataset)
			{
				foreach (BlockPoolSlice s in bpSlices.Values)
				{
					dfsUsed += s.GetDfsUsed();
				}
			}
			return dfsUsed;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long GetBlockPoolUsed(string bpid)
		{
			return GetBlockPoolSlice(bpid).GetDfsUsed();
		}

		/// <summary>
		/// Return either the configured capacity of the file system if configured; or
		/// the capacity of the file system excluding space reserved for non-HDFS.
		/// </summary>
		/// <returns>
		/// the unreserved number of bytes left in this filesystem. May be
		/// zero.
		/// </returns>
		[VisibleForTesting]
		public virtual long GetCapacity()
		{
			if (configuredCapacity < 0)
			{
				long remaining = usage.GetCapacity() - reserved;
				return remaining > 0 ? remaining : 0;
			}
			return configuredCapacity;
		}

		/// <summary>This function MUST NOT be used outside of tests.</summary>
		/// <param name="capacity"/>
		[VisibleForTesting]
		public virtual void SetCapacityForTesting(long capacity)
		{
			this.configuredCapacity = capacity;
		}

		/*
		* Calculate the available space of the filesystem, excluding space reserved
		* for non-HDFS and space reserved for RBW
		*
		* @return the available number of bytes left in this filesystem. May be zero.
		*/
		/// <exception cref="System.IO.IOException"/>
		public override long GetAvailable()
		{
			long remaining = GetCapacity() - GetDfsUsed() - reservedForRbw.Get();
			long available = usage.GetAvailable() - reserved - reservedForRbw.Get();
			if (remaining > available)
			{
				remaining = available;
			}
			return (remaining > 0) ? remaining : 0;
		}

		[VisibleForTesting]
		public virtual long GetReservedForRbw()
		{
			return reservedForRbw.Get();
		}

		internal virtual long GetReserved()
		{
			return reserved;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual BlockPoolSlice GetBlockPoolSlice(string bpid)
		{
			BlockPoolSlice bp = bpSlices[bpid];
			if (bp == null)
			{
				throw new IOException("block pool " + bpid + " is not found");
			}
			return bp;
		}

		public override string GetBasePath()
		{
			return currentDir.GetParent();
		}

		public override bool IsTransientStorage()
		{
			return storageType.IsTransient();
		}

		/// <exception cref="System.IO.IOException"/>
		public override string GetPath(string bpid)
		{
			return GetBlockPoolSlice(bpid).GetDirectory().GetAbsolutePath();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FilePath GetFinalizedDir(string bpid)
		{
			return GetBlockPoolSlice(bpid).GetFinalizedDir();
		}

		/// <summary>Make a deep copy of the list of currently active BPIDs</summary>
		public override string[] GetBlockPoolList()
		{
			return Sharpen.Collections.ToArray(bpSlices.Keys, new string[bpSlices.Keys.Count]
				);
		}

		/// <summary>Temporary files.</summary>
		/// <remarks>
		/// Temporary files. They get moved to the finalized block directory when
		/// the block is finalized.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath CreateTmpFile(string bpid, Block b)
		{
			CheckReference();
			return GetBlockPoolSlice(bpid).CreateTmpFile(b);
		}

		public override void ReserveSpaceForRbw(long bytesToReserve)
		{
			if (bytesToReserve != 0)
			{
				reservedForRbw.AddAndGet(bytesToReserve);
			}
		}

		public override void ReleaseReservedSpace(long bytesToRelease)
		{
			if (bytesToRelease != 0)
			{
				long oldReservation;
				long newReservation;
				do
				{
					oldReservation = reservedForRbw.Get();
					newReservation = oldReservation - bytesToRelease;
					if (newReservation < 0)
					{
						// Failsafe, this should never occur in practice, but if it does we don't
						// want to start advertising more space than we have available.
						newReservation = 0;
					}
				}
				while (!reservedForRbw.CompareAndSet(oldReservation, newReservation));
			}
		}

		[System.Serializable]
		private sealed class SubdirFilter : FilenameFilter
		{
			public static readonly FsVolumeImpl.SubdirFilter Instance = new FsVolumeImpl.SubdirFilter
				();

			public bool Accept(FilePath dir, string name)
			{
				return name.StartsWith("subdir");
			}
		}

		[System.Serializable]
		private sealed class BlockFileFilter : FilenameFilter
		{
			public static readonly FsVolumeImpl.BlockFileFilter Instance = new FsVolumeImpl.BlockFileFilter
				();

			public bool Accept(FilePath dir, string name)
			{
				return !name.EndsWith(".meta") && name.StartsWith(Block.BlockFilePrefix);
			}
		}

		[VisibleForTesting]
		public static string NextSorted(IList<string> arr, string prev)
		{
			int res = 0;
			if (prev != null)
			{
				res = Sharpen.Collections.BinarySearch(arr, prev);
				if (res < 0)
				{
					res = -1 - res;
				}
				else
				{
					res++;
				}
			}
			if (res >= arr.Count)
			{
				return null;
			}
			return arr[res];
		}

		private class BlockIteratorState
		{
			internal BlockIteratorState()
			{
				lastSavedMs = iterStartMs = Time.Now();
				curFinalizedDir = null;
				curFinalizedSubDir = null;
				curEntry = null;
				atEnd = false;
			}

			[JsonProperty]
			private long lastSavedMs;

			[JsonProperty]
			private long iterStartMs;

			[JsonProperty]
			private string curFinalizedDir;

			[JsonProperty]
			private string curFinalizedSubDir;

			[JsonProperty]
			private string curEntry;

			[JsonProperty]
			private bool atEnd;
			// The wall-clock ms since the epoch at which this iterator was last saved.
			// The wall-clock ms since the epoch at which this iterator was created.
		}

		/// <summary>A BlockIterator implementation for FsVolumeImpl.</summary>
		private class BlockIteratorImpl : FsVolumeSpi.BlockIterator
		{
			private readonly FilePath bpidDir;

			private readonly string name;

			private readonly string bpid;

			private long maxStalenessMs = 0;

			private IList<string> cache;

			private long cacheMs;

			private FsVolumeImpl.BlockIteratorState state;

			internal BlockIteratorImpl(FsVolumeImpl _enclosing, string bpid, string name)
			{
				this._enclosing = _enclosing;
				this.bpidDir = new FilePath(this._enclosing.currentDir, bpid);
				this.name = name;
				this.bpid = bpid;
				this.Rewind();
			}

			/// <summary>Get the next subdirectory within the block pool slice.</summary>
			/// <returns>
			/// The next subdirectory within the block pool slice, or
			/// null if there are no more.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			private string GetNextSubDir(string prev, FilePath dir)
			{
				IList<string> children = IOUtils.ListDirectory(dir, FsVolumeImpl.SubdirFilter.Instance
					);
				this.cache = null;
				this.cacheMs = 0;
				if (children.Count == 0)
				{
					FsVolumeImpl.Log.Trace("getNextSubDir({}, {}): no subdirectories found in {}", this
						._enclosing.storageID, this.bpid, dir.GetAbsolutePath());
					return null;
				}
				children.Sort();
				string nextSubDir = FsVolumeImpl.NextSorted(children, prev);
				if (nextSubDir == null)
				{
					FsVolumeImpl.Log.Trace("getNextSubDir({}, {}): no more subdirectories found in {}"
						, this._enclosing.storageID, this.bpid, dir.GetAbsolutePath());
				}
				else
				{
					FsVolumeImpl.Log.Trace("getNextSubDir({}, {}): picking next subdirectory {} " + "within {}"
						, this._enclosing.storageID, this.bpid, nextSubDir, dir.GetAbsolutePath());
				}
				return nextSubDir;
			}

			/// <exception cref="System.IO.IOException"/>
			private string GetNextFinalizedDir()
			{
				FilePath dir = Paths.Get(this.bpidDir.GetAbsolutePath(), "current", "finalized").
					ToFile();
				return this.GetNextSubDir(this.state.curFinalizedDir, dir);
			}

			/// <exception cref="System.IO.IOException"/>
			private string GetNextFinalizedSubDir()
			{
				if (this.state.curFinalizedDir == null)
				{
					return null;
				}
				FilePath dir = Paths.Get(this.bpidDir.GetAbsolutePath(), "current", "finalized", 
					this.state.curFinalizedDir).ToFile();
				return this.GetNextSubDir(this.state.curFinalizedSubDir, dir);
			}

			/// <exception cref="System.IO.IOException"/>
			private IList<string> GetSubdirEntries()
			{
				if (this.state.curFinalizedSubDir == null)
				{
					return null;
				}
				// There are no entries in the null subdir.
				long now = Time.MonotonicNow();
				if (this.cache != null)
				{
					long delta = now - this.cacheMs;
					if (delta < this.maxStalenessMs)
					{
						return this.cache;
					}
					else
					{
						FsVolumeImpl.Log.Trace("getSubdirEntries({}, {}): purging entries cache for {} " 
							+ "after {} ms.", this._enclosing.storageID, this.bpid, this.state.curFinalizedSubDir
							, delta);
						this.cache = null;
					}
				}
				FilePath dir = Paths.Get(this.bpidDir.GetAbsolutePath(), "current", "finalized", 
					this.state.curFinalizedDir, this.state.curFinalizedSubDir).ToFile();
				IList<string> entries = IOUtils.ListDirectory(dir, FsVolumeImpl.BlockFileFilter.Instance
					);
				if (entries.Count == 0)
				{
					entries = null;
				}
				else
				{
					entries.Sort();
				}
				if (entries == null)
				{
					FsVolumeImpl.Log.Trace("getSubdirEntries({}, {}): no entries found in {}", this._enclosing
						.storageID, this.bpid, dir.GetAbsolutePath());
				}
				else
				{
					FsVolumeImpl.Log.Trace("getSubdirEntries({}, {}): listed {} entries in {}", this.
						_enclosing.storageID, this.bpid, entries.Count, dir.GetAbsolutePath());
				}
				this.cache = entries;
				this.cacheMs = now;
				return this.cache;
			}

			/// <summary>
			/// Get the next block.<p/>
			/// Each volume has a hierarchical structure.<p/>
			/// <code>
			/// BPID B0
			/// finalized/
			/// subdir0
			/// subdir0
			/// blk_000
			/// blk_001
			/// ...
			/// </summary>
			/// <remarks>
			/// Get the next block.<p/>
			/// Each volume has a hierarchical structure.<p/>
			/// <code>
			/// BPID B0
			/// finalized/
			/// subdir0
			/// subdir0
			/// blk_000
			/// blk_001
			/// ...
			/// subdir1
			/// subdir0
			/// ...
			/// rbw/
			/// </code>
			/// When we run out of entries at one level of the structure, we search
			/// progressively higher levels.  For example, when we run out of blk_
			/// entries in a subdirectory, we search for the next subdirectory.
			/// And so on.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual ExtendedBlock NextBlock()
			{
				if (this.state.atEnd)
				{
					return null;
				}
				try
				{
					while (true)
					{
						IList<string> entries = this.GetSubdirEntries();
						if (entries != null)
						{
							this.state.curEntry = FsVolumeImpl.NextSorted(entries, this.state.curEntry);
							if (this.state.curEntry == null)
							{
								FsVolumeImpl.Log.Trace("nextBlock({}, {}): advancing from {} to next " + "subdirectory."
									, this._enclosing.storageID, this.bpid, this.state.curFinalizedSubDir);
							}
							else
							{
								ExtendedBlock block = new ExtendedBlock(this.bpid, Block.Filename2id(this.state.curEntry
									));
								FsVolumeImpl.Log.Trace("nextBlock({}, {}): advancing to {}", this._enclosing.storageID
									, this.bpid, block);
								return block;
							}
						}
						this.state.curFinalizedSubDir = this.GetNextFinalizedSubDir();
						if (this.state.curFinalizedSubDir == null)
						{
							this.state.curFinalizedDir = this.GetNextFinalizedDir();
							if (this.state.curFinalizedDir == null)
							{
								this.state.atEnd = true;
								return null;
							}
						}
					}
				}
				catch (IOException e)
				{
					this.state.atEnd = true;
					FsVolumeImpl.Log.Error("nextBlock({}, {}): I/O error", this._enclosing.storageID, 
						this.bpid, e);
					throw;
				}
			}

			public virtual bool AtEnd()
			{
				return this.state.atEnd;
			}

			public virtual void Rewind()
			{
				this.cache = null;
				this.cacheMs = 0;
				this.state = new FsVolumeImpl.BlockIteratorState();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Save()
			{
				this.state.lastSavedMs = Time.Now();
				bool success = false;
				ObjectMapper mapper = new ObjectMapper();
				try
				{
					using (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream
						(this.GetTempSaveFile(), false), "UTF-8")))
					{
						mapper.WriterWithDefaultPrettyPrinter().WriteValue(writer, this.state);
						success = true;
					}
				}
				finally
				{
					if (!success)
					{
						if (this.GetTempSaveFile().Delete())
						{
							FsVolumeImpl.Log.Debug("save({}, {}): error deleting temporary file.", this._enclosing
								.storageID, this.bpid);
						}
					}
				}
				Files.Move(this.GetTempSaveFile().ToPath(), this.GetSaveFile().ToPath(), StandardCopyOption
					.AtomicMove);
				if (FsVolumeImpl.Log.IsTraceEnabled())
				{
					FsVolumeImpl.Log.Trace("save({}, {}): saved {}", this._enclosing.storageID, this.
						bpid, mapper.WriterWithDefaultPrettyPrinter().WriteValueAsString(this.state));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Load()
			{
				ObjectMapper mapper = new ObjectMapper();
				FilePath file = this.GetSaveFile();
				this.state = mapper.Reader(typeof(FsVolumeImpl.BlockIteratorState)).ReadValue(file
					);
				FsVolumeImpl.Log.Trace("load({}, {}): loaded iterator {} from {}: {}", this._enclosing
					.storageID, this.bpid, this.name, file.GetAbsoluteFile(), mapper.WriterWithDefaultPrettyPrinter
					().WriteValueAsString(this.state));
			}

			internal virtual FilePath GetSaveFile()
			{
				return new FilePath(this.bpidDir, this.name + ".cursor");
			}

			internal virtual FilePath GetTempSaveFile()
			{
				return new FilePath(this.bpidDir, this.name + ".cursor.tmp");
			}

			public virtual void SetMaxStalenessMs(long maxStalenessMs)
			{
				this.maxStalenessMs = maxStalenessMs;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}

			// No action needed for this volume implementation.
			public virtual long GetIterStartMs()
			{
				return this.state.iterStartMs;
			}

			public virtual long GetLastSavedMs()
			{
				return this.state.lastSavedMs;
			}

			public virtual string GetBlockPoolId()
			{
				return this.bpid;
			}

			private readonly FsVolumeImpl _enclosing;
		}

		public override FsVolumeSpi.BlockIterator NewBlockIterator(string bpid, string name
			)
		{
			return new FsVolumeImpl.BlockIteratorImpl(this, bpid, name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FsVolumeSpi.BlockIterator LoadBlockIterator(string bpid, string name
			)
		{
			FsVolumeImpl.BlockIteratorImpl iter = new FsVolumeImpl.BlockIteratorImpl(this, bpid
				, name);
			iter.Load();
			return iter;
		}

		public override FsDatasetSpi GetDataset()
		{
			return dataset;
		}

		/// <summary>RBW files.</summary>
		/// <remarks>
		/// RBW files. They get moved to the finalized block directory when
		/// the block is finalized.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath CreateRbwFile(string bpid, Block b)
		{
			CheckReference();
			ReserveSpaceForRbw(b.GetNumBytes());
			try
			{
				return GetBlockPoolSlice(bpid).CreateRbwFile(b);
			}
			catch (IOException exception)
			{
				ReleaseReservedSpace(b.GetNumBytes());
				throw;
			}
		}

		/// <param name="bytesReservedForRbw">
		/// Space that was reserved during
		/// block creation. Now that the block is being finalized we
		/// can free up this space.
		/// </param>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath AddFinalizedBlock(string bpid, Block b, FilePath f, long
			 bytesReservedForRbw)
		{
			ReleaseReservedSpace(bytesReservedForRbw);
			return GetBlockPoolSlice(bpid).AddBlock(b, f);
		}

		internal virtual Executor GetCacheExecutor()
		{
			return cacheExecutor;
		}

		/// <exception cref="Org.Apache.Hadoop.Util.DiskChecker.DiskErrorException"/>
		internal virtual void CheckDirs()
		{
			// TODO:FEDERATION valid synchronization
			foreach (BlockPoolSlice s in bpSlices.Values)
			{
				s.CheckDirs();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void GetVolumeMap(ReplicaMap volumeMap, RamDiskReplicaTracker ramDiskReplicaMap
			)
		{
			foreach (BlockPoolSlice s in bpSlices.Values)
			{
				s.GetVolumeMap(volumeMap, ramDiskReplicaMap);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void GetVolumeMap(string bpid, ReplicaMap volumeMap, RamDiskReplicaTracker
			 ramDiskReplicaMap)
		{
			GetBlockPoolSlice(bpid).GetVolumeMap(volumeMap, ramDiskReplicaMap);
		}

		public override string ToString()
		{
			return currentDir.GetAbsolutePath();
		}

		internal virtual void Shutdown()
		{
			if (cacheExecutor != null)
			{
				cacheExecutor.Shutdown();
			}
			ICollection<KeyValuePair<string, BlockPoolSlice>> set = bpSlices;
			foreach (KeyValuePair<string, BlockPoolSlice> entry in set)
			{
				entry.Value.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AddBlockPool(string bpid, Configuration conf)
		{
			FilePath bpdir = new FilePath(currentDir, bpid);
			BlockPoolSlice bp = new BlockPoolSlice(bpid, this, bpdir, conf);
			bpSlices[bpid] = bp;
		}

		internal virtual void ShutdownBlockPool(string bpid)
		{
			BlockPoolSlice bp = bpSlices[bpid];
			if (bp != null)
			{
				bp.Shutdown();
			}
			Sharpen.Collections.Remove(bpSlices, bpid);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool IsBPDirEmpty(string bpid)
		{
			FilePath volumeCurrentDir = this.GetCurrentDir();
			FilePath bpDir = new FilePath(volumeCurrentDir, bpid);
			FilePath bpCurrentDir = new FilePath(bpDir, DataStorage.StorageDirCurrent);
			FilePath finalizedDir = new FilePath(bpCurrentDir, DataStorage.StorageDirFinalized
				);
			FilePath rbwDir = new FilePath(bpCurrentDir, DataStorage.StorageDirRbw);
			if (finalizedDir.Exists() && !DatanodeUtil.DirNoFilesRecursive(finalizedDir))
			{
				return false;
			}
			if (rbwDir.Exists() && FileUtil.List(rbwDir).Length != 0)
			{
				return false;
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void DeleteBPDirectories(string bpid, bool force)
		{
			FilePath volumeCurrentDir = this.GetCurrentDir();
			FilePath bpDir = new FilePath(volumeCurrentDir, bpid);
			if (!bpDir.IsDirectory())
			{
				// nothing to be deleted
				return;
			}
			FilePath tmpDir = new FilePath(bpDir, DataStorage.StorageDirTmp);
			FilePath bpCurrentDir = new FilePath(bpDir, DataStorage.StorageDirCurrent);
			FilePath finalizedDir = new FilePath(bpCurrentDir, DataStorage.StorageDirFinalized
				);
			FilePath lazypersistDir = new FilePath(bpCurrentDir, DataStorage.StorageDirLazyPersist
				);
			FilePath rbwDir = new FilePath(bpCurrentDir, DataStorage.StorageDirRbw);
			if (force)
			{
				FileUtil.FullyDelete(bpDir);
			}
			else
			{
				if (!rbwDir.Delete())
				{
					throw new IOException("Failed to delete " + rbwDir);
				}
				if (!DatanodeUtil.DirNoFilesRecursive(finalizedDir) || !FileUtil.FullyDelete(finalizedDir
					))
				{
					throw new IOException("Failed to delete " + finalizedDir);
				}
				if (lazypersistDir.Exists() && ((!DatanodeUtil.DirNoFilesRecursive(lazypersistDir
					) || !FileUtil.FullyDelete(lazypersistDir))))
				{
					throw new IOException("Failed to delete " + lazypersistDir);
				}
				FileUtil.FullyDelete(tmpDir);
				foreach (FilePath f in FileUtil.ListFiles(bpCurrentDir))
				{
					if (!f.Delete())
					{
						throw new IOException("Failed to delete " + f);
					}
				}
				if (!bpCurrentDir.Delete())
				{
					throw new IOException("Failed to delete " + bpCurrentDir);
				}
				foreach (FilePath f_1 in FileUtil.ListFiles(bpDir))
				{
					if (!f_1.Delete())
					{
						throw new IOException("Failed to delete " + f_1);
					}
				}
				if (!bpDir.Delete())
				{
					throw new IOException("Failed to delete " + bpDir);
				}
			}
		}

		public override string GetStorageID()
		{
			return storageID;
		}

		public override StorageType GetStorageType()
		{
			return storageType;
		}

		internal virtual DatanodeStorage ToDatanodeStorage()
		{
			return new DatanodeStorage(storageID, DatanodeStorage.State.Normal, storageType);
		}
	}
}
