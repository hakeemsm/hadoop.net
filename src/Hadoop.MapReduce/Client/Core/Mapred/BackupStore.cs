using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task.Reduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// <code>BackupStore</code> is an utility class that is used to support
	/// the mark-reset functionality of values iterator
	/// <p>It has two caches - a memory cache and a file cache where values are
	/// stored as they are iterated, after a mark.
	/// </summary>
	/// <remarks>
	/// <code>BackupStore</code> is an utility class that is used to support
	/// the mark-reset functionality of values iterator
	/// <p>It has two caches - a memory cache and a file cache where values are
	/// stored as they are iterated, after a mark. On reset, values are retrieved
	/// from these caches. Framework moves from the memory cache to the
	/// file cache when the memory cache becomes full.
	/// </remarks>
	public class BackupStore<K, V>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.BackupStore
			).FullName);

		private const int MaxVintSize = 9;

		private const int EofMarkerSize = 2 * MaxVintSize;

		private readonly TaskAttemptID tid;

		private BackupStore.MemoryCache memCache;

		private BackupStore.FileCache fileCache;

		internal IList<Merger.Segment<K, V>> segmentList = new List<Merger.Segment<K, V>>
			();

		private int readSegmentIndex = 0;

		private int firstSegmentOffset = 0;

		private int currentKVOffset = 0;

		private int nextKVOffset = -1;

		private DataInputBuffer currentKey = null;

		private DataInputBuffer currentValue = new DataInputBuffer();

		private DataInputBuffer currentDiskValue = new DataInputBuffer();

		private bool hasMore = false;

		private bool inReset = false;

		private bool clearMarkFlag = false;

		private bool lastSegmentEOF = false;

		private Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		public BackupStore(Configuration conf, TaskAttemptID taskid)
		{
			float bufferPercent = conf.GetFloat(JobContext.ReduceMarkresetBufferPercent, 0f);
			if (bufferPercent > 1.0 || bufferPercent < 0.0)
			{
				throw new IOException(JobContext.ReduceMarkresetBufferPercent + bufferPercent);
			}
			int maxSize = (int)Math.Min(Runtime.GetRuntime().MaxMemory() * bufferPercent, int.MaxValue
				);
			// Support an absolute size also.
			int tmp = conf.GetInt(JobContext.ReduceMarkresetBufferSize, 0);
			if (tmp > 0)
			{
				maxSize = tmp;
			}
			memCache = new BackupStore.MemoryCache(this, maxSize);
			fileCache = new BackupStore.FileCache(this, conf);
			tid = taskid;
			this.conf = conf;
			Log.Info("Created a new BackupStore with a memory of " + maxSize);
		}

		/// <summary>Write the given K,V to the cache.</summary>
		/// <remarks>
		/// Write the given K,V to the cache.
		/// Write to memcache if space is available, else write to the filecache
		/// </remarks>
		/// <param name="key"/>
		/// <param name="value"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataInputBuffer key, DataInputBuffer value)
		{
			System.Diagnostics.Debug.Assert((key != null && value != null));
			if (fileCache.IsActive())
			{
				fileCache.Write(key, value);
				return;
			}
			if (memCache.ReserveSpace(key, value))
			{
				memCache.Write(key, value);
			}
			else
			{
				fileCache.Activate();
				fileCache.Write(key, value);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Mark()
		{
			// We read one KV pair in advance in hasNext. 
			// If hasNext has read the next KV pair from a new segment, but the
			// user has not called next() for that KV, then reset the readSegmentIndex
			// to the previous segment
			if (nextKVOffset == 0)
			{
				System.Diagnostics.Debug.Assert((readSegmentIndex != 0));
				System.Diagnostics.Debug.Assert((currentKVOffset != 0));
				readSegmentIndex--;
			}
			// just drop segments before the current active segment
			int i = 0;
			IEnumerator<Merger.Segment<K, V>> itr = segmentList.GetEnumerator();
			while (itr.HasNext())
			{
				Merger.Segment<K, V> s = itr.Next();
				if (i == readSegmentIndex)
				{
					break;
				}
				s.Close();
				itr.Remove();
				i++;
				Log.Debug("Dropping a segment");
			}
			// FirstSegmentOffset is the offset in the current segment from where we
			// need to start reading on the next reset
			firstSegmentOffset = currentKVOffset;
			readSegmentIndex = 0;
			Log.Debug("Setting the FirsSegmentOffset to " + currentKVOffset);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Reset()
		{
			// Create a new segment for the previously written records only if we
			// are not already in the reset mode
			if (!inReset)
			{
				if (fileCache.isActive)
				{
					fileCache.CreateInDiskSegment();
				}
				else
				{
					memCache.CreateInMemorySegment();
				}
			}
			inReset = true;
			// Reset the segments to the correct position from where the next read
			// should begin. 
			for (int i = 0; i < segmentList.Count; i++)
			{
				Merger.Segment<K, V> s = segmentList[i];
				if (s.InMemory())
				{
					int offset = (i == 0) ? firstSegmentOffset : 0;
					s.GetReader().Reset(offset);
				}
				else
				{
					s.CloseReader();
					if (i == 0)
					{
						s.ReinitReader(firstSegmentOffset);
						s.GetReader().DisableChecksumValidation();
					}
				}
			}
			currentKVOffset = firstSegmentOffset;
			nextKVOffset = -1;
			readSegmentIndex = 0;
			hasMore = false;
			lastSegmentEOF = false;
			Log.Debug("Reset - First segment offset is " + firstSegmentOffset + " Segment List Size is "
				 + segmentList.Count);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool HasNext()
		{
			if (lastSegmentEOF)
			{
				return false;
			}
			// We read the next KV from the cache to decide if there is any left.
			// Since hasNext can be called several times before the actual call to 
			// next(), we use hasMore to avoid extra reads. hasMore is set to false
			// when the user actually consumes this record in next()
			if (hasMore)
			{
				return true;
			}
			Merger.Segment<K, V> seg = segmentList[readSegmentIndex];
			// Mark the current position. This would be set to currentKVOffset
			// when the user consumes this record in next(). 
			nextKVOffset = (int)seg.GetActualPosition();
			if (seg.NextRawKey())
			{
				currentKey = seg.GetKey();
				seg.GetValue(currentValue);
				hasMore = true;
				return true;
			}
			else
			{
				if (!seg.InMemory())
				{
					seg.CloseReader();
				}
			}
			// If this is the last segment, mark the lastSegmentEOF flag and return
			if (readSegmentIndex == segmentList.Count - 1)
			{
				nextKVOffset = -1;
				lastSegmentEOF = true;
				return false;
			}
			nextKVOffset = 0;
			readSegmentIndex++;
			Merger.Segment<K, V> nextSegment = segmentList[readSegmentIndex];
			// We possibly are moving from a memory segment to a disk segment.
			// Reset so that we do not corrupt the in-memory segment buffer.
			// See HADOOP-5494
			if (!nextSegment.InMemory())
			{
				currentValue.Reset(currentDiskValue.GetData(), currentDiskValue.GetLength());
				nextSegment.Init(null);
			}
			if (nextSegment.NextRawKey())
			{
				currentKey = nextSegment.GetKey();
				nextSegment.GetValue(currentValue);
				hasMore = true;
				return true;
			}
			else
			{
				throw new IOException("New segment did not have even one K/V");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Next()
		{
			if (!HasNext())
			{
				throw new NoSuchElementException("iterate past last value");
			}
			// Reset hasMore. See comment in hasNext()
			hasMore = false;
			currentKVOffset = nextKVOffset;
			nextKVOffset = -1;
		}

		public virtual DataInputBuffer NextValue()
		{
			return currentValue;
		}

		public virtual DataInputBuffer NextKey()
		{
			return currentKey;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Reinitialize()
		{
			if (segmentList.Count != 0)
			{
				ClearSegmentList();
			}
			memCache.Reinitialize(true);
			fileCache.Reinitialize();
			readSegmentIndex = firstSegmentOffset = 0;
			currentKVOffset = 0;
			nextKVOffset = -1;
			hasMore = inReset = clearMarkFlag = false;
		}

		/// <summary>
		/// This function is called the ValuesIterator when a mark is called
		/// outside of a reset zone.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ExitResetMode()
		{
			inReset = false;
			if (clearMarkFlag)
			{
				// If a flag was set to clear mark, do the reinit now.
				// See clearMark()
				Reinitialize();
				return;
			}
			if (!fileCache.isActive)
			{
				memCache.Reinitialize(false);
			}
		}

		/// <summary>
		/// For writing the first key and value bytes directly from the
		/// value iterators, pass the current underlying output stream
		/// </summary>
		/// <param name="length">The length of the impending write</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual DataOutputStream GetOutputStream(int length)
		{
			if (memCache.ReserveSpace(length))
			{
				return memCache.dataOut;
			}
			else
			{
				fileCache.Activate();
				return fileCache.writer.GetOutputStream();
			}
		}

		/// <summary>
		/// This method is called by the valueIterators after writing the first
		/// key and value bytes to the BackupStore
		/// </summary>
		/// <param name="length"></param>
		public virtual void UpdateCounters(int length)
		{
			if (fileCache.isActive)
			{
				fileCache.writer.UpdateCountersForExternalAppend(length);
			}
			else
			{
				memCache.usedSize += length;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ClearMark()
		{
			if (inReset)
			{
				// If we are in the reset mode, we just mark a flag and come out
				// The actual re initialization would be done when we exit the reset
				// mode
				clearMarkFlag = true;
			}
			else
			{
				Reinitialize();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ClearSegmentList()
		{
			foreach (Merger.Segment<K, V> segment in segmentList)
			{
				long len = segment.GetLength();
				segment.Close();
				if (segment.InMemory())
				{
					memCache.Unreserve(len);
				}
			}
			segmentList.Clear();
		}

		internal class MemoryCache
		{
			private DataOutputBuffer dataOut;

			private int blockSize;

			private int usedSize;

			private readonly BackupStore.BackupRamManager ramManager;

			private int defaultBlockSize = 1024 * 1024;

			public MemoryCache(BackupStore<K, V> _enclosing, int maxSize)
			{
				this._enclosing = _enclosing;
				// Memory cache is made up of blocks.
				this.ramManager = new BackupStore.BackupRamManager(maxSize);
				if (maxSize < this.defaultBlockSize)
				{
					this.defaultBlockSize = maxSize;
				}
			}

			public virtual void Unreserve(long len)
			{
				this.ramManager.Unreserve((int)len);
			}

			/// <summary>Re-initialize the memory cache.</summary>
			/// <param name="clearAll">If true, re-initialize the ramManager also.</param>
			internal virtual void Reinitialize(bool clearAll)
			{
				if (clearAll)
				{
					this.ramManager.Reinitialize();
				}
				int allocatedSize = this.CreateNewMemoryBlock(this.defaultBlockSize, this.defaultBlockSize
					);
				System.Diagnostics.Debug.Assert((allocatedSize == this.defaultBlockSize || allocatedSize
					 == 0));
				BackupStore.Log.Debug("Created a new mem block of " + allocatedSize);
			}

			private int CreateNewMemoryBlock(int requestedSize, int minSize)
			{
				int allocatedSize = this.ramManager.Reserve(requestedSize, minSize);
				this.usedSize = 0;
				if (allocatedSize == 0)
				{
					this.dataOut = null;
					this.blockSize = 0;
				}
				else
				{
					this.dataOut = new DataOutputBuffer(allocatedSize);
					this.blockSize = allocatedSize;
				}
				return allocatedSize;
			}

			/// <summary>
			/// This method determines if there is enough space left in the
			/// memory cache to write to the requested length + space for
			/// subsequent EOF makers.
			/// </summary>
			/// <param name="length"/>
			/// <returns>true if enough space is available</returns>
			/// <exception cref="System.IO.IOException"/>
			internal virtual bool ReserveSpace(int length)
			{
				int availableSize = this.blockSize - this.usedSize;
				if (availableSize >= length + BackupStore.EofMarkerSize)
				{
					return true;
				}
				// Not enough available. Close this block 
				System.Diagnostics.Debug.Assert((!this._enclosing.inReset));
				this.CreateInMemorySegment();
				// Create a new block
				int tmp = Math.Max(length + BackupStore.EofMarkerSize, this.defaultBlockSize);
				availableSize = this.CreateNewMemoryBlock(tmp, (length + BackupStore.EofMarkerSize
					));
				return (availableSize == 0) ? false : true;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual bool ReserveSpace(DataInputBuffer key, DataInputBuffer value)
			{
				int keyLength = key.GetLength() - key.GetPosition();
				int valueLength = value.GetLength() - value.GetPosition();
				int requestedSize = keyLength + valueLength + WritableUtils.GetVIntSize(keyLength
					) + WritableUtils.GetVIntSize(valueLength);
				return this.ReserveSpace(requestedSize);
			}

			/// <summary>Write the key and value to the cache in the IFile format</summary>
			/// <param name="key"/>
			/// <param name="value"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataInputBuffer key, DataInputBuffer value)
			{
				int keyLength = key.GetLength() - key.GetPosition();
				int valueLength = value.GetLength() - value.GetPosition();
				WritableUtils.WriteVInt(this.dataOut, keyLength);
				WritableUtils.WriteVInt(this.dataOut, valueLength);
				this.dataOut.Write(key.GetData(), key.GetPosition(), keyLength);
				this.dataOut.Write(value.GetData(), value.GetPosition(), valueLength);
				this.usedSize += keyLength + valueLength + WritableUtils.GetVIntSize(keyLength) +
					 WritableUtils.GetVIntSize(valueLength);
				BackupStore.Log.Debug("ID: " + this._enclosing.segmentList.Count + " WRITE TO MEM"
					);
			}

			/// <summary>This method creates a memory segment from the existing buffer</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void CreateInMemorySegment()
			{
				// If nothing was written in this block because the record size
				// was greater than the allocated block size, just return.
				if (this.usedSize == 0)
				{
					this.ramManager.Unreserve(this.blockSize);
					return;
				}
				// spaceAvailable would have ensured that there is enough space
				// left for the EOF markers.
				System.Diagnostics.Debug.Assert(((this.blockSize - this.usedSize) >= BackupStore.
					EofMarkerSize));
				WritableUtils.WriteVInt(this.dataOut, IFile.EofMarker);
				WritableUtils.WriteVInt(this.dataOut, IFile.EofMarker);
				this.usedSize += BackupStore.EofMarkerSize;
				this.ramManager.Unreserve(this.blockSize - this.usedSize);
				IFile.Reader<K, V> reader = new InMemoryReader<K, V>(null, (TaskAttemptID)this._enclosing
					.tid, this.dataOut.GetData(), 0, this.usedSize, this._enclosing.conf);
				Merger.Segment<K, V> segment = new Merger.Segment<K, V>(reader, false);
				this._enclosing.segmentList.AddItem(segment);
				BackupStore.Log.Debug("Added Memory Segment to List. List Size is " + this._enclosing
					.segmentList.Count);
			}

			private readonly BackupStore<K, V> _enclosing;
		}

		internal class FileCache
		{
			private LocalDirAllocator lDirAlloc;

			private readonly Configuration conf;

			private readonly FileSystem fs;

			private bool isActive = false;

			private Path file = null;

			private IFile.Writer<K, V> writer = null;

			private int spillNumber = 0;

			/// <exception cref="System.IO.IOException"/>
			public FileCache(BackupStore<K, V> _enclosing, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.conf = conf;
				this.fs = FileSystem.GetLocal(conf);
				this.lDirAlloc = new LocalDirAllocator(MRConfig.LocalDir);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Write(DataInputBuffer key, DataInputBuffer value)
			{
				if (this.writer == null)
				{
					// If spillNumber is 0, we should have called activate and not
					// come here at all
					System.Diagnostics.Debug.Assert((this.spillNumber != 0));
					this.writer = this.CreateSpillFile();
				}
				this.writer.Append(key, value);
				BackupStore.Log.Debug("ID: " + this._enclosing.segmentList.Count + " WRITE TO DISK"
					);
			}

			internal virtual void Reinitialize()
			{
				this.spillNumber = 0;
				this.writer = null;
				this.isActive = false;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Activate()
			{
				this.isActive = true;
				this.writer = this.CreateSpillFile();
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void CreateInDiskSegment()
			{
				System.Diagnostics.Debug.Assert((this.writer != null));
				this.writer.Close();
				Merger.Segment<K, V> s = new Merger.Segment<K, V>(this.conf, this.fs, this.file, 
					null, true);
				this.writer = null;
				this._enclosing.segmentList.AddItem(s);
				BackupStore.Log.Debug("Disk Segment added to List. Size is " + this._enclosing.segmentList
					.Count);
			}

			internal virtual bool IsActive()
			{
				return this.isActive;
			}

			/// <exception cref="System.IO.IOException"/>
			private IFile.Writer<K, V> CreateSpillFile()
			{
				Path tmp = new Path(MRJobConfig.Output + "/backup_" + this._enclosing.tid.GetId()
					 + "_" + (this.spillNumber++) + ".out");
				BackupStore.Log.Info("Created file: " + tmp);
				this.file = this.lDirAlloc.GetLocalPathForWrite(tmp.ToUri().GetPath(), -1, this.conf
					);
				FSDataOutputStream @out = this.fs.Create(this.file);
				@out = CryptoUtils.WrapIfNecessary(this.conf, @out);
				return new IFile.Writer<K, V>(this.conf, @out, null, null, null, null, true);
			}

			private readonly BackupStore<K, V> _enclosing;
		}

		internal class BackupRamManager : RamManager
		{
			private int availableSize = 0;

			private readonly int maxSize;

			public BackupRamManager(int size)
			{
				availableSize = maxSize = size;
			}

			public virtual bool Reserve(int requestedSize, InputStream @in)
			{
				// Not used
				Log.Warn("Reserve(int, InputStream) not supported by BackupRamManager");
				return false;
			}

			internal virtual int Reserve(int requestedSize)
			{
				if (availableSize == 0)
				{
					return 0;
				}
				int reservedSize = Math.Min(requestedSize, availableSize);
				availableSize -= reservedSize;
				Log.Debug("Reserving: " + reservedSize + " Requested: " + requestedSize);
				return reservedSize;
			}

			internal virtual int Reserve(int requestedSize, int minSize)
			{
				if (availableSize < minSize)
				{
					Log.Debug("No space available. Available: " + availableSize + " MinSize: " + minSize
						);
					return 0;
				}
				else
				{
					return Reserve(requestedSize);
				}
			}

			public virtual void Unreserve(int requestedSize)
			{
				availableSize += requestedSize;
				Log.Debug("Unreserving: " + requestedSize + ". Available: " + availableSize);
			}

			internal virtual void Reinitialize()
			{
				availableSize = maxSize;
			}
		}
	}
}
