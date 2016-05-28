using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public class MergeManagerImpl<K, V> : MergeManager<K, V>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Task.Reduce.MergeManagerImpl
			));

		private const float DefaultShuffleMemoryLimitPercent = 0.25f;

		private readonly TaskAttemptID reduceId;

		private readonly JobConf jobConf;

		private readonly FileSystem localFS;

		private readonly FileSystem rfs;

		private readonly LocalDirAllocator localDirAllocator;

		protected internal MapOutputFile mapOutputFile;

		internal ICollection<InMemoryMapOutput<K, V>> inMemoryMergedMapOutputs = new TreeSet
			<InMemoryMapOutput<K, V>>(new MapOutput.MapOutputComparator<K, V>());

		private MergeManagerImpl.IntermediateMemoryToMemoryMerger memToMemMerger;

		internal ICollection<InMemoryMapOutput<K, V>> inMemoryMapOutputs = new TreeSet<InMemoryMapOutput
			<K, V>>(new MapOutput.MapOutputComparator<K, V>());

		private readonly MergeThread<InMemoryMapOutput<K, V>, K, V> inMemoryMerger;

		internal ICollection<MergeManagerImpl.CompressAwarePath> onDiskMapOutputs = new TreeSet
			<MergeManagerImpl.CompressAwarePath>();

		private readonly MergeManagerImpl.OnDiskMerger onDiskMerger;

		[VisibleForTesting]
		internal readonly long memoryLimit;

		private long usedMemory;

		private long commitMemory;

		private readonly long maxSingleShuffleLimit;

		private readonly int memToMemMergeOutputsThreshold;

		private readonly long mergeThreshold;

		private readonly int ioSortFactor;

		private readonly Reporter reporter;

		private readonly ExceptionReporter exceptionReporter;

		/// <summary>Combiner class to run during in-memory merge, if defined.</summary>
		private readonly Type combinerClass;

		/// <summary>Resettable collector used for combine.</summary>
		private readonly Task.CombineOutputCollector<K, V> combineCollector;

		private readonly Counters.Counter spilledRecordsCounter;

		private readonly Counters.Counter reduceCombineInputCounter;

		private readonly Counters.Counter mergedMapOutputsCounter;

		private readonly CompressionCodec codec;

		private readonly Progress mergePhase;

		public MergeManagerImpl(TaskAttemptID reduceId, JobConf jobConf, FileSystem localFS
			, LocalDirAllocator localDirAllocator, Reporter reporter, CompressionCodec codec
			, Type combinerClass, Task.CombineOutputCollector<K, V> combineCollector, Counters.Counter
			 spilledRecordsCounter, Counters.Counter reduceCombineInputCounter, Counters.Counter
			 mergedMapOutputsCounter, ExceptionReporter exceptionReporter, Progress mergePhase
			, MapOutputFile mapOutputFile)
		{
			/* Maximum percentage of the in-memory limit that a single shuffle can
			* consume*/
			this.reduceId = reduceId;
			this.jobConf = jobConf;
			this.localDirAllocator = localDirAllocator;
			this.exceptionReporter = exceptionReporter;
			this.reporter = reporter;
			this.codec = codec;
			this.combinerClass = combinerClass;
			this.combineCollector = combineCollector;
			this.reduceCombineInputCounter = reduceCombineInputCounter;
			this.spilledRecordsCounter = spilledRecordsCounter;
			this.mergedMapOutputsCounter = mergedMapOutputsCounter;
			this.mapOutputFile = mapOutputFile;
			this.mapOutputFile.SetConf(jobConf);
			this.localFS = localFS;
			this.rfs = ((LocalFileSystem)localFS).GetRaw();
			float maxInMemCopyUse = jobConf.GetFloat(MRJobConfig.ShuffleInputBufferPercent, MRJobConfig
				.DefaultShuffleInputBufferPercent);
			if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0)
			{
				throw new ArgumentException("Invalid value for " + MRJobConfig.ShuffleInputBufferPercent
					 + ": " + maxInMemCopyUse);
			}
			// Allow unit tests to fix Runtime memory
			this.memoryLimit = (long)(jobConf.GetLong(MRJobConfig.ReduceMemoryTotalBytes, Runtime
				.GetRuntime().MaxMemory()) * maxInMemCopyUse);
			this.ioSortFactor = jobConf.GetInt(MRJobConfig.IoSortFactor, 100);
			float singleShuffleMemoryLimitPercent = jobConf.GetFloat(MRJobConfig.ShuffleMemoryLimitPercent
				, DefaultShuffleMemoryLimitPercent);
			if (singleShuffleMemoryLimitPercent <= 0.0f || singleShuffleMemoryLimitPercent > 
				1.0f)
			{
				throw new ArgumentException("Invalid value for " + MRJobConfig.ShuffleMemoryLimitPercent
					 + ": " + singleShuffleMemoryLimitPercent);
			}
			usedMemory = 0L;
			commitMemory = 0L;
			this.maxSingleShuffleLimit = (long)(memoryLimit * singleShuffleMemoryLimitPercent
				);
			this.memToMemMergeOutputsThreshold = jobConf.GetInt(MRJobConfig.ReduceMemtomemThreshold
				, ioSortFactor);
			this.mergeThreshold = (long)(this.memoryLimit * jobConf.GetFloat(MRJobConfig.ShuffleMergePercent
				, 0.90f));
			Log.Info("MergerManager: memoryLimit=" + memoryLimit + ", " + "maxSingleShuffleLimit="
				 + maxSingleShuffleLimit + ", " + "mergeThreshold=" + mergeThreshold + ", " + "ioSortFactor="
				 + ioSortFactor + ", " + "memToMemMergeOutputsThreshold=" + memToMemMergeOutputsThreshold
				);
			if (this.maxSingleShuffleLimit >= this.mergeThreshold)
			{
				throw new RuntimeException("Invalid configuration: " + "maxSingleShuffleLimit should be less than mergeThreshold "
					 + "maxSingleShuffleLimit: " + this.maxSingleShuffleLimit + "mergeThreshold: " +
					 this.mergeThreshold);
			}
			bool allowMemToMemMerge = jobConf.GetBoolean(MRJobConfig.ReduceMemtomemEnabled, false
				);
			if (allowMemToMemMerge)
			{
				this.memToMemMerger = new MergeManagerImpl.IntermediateMemoryToMemoryMerger(this, 
					this, memToMemMergeOutputsThreshold);
				this.memToMemMerger.Start();
			}
			else
			{
				this.memToMemMerger = null;
			}
			this.inMemoryMerger = CreateInMemoryMerger();
			this.inMemoryMerger.Start();
			this.onDiskMerger = new MergeManagerImpl.OnDiskMerger(this, this);
			this.onDiskMerger.Start();
			this.mergePhase = mergePhase;
		}

		protected internal virtual MergeThread<InMemoryMapOutput<K, V>, K, V> CreateInMemoryMerger
			()
		{
			return new MergeManagerImpl.InMemoryMerger(this, this);
		}

		protected internal virtual MergeThread<MergeManagerImpl.CompressAwarePath, K, V> 
			CreateOnDiskMerger()
		{
			return new MergeManagerImpl.OnDiskMerger(this, this);
		}

		internal virtual TaskAttemptID GetReduceId()
		{
			return reduceId;
		}

		[VisibleForTesting]
		internal virtual ExceptionReporter GetExceptionReporter()
		{
			return exceptionReporter;
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForResource()
		{
			inMemoryMerger.WaitForMerge();
		}

		private bool CanShuffleToMemory(long requestedSize)
		{
			return (requestedSize < maxSingleShuffleLimit);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual MapOutput<K, V> Reserve(TaskAttemptID mapId, long requestedSize, int
			 fetcher)
		{
			lock (this)
			{
				if (!CanShuffleToMemory(requestedSize))
				{
					Log.Info(mapId + ": Shuffling to disk since " + requestedSize + " is greater than maxSingleShuffleLimit ("
						 + maxSingleShuffleLimit + ")");
					return new OnDiskMapOutput<K, V>(mapId, reduceId, this, requestedSize, jobConf, mapOutputFile
						, fetcher, true);
				}
				// Stall shuffle if we are above the memory limit
				// It is possible that all threads could just be stalling and not make
				// progress at all. This could happen when:
				//
				// requested size is causing the used memory to go above limit &&
				// requested size < singleShuffleLimit &&
				// current used size < mergeThreshold (merge will not get triggered)
				//
				// To avoid this from happening, we allow exactly one thread to go past
				// the memory limit. We check (usedMemory > memoryLimit) and not
				// (usedMemory + requestedSize > memoryLimit). When this thread is done
				// fetching, this will automatically trigger a merge thereby unlocking
				// all the stalled threads
				if (usedMemory > memoryLimit)
				{
					Log.Debug(mapId + ": Stalling shuffle since usedMemory (" + usedMemory + ") is greater than memoryLimit ("
						 + memoryLimit + ")." + " CommitMemory is (" + commitMemory + ")");
					return null;
				}
				// Allow the in-memory shuffle to progress
				Log.Debug(mapId + ": Proceeding with shuffle since usedMemory (" + usedMemory + ") is lesser than memoryLimit ("
					 + memoryLimit + ")." + "CommitMemory is (" + commitMemory + ")");
				return UnconditionalReserve(mapId, requestedSize, true);
			}
		}

		/// <summary>Unconditional Reserve is used by the Memory-to-Memory thread</summary>
		/// <returns/>
		private InMemoryMapOutput<K, V> UnconditionalReserve(TaskAttemptID mapId, long requestedSize
			, bool primaryMapOutput)
		{
			lock (this)
			{
				usedMemory += requestedSize;
				return new InMemoryMapOutput<K, V>(jobConf, mapId, this, (int)requestedSize, codec
					, primaryMapOutput);
			}
		}

		internal virtual void Unreserve(long size)
		{
			lock (this)
			{
				usedMemory -= size;
			}
		}

		public virtual void CloseInMemoryFile(InMemoryMapOutput<K, V> mapOutput)
		{
			lock (this)
			{
				inMemoryMapOutputs.AddItem(mapOutput);
				Log.Info("closeInMemoryFile -> map-output of size: " + mapOutput.GetSize() + ", inMemoryMapOutputs.size() -> "
					 + inMemoryMapOutputs.Count + ", commitMemory -> " + commitMemory + ", usedMemory ->"
					 + usedMemory);
				commitMemory += mapOutput.GetSize();
				// Can hang if mergeThreshold is really low.
				if (commitMemory >= mergeThreshold)
				{
					Log.Info("Starting inMemoryMerger's merge since commitMemory=" + commitMemory + " > mergeThreshold="
						 + mergeThreshold + ". Current usedMemory=" + usedMemory);
					Sharpen.Collections.AddAll(inMemoryMapOutputs, inMemoryMergedMapOutputs);
					inMemoryMergedMapOutputs.Clear();
					inMemoryMerger.StartMerge(inMemoryMapOutputs);
					commitMemory = 0L;
				}
				// Reset commitMemory.
				if (memToMemMerger != null)
				{
					if (inMemoryMapOutputs.Count >= memToMemMergeOutputsThreshold)
					{
						memToMemMerger.StartMerge(inMemoryMapOutputs);
					}
				}
			}
		}

		public virtual void CloseInMemoryMergedFile(InMemoryMapOutput<K, V> mapOutput)
		{
			lock (this)
			{
				inMemoryMergedMapOutputs.AddItem(mapOutput);
				Log.Info("closeInMemoryMergedFile -> size: " + mapOutput.GetSize() + ", inMemoryMergedMapOutputs.size() -> "
					 + inMemoryMergedMapOutputs.Count);
			}
		}

		public virtual void CloseOnDiskFile(MergeManagerImpl.CompressAwarePath file)
		{
			lock (this)
			{
				onDiskMapOutputs.AddItem(file);
				if (onDiskMapOutputs.Count >= (2 * ioSortFactor - 1))
				{
					onDiskMerger.StartMerge(onDiskMapOutputs);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual RawKeyValueIterator Close()
		{
			// Wait for on-going merges to complete
			if (memToMemMerger != null)
			{
				memToMemMerger.Close();
			}
			inMemoryMerger.Close();
			onDiskMerger.Close();
			IList<InMemoryMapOutput<K, V>> memory = new AList<InMemoryMapOutput<K, V>>(inMemoryMergedMapOutputs
				);
			inMemoryMergedMapOutputs.Clear();
			Sharpen.Collections.AddAll(memory, inMemoryMapOutputs);
			inMemoryMapOutputs.Clear();
			IList<MergeManagerImpl.CompressAwarePath> disk = new AList<MergeManagerImpl.CompressAwarePath
				>(onDiskMapOutputs);
			onDiskMapOutputs.Clear();
			return FinalMerge(jobConf, rfs, memory, disk);
		}

		private class IntermediateMemoryToMemoryMerger : MergeThread<InMemoryMapOutput<K, 
			V>, K, V>
		{
			public IntermediateMemoryToMemoryMerger(MergeManagerImpl<K, V> _enclosing, MergeManagerImpl
				<K, V> manager, int mergeFactor)
				: base(manager, mergeFactor, this._enclosing.exceptionReporter)
			{
				this._enclosing = _enclosing;
				this.SetName("InMemoryMerger - Thread to do in-memory merge of in-memory " + "shuffled map-outputs"
					);
				this.SetDaemon(true);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Merge(IList<InMemoryMapOutput<K, V>> inputs)
			{
				if (inputs == null || inputs.Count == 0)
				{
					return;
				}
				TaskAttemptID dummyMapId = inputs[0].GetMapId();
				IList<Merger.Segment<K, V>> inMemorySegments = new AList<Merger.Segment<K, V>>();
				long mergeOutputSize = this._enclosing.CreateInMemorySegments(inputs, inMemorySegments
					, 0);
				int noInMemorySegments = inMemorySegments.Count;
				InMemoryMapOutput<K, V> mergedMapOutputs = this._enclosing.UnconditionalReserve(dummyMapId
					, mergeOutputSize, false);
				IFile.Writer<K, V> writer = new InMemoryWriter<K, V>(mergedMapOutputs.GetArrayStream
					());
				MergeManagerImpl.Log.Info("Initiating Memory-to-Memory merge with " + noInMemorySegments
					 + " segments of total-size: " + mergeOutputSize);
				RawKeyValueIterator rIter = Merger.Merge(this._enclosing.jobConf, this._enclosing
					.rfs, (Type)this._enclosing.jobConf.GetMapOutputKeyClass(), (Type)this._enclosing
					.jobConf.GetMapOutputValueClass(), inMemorySegments, inMemorySegments.Count, new 
					Path(this._enclosing.reduceId.ToString()), (RawComparator<K>)this._enclosing.jobConf
					.GetOutputKeyComparator(), this._enclosing.reporter, null, null, null);
				Merger.WriteFile(rIter, writer, this._enclosing.reporter, this._enclosing.jobConf
					);
				writer.Close();
				MergeManagerImpl.Log.Info(this._enclosing.reduceId + " Memory-to-Memory merge of the "
					 + noInMemorySegments + " files in-memory complete.");
				// Note the output of the merge
				this._enclosing.CloseInMemoryMergedFile(mergedMapOutputs);
			}

			private readonly MergeManagerImpl<K, V> _enclosing;
		}

		private class InMemoryMerger : MergeThread<InMemoryMapOutput<K, V>, K, V>
		{
			public InMemoryMerger(MergeManagerImpl<K, V> _enclosing, MergeManagerImpl<K, V> manager
				)
				: base(manager, int.MaxValue, this._enclosing.exceptionReporter)
			{
				this._enclosing = _enclosing;
				this.SetName("InMemoryMerger - Thread to merge in-memory shuffled map-outputs");
				this.SetDaemon(true);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Merge(IList<InMemoryMapOutput<K, V>> inputs)
			{
				if (inputs == null || inputs.Count == 0)
				{
					return;
				}
				//name this output file same as the name of the first file that is 
				//there in the current list of inmem files (this is guaranteed to
				//be absent on the disk currently. So we don't overwrite a prev. 
				//created spill). Also we need to create the output file now since
				//it is not guaranteed that this file will be present after merge
				//is called (we delete empty files as soon as we see them
				//in the merge method)
				//figure out the mapId 
				TaskAttemptID mapId = inputs[0].GetMapId();
				TaskID mapTaskId = mapId.GetTaskID();
				IList<Merger.Segment<K, V>> inMemorySegments = new AList<Merger.Segment<K, V>>();
				long mergeOutputSize = this._enclosing.CreateInMemorySegments(inputs, inMemorySegments
					, 0);
				int noInMemorySegments = inMemorySegments.Count;
				Path outputPath = this._enclosing.mapOutputFile.GetInputFileForWrite(mapTaskId, mergeOutputSize
					).Suffix(Org.Apache.Hadoop.Mapred.Task.MergedOutputPrefix);
				FSDataOutputStream @out = CryptoUtils.WrapIfNecessary(this._enclosing.jobConf, this
					._enclosing.rfs.Create(outputPath));
				IFile.Writer<K, V> writer = new IFile.Writer<K, V>(this._enclosing.jobConf, @out, 
					(Type)this._enclosing.jobConf.GetMapOutputKeyClass(), (Type)this._enclosing.jobConf
					.GetMapOutputValueClass(), this._enclosing.codec, null, true);
				RawKeyValueIterator rIter = null;
				MergeManagerImpl.CompressAwarePath compressAwarePath;
				try
				{
					MergeManagerImpl.Log.Info("Initiating in-memory merge with " + noInMemorySegments
						 + " segments...");
					rIter = Merger.Merge(this._enclosing.jobConf, this._enclosing.rfs, (Type)this._enclosing
						.jobConf.GetMapOutputKeyClass(), (Type)this._enclosing.jobConf.GetMapOutputValueClass
						(), inMemorySegments, inMemorySegments.Count, new Path(this._enclosing.reduceId.
						ToString()), (RawComparator<K>)this._enclosing.jobConf.GetOutputKeyComparator(), 
						this._enclosing.reporter, this._enclosing.spilledRecordsCounter, null, null);
					if (null == this._enclosing.combinerClass)
					{
						Merger.WriteFile(rIter, writer, this._enclosing.reporter, this._enclosing.jobConf
							);
					}
					else
					{
						this._enclosing.combineCollector.SetWriter(writer);
						this._enclosing.CombineAndSpill(rIter, this._enclosing.reduceCombineInputCounter);
					}
					writer.Close();
					compressAwarePath = new MergeManagerImpl.CompressAwarePath(outputPath, writer.GetRawLength
						(), writer.GetCompressedLength());
					MergeManagerImpl.Log.Info(this._enclosing.reduceId + " Merge of the " + noInMemorySegments
						 + " files in-memory complete." + " Local file is " + outputPath + " of size " +
						 this._enclosing.localFS.GetFileStatus(outputPath).GetLen());
				}
				catch (IOException e)
				{
					//make sure that we delete the ondisk file that we created 
					//earlier when we invoked cloneFileAttributes
					this._enclosing.localFS.Delete(outputPath, true);
					throw;
				}
				// Note the output of the merge
				this._enclosing.CloseOnDiskFile(compressAwarePath);
			}

			private readonly MergeManagerImpl<K, V> _enclosing;
		}

		private class OnDiskMerger : MergeThread<MergeManagerImpl.CompressAwarePath, K, V
			>
		{
			public OnDiskMerger(MergeManagerImpl<K, V> _enclosing, MergeManagerImpl<K, V> manager
				)
				: base(manager, this._enclosing.ioSortFactor, this._enclosing.exceptionReporter)
			{
				this._enclosing = _enclosing;
				this.SetName("OnDiskMerger - Thread to merge on-disk map-outputs");
				this.SetDaemon(true);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Merge(IList<MergeManagerImpl.CompressAwarePath> inputs)
			{
				// sanity check
				if (inputs == null || inputs.IsEmpty())
				{
					MergeManagerImpl.Log.Info("No ondisk files to merge...");
					return;
				}
				long approxOutputSize = 0;
				int bytesPerSum = this._enclosing.jobConf.GetInt("io.bytes.per.checksum", 512);
				MergeManagerImpl.Log.Info("OnDiskMerger: We have  " + inputs.Count + " map outputs on disk. Triggering merge..."
					);
				// 1. Prepare the list of files to be merged. 
				foreach (MergeManagerImpl.CompressAwarePath file in inputs)
				{
					approxOutputSize += this._enclosing.localFS.GetFileStatus(file).GetLen();
				}
				// add the checksum length
				approxOutputSize += ChecksumFileSystem.GetChecksumLength(approxOutputSize, bytesPerSum
					);
				// 2. Start the on-disk merge process
				Path outputPath = this._enclosing.localDirAllocator.GetLocalPathForWrite(inputs[0
					].ToString(), approxOutputSize, this._enclosing.jobConf).Suffix(Org.Apache.Hadoop.Mapred.Task
					.MergedOutputPrefix);
				FSDataOutputStream @out = CryptoUtils.WrapIfNecessary(this._enclosing.jobConf, this
					._enclosing.rfs.Create(outputPath));
				IFile.Writer<K, V> writer = new IFile.Writer<K, V>(this._enclosing.jobConf, @out, 
					(Type)this._enclosing.jobConf.GetMapOutputKeyClass(), (Type)this._enclosing.jobConf
					.GetMapOutputValueClass(), this._enclosing.codec, null, true);
				RawKeyValueIterator iter = null;
				MergeManagerImpl.CompressAwarePath compressAwarePath;
				Path tmpDir = new Path(this._enclosing.reduceId.ToString());
				try
				{
					iter = Merger.Merge(this._enclosing.jobConf, this._enclosing.rfs, (Type)this._enclosing
						.jobConf.GetMapOutputKeyClass(), (Type)this._enclosing.jobConf.GetMapOutputValueClass
						(), this._enclosing.codec, Sharpen.Collections.ToArray(inputs, new Path[inputs.Count
						]), true, this._enclosing.ioSortFactor, tmpDir, (RawComparator<K>)this._enclosing
						.jobConf.GetOutputKeyComparator(), this._enclosing.reporter, this._enclosing.spilledRecordsCounter
						, null, this._enclosing.mergedMapOutputsCounter, null);
					Merger.WriteFile(iter, writer, this._enclosing.reporter, this._enclosing.jobConf);
					writer.Close();
					compressAwarePath = new MergeManagerImpl.CompressAwarePath(outputPath, writer.GetRawLength
						(), writer.GetCompressedLength());
				}
				catch (IOException e)
				{
					this._enclosing.localFS.Delete(outputPath, true);
					throw;
				}
				this._enclosing.CloseOnDiskFile(compressAwarePath);
				MergeManagerImpl.Log.Info(this._enclosing.reduceId + " Finished merging " + inputs
					.Count + " map output files on disk of total-size " + approxOutputSize + "." + " Local output file is "
					 + outputPath + " of size " + this._enclosing.localFS.GetFileStatus(outputPath).
					GetLen());
			}

			private readonly MergeManagerImpl<K, V> _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CombineAndSpill(RawKeyValueIterator kvIter, Counters.Counter inCounter
			)
		{
			JobConf job = jobConf;
			Reducer combiner = ReflectionUtils.NewInstance(combinerClass, job);
			Type keyClass = (Type)job.GetMapOutputKeyClass();
			Type valClass = (Type)job.GetMapOutputValueClass();
			RawComparator<K> comparator = (RawComparator<K>)job.GetCombinerKeyGroupingComparator
				();
			try
			{
				Task.CombineValuesIterator values = new Task.CombineValuesIterator(kvIter, comparator
					, keyClass, valClass, job, Reporter.Null, inCounter);
				while (values.More())
				{
					combiner.Reduce(values.GetKey(), values, combineCollector, Reporter.Null);
					values.NextKey();
				}
			}
			finally
			{
				combiner.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private long CreateInMemorySegments(IList<InMemoryMapOutput<K, V>> inMemoryMapOutputs
			, IList<Merger.Segment<K, V>> inMemorySegments, long leaveBytes)
		{
			long totalSize = 0L;
			// We could use fullSize could come from the RamManager, but files can be
			// closed but not yet present in inMemoryMapOutputs
			long fullSize = 0L;
			foreach (InMemoryMapOutput<K, V> mo in inMemoryMapOutputs)
			{
				fullSize += mo.GetMemory().Length;
			}
			while (fullSize > leaveBytes)
			{
				InMemoryMapOutput<K, V> mo_1 = inMemoryMapOutputs.Remove(0);
				byte[] data = mo_1.GetMemory();
				long size = data.Length;
				totalSize += size;
				fullSize -= size;
				IFile.Reader<K, V> reader = new InMemoryReader<K, V>(this, mo_1.GetMapId(), data, 
					0, (int)size, jobConf);
				inMemorySegments.AddItem(new Merger.Segment<K, V>(reader, true, (mo_1.IsPrimaryMapOutput
					() ? mergedMapOutputsCounter : null)));
			}
			return totalSize;
		}

		internal class RawKVIteratorReader : IFile.Reader<K, V>
		{
			private readonly RawKeyValueIterator kvIter;

			/// <exception cref="System.IO.IOException"/>
			public RawKVIteratorReader(MergeManagerImpl<K, V> _enclosing, RawKeyValueIterator
				 kvIter, long size)
				: base(null, null, size, null, this._enclosing.spilledRecordsCounter)
			{
				this._enclosing = _enclosing;
				this.kvIter = kvIter;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool NextRawKey(DataInputBuffer key)
			{
				if (this.kvIter.Next())
				{
					DataInputBuffer kb = this.kvIter.GetKey();
					int kp = kb.GetPosition();
					int klen = kb.GetLength() - kp;
					key.Reset(kb.GetData(), kp, klen);
					this.bytesRead += klen;
					return true;
				}
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void NextRawValue(DataInputBuffer value)
			{
				DataInputBuffer vb = this.kvIter.GetValue();
				int vp = vb.GetPosition();
				int vlen = vb.GetLength() - vp;
				value.Reset(vb.GetData(), vp, vlen);
				this.bytesRead += vlen;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long GetPosition()
			{
				return this.bytesRead;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this.kvIter.Close();
			}

			private readonly MergeManagerImpl<K, V> _enclosing;
		}

		[VisibleForTesting]
		internal long GetMaxInMemReduceLimit()
		{
			float maxRedPer = jobConf.GetFloat(MRJobConfig.ReduceInputBufferPercent, 0f);
			if (maxRedPer > 1.0 || maxRedPer < 0.0)
			{
				throw new RuntimeException(maxRedPer + ": " + MRJobConfig.ReduceInputBufferPercent
					 + " must be a float between 0 and 1.0");
			}
			return (long)(memoryLimit * maxRedPer);
		}

		/// <exception cref="System.IO.IOException"/>
		private RawKeyValueIterator FinalMerge(JobConf job, FileSystem fs, IList<InMemoryMapOutput
			<K, V>> inMemoryMapOutputs, IList<MergeManagerImpl.CompressAwarePath> onDiskMapOutputs
			)
		{
			Log.Info("finalMerge called with " + inMemoryMapOutputs.Count + " in-memory map-outputs and "
				 + onDiskMapOutputs.Count + " on-disk map-outputs");
			long maxInMemReduce = GetMaxInMemReduceLimit();
			// merge config params
			Type keyClass = (Type)job.GetMapOutputKeyClass();
			Type valueClass = (Type)job.GetMapOutputValueClass();
			bool keepInputs = job.GetKeepFailedTaskFiles();
			Path tmpDir = new Path(reduceId.ToString());
			RawComparator<K> comparator = (RawComparator<K>)job.GetOutputKeyComparator();
			// segments required to vacate memory
			IList<Merger.Segment<K, V>> memDiskSegments = new AList<Merger.Segment<K, V>>();
			long inMemToDiskBytes = 0;
			bool mergePhaseFinished = false;
			if (inMemoryMapOutputs.Count > 0)
			{
				TaskID mapId = inMemoryMapOutputs[0].GetMapId().GetTaskID();
				inMemToDiskBytes = CreateInMemorySegments(inMemoryMapOutputs, memDiskSegments, maxInMemReduce
					);
				int numMemDiskSegments = memDiskSegments.Count;
				if (numMemDiskSegments > 0 && ioSortFactor > onDiskMapOutputs.Count)
				{
					// If we reach here, it implies that we have less than io.sort.factor
					// disk segments and this will be incremented by 1 (result of the 
					// memory segments merge). Since this total would still be 
					// <= io.sort.factor, we will not do any more intermediate merges,
					// the merge of all these disk segments would be directly fed to the
					// reduce method
					mergePhaseFinished = true;
					// must spill to disk, but can't retain in-mem for intermediate merge
					Path outputPath = mapOutputFile.GetInputFileForWrite(mapId, inMemToDiskBytes).Suffix
						(Org.Apache.Hadoop.Mapred.Task.MergedOutputPrefix);
					RawKeyValueIterator rIter = Merger.Merge(job, fs, keyClass, valueClass, memDiskSegments
						, numMemDiskSegments, tmpDir, comparator, reporter, spilledRecordsCounter, null, 
						mergePhase);
					FSDataOutputStream @out = CryptoUtils.WrapIfNecessary(job, fs.Create(outputPath));
					IFile.Writer<K, V> writer = new IFile.Writer<K, V>(job, @out, keyClass, valueClass
						, codec, null, true);
					try
					{
						Merger.WriteFile(rIter, writer, reporter, job);
						writer.Close();
						onDiskMapOutputs.AddItem(new MergeManagerImpl.CompressAwarePath(outputPath, writer
							.GetRawLength(), writer.GetCompressedLength()));
						writer = null;
					}
					catch (IOException e)
					{
						// add to list of final disk outputs.
						if (null != outputPath)
						{
							try
							{
								fs.Delete(outputPath, true);
							}
							catch (IOException)
							{
							}
						}
						// NOTHING
						throw;
					}
					finally
					{
						if (null != writer)
						{
							writer.Close();
						}
					}
					Log.Info("Merged " + numMemDiskSegments + " segments, " + inMemToDiskBytes + " bytes to disk to satisfy "
						 + "reduce memory limit");
					inMemToDiskBytes = 0;
					memDiskSegments.Clear();
				}
				else
				{
					if (inMemToDiskBytes != 0)
					{
						Log.Info("Keeping " + numMemDiskSegments + " segments, " + inMemToDiskBytes + " bytes in memory for "
							 + "intermediate, on-disk merge");
					}
				}
			}
			// segments on disk
			IList<Merger.Segment<K, V>> diskSegments = new AList<Merger.Segment<K, V>>();
			long onDiskBytes = inMemToDiskBytes;
			long rawBytes = inMemToDiskBytes;
			MergeManagerImpl.CompressAwarePath[] onDisk = Sharpen.Collections.ToArray(onDiskMapOutputs
				, new MergeManagerImpl.CompressAwarePath[onDiskMapOutputs.Count]);
			foreach (MergeManagerImpl.CompressAwarePath file in onDisk)
			{
				long fileLength = fs.GetFileStatus(file).GetLen();
				onDiskBytes += fileLength;
				rawBytes += (file.GetRawDataLength() > 0) ? file.GetRawDataLength() : fileLength;
				Log.Debug("Disk file: " + file + " Length is " + fileLength);
				diskSegments.AddItem(new Merger.Segment<K, V>(job, fs, file, codec, keepInputs, (
					file.ToString().EndsWith(Org.Apache.Hadoop.Mapred.Task.MergedOutputPrefix) ? null
					 : mergedMapOutputsCounter), file.GetRawDataLength()));
			}
			Log.Info("Merging " + onDisk.Length + " files, " + onDiskBytes + " bytes from disk"
				);
			diskSegments.Sort(new _IComparer_786());
			// build final list of segments from merged backed by disk + in-mem
			IList<Merger.Segment<K, V>> finalSegments = new AList<Merger.Segment<K, V>>();
			long inMemBytes = CreateInMemorySegments(inMemoryMapOutputs, finalSegments, 0);
			Log.Info("Merging " + finalSegments.Count + " segments, " + inMemBytes + " bytes from memory into reduce"
				);
			if (0 != onDiskBytes)
			{
				int numInMemSegments = memDiskSegments.Count;
				diskSegments.AddRange(0, memDiskSegments);
				memDiskSegments.Clear();
				// Pass mergePhase only if there is a going to be intermediate
				// merges. See comment where mergePhaseFinished is being set
				Progress thisPhase = (mergePhaseFinished) ? null : mergePhase;
				RawKeyValueIterator diskMerge = Merger.Merge(job, fs, keyClass, valueClass, codec
					, diskSegments, ioSortFactor, numInMemSegments, tmpDir, comparator, reporter, false
					, spilledRecordsCounter, null, thisPhase);
				diskSegments.Clear();
				if (0 == finalSegments.Count)
				{
					return diskMerge;
				}
				finalSegments.AddItem(new Merger.Segment<K, V>(new MergeManagerImpl.RawKVIteratorReader
					(this, diskMerge, onDiskBytes), true, rawBytes));
			}
			return Merger.Merge(job, fs, keyClass, valueClass, finalSegments, finalSegments.Count
				, tmpDir, comparator, reporter, spilledRecordsCounter, null, null);
		}

		private sealed class _IComparer_786 : IComparer<Merger.Segment<K, V>>
		{
			public _IComparer_786()
			{
			}

			public int Compare(Merger.Segment<K, V> o1, Merger.Segment<K, V> o2)
			{
				if (o1.GetLength() == o2.GetLength())
				{
					return 0;
				}
				return o1.GetLength() < o2.GetLength() ? -1 : 1;
			}
		}

		internal class CompressAwarePath : Path
		{
			private long rawDataLength;

			private long compressedSize;

			public CompressAwarePath(Path path, long rawDataLength, long compressSize)
				: base(path.ToUri())
			{
				this.rawDataLength = rawDataLength;
				this.compressedSize = compressSize;
			}

			public virtual long GetRawDataLength()
			{
				return rawDataLength;
			}

			public virtual long GetCompressedSize()
			{
				return compressedSize;
			}

			public override bool Equals(object other)
			{
				return base.Equals(other);
			}

			public override int GetHashCode()
			{
				return base.GetHashCode();
			}

			public override int CompareTo(object obj)
			{
				if (obj is MergeManagerImpl.CompressAwarePath)
				{
					MergeManagerImpl.CompressAwarePath compPath = (MergeManagerImpl.CompressAwarePath
						)obj;
					if (this.compressedSize < compPath.GetCompressedSize())
					{
						return -1;
					}
					else
					{
						if (this.GetCompressedSize() > compPath.GetCompressedSize())
						{
							return 1;
						}
					}
				}
				// Not returning 0 here so that objects with the same size (but
				// different paths) are still added to the TreeSet.
				return base.CompareTo(obj);
			}
		}
	}
}
