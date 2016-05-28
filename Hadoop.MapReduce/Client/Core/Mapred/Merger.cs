using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Merger is an utility class used by the Map and Reduce tasks for merging
	/// both their memory and disk segments
	/// </summary>
	public class Merger
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Merger));

		private static LocalDirAllocator lDirAlloc = new LocalDirAllocator(MRConfig.LocalDir
			);

		// Local directories
		/// <exception cref="System.IO.IOException"/>
		public static RawKeyValueIterator Merge<K, V>(Configuration conf, FileSystem fs, 
			CompressionCodec codec, Path[] inputs, bool deleteInputs, int mergeFactor, Path 
			tmpDir, RawComparator<K> comparator, Progressable reporter, Counters.Counter readsCounter
			, Counters.Counter writesCounter, Progress mergePhase)
		{
			System.Type keyClass = typeof(K);
			System.Type valueClass = typeof(V);
			return new Merger.MergeQueue<K, V>(conf, fs, inputs, deleteInputs, codec, comparator
				, reporter, null, TaskType.Reduce).Merge(keyClass, valueClass, mergeFactor, tmpDir
				, readsCounter, writesCounter, mergePhase);
		}

		/// <exception cref="System.IO.IOException"/>
		public static RawKeyValueIterator Merge<K, V>(Configuration conf, FileSystem fs, 
			CompressionCodec codec, Path[] inputs, bool deleteInputs, int mergeFactor, Path 
			tmpDir, RawComparator<K> comparator, Progressable reporter, Counters.Counter readsCounter
			, Counters.Counter writesCounter, Counters.Counter mergedMapOutputsCounter, Progress
			 mergePhase)
		{
			System.Type keyClass = typeof(K);
			System.Type valueClass = typeof(V);
			return new Merger.MergeQueue<K, V>(conf, fs, inputs, deleteInputs, codec, comparator
				, reporter, mergedMapOutputsCounter, TaskType.Reduce).Merge(keyClass, valueClass
				, mergeFactor, tmpDir, readsCounter, writesCounter, mergePhase);
		}

		/// <exception cref="System.IO.IOException"/>
		public static RawKeyValueIterator Merge<K, V>(Configuration conf, FileSystem fs, 
			IList<Merger.Segment<K, V>> segments, int mergeFactor, Path tmpDir, RawComparator
			<K> comparator, Progressable reporter, Counters.Counter readsCounter, Counters.Counter
			 writesCounter, Progress mergePhase)
		{
			System.Type keyClass = typeof(K);
			System.Type valueClass = typeof(V);
			return Merge(conf, fs, keyClass, valueClass, segments, mergeFactor, tmpDir, comparator
				, reporter, false, readsCounter, writesCounter, mergePhase);
		}

		/// <exception cref="System.IO.IOException"/>
		public static RawKeyValueIterator Merge<K, V>(Configuration conf, FileSystem fs, 
			IList<Merger.Segment<K, V>> segments, int mergeFactor, Path tmpDir, RawComparator
			<K> comparator, Progressable reporter, bool sortSegments, Counters.Counter readsCounter
			, Counters.Counter writesCounter, Progress mergePhase)
		{
			System.Type keyClass = typeof(K);
			System.Type valueClass = typeof(V);
			return new Merger.MergeQueue<K, V>(conf, fs, segments, comparator, reporter, sortSegments
				, TaskType.Reduce).Merge(keyClass, valueClass, mergeFactor, tmpDir, readsCounter
				, writesCounter, mergePhase);
		}

		/// <exception cref="System.IO.IOException"/>
		public static RawKeyValueIterator Merge<K, V>(Configuration conf, FileSystem fs, 
			CompressionCodec codec, IList<Merger.Segment<K, V>> segments, int mergeFactor, Path
			 tmpDir, RawComparator<K> comparator, Progressable reporter, bool sortSegments, 
			Counters.Counter readsCounter, Counters.Counter writesCounter, Progress mergePhase
			, TaskType taskType)
		{
			System.Type keyClass = typeof(K);
			System.Type valueClass = typeof(V);
			return new Merger.MergeQueue<K, V>(conf, fs, segments, comparator, reporter, sortSegments
				, codec, taskType).Merge(keyClass, valueClass, mergeFactor, tmpDir, readsCounter
				, writesCounter, mergePhase);
		}

		/// <exception cref="System.IO.IOException"/>
		public static RawKeyValueIterator Merge<K, V>(Configuration conf, FileSystem fs, 
			IList<Merger.Segment<K, V>> segments, int mergeFactor, int inMemSegments, Path tmpDir
			, RawComparator<K> comparator, Progressable reporter, bool sortSegments, Counters.Counter
			 readsCounter, Counters.Counter writesCounter, Progress mergePhase)
		{
			System.Type keyClass = typeof(K);
			System.Type valueClass = typeof(V);
			return new Merger.MergeQueue<K, V>(conf, fs, segments, comparator, reporter, sortSegments
				, TaskType.Reduce).Merge(keyClass, valueClass, mergeFactor, inMemSegments, tmpDir
				, readsCounter, writesCounter, mergePhase);
		}

		/// <exception cref="System.IO.IOException"/>
		public static RawKeyValueIterator Merge<K, V>(Configuration conf, FileSystem fs, 
			CompressionCodec codec, IList<Merger.Segment<K, V>> segments, int mergeFactor, int
			 inMemSegments, Path tmpDir, RawComparator<K> comparator, Progressable reporter, 
			bool sortSegments, Counters.Counter readsCounter, Counters.Counter writesCounter
			, Progress mergePhase)
		{
			System.Type keyClass = typeof(K);
			System.Type valueClass = typeof(V);
			return new Merger.MergeQueue<K, V>(conf, fs, segments, comparator, reporter, sortSegments
				, codec, TaskType.Reduce).Merge(keyClass, valueClass, mergeFactor, inMemSegments
				, tmpDir, readsCounter, writesCounter, mergePhase);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void WriteFile<K, V>(RawKeyValueIterator records, IFile.Writer<K, V
			> writer, Progressable progressable, Configuration conf)
		{
			long progressBar = conf.GetLong(JobContext.RecordsBeforeProgress, 10000);
			long recordCtr = 0;
			while (records.Next())
			{
				writer.Append(records.GetKey(), records.GetValue());
				if (((recordCtr++) % progressBar) == 0)
				{
					progressable.Progress();
				}
			}
		}

		public class Segment<K, V>
		{
			internal IFile.Reader<K, V> reader = null;

			internal readonly DataInputBuffer key = new DataInputBuffer();

			internal Configuration conf = null;

			internal FileSystem fs = null;

			internal Path file = null;

			internal bool preserve = false;

			internal CompressionCodec codec = null;

			internal long segmentOffset = 0;

			internal long segmentLength = -1;

			internal long rawDataLength = -1;

			internal Counters.Counter mapOutputsCounter = null;

			/// <exception cref="System.IO.IOException"/>
			public Segment(Configuration conf, FileSystem fs, Path file, CompressionCodec codec
				, bool preserve)
				: this(conf, fs, file, codec, preserve, null)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public Segment(Configuration conf, FileSystem fs, Path file, CompressionCodec codec
				, bool preserve, Counters.Counter mergedMapOutputsCounter)
				: this(conf, fs, file, 0, fs.GetFileStatus(file).GetLen(), codec, preserve, mergedMapOutputsCounter
					)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public Segment(Configuration conf, FileSystem fs, Path file, CompressionCodec codec
				, bool preserve, Counters.Counter mergedMapOutputsCounter, long rawDataLength)
				: this(conf, fs, file, 0, fs.GetFileStatus(file).GetLen(), codec, preserve, mergedMapOutputsCounter
					)
			{
				this.rawDataLength = rawDataLength;
			}

			/// <exception cref="System.IO.IOException"/>
			public Segment(Configuration conf, FileSystem fs, Path file, long segmentOffset, 
				long segmentLength, CompressionCodec codec, bool preserve)
				: this(conf, fs, file, segmentOffset, segmentLength, codec, preserve, null)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public Segment(Configuration conf, FileSystem fs, Path file, long segmentOffset, 
				long segmentLength, CompressionCodec codec, bool preserve, Counters.Counter mergedMapOutputsCounter
				)
			{
				this.conf = conf;
				this.fs = fs;
				this.file = file;
				this.codec = codec;
				this.preserve = preserve;
				this.segmentOffset = segmentOffset;
				this.segmentLength = segmentLength;
				this.mapOutputsCounter = mergedMapOutputsCounter;
			}

			public Segment(IFile.Reader<K, V> reader, bool preserve)
				: this(reader, preserve, null)
			{
			}

			public Segment(IFile.Reader<K, V> reader, bool preserve, long rawDataLength)
				: this(reader, preserve, null)
			{
				this.rawDataLength = rawDataLength;
			}

			public Segment(IFile.Reader<K, V> reader, bool preserve, Counters.Counter mapOutputsCounter
				)
			{
				this.reader = reader;
				this.preserve = preserve;
				this.segmentLength = reader.GetLength();
				this.mapOutputsCounter = mapOutputsCounter;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Init(Counters.Counter readsCounter)
			{
				if (reader == null)
				{
					FSDataInputStream @in = fs.Open(file);
					@in.Seek(segmentOffset);
					@in = CryptoUtils.WrapIfNecessary(conf, @in);
					reader = new IFile.Reader<K, V>(conf, @in, segmentLength - CryptoUtils.CryptoPadding
						(conf), codec, readsCounter);
				}
				if (mapOutputsCounter != null)
				{
					mapOutputsCounter.Increment(1);
				}
			}

			internal virtual bool InMemory()
			{
				return fs == null;
			}

			internal virtual DataInputBuffer GetKey()
			{
				return key;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual DataInputBuffer GetValue(DataInputBuffer value)
			{
				NextRawValue(value);
				return value;
			}

			public virtual long GetLength()
			{
				return (reader == null) ? segmentLength : reader.GetLength();
			}

			public virtual long GetRawDataLength()
			{
				return (rawDataLength > 0) ? rawDataLength : GetLength();
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual bool NextRawKey()
			{
				return reader.NextRawKey(key);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void NextRawValue(DataInputBuffer value)
			{
				reader.NextRawValue(value);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void CloseReader()
			{
				if (reader != null)
				{
					reader.Close();
					reader = null;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Close()
			{
				CloseReader();
				if (!preserve && fs != null)
				{
					fs.Delete(file, false);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPosition()
			{
				return reader.GetPosition();
			}

			// This method is used by BackupStore to extract the 
			// absolute position after a reset
			/// <exception cref="System.IO.IOException"/>
			internal virtual long GetActualPosition()
			{
				return segmentOffset + reader.GetPosition();
			}

			internal virtual IFile.Reader<K, V> GetReader()
			{
				return reader;
			}

			// This method is used by BackupStore to reinitialize the
			// reader to start reading from a different segment offset
			/// <exception cref="System.IO.IOException"/>
			internal virtual void ReinitReader(int offset)
			{
				if (!InMemory())
				{
					CloseReader();
					segmentOffset = offset;
					segmentLength = fs.GetFileStatus(file).GetLen() - segmentOffset;
					Init(null);
				}
			}
		}

		private class MergeQueue<K, V> : PriorityQueue<Merger.Segment<K, V>>, RawKeyValueIterator
		{
			internal Configuration conf;

			internal FileSystem fs;

			internal CompressionCodec codec;

			internal IList<Merger.Segment<K, V>> segments = new AList<Merger.Segment<K, V>>();

			internal RawComparator<K> comparator;

			private long totalBytesProcessed;

			private float progPerByte;

			private Progress mergeProgress = new Progress();

			internal Progressable reporter;

			internal DataInputBuffer key;

			internal readonly DataInputBuffer value = new DataInputBuffer();

			internal readonly DataInputBuffer diskIFileValue = new DataInputBuffer();

			private bool includeFinalMerge = false;

			// Boolean variable for including/considering final merge as part of sort
			// phase or not. This is true in map task, false in reduce task. It is
			// used in calculating mergeProgress.
			/// <summary>Sets the boolean variable includeFinalMerge to true.</summary>
			/// <remarks>
			/// Sets the boolean variable includeFinalMerge to true. Called from
			/// map task before calling merge() so that final merge of map task
			/// is also considered as part of sort phase.
			/// </remarks>
			private void ConsiderFinalMergeForProgress()
			{
				includeFinalMerge = true;
			}

			internal Merger.Segment<K, V> minSegment;

			private sealed class _IComparer_422 : IComparer<Merger.Segment<K, V>>
			{
				public _IComparer_422()
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

			internal IComparer<Merger.Segment<K, V>> segmentComparator = new _IComparer_422();

			/// <exception cref="System.IO.IOException"/>
			public MergeQueue(Configuration conf, FileSystem fs, Path[] inputs, bool deleteInputs
				, CompressionCodec codec, RawComparator<K> comparator, Progressable reporter)
				: this(conf, fs, inputs, deleteInputs, codec, comparator, reporter, null, TaskType
					.Reduce)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public MergeQueue(Configuration conf, FileSystem fs, Path[] inputs, bool deleteInputs
				, CompressionCodec codec, RawComparator<K> comparator, Progressable reporter, Counters.Counter
				 mergedMapOutputsCounter, TaskType taskType)
			{
				this.conf = conf;
				this.fs = fs;
				this.codec = codec;
				this.comparator = comparator;
				this.reporter = reporter;
				if (taskType == TaskType.Map)
				{
					ConsiderFinalMergeForProgress();
				}
				foreach (Path file in inputs)
				{
					Log.Debug("MergeQ: adding: " + file);
					segments.AddItem(new Merger.Segment<K, V>(conf, fs, file, codec, !deleteInputs, (
						file.ToString().EndsWith(Task.MergedOutputPrefix) ? null : mergedMapOutputsCounter
						)));
				}
				// Sort segments on file-lengths
				segments.Sort(segmentComparator);
			}

			public MergeQueue(Configuration conf, FileSystem fs, IList<Merger.Segment<K, V>> 
				segments, RawComparator<K> comparator, Progressable reporter)
				: this(conf, fs, segments, comparator, reporter, false, TaskType.Reduce)
			{
			}

			public MergeQueue(Configuration conf, FileSystem fs, IList<Merger.Segment<K, V>> 
				segments, RawComparator<K> comparator, Progressable reporter, bool sortSegments, 
				TaskType taskType)
			{
				this.conf = conf;
				this.fs = fs;
				this.comparator = comparator;
				this.segments = segments;
				this.reporter = reporter;
				if (taskType == TaskType.Map)
				{
					ConsiderFinalMergeForProgress();
				}
				if (sortSegments)
				{
					segments.Sort(segmentComparator);
				}
			}

			public MergeQueue(Configuration conf, FileSystem fs, IList<Merger.Segment<K, V>> 
				segments, RawComparator<K> comparator, Progressable reporter, bool sortSegments, 
				CompressionCodec codec, TaskType taskType)
				: this(conf, fs, segments, comparator, reporter, sortSegments, taskType)
			{
				this.codec = codec;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				Merger.Segment<K, V> segment;
				while ((segment = Pop()) != null)
				{
					segment.Close();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual DataInputBuffer GetKey()
			{
				return key;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual DataInputBuffer GetValue()
			{
				return value;
			}

			/// <exception cref="System.IO.IOException"/>
			private void AdjustPriorityQueue(Merger.Segment<K, V> reader)
			{
				long startPos = reader.GetReader().bytesRead;
				bool hasNext = reader.NextRawKey();
				long endPos = reader.GetReader().bytesRead;
				totalBytesProcessed += endPos - startPos;
				mergeProgress.Set(totalBytesProcessed * progPerByte);
				if (hasNext)
				{
					AdjustTop();
				}
				else
				{
					Pop();
					reader.Close();
				}
			}

			private void ResetKeyValue()
			{
				key = null;
				value.Reset(new byte[] {  }, 0);
				diskIFileValue.Reset(new byte[] {  }, 0);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next()
			{
				if (Size() == 0)
				{
					ResetKeyValue();
					return false;
				}
				if (minSegment != null)
				{
					//minSegment is non-null for all invocations of next except the first
					//one. For the first invocation, the priority queue is ready for use
					//but for the subsequent invocations, first adjust the queue 
					AdjustPriorityQueue(minSegment);
					if (Size() == 0)
					{
						minSegment = null;
						ResetKeyValue();
						return false;
					}
				}
				minSegment = Top();
				long startPos = minSegment.GetReader().bytesRead;
				key = minSegment.GetKey();
				if (!minSegment.InMemory())
				{
					//When we load the value from an inmemory segment, we reset
					//the "value" DIB in this class to the inmem segment's byte[].
					//When we load the value bytes from disk, we shouldn't use
					//the same byte[] since it would corrupt the data in the inmem
					//segment. So we maintain an explicit DIB for value bytes
					//obtained from disk, and if the current segment is a disk
					//segment, we reset the "value" DIB to the byte[] in that (so 
					//we reuse the disk segment DIB whenever we consider
					//a disk segment).
					minSegment.GetValue(diskIFileValue);
					value.Reset(diskIFileValue.GetData(), diskIFileValue.GetLength());
				}
				else
				{
					minSegment.GetValue(value);
				}
				long endPos = minSegment.GetReader().bytesRead;
				totalBytesProcessed += endPos - startPos;
				mergeProgress.Set(totalBytesProcessed * progPerByte);
				return true;
			}

			protected override bool LessThan(object a, object b)
			{
				DataInputBuffer key1 = ((Merger.Segment<K, V>)a).GetKey();
				DataInputBuffer key2 = ((Merger.Segment<K, V>)b).GetKey();
				int s1 = key1.GetPosition();
				int l1 = key1.GetLength() - s1;
				int s2 = key2.GetPosition();
				int l2 = key2.GetLength() - s2;
				return comparator.Compare(key1.GetData(), s1, l1, key2.GetData(), s2, l2) < 0;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual RawKeyValueIterator Merge(Type keyClass, Type valueClass, int factor
				, Path tmpDir, Counters.Counter readsCounter, Counters.Counter writesCounter, Progress
				 mergePhase)
			{
				return Merge(keyClass, valueClass, factor, 0, tmpDir, readsCounter, writesCounter
					, mergePhase);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual RawKeyValueIterator Merge(Type keyClass, Type valueClass, int factor
				, int inMem, Path tmpDir, Counters.Counter readsCounter, Counters.Counter writesCounter
				, Progress mergePhase)
			{
				Log.Info("Merging " + segments.Count + " sorted segments");
				/*
				* If there are inMemory segments, then they come first in the segments
				* list and then the sorted disk segments. Otherwise(if there are only
				* disk segments), then they are sorted segments if there are more than
				* factor segments in the segments list.
				*/
				int numSegments = segments.Count;
				int origFactor = factor;
				int passNo = 1;
				if (mergePhase != null)
				{
					mergeProgress = mergePhase;
				}
				long totalBytes = ComputeBytesInMerges(factor, inMem);
				if (totalBytes != 0)
				{
					progPerByte = 1.0f / (float)totalBytes;
				}
				do
				{
					//create the MergeStreams from the sorted map created in the constructor
					//and dump the final output to a file
					//get the factor for this pass of merge. We assume in-memory segments
					//are the first entries in the segment list and that the pass factor
					//doesn't apply to them
					factor = GetPassFactor(factor, passNo, numSegments - inMem);
					if (1 == passNo)
					{
						factor += inMem;
					}
					IList<Merger.Segment<K, V>> segmentsToMerge = new AList<Merger.Segment<K, V>>();
					int segmentsConsidered = 0;
					int numSegmentsToConsider = factor;
					long startBytes = 0;
					// starting bytes of segments of this merge
					while (true)
					{
						//extract the smallest 'factor' number of segments  
						//Call cleanup on the empty segments (no key/value data)
						IList<Merger.Segment<K, V>> mStream = GetSegmentDescriptors(numSegmentsToConsider
							);
						foreach (Merger.Segment<K, V> segment in mStream)
						{
							// Initialize the segment at the last possible moment;
							// this helps in ensuring we don't use buffers until we need them
							segment.Init(readsCounter);
							long startPos = segment.GetReader().bytesRead;
							bool hasNext = segment.NextRawKey();
							long endPos = segment.GetReader().bytesRead;
							if (hasNext)
							{
								startBytes += endPos - startPos;
								segmentsToMerge.AddItem(segment);
								segmentsConsidered++;
							}
							else
							{
								segment.Close();
								numSegments--;
							}
						}
						//we ignore this segment for the merge
						//if we have the desired number of segments
						//or looked at all available segments, we break
						if (segmentsConsidered == factor || segments.Count == 0)
						{
							break;
						}
						numSegmentsToConsider = factor - segmentsConsidered;
					}
					//feed the streams to the priority queue
					Initialize(segmentsToMerge.Count);
					Clear();
					foreach (Merger.Segment<K, V> segment_1 in segmentsToMerge)
					{
						Put(segment_1);
					}
					//if we have lesser number of segments remaining, then just return the
					//iterator, else do another single level merge
					if (numSegments <= factor)
					{
						if (!includeFinalMerge)
						{
							// for reduce task
							// Reset totalBytesProcessed and recalculate totalBytes from the
							// remaining segments to track the progress of the final merge.
							// Final merge is considered as the progress of the reducePhase,
							// the 3rd phase of reduce task.
							totalBytesProcessed = 0;
							totalBytes = 0;
							for (int i = 0; i < segmentsToMerge.Count; i++)
							{
								totalBytes += segmentsToMerge[i].GetRawDataLength();
							}
						}
						if (totalBytes != 0)
						{
							//being paranoid
							progPerByte = 1.0f / (float)totalBytes;
						}
						totalBytesProcessed += startBytes;
						if (totalBytes != 0)
						{
							mergeProgress.Set(totalBytesProcessed * progPerByte);
						}
						else
						{
							mergeProgress.Set(1.0f);
						}
						// Last pass and no segments left - we're done
						Log.Info("Down to the last merge-pass, with " + numSegments + " segments left of total size: "
							 + (totalBytes - totalBytesProcessed) + " bytes");
						return this;
					}
					else
					{
						Log.Info("Merging " + segmentsToMerge.Count + " intermediate segments out of a total of "
							 + (segments.Count + segmentsToMerge.Count));
						long bytesProcessedInPrevMerges = totalBytesProcessed;
						totalBytesProcessed += startBytes;
						//we want to spread the creation of temp files on multiple disks if 
						//available under the space constraints
						long approxOutputSize = 0;
						foreach (Merger.Segment<K, V> s in segmentsToMerge)
						{
							approxOutputSize += s.GetLength() + ChecksumFileSystem.GetApproxChkSumLength(s.GetLength
								());
						}
						Path tmpFilename = new Path(tmpDir, "intermediate").Suffix("." + passNo);
						Path outputFile = lDirAlloc.GetLocalPathForWrite(tmpFilename.ToString(), approxOutputSize
							, conf);
						FSDataOutputStream @out = fs.Create(outputFile);
						@out = CryptoUtils.WrapIfNecessary(conf, @out);
						IFile.Writer<K, V> writer = new IFile.Writer<K, V>(conf, @out, keyClass, valueClass
							, codec, writesCounter, true);
						WriteFile(this, writer, reporter, conf);
						writer.Close();
						//we finished one single level merge; now clean up the priority 
						//queue
						this.Close();
						// Add the newly create segment to the list of segments to be merged
						Merger.Segment<K, V> tempSegment = new Merger.Segment<K, V>(conf, fs, outputFile, 
							codec, false);
						// Insert new merged segment into the sorted list
						int pos = Sharpen.Collections.BinarySearch(segments, tempSegment, segmentComparator
							);
						if (pos < 0)
						{
							// binary search failed. So position to be inserted at is -pos-1
							pos = -pos - 1;
						}
						segments.Add(pos, tempSegment);
						numSegments = segments.Count;
						// Subtract the difference between expected size of new segment and 
						// actual size of new segment(Expected size of new segment is
						// inputBytesOfThisMerge) from totalBytes. Expected size and actual
						// size will match(almost) if combiner is not called in merge.
						long inputBytesOfThisMerge = totalBytesProcessed - bytesProcessedInPrevMerges;
						totalBytes -= inputBytesOfThisMerge - tempSegment.GetRawDataLength();
						if (totalBytes != 0)
						{
							progPerByte = 1.0f / (float)totalBytes;
						}
						passNo++;
					}
					//we are worried about only the first pass merge factor. So reset the 
					//factor to what it originally was
					factor = origFactor;
				}
				while (true);
			}

			/// <summary>Determine the number of segments to merge in a given pass.</summary>
			/// <remarks>
			/// Determine the number of segments to merge in a given pass. Assuming more
			/// than factor segments, the first pass should attempt to bring the total
			/// number of segments - 1 to be divisible by the factor - 1 (each pass
			/// takes X segments and produces 1) to minimize the number of merges.
			/// </remarks>
			private int GetPassFactor(int factor, int passNo, int numSegments)
			{
				if (passNo > 1 || numSegments <= factor || factor == 1)
				{
					return factor;
				}
				int mod = (numSegments - 1) % (factor - 1);
				if (mod == 0)
				{
					return factor;
				}
				return mod + 1;
			}

			/// <summary>
			/// Return (& remove) the requested number of segment descriptors from the
			/// sorted map.
			/// </summary>
			private IList<Merger.Segment<K, V>> GetSegmentDescriptors(int numDescriptors)
			{
				if (numDescriptors > segments.Count)
				{
					IList<Merger.Segment<K, V>> subList = new AList<Merger.Segment<K, V>>(segments);
					segments.Clear();
					return subList;
				}
				IList<Merger.Segment<K, V>> subList_1 = new AList<Merger.Segment<K, V>>(segments.
					SubList(0, numDescriptors));
				for (int i = 0; i < numDescriptors; ++i)
				{
					segments.Remove(0);
				}
				return subList_1;
			}

			/// <summary>
			/// Compute expected size of input bytes to merges, will be used in
			/// calculating mergeProgress.
			/// </summary>
			/// <remarks>
			/// Compute expected size of input bytes to merges, will be used in
			/// calculating mergeProgress. This simulates the above merge() method and
			/// tries to obtain the number of bytes that are going to be merged in all
			/// merges(assuming that there is no combiner called while merging).
			/// </remarks>
			/// <param name="factor">mapreduce.task.io.sort.factor</param>
			/// <param name="inMem">number of segments in memory to be merged</param>
			internal virtual long ComputeBytesInMerges(int factor, int inMem)
			{
				int numSegments = segments.Count;
				IList<long> segmentSizes = new AList<long>(numSegments);
				long totalBytes = 0;
				int n = numSegments - inMem;
				// factor for 1st pass
				int f = GetPassFactor(factor, 1, n) + inMem;
				n = numSegments;
				for (int i = 0; i < numSegments; i++)
				{
					// Not handling empty segments here assuming that it would not affect
					// much in calculation of mergeProgress.
					segmentSizes.AddItem(segments[i].GetRawDataLength());
				}
				// If includeFinalMerge is true, allow the following while loop iterate
				// for 1 more iteration. This is to include final merge as part of the
				// computation of expected input bytes of merges
				bool considerFinalMerge = includeFinalMerge;
				while (n > f || considerFinalMerge)
				{
					if (n <= f)
					{
						considerFinalMerge = false;
					}
					long mergedSize = 0;
					f = Math.Min(f, segmentSizes.Count);
					for (int j = 0; j < f; j++)
					{
						mergedSize += segmentSizes.Remove(0);
					}
					totalBytes += mergedSize;
					// insert new size into the sorted list
					int pos = Sharpen.Collections.BinarySearch(segmentSizes, mergedSize);
					if (pos < 0)
					{
						pos = -pos - 1;
					}
					segmentSizes.Add(pos, mergedSize);
					n -= (f - 1);
					f = factor;
				}
				return totalBytes;
			}

			public virtual Progress GetProgress()
			{
				return mergeProgress;
			}
		}
	}
}
