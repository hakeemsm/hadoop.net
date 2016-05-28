using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Mapreduce.Task.Reduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A Reduce task.</summary>
	public class ReduceTask : Task
	{
		static ReduceTask()
		{
			mapOutputFilesOnDisk = new TreeSet<FileStatus>(mapOutputFileComparator);
			// register a ctor
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.Mapred.ReduceTask), new _WritableFactory_68
				());
		}

		private sealed class _WritableFactory_68 : WritableFactory
		{
			public _WritableFactory_68()
			{
			}

			public Writable NewInstance()
			{
				return new Org.Apache.Hadoop.Mapred.ReduceTask();
			}
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.ReduceTask
			).FullName);

		private int numMaps;

		private CompressionCodec codec;

		private IDictionary<TaskAttemptID, MapOutputFile> localMapFiles;

		private Progress copyPhase;

		private Progress sortPhase;

		private Progress reducePhase;

		private Counters.Counter shuffledMapsCounter = GetCounters().FindCounter(TaskCounter
			.ShuffledMaps);

		private Counters.Counter reduceShuffleBytes = GetCounters().FindCounter(TaskCounter
			.ReduceShuffleBytes);

		private Counters.Counter reduceInputKeyCounter = GetCounters().FindCounter(TaskCounter
			.ReduceInputGroups);

		private Counters.Counter reduceInputValueCounter = GetCounters().FindCounter(TaskCounter
			.ReduceInputRecords);

		private Counters.Counter reduceOutputCounter = GetCounters().FindCounter(TaskCounter
			.ReduceOutputRecords);

		private Counters.Counter reduceCombineInputCounter = GetCounters().FindCounter(TaskCounter
			.CombineInputRecords);

		private Counters.Counter reduceCombineOutputCounter = GetCounters().FindCounter(TaskCounter
			.CombineOutputRecords);

		private Counters.Counter fileOutputByteCounter = GetCounters().FindCounter(FileOutputFormatCounter
			.BytesWritten);

		private sealed class _IComparer_113 : IComparer<FileStatus>
		{
			public _IComparer_113()
			{
			}

			// If this is a LocalJobRunner-based job, this will
			// be a mapping from map task attempts to their output files.
			// This will be null in other cases.
			// phase to start with 
			// A custom comparator for map output files. Here the ordering is determined
			// by the file's size and path. In case of files with same size and different
			// file paths, the first parameter is considered smaller than the second one.
			// In case of files with same size and path are considered equal.
			public int Compare(FileStatus a, FileStatus b)
			{
				if (a.GetLen() < b.GetLen())
				{
					return -1;
				}
				else
				{
					if (a.GetLen() == b.GetLen())
					{
						if (a.GetPath().ToString().Equals(b.GetPath().ToString()))
						{
							return 0;
						}
						else
						{
							return -1;
						}
					}
					else
					{
						return 1;
					}
				}
			}
		}

		private IComparer<FileStatus> mapOutputFileComparator = new _IComparer_113();

		private readonly ICollection<FileStatus> mapOutputFilesOnDisk;

		public ReduceTask()
			: base()
		{
			mapOutputFilesOnDisk = new TreeSet<FileStatus>(mapOutputFileComparator);
			{
				GetProgress().SetStatus("reduce");
				SetPhase(TaskStatus.Phase.Shuffle);
			}
		}

		public ReduceTask(string jobFile, TaskAttemptID taskId, int partition, int numMaps
			, int numSlotsRequired)
			: base(jobFile, taskId, partition, numSlotsRequired)
		{
			mapOutputFilesOnDisk = new TreeSet<FileStatus>(mapOutputFileComparator);
			{
				GetProgress().SetStatus("reduce");
				SetPhase(TaskStatus.Phase.Shuffle);
			}
			// A sorted set for keeping a set of map output files on disk
			this.numMaps = numMaps;
		}

		/// <summary>
		/// Register the set of mapper outputs created by a LocalJobRunner-based
		/// job with this ReduceTask so it knows where to fetch from.
		/// </summary>
		/// <remarks>
		/// Register the set of mapper outputs created by a LocalJobRunner-based
		/// job with this ReduceTask so it knows where to fetch from.
		/// This should not be called in normal (networked) execution.
		/// </remarks>
		public virtual void SetLocalMapFiles(IDictionary<TaskAttemptID, MapOutputFile> mapFiles
			)
		{
			this.localMapFiles = mapFiles;
		}

		private CompressionCodec InitCodec()
		{
			// check if map-outputs are to be compressed
			if (conf.GetCompressMapOutput())
			{
				Type codecClass = conf.GetMapOutputCompressorClass(typeof(DefaultCodec));
				return ReflectionUtils.NewInstance(codecClass, conf);
			}
			return null;
		}

		public override bool IsMapTask()
		{
			return false;
		}

		public virtual int GetNumMaps()
		{
			return numMaps;
		}

		/// <summary>Localize the given JobConf to be specific for this task.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void LocalizeConfiguration(JobConf conf)
		{
			base.LocalizeConfiguration(conf);
			conf.SetNumMapTasks(numMaps);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			base.Write(@out);
			@out.WriteInt(numMaps);
		}

		// write the number of maps
		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			base.ReadFields(@in);
			numMaps = @in.ReadInt();
		}

		// Get the input files for the reducer (for local jobs).
		/// <exception cref="System.IO.IOException"/>
		private Path[] GetMapFiles(FileSystem fs)
		{
			IList<Path> fileList = new AList<Path>();
			for (int i = 0; i < numMaps; ++i)
			{
				fileList.AddItem(mapOutputFile.GetInputFile(i));
			}
			return Sharpen.Collections.ToArray(fileList, new Path[0]);
		}

		private class ReduceValuesIterator<Key, Value> : Task.ValuesIterator<KEY, VALUE>
		{
			/// <exception cref="System.IO.IOException"/>
			public ReduceValuesIterator(ReduceTask _enclosing, RawKeyValueIterator @in, RawComparator
				<KEY> comparator, Type keyClass, Type valClass, Configuration conf, Progressable
				 reporter)
				: base(@in, comparator, keyClass, valClass, conf, reporter)
			{
				this._enclosing = _enclosing;
			}

			public override VALUE Next()
			{
				this._enclosing.reduceInputValueCounter.Increment(1);
				return this.MoveToNext();
			}

			protected internal virtual VALUE MoveToNext()
			{
				return base.Next();
			}

			public virtual void InformReduceProgress()
			{
				this._enclosing.reducePhase.Set(base.@in.GetProgress().GetProgress());
				// update progress
				this.reporter.Progress();
			}

			private readonly ReduceTask _enclosing;
		}

		private class SkippingReduceValuesIterator<Key, Value> : ReduceTask.ReduceValuesIterator
			<KEY, VALUE>
		{
			private SortedRanges.SkipRangeIterator skipIt;

			private TaskUmbilicalProtocol umbilical;

			private Counters.Counter skipGroupCounter;

			private Counters.Counter skipRecCounter;

			private long grpIndex = -1;

			private Type keyClass;

			private Type valClass;

			private SequenceFile.Writer skipWriter;

			private bool toWriteSkipRecs;

			private bool hasNext;

			private Task.TaskReporter reporter;

			/// <exception cref="System.IO.IOException"/>
			public SkippingReduceValuesIterator(ReduceTask _enclosing, RawKeyValueIterator @in
				, RawComparator<KEY> comparator, Type keyClass, Type valClass, Configuration conf
				, Task.TaskReporter reporter, TaskUmbilicalProtocol umbilical)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.umbilical = umbilical;
				this.skipGroupCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter.ReduceSkippedGroups
					));
				this.skipRecCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter.ReduceSkippedRecords
					));
				this.toWriteSkipRecs = this._enclosing.ToWriteSkipRecs() && SkipBadRecords.GetSkipOutputPath
					(conf) != null;
				this.keyClass = keyClass;
				this.valClass = valClass;
				this.reporter = reporter;
				this.skipIt = this._enclosing.GetSkipRanges().SkipRangeIterator();
				this.MayBeSkip();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void NextKey()
			{
				base.NextKey();
				this.MayBeSkip();
			}

			public override bool More()
			{
				return base.More() && this.hasNext;
			}

			/// <exception cref="System.IO.IOException"/>
			private void MayBeSkip()
			{
				this.hasNext = this.skipIt.HasNext();
				if (!this.hasNext)
				{
					ReduceTask.Log.Warn("Further groups got skipped.");
					return;
				}
				this.grpIndex++;
				long nextGrpIndex = this.skipIt.Next();
				long skip = 0;
				long skipRec = 0;
				while (this.grpIndex < nextGrpIndex && base.More())
				{
					while (this._enclosing._enclosing.HasNext())
					{
						VALUE value = this.MoveToNext();
						if (this.toWriteSkipRecs)
						{
							this.WriteSkippedRec(this._enclosing._enclosing.GetKey(), value);
						}
						skipRec++;
					}
					base.NextKey();
					this.grpIndex++;
					skip++;
				}
				//close the skip writer once all the ranges are skipped
				if (skip > 0 && this.skipIt.SkippedAllRanges() && this.skipWriter != null)
				{
					this.skipWriter.Close();
				}
				this.skipGroupCounter.Increment(skip);
				this.skipRecCounter.Increment(skipRec);
				this._enclosing.ReportNextRecordRange(this.umbilical, this.grpIndex);
			}

			/// <exception cref="System.IO.IOException"/>
			private void WriteSkippedRec(KEY key, VALUE value)
			{
				if (this.skipWriter == null)
				{
					Path skipDir = SkipBadRecords.GetSkipOutputPath(this._enclosing.conf);
					Path skipFile = new Path(skipDir, this._enclosing.GetTaskID().ToString());
					this.skipWriter = SequenceFile.CreateWriter(skipFile.GetFileSystem(this._enclosing
						.conf), this._enclosing.conf, skipFile, this.keyClass, this.valClass, SequenceFile.CompressionType
						.Block, this.reporter);
				}
				this.skipWriter.Append(key, value);
			}

			private readonly ReduceTask _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public override void Run(JobConf job, TaskUmbilicalProtocol umbilical)
		{
			job.SetBoolean(JobContext.SkipRecords, IsSkipping());
			if (IsMapOrReduce())
			{
				copyPhase = GetProgress().AddPhase("copy");
				sortPhase = GetProgress().AddPhase("sort");
				reducePhase = GetProgress().AddPhase("reduce");
			}
			// start thread that will handle communication with parent
			Task.TaskReporter reporter = StartReporter(umbilical);
			bool useNewApi = job.GetUseNewReducer();
			Initialize(job, GetJobID(), reporter, useNewApi);
			// check if it is a cleanupJobTask
			if (jobCleanup)
			{
				RunJobCleanupTask(umbilical, reporter);
				return;
			}
			if (jobSetup)
			{
				RunJobSetupTask(umbilical, reporter);
				return;
			}
			if (taskCleanup)
			{
				RunTaskCleanupTask(umbilical, reporter);
				return;
			}
			// Initialize the codec
			codec = InitCodec();
			RawKeyValueIterator rIter = null;
			ShuffleConsumerPlugin shuffleConsumerPlugin = null;
			Type combinerClass = conf.GetCombinerClass();
			Task.CombineOutputCollector combineCollector = (null != combinerClass) ? new Task.CombineOutputCollector
				(reduceCombineOutputCounter, reporter, conf) : null;
			Type clazz = job.GetClass<ShuffleConsumerPlugin>(MRConfig.ShuffleConsumerPlugin, 
				typeof(Shuffle));
			shuffleConsumerPlugin = ReflectionUtils.NewInstance(clazz, job);
			Log.Info("Using ShuffleConsumerPlugin: " + shuffleConsumerPlugin);
			ShuffleConsumerPlugin.Context shuffleContext = new ShuffleConsumerPlugin.Context(
				GetTaskID(), job, FileSystem.GetLocal(job), umbilical, base.lDirAlloc, reporter, 
				codec, combinerClass, combineCollector, spilledRecordsCounter, reduceCombineInputCounter
				, shuffledMapsCounter, reduceShuffleBytes, failedShuffleCounter, mergedMapOutputsCounter
				, taskStatus, copyPhase, sortPhase, this, mapOutputFile, localMapFiles);
			shuffleConsumerPlugin.Init(shuffleContext);
			rIter = shuffleConsumerPlugin.Run();
			// free up the data structures
			mapOutputFilesOnDisk.Clear();
			sortPhase.Complete();
			// sort is complete
			SetPhase(TaskStatus.Phase.Reduce);
			StatusUpdate(umbilical);
			Type keyClass = job.GetMapOutputKeyClass();
			Type valueClass = job.GetMapOutputValueClass();
			RawComparator comparator = job.GetOutputValueGroupingComparator();
			if (useNewApi)
			{
				RunNewReducer(job, umbilical, reporter, rIter, comparator, keyClass, valueClass);
			}
			else
			{
				RunOldReducer(job, umbilical, reporter, rIter, comparator, keyClass, valueClass);
			}
			shuffleConsumerPlugin.Close();
			Done(umbilical, reporter);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunOldReducer<Inkey, Invalue, Outkey, Outvalue>(JobConf job, TaskUmbilicalProtocol
			 umbilical, Task.TaskReporter reporter, RawKeyValueIterator rIter, RawComparator
			<INKEY> comparator)
		{
			System.Type keyClass = typeof(INKEY);
			System.Type valueClass = typeof(INVALUE);
			Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = ReflectionUtils.NewInstance(job
				.GetReducerClass(), job);
			// make output collector
			string finalName = GetOutputName(GetPartition());
			RecordWriter<OUTKEY, OUTVALUE> @out = new ReduceTask.OldTrackingRecordWriter<OUTKEY
				, OUTVALUE>(this, job, reporter, finalName);
			RecordWriter<OUTKEY, OUTVALUE> finalOut = @out;
			OutputCollector<OUTKEY, OUTVALUE> collector = new _OutputCollector_419(finalOut, 
				reporter);
			// indicate that progress update needs to be sent
			// apply reduce function
			try
			{
				//increment processed counter only if skipping feature is enabled
				bool incrProcCount = SkipBadRecords.GetReducerMaxSkipGroups(job) > 0 && SkipBadRecords
					.GetAutoIncrReducerProcCount(job);
				ReduceTask.ReduceValuesIterator<INKEY, INVALUE> values = IsSkipping() ? new ReduceTask.SkippingReduceValuesIterator
					<INKEY, INVALUE>(this, rIter, comparator, keyClass, valueClass, job, reporter, umbilical
					) : new ReduceTask.ReduceValuesIterator<INKEY, INVALUE>(this, rIter, job.GetOutputValueGroupingComparator
					(), keyClass, valueClass, job, reporter);
				values.InformReduceProgress();
				while (values.More())
				{
					reduceInputKeyCounter.Increment(1);
					reducer.Reduce(values.GetKey(), values, collector, reporter);
					if (incrProcCount)
					{
						reporter.IncrCounter(SkipBadRecords.CounterGroup, SkipBadRecords.CounterReduceProcessedGroups
							, 1);
					}
					values.NextKey();
					values.InformReduceProgress();
				}
				reducer.Close();
				reducer = null;
				@out.Close(reporter);
				@out = null;
			}
			finally
			{
				IOUtils.Cleanup(Log, reducer);
				CloseQuietly(@out, reporter);
			}
		}

		private sealed class _OutputCollector_419 : OutputCollector<OUTKEY, OUTVALUE>
		{
			public _OutputCollector_419(RecordWriter<OUTKEY, OUTVALUE> finalOut, Task.TaskReporter
				 reporter)
			{
				this.finalOut = finalOut;
				this.reporter = reporter;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Collect(OUTKEY key, OUTVALUE value)
			{
				finalOut.Write(key, value);
				reporter.Progress();
			}

			private readonly RecordWriter<OUTKEY, OUTVALUE> finalOut;

			private readonly Task.TaskReporter reporter;
		}

		internal class OldTrackingRecordWriter<K, V> : RecordWriter<K, V>
		{
			private readonly RecordWriter<K, V> real;

			private readonly Counters.Counter reduceOutputCounter;

			private readonly Counters.Counter fileOutputByteCounter;

			private readonly IList<FileSystem.Statistics> fsStats;

			/// <exception cref="System.IO.IOException"/>
			public OldTrackingRecordWriter(ReduceTask reduce, JobConf job, Task.TaskReporter 
				reporter, string finalName)
			{
				this.reduceOutputCounter = reduce.reduceOutputCounter;
				this.fileOutputByteCounter = reduce.fileOutputByteCounter;
				IList<FileSystem.Statistics> matchedStats = null;
				if (job.GetOutputFormat() is FileOutputFormat)
				{
					matchedStats = GetFsStatistics(FileOutputFormat.GetOutputPath(job), job);
				}
				fsStats = matchedStats;
				FileSystem fs = FileSystem.Get(job);
				long bytesOutPrev = GetOutputBytes(fsStats);
				this.real = job.GetOutputFormat().GetRecordWriter(fs, job, finalName, reporter);
				long bytesOutCurr = GetOutputBytes(fsStats);
				fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(K key, V value)
			{
				long bytesOutPrev = GetOutputBytes(fsStats);
				real.Write(key, value);
				long bytesOutCurr = GetOutputBytes(fsStats);
				fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
				reduceOutputCounter.Increment(1);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close(Reporter reporter)
			{
				long bytesOutPrev = GetOutputBytes(fsStats);
				real.Close(reporter);
				long bytesOutCurr = GetOutputBytes(fsStats);
				fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
			}

			private long GetOutputBytes(IList<FileSystem.Statistics> stats)
			{
				if (stats == null)
				{
					return 0;
				}
				long bytesWritten = 0;
				foreach (FileSystem.Statistics stat in stats)
				{
					bytesWritten = bytesWritten + stat.GetBytesWritten();
				}
				return bytesWritten;
			}
		}

		internal class NewTrackingRecordWriter<K, V> : RecordWriter<K, V>
		{
			private readonly RecordWriter<K, V> real;

			private readonly Counter outputRecordCounter;

			private readonly Counter fileOutputByteCounter;

			private readonly IList<FileSystem.Statistics> fsStats;

			/// <exception cref="System.Exception"/>
			/// <exception cref="System.IO.IOException"/>
			internal NewTrackingRecordWriter(ReduceTask reduce, TaskAttemptContext taskContext
				)
			{
				this.outputRecordCounter = reduce.reduceOutputCounter;
				this.fileOutputByteCounter = reduce.fileOutputByteCounter;
				IList<FileSystem.Statistics> matchedStats = null;
				if (reduce.outputFormat is FileOutputFormat)
				{
					matchedStats = GetFsStatistics(FileOutputFormat.GetOutputPath(taskContext), taskContext
						.GetConfiguration());
				}
				fsStats = matchedStats;
				long bytesOutPrev = GetOutputBytes(fsStats);
				this.real = (RecordWriter<K, V>)reduce.outputFormat.GetRecordWriter(taskContext);
				long bytesOutCurr = GetOutputBytes(fsStats);
				fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Close(TaskAttemptContext context)
			{
				long bytesOutPrev = GetOutputBytes(fsStats);
				real.Close(context);
				long bytesOutCurr = GetOutputBytes(fsStats);
				fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(K key, V value)
			{
				long bytesOutPrev = GetOutputBytes(fsStats);
				real.Write(key, value);
				long bytesOutCurr = GetOutputBytes(fsStats);
				fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
				outputRecordCounter.Increment(1);
			}

			private long GetOutputBytes(IList<FileSystem.Statistics> stats)
			{
				if (stats == null)
				{
					return 0;
				}
				long bytesWritten = 0;
				foreach (FileSystem.Statistics stat in stats)
				{
					bytesWritten = bytesWritten + stat.GetBytesWritten();
				}
				return bytesWritten;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		private void RunNewReducer<Inkey, Invalue, Outkey, Outvalue>(JobConf job, TaskUmbilicalProtocol
			 umbilical, Task.TaskReporter reporter, RawKeyValueIterator rIter, RawComparator
			<INKEY> comparator)
		{
			System.Type keyClass = typeof(INKEY);
			System.Type valueClass = typeof(INVALUE);
			// wrap value iterator to report progress.
			RawKeyValueIterator rawIter = rIter;
			rIter = new _RawKeyValueIterator_587(rawIter, reporter);
			// make a task context so we can get the classes
			TaskAttemptContext taskContext = new TaskAttemptContextImpl(job, GetTaskID(), reporter
				);
			// make a reducer
			Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = (Reducer<INKEY, INVALUE, OUTKEY
				, OUTVALUE>)ReflectionUtils.NewInstance(taskContext.GetReducerClass(), job);
			RecordWriter<OUTKEY, OUTVALUE> trackedRW = new ReduceTask.NewTrackingRecordWriter
				<OUTKEY, OUTVALUE>(this, taskContext);
			job.SetBoolean("mapred.skip.on", IsSkipping());
			job.SetBoolean(JobContext.SkipRecords, IsSkipping());
			Reducer.Context reducerContext = CreateReduceContext(reducer, job, GetTaskID(), rIter
				, reduceInputKeyCounter, reduceInputValueCounter, trackedRW, committer, reporter
				, comparator, keyClass, valueClass);
			try
			{
				reducer.Run(reducerContext);
			}
			finally
			{
				trackedRW.Close(reducerContext);
			}
		}

		private sealed class _RawKeyValueIterator_587 : RawKeyValueIterator
		{
			public _RawKeyValueIterator_587(RawKeyValueIterator rawIter, Task.TaskReporter reporter
				)
			{
				this.rawIter = rawIter;
				this.reporter = reporter;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Close()
			{
				rawIter.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public DataInputBuffer GetKey()
			{
				return rawIter.GetKey();
			}

			public Progress GetProgress()
			{
				return rawIter.GetProgress();
			}

			/// <exception cref="System.IO.IOException"/>
			public DataInputBuffer GetValue()
			{
				return rawIter.GetValue();
			}

			/// <exception cref="System.IO.IOException"/>
			public bool Next()
			{
				bool ret = rawIter.Next();
				reporter.SetProgress(rawIter.GetProgress().GetProgress());
				return ret;
			}

			private readonly RawKeyValueIterator rawIter;

			private readonly Task.TaskReporter reporter;
		}

		private void CloseQuietly<Outkey, Outvalue>(RecordWriter<OUTKEY, OUTVALUE> c, Reporter
			 r)
		{
			if (c != null)
			{
				try
				{
					c.Close(r);
				}
				catch (Exception e)
				{
					Log.Info("Exception in closing " + c, e);
				}
			}
		}
	}
}
