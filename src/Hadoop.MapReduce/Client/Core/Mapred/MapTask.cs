using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Map;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A Map task.</summary>
	public class MapTask : Task
	{
		/// <summary>The size of each record in the index file for the map-outputs.</summary>
		public const int MapOutputIndexRecordLength = 24;

		private JobSplit.TaskSplitIndex splitMetaInfo = new JobSplit.TaskSplitIndex();

		private const int ApproxHeaderLength = 150;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.MapTask
			).FullName);

		private Progress mapPhase;

		private Progress sortPhase;

		public MapTask()
			: base()
		{
			{
				// set phase for this task
				SetPhase(TaskStatus.Phase.Map);
				GetProgress().SetStatus("map");
			}
		}

		public MapTask(string jobFile, TaskAttemptID taskId, int partition, JobSplit.TaskSplitIndex
			 splitIndex, int numSlotsRequired)
			: base(jobFile, taskId, partition, numSlotsRequired)
		{
			{
				SetPhase(TaskStatus.Phase.Map);
				GetProgress().SetStatus("map");
			}
			this.splitMetaInfo = splitIndex;
		}

		public override bool IsMapTask()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void LocalizeConfiguration(JobConf conf)
		{
			base.LocalizeConfiguration(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			base.Write(@out);
			if (IsMapOrReduce())
			{
				splitMetaInfo.Write(@out);
				splitMetaInfo = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			base.ReadFields(@in);
			if (IsMapOrReduce())
			{
				splitMetaInfo.ReadFields(@in);
			}
		}

		/// <summary>
		/// This class wraps the user's record reader to update the counters and progress
		/// as records are read.
		/// </summary>
		/// <?/>
		/// <?/>
		internal class TrackedRecordReader<K, V> : RecordReader<K, V>
		{
			private RecordReader<K, V> rawIn;

			private Counters.Counter fileInputByteCounter;

			private Counters.Counter inputRecordCounter;

			private Task.TaskReporter reporter;

			private long bytesInPrev = -1;

			private long bytesInCurr = -1;

			private readonly IList<FileSystem.Statistics> fsStats;

			/// <exception cref="System.IO.IOException"/>
			internal TrackedRecordReader(MapTask _enclosing, Task.TaskReporter reporter, JobConf
				 job)
			{
				this._enclosing = _enclosing;
				this.inputRecordCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter.MapInputRecords
					));
				this.fileInputByteCounter = ((Counters.Counter)reporter.GetCounter(FileInputFormatCounter
					.BytesRead));
				this.reporter = reporter;
				IList<FileSystem.Statistics> matchedStats = null;
				if (this.reporter.GetInputSplit() is FileSplit)
				{
					matchedStats = Task.GetFsStatistics(((FileSplit)this.reporter.GetInputSplit()).GetPath
						(), job);
				}
				this.fsStats = matchedStats;
				this.bytesInPrev = this.GetInputBytes(this.fsStats);
				this.rawIn = job.GetInputFormat().GetRecordReader(reporter.GetInputSplit(), job, 
					reporter);
				this.bytesInCurr = this.GetInputBytes(this.fsStats);
				this.fileInputByteCounter.Increment(this.bytesInCurr - this.bytesInPrev);
			}

			public virtual K CreateKey()
			{
				return this.rawIn.CreateKey();
			}

			public virtual V CreateValue()
			{
				return this.rawIn.CreateValue();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(K key, V value)
			{
				lock (this)
				{
					bool ret = this.MoveToNext(key, value);
					if (ret)
					{
						this.IncrCounters();
					}
					return ret;
				}
			}

			protected internal virtual void IncrCounters()
			{
				this.inputRecordCounter.Increment(1);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual bool MoveToNext(K key, V value)
			{
				lock (this)
				{
					this.bytesInPrev = this.GetInputBytes(this.fsStats);
					bool ret = this.rawIn.Next(key, value);
					this.bytesInCurr = this.GetInputBytes(this.fsStats);
					this.fileInputByteCounter.Increment(this.bytesInCurr - this.bytesInPrev);
					this.reporter.SetProgress(this.GetProgress());
					return ret;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPos()
			{
				return this.rawIn.GetPos();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				this.bytesInPrev = this.GetInputBytes(this.fsStats);
				this.rawIn.Close();
				this.bytesInCurr = this.GetInputBytes(this.fsStats);
				this.fileInputByteCounter.Increment(this.bytesInCurr - this.bytesInPrev);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual float GetProgress()
			{
				return this.rawIn.GetProgress();
			}

			internal virtual Task.TaskReporter GetTaskReporter()
			{
				return this.reporter;
			}

			private long GetInputBytes(IList<FileSystem.Statistics> stats)
			{
				if (stats == null)
				{
					return 0;
				}
				long bytesRead = 0;
				foreach (FileSystem.Statistics stat in stats)
				{
					bytesRead = bytesRead + stat.GetBytesRead();
				}
				return bytesRead;
			}

			private readonly MapTask _enclosing;
		}

		/// <summary>
		/// This class skips the records based on the failed ranges from previous
		/// attempts.
		/// </summary>
		internal class SkippingRecordReader<K, V> : MapTask.TrackedRecordReader<K, V>
		{
			private SortedRanges.SkipRangeIterator skipIt;

			private SequenceFile.Writer skipWriter;

			private bool toWriteSkipRecs;

			private TaskUmbilicalProtocol umbilical;

			private Counters.Counter skipRecCounter;

			private long recIndex = -1;

			/// <exception cref="System.IO.IOException"/>
			internal SkippingRecordReader(MapTask _enclosing, TaskUmbilicalProtocol umbilical
				, Task.TaskReporter reporter, JobConf job)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.umbilical = umbilical;
				this.skipRecCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter.MapSkippedRecords
					));
				this.toWriteSkipRecs = this._enclosing.ToWriteSkipRecs() && SkipBadRecords.GetSkipOutputPath
					(this._enclosing.conf) != null;
				this.skipIt = this._enclosing.GetSkipRanges().SkipRangeIterator();
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Next(K key, V value)
			{
				lock (this)
				{
					if (!this.skipIt.HasNext())
					{
						MapTask.Log.Warn("Further records got skipped.");
						return false;
					}
					bool ret = this.MoveToNext(key, value);
					long nextRecIndex = this.skipIt.Next();
					long skip = 0;
					while (this.recIndex < nextRecIndex && ret)
					{
						if (this.toWriteSkipRecs)
						{
							this.WriteSkippedRec(key, value);
						}
						ret = this.MoveToNext(key, value);
						skip++;
					}
					//close the skip writer once all the ranges are skipped
					if (skip > 0 && this.skipIt.SkippedAllRanges() && this.skipWriter != null)
					{
						this.skipWriter.Close();
					}
					this.skipRecCounter.Increment(skip);
					this._enclosing.ReportNextRecordRange(this.umbilical, this.recIndex);
					if (ret)
					{
						this.IncrCounters();
					}
					return ret;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override bool MoveToNext(K key, V value)
			{
				lock (this)
				{
					this.recIndex++;
					return base.MoveToNext(key, value);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void WriteSkippedRec(K key, V value)
			{
				if (this.skipWriter == null)
				{
					Path skipDir = SkipBadRecords.GetSkipOutputPath(this._enclosing.conf);
					Path skipFile = new Path(skipDir, this._enclosing.GetTaskID().ToString());
					this.skipWriter = SequenceFile.CreateWriter(skipFile.GetFileSystem(this._enclosing
						.conf), this._enclosing.conf, skipFile, (Type)this.CreateKey().GetType(), (Type)
						this.CreateValue().GetType(), SequenceFile.CompressionType.Block, this.GetTaskReporter
						());
				}
				this.skipWriter.Append(key, value);
			}

			private readonly MapTask _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		public override void Run(JobConf job, TaskUmbilicalProtocol umbilical)
		{
			this.umbilical = umbilical;
			if (IsMapTask())
			{
				// If there are no reducers then there won't be any sort. Hence the map 
				// phase will govern the entire attempt's progress.
				if (conf.GetNumReduceTasks() == 0)
				{
					mapPhase = GetProgress().AddPhase("map", 1.0f);
				}
				else
				{
					// If there are reducers then the entire attempt's progress will be 
					// split between the map phase (67%) and the sort phase (33%).
					mapPhase = GetProgress().AddPhase("map", 0.667f);
					sortPhase = GetProgress().AddPhase("sort", 0.333f);
				}
			}
			Task.TaskReporter reporter = StartReporter(umbilical);
			bool useNewApi = job.GetUseNewMapper();
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
			if (useNewApi)
			{
				RunNewMapper(job, splitMetaInfo, umbilical, reporter);
			}
			else
			{
				RunOldMapper(job, splitMetaInfo, umbilical, reporter);
			}
			Done(umbilical, reporter);
		}

		public virtual Progress GetSortPhase()
		{
			return sortPhase;
		}

		/// <exception cref="System.IO.IOException"/>
		private T GetSplitDetails<T>(Path file, long offset)
		{
			FileSystem fs = file.GetFileSystem(conf);
			FSDataInputStream inFile = fs.Open(file);
			inFile.Seek(offset);
			string className = StringInterner.WeakIntern(Text.ReadString(inFile));
			Type cls;
			try
			{
				cls = (Type)conf.GetClassByName(className);
			}
			catch (TypeLoadException ce)
			{
				IOException wrap = new IOException("Split class " + className + " not found");
				Sharpen.Extensions.InitCause(wrap, ce);
				throw wrap;
			}
			SerializationFactory factory = new SerializationFactory(conf);
			Deserializer<T> deserializer = (Deserializer<T>)factory.GetDeserializer(cls);
			deserializer.Open(inFile);
			T split = deserializer.Deserialize(null);
			long pos = inFile.GetPos();
			GetCounters().FindCounter(TaskCounter.SplitRawBytes).Increment(pos - offset);
			inFile.Close();
			return split;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		private MapOutputCollector<KEY, VALUE> CreateSortingCollector<Key, Value>(JobConf
			 job, Task.TaskReporter reporter)
		{
			MapOutputCollector.Context context = new MapOutputCollector.Context(this, job, reporter
				);
			Type[] collectorClasses = job.GetClasses(JobContext.MapOutputCollectorClassAttr, 
				typeof(MapTask.MapOutputBuffer));
			int remainingCollectors = collectorClasses.Length;
			Exception lastException = null;
			foreach (Type clazz in collectorClasses)
			{
				try
				{
					if (!typeof(MapOutputCollector).IsAssignableFrom(clazz))
					{
						throw new IOException("Invalid output collector class: " + clazz.FullName + " (does not implement MapOutputCollector)"
							);
					}
					//HM hack added to CSharpBuilder.genericRuntimeTypeIdiomType to get around issue with generic type arg length 0
					//it was throwing an index oob exception
					Type subclazz = clazz.AsSubclass<Type>();
					Log.Debug("Trying map output collector class: " + subclazz.FullName);
					MapOutputCollector<KEY, VALUE> collector = ReflectionUtils.NewInstance(subclazz, 
						job);
					collector.Init(context);
					Log.Info("Map output collector class = " + collector.GetType().FullName);
					return collector;
				}
				catch (Exception e)
				{
					string msg = "Unable to initialize MapOutputCollector " + clazz.FullName;
					if (--remainingCollectors > 0)
					{
						msg += " (" + remainingCollectors + " more collector(s) to try)";
					}
					lastException = e;
					Log.Warn(msg, e);
				}
			}
			throw new IOException("Initialization of all the collectors failed. " + "Error in last collector was :"
				 + lastException.Message, lastException);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		private void RunOldMapper<Inkey, Invalue, Outkey, Outvalue>(JobConf job, JobSplit.TaskSplitIndex
			 splitIndex, TaskUmbilicalProtocol umbilical, Task.TaskReporter reporter)
		{
			InputSplit inputSplit = GetSplitDetails(new Path(splitIndex.GetSplitLocation()), 
				splitIndex.GetStartOffset());
			UpdateJobWithSplit(job, inputSplit);
			reporter.SetInputSplit(inputSplit);
			RecordReader<INKEY, INVALUE> @in = IsSkipping() ? new MapTask.SkippingRecordReader
				<INKEY, INVALUE>(this, umbilical, reporter, job) : new MapTask.TrackedRecordReader
				<INKEY, INVALUE>(this, reporter, job);
			job.SetBoolean(JobContext.SkipRecords, IsSkipping());
			int numReduceTasks = conf.GetNumReduceTasks();
			Log.Info("numReduceTasks: " + numReduceTasks);
			MapOutputCollector<OUTKEY, OUTVALUE> collector = null;
			if (numReduceTasks > 0)
			{
				collector = CreateSortingCollector(job, reporter);
			}
			else
			{
				collector = new MapTask.DirectMapOutputCollector<OUTKEY, OUTVALUE>(this);
				MapOutputCollector.Context context = new MapOutputCollector.Context(this, job, reporter
					);
				collector.Init(context);
			}
			MapRunnable<INKEY, INVALUE, OUTKEY, OUTVALUE> runner = ReflectionUtils.NewInstance
				(job.GetMapRunnerClass(), job);
			try
			{
				runner.Run(@in, new MapTask.OldOutputCollector(collector, conf), reporter);
				mapPhase.Complete();
				// start the sort phase only if there are reducers
				if (numReduceTasks > 0)
				{
					SetPhase(TaskStatus.Phase.Sort);
				}
				StatusUpdate(umbilical);
				collector.Flush();
				@in.Close();
				@in = null;
				collector.Close();
				collector = null;
			}
			finally
			{
				CloseQuietly(@in);
				CloseQuietly(collector);
			}
		}

		/// <summary>Update the job with details about the file split</summary>
		/// <param name="job">the job configuration to update</param>
		/// <param name="inputSplit">the file split</param>
		private void UpdateJobWithSplit(JobConf job, InputSplit inputSplit)
		{
			if (inputSplit is FileSplit)
			{
				FileSplit fileSplit = (FileSplit)inputSplit;
				job.Set(JobContext.MapInputFile, fileSplit.GetPath().ToString());
				job.SetLong(JobContext.MapInputStart, fileSplit.GetStart());
				job.SetLong(JobContext.MapInputPath, fileSplit.GetLength());
			}
			Log.Info("Processing split: " + inputSplit);
		}

		internal class NewTrackingRecordReader<K, V> : RecordReader<K, V>
		{
			private readonly RecordReader<K, V> real;

			private readonly Counter inputRecordCounter;

			private readonly Counter fileInputByteCounter;

			private readonly Task.TaskReporter reporter;

			private readonly IList<FileSystem.Statistics> fsStats;

			/// <exception cref="System.Exception"/>
			/// <exception cref="System.IO.IOException"/>
			internal NewTrackingRecordReader(InputSplit split, InputFormat<K, V> inputFormat, 
				Task.TaskReporter reporter, TaskAttemptContext taskContext)
			{
				this.reporter = reporter;
				this.inputRecordCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter.MapInputRecords
					));
				this.fileInputByteCounter = ((Counters.Counter)reporter.GetCounter(FileInputFormatCounter
					.BytesRead));
				IList<FileSystem.Statistics> matchedStats = null;
				if (split is FileSplit)
				{
					matchedStats = GetFsStatistics(((FileSplit)split).GetPath(), taskContext.GetConfiguration
						());
				}
				fsStats = matchedStats;
				long bytesInPrev = GetInputBytes(fsStats);
				this.real = inputFormat.CreateRecordReader(split, taskContext);
				long bytesInCurr = GetInputBytes(fsStats);
				fileInputByteCounter.Increment(bytesInCurr - bytesInPrev);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				long bytesInPrev = GetInputBytes(fsStats);
				real.Close();
				long bytesInCurr = GetInputBytes(fsStats);
				fileInputByteCounter.Increment(bytesInCurr - bytesInPrev);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override K GetCurrentKey()
			{
				return real.GetCurrentKey();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override V GetCurrentValue()
			{
				return real.GetCurrentValue();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override float GetProgress()
			{
				return real.GetProgress();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Initialize(InputSplit split, TaskAttemptContext context)
			{
				long bytesInPrev = GetInputBytes(fsStats);
				real.Initialize(split, context);
				long bytesInCurr = GetInputBytes(fsStats);
				fileInputByteCounter.Increment(bytesInCurr - bytesInPrev);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool NextKeyValue()
			{
				long bytesInPrev = GetInputBytes(fsStats);
				bool result = real.NextKeyValue();
				long bytesInCurr = GetInputBytes(fsStats);
				if (result)
				{
					inputRecordCounter.Increment(1);
				}
				fileInputByteCounter.Increment(bytesInCurr - bytesInPrev);
				reporter.SetProgress(GetProgress());
				return result;
			}

			private long GetInputBytes(IList<FileSystem.Statistics> stats)
			{
				if (stats == null)
				{
					return 0;
				}
				long bytesRead = 0;
				foreach (FileSystem.Statistics stat in stats)
				{
					bytesRead = bytesRead + stat.GetBytesRead();
				}
				return bytesRead;
			}
		}

		/// <summary>
		/// Since the mapred and mapreduce Partitioners don't share a common interface
		/// (JobConfigurable is deprecated and a subtype of mapred.Partitioner), the
		/// partitioner lives in Old/NewOutputCollector.
		/// </summary>
		/// <remarks>
		/// Since the mapred and mapreduce Partitioners don't share a common interface
		/// (JobConfigurable is deprecated and a subtype of mapred.Partitioner), the
		/// partitioner lives in Old/NewOutputCollector. Note that, for map-only jobs,
		/// the configured partitioner should not be called. It's common for
		/// partitioners to compute a result mod numReduces, which causes a div0 error
		/// </remarks>
		private class OldOutputCollector<K, V> : OutputCollector<K, V>
		{
			private readonly Partitioner<K, V> partitioner;

			private readonly MapOutputCollector<K, V> collector;

			private readonly int numPartitions;

			internal OldOutputCollector(MapOutputCollector<K, V> collector, JobConf conf)
			{
				numPartitions = conf.GetNumReduceTasks();
				if (numPartitions > 1)
				{
					partitioner = (Partitioner<K, V>)ReflectionUtils.NewInstance(conf.GetPartitionerClass
						(), conf);
				}
				else
				{
					partitioner = new _Partitioner_597();
				}
				this.collector = collector;
			}

			private sealed class _Partitioner_597 : Partitioner<K, V>
			{
				public _Partitioner_597()
				{
				}

				public void Configure(JobConf job)
				{
				}

				public int GetPartition(K key, V value, int numPartitions)
				{
					return numPartitions - 1;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Collect(K key, V value)
			{
				try
				{
					collector.Collect(key, value, partitioner.GetPartition(key, value, numPartitions)
						);
				}
				catch (Exception ie)
				{
					Sharpen.Thread.CurrentThread().Interrupt();
					throw new IOException("interrupt exception", ie);
				}
			}
		}

		private class NewDirectOutputCollector<K, V> : RecordWriter<K, V>
		{
			private readonly RecordWriter @out;

			private readonly Task.TaskReporter reporter;

			private readonly Counters.Counter mapOutputRecordCounter;

			private readonly Counters.Counter fileOutputByteCounter;

			private readonly IList<FileSystem.Statistics> fsStats;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.TypeLoadException"/>
			/// <exception cref="System.Exception"/>
			internal NewDirectOutputCollector(MapTask _enclosing, MRJobConfig jobContext, JobConf
				 job, TaskUmbilicalProtocol umbilical, Task.TaskReporter reporter)
			{
				this._enclosing = _enclosing;
				this.reporter = reporter;
				this.mapOutputRecordCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter.
					MapOutputRecords));
				this.fileOutputByteCounter = ((Counters.Counter)reporter.GetCounter(FileOutputFormatCounter
					.BytesWritten));
				IList<FileSystem.Statistics> matchedStats = null;
				if (this._enclosing.outputFormat is FileOutputFormat)
				{
					matchedStats = Task.GetFsStatistics(FileOutputFormat.GetOutputPath(this._enclosing
						.taskContext), this._enclosing.taskContext.GetConfiguration());
				}
				this.fsStats = matchedStats;
				long bytesOutPrev = this.GetOutputBytes(this.fsStats);
				this.@out = this._enclosing.outputFormat.GetRecordWriter(this._enclosing.taskContext
					);
				long bytesOutCurr = this.GetOutputBytes(this.fsStats);
				this.fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(K key, V value)
			{
				this.reporter.Progress();
				long bytesOutPrev = this.GetOutputBytes(this.fsStats);
				this.@out.Write(key, value);
				long bytesOutCurr = this.GetOutputBytes(this.fsStats);
				this.fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
				this.mapOutputRecordCounter.Increment(1);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Close(TaskAttemptContext context)
			{
				this.reporter.Progress();
				if (this.@out != null)
				{
					long bytesOutPrev = this.GetOutputBytes(this.fsStats);
					this.@out.Close(context);
					long bytesOutCurr = this.GetOutputBytes(this.fsStats);
					this.fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
				}
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

			private readonly MapTask _enclosing;
		}

		private class NewOutputCollector<K, V> : RecordWriter<K, V>
		{
			private readonly MapOutputCollector<K, V> collector;

			private readonly Partitioner<K, V> partitioner;

			private readonly int partitions;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.TypeLoadException"/>
			internal NewOutputCollector(MapTask _enclosing, JobContext jobContext, JobConf job
				, TaskUmbilicalProtocol umbilical, Task.TaskReporter reporter)
			{
				this._enclosing = _enclosing;
				this.collector = this._enclosing.CreateSortingCollector(job, reporter);
				this.partitions = jobContext.GetNumReduceTasks();
				if (this.partitions > 1)
				{
					this.partitioner = (Partitioner<K, V>)ReflectionUtils.NewInstance(jobContext.GetPartitionerClass
						(), job);
				}
				else
				{
					this.partitioner = new _Partitioner_706(this);
				}
			}

			private sealed class _Partitioner_706 : Partitioner<K, V>
			{
				public _Partitioner_706(NewOutputCollector<K, V> _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override int GetPartition(K key, V value, int numPartitions)
				{
					return this._enclosing.partitions - 1;
				}

				private readonly NewOutputCollector<K, V> _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(K key, V value)
			{
				this.collector.Collect(key, value, this.partitioner.GetPartition(key, value, this
					.partitions));
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Close(TaskAttemptContext context)
			{
				try
				{
					this.collector.Flush();
				}
				catch (TypeLoadException cnf)
				{
					throw new IOException("can't find class ", cnf);
				}
				this.collector.Close();
			}

			private readonly MapTask _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		private void RunNewMapper<Inkey, Invalue, Outkey, Outvalue>(JobConf job, JobSplit.TaskSplitIndex
			 splitIndex, TaskUmbilicalProtocol umbilical, Task.TaskReporter reporter)
		{
			// make a task context so we can get the classes
			TaskAttemptContext taskContext = new TaskAttemptContextImpl(job, GetTaskID(), reporter
				);
			// make a mapper
			Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper = (Mapper<INKEY, INVALUE, OUTKEY, 
				OUTVALUE>)ReflectionUtils.NewInstance(taskContext.GetMapperClass(), job);
			// make the input format
			InputFormat<INKEY, INVALUE> inputFormat = (InputFormat<INKEY, INVALUE>)ReflectionUtils
				.NewInstance(taskContext.GetInputFormatClass(), job);
			// rebuild the input split
			InputSplit split = null;
			split = GetSplitDetails(new Path(splitIndex.GetSplitLocation()), splitIndex.GetStartOffset
				());
			Log.Info("Processing split: " + split);
			RecordReader<INKEY, INVALUE> input = new MapTask.NewTrackingRecordReader<INKEY, INVALUE
				>(split, inputFormat, reporter, taskContext);
			job.SetBoolean(JobContext.SkipRecords, IsSkipping());
			RecordWriter output = null;
			// get an output object
			if (job.GetNumReduceTasks() == 0)
			{
				output = new MapTask.NewDirectOutputCollector(this, taskContext, job, umbilical, 
					reporter);
			}
			else
			{
				output = new MapTask.NewOutputCollector(this, taskContext, job, umbilical, reporter
					);
			}
			MapContext<INKEY, INVALUE, OUTKEY, OUTVALUE> mapContext = new MapContextImpl<INKEY
				, INVALUE, OUTKEY, OUTVALUE>(job, GetTaskID(), input, output, committer, reporter
				, split);
			Mapper.Context mapperContext = new WrappedMapper<INKEY, INVALUE, OUTKEY, OUTVALUE
				>().GetMapContext(mapContext);
			try
			{
				input.Initialize(split, mapperContext);
				mapper.Run(mapperContext);
				mapPhase.Complete();
				SetPhase(TaskStatus.Phase.Sort);
				StatusUpdate(umbilical);
				input.Close();
				input = null;
				output.Close(mapperContext);
				output = null;
			}
			finally
			{
				CloseQuietly(input);
				CloseQuietly(output, mapperContext);
			}
		}

		internal class DirectMapOutputCollector<K, V> : MapOutputCollector<K, V>
		{
			private RecordWriter<K, V> @out = null;

			private Task.TaskReporter reporter = null;

			private Counters.Counter mapOutputRecordCounter;

			private Counters.Counter fileOutputByteCounter;

			private IList<FileSystem.Statistics> fsStats;

			public DirectMapOutputCollector(MapTask _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.TypeLoadException"/>
			public override void Init(MapOutputCollector.Context context)
			{
				this.reporter = context.GetReporter();
				JobConf job = context.GetJobConf();
				string finalName = Org.Apache.Hadoop.Mapred.Task.GetOutputName(this._enclosing.GetPartition
					());
				FileSystem fs = FileSystem.Get(job);
				OutputFormat<K, V> outputFormat = job.GetOutputFormat();
				this.mapOutputRecordCounter = ((Counters.Counter)this.reporter.GetCounter(TaskCounter
					.MapOutputRecords));
				this.fileOutputByteCounter = ((Counters.Counter)this.reporter.GetCounter(FileOutputFormatCounter
					.BytesWritten));
				IList<FileSystem.Statistics> matchedStats = null;
				if (outputFormat is FileOutputFormat)
				{
					matchedStats = Org.Apache.Hadoop.Mapred.Task.GetFsStatistics(FileOutputFormat.GetOutputPath
						(job), job);
				}
				this.fsStats = matchedStats;
				long bytesOutPrev = this.GetOutputBytes(this.fsStats);
				this.@out = job.GetOutputFormat().GetRecordWriter(fs, job, finalName, this.reporter
					);
				long bytesOutCurr = this.GetOutputBytes(this.fsStats);
				this.fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				if (this.@out != null)
				{
					long bytesOutPrev = this.GetOutputBytes(this.fsStats);
					this.@out.Close(this.reporter);
					long bytesOutCurr = this.GetOutputBytes(this.fsStats);
					this.fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			/// <exception cref="System.TypeLoadException"/>
			public override void Flush()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Collect(K key, V value, int partition)
			{
				this.reporter.Progress();
				long bytesOutPrev = this.GetOutputBytes(this.fsStats);
				this.@out.Write(key, value);
				long bytesOutCurr = this.GetOutputBytes(this.fsStats);
				this.fileOutputByteCounter.Increment(bytesOutCurr - bytesOutPrev);
				this.mapOutputRecordCounter.Increment(1);
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

			private readonly MapTask _enclosing;
		}

		public class MapOutputBuffer<K, V> : MapOutputCollector<K, V>, IndexedSortable
		{
			private int partitions;

			private JobConf job;

			private Task.TaskReporter reporter;

			private Type keyClass;

			private Type valClass;

			private RawComparator<K> comparator;

			private SerializationFactory serializationFactory;

			private Org.Apache.Hadoop.IO.Serializer.Serializer<K> keySerializer;

			private Org.Apache.Hadoop.IO.Serializer.Serializer<V> valSerializer;

			private Task.CombinerRunner<K, V> combinerRunner;

			private Task.CombineOutputCollector<K, V> combineCollector;

			private CompressionCodec codec;

			private IntBuffer kvmeta;

			internal int kvstart;

			internal int kvend;

			internal int kvindex;

			internal int equator;

			internal int bufstart;

			internal int bufend;

			internal int bufmark;

			internal int bufindex;

			internal int bufvoid;

			internal byte[] kvbuffer;

			private readonly byte[] b0 = new byte[0];

			private const int Valstart = 0;

			private const int Keystart = 1;

			private const int Partition = 2;

			private const int Vallen = 3;

			private const int Nmeta = 4;

			private const int Metasize = Nmeta * 4;

			private int maxRec;

			private int softLimit;

			internal bool spillInProgress;

			internal int bufferRemaining;

			internal volatile Exception sortSpillException = null;

			internal int numSpills = 0;

			private int minSpillsForCombine;

			private IndexedSorter sorter;

			internal readonly ReentrantLock spillLock = new ReentrantLock();

			internal readonly Condition spillDone = spillLock.NewCondition();

			internal readonly Condition spillReady = spillLock.NewCondition();

			internal readonly MapTask.MapOutputBuffer.BlockingBuffer bb;

			internal volatile bool spillThreadRunning = false;

			internal readonly MapTask.MapOutputBuffer.SpillThread spillThread;

			private FileSystem rfs;

			private Counters.Counter mapOutputByteCounter;

			private Counters.Counter mapOutputRecordCounter;

			private Counters.Counter fileOutputByteCounter;

			internal readonly AList<SpillRecord> indexCacheList = new AList<SpillRecord>();

			private int totalIndexCacheMemory;

			private int indexCacheMemoryLimit;

			private const int IndexCacheMemoryLimitDefault = 1024 * 1024;

			private MapTask mapTask;

			private MapOutputFile mapOutputFile;

			private Progress sortPhase;

			private Counters.Counter spilledRecordsCounter;

			public MapOutputBuffer()
			{
				bb = new MapTask.MapOutputBuffer.BlockingBuffer(this);
				spillThread = new MapTask.MapOutputBuffer.SpillThread(this);
			}

			// Compression for map-outputs
			// k/v accounting
			// metadata overlay on backing store
			// marks origin of spill metadata
			// marks end of spill metadata
			// marks end of fully serialized records
			// marks origin of meta/serialization
			// marks beginning of spill
			// marks beginning of collectable
			// marks end of record
			// marks end of collected
			// marks the point where we should stop
			// reading at the end of the buffer
			// main output buffer
			// val offset in acct
			// key offset in acct
			// partition offset in acct
			// length of value
			// num meta ints
			// size in bytes
			// spill accounting
			// Counters
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.TypeLoadException"/>
			public virtual void Init(MapOutputCollector.Context context)
			{
				job = context.GetJobConf();
				reporter = context.GetReporter();
				mapTask = context.GetMapTask();
				mapOutputFile = mapTask.GetMapOutputFile();
				sortPhase = mapTask.GetSortPhase();
				spilledRecordsCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter.SpilledRecords
					));
				partitions = job.GetNumReduceTasks();
				rfs = ((LocalFileSystem)FileSystem.GetLocal(job)).GetRaw();
				//sanity checks
				float spillper = job.GetFloat(JobContext.MapSortSpillPercent, (float)0.8);
				int sortmb = job.GetInt(JobContext.IoSortMb, 100);
				indexCacheMemoryLimit = job.GetInt(JobContext.IndexCacheMemoryLimit, IndexCacheMemoryLimitDefault
					);
				if (spillper > (float)1.0 || spillper <= (float)0.0)
				{
					throw new IOException("Invalid \"" + JobContext.MapSortSpillPercent + "\": " + spillper
						);
				}
				if ((sortmb & unchecked((int)(0x7FF))) != sortmb)
				{
					throw new IOException("Invalid \"" + JobContext.IoSortMb + "\": " + sortmb);
				}
				sorter = ReflectionUtils.NewInstance(job.GetClass<IndexedSorter>("map.sort.class"
					, typeof(QuickSort)), job);
				// buffers and accounting
				int maxMemUsage = sortmb << 20;
				maxMemUsage -= maxMemUsage % Metasize;
				kvbuffer = new byte[maxMemUsage];
				bufvoid = kvbuffer.Length;
				kvmeta = ByteBuffer.Wrap(kvbuffer).Order(ByteOrder.NativeOrder()).AsIntBuffer();
				SetEquator(0);
				bufstart = bufend = bufindex = equator;
				kvstart = kvend = kvindex;
				maxRec = kvmeta.Capacity() / Nmeta;
				softLimit = (int)(kvbuffer.Length * spillper);
				bufferRemaining = softLimit;
				Log.Info(JobContext.IoSortMb + ": " + sortmb);
				Log.Info("soft limit at " + softLimit);
				Log.Info("bufstart = " + bufstart + "; bufvoid = " + bufvoid);
				Log.Info("kvstart = " + kvstart + "; length = " + maxRec);
				// k/v serialization
				comparator = job.GetOutputKeyComparator();
				keyClass = (Type)job.GetMapOutputKeyClass();
				valClass = (Type)job.GetMapOutputValueClass();
				serializationFactory = new SerializationFactory(job);
				keySerializer = serializationFactory.GetSerializer(keyClass);
				keySerializer.Open(bb);
				valSerializer = serializationFactory.GetSerializer(valClass);
				valSerializer.Open(bb);
				// output counters
				mapOutputByteCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter.MapOutputBytes
					));
				mapOutputRecordCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter.MapOutputRecords
					));
				fileOutputByteCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter.MapOutputMaterializedBytes
					));
				// compression
				if (job.GetCompressMapOutput())
				{
					Type codecClass = job.GetMapOutputCompressorClass(typeof(DefaultCodec));
					codec = ReflectionUtils.NewInstance(codecClass, job);
				}
				else
				{
					codec = null;
				}
				// combiner
				Counters.Counter combineInputCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter
					.CombineInputRecords));
				combinerRunner = Task.CombinerRunner.Create(job, GetTaskID(), combineInputCounter
					, reporter, null);
				if (combinerRunner != null)
				{
					Counters.Counter combineOutputCounter = ((Counters.Counter)reporter.GetCounter(TaskCounter
						.CombineOutputRecords));
					combineCollector = new Task.CombineOutputCollector<K, V>(combineOutputCounter, reporter
						, job);
				}
				else
				{
					combineCollector = null;
				}
				spillInProgress = false;
				minSpillsForCombine = job.GetInt(JobContext.MapCombineMinSpills, 3);
				spillThread.SetDaemon(true);
				spillThread.SetName("SpillThread");
				spillLock.Lock();
				try
				{
					spillThread.Start();
					while (!spillThreadRunning)
					{
						spillDone.Await();
					}
				}
				catch (Exception e)
				{
					throw new IOException("Spill thread failed to initialize", e);
				}
				finally
				{
					spillLock.Unlock();
				}
				if (sortSpillException != null)
				{
					throw new IOException("Spill thread failed to initialize", sortSpillException);
				}
			}

			/// <summary>Serialize the key, value to intermediate storage.</summary>
			/// <remarks>
			/// Serialize the key, value to intermediate storage.
			/// When this method returns, kvindex must refer to sufficient unused
			/// storage to store one METADATA.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Collect(K key, V value, int partition)
			{
				lock (this)
				{
					reporter.Progress();
					if (key.GetType() != keyClass)
					{
						throw new IOException("Type mismatch in key from map: expected " + keyClass.FullName
							 + ", received " + key.GetType().FullName);
					}
					if (value.GetType() != valClass)
					{
						throw new IOException("Type mismatch in value from map: expected " + valClass.FullName
							 + ", received " + value.GetType().FullName);
					}
					if (partition < 0 || partition >= partitions)
					{
						throw new IOException("Illegal partition for " + key + " (" + partition + ")");
					}
					CheckSpillException();
					bufferRemaining -= Metasize;
					if (bufferRemaining <= 0)
					{
						// start spill if the thread is not running and the soft limit has been
						// reached
						spillLock.Lock();
						try
						{
							do
							{
								if (!spillInProgress)
								{
									int kvbidx = 4 * kvindex;
									int kvbend = 4 * kvend;
									// serialized, unspilled bytes always lie between kvindex and
									// bufindex, crossing the equator. Note that any void space
									// created by a reset must be included in "used" bytes
									int bUsed = DistanceTo(kvbidx, bufindex);
									bool bufsoftlimit = bUsed >= softLimit;
									if ((kvbend + Metasize) % kvbuffer.Length != equator - (equator % Metasize))
									{
										// spill finished, reclaim space
										ResetSpill();
										bufferRemaining = Math.Min(DistanceTo(bufindex, kvbidx) - 2 * Metasize, softLimit
											 - bUsed) - Metasize;
										continue;
									}
									else
									{
										if (bufsoftlimit && kvindex != kvend)
										{
											// spill records, if any collected; check latter, as it may
											// be possible for metadata alignment to hit spill pcnt
											StartSpill();
											int avgRec = (int)(mapOutputByteCounter.GetCounter() / mapOutputRecordCounter.GetCounter
												());
											// leave at least half the split buffer for serialization data
											// ensure that kvindex >= bufindex
											int distkvi = DistanceTo(bufindex, kvbidx);
											int newPos = (bufindex + Math.Max(2 * Metasize - 1, Math.Min(distkvi / 2, distkvi
												 / (Metasize + avgRec) * Metasize))) % kvbuffer.Length;
											SetEquator(newPos);
											bufmark = bufindex = newPos;
											int serBound = 4 * kvend;
											// bytes remaining before the lock must be held and limits
											// checked is the minimum of three arcs: the metadata space, the
											// serialization space, and the soft limit
											bufferRemaining = Math.Min(DistanceTo(bufend, newPos), Math.Min(DistanceTo(newPos
												, serBound), softLimit)) - 2 * Metasize;
										}
									}
								}
							}
							while (false);
						}
						finally
						{
							// metadata max
							// serialization max
							// soft limit
							spillLock.Unlock();
						}
					}
					try
					{
						// serialize key bytes into buffer
						int keystart = bufindex;
						keySerializer.Serialize(key);
						if (bufindex < keystart)
						{
							// wrapped the key; must make contiguous
							bb.ShiftBufferedKey();
							keystart = 0;
						}
						// serialize value bytes into buffer
						int valstart = bufindex;
						valSerializer.Serialize(value);
						// It's possible for records to have zero length, i.e. the serializer
						// will perform no writes. To ensure that the boundary conditions are
						// checked and that the kvindex invariant is maintained, perform a
						// zero-length write into the buffer. The logic monitoring this could be
						// moved into collect, but this is cleaner and inexpensive. For now, it
						// is acceptable.
						bb.Write(b0, 0, 0);
						// the record must be marked after the preceding write, as the metadata
						// for this record are not yet written
						int valend = bb.MarkRecord();
						mapOutputRecordCounter.Increment(1);
						mapOutputByteCounter.Increment(DistanceTo(keystart, valend, bufvoid));
						// write accounting info
						kvmeta.Put(kvindex + Partition, partition);
						kvmeta.Put(kvindex + Keystart, keystart);
						kvmeta.Put(kvindex + Valstart, valstart);
						kvmeta.Put(kvindex + Vallen, DistanceTo(valstart, valend));
						// advance kvindex
						kvindex = (kvindex - Nmeta + kvmeta.Capacity()) % kvmeta.Capacity();
					}
					catch (MapTask.MapBufferTooSmallException e)
					{
						Log.Info("Record too large for in-memory buffer: " + e.Message);
						SpillSingleRecord(key, value, partition);
						mapOutputRecordCounter.Increment(1);
						return;
					}
				}
			}

			private TaskAttemptID GetTaskID()
			{
				return mapTask.GetTaskID();
			}

			/// <summary>Set the point from which meta and serialization data expand.</summary>
			/// <remarks>
			/// Set the point from which meta and serialization data expand. The meta
			/// indices are aligned with the buffer, so metadata never spans the ends of
			/// the circular buffer.
			/// </remarks>
			private void SetEquator(int pos)
			{
				equator = pos;
				// set index prior to first entry, aligned at meta boundary
				int aligned = pos - (pos % Metasize);
				// Cast one of the operands to long to avoid integer overflow
				kvindex = (int)(((long)aligned - Metasize + kvbuffer.Length) % kvbuffer.Length) /
					 4;
				Log.Info("(EQUATOR) " + pos + " kvi " + kvindex + "(" + (kvindex * 4) + ")");
			}

			/// <summary>
			/// The spill is complete, so set the buffer and meta indices to be equal to
			/// the new equator to free space for continuing collection.
			/// </summary>
			/// <remarks>
			/// The spill is complete, so set the buffer and meta indices to be equal to
			/// the new equator to free space for continuing collection. Note that when
			/// kvindex == kvend == kvstart, the buffer is empty.
			/// </remarks>
			private void ResetSpill()
			{
				int e = equator;
				bufstart = bufend = e;
				int aligned = e - (e % Metasize);
				// set start/end to point to first meta record
				// Cast one of the operands to long to avoid integer overflow
				kvstart = kvend = (int)(((long)aligned - Metasize + kvbuffer.Length) % kvbuffer.Length
					) / 4;
				Log.Info("(RESET) equator " + e + " kv " + kvstart + "(" + (kvstart * 4) + ")" + 
					" kvi " + kvindex + "(" + (kvindex * 4) + ")");
			}

			/// <summary>
			/// Compute the distance in bytes between two indices in the serialization
			/// buffer.
			/// </summary>
			/// <seealso cref="MapOutputBuffer{K, V}.DistanceTo(int, int, int)"/>
			internal int DistanceTo(int i, int j)
			{
				return DistanceTo(i, j, kvbuffer.Length);
			}

			/// <summary>
			/// Compute the distance between two indices in the circular buffer given the
			/// max distance.
			/// </summary>
			internal virtual int DistanceTo(int i, int j, int mod)
			{
				return i <= j ? j - i : mod - i + j;
			}

			/// <summary>
			/// For the given meta position, return the offset into the int-sized
			/// kvmeta buffer.
			/// </summary>
			internal virtual int OffsetFor(int metapos)
			{
				return metapos * Nmeta;
			}

			/// <summary>Compare logical range, st i, j MOD offset capacity.</summary>
			/// <remarks>
			/// Compare logical range, st i, j MOD offset capacity.
			/// Compare by partition, then by key.
			/// </remarks>
			/// <seealso cref="Org.Apache.Hadoop.Util.IndexedSortable.Compare(int, int)"/>
			public virtual int Compare(int mi, int mj)
			{
				int kvi = OffsetFor(mi % maxRec);
				int kvj = OffsetFor(mj % maxRec);
				int kvip = kvmeta.Get(kvi + Partition);
				int kvjp = kvmeta.Get(kvj + Partition);
				// sort by partition
				if (kvip != kvjp)
				{
					return kvip - kvjp;
				}
				// sort by key
				return comparator.Compare(kvbuffer, kvmeta.Get(kvi + Keystart), kvmeta.Get(kvi + 
					Valstart) - kvmeta.Get(kvi + Keystart), kvbuffer, kvmeta.Get(kvj + Keystart), kvmeta
					.Get(kvj + Valstart) - kvmeta.Get(kvj + Keystart));
			}

			internal readonly byte[] MetaBufferTmp = new byte[Metasize];

			/// <summary>Swap metadata for items i, j</summary>
			/// <seealso cref="Org.Apache.Hadoop.Util.IndexedSortable.Swap(int, int)"/>
			public virtual void Swap(int mi, int mj)
			{
				int iOff = (mi % maxRec) * Metasize;
				int jOff = (mj % maxRec) * Metasize;
				System.Array.Copy(kvbuffer, iOff, MetaBufferTmp, 0, Metasize);
				System.Array.Copy(kvbuffer, jOff, kvbuffer, iOff, Metasize);
				System.Array.Copy(MetaBufferTmp, 0, kvbuffer, jOff, Metasize);
			}

			/// <summary>Inner class managing the spill of serialized records to disk.</summary>
			protected internal class BlockingBuffer : DataOutputStream
			{
				public BlockingBuffer(MapOutputBuffer<K, V> _enclosing)
					: base(new MapTask.MapOutputBuffer.Buffer(this))
				{
					this._enclosing = _enclosing;
				}

				/// <summary>Mark end of record.</summary>
				/// <remarks>
				/// Mark end of record. Note that this is required if the buffer is to
				/// cut the spill in the proper place.
				/// </remarks>
				public virtual int MarkRecord()
				{
					this._enclosing.bufmark = this._enclosing.bufindex;
					return this._enclosing.bufindex;
				}

				/// <summary>
				/// Set position from last mark to end of writable buffer, then rewrite
				/// the data between last mark and kvindex.
				/// </summary>
				/// <remarks>
				/// Set position from last mark to end of writable buffer, then rewrite
				/// the data between last mark and kvindex.
				/// This handles a special case where the key wraps around the buffer.
				/// If the key is to be passed to a RawComparator, then it must be
				/// contiguous in the buffer. This recopies the data in the buffer back
				/// into itself, but starting at the beginning of the buffer. Note that
				/// this method should <b>only</b> be called immediately after detecting
				/// this condition. To call it at any other time is undefined and would
				/// likely result in data loss or corruption.
				/// </remarks>
				/// <seealso cref="BlockingBuffer.MarkRecord()"/>
				/// <exception cref="System.IO.IOException"/>
				protected internal virtual void ShiftBufferedKey()
				{
					// spillLock unnecessary; both kvend and kvindex are current
					int headbytelen = this._enclosing.bufvoid - this._enclosing.bufmark;
					this._enclosing.bufvoid = this._enclosing.bufmark;
					int kvbidx = 4 * this._enclosing.kvindex;
					int kvbend = 4 * this._enclosing.kvend;
					int avail = Math.Min(this._enclosing.DistanceTo(0, kvbidx), this._enclosing.DistanceTo
						(0, kvbend));
					if (this._enclosing.bufindex + headbytelen < avail)
					{
						System.Array.Copy(this._enclosing.kvbuffer, 0, this._enclosing.kvbuffer, headbytelen
							, this._enclosing.bufindex);
						System.Array.Copy(this._enclosing.kvbuffer, this._enclosing.bufvoid, this._enclosing
							.kvbuffer, 0, headbytelen);
						this._enclosing.bufindex += headbytelen;
						this._enclosing.bufferRemaining -= this._enclosing.kvbuffer.Length - this._enclosing
							.bufvoid;
					}
					else
					{
						byte[] keytmp = new byte[this._enclosing.bufindex];
						System.Array.Copy(this._enclosing.kvbuffer, 0, keytmp, 0, this._enclosing.bufindex
							);
						this._enclosing.bufindex = 0;
						this.@out.Write(this._enclosing.kvbuffer, this._enclosing.bufmark, headbytelen);
						this.@out.Write(keytmp);
					}
				}

				private readonly MapOutputBuffer<K, V> _enclosing;
			}

			public class Buffer : OutputStream
			{
				private readonly byte[] scratch = new byte[1];

				/// <exception cref="System.IO.IOException"/>
				public override void Write(int v)
				{
					this.scratch[0] = unchecked((byte)v);
					this.Write(this.scratch, 0, 1);
				}

				/// <summary>Attempt to write a sequence of bytes to the collection buffer.</summary>
				/// <remarks>
				/// Attempt to write a sequence of bytes to the collection buffer.
				/// This method will block if the spill thread is running and it
				/// cannot write.
				/// </remarks>
				/// <exception cref="MapBufferTooSmallException">
				/// if record is too large to
				/// deserialize into the collection buffer.
				/// </exception>
				/// <exception cref="System.IO.IOException"/>
				public override void Write(byte[] b, int off, int len)
				{
					// must always verify the invariant that at least METASIZE bytes are
					// available beyond kvindex, even when len == 0
					this._enclosing.bufferRemaining -= len;
					if (this._enclosing.bufferRemaining <= 0)
					{
						// writing these bytes could exhaust available buffer space or fill
						// the buffer to soft limit. check if spill or blocking are necessary
						bool blockwrite = false;
						this._enclosing.spillLock.Lock();
						try
						{
							do
							{
								this._enclosing.CheckSpillException();
								int kvbidx = 4 * this._enclosing.kvindex;
								int kvbend = 4 * this._enclosing.kvend;
								// ser distance to key index
								int distkvi = this._enclosing.DistanceTo(this._enclosing.bufindex, kvbidx);
								// ser distance to spill end index
								int distkve = this._enclosing.DistanceTo(this._enclosing.bufindex, kvbend);
								// if kvindex is closer than kvend, then a spill is neither in
								// progress nor complete and reset since the lock was held. The
								// write should block only if there is insufficient space to
								// complete the current write, write the metadata for this record,
								// and write the metadata for the next record. If kvend is closer,
								// then the write should block if there is too little space for
								// either the metadata or the current write. Note that collect
								// ensures its metadata requirement with a zero-length write
								blockwrite = distkvi <= distkve ? distkvi <= len + 2 * MapTask.MapOutputBuffer.Metasize
									 : distkve <= len || this._enclosing.DistanceTo(this._enclosing.bufend, kvbidx) 
									< 2 * MapTask.MapOutputBuffer.Metasize;
								if (!this._enclosing.spillInProgress)
								{
									if (blockwrite)
									{
										if ((kvbend + MapTask.MapOutputBuffer.Metasize) % this._enclosing.kvbuffer.Length
											 != this._enclosing.equator - (this._enclosing.equator % MapTask.MapOutputBuffer
											.Metasize))
										{
											// spill finished, reclaim space
											// need to use meta exclusively; zero-len rec & 100% spill
											// pcnt would fail
											this._enclosing.ResetSpill();
											// resetSpill doesn't move bufindex, kvindex
											this._enclosing.bufferRemaining = Math.Min(distkvi - 2 * MapTask.MapOutputBuffer.
												Metasize, this._enclosing.softLimit - this._enclosing.DistanceTo(kvbidx, this._enclosing
												.bufindex)) - len;
											continue;
										}
										// we have records we can spill; only spill if blocked
										if (this._enclosing.kvindex != this._enclosing.kvend)
										{
											this._enclosing.StartSpill();
											// Blocked on this write, waiting for the spill just
											// initiated to finish. Instead of repositioning the marker
											// and copying the partial record, we set the record start
											// to be the new equator
											this._enclosing.SetEquator(this._enclosing.bufmark);
										}
										else
										{
											// We have no buffered records, and this record is too large
											// to write into kvbuffer. We must spill it directly from
											// collect
											int size = this._enclosing.DistanceTo(this._enclosing.bufstart, this._enclosing.bufindex
												) + len;
											this._enclosing.SetEquator(0);
											this._enclosing.bufstart = this._enclosing.bufend = this._enclosing.bufindex = this
												._enclosing.equator;
											this._enclosing.kvstart = this._enclosing.kvend = this._enclosing.kvindex;
											this._enclosing.bufvoid = this._enclosing.kvbuffer.Length;
											throw new MapTask.MapBufferTooSmallException(size + " bytes");
										}
									}
								}
								if (blockwrite)
								{
									// wait for spill
									try
									{
										while (this._enclosing.spillInProgress)
										{
											this._enclosing.reporter.Progress();
											this._enclosing.spillDone.Await();
										}
									}
									catch (Exception e)
									{
										throw new IOException("Buffer interrupted while waiting for the writer", e);
									}
								}
							}
							while (blockwrite);
						}
						finally
						{
							this._enclosing.spillLock.Unlock();
						}
					}
					// here, we know that we have sufficient space to write
					if (this._enclosing.bufindex + len > this._enclosing.bufvoid)
					{
						int gaplen = this._enclosing.bufvoid - this._enclosing.bufindex;
						System.Array.Copy(b, off, this._enclosing.kvbuffer, this._enclosing.bufindex, gaplen
							);
						len -= gaplen;
						off += gaplen;
						this._enclosing.bufindex = 0;
					}
					System.Array.Copy(b, off, this._enclosing.kvbuffer, this._enclosing.bufindex, len
						);
					this._enclosing.bufindex += len;
				}

				internal Buffer(MapOutputBuffer<K, V> _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly MapOutputBuffer<K, V> _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.TypeLoadException"/>
			/// <exception cref="System.Exception"/>
			public virtual void Flush()
			{
				Log.Info("Starting flush of map output");
				if (kvbuffer == null)
				{
					Log.Info("kvbuffer is null. Skipping flush.");
					return;
				}
				spillLock.Lock();
				try
				{
					while (spillInProgress)
					{
						reporter.Progress();
						spillDone.Await();
					}
					CheckSpillException();
					int kvbend = 4 * kvend;
					if ((kvbend + Metasize) % kvbuffer.Length != equator - (equator % Metasize))
					{
						// spill finished
						ResetSpill();
					}
					if (kvindex != kvend)
					{
						kvend = (kvindex + Nmeta) % kvmeta.Capacity();
						bufend = bufmark;
						Log.Info("Spilling map output");
						Log.Info("bufstart = " + bufstart + "; bufend = " + bufmark + "; bufvoid = " + bufvoid
							);
						Log.Info("kvstart = " + kvstart + "(" + (kvstart * 4) + "); kvend = " + kvend + "("
							 + (kvend * 4) + "); length = " + (DistanceTo(kvend, kvstart, kvmeta.Capacity())
							 + 1) + "/" + maxRec);
						SortAndSpill();
					}
				}
				catch (Exception e)
				{
					throw new IOException("Interrupted while waiting for the writer", e);
				}
				finally
				{
					spillLock.Unlock();
				}
				System.Diagnostics.Debug.Assert(!spillLock.IsHeldByCurrentThread());
				// shut down spill thread and wait for it to exit. Since the preceding
				// ensures that it is finished with its work (and sortAndSpill did not
				// throw), we elect to use an interrupt instead of setting a flag.
				// Spilling simultaneously from this thread while the spill thread
				// finishes its work might be both a useful way to extend this and also
				// sufficient motivation for the latter approach.
				try
				{
					spillThread.Interrupt();
					spillThread.Join();
				}
				catch (Exception e)
				{
					throw new IOException("Spill failed", e);
				}
				// release sort buffer before the merge
				kvbuffer = null;
				MergeParts();
				Path outputPath = mapOutputFile.GetOutputFile();
				fileOutputByteCounter.Increment(rfs.GetFileStatus(outputPath).GetLen());
			}

			public virtual void Close()
			{
			}

			protected internal class SpillThread : Sharpen.Thread
			{
				public override void Run()
				{
					this._enclosing.spillLock.Lock();
					this._enclosing.spillThreadRunning = true;
					try
					{
						while (true)
						{
							this._enclosing.spillDone.Signal();
							while (!this._enclosing.spillInProgress)
							{
								this._enclosing.spillReady.Await();
							}
							try
							{
								this._enclosing.spillLock.Unlock();
								this._enclosing.SortAndSpill();
							}
							catch (Exception t)
							{
								this._enclosing.sortSpillException = t;
							}
							finally
							{
								this._enclosing.spillLock.Lock();
								if (this._enclosing.bufend < this._enclosing.bufstart)
								{
									this._enclosing.bufvoid = this._enclosing.kvbuffer.Length;
								}
								this._enclosing.kvstart = this._enclosing.kvend;
								this._enclosing.bufstart = this._enclosing.bufend;
								this._enclosing.spillInProgress = false;
							}
						}
					}
					catch (Exception)
					{
						Sharpen.Thread.CurrentThread().Interrupt();
					}
					finally
					{
						this._enclosing.spillLock.Unlock();
						this._enclosing.spillThreadRunning = false;
					}
				}

				internal SpillThread(MapOutputBuffer<K, V> _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly MapOutputBuffer<K, V> _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			private void CheckSpillException()
			{
				Exception lspillException = sortSpillException;
				if (lspillException != null)
				{
					if (lspillException is Error)
					{
						string logMsg = "Task " + GetTaskID() + " failed : " + StringUtils.StringifyException
							(lspillException);
						mapTask.ReportFatalError(GetTaskID(), lspillException, logMsg);
					}
					throw new IOException("Spill failed", lspillException);
				}
			}

			private void StartSpill()
			{
				System.Diagnostics.Debug.Assert(!spillInProgress);
				kvend = (kvindex + Nmeta) % kvmeta.Capacity();
				bufend = bufmark;
				spillInProgress = true;
				Log.Info("Spilling map output");
				Log.Info("bufstart = " + bufstart + "; bufend = " + bufmark + "; bufvoid = " + bufvoid
					);
				Log.Info("kvstart = " + kvstart + "(" + (kvstart * 4) + "); kvend = " + kvend + "("
					 + (kvend * 4) + "); length = " + (DistanceTo(kvend, kvstart, kvmeta.Capacity())
					 + 1) + "/" + maxRec);
				spillReady.Signal();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.TypeLoadException"/>
			/// <exception cref="System.Exception"/>
			private void SortAndSpill()
			{
				//approximate the length of the output file to be the length of the
				//buffer + header lengths for the partitions
				long size = DistanceTo(bufstart, bufend, bufvoid) + partitions * ApproxHeaderLength;
				FSDataOutputStream @out = null;
				try
				{
					// create spill file
					SpillRecord spillRec = new SpillRecord(partitions);
					Path filename = mapOutputFile.GetSpillFileForWrite(numSpills, size);
					@out = rfs.Create(filename);
					int mstart = kvend / Nmeta;
					int mend = 1 + (kvstart >= kvend ? kvstart : kvmeta.Capacity() + kvstart) / Nmeta;
					// kvend is a valid record
					sorter.Sort(this, mstart, mend, reporter);
					int spindex = mstart;
					IndexRecord rec = new IndexRecord();
					MapTask.MapOutputBuffer.InMemValBytes value = new MapTask.MapOutputBuffer.InMemValBytes
						(this);
					for (int i = 0; i < partitions; ++i)
					{
						IFile.Writer<K, V> writer = null;
						try
						{
							long segmentStart = @out.GetPos();
							FSDataOutputStream partitionOut = CryptoUtils.WrapIfNecessary(job, @out);
							writer = new IFile.Writer<K, V>(job, partitionOut, keyClass, valClass, codec, spilledRecordsCounter
								);
							if (combinerRunner == null)
							{
								// spill directly
								DataInputBuffer key = new DataInputBuffer();
								while (spindex < mend && kvmeta.Get(OffsetFor(spindex % maxRec) + Partition) == i
									)
								{
									int kvoff = OffsetFor(spindex % maxRec);
									int keystart = kvmeta.Get(kvoff + Keystart);
									int valstart = kvmeta.Get(kvoff + Valstart);
									key.Reset(kvbuffer, keystart, valstart - keystart);
									GetVBytesForOffset(kvoff, value);
									writer.Append(key, value);
									++spindex;
								}
							}
							else
							{
								int spstart = spindex;
								while (spindex < mend && kvmeta.Get(OffsetFor(spindex % maxRec) + Partition) == i
									)
								{
									++spindex;
								}
								// Note: we would like to avoid the combiner if we've fewer
								// than some threshold of records for a partition
								if (spstart != spindex)
								{
									combineCollector.SetWriter(writer);
									RawKeyValueIterator kvIter = new MapTask.MapOutputBuffer.MRResultIterator(this, spstart
										, spindex);
									combinerRunner.Combine(kvIter, combineCollector);
								}
							}
							// close the writer
							writer.Close();
							// record offsets
							rec.startOffset = segmentStart;
							rec.rawLength = writer.GetRawLength() + CryptoUtils.CryptoPadding(job);
							rec.partLength = writer.GetCompressedLength() + CryptoUtils.CryptoPadding(job);
							spillRec.PutIndex(rec, i);
							writer = null;
						}
						finally
						{
							if (null != writer)
							{
								writer.Close();
							}
						}
					}
					if (totalIndexCacheMemory >= indexCacheMemoryLimit)
					{
						// create spill index file
						Path indexFilename = mapOutputFile.GetSpillIndexFileForWrite(numSpills, partitions
							 * MapOutputIndexRecordLength);
						spillRec.WriteToFile(indexFilename, job);
					}
					else
					{
						indexCacheList.AddItem(spillRec);
						totalIndexCacheMemory += spillRec.Size() * MapOutputIndexRecordLength;
					}
					Log.Info("Finished spill " + numSpills);
					++numSpills;
				}
				finally
				{
					if (@out != null)
					{
						@out.Close();
					}
				}
			}

			/// <summary>
			/// Handles the degenerate case where serialization fails to fit in
			/// the in-memory buffer, so we must spill the record from collect
			/// directly to a spill file.
			/// </summary>
			/// <remarks>
			/// Handles the degenerate case where serialization fails to fit in
			/// the in-memory buffer, so we must spill the record from collect
			/// directly to a spill file. Consider this "losing".
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			private void SpillSingleRecord(K key, V value, int partition)
			{
				long size = kvbuffer.Length + partitions * ApproxHeaderLength;
				FSDataOutputStream @out = null;
				try
				{
					// create spill file
					SpillRecord spillRec = new SpillRecord(partitions);
					Path filename = mapOutputFile.GetSpillFileForWrite(numSpills, size);
					@out = rfs.Create(filename);
					// we don't run the combiner for a single record
					IndexRecord rec = new IndexRecord();
					for (int i = 0; i < partitions; ++i)
					{
						IFile.Writer<K, V> writer = null;
						try
						{
							long segmentStart = @out.GetPos();
							// Create a new codec, don't care!
							FSDataOutputStream partitionOut = CryptoUtils.WrapIfNecessary(job, @out);
							writer = new IFile.Writer<K, V>(job, partitionOut, keyClass, valClass, codec, spilledRecordsCounter
								);
							if (i == partition)
							{
								long recordStart = @out.GetPos();
								writer.Append(key, value);
								// Note that our map byte count will not be accurate with
								// compression
								mapOutputByteCounter.Increment(@out.GetPos() - recordStart);
							}
							writer.Close();
							// record offsets
							rec.startOffset = segmentStart;
							rec.rawLength = writer.GetRawLength() + CryptoUtils.CryptoPadding(job);
							rec.partLength = writer.GetCompressedLength() + CryptoUtils.CryptoPadding(job);
							spillRec.PutIndex(rec, i);
							writer = null;
						}
						catch (IOException e)
						{
							if (null != writer)
							{
								writer.Close();
							}
							throw;
						}
					}
					if (totalIndexCacheMemory >= indexCacheMemoryLimit)
					{
						// create spill index file
						Path indexFilename = mapOutputFile.GetSpillIndexFileForWrite(numSpills, partitions
							 * MapOutputIndexRecordLength);
						spillRec.WriteToFile(indexFilename, job);
					}
					else
					{
						indexCacheList.AddItem(spillRec);
						totalIndexCacheMemory += spillRec.Size() * MapOutputIndexRecordLength;
					}
					++numSpills;
				}
				finally
				{
					if (@out != null)
					{
						@out.Close();
					}
				}
			}

			/// <summary>
			/// Given an offset, populate vbytes with the associated set of
			/// deserialized value bytes.
			/// </summary>
			/// <remarks>
			/// Given an offset, populate vbytes with the associated set of
			/// deserialized value bytes. Should only be called during a spill.
			/// </remarks>
			private void GetVBytesForOffset(int kvoff, MapTask.MapOutputBuffer.InMemValBytes 
				vbytes)
			{
				// get the keystart for the next serialized value to be the end
				// of this value. If this is the last value in the buffer, use bufend
				int vallen = kvmeta.Get(kvoff + Vallen);
				System.Diagnostics.Debug.Assert(vallen >= 0);
				vbytes.Reset(kvbuffer, kvmeta.Get(kvoff + Valstart), vallen);
			}

			/// <summary>Inner class wrapping valuebytes, used for appendRaw.</summary>
			protected internal class InMemValBytes : DataInputBuffer
			{
				private byte[] buffer;

				private int start;

				private int length;

				public override void Reset(byte[] buffer, int start, int length)
				{
					this.buffer = buffer;
					this.start = start;
					this.length = length;
					if (start + length > this._enclosing.bufvoid)
					{
						this.buffer = new byte[this.length];
						int taillen = this._enclosing.bufvoid - start;
						System.Array.Copy(buffer, start, this.buffer, 0, taillen);
						System.Array.Copy(buffer, 0, this.buffer, taillen, length - taillen);
						this.start = 0;
					}
					base.Reset(this.buffer, this.start, this.length);
				}

				internal InMemValBytes(MapOutputBuffer<K, V> _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly MapOutputBuffer<K, V> _enclosing;
			}

			protected internal class MRResultIterator : RawKeyValueIterator
			{
				private readonly DataInputBuffer keybuf = new DataInputBuffer();

				private readonly MapTask.MapOutputBuffer.InMemValBytes vbytes;

				private readonly int end;

				private int current;

				public MRResultIterator(MapOutputBuffer<K, V> _enclosing, int start, int end)
				{
					this._enclosing = _enclosing;
					vbytes = new MapTask.MapOutputBuffer.InMemValBytes(this);
					this.end = end;
					this.current = start - 1;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual bool Next()
				{
					return ++this.current < this.end;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual DataInputBuffer GetKey()
				{
					int kvoff = this._enclosing.OffsetFor(this.current % this._enclosing.maxRec);
					this.keybuf.Reset(this._enclosing.kvbuffer, this._enclosing.kvmeta.Get(kvoff + MapTask.MapOutputBuffer
						.Keystart), this._enclosing.kvmeta.Get(kvoff + MapTask.MapOutputBuffer.Valstart)
						 - this._enclosing.kvmeta.Get(kvoff + MapTask.MapOutputBuffer.Keystart));
					return this.keybuf;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual DataInputBuffer GetValue()
				{
					this._enclosing.GetVBytesForOffset(this._enclosing.OffsetFor(this.current % this.
						_enclosing.maxRec), this.vbytes);
					return this.vbytes;
				}

				public virtual Progress GetProgress()
				{
					return null;
				}

				public virtual void Close()
				{
				}

				private readonly MapOutputBuffer<K, V> _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			/// <exception cref="System.TypeLoadException"/>
			private void MergeParts()
			{
				// get the approximate size of the final output/index files
				long finalOutFileSize = 0;
				long finalIndexFileSize = 0;
				Path[] filename = new Path[numSpills];
				TaskAttemptID mapId = GetTaskID();
				for (int i = 0; i < numSpills; i++)
				{
					filename[i] = mapOutputFile.GetSpillFile(i);
					finalOutFileSize += rfs.GetFileStatus(filename[i]).GetLen();
				}
				if (numSpills == 1)
				{
					//the spill is the final output
					SameVolRename(filename[0], mapOutputFile.GetOutputFileForWriteInVolume(filename[0
						]));
					if (indexCacheList.Count == 0)
					{
						SameVolRename(mapOutputFile.GetSpillIndexFile(0), mapOutputFile.GetOutputIndexFileForWriteInVolume
							(filename[0]));
					}
					else
					{
						indexCacheList[0].WriteToFile(mapOutputFile.GetOutputIndexFileForWriteInVolume(filename
							[0]), job);
					}
					sortPhase.Complete();
					return;
				}
				// read in paged indices
				for (int i_1 = indexCacheList.Count; i_1 < numSpills; ++i_1)
				{
					Path indexFileName = mapOutputFile.GetSpillIndexFile(i_1);
					indexCacheList.AddItem(new SpillRecord(indexFileName, job));
				}
				//make correction in the length to include the sequence file header
				//lengths for each partition
				finalOutFileSize += partitions * ApproxHeaderLength;
				finalIndexFileSize = partitions * MapOutputIndexRecordLength;
				Path finalOutputFile = mapOutputFile.GetOutputFileForWrite(finalOutFileSize);
				Path finalIndexFile = mapOutputFile.GetOutputIndexFileForWrite(finalIndexFileSize
					);
				//The output stream for the final single output file
				FSDataOutputStream finalOut = rfs.Create(finalOutputFile, true, 4096);
				if (numSpills == 0)
				{
					//create dummy files
					IndexRecord rec = new IndexRecord();
					SpillRecord sr = new SpillRecord(partitions);
					try
					{
						for (int i_2 = 0; i_2 < partitions; i_2++)
						{
							long segmentStart = finalOut.GetPos();
							FSDataOutputStream finalPartitionOut = CryptoUtils.WrapIfNecessary(job, finalOut);
							IFile.Writer<K, V> writer = new IFile.Writer<K, V>(job, finalPartitionOut, keyClass
								, valClass, codec, null);
							writer.Close();
							rec.startOffset = segmentStart;
							rec.rawLength = writer.GetRawLength() + CryptoUtils.CryptoPadding(job);
							rec.partLength = writer.GetCompressedLength() + CryptoUtils.CryptoPadding(job);
							sr.PutIndex(rec, i_2);
						}
						sr.WriteToFile(finalIndexFile, job);
					}
					finally
					{
						finalOut.Close();
					}
					sortPhase.Complete();
					return;
				}
				{
					sortPhase.AddPhases(partitions);
					// Divide sort phase into sub-phases
					IndexRecord rec = new IndexRecord();
					SpillRecord spillRec = new SpillRecord(partitions);
					for (int parts = 0; parts < partitions; parts++)
					{
						//create the segments to be merged
						IList<Merger.Segment<K, V>> segmentList = new AList<Merger.Segment<K, V>>(numSpills
							);
						for (int i_2 = 0; i_2 < numSpills; i_2++)
						{
							IndexRecord indexRecord = indexCacheList[i_2].GetIndex(parts);
							Merger.Segment<K, V> s = new Merger.Segment<K, V>(job, rfs, filename[i_2], indexRecord
								.startOffset, indexRecord.partLength, codec, true);
							segmentList.Add(i_2, s);
							if (Log.IsDebugEnabled())
							{
								Log.Debug("MapId=" + mapId + " Reducer=" + parts + "Spill =" + i_2 + "(" + indexRecord
									.startOffset + "," + indexRecord.rawLength + ", " + indexRecord.partLength + ")"
									);
							}
						}
						int mergeFactor = job.GetInt(JobContext.IoSortFactor, 100);
						// sort the segments only if there are intermediate merges
						bool sortSegments = segmentList.Count > mergeFactor;
						//merge
						RawKeyValueIterator kvIter = Merger.Merge(job, rfs, keyClass, valClass, codec, segmentList
							, mergeFactor, new Path(mapId.ToString()), job.GetOutputKeyComparator(), reporter
							, sortSegments, null, spilledRecordsCounter, sortPhase.Phase(), TaskType.Map);
						//write merged output to disk
						long segmentStart = finalOut.GetPos();
						FSDataOutputStream finalPartitionOut = CryptoUtils.WrapIfNecessary(job, finalOut);
						IFile.Writer<K, V> writer = new IFile.Writer<K, V>(job, finalPartitionOut, keyClass
							, valClass, codec, spilledRecordsCounter);
						if (combinerRunner == null || numSpills < minSpillsForCombine)
						{
							Merger.WriteFile(kvIter, writer, reporter, job);
						}
						else
						{
							combineCollector.SetWriter(writer);
							combinerRunner.Combine(kvIter, combineCollector);
						}
						//close
						writer.Close();
						sortPhase.StartNextPhase();
						// record offsets
						rec.startOffset = segmentStart;
						rec.rawLength = writer.GetRawLength() + CryptoUtils.CryptoPadding(job);
						rec.partLength = writer.GetCompressedLength() + CryptoUtils.CryptoPadding(job);
						spillRec.PutIndex(rec, parts);
					}
					spillRec.WriteToFile(finalIndexFile, job);
					finalOut.Close();
					for (int i_3 = 0; i_3 < numSpills; i_3++)
					{
						rfs.Delete(filename[i_3], true);
					}
				}
			}

			/// <summary>Rename srcPath to dstPath on the same volume.</summary>
			/// <remarks>
			/// Rename srcPath to dstPath on the same volume. This is the same
			/// as RawLocalFileSystem's rename method, except that it will not
			/// fall back to a copy, and it will create the target directory
			/// if it doesn't exist.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			private void SameVolRename(Path srcPath, Path dstPath)
			{
				RawLocalFileSystem rfs = (RawLocalFileSystem)this.rfs;
				FilePath src = rfs.PathToFile(srcPath);
				FilePath dst = rfs.PathToFile(dstPath);
				if (!dst.GetParentFile().Exists())
				{
					if (!dst.GetParentFile().Mkdirs())
					{
						throw new IOException("Unable to rename " + src + " to " + dst + ": couldn't create parent directory"
							);
					}
				}
				if (!src.RenameTo(dst))
				{
					throw new IOException("Unable to rename " + src + " to " + dst);
				}
			}
		}

		/// <summary>
		/// Exception indicating that the allocated sort buffer is insufficient
		/// to hold the current record.
		/// </summary>
		[System.Serializable]
		private class MapBufferTooSmallException : IOException
		{
			public MapBufferTooSmallException(string s)
				: base(s)
			{
			}
			// MapOutputBuffer
		}

		private void CloseQuietly<Inkey, Invalue, Outkey, Outvalue>(RecordReader<INKEY, INVALUE
			> c)
		{
			if (c != null)
			{
				try
				{
					c.Close();
				}
				catch (IOException ie)
				{
					// Ignore
					Log.Info("Ignoring exception during close for " + c, ie);
				}
			}
		}

		private void CloseQuietly<Outkey, Outvalue>(MapOutputCollector<OUTKEY, OUTVALUE> 
			c)
		{
			if (c != null)
			{
				try
				{
					c.Close();
				}
				catch (Exception ie)
				{
					// Ignore
					Log.Info("Ignoring exception during close for " + c, ie);
				}
			}
		}

		private void CloseQuietly<Inkey, Invalue, Outkey, Outvalue>(RecordReader<INKEY, INVALUE
			> c)
		{
			if (c != null)
			{
				try
				{
					c.Close();
				}
				catch (Exception ie)
				{
					// Ignore
					Log.Info("Ignoring exception during close for " + c, ie);
				}
			}
		}

		private void CloseQuietly<Inkey, Invalue, Outkey, Outvalue>(RecordWriter<OUTKEY, 
			OUTVALUE> c, Mapper.Context mapperContext)
		{
			if (c != null)
			{
				try
				{
					c.Close(mapperContext);
				}
				catch (Exception ie)
				{
					// Ignore
					Log.Info("Ignoring exception during close for " + c, ie);
				}
			}
		}
	}
}
