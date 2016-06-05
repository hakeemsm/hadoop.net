using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>ShuffleConsumerPlugin for serving Reducers.</summary>
	/// <remarks>
	/// ShuffleConsumerPlugin for serving Reducers.  It may shuffle MOF files from
	/// either the built-in ShuffleHandler or from a 3rd party AuxiliaryService.
	/// </remarks>
	public abstract class ShuffleConsumerPlugin<K, V>
	{
		public abstract void Init(ShuffleConsumerPlugin.Context<K, V> context);

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract RawKeyValueIterator Run();

		public abstract void Close();

		public class Context<K, V>
		{
			private readonly TaskAttemptID reduceId;

			private readonly JobConf jobConf;

			private readonly FileSystem localFS;

			private readonly TaskUmbilicalProtocol umbilical;

			private readonly LocalDirAllocator localDirAllocator;

			private readonly Reporter reporter;

			private readonly CompressionCodec codec;

			private readonly Type combinerClass;

			private readonly Task.CombineOutputCollector<K, V> combineCollector;

			private readonly Counters.Counter spilledRecordsCounter;

			private readonly Counters.Counter reduceCombineInputCounter;

			private readonly Counters.Counter shuffledMapsCounter;

			private readonly Counters.Counter reduceShuffleBytes;

			private readonly Counters.Counter failedShuffleCounter;

			private readonly Counters.Counter mergedMapOutputsCounter;

			private readonly TaskStatus status;

			private readonly Progress copyPhase;

			private readonly Progress mergePhase;

			private readonly Task reduceTask;

			private readonly MapOutputFile mapOutputFile;

			private readonly IDictionary<TaskAttemptID, MapOutputFile> localMapFiles;

			public Context(TaskAttemptID reduceId, JobConf jobConf, FileSystem localFS, TaskUmbilicalProtocol
				 umbilical, LocalDirAllocator localDirAllocator, Reporter reporter, CompressionCodec
				 codec, Type combinerClass, Task.CombineOutputCollector<K, V> combineCollector, 
				Counters.Counter spilledRecordsCounter, Counters.Counter reduceCombineInputCounter
				, Counters.Counter shuffledMapsCounter, Counters.Counter reduceShuffleBytes, Counters.Counter
				 failedShuffleCounter, Counters.Counter mergedMapOutputsCounter, TaskStatus status
				, Progress copyPhase, Progress mergePhase, Task reduceTask, MapOutputFile mapOutputFile
				, IDictionary<TaskAttemptID, MapOutputFile> localMapFiles)
			{
				this.reduceId = reduceId;
				this.jobConf = jobConf;
				this.localFS = localFS;
				this.umbilical = umbilical;
				this.localDirAllocator = localDirAllocator;
				this.reporter = reporter;
				this.codec = codec;
				this.combinerClass = combinerClass;
				this.combineCollector = combineCollector;
				this.spilledRecordsCounter = spilledRecordsCounter;
				this.reduceCombineInputCounter = reduceCombineInputCounter;
				this.shuffledMapsCounter = shuffledMapsCounter;
				this.reduceShuffleBytes = reduceShuffleBytes;
				this.failedShuffleCounter = failedShuffleCounter;
				this.mergedMapOutputsCounter = mergedMapOutputsCounter;
				this.status = status;
				this.copyPhase = copyPhase;
				this.mergePhase = mergePhase;
				this.reduceTask = reduceTask;
				this.mapOutputFile = mapOutputFile;
				this.localMapFiles = localMapFiles;
			}

			public virtual TaskAttemptID GetReduceId()
			{
				return reduceId;
			}

			public virtual JobConf GetJobConf()
			{
				return jobConf;
			}

			public virtual FileSystem GetLocalFS()
			{
				return localFS;
			}

			public virtual TaskUmbilicalProtocol GetUmbilical()
			{
				return umbilical;
			}

			public virtual LocalDirAllocator GetLocalDirAllocator()
			{
				return localDirAllocator;
			}

			public virtual Reporter GetReporter()
			{
				return reporter;
			}

			public virtual CompressionCodec GetCodec()
			{
				return codec;
			}

			public virtual Type GetCombinerClass()
			{
				return combinerClass;
			}

			public virtual Task.CombineOutputCollector<K, V> GetCombineCollector()
			{
				return combineCollector;
			}

			public virtual Counters.Counter GetSpilledRecordsCounter()
			{
				return spilledRecordsCounter;
			}

			public virtual Counters.Counter GetReduceCombineInputCounter()
			{
				return reduceCombineInputCounter;
			}

			public virtual Counters.Counter GetShuffledMapsCounter()
			{
				return shuffledMapsCounter;
			}

			public virtual Counters.Counter GetReduceShuffleBytes()
			{
				return reduceShuffleBytes;
			}

			public virtual Counters.Counter GetFailedShuffleCounter()
			{
				return failedShuffleCounter;
			}

			public virtual Counters.Counter GetMergedMapOutputsCounter()
			{
				return mergedMapOutputsCounter;
			}

			public virtual TaskStatus GetStatus()
			{
				return status;
			}

			public virtual Progress GetCopyPhase()
			{
				return copyPhase;
			}

			public virtual Progress GetMergePhase()
			{
				return mergePhase;
			}

			public virtual Task GetReduceTask()
			{
				return reduceTask;
			}

			public virtual MapOutputFile GetMapOutputFile()
			{
				return mapOutputFile;
			}

			public virtual IDictionary<TaskAttemptID, MapOutputFile> GetLocalMapFiles()
			{
				return localMapFiles;
			}
		}
		// end of public static class Context<K,V>
	}

	public static class ShuffleConsumerPluginConstants
	{
	}
}
