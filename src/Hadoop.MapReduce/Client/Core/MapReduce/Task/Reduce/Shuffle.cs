using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public class Shuffle<K, V> : ShuffleConsumerPlugin<K, V>, ExceptionReporter
	{
		private const int ProgressFrequency = 2000;

		private const int MaxEventsToFetch = 10000;

		private const int MinEventsToFetch = 100;

		private const int MaxRpcOutstandingEvents = 3000000;

		private ShuffleConsumerPlugin.Context context;

		private TaskAttemptID reduceId;

		private JobConf jobConf;

		private Reporter reporter;

		private ShuffleClientMetrics metrics;

		private TaskUmbilicalProtocol umbilical;

		private ShuffleSchedulerImpl<K, V> scheduler;

		private MergeManager<K, V> merger;

		private Exception throwable = null;

		private string throwingThreadName = null;

		private Progress copyPhase;

		private TaskStatus taskStatus;

		private Org.Apache.Hadoop.Mapred.Task reduceTask;

		private IDictionary<TaskAttemptID, MapOutputFile> localMapFiles;

		//Used for status updates
		public virtual void Init(ShuffleConsumerPlugin.Context context)
		{
			this.context = context;
			this.reduceId = context.GetReduceId();
			this.jobConf = context.GetJobConf();
			this.umbilical = context.GetUmbilical();
			this.reporter = context.GetReporter();
			this.metrics = new ShuffleClientMetrics(reduceId, jobConf);
			this.copyPhase = context.GetCopyPhase();
			this.taskStatus = context.GetStatus();
			this.reduceTask = context.GetReduceTask();
			this.localMapFiles = context.GetLocalMapFiles();
			scheduler = new ShuffleSchedulerImpl<K, V>(jobConf, taskStatus, reduceId, this, copyPhase
				, context.GetShuffledMapsCounter(), context.GetReduceShuffleBytes(), context.GetFailedShuffleCounter
				());
			merger = CreateMergeManager(context);
		}

		protected internal virtual MergeManager<K, V> CreateMergeManager(ShuffleConsumerPlugin.Context
			 context)
		{
			return new MergeManagerImpl<K, V>(reduceId, jobConf, context.GetLocalFS(), context
				.GetLocalDirAllocator(), reporter, context.GetCodec(), context.GetCombinerClass(
				), context.GetCombineCollector(), context.GetSpilledRecordsCounter(), context.GetReduceCombineInputCounter
				(), context.GetMergedMapOutputsCounter(), this, context.GetMergePhase(), context
				.GetMapOutputFile());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual RawKeyValueIterator Run()
		{
			// Scale the maximum events we fetch per RPC call to mitigate OOM issues
			// on the ApplicationMaster when a thundering herd of reducers fetch events
			// TODO: This should not be necessary after HADOOP-8942
			int eventsPerReducer = Math.Max(MinEventsToFetch, MaxRpcOutstandingEvents / jobConf
				.GetNumReduceTasks());
			int maxEventsToFetch = Math.Min(MaxEventsToFetch, eventsPerReducer);
			// Start the map-completion events fetcher thread
			EventFetcher<K, V> eventFetcher = new EventFetcher<K, V>(reduceId, umbilical, scheduler
				, this, maxEventsToFetch);
			eventFetcher.Start();
			// Start the map-output fetcher threads
			bool isLocal = localMapFiles != null;
			int numFetchers = isLocal ? 1 : jobConf.GetInt(MRJobConfig.ShuffleParallelCopies, 
				5);
			Fetcher<K, V>[] fetchers = new Fetcher[numFetchers];
			if (isLocal)
			{
				fetchers[0] = new LocalFetcher<K, V>(jobConf, reduceId, scheduler, merger, reporter
					, metrics, this, reduceTask.GetShuffleSecret(), localMapFiles);
				fetchers[0].Start();
			}
			else
			{
				for (int i = 0; i < numFetchers; ++i)
				{
					fetchers[i] = new Fetcher<K, V>(jobConf, reduceId, scheduler, merger, reporter, metrics
						, this, reduceTask.GetShuffleSecret());
					fetchers[i].Start();
				}
			}
			// Wait for shuffle to complete successfully
			while (!scheduler.WaitUntilDone(ProgressFrequency))
			{
				reporter.Progress();
				lock (this)
				{
					if (throwable != null)
					{
						throw new Shuffle.ShuffleError("error in shuffle in " + throwingThreadName, throwable
							);
					}
				}
			}
			// Stop the event-fetcher thread
			eventFetcher.ShutDown();
			// Stop the map-output fetcher threads
			foreach (Fetcher<K, V> fetcher in fetchers)
			{
				fetcher.ShutDown();
			}
			// stop the scheduler
			scheduler.Close();
			copyPhase.Complete();
			// copy is already complete
			taskStatus.SetPhase(TaskStatus.Phase.Sort);
			reduceTask.StatusUpdate(umbilical);
			// Finish the on-going merges...
			RawKeyValueIterator kvIter = null;
			try
			{
				kvIter = merger.Close();
			}
			catch (Exception e)
			{
				throw new Shuffle.ShuffleError("Error while doing final merge ", e);
			}
			// Sanity check
			lock (this)
			{
				if (throwable != null)
				{
					throw new Shuffle.ShuffleError("error in shuffle in " + throwingThreadName, throwable
						);
				}
			}
			return kvIter;
		}

		public virtual void Close()
		{
		}

		public virtual void ReportException(Exception t)
		{
			lock (this)
			{
				if (throwable == null)
				{
					throwable = t;
					throwingThreadName = Sharpen.Thread.CurrentThread().GetName();
					// Notify the scheduler so that the reporting thread finds the 
					// exception immediately.
					lock (scheduler)
					{
						Sharpen.Runtime.NotifyAll(scheduler);
					}
				}
			}
		}

		[System.Serializable]
		public class ShuffleError : IOException
		{
			private const long serialVersionUID = 5753909320586607881L;

			internal ShuffleError(string msg, Exception t)
				: base(msg, t)
			{
			}
		}
	}
}
