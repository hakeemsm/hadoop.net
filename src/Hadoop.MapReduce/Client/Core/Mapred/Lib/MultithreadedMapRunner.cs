using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Map;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// Multithreaded implementation for
	/// <see cref="Org.Apache.Hadoop.Mapred.MapRunnable{K1, V1, K2, V2}"/>
	/// .
	/// <p>
	/// It can be used instead of the default implementation,
	/// of
	/// <see cref="Org.Apache.Hadoop.Mapred.MapRunner{K1, V1, K2, V2}"/>
	/// , when the Map
	/// operation is not CPU bound in order to improve throughput.
	/// <p>
	/// Map implementations using this MapRunnable must be thread-safe.
	/// <p>
	/// The Map-Reduce job has to be configured to use this MapRunnable class (using
	/// the JobConf.setMapRunnerClass method) and
	/// the number of threads the thread-pool can use with the
	/// <code>mapred.map.multithreadedrunner.threads</code> property, its default
	/// value is 10 threads.
	/// <p>
	/// </summary>
	public class MultithreadedMapRunner<K1, V1, K2, V2> : MapRunnable<K1, V1, K2, V2>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(MultithreadedMapRunner
			).FullName);

		private JobConf job;

		private Mapper<K1, V1, K2, V2> mapper;

		private ExecutorService executorService;

		private volatile IOException ioException;

		private volatile RuntimeException runtimeException;

		private bool incrProcCount;

		public virtual void Configure(JobConf jobConf)
		{
			int numberOfThreads = jobConf.GetInt(MultithreadedMapper.NumThreads, 10);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Configuring jobConf " + jobConf.GetJobName() + " to use " + numberOfThreads
					 + " threads");
			}
			this.job = jobConf;
			//increment processed counter only if skipping feature is enabled
			this.incrProcCount = SkipBadRecords.GetMapperMaxSkipRecords(job) > 0 && SkipBadRecords
				.GetAutoIncrMapperProcCount(job);
			this.mapper = ReflectionUtils.NewInstance(jobConf.GetMapperClass(), jobConf);
			// Creating a threadpool of the configured size to execute the Mapper
			// map method in parallel.
			executorService = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit
				.Milliseconds, new MultithreadedMapRunner.BlockingArrayQueue(numberOfThreads));
		}

		/// <summary>
		/// A blocking array queue that replaces offer and add, which throws on a full
		/// queue, to a put, which waits on a full queue.
		/// </summary>
		[System.Serializable]
		private class BlockingArrayQueue : ArrayBlockingQueue<Runnable>
		{
			private const long serialVersionUID = 1L;

			public BlockingArrayQueue(int capacity)
				: base(capacity)
			{
			}

			public override bool Offer(Runnable r)
			{
				return AddItem(r);
			}

			public override bool AddItem(Runnable r)
			{
				try
				{
					Put(r);
				}
				catch (Exception)
				{
					Sharpen.Thread.CurrentThread().Interrupt();
				}
				return true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.RuntimeException"/>
		private void CheckForExceptionsFromProcessingThreads()
		{
			// Checking if a Mapper.map within a Runnable has generated an
			// IOException. If so we rethrow it to force an abort of the Map
			// operation thus keeping the semantics of the default
			// implementation.
			if (ioException != null)
			{
				throw ioException;
			}
			// Checking if a Mapper.map within a Runnable has generated a
			// RuntimeException. If so we rethrow it to force an abort of the Map
			// operation thus keeping the semantics of the default
			// implementation.
			if (runtimeException != null)
			{
				throw runtimeException;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output
			, Reporter reporter)
		{
			try
			{
				// allocate key & value instances these objects will not be reused
				// because execution of Mapper.map is not serialized.
				K1 key = input.CreateKey();
				V1 value = input.CreateValue();
				while (input.Next(key, value))
				{
					executorService.Execute(new MultithreadedMapRunner.MapperInvokeRunable(this, key, 
						value, output, reporter));
					CheckForExceptionsFromProcessingThreads();
					// Allocate new key & value instances as mapper is running in parallel
					key = input.CreateKey();
					value = input.CreateValue();
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Finished dispatching all Mappper.map calls, job " + job.GetJobName());
				}
				// Graceful shutdown of the Threadpool, it will let all scheduled
				// Runnables to end.
				executorService.Shutdown();
				try
				{
					// Now waiting for all Runnables to end.
					while (!executorService.AwaitTermination(100, TimeUnit.Milliseconds))
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Awaiting all running Mappper.map calls to finish, job " + job.GetJobName
								());
						}
						// NOTE: while Mapper.map dispatching has concluded there are still
						// map calls in progress and exceptions would be thrown.
						CheckForExceptionsFromProcessingThreads();
					}
					// NOTE: it could be that a map call has had an exception after the
					// call for awaitTermination() returing true. And edge case but it
					// could happen.
					CheckForExceptionsFromProcessingThreads();
				}
				catch (IOException ioEx)
				{
					// Forcing a shutdown of all thread of the threadpool and rethrowing
					// the IOException
					executorService.ShutdownNow();
					throw;
				}
				catch (Exception iEx)
				{
					throw new RuntimeException(iEx);
				}
			}
			finally
			{
				mapper.Close();
			}
		}

		/// <summary>Runnable to execute a single Mapper.map call from a forked thread.</summary>
		private class MapperInvokeRunable : Runnable
		{
			private K1 key;

			private V1 value;

			private OutputCollector<K2, V2> output;

			private Reporter reporter;

			/// <summary>Collecting all required parameters to execute a Mapper.map call.</summary>
			/// <remarks>
			/// Collecting all required parameters to execute a Mapper.map call.
			/// <p>
			/// </remarks>
			/// <param name="key"/>
			/// <param name="value"/>
			/// <param name="output"/>
			/// <param name="reporter"/>
			public MapperInvokeRunable(MultithreadedMapRunner<K1, V1, K2, V2> _enclosing, K1 
				key, V1 value, OutputCollector<K2, V2> output, Reporter reporter)
			{
				this._enclosing = _enclosing;
				this.key = key;
				this.value = value;
				this.output = output;
				this.reporter = reporter;
			}

			/// <summary>Executes a Mapper.map call with the given Mapper and parameters.</summary>
			/// <remarks>
			/// Executes a Mapper.map call with the given Mapper and parameters.
			/// <p>
			/// This method is called from the thread-pool thread.
			/// </remarks>
			public virtual void Run()
			{
				try
				{
					// map pair to output
					this._enclosing._enclosing.mapper.Map(this.key, this.value, this.output, this.reporter
						);
					if (this._enclosing.incrProcCount)
					{
						this.reporter.IncrCounter(SkipBadRecords.CounterGroup, SkipBadRecords.CounterMapProcessedRecords
							, 1);
					}
				}
				catch (IOException ex)
				{
					// If there is an IOException during the call it is set in an instance
					// variable of the MultithreadedMapRunner from where it will be
					// rethrown.
					lock (this._enclosing._enclosing)
					{
						if (this._enclosing._enclosing.ioException == null)
						{
							this._enclosing._enclosing.ioException = ex;
						}
					}
				}
				catch (RuntimeException ex)
				{
					// If there is a RuntimeException during the call it is set in an
					// instance variable of the MultithreadedMapRunner from where it will be
					// rethrown.
					lock (this._enclosing._enclosing)
					{
						if (this._enclosing._enclosing.runtimeException == null)
						{
							this._enclosing._enclosing.runtimeException = ex;
						}
					}
				}
			}

			private readonly MultithreadedMapRunner<K1, V1, K2, V2> _enclosing;
		}
	}
}
