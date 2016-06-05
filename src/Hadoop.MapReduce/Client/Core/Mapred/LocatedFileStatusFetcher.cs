using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Utility class to fetch block locations for specified Input paths using a
	/// configured number of threads.
	/// </summary>
	public class LocatedFileStatusFetcher
	{
		private readonly Path[] inputDirs;

		private readonly PathFilter inputFilter;

		private readonly Configuration conf;

		private readonly bool recursive;

		private readonly bool newApi;

		private readonly ExecutorService rawExec;

		private readonly ListeningExecutorService exec;

		private readonly BlockingQueue<IList<FileStatus>> resultQueue;

		private readonly IList<IOException> invalidInputErrors = new List<IOException>();

		private readonly LocatedFileStatusFetcher.ProcessInitialInputPathCallback processInitialInputPathCallback;

		private readonly LocatedFileStatusFetcher.ProcessInputDirCallback processInputDirCallback;

		private readonly AtomicInteger runningTasks = new AtomicInteger(0);

		private readonly ReentrantLock Lock = new ReentrantLock();

		private readonly Condition condition = Lock.NewCondition();

		private volatile Exception unknownError;

		/// <param name="conf">configuration for the job</param>
		/// <param name="dirs">the initial list of paths</param>
		/// <param name="recursive">whether to traverse the patchs recursively</param>
		/// <param name="inputFilter">inputFilter to apply to the resulting paths</param>
		/// <param name="newApi">whether using the mapred or mapreduce API</param>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		public LocatedFileStatusFetcher(Configuration conf, Path[] dirs, bool recursive, 
			PathFilter inputFilter, bool newApi)
		{
			processInitialInputPathCallback = new LocatedFileStatusFetcher.ProcessInitialInputPathCallback
				(this);
			processInputDirCallback = new LocatedFileStatusFetcher.ProcessInputDirCallback(this
				);
			int numThreads = conf.GetInt(FileInputFormat.ListStatusNumThreads, FileInputFormat
				.DefaultListStatusNumThreads);
			rawExec = Executors.NewFixedThreadPool(numThreads, new ThreadFactoryBuilder().SetDaemon
				(true).SetNameFormat("GetFileInfo #%d").Build());
			exec = MoreExecutors.ListeningDecorator(rawExec);
			resultQueue = new LinkedBlockingQueue<IList<FileStatus>>();
			this.conf = conf;
			this.inputDirs = dirs;
			this.recursive = recursive;
			this.inputFilter = inputFilter;
			this.newApi = newApi;
		}

		/// <summary>Start executing and return FileStatuses based on the parameters specified
		/// 	</summary>
		/// <returns>fetched file statuses</returns>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual IEnumerable<FileStatus> GetFileStatuses()
		{
			// Increment to make sure a race between the first thread completing and the
			// rest being scheduled does not lead to a termination.
			runningTasks.IncrementAndGet();
			foreach (Path p in inputDirs)
			{
				runningTasks.IncrementAndGet();
				ListenableFuture<LocatedFileStatusFetcher.ProcessInitialInputPathCallable.Result>
					 future = exec.Submit(new LocatedFileStatusFetcher.ProcessInitialInputPathCallable
					(p, conf, inputFilter));
				Futures.AddCallback(future, processInitialInputPathCallback);
			}
			runningTasks.DecrementAndGet();
			Lock.Lock();
			try
			{
				while (runningTasks.Get() != 0 && unknownError == null)
				{
					condition.Await();
				}
			}
			finally
			{
				Lock.Unlock();
			}
			this.exec.ShutdownNow();
			if (this.unknownError != null)
			{
				if (this.unknownError is Error)
				{
					throw (Error)this.unknownError;
				}
				else
				{
					if (this.unknownError is RuntimeException)
					{
						throw (RuntimeException)this.unknownError;
					}
					else
					{
						if (this.unknownError is IOException)
						{
							throw (IOException)this.unknownError;
						}
						else
						{
							if (this.unknownError is Exception)
							{
								throw (Exception)this.unknownError;
							}
							else
							{
								throw new IOException(this.unknownError);
							}
						}
					}
				}
			}
			if (this.invalidInputErrors.Count != 0)
			{
				if (this.newApi)
				{
					throw new InvalidInputException(invalidInputErrors);
				}
				else
				{
					throw new InvalidInputException(invalidInputErrors);
				}
			}
			return Iterables.Concat(resultQueue);
		}

		/// <summary>Collect misconfigured Input errors.</summary>
		/// <remarks>
		/// Collect misconfigured Input errors. Errors while actually reading file info
		/// are reported immediately
		/// </remarks>
		private void RegisterInvalidInputError(IList<IOException> errors)
		{
			lock (this)
			{
				Sharpen.Collections.AddAll(this.invalidInputErrors, errors);
			}
		}

		/// <summary>
		/// Register fatal errors - example an IOException while accessing a file or a
		/// full exection queue
		/// </summary>
		private void RegisterError(Exception t)
		{
			Lock.Lock();
			try
			{
				if (unknownError != null)
				{
					unknownError = t;
					condition.Signal();
				}
			}
			finally
			{
				Lock.Unlock();
			}
		}

		private void DecrementRunningAndCheckCompletion()
		{
			Lock.Lock();
			try
			{
				if (runningTasks.DecrementAndGet() == 0)
				{
					condition.Signal();
				}
			}
			finally
			{
				Lock.Unlock();
			}
		}

		/// <summary>
		/// Retrieves block locations for the given @link
		/// <see cref="Org.Apache.Hadoop.FS.FileStatus"/>
		/// , and adds
		/// additional paths to the process queue if required.
		/// </summary>
		private class ProcessInputDirCallable : Callable<LocatedFileStatusFetcher.ProcessInputDirCallable.Result
			>
		{
			private readonly FileSystem fs;

			private readonly FileStatus fileStatus;

			private readonly bool recursive;

			private readonly PathFilter inputFilter;

			internal ProcessInputDirCallable(FileSystem fs, FileStatus fileStatus, bool recursive
				, PathFilter inputFilter)
			{
				this.fs = fs;
				this.fileStatus = fileStatus;
				this.recursive = recursive;
				this.inputFilter = inputFilter;
			}

			/// <exception cref="System.Exception"/>
			public virtual LocatedFileStatusFetcher.ProcessInputDirCallable.Result Call()
			{
				LocatedFileStatusFetcher.ProcessInputDirCallable.Result result = new LocatedFileStatusFetcher.ProcessInputDirCallable.Result
					();
				result.fs = fs;
				if (fileStatus.IsDirectory())
				{
					RemoteIterator<LocatedFileStatus> iter = fs.ListLocatedStatus(fileStatus.GetPath(
						));
					while (iter.HasNext())
					{
						LocatedFileStatus stat = iter.Next();
						if (inputFilter.Accept(stat.GetPath()))
						{
							if (recursive && stat.IsDirectory())
							{
								result.dirsNeedingRecursiveCalls.AddItem(stat);
							}
							else
							{
								result.locatedFileStatuses.AddItem(stat);
							}
						}
					}
				}
				else
				{
					result.locatedFileStatuses.AddItem(fileStatus);
				}
				return result;
			}

			private class Result
			{
				private IList<FileStatus> locatedFileStatuses = new List<FileStatus>();

				private IList<FileStatus> dirsNeedingRecursiveCalls = new List<FileStatus>();

				private FileSystem fs;
			}
		}

		/// <summary>
		/// The callback handler to handle results generated by
		/// <see cref="ProcessInputDirCallable"/>
		/// . This populates the final result set.
		/// </summary>
		private class ProcessInputDirCallback : FutureCallback<LocatedFileStatusFetcher.ProcessInputDirCallable.Result
			>
		{
			public virtual void OnSuccess(LocatedFileStatusFetcher.ProcessInputDirCallable.Result
				 result)
			{
				try
				{
					if (result.locatedFileStatuses.Count != 0)
					{
						this._enclosing.resultQueue.AddItem(result.locatedFileStatuses);
					}
					if (result.dirsNeedingRecursiveCalls.Count != 0)
					{
						foreach (FileStatus fileStatus in result.dirsNeedingRecursiveCalls)
						{
							this._enclosing.runningTasks.IncrementAndGet();
							ListenableFuture<LocatedFileStatusFetcher.ProcessInputDirCallable.Result> future = 
								this._enclosing.exec.Submit(new LocatedFileStatusFetcher.ProcessInputDirCallable
								(result.fs, fileStatus, this._enclosing.recursive, this._enclosing.inputFilter));
							Futures.AddCallback(future, this._enclosing.processInputDirCallback);
						}
					}
					this._enclosing.DecrementRunningAndCheckCompletion();
				}
				catch (Exception t)
				{
					// Error within the callback itself.
					this._enclosing.RegisterError(t);
				}
			}

			public virtual void OnFailure(Exception t)
			{
				// Any generated exceptions. Leads to immediate termination.
				this._enclosing.RegisterError(t);
			}

			internal ProcessInputDirCallback(LocatedFileStatusFetcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly LocatedFileStatusFetcher _enclosing;
		}

		/// <summary>
		/// Processes an initial Input Path pattern through the globber and PathFilter
		/// to generate a list of files which need further processing.
		/// </summary>
		private class ProcessInitialInputPathCallable : Callable<LocatedFileStatusFetcher.ProcessInitialInputPathCallable.Result
			>
		{
			private readonly Path path;

			private readonly Configuration conf;

			private readonly PathFilter inputFilter;

			public ProcessInitialInputPathCallable(Path path, Configuration conf, PathFilter 
				pathFilter)
			{
				this.path = path;
				this.conf = conf;
				this.inputFilter = pathFilter;
			}

			/// <exception cref="System.Exception"/>
			public virtual LocatedFileStatusFetcher.ProcessInitialInputPathCallable.Result Call
				()
			{
				LocatedFileStatusFetcher.ProcessInitialInputPathCallable.Result result = new LocatedFileStatusFetcher.ProcessInitialInputPathCallable.Result
					();
				FileSystem fs = path.GetFileSystem(conf);
				result.fs = fs;
				FileStatus[] matches = fs.GlobStatus(path, inputFilter);
				if (matches == null)
				{
					result.AddError(new IOException("Input path does not exist: " + path));
				}
				else
				{
					if (matches.Length == 0)
					{
						result.AddError(new IOException("Input Pattern " + path + " matches 0 files"));
					}
					else
					{
						result.matchedFileStatuses = matches;
					}
				}
				return result;
			}

			private class Result
			{
				private IList<IOException> errors;

				private FileStatus[] matchedFileStatuses;

				private FileSystem fs;

				internal virtual void AddError(IOException ioe)
				{
					if (errors == null)
					{
						errors = new List<IOException>();
					}
					errors.AddItem(ioe);
				}
			}
		}

		/// <summary>
		/// The callback handler to handle results generated by
		/// <see cref="ProcessInitialInputPathCallable"/>
		/// </summary>
		private class ProcessInitialInputPathCallback : FutureCallback<LocatedFileStatusFetcher.ProcessInitialInputPathCallable.Result
			>
		{
			public virtual void OnSuccess(LocatedFileStatusFetcher.ProcessInitialInputPathCallable.Result
				 result)
			{
				try
				{
					if (result.errors != null)
					{
						this._enclosing.RegisterInvalidInputError(result.errors);
					}
					if (result.matchedFileStatuses != null)
					{
						foreach (FileStatus matched in result.matchedFileStatuses)
						{
							this._enclosing.runningTasks.IncrementAndGet();
							ListenableFuture<LocatedFileStatusFetcher.ProcessInputDirCallable.Result> future = 
								this._enclosing.exec.Submit(new LocatedFileStatusFetcher.ProcessInputDirCallable
								(result.fs, matched, this._enclosing.recursive, this._enclosing.inputFilter));
							Futures.AddCallback(future, this._enclosing.processInputDirCallback);
						}
					}
					this._enclosing.DecrementRunningAndCheckCompletion();
				}
				catch (Exception t)
				{
					// Exception within the callback
					this._enclosing.RegisterError(t);
				}
			}

			public virtual void OnFailure(Exception t)
			{
				// Any generated exceptions. Leads to immediate termination.
				this._enclosing.RegisterError(t);
			}

			internal ProcessInitialInputPathCallback(LocatedFileStatusFetcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly LocatedFileStatusFetcher _enclosing;
		}
	}
}
