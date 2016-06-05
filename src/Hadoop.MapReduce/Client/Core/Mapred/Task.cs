using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Reduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Base class for tasks.</summary>
	public abstract class Task : Writable, Configurable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.Task
			));

		public static string MergedOutputPrefix = ".merged";

		public const long DefaultCombineRecordsBeforeProgress = 10000;

		[System.ObsoleteAttribute(@"Provided for compatibility. Use TaskCounter instead."
			)]
		public enum Counter
		{
			MapInputRecords,
			MapOutputRecords,
			MapSkippedRecords,
			MapInputBytes,
			MapOutputBytes,
			MapOutputMaterializedBytes,
			CombineInputRecords,
			CombineOutputRecords,
			ReduceInputGroups,
			ReduceShuffleBytes,
			ReduceInputRecords,
			ReduceOutputRecords,
			ReduceSkippedGroups,
			ReduceSkippedRecords,
			SpilledRecords,
			SplitRawBytes,
			CpuMilliseconds,
			PhysicalMemoryBytes,
			VirtualMemoryBytes,
			CommittedHeapBytes
		}

		/// <summary>Counters to measure the usage of the different file systems.</summary>
		/// <remarks>
		/// Counters to measure the usage of the different file systems.
		/// Always return the String array with two elements. First one is the name of
		/// BYTES_READ counter and second one is of the BYTES_WRITTEN counter.
		/// </remarks>
		protected internal static string[] GetFileSystemCounterNames(string uriScheme)
		{
			string scheme = StringUtils.ToUpperCase(uriScheme);
			return new string[] { scheme + "_BYTES_READ", scheme + "_BYTES_WRITTEN" };
		}

		/// <summary>Name of the FileSystem counters' group</summary>
		protected internal const string FilesystemCounterGroup = "FileSystemCounters";

		/// <summary>
		/// Construct output file names so that, when an output directory listing is
		/// sorted lexicographically, positions correspond to output partitions.
		/// </summary>
		private static readonly NumberFormat NumberFormat = NumberFormat.GetInstance();

		static Task()
		{
			///////////////////////////////////////////////////////////
			// Helper methods to construct task-output paths
			///////////////////////////////////////////////////////////
			NumberFormat.SetMinimumIntegerDigits(5);
			NumberFormat.SetGroupingUsed(false);
		}

		internal static string GetOutputName(int partition)
		{
			lock (typeof(Task))
			{
				return "part-" + NumberFormat.Format(partition);
			}
		}

		private string jobFile;

		private string user;

		private TaskAttemptID taskId;

		private int partition;

		private byte[] encryptedSpillKey = new byte[] { 0 };

		internal TaskStatus taskStatus;

		protected internal JobStatus.State jobRunStateForCleanup;

		protected internal bool jobCleanup = false;

		protected internal bool jobSetup = false;

		protected internal bool taskCleanup = false;

		protected internal BytesWritable extraData = new BytesWritable();

		private SortedRanges skipRanges = new SortedRanges();

		private bool skipping = false;

		private bool writeSkipRecs = true;

		private volatile long currentRecStartIndex;

		private IEnumerator<long> currentRecIndexIterator = skipRanges.SkipRangeIterator(
			);

		private ResourceCalculatorProcessTree pTree;

		private long initCpuCumulativeTime = ResourceCalculatorProcessTree.Unavailable;

		protected internal JobConf conf;

		protected internal MapOutputFile mapOutputFile;

		protected internal LocalDirAllocator lDirAlloc;

		private const int MaxRetries = 10;

		protected internal JobContext jobContext;

		protected internal TaskAttemptContext taskContext;

		protected internal OutputFormat<object, object> outputFormat;

		protected internal OutputCommitter committer;

		protected internal readonly Counters.Counter spilledRecordsCounter;

		protected internal readonly Counters.Counter failedShuffleCounter;

		protected internal readonly Counters.Counter mergedMapOutputsCounter;

		private int numSlotsRequired;

		protected internal TaskUmbilicalProtocol umbilical;

		protected internal SecretKey tokenSecret;

		protected internal SecretKey shuffleSecret;

		protected internal Task.GcTimeUpdater gcUpdater;

		public Task()
		{
			////////////////////////////////////////////
			// Fields
			////////////////////////////////////////////
			// job configuration file
			// user running the job
			// unique, includes job id
			// id within job
			// Key Used to encrypt
			// intermediate spills
			// current status of the task
			// An opaque data field used to attach extra data to each task. This is used
			// by the Hadoop scheduler for Mesos to associate a Mesos task ID with each
			// task and recover these IDs on the TaskTracker.
			//skip ranges based on failed ranges from previous attempts
			//currently processing record start index
			////////////////////////////////////////////
			// Constructors
			////////////////////////////////////////////
			taskStatus = TaskStatus.CreateTaskStatus(IsMapTask());
			taskId = new TaskAttemptID();
			spilledRecordsCounter = counters.FindCounter(TaskCounter.SpilledRecords);
			failedShuffleCounter = counters.FindCounter(TaskCounter.FailedShuffle);
			mergedMapOutputsCounter = counters.FindCounter(TaskCounter.MergedMapOutputs);
			gcUpdater = new Task.GcTimeUpdater(this);
		}

		public Task(string jobFile, TaskAttemptID taskId, int partition, int numSlotsRequired
			)
		{
			this.jobFile = jobFile;
			this.taskId = taskId;
			this.partition = partition;
			this.numSlotsRequired = numSlotsRequired;
			this.taskStatus = TaskStatus.CreateTaskStatus(IsMapTask(), this.taskId, 0.0f, numSlotsRequired
				, TaskStatus.State.Unassigned, string.Empty, string.Empty, string.Empty, IsMapTask
				() ? TaskStatus.Phase.Map : TaskStatus.Phase.Shuffle, counters);
			spilledRecordsCounter = counters.FindCounter(TaskCounter.SpilledRecords);
			failedShuffleCounter = counters.FindCounter(TaskCounter.FailedShuffle);
			mergedMapOutputsCounter = counters.FindCounter(TaskCounter.MergedMapOutputs);
			gcUpdater = new Task.GcTimeUpdater(this);
		}

		////////////////////////////////////////////
		// Accessors
		////////////////////////////////////////////
		public virtual void SetJobFile(string jobFile)
		{
			this.jobFile = jobFile;
		}

		public virtual string GetJobFile()
		{
			return jobFile;
		}

		public virtual TaskAttemptID GetTaskID()
		{
			return taskId;
		}

		public virtual int GetNumSlotsRequired()
		{
			return numSlotsRequired;
		}

		internal virtual Counters GetCounters()
		{
			return counters;
		}

		/// <summary>Get the job name for this task.</summary>
		/// <returns>the job name</returns>
		public virtual JobID GetJobID()
		{
			return ((JobID)taskId.GetJobID());
		}

		/// <summary>Set the job token secret</summary>
		/// <param name="tokenSecret">the secret</param>
		public virtual void SetJobTokenSecret(SecretKey tokenSecret)
		{
			this.tokenSecret = tokenSecret;
		}

		/// <summary>Get Encrypted spill key</summary>
		/// <returns>encrypted spill key</returns>
		public virtual byte[] GetEncryptedSpillKey()
		{
			return encryptedSpillKey;
		}

		/// <summary>Set Encrypted spill key</summary>
		/// <param name="encryptedSpillKey">key</param>
		public virtual void SetEncryptedSpillKey(byte[] encryptedSpillKey)
		{
			if (encryptedSpillKey != null)
			{
				this.encryptedSpillKey = encryptedSpillKey;
			}
		}

		/// <summary>Get the job token secret</summary>
		/// <returns>the token secret</returns>
		public virtual SecretKey GetJobTokenSecret()
		{
			return this.tokenSecret;
		}

		/// <summary>Set the secret key used to authenticate the shuffle</summary>
		/// <param name="shuffleSecret">the secret</param>
		public virtual void SetShuffleSecret(SecretKey shuffleSecret)
		{
			this.shuffleSecret = shuffleSecret;
		}

		/// <summary>Get the secret key used to authenticate the shuffle</summary>
		/// <returns>the shuffle secret</returns>
		public virtual SecretKey GetShuffleSecret()
		{
			return this.shuffleSecret;
		}

		/// <summary>Get the index of this task within the job.</summary>
		/// <returns>the integer part of the task id</returns>
		public virtual int GetPartition()
		{
			return partition;
		}

		/// <summary>Return current phase of the task.</summary>
		/// <remarks>
		/// Return current phase of the task.
		/// needs to be synchronized as communication thread sends the phase every second
		/// </remarks>
		/// <returns>the curent phase of the task</returns>
		public virtual TaskStatus.Phase GetPhase()
		{
			lock (this)
			{
				return this.taskStatus.GetPhase();
			}
		}

		/// <summary>Set current phase of the task.</summary>
		/// <param name="phase">task phase</param>
		protected internal virtual void SetPhase(TaskStatus.Phase phase)
		{
			lock (this)
			{
				this.taskStatus.SetPhase(phase);
			}
		}

		/// <summary>Get whether to write skip records.</summary>
		protected internal virtual bool ToWriteSkipRecs()
		{
			return writeSkipRecs;
		}

		/// <summary>Set whether to write skip records.</summary>
		protected internal virtual void SetWriteSkipRecs(bool writeSkipRecs)
		{
			this.writeSkipRecs = writeSkipRecs;
		}

		/// <summary>Report a fatal error to the parent (task) tracker.</summary>
		protected internal virtual void ReportFatalError(TaskAttemptID id, Exception throwable
			, string logMsg)
		{
			Log.Fatal(logMsg);
			if (ShutdownHookManager.Get().IsShutdownInProgress())
			{
				return;
			}
			Exception tCause = throwable.InnerException;
			string cause = tCause == null ? StringUtils.StringifyException(throwable) : StringUtils
				.StringifyException(tCause);
			try
			{
				umbilical.FatalError(id, cause);
			}
			catch (IOException ioe)
			{
				Log.Fatal("Failed to contact the tasktracker", ioe);
				System.Environment.Exit(-1);
			}
		}

		/// <summary>
		/// Gets a handle to the Statistics instance based on the scheme associated
		/// with path.
		/// </summary>
		/// <param name="path">the path.</param>
		/// <param name="conf">
		/// the configuration to extract the scheme from if not part of
		/// the path.
		/// </param>
		/// <returns>a Statistics instance, or null if none is found for the scheme.</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal static IList<FileSystem.Statistics> GetFsStatistics(Path path, 
			Configuration conf)
		{
			IList<FileSystem.Statistics> matchedStats = new AList<FileSystem.Statistics>();
			path = path.GetFileSystem(conf).MakeQualified(path);
			string scheme = path.ToUri().GetScheme();
			foreach (FileSystem.Statistics stats in FileSystem.GetAllStatistics())
			{
				if (stats.GetScheme().Equals(scheme))
				{
					matchedStats.AddItem(stats);
				}
			}
			return matchedStats;
		}

		/// <summary>Get skipRanges.</summary>
		public virtual SortedRanges GetSkipRanges()
		{
			return skipRanges;
		}

		/// <summary>Set skipRanges.</summary>
		public virtual void SetSkipRanges(SortedRanges skipRanges)
		{
			this.skipRanges = skipRanges;
		}

		/// <summary>Is Task in skipping mode.</summary>
		public virtual bool IsSkipping()
		{
			return skipping;
		}

		/// <summary>Sets whether to run Task in skipping mode.</summary>
		/// <param name="skipping"/>
		public virtual void SetSkipping(bool skipping)
		{
			this.skipping = skipping;
		}

		/// <summary>Return current state of the task.</summary>
		/// <remarks>
		/// Return current state of the task.
		/// needs to be synchronized as communication thread
		/// sends the state every second
		/// </remarks>
		/// <returns>task state</returns>
		internal virtual TaskStatus.State GetState()
		{
			lock (this)
			{
				return this.taskStatus.GetRunState();
			}
		}

		/// <summary>Set current state of the task.</summary>
		/// <param name="state"/>
		internal virtual void SetState(TaskStatus.State state)
		{
			lock (this)
			{
				this.taskStatus.SetRunState(state);
			}
		}

		internal virtual void SetTaskCleanupTask()
		{
			taskCleanup = true;
		}

		internal virtual bool IsTaskCleanupTask()
		{
			return taskCleanup;
		}

		internal virtual bool IsJobCleanupTask()
		{
			return jobCleanup;
		}

		internal virtual bool IsJobAbortTask()
		{
			// the task is an abort task if its marked for cleanup and the final 
			// expected state is either failed or killed.
			return IsJobCleanupTask() && (jobRunStateForCleanup == JobStatus.State.Killed || 
				jobRunStateForCleanup == JobStatus.State.Failed);
		}

		internal virtual bool IsJobSetupTask()
		{
			return jobSetup;
		}

		internal virtual void SetJobSetupTask()
		{
			jobSetup = true;
		}

		internal virtual void SetJobCleanupTask()
		{
			jobCleanup = true;
		}

		/// <summary>Sets the task to do job abort in the cleanup.</summary>
		/// <param name="status">the final runstate of the job.</param>
		internal virtual void SetJobCleanupTaskState(JobStatus.State status)
		{
			jobRunStateForCleanup = status;
		}

		internal virtual bool IsMapOrReduce()
		{
			return !jobSetup && !jobCleanup && !taskCleanup;
		}

		/// <summary>Get the name of the user running the job/task.</summary>
		/// <remarks>
		/// Get the name of the user running the job/task. TaskTracker needs task's
		/// user name even before it's JobConf is localized. So we explicitly serialize
		/// the user name.
		/// </remarks>
		/// <returns>user</returns>
		internal virtual string GetUser()
		{
			return user;
		}

		internal virtual void SetUser(string user)
		{
			this.user = user;
		}

		////////////////////////////////////////////
		// Writable methods
		////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			Text.WriteString(@out, jobFile);
			taskId.Write(@out);
			@out.WriteInt(partition);
			@out.WriteInt(numSlotsRequired);
			taskStatus.Write(@out);
			skipRanges.Write(@out);
			@out.WriteBoolean(skipping);
			@out.WriteBoolean(jobCleanup);
			if (jobCleanup)
			{
				WritableUtils.WriteEnum(@out, jobRunStateForCleanup);
			}
			@out.WriteBoolean(jobSetup);
			@out.WriteBoolean(writeSkipRecs);
			@out.WriteBoolean(taskCleanup);
			Text.WriteString(@out, user);
			@out.WriteInt(encryptedSpillKey.Length);
			extraData.Write(@out);
			@out.Write(encryptedSpillKey);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			jobFile = StringInterner.WeakIntern(Text.ReadString(@in));
			taskId = TaskAttemptID.Read(@in);
			partition = @in.ReadInt();
			numSlotsRequired = @in.ReadInt();
			taskStatus.ReadFields(@in);
			skipRanges.ReadFields(@in);
			currentRecIndexIterator = skipRanges.SkipRangeIterator();
			currentRecStartIndex = currentRecIndexIterator.Next();
			skipping = @in.ReadBoolean();
			jobCleanup = @in.ReadBoolean();
			if (jobCleanup)
			{
				jobRunStateForCleanup = WritableUtils.ReadEnum<JobStatus.State>(@in);
			}
			jobSetup = @in.ReadBoolean();
			writeSkipRecs = @in.ReadBoolean();
			taskCleanup = @in.ReadBoolean();
			if (taskCleanup)
			{
				SetPhase(TaskStatus.Phase.Cleanup);
			}
			user = StringInterner.WeakIntern(Text.ReadString(@in));
			int len = @in.ReadInt();
			encryptedSpillKey = new byte[len];
			extraData.ReadFields(@in);
			@in.ReadFully(encryptedSpillKey);
		}

		public override string ToString()
		{
			return taskId.ToString();
		}

		/// <summary>Localize the given JobConf to be specific for this task.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void LocalizeConfiguration(JobConf conf)
		{
			conf.Set(JobContext.TaskId, ((TaskID)taskId.GetTaskID()).ToString());
			conf.Set(JobContext.TaskAttemptId, taskId.ToString());
			conf.SetBoolean(JobContext.TaskIsmap, IsMapTask());
			conf.SetInt(JobContext.TaskPartition, partition);
			conf.Set(JobContext.Id, ((JobID)taskId.GetJobID()).ToString());
		}

		/// <summary>Run this task as a part of the named job.</summary>
		/// <remarks>
		/// Run this task as a part of the named job.  This method is executed in the
		/// child process and is what invokes user-supplied map, reduce, etc. methods.
		/// </remarks>
		/// <param name="umbilical">for progress reports</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		public abstract void Run(JobConf job, TaskUmbilicalProtocol umbilical);

		/// <summary>The number of milliseconds between progress reports.</summary>
		public const int ProgressInterval = 3000;

		[System.NonSerialized]
		private Progress taskProgress = new Progress();

		[System.NonSerialized]
		private Counters counters = new Counters();

		private AtomicBoolean taskDone = new AtomicBoolean(false);

		// Current counters
		/* flag to track whether task is done */
		public abstract bool IsMapTask();

		public virtual Progress GetProgress()
		{
			return taskProgress;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Initialize(JobConf job, JobID id, Reporter reporter, bool useNewApi
			)
		{
			jobContext = new JobContextImpl(job, id, reporter);
			taskContext = new TaskAttemptContextImpl(job, taskId, reporter);
			if (GetState() == TaskStatus.State.Unassigned)
			{
				SetState(TaskStatus.State.Running);
			}
			if (useNewApi)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("using new api for output committer");
				}
				outputFormat = ReflectionUtils.NewInstance(taskContext.GetOutputFormatClass(), job
					);
				committer = outputFormat.GetOutputCommitter(taskContext);
			}
			else
			{
				committer = conf.GetOutputCommitter();
			}
			Path outputPath = FileOutputFormat.GetOutputPath(conf);
			if (outputPath != null)
			{
				if ((committer is FileOutputCommitter))
				{
					FileOutputFormat.SetWorkOutputPath(conf, ((FileOutputCommitter)committer).GetTaskAttemptPath
						(taskContext));
				}
				else
				{
					FileOutputFormat.SetWorkOutputPath(conf, outputPath);
				}
			}
			committer.SetupTask(taskContext);
			Type clazz = conf.GetClass<ResourceCalculatorProcessTree>(MRConfig.ResourceCalculatorProcessTree
				, null);
			pTree = ResourceCalculatorProcessTree.GetResourceCalculatorProcessTree(Sharpen.Runtime.GetEnv
				()["JVM_PID"], clazz, conf);
			Log.Info(" Using ResourceCalculatorProcessTree : " + pTree);
			if (pTree != null)
			{
				pTree.UpdateProcessTree();
				initCpuCumulativeTime = pTree.GetCumulativeCpuTime();
			}
		}

		public static string NormalizeStatus(string status, Configuration conf)
		{
			// Check to see if the status string is too long
			// and truncate it if needed.
			int progressStatusLength = conf.GetInt(MRConfig.ProgressStatusLenLimitKey, MRConfig
				.ProgressStatusLenLimitDefault);
			if (status.Length > progressStatusLength)
			{
				Log.Warn("Task status: \"" + status + "\" truncated to max limit (" + progressStatusLength
					 + " characters)");
				status = Sharpen.Runtime.Substring(status, 0, progressStatusLength);
			}
			return status;
		}

		public class TaskReporter : StatusReporter, Runnable, Reporter
		{
			private TaskUmbilicalProtocol umbilical;

			private InputSplit split = null;

			private Org.Apache.Hadoop.Util.Progress taskProgress;

			private Sharpen.Thread pingThread = null;

			private bool done = true;

			private object Lock = new object();

			/// <summary>flag that indicates whether progress update needs to be sent to parent.</summary>
			/// <remarks>
			/// flag that indicates whether progress update needs to be sent to parent.
			/// If true, it has been set. If false, it has been reset.
			/// Using AtomicBoolean since we need an atomic read & reset method.
			/// </remarks>
			private AtomicBoolean progressFlag = new AtomicBoolean(false);

			internal TaskReporter(Task _enclosing, Org.Apache.Hadoop.Util.Progress taskProgress
				, TaskUmbilicalProtocol umbilical)
			{
				this._enclosing = _enclosing;
				this.umbilical = umbilical;
				this.taskProgress = taskProgress;
			}

			// getters and setters for flag
			internal virtual void SetProgressFlag()
			{
				this.progressFlag.Set(true);
			}

			internal virtual bool ResetProgressFlag()
			{
				return this.progressFlag.GetAndSet(false);
			}

			public override void SetStatus(string status)
			{
				this.taskProgress.SetStatus(Task.NormalizeStatus(status, this._enclosing.conf));
				// indicate that progress update needs to be sent
				this.SetProgressFlag();
			}

			public virtual void SetProgress(float progress)
			{
				// set current phase progress.
				// This method assumes that task has phases.
				this.taskProgress.Phase().Set(progress);
				// indicate that progress update needs to be sent
				this.SetProgressFlag();
			}

			public override float GetProgress()
			{
				return this.taskProgress.GetProgress();
			}

			public override void Progress()
			{
				// indicate that progress update needs to be sent
				this.SetProgressFlag();
			}

			public override Counter GetCounter(string group, string name)
			{
				Counters.Counter counter = null;
				if (this._enclosing.counters != null)
				{
					counter = this._enclosing.counters.FindCounter(group, name);
				}
				return counter;
			}

			public override Counter GetCounter<_T0>(Enum<_T0> name)
			{
				return this._enclosing.counters == null ? null : this._enclosing.counters.FindCounter
					(name);
			}

			public virtual void IncrCounter(Enum key, long amount)
			{
				if (this._enclosing.counters != null)
				{
					this._enclosing.counters.IncrCounter(key, amount);
				}
				this.SetProgressFlag();
			}

			public virtual void IncrCounter(string group, string counter, long amount)
			{
				if (this._enclosing.counters != null)
				{
					this._enclosing.counters.IncrCounter(group, counter, amount);
				}
				if (this._enclosing.skipping && SkipBadRecords.CounterGroup.Equals(group) && (SkipBadRecords
					.CounterMapProcessedRecords.Equals(counter) || SkipBadRecords.CounterReduceProcessedGroups
					.Equals(counter)))
				{
					//if application reports the processed records, move the 
					//currentRecStartIndex to the next.
					//currentRecStartIndex is the start index which has not yet been 
					//finished and is still in task's stomach.
					for (int i = 0; i < amount; i++)
					{
						this._enclosing.currentRecStartIndex = this._enclosing.currentRecIndexIterator.Next
							();
					}
				}
				this.SetProgressFlag();
			}

			public virtual void SetInputSplit(InputSplit split)
			{
				this.split = split;
			}

			/// <exception cref="System.NotSupportedException"/>
			public virtual InputSplit GetInputSplit()
			{
				if (this.split == null)
				{
					throw new NotSupportedException("Input only available on map");
				}
				else
				{
					return this.split;
				}
			}

			/// <summary>The communication thread handles communication with the parent (Task Tracker).
			/// 	</summary>
			/// <remarks>
			/// The communication thread handles communication with the parent (Task Tracker).
			/// It sends progress updates if progress has been made or if the task needs to
			/// let the parent know that it's alive. It also pings the parent to see if it's alive.
			/// </remarks>
			public virtual void Run()
			{
				int MaxRetries = 3;
				int remainingRetries = MaxRetries;
				// get current flag value and reset it as well
				bool sendProgress = this.ResetProgressFlag();
				while (!this._enclosing.taskDone.Get())
				{
					lock (this.Lock)
					{
						this.done = false;
					}
					try
					{
						bool taskFound = true;
						// whether TT knows about this task
						// sleep for a bit
						lock (this.Lock)
						{
							if (this._enclosing.taskDone.Get())
							{
								break;
							}
							Sharpen.Runtime.Wait(this.Lock, Task.ProgressInterval);
						}
						if (this._enclosing.taskDone.Get())
						{
							break;
						}
						if (sendProgress)
						{
							// we need to send progress update
							this._enclosing.UpdateCounters();
							this._enclosing.taskStatus.StatusUpdate(this.taskProgress.Get(), this.taskProgress
								.ToString(), this._enclosing.counters);
							taskFound = this.umbilical.StatusUpdate(this._enclosing.taskId, this._enclosing.taskStatus
								);
							this._enclosing.taskStatus.ClearStatus();
						}
						else
						{
							// send ping 
							taskFound = this.umbilical.Ping(this._enclosing.taskId);
						}
						// if Task Tracker is not aware of our task ID (probably because it died and 
						// came back up), kill ourselves
						if (!taskFound)
						{
							Task.Log.Warn("Parent died.  Exiting " + this._enclosing.taskId);
							this.ResetDoneFlag();
							System.Environment.Exit(66);
						}
						sendProgress = this.ResetProgressFlag();
						remainingRetries = MaxRetries;
					}
					catch (Exception t)
					{
						Task.Log.Info("Communication exception: " + StringUtils.StringifyException(t));
						remainingRetries -= 1;
						if (remainingRetries == 0)
						{
							ReflectionUtils.LogThreadInfo(Task.Log, "Communication exception", 0);
							Task.Log.Warn("Last retry, killing " + this._enclosing.taskId);
							this.ResetDoneFlag();
							System.Environment.Exit(65);
						}
					}
				}
				//Notify that we are done with the work
				this.ResetDoneFlag();
			}

			internal virtual void ResetDoneFlag()
			{
				lock (this.Lock)
				{
					this.done = true;
					Sharpen.Runtime.Notify(this.Lock);
				}
			}

			public virtual void StartCommunicationThread()
			{
				if (this.pingThread == null)
				{
					this.pingThread = new Sharpen.Thread(this, "communication thread");
					this.pingThread.SetDaemon(true);
					this.pingThread.Start();
				}
			}

			/// <exception cref="System.Exception"/>
			public virtual void StopCommunicationThread()
			{
				if (this.pingThread != null)
				{
					// Intent of the lock is to not send an interupt in the middle of an
					// umbilical.ping or umbilical.statusUpdate
					lock (this.Lock)
					{
						//Interrupt if sleeping. Otherwise wait for the RPC call to return.
						Sharpen.Runtime.Notify(this.Lock);
					}
					lock (this.Lock)
					{
						while (!this.done)
						{
							Sharpen.Runtime.Wait(this.Lock);
						}
					}
					this.pingThread.Interrupt();
					this.pingThread.Join();
				}
			}

			private readonly Task _enclosing;
		}

		/// <summary>Reports the next executing record range to TaskTracker.</summary>
		/// <param name="umbilical"/>
		/// <param name="nextRecIndex">the record index which would be fed next.</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void ReportNextRecordRange(TaskUmbilicalProtocol umbilical
			, long nextRecIndex)
		{
			//currentRecStartIndex is the start index which has not yet been finished 
			//and is still in task's stomach.
			long len = nextRecIndex - currentRecStartIndex + 1;
			SortedRanges.Range range = new SortedRanges.Range(currentRecStartIndex, len);
			taskStatus.SetNextRecordRange(range);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("sending reportNextRecordRange " + range);
			}
			umbilical.ReportNextRecordRange(taskId, range);
		}

		/// <summary>Create a TaskReporter and start communication thread</summary>
		internal virtual Task.TaskReporter StartReporter(TaskUmbilicalProtocol umbilical)
		{
			// start thread that will handle communication with parent
			Task.TaskReporter reporter = new Task.TaskReporter(this, GetProgress(), umbilical
				);
			reporter.StartCommunicationThread();
			return reporter;
		}

		/// <summary>Update resource information counters</summary>
		internal virtual void UpdateResourceCounters()
		{
			// Update generic resource counters
			UpdateHeapUsageCounter();
			// Updating resources specified in ResourceCalculatorProcessTree
			if (pTree == null)
			{
				return;
			}
			pTree.UpdateProcessTree();
			long cpuTime = pTree.GetCumulativeCpuTime();
			long pMem = pTree.GetRssMemorySize();
			long vMem = pTree.GetVirtualMemorySize();
			// Remove the CPU time consumed previously by JVM reuse
			if (cpuTime != ResourceCalculatorProcessTree.Unavailable && initCpuCumulativeTime
				 != ResourceCalculatorProcessTree.Unavailable)
			{
				cpuTime -= initCpuCumulativeTime;
			}
			if (cpuTime != ResourceCalculatorProcessTree.Unavailable)
			{
				counters.FindCounter(TaskCounter.CpuMilliseconds).SetValue(cpuTime);
			}
			if (pMem != ResourceCalculatorProcessTree.Unavailable)
			{
				counters.FindCounter(TaskCounter.PhysicalMemoryBytes).SetValue(pMem);
			}
			if (vMem != ResourceCalculatorProcessTree.Unavailable)
			{
				counters.FindCounter(TaskCounter.VirtualMemoryBytes).SetValue(vMem);
			}
		}

		/// <summary>An updater that tracks the amount of time this task has spent in GC.</summary>
		internal class GcTimeUpdater
		{
			private long lastGcMillis = 0;

			private IList<GarbageCollectorMXBean> gcBeans = null;

			public GcTimeUpdater(Task _enclosing)
			{
				this._enclosing = _enclosing;
				this.gcBeans = ManagementFactory.GetGarbageCollectorMXBeans();
				this.GetElapsedGc();
			}

			// Initialize 'lastGcMillis' with the current time spent.
			/// <returns>
			/// the number of milliseconds that the gc has used for CPU
			/// since the last time this method was called.
			/// </returns>
			protected internal virtual long GetElapsedGc()
			{
				long thisGcMillis = 0;
				foreach (GarbageCollectorMXBean gcBean in this.gcBeans)
				{
					thisGcMillis += gcBean.GetCollectionTime();
				}
				long delta = thisGcMillis - this.lastGcMillis;
				this.lastGcMillis = thisGcMillis;
				return delta;
			}

			/// <summary>Increment the gc-elapsed-time counter.</summary>
			public virtual void IncrementGcCounter()
			{
				if (null == this._enclosing.counters)
				{
					return;
				}
				// nothing to do.
				Counters.Counter gcCounter = this._enclosing.counters.FindCounter(TaskCounter.GcTimeMillis
					);
				if (null != gcCounter)
				{
					gcCounter.Increment(this.GetElapsedGc());
				}
			}

			private readonly Task _enclosing;
		}

		/// <summary>
		/// An updater that tracks the last number reported for a given file
		/// system and only creates the counters when they are needed.
		/// </summary>
		internal class FileSystemStatisticUpdater
		{
			private IList<FileSystem.Statistics> stats;

			private Counters.Counter readBytesCounter;

			private Counters.Counter writeBytesCounter;

			private Counters.Counter readOpsCounter;

			private Counters.Counter largeReadOpsCounter;

			private Counters.Counter writeOpsCounter;

			private string scheme;

			internal FileSystemStatisticUpdater(Task _enclosing, IList<FileSystem.Statistics>
				 stats, string scheme)
			{
				this._enclosing = _enclosing;
				this.stats = stats;
				this.scheme = scheme;
			}

			internal virtual void UpdateCounters()
			{
				if (this.readBytesCounter == null)
				{
					this.readBytesCounter = this._enclosing.counters.FindCounter(this.scheme, FileSystemCounter
						.BytesRead);
				}
				if (this.writeBytesCounter == null)
				{
					this.writeBytesCounter = this._enclosing.counters.FindCounter(this.scheme, FileSystemCounter
						.BytesWritten);
				}
				if (this.readOpsCounter == null)
				{
					this.readOpsCounter = this._enclosing.counters.FindCounter(this.scheme, FileSystemCounter
						.ReadOps);
				}
				if (this.largeReadOpsCounter == null)
				{
					this.largeReadOpsCounter = this._enclosing.counters.FindCounter(this.scheme, FileSystemCounter
						.LargeReadOps);
				}
				if (this.writeOpsCounter == null)
				{
					this.writeOpsCounter = this._enclosing.counters.FindCounter(this.scheme, FileSystemCounter
						.WriteOps);
				}
				long readBytes = 0;
				long writeBytes = 0;
				long readOps = 0;
				long largeReadOps = 0;
				long writeOps = 0;
				foreach (FileSystem.Statistics stat in this.stats)
				{
					readBytes = readBytes + stat.GetBytesRead();
					writeBytes = writeBytes + stat.GetBytesWritten();
					readOps = readOps + stat.GetReadOps();
					largeReadOps = largeReadOps + stat.GetLargeReadOps();
					writeOps = writeOps + stat.GetWriteOps();
				}
				this.readBytesCounter.SetValue(readBytes);
				this.writeBytesCounter.SetValue(writeBytes);
				this.readOpsCounter.SetValue(readOps);
				this.largeReadOpsCounter.SetValue(largeReadOps);
				this.writeOpsCounter.SetValue(writeOps);
			}

			private readonly Task _enclosing;
		}

		/// <summary>A Map where Key-&gt; URIScheme and value-&gt;FileSystemStatisticUpdater</summary>
		private IDictionary<string, Task.FileSystemStatisticUpdater> statisticUpdaters = 
			new Dictionary<string, Task.FileSystemStatisticUpdater>();

		private void UpdateCounters()
		{
			lock (this)
			{
				IDictionary<string, IList<FileSystem.Statistics>> map = new Dictionary<string, IList
					<FileSystem.Statistics>>();
				foreach (FileSystem.Statistics stat in FileSystem.GetAllStatistics())
				{
					string uriScheme = stat.GetScheme();
					if (map.Contains(uriScheme))
					{
						IList<FileSystem.Statistics> list = map[uriScheme];
						list.AddItem(stat);
					}
					else
					{
						IList<FileSystem.Statistics> list = new AList<FileSystem.Statistics>();
						list.AddItem(stat);
						map[uriScheme] = list;
					}
				}
				foreach (KeyValuePair<string, IList<FileSystem.Statistics>> entry in map)
				{
					Task.FileSystemStatisticUpdater updater = statisticUpdaters[entry.Key];
					if (updater == null)
					{
						//new FileSystem has been found in the cache
						updater = new Task.FileSystemStatisticUpdater(this, entry.Value, entry.Key);
						statisticUpdaters[entry.Key] = updater;
					}
					updater.UpdateCounters();
				}
				gcUpdater.IncrementGcCounter();
				UpdateResourceCounters();
			}
		}

		/// <summary>
		/// Updates the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.TaskCounter.CommittedHeapBytes"/>
		/// counter to reflect the
		/// current total committed heap space usage of this JVM.
		/// </summary>
		private void UpdateHeapUsageCounter()
		{
			long currentHeapUsage = Runtime.GetRuntime().TotalMemory();
			counters.FindCounter(TaskCounter.CommittedHeapBytes).SetValue(currentHeapUsage);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Done(TaskUmbilicalProtocol umbilical, Task.TaskReporter reporter
			)
		{
			Log.Info("Task:" + taskId + " is done." + " And is in the process of committing");
			UpdateCounters();
			bool commitRequired = IsCommitRequired();
			if (commitRequired)
			{
				int retries = MaxRetries;
				SetState(TaskStatus.State.CommitPending);
				// say the task tracker that task is commit pending
				while (true)
				{
					try
					{
						umbilical.CommitPending(taskId, taskStatus);
						break;
					}
					catch (Exception)
					{
					}
					catch (IOException ie)
					{
						// ignore
						Log.Warn("Failure sending commit pending: " + StringUtils.StringifyException(ie));
						if (--retries == 0)
						{
							System.Environment.Exit(67);
						}
					}
				}
				//wait for commit approval and commit
				Commit(umbilical, reporter, committer);
			}
			taskDone.Set(true);
			reporter.StopCommunicationThread();
			// Make sure we send at least one set of counter increments. It's
			// ok to call updateCounters() in this thread after comm thread stopped.
			UpdateCounters();
			SendLastUpdate(umbilical);
			//signal the tasktracker that we are done
			SendDone(umbilical);
		}

		/// <summary>
		/// Checks if this task has anything to commit, depending on the
		/// type of task, as well as on whether the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.OutputCommitter"/>
		/// has anything to commit.
		/// </summary>
		/// <returns>true if the task has to commit</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool IsCommitRequired()
		{
			bool commitRequired = false;
			if (IsMapOrReduce())
			{
				commitRequired = committer.NeedsTaskCommit(taskContext);
			}
			return commitRequired;
		}

		/// <summary>Send a status update to the task tracker</summary>
		/// <param name="umbilical"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void StatusUpdate(TaskUmbilicalProtocol umbilical)
		{
			int retries = MaxRetries;
			while (true)
			{
				try
				{
					if (!umbilical.StatusUpdate(GetTaskID(), taskStatus))
					{
						Log.Warn("Parent died.  Exiting " + taskId);
						System.Environment.Exit(66);
					}
					taskStatus.ClearStatus();
					return;
				}
				catch (Exception)
				{
					Sharpen.Thread.CurrentThread().Interrupt();
				}
				catch (IOException ie)
				{
					// interrupt ourself
					Log.Warn("Failure sending status update: " + StringUtils.StringifyException(ie));
					if (--retries == 0)
					{
						throw;
					}
				}
			}
		}

		/// <summary>Sends last status update before sending umbilical.done();</summary>
		/// <exception cref="System.IO.IOException"/>
		private void SendLastUpdate(TaskUmbilicalProtocol umbilical)
		{
			taskStatus.SetOutputSize(CalculateOutputSize());
			// send a final status report
			taskStatus.StatusUpdate(taskProgress.Get(), taskProgress.ToString(), counters);
			StatusUpdate(umbilical);
		}

		/// <summary>Calculates the size of output for this task.</summary>
		/// <returns>-1 if it can't be found.</returns>
		/// <exception cref="System.IO.IOException"/>
		private long CalculateOutputSize()
		{
			if (!IsMapOrReduce())
			{
				return -1;
			}
			if (IsMapTask() && conf.GetNumReduceTasks() > 0)
			{
				try
				{
					Path mapOutput = mapOutputFile.GetOutputFile();
					FileSystem localFS = FileSystem.GetLocal(conf);
					return localFS.GetFileStatus(mapOutput).GetLen();
				}
				catch (IOException e)
				{
					Log.Warn("Could not find output size ", e);
				}
			}
			return -1;
		}

		/// <exception cref="System.IO.IOException"/>
		private void SendDone(TaskUmbilicalProtocol umbilical)
		{
			int retries = MaxRetries;
			while (true)
			{
				try
				{
					umbilical.Done(GetTaskID());
					Log.Info("Task '" + taskId + "' done.");
					return;
				}
				catch (IOException ie)
				{
					Log.Warn("Failure signalling completion: " + StringUtils.StringifyException(ie));
					if (--retries == 0)
					{
						throw;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void Commit(TaskUmbilicalProtocol umbilical, Task.TaskReporter reporter, 
			OutputCommitter committer)
		{
			int retries = MaxRetries;
			while (true)
			{
				try
				{
					while (!umbilical.CanCommit(taskId))
					{
						try
						{
							Sharpen.Thread.Sleep(1000);
						}
						catch (Exception)
						{
						}
						//ignore
						reporter.SetProgressFlag();
					}
					break;
				}
				catch (IOException ie)
				{
					Log.Warn("Failure asking whether task can commit: " + StringUtils.StringifyException
						(ie));
					if (--retries == 0)
					{
						//if it couldn't query successfully then delete the output
						DiscardOutput(taskContext);
						System.Environment.Exit(68);
					}
				}
			}
			// task can Commit now  
			try
			{
				Log.Info("Task " + taskId + " is allowed to commit now");
				committer.CommitTask(taskContext);
				return;
			}
			catch (IOException iee)
			{
				Log.Warn("Failure committing: " + StringUtils.StringifyException(iee));
				//if it couldn't commit a successfully then delete the output
				DiscardOutput(taskContext);
				throw;
			}
		}

		private void DiscardOutput(TaskAttemptContext taskContext)
		{
			try
			{
				committer.AbortTask(taskContext);
			}
			catch (IOException ioe)
			{
				Log.Warn("Failure cleaning up: " + StringUtils.StringifyException(ioe));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual void RunTaskCleanupTask(TaskUmbilicalProtocol umbilical
			, Task.TaskReporter reporter)
		{
			TaskCleanup(umbilical);
			Done(umbilical, reporter);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void TaskCleanup(TaskUmbilicalProtocol umbilical)
		{
			// set phase for this task
			SetPhase(TaskStatus.Phase.Cleanup);
			GetProgress().SetStatus("cleanup");
			StatusUpdate(umbilical);
			Log.Info("Runnning cleanup for the task");
			// do the cleanup
			committer.AbortTask(taskContext);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual void RunJobCleanupTask(TaskUmbilicalProtocol umbilical
			, Task.TaskReporter reporter)
		{
			// set phase for this task
			SetPhase(TaskStatus.Phase.Cleanup);
			GetProgress().SetStatus("cleanup");
			StatusUpdate(umbilical);
			// do the cleanup
			Log.Info("Cleaning up job");
			if (jobRunStateForCleanup == JobStatus.State.Failed || jobRunStateForCleanup == JobStatus.State
				.Killed)
			{
				Log.Info("Aborting job with runstate : " + jobRunStateForCleanup.ToString());
				if (conf.GetUseNewMapper())
				{
					committer.AbortJob(jobContext, jobRunStateForCleanup);
				}
				else
				{
					OutputCommitter oldCommitter = (OutputCommitter)committer;
					oldCommitter.AbortJob(jobContext, jobRunStateForCleanup);
				}
			}
			else
			{
				if (jobRunStateForCleanup == JobStatus.State.Succeeded)
				{
					Log.Info("Committing job");
					committer.CommitJob(jobContext);
				}
				else
				{
					throw new IOException("Invalid state of the job for cleanup. State found " + jobRunStateForCleanup
						 + " expecting " + JobStatus.State.Succeeded + ", " + JobStatus.State.Failed + " or "
						 + JobStatus.State.Killed);
				}
			}
			// delete the staging area for the job
			JobConf conf = new JobConf(jobContext.GetConfiguration());
			if (!KeepTaskFiles(conf))
			{
				string jobTempDir = conf.Get(MRJobConfig.MapreduceJobDir);
				Path jobTempDirPath = new Path(jobTempDir);
				FileSystem fs = jobTempDirPath.GetFileSystem(conf);
				fs.Delete(jobTempDirPath, true);
			}
			Done(umbilical, reporter);
		}

		protected internal virtual bool KeepTaskFiles(JobConf conf)
		{
			return (conf.GetKeepTaskFilesPattern() != null || conf.GetKeepFailedTaskFiles());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual void RunJobSetupTask(TaskUmbilicalProtocol umbilical, 
			Task.TaskReporter reporter)
		{
			// do the setup
			GetProgress().SetStatus("setup");
			committer.SetupJob(jobContext);
			Done(umbilical, reporter);
		}

		public virtual void SetConf(Configuration conf)
		{
			if (conf is JobConf)
			{
				this.conf = (JobConf)conf;
			}
			else
			{
				this.conf = new JobConf(conf);
			}
			this.mapOutputFile = ReflectionUtils.NewInstance(conf.GetClass<MapOutputFile>(MRConfig
				.TaskLocalOutputClass, typeof(MROutputFiles)), conf);
			this.lDirAlloc = new LocalDirAllocator(MRConfig.LocalDir);
			// add the static resolutions (this is required for the junit to
			// work on testcases that simulate multiple nodes on a single physical
			// node.
			string[] hostToResolved = conf.GetStrings(MRConfig.StaticResolutions);
			if (hostToResolved != null)
			{
				foreach (string str in hostToResolved)
				{
					string name = Sharpen.Runtime.Substring(str, 0, str.IndexOf('='));
					string resolvedName = Sharpen.Runtime.Substring(str, str.IndexOf('=') + 1);
					NetUtils.AddStaticResolution(name, resolvedName);
				}
			}
		}

		public virtual Configuration GetConf()
		{
			return this.conf;
		}

		public virtual MapOutputFile GetMapOutputFile()
		{
			return mapOutputFile;
		}

		/// <summary>OutputCollector for the combiner.</summary>
		public class CombineOutputCollector<K, V> : OutputCollector<K, V>
		{
			private IFile.Writer<K, V> writer;

			private Counters.Counter outCounter;

			private Progressable progressable;

			private long progressBar;

			public CombineOutputCollector(Counters.Counter outCounter, Progressable progressable
				, Configuration conf)
			{
				this.outCounter = outCounter;
				this.progressable = progressable;
				progressBar = conf.GetLong(MRJobConfig.CombineRecordsBeforeProgress, DefaultCombineRecordsBeforeProgress
					);
			}

			public virtual void SetWriter(IFile.Writer<K, V> writer)
			{
				lock (this)
				{
					this.writer = writer;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Collect(K key, V value)
			{
				lock (this)
				{
					outCounter.Increment(1);
					writer.Append(key, value);
					if ((outCounter.GetValue() % progressBar) == 0)
					{
						progressable.Progress();
					}
				}
			}
		}

		/// <summary>Iterates values while keys match in sorted input.</summary>
		internal class ValuesIterator<Key, Value> : IEnumerator<VALUE>
		{
			protected internal RawKeyValueIterator @in;

			private KEY key;

			private KEY nextKey;

			private VALUE value;

			private bool hasNext;

			private bool more;

			private RawComparator<KEY> comparator;

			protected internal Progressable reporter;

			private Deserializer<KEY> keyDeserializer;

			private Deserializer<VALUE> valDeserializer;

			private DataInputBuffer keyIn = new DataInputBuffer();

			private DataInputBuffer valueIn = new DataInputBuffer();

			/// <exception cref="System.IO.IOException"/>
			public ValuesIterator(RawKeyValueIterator @in, RawComparator<KEY> comparator, Type
				 keyClass, Type valClass, Configuration conf, Progressable reporter)
			{
				//input iterator
				// current key
				// current value
				// more w/ this key
				// more in file
				this.@in = @in;
				this.comparator = comparator;
				this.reporter = reporter;
				SerializationFactory serializationFactory = new SerializationFactory(conf);
				this.keyDeserializer = serializationFactory.GetDeserializer(keyClass);
				this.keyDeserializer.Open(keyIn);
				this.valDeserializer = serializationFactory.GetDeserializer(valClass);
				this.valDeserializer.Open(this.valueIn);
				ReadNextKey();
				key = nextKey;
				nextKey = null;
				// force new instance creation
				hasNext = more;
			}

			internal virtual RawKeyValueIterator GetRawIterator()
			{
				return @in;
			}

			/// Iterator methods
			public override bool HasNext()
			{
				return hasNext;
			}

			private int ctr = 0;

			public override VALUE Next()
			{
				if (!hasNext)
				{
					throw new NoSuchElementException("iterate past last value");
				}
				try
				{
					ReadNextValue();
					ReadNextKey();
				}
				catch (IOException ie)
				{
					throw new RuntimeException("problem advancing post rec#" + ctr, ie);
				}
				reporter.Progress();
				return value;
			}

			public override void Remove()
			{
				throw new RuntimeException("not implemented");
			}

			/// Auxiliary methods
			/// <summary>Start processing next unique key.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void NextKey()
			{
				// read until we find a new key
				while (hasNext)
				{
					ReadNextKey();
				}
				++ctr;
				// move the next key to the current one
				KEY tmpKey = key;
				key = nextKey;
				nextKey = tmpKey;
				hasNext = more;
			}

			/// <summary>True iff more keys remain.</summary>
			public virtual bool More()
			{
				return more;
			}

			/// <summary>The current key.</summary>
			public virtual KEY GetKey()
			{
				return key;
			}

			/// <summary>read the next key</summary>
			/// <exception cref="System.IO.IOException"/>
			private void ReadNextKey()
			{
				more = @in.Next();
				if (more)
				{
					DataInputBuffer nextKeyBytes = @in.GetKey();
					keyIn.Reset(nextKeyBytes.GetData(), nextKeyBytes.GetPosition(), nextKeyBytes.GetLength
						());
					nextKey = keyDeserializer.Deserialize(nextKey);
					hasNext = key != null && (comparator.Compare(key, nextKey) == 0);
				}
				else
				{
					hasNext = false;
				}
			}

			/// <summary>Read the next value</summary>
			/// <exception cref="System.IO.IOException"/>
			private void ReadNextValue()
			{
				DataInputBuffer nextValueBytes = @in.GetValue();
				valueIn.Reset(nextValueBytes.GetData(), nextValueBytes.GetPosition(), nextValueBytes
					.GetLength());
				value = valDeserializer.Deserialize(value);
			}
		}

		/// <summary>Iterator to return Combined values</summary>
		public class CombineValuesIterator<Key, Value> : Task.ValuesIterator<KEY, VALUE>
		{
			private readonly Counters.Counter combineInputCounter;

			/// <exception cref="System.IO.IOException"/>
			public CombineValuesIterator(RawKeyValueIterator @in, RawComparator<KEY> comparator
				, Type keyClass, Type valClass, Configuration conf, Reporter reporter, Counters.Counter
				 combineInputCounter)
				: base(@in, comparator, keyClass, valClass, conf, reporter)
			{
				this.combineInputCounter = combineInputCounter;
			}

			public override VALUE Next()
			{
				combineInputCounter.Increment(1);
				return base.Next();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal static Reducer.Context CreateReduceContext<Inkey, Invalue, Outkey
			, Outvalue>(Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer, Configuration job
			, TaskAttemptID taskId, RawKeyValueIterator rIter, Counter inputKeyCounter, Counter
			 inputValueCounter, RecordWriter<OUTKEY, OUTVALUE> output, OutputCommitter committer
			, StatusReporter reporter, RawComparator<INKEY> comparator)
		{
			System.Type keyClass = typeof(INKEY);
			System.Type valueClass = typeof(INVALUE);
			ReduceContext<INKEY, INVALUE, OUTKEY, OUTVALUE> reduceContext = new ReduceContextImpl
				<INKEY, INVALUE, OUTKEY, OUTVALUE>(job, taskId, rIter, inputKeyCounter, inputValueCounter
				, output, committer, reporter, comparator, keyClass, valueClass);
			Reducer.Context reducerContext = new WrappedReducer<INKEY, INVALUE, OUTKEY, OUTVALUE
				>().GetReducerContext(reduceContext);
			return reducerContext;
		}

		public abstract class CombinerRunner<K, V>
		{
			protected internal readonly Counters.Counter inputCounter;

			protected internal readonly JobConf job;

			protected internal readonly Task.TaskReporter reporter;

			internal CombinerRunner(Counters.Counter inputCounter, JobConf job, Task.TaskReporter
				 reporter)
			{
				this.inputCounter = inputCounter;
				this.job = job;
				this.reporter = reporter;
			}

			/// <summary>Run the combiner over a set of inputs.</summary>
			/// <param name="iterator">the key/value pairs to use as input</param>
			/// <param name="collector">the output collector</param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			/// <exception cref="System.TypeLoadException"/>
			public abstract void Combine(RawKeyValueIterator iterator, OutputCollector<K, V> 
				collector);

			/// <exception cref="System.TypeLoadException"/>
			public static Task.CombinerRunner<K, V> Create<K, V>(JobConf job, TaskAttemptID taskId
				, Counters.Counter inputCounter, Task.TaskReporter reporter, OutputCommitter committer
				)
			{
				Type cls = (Type)job.GetCombinerClass();
				if (cls != null)
				{
					return new Task.OldCombinerRunner(cls, job, inputCounter, reporter);
				}
				// make a task context so we can get the classes
				TaskAttemptContext taskContext = new TaskAttemptContextImpl(job, taskId, reporter
					);
				Type newcls = (Type)taskContext.GetCombinerClass();
				if (newcls != null)
				{
					return new Task.NewCombinerRunner<K, V>(newcls, job, taskId, taskContext, inputCounter
						, reporter, committer);
				}
				return null;
			}
		}

		protected internal class OldCombinerRunner<K, V> : Task.CombinerRunner<K, V>
		{
			private readonly Type combinerClass;

			private readonly Type keyClass;

			private readonly Type valueClass;

			private readonly RawComparator<K> comparator;

			protected internal OldCombinerRunner(Type cls, JobConf conf, Counters.Counter inputCounter
				, Task.TaskReporter reporter)
				: base(inputCounter, conf, reporter)
			{
				combinerClass = cls;
				keyClass = (Type)job.GetMapOutputKeyClass();
				valueClass = (Type)job.GetMapOutputValueClass();
				comparator = (RawComparator<K>)job.GetCombinerKeyGroupingComparator();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Combine(RawKeyValueIterator kvIter, OutputCollector<K, V> combineCollector
				)
			{
				Reducer<K, V, K, V> combiner = ReflectionUtils.NewInstance(combinerClass, job);
				try
				{
					Task.CombineValuesIterator<K, V> values = new Task.CombineValuesIterator<K, V>(kvIter
						, comparator, keyClass, valueClass, job, reporter, inputCounter);
					while (values.More())
					{
						combiner.Reduce(values.GetKey(), values, combineCollector, reporter);
						values.NextKey();
					}
				}
				finally
				{
					combiner.Close();
				}
			}
		}

		protected internal class NewCombinerRunner<K, V> : Task.CombinerRunner<K, V>
		{
			private readonly Type reducerClass;

			private readonly TaskAttemptID taskId;

			private readonly RawComparator<K> comparator;

			private readonly Type keyClass;

			private readonly Type valueClass;

			private readonly OutputCommitter committer;

			internal NewCombinerRunner(Type reducerClass, JobConf job, TaskAttemptID taskId, 
				TaskAttemptContext context, Counters.Counter inputCounter, Task.TaskReporter reporter
				, OutputCommitter committer)
				: base(inputCounter, job, reporter)
			{
				this.reducerClass = reducerClass;
				this.taskId = taskId;
				keyClass = (Type)context.GetMapOutputKeyClass();
				valueClass = (Type)context.GetMapOutputValueClass();
				comparator = (RawComparator<K>)context.GetCombinerKeyGroupingComparator();
				this.committer = committer;
			}

			private class OutputConverter<K, V> : RecordWriter<K, V>
			{
				internal OutputCollector<K, V> output;

				internal OutputConverter(OutputCollector<K, V> output)
				{
					this.output = output;
				}

				public override void Close(TaskAttemptContext context)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				public override void Write(K key, V value)
				{
					output.Collect(key, value);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			/// <exception cref="System.TypeLoadException"/>
			public override void Combine(RawKeyValueIterator iterator, OutputCollector<K, V> 
				collector)
			{
				// make a reducer
				Reducer<K, V, K, V> reducer = (Reducer<K, V, K, V>)ReflectionUtils.NewInstance(reducerClass
					, job);
				Reducer.Context reducerContext = CreateReduceContext(reducer, job, taskId, iterator
					, null, inputCounter, new Task.NewCombinerRunner.OutputConverter(collector), committer
					, reporter, comparator, keyClass, valueClass);
				reducer.Run(reducerContext);
			}
		}

		internal virtual BytesWritable GetExtraData()
		{
			return extraData;
		}

		internal virtual void SetExtraData(BytesWritable extraData)
		{
			this.extraData = extraData;
		}
	}
}
