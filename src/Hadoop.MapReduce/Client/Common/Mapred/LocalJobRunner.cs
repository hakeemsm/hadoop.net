using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Protocol;
using Org.Apache.Hadoop.Mapreduce.Security.Token.Delegation;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Implements MapReduce locally, in-process, for debugging.</summary>
	public class LocalJobRunner : ClientProtocol
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.LocalJobRunner
			));

		/// <summary>The maximum number of map tasks to run in parallel in LocalJobRunner</summary>
		public const string LocalMaxMaps = "mapreduce.local.map.tasks.maximum";

		/// <summary>The maximum number of reduce tasks to run in parallel in LocalJobRunner</summary>
		public const string LocalMaxReduces = "mapreduce.local.reduce.tasks.maximum";

		private FileSystem fs;

		private Dictionary<JobID, LocalJobRunner.Job> jobs = new Dictionary<JobID, LocalJobRunner.Job
			>();

		private JobConf conf;

		private AtomicInteger map_tasks = new AtomicInteger(0);

		private AtomicInteger reduce_tasks = new AtomicInteger(0);

		internal readonly Random rand = new Random();

		private LocalJobRunnerMetrics myMetrics = null;

		private const string jobDir = "localRunner/";

		public virtual long GetProtocolVersion(string protocol, long clientVersion)
		{
			return ClientProtocol.versionID;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
			, int clientMethodsHash)
		{
			return ProtocolSignature.GetProtocolSignature(this, protocol, clientVersion, clientMethodsHash
				);
		}

		private class Job : Sharpen.Thread, TaskUmbilicalProtocol
		{
			private Path systemJobDir;

			private Path systemJobFile;

			private Path localJobDir;

			private Path localJobFile;

			private JobID id;

			private JobConf job;

			private int numMapTasks;

			private int numReduceTasks;

			private float[] partialMapProgress;

			private float[] partialReduceProgress;

			private Counters[] mapCounters;

			private Counters[] reduceCounters;

			private JobStatus status;

			private IList<TaskAttemptID> mapIds = Sharpen.Collections.SynchronizedList(new AList
				<TaskAttemptID>());

			private JobProfile profile;

			private FileSystem localFs;

			internal bool killed = false;

			private LocalDistributedCacheManager localDistributedCacheManager;

			// The job directory on the system: JobClient places job configurations here.
			// This is analogous to JobTracker's system directory.
			// The job directory for the task.  Analagous to a task's job directory.
			public virtual long GetProtocolVersion(string protocol, long clientVersion)
			{
				return TaskUmbilicalProtocol.versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
				, int clientMethodsHash)
			{
				return ProtocolSignature.GetProtocolSignature(this, protocol, clientVersion, clientMethodsHash
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public Job(LocalJobRunner _enclosing, JobID jobid, string jobSubmitDir)
			{
				this._enclosing = _enclosing;
				this.systemJobDir = new Path(jobSubmitDir);
				this.systemJobFile = new Path(this.systemJobDir, "job.xml");
				this.id = jobid;
				JobConf conf = new JobConf(this.systemJobFile);
				this.localFs = FileSystem.GetLocal(conf);
				string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
				this.localJobDir = this.localFs.MakeQualified(new Path(new Path(conf.GetLocalPath
					(LocalJobRunner.jobDir), user), jobid.ToString()));
				this.localJobFile = new Path(this.localJobDir, this.id + ".xml");
				// Manage the distributed cache.  If there are files to be copied,
				// this will trigger localFile to be re-written again.
				this.localDistributedCacheManager = new LocalDistributedCacheManager();
				this.localDistributedCacheManager.Setup(conf);
				// Write out configuration file.  Instead of copying it from
				// systemJobFile, we re-write it, since setup(), above, may have
				// updated it.
				OutputStream @out = this.localFs.Create(this.localJobFile);
				try
				{
					conf.WriteXml(@out);
				}
				finally
				{
					@out.Close();
				}
				this.job = new JobConf(this.localJobFile);
				// Job (the current object) is a Thread, so we wrap its class loader.
				if (this.localDistributedCacheManager.HasLocalClasspaths())
				{
					this.SetContextClassLoader(this.localDistributedCacheManager.MakeClassLoader(this
						.GetContextClassLoader()));
				}
				this.profile = new JobProfile(this.job.GetUser(), this.id, this.systemJobFile.ToString
					(), "http://localhost:8080/", this.job.GetJobName());
				this.status = new JobStatus(this.id, 0.0f, 0.0f, JobStatus.Running, this.profile.
					GetUser(), this.profile.GetJobName(), this.profile.GetJobFile(), this.profile.GetURL
					().ToString());
				this._enclosing.jobs[this.id] = this;
				this.Start();
			}

			protected internal abstract class RunnableWithThrowable : Runnable
			{
				public volatile Exception storedException;

				public abstract void Run();

				internal RunnableWithThrowable(Job _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly Job _enclosing;
			}

			/// <summary>A Runnable instance that handles a map task to be run by an executor.</summary>
			protected internal class MapTaskRunnable : LocalJobRunner.Job.RunnableWithThrowable
			{
				private readonly int taskId;

				private readonly JobSplit.TaskSplitMetaInfo info;

				private readonly JobID jobId;

				private readonly JobConf localConf;

				private readonly IDictionary<TaskAttemptID, MapOutputFile> mapOutputFiles;

				public MapTaskRunnable(Job _enclosing, JobSplit.TaskSplitMetaInfo info, int taskId
					, JobID jobId, IDictionary<TaskAttemptID, MapOutputFile> mapOutputFiles)
					: base(_enclosing)
				{
					this._enclosing = _enclosing;
					// This is a reference to a shared object passed in by the
					// external context; this delivers state to the reducers regarding
					// where to fetch mapper outputs.
					this.info = info;
					this.taskId = taskId;
					this.mapOutputFiles = mapOutputFiles;
					this.jobId = jobId;
					this.localConf = new JobConf(this._enclosing.job);
				}

				public override void Run()
				{
					try
					{
						TaskAttemptID mapId = new TaskAttemptID(new TaskID(this.jobId, TaskType.Map, this
							.taskId), 0);
						LocalJobRunner.Log.Info("Starting task: " + mapId);
						this._enclosing.mapIds.AddItem(mapId);
						MapTask map = new MapTask(this._enclosing.systemJobFile.ToString(), mapId, this.taskId
							, this.info.GetSplitIndex(), 1);
						map.SetUser(UserGroupInformation.GetCurrentUser().GetShortUserName());
						LocalJobRunner.SetupChildMapredLocalDirs(map, this.localConf);
						MapOutputFile mapOutput = new MROutputFiles();
						mapOutput.SetConf(this.localConf);
						this.mapOutputFiles[mapId] = mapOutput;
						map.SetJobFile(this._enclosing.localJobFile.ToString());
						this.localConf.SetUser(map.GetUser());
						map.LocalizeConfiguration(this.localConf);
						map.SetConf(this.localConf);
						try
						{
							this._enclosing._enclosing.map_tasks.GetAndIncrement();
							this._enclosing._enclosing.myMetrics.LaunchMap(mapId);
							map.Run(this.localConf, this._enclosing);
							this._enclosing._enclosing.myMetrics.CompleteMap(mapId);
						}
						finally
						{
							this._enclosing._enclosing.map_tasks.GetAndDecrement();
						}
						LocalJobRunner.Log.Info("Finishing task: " + mapId);
					}
					catch (Exception e)
					{
						this.storedException = e;
					}
				}

				private readonly Job _enclosing;
			}

			/// <summary>
			/// Create Runnables to encapsulate map tasks for use by the executor
			/// service.
			/// </summary>
			/// <param name="taskInfo">Info about the map task splits</param>
			/// <param name="jobId">the job id</param>
			/// <param name="mapOutputFiles">a mapping from task attempts to output files</param>
			/// <returns>a List of Runnables, one per map task.</returns>
			protected internal virtual IList<LocalJobRunner.Job.RunnableWithThrowable> GetMapTaskRunnables
				(JobSplit.TaskSplitMetaInfo[] taskInfo, JobID jobId, IDictionary<TaskAttemptID, 
				MapOutputFile> mapOutputFiles)
			{
				int numTasks = 0;
				AList<LocalJobRunner.Job.RunnableWithThrowable> list = new AList<LocalJobRunner.Job.RunnableWithThrowable
					>();
				foreach (JobSplit.TaskSplitMetaInfo task in taskInfo)
				{
					list.AddItem(new LocalJobRunner.Job.MapTaskRunnable(this, task, numTasks++, jobId
						, mapOutputFiles));
				}
				return list;
			}

			protected internal class ReduceTaskRunnable : LocalJobRunner.Job.RunnableWithThrowable
			{
				private readonly int taskId;

				private readonly JobID jobId;

				private readonly JobConf localConf;

				private readonly IDictionary<TaskAttemptID, MapOutputFile> mapOutputFiles;

				public ReduceTaskRunnable(Job _enclosing, int taskId, JobID jobId, IDictionary<TaskAttemptID
					, MapOutputFile> mapOutputFiles)
					: base(_enclosing)
				{
					this._enclosing = _enclosing;
					// This is a reference to a shared object passed in by the
					// external context; this delivers state to the reducers regarding
					// where to fetch mapper outputs.
					this.taskId = taskId;
					this.jobId = jobId;
					this.mapOutputFiles = mapOutputFiles;
					this.localConf = new JobConf(this._enclosing.job);
					this.localConf.Set("mapreduce.jobtracker.address", "local");
				}

				public override void Run()
				{
					try
					{
						TaskAttemptID reduceId = new TaskAttemptID(new TaskID(this.jobId, TaskType.Reduce
							, this.taskId), 0);
						LocalJobRunner.Log.Info("Starting task: " + reduceId);
						ReduceTask reduce = new ReduceTask(this._enclosing.systemJobFile.ToString(), reduceId
							, this.taskId, this._enclosing.mapIds.Count, 1);
						reduce.SetUser(UserGroupInformation.GetCurrentUser().GetShortUserName());
						LocalJobRunner.SetupChildMapredLocalDirs(reduce, this.localConf);
						reduce.SetLocalMapFiles(this.mapOutputFiles);
						if (!this._enclosing.IsInterrupted())
						{
							reduce.SetJobFile(this._enclosing.localJobFile.ToString());
							this.localConf.SetUser(reduce.GetUser());
							reduce.LocalizeConfiguration(this.localConf);
							reduce.SetConf(this.localConf);
							try
							{
								this._enclosing._enclosing.reduce_tasks.GetAndIncrement();
								this._enclosing._enclosing.myMetrics.LaunchReduce(reduce.GetTaskID());
								reduce.Run(this.localConf, this._enclosing);
								this._enclosing._enclosing.myMetrics.CompleteReduce(reduce.GetTaskID());
							}
							finally
							{
								this._enclosing._enclosing.reduce_tasks.GetAndDecrement();
							}
							LocalJobRunner.Log.Info("Finishing task: " + reduceId);
						}
						else
						{
							throw new Exception();
						}
					}
					catch (Exception t)
					{
						// store this to be rethrown in the initial thread context.
						this.storedException = t;
					}
				}

				private readonly Job _enclosing;
			}

			/// <summary>
			/// Create Runnables to encapsulate reduce tasks for use by the executor
			/// service.
			/// </summary>
			/// <param name="jobId">the job id</param>
			/// <param name="mapOutputFiles">a mapping from task attempts to output files</param>
			/// <returns>a List of Runnables, one per reduce task.</returns>
			protected internal virtual IList<LocalJobRunner.Job.RunnableWithThrowable> GetReduceTaskRunnables
				(JobID jobId, IDictionary<TaskAttemptID, MapOutputFile> mapOutputFiles)
			{
				int taskId = 0;
				AList<LocalJobRunner.Job.RunnableWithThrowable> list = new AList<LocalJobRunner.Job.RunnableWithThrowable
					>();
				for (int i = 0; i < this.numReduceTasks; i++)
				{
					list.AddItem(new LocalJobRunner.Job.ReduceTaskRunnable(this, taskId++, jobId, mapOutputFiles
						));
				}
				return list;
			}

			/// <summary>
			/// Initialize the counters that will hold partial-progress from
			/// the various task attempts.
			/// </summary>
			/// <param name="numMaps">the number of map tasks in this job.</param>
			private void InitCounters(int numMaps, int numReduces)
			{
				lock (this)
				{
					// Initialize state trackers for all map tasks.
					this.partialMapProgress = new float[numMaps];
					this.mapCounters = new Counters[numMaps];
					for (int i = 0; i < numMaps; i++)
					{
						this.mapCounters[i] = new Counters();
					}
					this.partialReduceProgress = new float[numReduces];
					this.reduceCounters = new Counters[numReduces];
					for (int i_1 = 0; i_1 < numReduces; i_1++)
					{
						this.reduceCounters[i_1] = new Counters();
					}
					this.numMapTasks = numMaps;
					this.numReduceTasks = numReduces;
				}
			}

			/// <summary>Creates the executor service used to run map tasks.</summary>
			/// <returns>an ExecutorService instance that handles map tasks</returns>
			protected internal virtual ExecutorService CreateMapExecutor()
			{
				lock (this)
				{
					// Determine the size of the thread pool to use
					int maxMapThreads = this.job.GetInt(LocalJobRunner.LocalMaxMaps, 1);
					if (maxMapThreads < 1)
					{
						throw new ArgumentException("Configured " + LocalJobRunner.LocalMaxMaps + " must be >= 1"
							);
					}
					maxMapThreads = Math.Min(maxMapThreads, this.numMapTasks);
					maxMapThreads = Math.Max(maxMapThreads, 1);
					// In case of no tasks.
					LocalJobRunner.Log.Debug("Starting mapper thread pool executor.");
					LocalJobRunner.Log.Debug("Max local threads: " + maxMapThreads);
					LocalJobRunner.Log.Debug("Map tasks to process: " + this.numMapTasks);
					// Create a new executor service to drain the work queue.
					ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("LocalJobRunner Map Task Executor #%d"
						).Build();
					ExecutorService executor = Executors.NewFixedThreadPool(maxMapThreads, tf);
					return executor;
				}
			}

			/// <summary>Creates the executor service used to run reduce tasks.</summary>
			/// <returns>an ExecutorService instance that handles reduce tasks</returns>
			protected internal virtual ExecutorService CreateReduceExecutor()
			{
				lock (this)
				{
					// Determine the size of the thread pool to use
					int maxReduceThreads = this.job.GetInt(LocalJobRunner.LocalMaxReduces, 1);
					if (maxReduceThreads < 1)
					{
						throw new ArgumentException("Configured " + LocalJobRunner.LocalMaxReduces + " must be >= 1"
							);
					}
					maxReduceThreads = Math.Min(maxReduceThreads, this.numReduceTasks);
					maxReduceThreads = Math.Max(maxReduceThreads, 1);
					// In case of no tasks.
					LocalJobRunner.Log.Debug("Starting reduce thread pool executor.");
					LocalJobRunner.Log.Debug("Max local threads: " + maxReduceThreads);
					LocalJobRunner.Log.Debug("Reduce tasks to process: " + this.numReduceTasks);
					// Create a new executor service to drain the work queue.
					ExecutorService executor = Executors.NewFixedThreadPool(maxReduceThreads);
					return executor;
				}
			}

			/// <summary>Run a set of tasks and waits for them to complete.</summary>
			/// <exception cref="System.Exception"/>
			private void RunTasks(IList<LocalJobRunner.Job.RunnableWithThrowable> runnables, 
				ExecutorService service, string taskType)
			{
				// Start populating the executor with work units.
				// They may begin running immediately (in other threads).
				foreach (Runnable r in runnables)
				{
					service.Submit(r);
				}
				try
				{
					service.Shutdown();
					// Instructs queue to drain.
					// Wait for tasks to finish; do not use a time-based timeout.
					// (See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6179024)
					LocalJobRunner.Log.Info("Waiting for " + taskType + " tasks");
					service.AwaitTermination(long.MaxValue, TimeUnit.Nanoseconds);
				}
				catch (Exception ie)
				{
					// Cancel all threads.
					service.ShutdownNow();
					throw;
				}
				LocalJobRunner.Log.Info(taskType + " task executor complete.");
				// After waiting for the tasks to complete, if any of these
				// have thrown an exception, rethrow it now in the main thread context.
				foreach (LocalJobRunner.Job.RunnableWithThrowable r_1 in runnables)
				{
					if (r_1.storedException != null)
					{
						throw new Exception(r_1.storedException);
					}
				}
			}

			/// <exception cref="System.Exception"/>
			private OutputCommitter CreateOutputCommitter(bool newApiCommitter, JobID jobId, 
				Configuration conf)
			{
				OutputCommitter committer = null;
				LocalJobRunner.Log.Info("OutputCommitter set in config " + conf.Get("mapred.output.committer.class"
					));
				if (newApiCommitter)
				{
					TaskID taskId = new TaskID(jobId, TaskType.Map, 0);
					TaskAttemptID taskAttemptID = new TaskAttemptID(taskId, 0);
					TaskAttemptContext taskContext = new TaskAttemptContextImpl(conf, taskAttemptID);
					OutputFormat outputFormat = ReflectionUtils.NewInstance(taskContext.GetOutputFormatClass
						(), conf);
					committer = outputFormat.GetOutputCommitter(taskContext);
				}
				else
				{
					committer = ReflectionUtils.NewInstance(conf.GetClass<OutputCommitter>("mapred.output.committer.class"
						, typeof(FileOutputCommitter)), conf);
				}
				LocalJobRunner.Log.Info("OutputCommitter is " + committer.GetType().FullName);
				return committer;
			}

			public override void Run()
			{
				JobID jobId = this.profile.GetJobID();
				JobContext jContext = new JobContextImpl(this.job, jobId);
				OutputCommitter outputCommitter = null;
				try
				{
					outputCommitter = this.CreateOutputCommitter(this._enclosing.conf.GetUseNewMapper
						(), jobId, this._enclosing.conf);
				}
				catch (Exception e)
				{
					LocalJobRunner.Log.Info("Failed to createOutputCommitter", e);
					return;
				}
				try
				{
					JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfos = SplitMetaInfoReader.ReadSplitMetaInfo
						(jobId, this.localFs, this._enclosing.conf, this.systemJobDir);
					int numReduceTasks = this.job.GetNumReduceTasks();
					outputCommitter.SetupJob(jContext);
					this.status.SetSetupProgress(1.0f);
					IDictionary<TaskAttemptID, MapOutputFile> mapOutputFiles = Sharpen.Collections.SynchronizedMap
						(new Dictionary<TaskAttemptID, MapOutputFile>());
					IList<LocalJobRunner.Job.RunnableWithThrowable> mapRunnables = this.GetMapTaskRunnables
						(taskSplitMetaInfos, jobId, mapOutputFiles);
					this.InitCounters(mapRunnables.Count, numReduceTasks);
					ExecutorService mapService = this.CreateMapExecutor();
					this.RunTasks(mapRunnables, mapService, "map");
					try
					{
						if (numReduceTasks > 0)
						{
							IList<LocalJobRunner.Job.RunnableWithThrowable> reduceRunnables = this.GetReduceTaskRunnables
								(jobId, mapOutputFiles);
							ExecutorService reduceService = this.CreateReduceExecutor();
							this.RunTasks(reduceRunnables, reduceService, "reduce");
						}
					}
					finally
					{
						foreach (MapOutputFile output in mapOutputFiles.Values)
						{
							output.RemoveAll();
						}
					}
					// delete the temporary directory in output directory
					outputCommitter.CommitJob(jContext);
					this.status.SetCleanupProgress(1.0f);
					if (this.killed)
					{
						this.status.SetRunState(JobStatus.Killed);
					}
					else
					{
						this.status.SetRunState(JobStatus.Succeeded);
					}
					JobEndNotifier.LocalRunnerNotification(this.job, this.status);
				}
				catch (Exception t)
				{
					try
					{
						outputCommitter.AbortJob(jContext, JobStatus.State.Failed);
					}
					catch (IOException)
					{
						LocalJobRunner.Log.Info("Error cleaning up job:" + this.id);
					}
					this.status.SetCleanupProgress(1.0f);
					if (this.killed)
					{
						this.status.SetRunState(JobStatus.Killed);
					}
					else
					{
						this.status.SetRunState(JobStatus.Failed);
					}
					LocalJobRunner.Log.Warn(this.id, t);
					JobEndNotifier.LocalRunnerNotification(this.job, this.status);
				}
				finally
				{
					try
					{
						this._enclosing.fs.Delete(this.systemJobFile.GetParent(), true);
						// delete submit dir
						this.localFs.Delete(this.localJobFile, true);
						// delete local copy
						// Cleanup distributed cache
						this.localDistributedCacheManager.Close();
					}
					catch (IOException e)
					{
						LocalJobRunner.Log.Warn("Error cleaning up " + this.id + ": " + e);
					}
				}
			}

			// TaskUmbilicalProtocol methods
			public override JvmTask GetTask(JvmContext context)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool StatusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
			{
				lock (this)
				{
					// Serialize as we would if distributed in order to make deep copy
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					DataOutputStream dos = new DataOutputStream(baos);
					taskStatus.Write(dos);
					dos.Close();
					taskStatus = TaskStatus.CreateTaskStatus(taskStatus.GetIsMap());
					taskStatus.ReadFields(new DataInputStream(new ByteArrayInputStream(baos.ToByteArray
						())));
					LocalJobRunner.Log.Info(taskStatus.GetStateString());
					int mapTaskIndex = this.mapIds.IndexOf(taskId);
					if (mapTaskIndex >= 0)
					{
						// mapping
						float numTasks = (float)this.numMapTasks;
						this.partialMapProgress[mapTaskIndex] = taskStatus.GetProgress();
						this.mapCounters[mapTaskIndex] = taskStatus.GetCounters();
						float partialProgress = 0.0f;
						foreach (float f in this.partialMapProgress)
						{
							partialProgress += f;
						}
						this.status.SetMapProgress(partialProgress / numTasks);
					}
					else
					{
						// reducing
						int reduceTaskIndex = ((TaskID)taskId.GetTaskID()).GetId();
						float numTasks = (float)this.numReduceTasks;
						this.partialReduceProgress[reduceTaskIndex] = taskStatus.GetProgress();
						this.reduceCounters[reduceTaskIndex] = taskStatus.GetCounters();
						float partialProgress = 0.0f;
						foreach (float f in this.partialReduceProgress)
						{
							partialProgress += f;
						}
						this.status.SetReduceProgress(partialProgress / numTasks);
					}
					// ignore phase
					return true;
				}
			}

			/// <summary>
			/// Return the current values of the counters for this job,
			/// including tasks that are in progress.
			/// </summary>
			public virtual Counters GetCurrentCounters()
			{
				lock (this)
				{
					if (null == this.mapCounters)
					{
						// Counters not yet initialized for job.
						return new Counters();
					}
					Counters current = new Counters();
					foreach (Counters c in this.mapCounters)
					{
						current = Counters.Sum(current, c);
					}
					if (null != this.reduceCounters && this.reduceCounters.Length > 0)
					{
						foreach (Counters c_1 in this.reduceCounters)
						{
							current = Counters.Sum(current, c_1);
						}
					}
					return current;
				}
			}

			/// <summary>
			/// Task is reporting that it is in commit_pending
			/// and it is waiting for the commit Response
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void CommitPending(TaskAttemptID taskid, TaskStatus taskStatus)
			{
				this.StatusUpdate(taskid, taskStatus);
			}

			public override void ReportDiagnosticInfo(TaskAttemptID taskid, string trace)
			{
			}

			// Ignore for now
			/// <exception cref="System.IO.IOException"/>
			public override void ReportNextRecordRange(TaskAttemptID taskid, SortedRanges.Range
				 range)
			{
				LocalJobRunner.Log.Info("Task " + taskid + " reportedNextRecordRange " + range);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Ping(TaskAttemptID taskid)
			{
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool CanCommit(TaskAttemptID taskid)
			{
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Done(TaskAttemptID taskId)
			{
				int taskIndex = this.mapIds.IndexOf(taskId);
				if (taskIndex >= 0)
				{
					// mapping
					this.status.SetMapProgress(1.0f);
				}
				else
				{
					this.status.SetReduceProgress(1.0f);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void FsError(TaskAttemptID taskId, string message)
			{
				lock (this)
				{
					LocalJobRunner.Log.Fatal("FSError: " + message + "from task: " + taskId);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ShuffleError(TaskAttemptID taskId, string message)
			{
				LocalJobRunner.Log.Fatal("shuffleError: " + message + "from task: " + taskId);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void FatalError(TaskAttemptID taskId, string msg)
			{
				lock (this)
				{
					LocalJobRunner.Log.Fatal("Fatal: " + msg + "from task: " + taskId);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override MapTaskCompletionEventsUpdate GetMapCompletionEvents(JobID jobId, 
				int fromEventId, int maxLocs, TaskAttemptID id)
			{
				return new MapTaskCompletionEventsUpdate(TaskCompletionEvent.EmptyArray, false);
			}

			private readonly LocalJobRunner _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public LocalJobRunner(Configuration conf)
			: this(new JobConf(conf))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public LocalJobRunner(JobConf conf)
		{
			this.fs = FileSystem.GetLocal(conf);
			this.conf = conf;
			myMetrics = new LocalJobRunnerMetrics(new JobConf(conf));
		}

		private static int jobid = 0;

		private int randid;

		// JobSubmissionProtocol methods
		// used for making sure that local jobs run in different jvms don't
		// collide on staging or job directories
		public override JobID GetNewJobID()
		{
			lock (this)
			{
				return new JobID("local" + randid, ++jobid);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override JobStatus SubmitJob(JobID jobid, string jobSubmitDir, Credentials
			 credentials)
		{
			LocalJobRunner.Job job = new LocalJobRunner.Job(this, JobID.Downgrade(jobid), jobSubmitDir
				);
			job.job.SetCredentials(credentials);
			return job.status;
		}

		public override void KillJob(JobID id)
		{
			jobs[JobID.Downgrade(id)].killed = true;
			jobs[JobID.Downgrade(id)].Interrupt();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetJobPriority(JobID id, string jp)
		{
			throw new NotSupportedException("Changing job priority " + "in LocalJobRunner is not supported."
				);
		}

		/// <summary>
		/// Throws
		/// <see cref="System.NotSupportedException"/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool KillTask(TaskAttemptID taskId, bool shouldFail)
		{
			throw new NotSupportedException("Killing tasks in " + "LocalJobRunner is not supported"
				);
		}

		public override TaskReport[] GetTaskReports(JobID id, TaskType type)
		{
			return new TaskReport[0];
		}

		public override JobStatus GetJobStatus(JobID id)
		{
			LocalJobRunner.Job job = jobs[JobID.Downgrade(id)];
			if (job != null)
			{
				return job.status;
			}
			else
			{
				return null;
			}
		}

		public override Counters GetJobCounters(JobID id)
		{
			LocalJobRunner.Job job = jobs[JobID.Downgrade(id)];
			return new Counters(job.GetCurrentCounters());
		}

		/// <exception cref="System.IO.IOException"/>
		public override string GetFilesystemName()
		{
			return fs.GetUri().ToString();
		}

		public override ClusterMetrics GetClusterMetrics()
		{
			int numMapTasks = map_tasks.Get();
			int numReduceTasks = reduce_tasks.Get();
			return new ClusterMetrics(numMapTasks, numReduceTasks, numMapTasks, numReduceTasks
				, 0, 0, 1, 1, jobs.Count, 1, 0, 0);
		}

		public override Cluster.JobTrackerStatus GetJobTrackerStatus()
		{
			return Cluster.JobTrackerStatus.Running;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override long GetTaskTrackerExpiryInterval()
		{
			return 0;
		}

		/// <summary>Get all active trackers in cluster.</summary>
		/// <returns>array of TaskTrackerInfo</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override TaskTrackerInfo[] GetActiveTrackers()
		{
			return new TaskTrackerInfo[0];
		}

		/// <summary>Get all blacklisted trackers in cluster.</summary>
		/// <returns>array of TaskTrackerInfo</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override TaskTrackerInfo[] GetBlacklistedTrackers()
		{
			return new TaskTrackerInfo[0];
		}

		/// <exception cref="System.IO.IOException"/>
		public override TaskCompletionEvent[] GetTaskCompletionEvents(JobID jobid, int fromEventId
			, int maxEvents)
		{
			return TaskCompletionEvent.EmptyArray;
		}

		public override JobStatus[] GetAllJobs()
		{
			return null;
		}

		/// <summary>Returns the diagnostic information for a particular task in the given job.
		/// 	</summary>
		/// <remarks>
		/// Returns the diagnostic information for a particular task in the given job.
		/// To be implemented
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override string[] GetTaskDiagnostics(TaskAttemptID taskid)
		{
			return new string[0];
		}

		/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Protocol.ClientProtocol.GetSystemDir()
		/// 	"/>
		public override string GetSystemDir()
		{
			Path sysDir = new Path(conf.Get(JTConfig.JtSystemDir, "/tmp/hadoop/mapred/system"
				));
			return fs.MakeQualified(sysDir).ToString();
		}

		/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Protocol.ClientProtocol.GetQueueAdmins(string)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public override AccessControlList GetQueueAdmins(string queueName)
		{
			return new AccessControlList(" ");
		}

		// no queue admins for local job runner
		/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Protocol.ClientProtocol.GetStagingAreaDir()
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public override string GetStagingAreaDir()
		{
			Path stagingRootDir = new Path(conf.Get(JTConfig.JtStagingAreaRoot, "/tmp/hadoop/mapred/staging"
				));
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			string user;
			randid = rand.Next(int.MaxValue);
			if (ugi != null)
			{
				user = ugi.GetShortUserName() + randid;
			}
			else
			{
				user = "dummy" + randid;
			}
			return fs.MakeQualified(new Path(stagingRootDir, user + "/.staging")).ToString();
		}

		public override string GetJobHistoryDir()
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override QueueInfo[] GetChildQueues(string queueName)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override QueueInfo[] GetRootQueues()
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override QueueInfo[] GetQueues()
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override QueueInfo GetQueue(string queue)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override QueueAclsInfo[] GetQueueAclsForCurrentUser()
		{
			return null;
		}

		/// <summary>Set the max number of map tasks to run concurrently in the LocalJobRunner.
		/// 	</summary>
		/// <param name="job">the job to configure</param>
		/// <param name="maxMaps">the maximum number of map tasks to allow.</param>
		public static void SetLocalMaxRunningMaps(JobContext job, int maxMaps)
		{
			job.GetConfiguration().SetInt(LocalMaxMaps, maxMaps);
		}

		/// <returns>
		/// the max number of map tasks to run concurrently in the
		/// LocalJobRunner.
		/// </returns>
		public static int GetLocalMaxRunningMaps(JobContext job)
		{
			return job.GetConfiguration().GetInt(LocalMaxMaps, 1);
		}

		/// <summary>Set the max number of reduce tasks to run concurrently in the LocalJobRunner.
		/// 	</summary>
		/// <param name="job">the job to configure</param>
		/// <param name="maxReduces">the maximum number of reduce tasks to allow.</param>
		public static void SetLocalMaxRunningReduces(JobContext job, int maxReduces)
		{
			job.GetConfiguration().SetInt(LocalMaxReduces, maxReduces);
		}

		/// <returns>
		/// the max number of reduce tasks to run concurrently in the
		/// LocalJobRunner.
		/// </returns>
		public static int GetLocalMaxRunningReduces(JobContext job)
		{
			return job.GetConfiguration().GetInt(LocalMaxReduces, 1);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier> token)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>
			 GetDelegationToken(Text renewer)
		{
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			DelegationTokenIdentifier> token)
		{
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override LogParams GetLogFileParams(JobID jobID, TaskAttemptID taskAttemptID
			)
		{
			throw new NotSupportedException("Not supported");
		}

		internal static void SetupChildMapredLocalDirs(Org.Apache.Hadoop.Mapred.Task t, JobConf
			 conf)
		{
			string[] localDirs = conf.GetTrimmedStrings(MRConfig.LocalDir);
			string jobId = t.GetJobID().ToString();
			string taskId = t.GetTaskID().ToString();
			bool isCleanup = t.IsTaskCleanupTask();
			string user = t.GetUser();
			StringBuilder childMapredLocalDir = new StringBuilder(localDirs[0] + Path.Separator
				 + GetLocalTaskDir(user, jobId, taskId, isCleanup));
			for (int i = 1; i < localDirs.Length; i++)
			{
				childMapredLocalDir.Append("," + localDirs[i] + Path.Separator + GetLocalTaskDir(
					user, jobId, taskId, isCleanup));
			}
			Log.Debug(MRConfig.LocalDir + " for child : " + childMapredLocalDir);
			conf.Set(MRConfig.LocalDir, childMapredLocalDir.ToString());
		}

		internal const string TaskCleanupSuffix = ".cleanup";

		internal const string Jobcache = "jobcache";

		internal static string GetLocalTaskDir(string user, string jobid, string taskid, 
			bool isCleanupAttempt)
		{
			string taskDir = jobDir + Path.Separator + user + Path.Separator + Jobcache + Path
				.Separator + jobid + Path.Separator + taskid;
			if (isCleanupAttempt)
			{
				taskDir = taskDir + TaskCleanupSuffix;
			}
			return taskDir;
		}
	}
}
