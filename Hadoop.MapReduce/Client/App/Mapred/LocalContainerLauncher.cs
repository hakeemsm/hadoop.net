using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Launcher;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Runs the container task locally in a thread.</summary>
	/// <remarks>
	/// Runs the container task locally in a thread.
	/// Since all (sub)tasks share the same local directory, they must be executed
	/// sequentially in order to avoid creating/deleting the same files/dirs.
	/// </remarks>
	public class LocalContainerLauncher : AbstractService, ContainerLauncher
	{
		private static readonly FilePath curDir = new FilePath(".");

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.LocalContainerLauncher
			));

		private FileContext curFC = null;

		private readonly HashSet<FilePath> localizedFiles;

		private readonly AppContext context;

		private readonly TaskUmbilicalProtocol umbilical;

		private ExecutorService taskRunner;

		private Sharpen.Thread eventHandler;

		private byte[] encryptedSpillKey = new byte[] { 0 };

		private BlockingQueue<ContainerLauncherEvent> eventQueue = new LinkedBlockingQueue
			<ContainerLauncherEvent>();

		public LocalContainerLauncher(AppContext context, TaskUmbilicalProtocol umbilical
			)
			: base(typeof(Org.Apache.Hadoop.Mapred.LocalContainerLauncher).FullName)
		{
			this.context = context;
			this.umbilical = umbilical;
			// umbilical:  MRAppMaster creates (taskAttemptListener), passes to us
			// (TODO/FIXME:  pointless to use RPC to talk to self; should create
			// LocalTaskAttemptListener or similar:  implement umbilical protocol
			// but skip RPC stuff)
			try
			{
				curFC = FileContext.GetFileContext(curDir.ToURI());
			}
			catch (UnsupportedFileSystemException)
			{
				Log.Error("Local filesystem " + curDir.ToURI().ToString() + " is unsupported?? (should never happen)"
					);
			}
			// Save list of files/dirs that are supposed to be present so can delete
			// any extras created by one task before starting subsequent task.  Note
			// that there's no protection against deleted or renamed localization;
			// users who do that get what they deserve (and will have to disable
			// uberization in order to run correctly).
			FilePath[] curLocalFiles = curDir.ListFiles();
			localizedFiles = new HashSet<FilePath>(curLocalFiles.Length);
			for (int j = 0; j < curLocalFiles.Length; ++j)
			{
				localizedFiles.AddItem(curLocalFiles[j]);
			}
		}

		// Relocalization note/future FIXME (per chrisdo, 20110315):  At moment,
		// full localization info is in AppSubmissionContext passed from client to
		// RM and then to NM for AM-container launch:  no difference between AM-
		// localization and MapTask- or ReduceTask-localization, so can assume all
		// OK.  Longer-term, will need to override uber-AM container-localization
		// request ("needed resources") with union of regular-AM-resources + task-
		// resources (and, if maps and reduces ever differ, then union of all three
		// types), OR will need localizer service/API that uber-AM can request
		// after running (e.g., "localizeForTask()" or "localizeForMapTask()").
		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			// create a single thread for serial execution of tasks
			// make it a daemon thread so that the process can exit even if the task is
			// not interruptible
			taskRunner = Executors.NewSingleThreadExecutor(new ThreadFactoryBuilder().SetDaemon
				(true).SetNameFormat("uber-SubtaskRunner").Build());
			// create and start an event handling thread
			eventHandler = new Sharpen.Thread(new LocalContainerLauncher.EventHandler(this), 
				"uber-EventHandler");
			eventHandler.Start();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (eventHandler != null)
			{
				eventHandler.Interrupt();
			}
			if (taskRunner != null)
			{
				taskRunner.ShutdownNow();
			}
			base.ServiceStop();
		}

		public virtual void Handle(ContainerLauncherEvent @event)
		{
			try
			{
				eventQueue.Put(@event);
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		// FIXME? YarnRuntimeException is "for runtime exceptions only"
		public virtual void SetEncryptedSpillKey(byte[] encryptedSpillKey)
		{
			if (encryptedSpillKey != null)
			{
				this.encryptedSpillKey = encryptedSpillKey;
			}
		}

		private class EventHandler : Runnable
		{
			private bool doneWithMaps = false;

			private int finishedSubMaps = 0;

			private readonly IDictionary<TaskAttemptId, Future<object>> futures = new ConcurrentHashMap
				<TaskAttemptId, Future<object>>();

			internal EventHandler(LocalContainerLauncher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/*
			* Uber-AM lifecycle/ordering ("normal" case):
			*
			* - [somebody] sends TA_ASSIGNED
			*   - handled by ContainerAssignedTransition (TaskAttemptImpl.java)
			*     - creates "remoteTask" for us == real Task
			*     - sends CONTAINER_REMOTE_LAUNCH
			*     - TA: UNASSIGNED -> ASSIGNED
			* - CONTAINER_REMOTE_LAUNCH handled by LocalContainerLauncher (us)
			*   - sucks "remoteTask" out of TaskAttemptImpl via getRemoteTask()
			*   - sends TA_CONTAINER_LAUNCHED
			*     [[ elsewhere...
			*       - TA_CONTAINER_LAUNCHED handled by LaunchedContainerTransition
			*         - registers "remoteTask" with TaskAttemptListener (== umbilical)
			*         - NUKES "remoteTask"
			*         - sends T_ATTEMPT_LAUNCHED (Task: SCHEDULED -> RUNNING)
			*         - TA: ASSIGNED -> RUNNING
			*     ]]
			*   - runs Task (runSubMap() or runSubReduce())
			*     - TA can safely send TA_UPDATE since in RUNNING state
			*/
			// doneWithMaps and finishedSubMaps are accessed from only
			// one thread. Therefore, no need to make them volatile.
			public virtual void Run()
			{
				ContainerLauncherEvent @event = null;
				// Collect locations of map outputs to give to reduces
				IDictionary<TaskAttemptID, MapOutputFile> localMapFiles = new Dictionary<TaskAttemptID
					, MapOutputFile>();
				// _must_ either run subtasks sequentially or accept expense of new JVMs
				// (i.e., fork()), else will get weird failures when maps try to create/
				// write same dirname or filename:  no chdir() in Java
				while (!Sharpen.Thread.CurrentThread().IsInterrupted())
				{
					try
					{
						@event = this._enclosing.eventQueue.Take();
					}
					catch (Exception e)
					{
						// mostly via T_KILL? JOB_KILL?
						LocalContainerLauncher.Log.Error("Returning, interrupted : " + e);
						break;
					}
					LocalContainerLauncher.Log.Info("Processing the event " + @event.ToString());
					if (@event.GetType() == ContainerLauncher.EventType.ContainerRemoteLaunch)
					{
						ContainerRemoteLaunchEvent launchEv = (ContainerRemoteLaunchEvent)@event;
						// execute the task on a separate thread
						Future<object> future = this._enclosing.taskRunner.Submit(new _Runnable_228(this, 
							launchEv, localMapFiles));
						// remember the current attempt
						this.futures[@event.GetTaskAttemptID()] = future;
					}
					else
					{
						if (@event.GetType() == ContainerLauncher.EventType.ContainerRemoteCleanup)
						{
							// cancel (and interrupt) the current running task associated with the
							// event
							TaskAttemptId taId = @event.GetTaskAttemptID();
							Future<object> future = Sharpen.Collections.Remove(this.futures, taId);
							if (future != null)
							{
								LocalContainerLauncher.Log.Info("canceling the task attempt " + taId);
								future.Cancel(true);
							}
							// send "cleaned" event to task attempt to move us from
							// SUCCESS_CONTAINER_CLEANUP to SUCCEEDED state (or 
							// {FAIL|KILL}_CONTAINER_CLEANUP to {FAIL|KILL}_TASK_CLEANUP)
							this._enclosing.context.GetEventHandler().Handle(new TaskAttemptEvent(taId, TaskAttemptEventType
								.TaContainerCleaned));
						}
						else
						{
							LocalContainerLauncher.Log.Warn("Ignoring unexpected event " + @event.ToString());
						}
					}
				}
			}

			private sealed class _Runnable_228 : Runnable
			{
				public _Runnable_228(EventHandler _enclosing, ContainerRemoteLaunchEvent launchEv
					, IDictionary<TaskAttemptID, MapOutputFile> localMapFiles)
				{
					this._enclosing = _enclosing;
					this.launchEv = launchEv;
					this.localMapFiles = localMapFiles;
				}

				public void Run()
				{
					this._enclosing.RunTask(launchEv, localMapFiles);
				}

				private readonly EventHandler _enclosing;

				private readonly ContainerRemoteLaunchEvent launchEv;

				private readonly IDictionary<TaskAttemptID, MapOutputFile> localMapFiles;
			}

			private void RunTask(ContainerRemoteLaunchEvent launchEv, IDictionary<TaskAttemptID
				, MapOutputFile> localMapFiles)
			{
				TaskAttemptId attemptID = launchEv.GetTaskAttemptID();
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this._enclosing.context.GetAllJobs
					()[attemptID.GetTaskId().GetJobId()];
				int numMapTasks = job.GetTotalMaps();
				int numReduceTasks = job.GetTotalReduces();
				// YARN (tracking) Task:
				Task ytask = job.GetTask(attemptID.GetTaskId());
				// classic mapred Task:
				Task remoteTask = launchEv.GetRemoteTask();
				// after "launching," send launched event to task attempt to move
				// state from ASSIGNED to RUNNING (also nukes "remoteTask", so must
				// do getRemoteTask() call first)
				//There is no port number because we are not really talking to a task
				// tracker.  The shuffle is just done through local files.  So the
				// port number is set to -1 in this case.
				this._enclosing.context.GetEventHandler().Handle(new TaskAttemptContainerLaunchedEvent
					(attemptID, -1));
				if (numMapTasks == 0)
				{
					this.doneWithMaps = true;
				}
				try
				{
					if (remoteTask.IsMapOrReduce())
					{
						JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID.GetTaskId().GetJobId
							());
						jce.AddCounterUpdate(JobCounter.TotalLaunchedUbertasks, 1);
						if (remoteTask.IsMapTask())
						{
							jce.AddCounterUpdate(JobCounter.NumUberSubmaps, 1);
						}
						else
						{
							jce.AddCounterUpdate(JobCounter.NumUberSubreduces, 1);
						}
						this._enclosing.context.GetEventHandler().Handle(jce);
					}
					this.RunSubtask(remoteTask, ytask.GetType(), attemptID, numMapTasks, (numReduceTasks
						 > 0), localMapFiles);
				}
				catch (RuntimeException)
				{
					JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptID.GetTaskId().GetJobId
						());
					jce.AddCounterUpdate(JobCounter.NumFailedUbertasks, 1);
					this._enclosing.context.GetEventHandler().Handle(jce);
					// this is our signal that the subtask failed in some way, so
					// simulate a failed JVM/container and send a container-completed
					// event to task attempt (i.e., move state machine from RUNNING
					// to FAIL_CONTAINER_CLEANUP [and ultimately to FAILED])
					this._enclosing.context.GetEventHandler().Handle(new TaskAttemptEvent(attemptID, 
						TaskAttemptEventType.TaContainerCompleted));
				}
				catch (IOException ioe)
				{
					// if umbilical itself barfs (in error-handler of runSubMap()),
					// we're pretty much hosed, so do what YarnChild main() does
					// (i.e., exit clumsily--but can never happen, so no worries!)
					LocalContainerLauncher.Log.Fatal("oopsie...  this can never happen: " + StringUtils
						.StringifyException(ioe));
					ExitUtil.Terminate(-1);
				}
				finally
				{
					// remove my future
					if (Sharpen.Collections.Remove(this.futures, attemptID) != null)
					{
						LocalContainerLauncher.Log.Info("removed attempt " + attemptID + " from the futures to keep track of"
							);
					}
				}
			}

			/// <exception cref="Sharpen.RuntimeException"/>
			/// <exception cref="System.IO.IOException"/>
			private void RunSubtask(Task task, TaskType taskType, TaskAttemptId attemptID, int
				 numMapTasks, bool renameOutputs, IDictionary<TaskAttemptID, MapOutputFile> localMapFiles
				)
			{
				TaskAttemptID classicAttemptID = TypeConverter.FromYarn(attemptID);
				try
				{
					JobConf conf = new JobConf(this._enclosing.GetConfig());
					conf.Set(JobContext.TaskId, task.GetTaskID().ToString());
					conf.Set(JobContext.TaskAttemptId, classicAttemptID.ToString());
					conf.SetBoolean(JobContext.TaskIsmap, (taskType == TaskType.Map));
					conf.SetInt(JobContext.TaskPartition, task.GetPartition());
					conf.Set(JobContext.Id, task.GetJobID().ToString());
					// Use the AM's local dir env to generate the intermediate step 
					// output files
					string[] localSysDirs = StringUtils.GetTrimmedStrings(Runtime.Getenv(ApplicationConstants.Environment
						.LocalDirs.ToString()));
					conf.SetStrings(MRConfig.LocalDir, localSysDirs);
					LocalContainerLauncher.Log.Info(MRConfig.LocalDir + " for uber task: " + conf.Get
						(MRConfig.LocalDir));
					// mark this as an uberized subtask so it can set task counter
					// (longer-term/FIXME:  could redefine as job counter and send
					// "JobCounterEvent" to JobImpl on [successful] completion of subtask;
					// will need new Job state-machine transition and JobImpl jobCounters
					// map to handle)
					conf.SetBoolean("mapreduce.task.uberized", true);
					// Check and handle Encrypted spill key
					task.SetEncryptedSpillKey(this._enclosing.encryptedSpillKey);
					YarnChild.SetEncryptedSpillKeyIfRequired(task);
					// META-FIXME: do we want the extra sanity-checking (doneWithMaps,
					// etc.), or just assume/hope the state machine(s) and uber-AM work
					// as expected?
					if (taskType == TaskType.Map)
					{
						if (this.doneWithMaps)
						{
							LocalContainerLauncher.Log.Error("CONTAINER_REMOTE_LAUNCH contains a map task (" 
								+ attemptID + "), but should be finished with maps");
							throw new RuntimeException();
						}
						MapTask map = (MapTask)task;
						map.SetConf(conf);
						map.Run(conf, this._enclosing.umbilical);
						if (renameOutputs)
						{
							MapOutputFile renamed = LocalContainerLauncher.RenameMapOutputForReduce(conf, attemptID
								, map.GetMapOutputFile());
							localMapFiles[classicAttemptID] = renamed;
						}
						this.Relocalize();
						if (++this.finishedSubMaps == numMapTasks)
						{
							this.doneWithMaps = true;
						}
					}
					else
					{
						/* TaskType.REDUCE */
						if (!this.doneWithMaps)
						{
							// check if event-queue empty?  whole idea of counting maps vs. 
							// checking event queue is a tad wacky...but could enforce ordering
							// (assuming no "lost events") at LocalMRAppMaster [CURRENT BUG(?): 
							// doesn't send reduce event until maps all done]
							LocalContainerLauncher.Log.Error("CONTAINER_REMOTE_LAUNCH contains a reduce task ("
								 + attemptID + "), but not yet finished with maps");
							throw new RuntimeException();
						}
						// a.k.a. "mapreduce.jobtracker.address" in LocalJobRunner:
						// set framework name to local to make task local
						conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
						conf.Set(MRConfig.MasterAddress, "local");
						// bypass shuffle
						ReduceTask reduce = (ReduceTask)task;
						reduce.SetLocalMapFiles(localMapFiles);
						reduce.SetConf(conf);
						reduce.Run(conf, this._enclosing.umbilical);
						this.Relocalize();
					}
				}
				catch (FSError e)
				{
					LocalContainerLauncher.Log.Fatal("FSError from child", e);
					// umbilical:  MRAppMaster creates (taskAttemptListener), passes to us
					if (!ShutdownHookManager.Get().IsShutdownInProgress())
					{
						this._enclosing.umbilical.FsError(classicAttemptID, e.Message);
					}
					throw new RuntimeException();
				}
				catch (Exception exception)
				{
					LocalContainerLauncher.Log.Warn("Exception running local (uberized) 'child' : " +
						 StringUtils.StringifyException(exception));
					try
					{
						if (task != null)
						{
							// do cleanup for the task
							task.TaskCleanup(this._enclosing.umbilical);
						}
					}
					catch (Exception e)
					{
						LocalContainerLauncher.Log.Info("Exception cleaning up: " + StringUtils.StringifyException
							(e));
					}
					// Report back any failures, for diagnostic purposes
					this._enclosing.umbilical.ReportDiagnosticInfo(classicAttemptID, StringUtils.StringifyException
						(exception));
					throw new RuntimeException();
				}
				catch (Exception throwable)
				{
					LocalContainerLauncher.Log.Fatal("Error running local (uberized) 'child' : " + StringUtils
						.StringifyException(throwable));
					if (!ShutdownHookManager.Get().IsShutdownInProgress())
					{
						Exception tCause = throwable.InnerException;
						string cause = (tCause == null) ? throwable.Message : StringUtils.StringifyException
							(tCause);
						this._enclosing.umbilical.FatalError(classicAttemptID, cause);
					}
					throw new RuntimeException();
				}
			}

			/// <summary>
			/// Also within the local filesystem, we need to restore the initial state
			/// of the directory as much as possible.
			/// </summary>
			/// <remarks>
			/// Also within the local filesystem, we need to restore the initial state
			/// of the directory as much as possible.  Compare current contents against
			/// the saved original state and nuke everything that doesn't belong, with
			/// the exception of the renamed map outputs.
			/// Any jobs that go out of their way to rename or delete things from the
			/// local directory are considered broken and deserve what they get...
			/// </remarks>
			private void Relocalize()
			{
				FilePath[] curLocalFiles = LocalContainerLauncher.curDir.ListFiles();
				for (int j = 0; j < curLocalFiles.Length; ++j)
				{
					if (!this._enclosing.localizedFiles.Contains(curLocalFiles[j]))
					{
						// found one that wasn't there before:  delete it
						bool deleted = false;
						try
						{
							if (this._enclosing.curFC != null)
							{
								// this is recursive, unlike File delete():
								deleted = this._enclosing.curFC.Delete(new Path(curLocalFiles[j].GetName()), true
									);
							}
						}
						catch (IOException)
						{
							deleted = false;
						}
						if (!deleted)
						{
							LocalContainerLauncher.Log.Warn("Unable to delete unexpected local file/dir " + curLocalFiles
								[j].GetName() + ": insufficient permissions?");
						}
					}
				}
			}

			private readonly LocalContainerLauncher _enclosing;
		}

		// end EventHandler
		/// <summary>
		/// Within the _local_ filesystem (not HDFS), all activity takes place within
		/// a subdir inside one of the LOCAL_DIRS
		/// (${local.dir}/usercache/$user/appcache/$appId/$contId/),
		/// and all sub-MapTasks create the same filename ("file.out").
		/// </summary>
		/// <remarks>
		/// Within the _local_ filesystem (not HDFS), all activity takes place within
		/// a subdir inside one of the LOCAL_DIRS
		/// (${local.dir}/usercache/$user/appcache/$appId/$contId/),
		/// and all sub-MapTasks create the same filename ("file.out").  Rename that
		/// to something unique (e.g., "map_0.out") to avoid possible collisions.
		/// Longer-term, we'll modify [something] to use TaskAttemptID-based
		/// filenames instead of "file.out". (All of this is entirely internal,
		/// so there are no particular compatibility issues.)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal static MapOutputFile RenameMapOutputForReduce(JobConf conf, TaskAttemptId
			 mapId, MapOutputFile subMapOutputFile)
		{
			FileSystem localFs = FileSystem.GetLocal(conf);
			// move map output to reduce input
			Path mapOut = subMapOutputFile.GetOutputFile();
			FileStatus mStatus = localFs.GetFileStatus(mapOut);
			Path reduceIn = subMapOutputFile.GetInputFileForWrite(((TaskID)TypeConverter.FromYarn
				(mapId).GetTaskID()), mStatus.GetLen());
			Path mapOutIndex = subMapOutputFile.GetOutputIndexFile();
			Path reduceInIndex = new Path(reduceIn.ToString() + ".index");
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Renaming map output file for task attempt " + mapId.ToString() + " from original location "
					 + mapOut.ToString() + " to destination " + reduceIn.ToString());
			}
			if (!localFs.Mkdirs(reduceIn.GetParent()))
			{
				throw new IOException("Mkdirs failed to create " + reduceIn.GetParent().ToString(
					));
			}
			if (!localFs.Rename(mapOut, reduceIn))
			{
				throw new IOException("Couldn't rename " + mapOut);
			}
			if (!localFs.Rename(mapOutIndex, reduceInIndex))
			{
				throw new IOException("Couldn't rename " + mapOutIndex);
			}
			return new LocalContainerLauncher.RenamedMapOutputFile(reduceIn);
		}

		private class RenamedMapOutputFile : MapOutputFile
		{
			private Path path;

			public RenamedMapOutputFile(Path path)
			{
				this.path = path;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetOutputFile()
			{
				return path;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetOutputFileForWrite(long size)
			{
				throw new NotSupportedException();
			}

			public override Path GetOutputFileForWriteInVolume(Path existing)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetOutputIndexFile()
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetOutputIndexFileForWrite(long size)
			{
				throw new NotSupportedException();
			}

			public override Path GetOutputIndexFileForWriteInVolume(Path existing)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetSpillFile(int spillNumber)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetSpillFileForWrite(int spillNumber, long size)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetSpillIndexFile(int spillNumber)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetSpillIndexFileForWrite(int spillNumber, long size)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetInputFile(int mapId)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetInputFileForWrite(TaskID mapId, long size)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RemoveAll()
			{
				throw new NotSupportedException();
			}
		}
	}
}
