using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class DeletionService : AbstractService
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.DeletionService
			));

		private int debugDelay;

		private readonly ContainerExecutor exec;

		private ScheduledThreadPoolExecutor sched;

		private static readonly FileContext lfs = GetLfs();

		private readonly NMStateStoreService stateStore;

		private AtomicInteger nextTaskId = new AtomicInteger(0);

		internal static FileContext GetLfs()
		{
			try
			{
				return FileContext.GetLocalFSFileContext();
			}
			catch (UnsupportedFileSystemException e)
			{
				throw new RuntimeException(e);
			}
		}

		public DeletionService(ContainerExecutor exec)
			: this(exec, new NMNullStateStoreService())
		{
		}

		public DeletionService(ContainerExecutor exec, NMStateStoreService stateStore)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.DeletionService).FullName
				)
		{
			this.exec = exec;
			this.debugDelay = 0;
			this.stateStore = stateStore;
		}

		/// <summary>Delete the path(s) as this user.</summary>
		/// <param name="user">The user to delete as, or the JVM user if null</param>
		/// <param name="subDir">the sub directory name</param>
		/// <param name="baseDirs">the base directories which contains the subDir's</param>
		public virtual void Delete(string user, Path subDir, params Path[] baseDirs)
		{
			// TODO if parent owned by NM, rename within parent inline
			if (debugDelay != -1)
			{
				IList<Path> baseDirList = null;
				if (baseDirs != null && baseDirs.Length != 0)
				{
					baseDirList = Arrays.AsList(baseDirs);
				}
				DeletionService.FileDeletionTask task = new DeletionService.FileDeletionTask(this
					, user, subDir, baseDirList);
				RecordDeletionTaskInStateStore(task);
				sched.Schedule(task, debugDelay, TimeUnit.Seconds);
			}
		}

		public virtual void ScheduleFileDeletionTask(DeletionService.FileDeletionTask fileDeletionTask
			)
		{
			if (debugDelay != -1)
			{
				RecordDeletionTaskInStateStore(fileDeletionTask);
				sched.Schedule(fileDeletionTask, debugDelay, TimeUnit.Seconds);
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("DeletionService #%d"
				).Build();
			if (conf != null)
			{
				sched = new DeletionService.DelServiceSchedThreadPoolExecutor(conf.GetInt(YarnConfiguration
					.NmDeleteThreadCount, YarnConfiguration.DefaultNmDeleteThreadCount), tf);
				debugDelay = conf.GetInt(YarnConfiguration.DebugNmDeleteDelaySec, 0);
			}
			else
			{
				sched = new DeletionService.DelServiceSchedThreadPoolExecutor(YarnConfiguration.DefaultNmDeleteThreadCount
					, tf);
			}
			sched.SetExecuteExistingDelayedTasksAfterShutdownPolicy(false);
			sched.SetKeepAliveTime(60L, TimeUnit.Seconds);
			if (stateStore.CanRecover())
			{
				Recover(stateStore.LoadDeletionServiceState());
			}
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (sched != null)
			{
				sched.Shutdown();
				bool terminated = false;
				try
				{
					terminated = sched.AwaitTermination(10, TimeUnit.Seconds);
				}
				catch (Exception)
				{
				}
				if (terminated != true)
				{
					sched.ShutdownNow();
				}
			}
			base.ServiceStop();
		}

		/// <summary>Determine if the service has completely stopped.</summary>
		/// <remarks>
		/// Determine if the service has completely stopped.
		/// Used only by unit tests
		/// </remarks>
		/// <returns>true if service has completely stopped</returns>
		[InterfaceAudience.Private]
		public virtual bool IsTerminated()
		{
			return GetServiceState() == Service.STATE.Stopped && sched.IsTerminated();
		}

		private class DelServiceSchedThreadPoolExecutor : ScheduledThreadPoolExecutor
		{
			public DelServiceSchedThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory
				)
				: base(corePoolSize, threadFactory)
			{
			}

			protected override void AfterExecute(Runnable task, Exception exception)
			{
				if (task is FutureTask<object>)
				{
					FutureTask<object> futureTask = (FutureTask<object>)task;
					if (!futureTask.IsCancelled())
					{
						try
						{
							futureTask.Get();
						}
						catch (ExecutionException ee)
						{
							exception = ee.InnerException;
						}
						catch (Exception ie)
						{
							exception = ie;
						}
					}
				}
				if (exception != null)
				{
					Log.Error("Exception during execution of task in DeletionService", exception);
				}
			}
		}

		public class FileDeletionTask : Runnable
		{
			public const int InvalidTaskId = -1;

			private int taskId;

			private readonly string user;

			private readonly Path subDir;

			private readonly IList<Path> baseDirs;

			private readonly AtomicInteger numberOfPendingPredecessorTasks;

			private readonly ICollection<DeletionService.FileDeletionTask> successorTaskSet;

			private readonly DeletionService delService;

			private bool success;

			private FileDeletionTask(DeletionService delService, string user, Path subDir, IList
				<Path> baseDirs)
				: this(InvalidTaskId, delService, user, subDir, baseDirs)
			{
			}

			private FileDeletionTask(int taskId, DeletionService delService, string user, Path
				 subDir, IList<Path> baseDirs)
			{
				// By default all tasks will start as success=true; however if any of
				// the dependent task fails then it will be marked as false in
				// fileDeletionTaskFinished().
				this.taskId = taskId;
				this.delService = delService;
				this.user = user;
				this.subDir = subDir;
				this.baseDirs = baseDirs;
				this.successorTaskSet = new HashSet<DeletionService.FileDeletionTask>();
				this.numberOfPendingPredecessorTasks = new AtomicInteger(0);
				success = true;
			}

			/// <summary>increments and returns pending predecessor task count</summary>
			public virtual int IncrementAndGetPendingPredecessorTasks()
			{
				return numberOfPendingPredecessorTasks.IncrementAndGet();
			}

			/// <summary>decrements and returns pending predecessor task count</summary>
			public virtual int DecrementAndGetPendingPredecessorTasks()
			{
				return numberOfPendingPredecessorTasks.DecrementAndGet();
			}

			[VisibleForTesting]
			public virtual string GetUser()
			{
				return this.user;
			}

			[VisibleForTesting]
			public virtual Path GetSubDir()
			{
				return this.subDir;
			}

			[VisibleForTesting]
			public virtual IList<Path> GetBaseDirs()
			{
				return this.baseDirs;
			}

			public virtual void SetSuccess(bool success)
			{
				lock (this)
				{
					this.success = success;
				}
			}

			public virtual bool GetSucess()
			{
				lock (this)
				{
					return this.success;
				}
			}

			public virtual DeletionService.FileDeletionTask[] GetSuccessorTasks()
			{
				lock (this)
				{
					DeletionService.FileDeletionTask[] successors = new DeletionService.FileDeletionTask
						[successorTaskSet.Count];
					return Sharpen.Collections.ToArray(successorTaskSet, successors);
				}
			}

			public virtual void Run()
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug(this);
				}
				bool error = false;
				if (null == user)
				{
					if (baseDirs == null || baseDirs.Count == 0)
					{
						Log.Debug("NM deleting absolute path : " + subDir);
						try
						{
							lfs.Delete(subDir, true);
						}
						catch (IOException)
						{
							error = true;
							Log.Warn("Failed to delete " + subDir);
						}
					}
					else
					{
						foreach (Path baseDir in baseDirs)
						{
							Path del = subDir == null ? baseDir : new Path(baseDir, subDir);
							Log.Debug("NM deleting path : " + del);
							try
							{
								lfs.Delete(del, true);
							}
							catch (IOException)
							{
								error = true;
								Log.Warn("Failed to delete " + subDir);
							}
						}
					}
				}
				else
				{
					try
					{
						Log.Debug("Deleting path: [" + subDir + "] as user: [" + user + "]");
						if (baseDirs == null || baseDirs.Count == 0)
						{
							delService.exec.DeleteAsUser(user, subDir, (Path[])null);
						}
						else
						{
							delService.exec.DeleteAsUser(user, subDir, Sharpen.Collections.ToArray(baseDirs, 
								new Path[0]));
						}
					}
					catch (IOException e)
					{
						error = true;
						Log.Warn("Failed to delete as user " + user, e);
					}
					catch (Exception e)
					{
						error = true;
						Log.Warn("Failed to delete as user " + user, e);
					}
				}
				if (error)
				{
					SetSuccess(!error);
				}
				FileDeletionTaskFinished();
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder("\nFileDeletionTask : ");
				sb.Append("  user : ").Append(this.user);
				sb.Append("  subDir : ").Append(subDir == null ? "null" : subDir.ToString());
				sb.Append("  baseDir : ");
				if (baseDirs == null || baseDirs.Count == 0)
				{
					sb.Append("null");
				}
				else
				{
					foreach (Path baseDir in baseDirs)
					{
						sb.Append(baseDir.ToString()).Append(',');
					}
				}
				return sb.ToString();
			}

			/// <summary>
			/// If there is a task dependency between say tasks 1,2,3 such that
			/// task2 and task3 can be started only after task1 then we should define
			/// task2 and task3 as successor tasks for task1.
			/// </summary>
			/// <remarks>
			/// If there is a task dependency between say tasks 1,2,3 such that
			/// task2 and task3 can be started only after task1 then we should define
			/// task2 and task3 as successor tasks for task1.
			/// Note:- Task dependency should be defined prior to
			/// </remarks>
			/// <param name="successorTask"/>
			public virtual void AddFileDeletionTaskDependency(DeletionService.FileDeletionTask
				 successorTask)
			{
				lock (this)
				{
					if (successorTaskSet.AddItem(successorTask))
					{
						successorTask.IncrementAndGetPendingPredecessorTasks();
					}
				}
			}

			/*
			* This is called when
			* 1) Current file deletion task ran and finished.
			* 2) This can be even directly called by predecessor task if one of the
			* dependent tasks of it has failed marking its success = false.
			*/
			private void FileDeletionTaskFinished()
			{
				lock (this)
				{
					try
					{
						delService.stateStore.RemoveDeletionTask(taskId);
					}
					catch (IOException e)
					{
						Log.Error("Unable to remove deletion task " + taskId + " from state store", e);
					}
					IEnumerator<DeletionService.FileDeletionTask> successorTaskI = this.successorTaskSet
						.GetEnumerator();
					while (successorTaskI.HasNext())
					{
						DeletionService.FileDeletionTask successorTask = successorTaskI.Next();
						if (!success)
						{
							successorTask.SetSuccess(success);
						}
						int count = successorTask.DecrementAndGetPendingPredecessorTasks();
						if (count == 0)
						{
							if (successorTask.GetSucess())
							{
								successorTask.delService.ScheduleFileDeletionTask(successorTask);
							}
							else
							{
								successorTask.FileDeletionTaskFinished();
							}
						}
					}
				}
			}
		}

		/// <summary>Helper method to create file deletion task.</summary>
		/// <remarks>
		/// Helper method to create file deletion task. To be used only if we need
		/// a way to define dependencies between deletion tasks.
		/// </remarks>
		/// <param name="user">user on whose behalf this task is suppose to run</param>
		/// <param name="subDir">
		/// sub directory as required in
		/// <see cref="Delete(string, Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.FS.Path[])
		/// 	"/>
		/// </param>
		/// <param name="baseDirs">
		/// base directories as required in
		/// <see cref="Delete(string, Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.FS.Path[])
		/// 	"/>
		/// </param>
		public virtual DeletionService.FileDeletionTask CreateFileDeletionTask(string user
			, Path subDir, Path[] baseDirs)
		{
			return new DeletionService.FileDeletionTask(this, user, subDir, Arrays.AsList(baseDirs
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private void Recover(NMStateStoreService.RecoveredDeletionServiceState state)
		{
			IList<YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto> taskProtos
				 = state.GetTasks();
			IDictionary<int, DeletionService.DeletionTaskRecoveryInfo> idToInfoMap = new Dictionary
				<int, DeletionService.DeletionTaskRecoveryInfo>(taskProtos.Count);
			ICollection<int> successorTasks = new HashSet<int>();
			foreach (YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto proto
				 in taskProtos)
			{
				DeletionService.DeletionTaskRecoveryInfo info = ParseTaskProto(proto);
				idToInfoMap[info.task.taskId] = info;
				nextTaskId.Set(Math.Max(nextTaskId.Get(), info.task.taskId));
				Sharpen.Collections.AddAll(successorTasks, info.successorTaskIds);
			}
			// restore the task dependencies and schedule the deletion tasks that
			// have no predecessors
			long now = Runtime.CurrentTimeMillis();
			foreach (DeletionService.DeletionTaskRecoveryInfo info_1 in idToInfoMap.Values)
			{
				foreach (int successorId in info_1.successorTaskIds)
				{
					DeletionService.DeletionTaskRecoveryInfo successor = idToInfoMap[successorId];
					if (successor != null)
					{
						info_1.task.AddFileDeletionTaskDependency(successor.task);
					}
					else
					{
						Log.Error("Unable to locate dependency task for deletion task " + info_1.task.taskId
							 + " at " + info_1.task.GetSubDir());
					}
				}
				if (!successorTasks.Contains(info_1.task.taskId))
				{
					long msecTilDeletion = info_1.deletionTimestamp - now;
					sched.Schedule(info_1.task, msecTilDeletion, TimeUnit.Milliseconds);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private DeletionService.DeletionTaskRecoveryInfo ParseTaskProto(YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
			 proto)
		{
			int taskId = proto.GetId();
			string user = proto.HasUser() ? proto.GetUser() : null;
			Path subdir = null;
			IList<Path> basePaths = null;
			if (proto.HasSubdir())
			{
				subdir = new Path(proto.GetSubdir());
			}
			IList<string> basedirs = proto.GetBasedirsList();
			if (basedirs != null && basedirs.Count > 0)
			{
				basePaths = new AList<Path>(basedirs.Count);
				foreach (string basedir in basedirs)
				{
					basePaths.AddItem(new Path(basedir));
				}
			}
			DeletionService.FileDeletionTask task = new DeletionService.FileDeletionTask(taskId
				, this, user, subdir, basePaths);
			return new DeletionService.DeletionTaskRecoveryInfo(task, proto.GetSuccessorIdsList
				(), proto.GetDeletionTime());
		}

		private int GenerateTaskId()
		{
			// get the next ID but avoid an invalid ID
			int taskId = nextTaskId.IncrementAndGet();
			while (taskId == DeletionService.FileDeletionTask.InvalidTaskId)
			{
				taskId = nextTaskId.IncrementAndGet();
			}
			return taskId;
		}

		private void RecordDeletionTaskInStateStore(DeletionService.FileDeletionTask task
			)
		{
			if (!stateStore.CanRecover())
			{
				// optimize the case where we aren't really recording
				return;
			}
			if (task.taskId != DeletionService.FileDeletionTask.InvalidTaskId)
			{
				return;
			}
			// task already recorded
			task.taskId = GenerateTaskId();
			DeletionService.FileDeletionTask[] successors = task.GetSuccessorTasks();
			// store successors first to ensure task IDs have been generated for them
			foreach (DeletionService.FileDeletionTask successor in successors)
			{
				RecordDeletionTaskInStateStore(successor);
			}
			YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto.Builder builder
				 = YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto.NewBuilder
				();
			builder.SetId(task.taskId);
			if (task.GetUser() != null)
			{
				builder.SetUser(task.GetUser());
			}
			if (task.GetSubDir() != null)
			{
				builder.SetSubdir(task.GetSubDir().ToString());
			}
			builder.SetDeletionTime(Runtime.CurrentTimeMillis() + TimeUnit.Milliseconds.Convert
				(debugDelay, TimeUnit.Seconds));
			if (task.GetBaseDirs() != null)
			{
				foreach (Path dir in task.GetBaseDirs())
				{
					builder.AddBasedirs(dir.ToString());
				}
			}
			foreach (DeletionService.FileDeletionTask successor_1 in successors)
			{
				builder.AddSuccessorIds(successor_1.taskId);
			}
			try
			{
				stateStore.StoreDeletionTask(task.taskId, ((YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
					)builder.Build()));
			}
			catch (IOException e)
			{
				Log.Error("Unable to store deletion task " + task.taskId + " for " + task.GetSubDir
					(), e);
			}
		}

		private class DeletionTaskRecoveryInfo
		{
			internal DeletionService.FileDeletionTask task;

			internal IList<int> successorTaskIds;

			internal long deletionTimestamp;

			public DeletionTaskRecoveryInfo(DeletionService.FileDeletionTask task, IList<int>
				 successorTaskIds, long deletionTimestamp)
			{
				this.task = task;
				this.successorTaskIds = successorTaskIds;
				this.deletionTimestamp = deletionTimestamp;
			}
		}
	}
}
