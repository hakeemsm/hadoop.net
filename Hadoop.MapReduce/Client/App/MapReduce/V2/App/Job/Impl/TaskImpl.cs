using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Metrics;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.State;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	/// <summary>Implementation of Task interface.</summary>
	public abstract class TaskImpl : Task, EventHandler<TaskEvent>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskImpl
			));

		private const string Speculation = "Speculation: ";

		protected internal readonly JobConf conf;

		protected internal readonly Path jobFile;

		protected internal readonly int partition;

		protected internal readonly TaskAttemptListener taskAttemptListener;

		protected internal readonly EventHandler eventHandler;

		private readonly TaskId taskId;

		private IDictionary<TaskAttemptId, TaskAttempt> attempts;

		private readonly int maxAttempts;

		protected internal readonly Clock clock;

		private readonly Lock readLock;

		private readonly Lock writeLock;

		private readonly MRAppMetrics metrics;

		protected internal readonly AppContext appContext;

		private long scheduledTime;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		protected internal bool encryptedShuffle;

		protected internal Credentials credentials;

		protected internal Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken;

		private TaskAttemptId commitAttempt;

		private TaskAttemptId successfulAttempt;

		private readonly ICollection<TaskAttemptId> failedAttempts;

		private readonly ICollection<TaskAttemptId> finishedAttempts;

		private readonly ICollection<TaskAttemptId> inProgressAttempts;

		private bool historyTaskStartGenerated = false;

		private static readonly SingleArcTransition<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskImpl
			, TaskEvent> AttemptKilledTransition = new TaskImpl.AttemptKilledTransition();

		private static readonly SingleArcTransition<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskImpl
			, TaskEvent> KillTransition = new TaskImpl.KillTransition();

		private static readonly StateMachineFactory<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskImpl
			, TaskStateInternal, TaskEventType, TaskEvent> stateMachineFactory = new StateMachineFactory
			<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskImpl, TaskStateInternal, TaskEventType
			, TaskEvent>(TaskStateInternal.New).AddTransition(TaskStateInternal.New, TaskStateInternal
			.Scheduled, TaskEventType.TSchedule, new TaskImpl.InitialScheduleTransition()).AddTransition
			(TaskStateInternal.New, TaskStateInternal.Killed, TaskEventType.TKill, new TaskImpl.KillNewTransition
			()).AddTransition(TaskStateInternal.New, EnumSet.Of(TaskStateInternal.Failed, TaskStateInternal
			.Killed, TaskStateInternal.Running, TaskStateInternal.Succeeded), TaskEventType.
			TRecover, new TaskImpl.RecoverTransition()).AddTransition(TaskStateInternal.Scheduled
			, TaskStateInternal.Running, TaskEventType.TAttemptLaunched, new TaskImpl.LaunchTransition
			()).AddTransition(TaskStateInternal.Scheduled, TaskStateInternal.KillWait, TaskEventType
			.TKill, KillTransition).AddTransition(TaskStateInternal.Scheduled, TaskStateInternal
			.Scheduled, TaskEventType.TAttemptKilled, AttemptKilledTransition).AddTransition
			(TaskStateInternal.Scheduled, EnumSet.Of(TaskStateInternal.Scheduled, TaskStateInternal
			.Failed), TaskEventType.TAttemptFailed, new TaskImpl.AttemptFailedTransition()).
			AddTransition(TaskStateInternal.Running, TaskStateInternal.Running, TaskEventType
			.TAttemptLaunched).AddTransition(TaskStateInternal.Running, TaskStateInternal.Running
			, TaskEventType.TAttemptCommitPending, new TaskImpl.AttemptCommitPendingTransition
			()).AddTransition(TaskStateInternal.Running, TaskStateInternal.Running, TaskEventType
			.TAddSpecAttempt, new TaskImpl.RedundantScheduleTransition()).AddTransition(TaskStateInternal
			.Running, TaskStateInternal.Succeeded, TaskEventType.TAttemptSucceeded, new TaskImpl.AttemptSucceededTransition
			()).AddTransition(TaskStateInternal.Running, TaskStateInternal.Running, TaskEventType
			.TAttemptKilled, AttemptKilledTransition).AddTransition(TaskStateInternal.Running
			, EnumSet.Of(TaskStateInternal.Running, TaskStateInternal.Failed), TaskEventType
			.TAttemptFailed, new TaskImpl.AttemptFailedTransition()).AddTransition(TaskStateInternal
			.Running, TaskStateInternal.KillWait, TaskEventType.TKill, KillTransition).AddTransition
			(TaskStateInternal.KillWait, EnumSet.Of(TaskStateInternal.KillWait, TaskStateInternal
			.Killed), TaskEventType.TAttemptKilled, new TaskImpl.KillWaitAttemptKilledTransition
			()).AddTransition(TaskStateInternal.KillWait, EnumSet.Of(TaskStateInternal.KillWait
			, TaskStateInternal.Killed), TaskEventType.TAttemptSucceeded, new TaskImpl.KillWaitAttemptSucceededTransition
			()).AddTransition(TaskStateInternal.KillWait, EnumSet.Of(TaskStateInternal.KillWait
			, TaskStateInternal.Killed), TaskEventType.TAttemptFailed, new TaskImpl.KillWaitAttemptFailedTransition
			()).AddTransition(TaskStateInternal.KillWait, TaskStateInternal.KillWait, EnumSet
			.Of(TaskEventType.TKill, TaskEventType.TAttemptLaunched, TaskEventType.TAttemptCommitPending
			, TaskEventType.TAddSpecAttempt)).AddTransition(TaskStateInternal.Succeeded, EnumSet
			.Of(TaskStateInternal.Scheduled, TaskStateInternal.Succeeded, TaskStateInternal.
			Failed), TaskEventType.TAttemptFailed, new TaskImpl.RetroactiveFailureTransition
			()).AddTransition(TaskStateInternal.Succeeded, EnumSet.Of(TaskStateInternal.Scheduled
			, TaskStateInternal.Succeeded), TaskEventType.TAttemptKilled, new TaskImpl.RetroactiveKilledTransition
			()).AddTransition(TaskStateInternal.Succeeded, TaskStateInternal.Succeeded, TaskEventType
			.TAttemptSucceeded, new TaskImpl.AttemptSucceededAtSucceededTransition()).AddTransition
			(TaskStateInternal.Succeeded, TaskStateInternal.Succeeded, EnumSet.Of(TaskEventType
			.TAddSpecAttempt, TaskEventType.TAttemptCommitPending, TaskEventType.TAttemptLaunched
			, TaskEventType.TKill)).AddTransition(TaskStateInternal.Failed, TaskStateInternal
			.Failed, EnumSet.Of(TaskEventType.TKill, TaskEventType.TAddSpecAttempt, TaskEventType
			.TAttemptCommitPending, TaskEventType.TAttemptFailed, TaskEventType.TAttemptKilled
			, TaskEventType.TAttemptLaunched, TaskEventType.TAttemptSucceeded)).AddTransition
			(TaskStateInternal.Killed, TaskStateInternal.Killed, EnumSet.Of(TaskEventType.TKill
			, TaskEventType.TAttemptKilled, TaskEventType.TAddSpecAttempt)).InstallTopology(
			);

		private readonly StateMachine<TaskStateInternal, TaskEventType, TaskEvent> stateMachine;

		protected internal int nextAttemptNumber = 0;

		private sealed class _IComparer_275 : IComparer<JobHistoryParser.TaskAttemptInfo>
		{
			public _IComparer_275()
			{
			}

			//should be set to one which comes first
			//saying COMMIT_PENDING
			// Track the finished attempts - successful, failed and killed
			// counts the number of attempts that are either running or in a state where
			//  they will come to be running when they get a Container
			// define the state machine of Task
			// Transitions from NEW state
			// Transitions from SCHEDULED state
			//when the first attempt is launched, the task state is set to RUNNING
			// Transitions from RUNNING state
			//more attempts may start later
			// Transitions from KILL_WAIT state
			// Ignore-able transitions.
			// Transitions from SUCCEEDED state
			// Ignore-able transitions.
			// Transitions from FAILED state        
			// Transitions from KILLED state
			// There could be a race condition where TaskImpl might receive
			// T_ATTEMPT_SUCCEEDED followed by T_ATTEMPTED_KILLED for the same attempt.
			// a. The task is in KILL_WAIT.
			// b. Before TA transitions to SUCCEEDED state, Task sends TA_KILL event.
			// c. TA transitions to SUCCEEDED state and thus send T_ATTEMPT_SUCCEEDED
			//    to the task. The task transitions to KILLED state.
			// d. TA processes TA_KILL event and sends T_ATTEMPT_KILLED to the task.
			// create the topology tables
			// By default, the next TaskAttempt number is zero. Changes during recovery  
			// For sorting task attempts by completion time
			public int Compare(JobHistoryParser.TaskAttemptInfo a, JobHistoryParser.TaskAttemptInfo
				 b)
			{
				long diff = a.GetFinishTime() - b.GetFinishTime();
				return diff == 0 ? 0 : (diff < 0 ? -1 : 1);
			}
		}

		private static readonly IComparer<JobHistoryParser.TaskAttemptInfo> TaInfoComparator
			 = new _IComparer_275();

		public virtual TaskState GetState()
		{
			readLock.Lock();
			try
			{
				return GetExternalState(GetInternalState());
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public TaskImpl(JobId jobId, TaskType taskType, int partition, EventHandler eventHandler
			, Path remoteJobConfFile, JobConf conf, TaskAttemptListener taskAttemptListener, 
			Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken, Credentials
			 credentials, Clock clock, int appAttemptId, MRAppMetrics metrics, AppContext appContext
			)
		{
			this.conf = conf;
			this.clock = clock;
			this.jobFile = remoteJobConfFile;
			ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
			readLock = readWriteLock.ReadLock();
			writeLock = readWriteLock.WriteLock();
			this.attempts = Sharpen.Collections.EmptyMap();
			this.finishedAttempts = new HashSet<TaskAttemptId>(2);
			this.failedAttempts = new HashSet<TaskAttemptId>(2);
			this.inProgressAttempts = new HashSet<TaskAttemptId>(2);
			// This overridable method call is okay in a constructor because we
			//  have a convention that none of the overrides depends on any
			//  fields that need initialization.
			maxAttempts = GetMaxAttempts();
			taskId = MRBuilderUtils.NewTaskId(jobId, partition, taskType);
			this.partition = partition;
			this.taskAttemptListener = taskAttemptListener;
			this.eventHandler = eventHandler;
			this.credentials = credentials;
			this.jobToken = jobToken;
			this.metrics = metrics;
			this.appContext = appContext;
			this.encryptedShuffle = conf.GetBoolean(MRConfig.ShuffleSslEnabledKey, MRConfig.ShuffleSslEnabledDefault
				);
			// This "this leak" is okay because the retained pointer is in an
			//  instance variable.
			stateMachine = stateMachineFactory.Make(this);
			// All the new TaskAttemptIDs are generated based on MR
			// ApplicationAttemptID so that attempts from previous lives don't
			// over-step the current one. This assumes that a task won't have more
			// than 1000 attempts in its single generation, which is very reasonable.
			nextAttemptNumber = (appAttemptId - 1) * 1000;
		}

		public virtual IDictionary<TaskAttemptId, TaskAttempt> GetAttempts()
		{
			readLock.Lock();
			try
			{
				if (attempts.Count <= 1)
				{
					return attempts;
				}
				IDictionary<TaskAttemptId, TaskAttempt> result = new LinkedHashMap<TaskAttemptId, 
					TaskAttempt>();
				result.PutAll(attempts);
				return result;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual TaskAttempt GetAttempt(TaskAttemptId attemptID)
		{
			readLock.Lock();
			try
			{
				return attempts[attemptID];
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual TaskId GetID()
		{
			return taskId;
		}

		public virtual bool IsFinished()
		{
			readLock.Lock();
			try
			{
				// TODO: Use stateMachine level method?
				return (GetInternalState() == TaskStateInternal.Succeeded || GetInternalState() ==
					 TaskStateInternal.Failed || GetInternalState() == TaskStateInternal.Killed);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual TaskReport GetReport()
		{
			TaskReport report = recordFactory.NewRecordInstance<TaskReport>();
			readLock.Lock();
			try
			{
				TaskAttempt bestAttempt = SelectBestAttempt();
				report.SetTaskId(taskId);
				report.SetStartTime(GetLaunchTime());
				report.SetFinishTime(GetFinishTime());
				report.SetTaskState(GetState());
				report.SetProgress(bestAttempt == null ? 0f : bestAttempt.GetProgress());
				report.SetStatus(bestAttempt == null ? string.Empty : bestAttempt.GetReport().GetStateString
					());
				foreach (TaskAttempt attempt in attempts.Values)
				{
					if (TaskAttemptState.Running.Equals(attempt.GetState()))
					{
						report.AddRunningAttempt(attempt.GetID());
					}
				}
				report.SetSuccessfulAttempt(successfulAttempt);
				foreach (TaskAttempt att in attempts.Values)
				{
					string prefix = "AttemptID:" + att.GetID() + " Info:";
					foreach (CharSequence cs in att.GetDiagnostics())
					{
						report.AddDiagnostics(prefix + cs);
					}
				}
				// Add a copy of counters as the last step so that their lifetime on heap
				// is as small as possible.
				report.SetCounters(TypeConverter.ToYarn(bestAttempt == null ? TaskAttemptImpl.EmptyCounters
					 : bestAttempt.GetCounters()));
				return report;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual Counters GetCounters()
		{
			Counters counters = null;
			readLock.Lock();
			try
			{
				TaskAttempt bestAttempt = SelectBestAttempt();
				if (bestAttempt != null)
				{
					counters = bestAttempt.GetCounters();
				}
				else
				{
					counters = TaskAttemptImpl.EmptyCounters;
				}
				//        counters.groups = new HashMap<CharSequence, CounterGroup>();
				return counters;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual float GetProgress()
		{
			readLock.Lock();
			try
			{
				TaskAttempt bestAttempt = SelectBestAttempt();
				if (bestAttempt == null)
				{
					return 0f;
				}
				return bestAttempt.GetProgress();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		[VisibleForTesting]
		public virtual TaskStateInternal GetInternalState()
		{
			readLock.Lock();
			try
			{
				return stateMachine.GetCurrentState();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		private static TaskState GetExternalState(TaskStateInternal smState)
		{
			if (smState == TaskStateInternal.KillWait)
			{
				return TaskState.Killed;
			}
			else
			{
				return TaskState.ValueOf(smState.ToString());
			}
		}

		//this is always called in read/write lock
		private long GetLaunchTime()
		{
			long taskLaunchTime = 0;
			bool launchTimeSet = false;
			foreach (TaskAttempt at in attempts.Values)
			{
				// select the least launch time of all attempts
				long attemptLaunchTime = at.GetLaunchTime();
				if (attemptLaunchTime != 0 && !launchTimeSet)
				{
					// For the first non-zero launch time
					launchTimeSet = true;
					taskLaunchTime = attemptLaunchTime;
				}
				else
				{
					if (attemptLaunchTime != 0 && taskLaunchTime > attemptLaunchTime)
					{
						taskLaunchTime = attemptLaunchTime;
					}
				}
			}
			if (!launchTimeSet)
			{
				return this.scheduledTime;
			}
			return taskLaunchTime;
		}

		//this is always called in read/write lock
		//TODO Verify behaviour is Task is killed (no finished attempt)
		private long GetFinishTime()
		{
			if (!IsFinished())
			{
				return 0;
			}
			long finishTime = 0;
			foreach (TaskAttempt at in attempts.Values)
			{
				//select the max finish time of all attempts
				if (finishTime < at.GetFinishTime())
				{
					finishTime = at.GetFinishTime();
				}
			}
			return finishTime;
		}

		private long GetFinishTime(TaskAttemptId taId)
		{
			if (taId == null)
			{
				return clock.GetTime();
			}
			long finishTime = 0;
			foreach (TaskAttempt at in attempts.Values)
			{
				//select the max finish time of all attempts
				if (at.GetID().Equals(taId))
				{
					return at.GetFinishTime();
				}
			}
			return finishTime;
		}

		private TaskStateInternal Finished(TaskStateInternal finalState)
		{
			if (GetInternalState() == TaskStateInternal.Running)
			{
				metrics.EndRunningTask(this);
			}
			return finalState;
		}

		//select the nextAttemptNumber with best progress
		// always called inside the Read Lock
		private TaskAttempt SelectBestAttempt()
		{
			if (successfulAttempt != null)
			{
				return attempts[successfulAttempt];
			}
			float progress = 0f;
			TaskAttempt result = null;
			foreach (TaskAttempt at in attempts.Values)
			{
				switch (at.GetState())
				{
					case TaskAttemptState.Failed:
					case TaskAttemptState.Killed:
					{
						// ignore all failed task attempts
						continue;
					}
				}
				if (result == null)
				{
					result = at;
				}
				//The first time around
				// calculate the best progress
				float attemptProgress = at.GetProgress();
				if (attemptProgress > progress)
				{
					result = at;
					progress = attemptProgress;
				}
			}
			return result;
		}

		public virtual bool CanCommit(TaskAttemptId taskAttemptID)
		{
			readLock.Lock();
			bool canCommit = false;
			try
			{
				if (commitAttempt != null)
				{
					canCommit = taskAttemptID.Equals(commitAttempt);
					Log.Info("Result of canCommit for " + taskAttemptID + ":" + canCommit);
				}
			}
			finally
			{
				readLock.Unlock();
			}
			return canCommit;
		}

		protected internal abstract TaskAttemptImpl CreateAttempt();

		// No override of this method may require that the subclass be initialized.
		protected internal abstract int GetMaxAttempts();

		protected internal virtual TaskAttempt GetSuccessfulAttempt()
		{
			readLock.Lock();
			try
			{
				if (null == successfulAttempt)
				{
					return null;
				}
				return attempts[successfulAttempt];
			}
			finally
			{
				readLock.Unlock();
			}
		}

		// This is always called in the Write Lock
		private void AddAndScheduleAttempt(Avataar avataar)
		{
			TaskAttempt attempt = AddAttempt(avataar);
			inProgressAttempts.AddItem(attempt.GetID());
			//schedule the nextAttemptNumber
			if (failedAttempts.Count > 0)
			{
				eventHandler.Handle(new TaskAttemptEvent(attempt.GetID(), TaskAttemptEventType.TaReschedule
					));
			}
			else
			{
				eventHandler.Handle(new TaskAttemptEvent(attempt.GetID(), TaskAttemptEventType.TaSchedule
					));
			}
		}

		private TaskAttemptImpl AddAttempt(Avataar avataar)
		{
			TaskAttemptImpl attempt = CreateAttempt();
			attempt.SetAvataar(avataar);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Created attempt " + attempt.GetID());
			}
			switch (attempts.Count)
			{
				case 0:
				{
					attempts = Sharpen.Collections.SingletonMap(attempt.GetID(), (TaskAttempt)attempt
						);
					break;
				}

				case 1:
				{
					IDictionary<TaskAttemptId, TaskAttempt> newAttempts = new LinkedHashMap<TaskAttemptId
						, TaskAttempt>(maxAttempts);
					newAttempts.PutAll(attempts);
					attempts = newAttempts;
					attempts[attempt.GetID()] = attempt;
					break;
				}

				default:
				{
					attempts[attempt.GetID()] = attempt;
					break;
				}
			}
			++nextAttemptNumber;
			return attempt;
		}

		public virtual void Handle(TaskEvent @event)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Processing " + @event.GetTaskID() + " of type " + @event.GetType());
			}
			try
			{
				writeLock.Lock();
				TaskStateInternal oldState = GetInternalState();
				try
				{
					stateMachine.DoTransition(@event.GetType(), @event);
				}
				catch (InvalidStateTransitonException e)
				{
					Log.Error("Can't handle this event at current state for " + this.taskId, e);
					InternalError(@event.GetType());
				}
				if (oldState != GetInternalState())
				{
					Log.Info(taskId + " Task Transitioned from " + oldState + " to " + GetInternalState
						());
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		protected internal virtual void InternalError(TaskEventType type)
		{
			Log.Error("Invalid event " + type + " on Task " + this.taskId);
			eventHandler.Handle(new JobDiagnosticsUpdateEvent(this.taskId.GetJobId(), "Invalid event "
				 + type + " on Task " + this.taskId));
			eventHandler.Handle(new JobEvent(this.taskId.GetJobId(), JobEventType.InternalError
				));
		}

		// always called inside a transition, in turn inside the Write Lock
		private void HandleTaskAttemptCompletion(TaskAttemptId attemptId, TaskAttemptCompletionEventStatus
			 status)
		{
			TaskAttempt attempt = attempts[attemptId];
			//raise the completion event only if the container is assigned
			// to nextAttemptNumber
			if (attempt.GetNodeHttpAddress() != null)
			{
				TaskAttemptCompletionEvent tce = recordFactory.NewRecordInstance<TaskAttemptCompletionEvent
					>();
				tce.SetEventId(-1);
				string scheme = (encryptedShuffle) ? "https://" : "http://";
				tce.SetMapOutputServerAddress(StringInterner.WeakIntern(scheme + attempt.GetNodeHttpAddress
					().Split(":")[0] + ":" + attempt.GetShufflePort()));
				tce.SetStatus(status);
				tce.SetAttemptId(attempt.GetID());
				int runTime = 0;
				if (attempt.GetFinishTime() != 0 && attempt.GetLaunchTime() != 0)
				{
					runTime = (int)(attempt.GetFinishTime() - attempt.GetLaunchTime());
				}
				tce.SetAttemptRunTime(runTime);
				//raise the event to job so that it adds the completion event to its
				//data structures
				eventHandler.Handle(new JobTaskAttemptCompletedEvent(tce));
			}
		}

		private void SendTaskStartedEvent()
		{
			TaskStartedEvent tse = new TaskStartedEvent(TypeConverter.FromYarn(taskId), GetLaunchTime
				(), TypeConverter.FromYarn(taskId.GetTaskType()), GetSplitsAsString());
			eventHandler.Handle(new JobHistoryEvent(taskId.GetJobId(), tse));
			historyTaskStartGenerated = true;
		}

		private static TaskFinishedEvent CreateTaskFinishedEvent(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskImpl
			 task, TaskStateInternal taskState)
		{
			TaskFinishedEvent tfe = new TaskFinishedEvent(TypeConverter.FromYarn(task.taskId)
				, TypeConverter.FromYarn(task.successfulAttempt), task.GetFinishTime(task.successfulAttempt
				), TypeConverter.FromYarn(task.taskId.GetTaskType()), taskState.ToString(), task
				.GetCounters());
			return tfe;
		}

		private static TaskFailedEvent CreateTaskFailedEvent(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskImpl
			 task, IList<string> diag, TaskStateInternal taskState, TaskAttemptId taId)
		{
			StringBuilder errorSb = new StringBuilder();
			if (diag != null)
			{
				foreach (string d in diag)
				{
					errorSb.Append(", ").Append(d);
				}
			}
			TaskFailedEvent taskFailedEvent = new TaskFailedEvent(TypeConverter.FromYarn(task
				.taskId), task.GetFinishTime(taId), TypeConverter.FromYarn(task.GetType()), errorSb
				.ToString(), taskState.ToString(), taId == null ? null : TypeConverter.FromYarn(
				taId), task.GetCounters());
			// Hack since getFinishTime needs isFinished to be true and that doesn't happen till after the transition.
			return taskFailedEvent;
		}

		private static void UnSucceed(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskImpl
			 task)
		{
			task.commitAttempt = null;
			task.successfulAttempt = null;
		}

		private void SendTaskSucceededEvents()
		{
			eventHandler.Handle(new JobTaskEvent(taskId, TaskState.Succeeded));
			Log.Info("Task succeeded with attempt " + successfulAttempt);
			if (historyTaskStartGenerated)
			{
				TaskFinishedEvent tfe = CreateTaskFinishedEvent(this, TaskStateInternal.Succeeded
					);
				eventHandler.Handle(new JobHistoryEvent(taskId.GetJobId(), tfe));
			}
		}

		/// <returns>
		/// a String representation of the splits.
		/// Subclasses can override this method to provide their own representations
		/// of splits (if any).
		/// </returns>
		protected internal virtual string GetSplitsAsString()
		{
			return string.Empty;
		}

		/// <summary>Recover a completed task from a previous application attempt</summary>
		/// <param name="taskInfo">recovered info about the task</param>
		/// <param name="recoverTaskOutput">whether to recover task outputs</param>
		/// <returns>state of the task after recovery</returns>
		private TaskStateInternal Recover(JobHistoryParser.TaskInfo taskInfo, OutputCommitter
			 committer, bool recoverTaskOutput)
		{
			Log.Info("Recovering task " + taskId + " from prior app attempt, status was " + taskInfo
				.GetTaskStatus());
			scheduledTime = taskInfo.GetStartTime();
			SendTaskStartedEvent();
			ICollection<JobHistoryParser.TaskAttemptInfo> attemptInfos = taskInfo.GetAllTaskAttempts
				().Values;
			if (attemptInfos.Count > 0)
			{
				metrics.LaunchedTask(this);
			}
			// recover the attempts for this task in the order they finished
			// so task attempt completion events are ordered properly
			int savedNextAttemptNumber = nextAttemptNumber;
			AList<JobHistoryParser.TaskAttemptInfo> taInfos = new AList<JobHistoryParser.TaskAttemptInfo
				>(taskInfo.GetAllTaskAttempts().Values);
			taInfos.Sort(TaInfoComparator);
			foreach (JobHistoryParser.TaskAttemptInfo taInfo in taInfos)
			{
				nextAttemptNumber = taInfo.GetAttemptId().GetId();
				TaskAttemptImpl attempt = AddAttempt(Avataar.Virgin);
				// handle the recovery inline so attempts complete before task does
				attempt.Handle(new TaskAttemptRecoverEvent(attempt.GetID(), taInfo, committer, recoverTaskOutput
					));
				finishedAttempts.AddItem(attempt.GetID());
				TaskAttemptCompletionEventStatus taces = null;
				TaskAttemptState attemptState = attempt.GetState();
				switch (attemptState)
				{
					case TaskAttemptState.Failed:
					{
						taces = TaskAttemptCompletionEventStatus.Failed;
						break;
					}

					case TaskAttemptState.Killed:
					{
						taces = TaskAttemptCompletionEventStatus.Killed;
						break;
					}

					case TaskAttemptState.Succeeded:
					{
						taces = TaskAttemptCompletionEventStatus.Succeeded;
						break;
					}

					default:
					{
						throw new InvalidOperationException("Unexpected attempt state during recovery: " 
							+ attemptState);
					}
				}
				if (attemptState == TaskAttemptState.Failed)
				{
					failedAttempts.AddItem(attempt.GetID());
					if (failedAttempts.Count >= maxAttempts)
					{
						taces = TaskAttemptCompletionEventStatus.Tipfailed;
					}
				}
				// don't clobber the successful attempt completion event
				// TODO: this shouldn't be necessary after MAPREDUCE-4330
				if (successfulAttempt == null)
				{
					HandleTaskAttemptCompletion(attempt.GetID(), taces);
					if (attemptState == TaskAttemptState.Succeeded)
					{
						successfulAttempt = attempt.GetID();
					}
				}
			}
			nextAttemptNumber = savedNextAttemptNumber;
			TaskStateInternal taskState = TaskStateInternal.ValueOf(taskInfo.GetTaskStatus());
			switch (taskState)
			{
				case TaskStateInternal.Succeeded:
				{
					if (successfulAttempt != null)
					{
						SendTaskSucceededEvents();
					}
					else
					{
						Log.Info("Missing successful attempt for task " + taskId + ", recovering as RUNNING"
							);
						// there must have been a fetch failure and the retry wasn't complete
						taskState = TaskStateInternal.Running;
						metrics.RunningTask(this);
						AddAndScheduleAttempt(Avataar.Virgin);
					}
					break;
				}

				case TaskStateInternal.Failed:
				case TaskStateInternal.Killed:
				{
					if (taskState == TaskStateInternal.Killed && attemptInfos.Count == 0)
					{
						metrics.EndWaitingTask(this);
					}
					TaskFailedEvent tfe = new TaskFailedEvent(taskInfo.GetTaskId(), taskInfo.GetFinishTime
						(), taskInfo.GetTaskType(), taskInfo.GetError(), taskInfo.GetTaskStatus(), taskInfo
						.GetFailedDueToAttemptId(), taskInfo.GetCounters());
					eventHandler.Handle(new JobHistoryEvent(taskId.GetJobId(), tfe));
					eventHandler.Handle(new JobTaskEvent(taskId, GetExternalState(taskState)));
					break;
				}

				default:
				{
					throw new Exception("Unexpected recovered task state: " + taskState);
				}
			}
			return taskState;
		}

		private class RecoverTransition : MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal
			>
		{
			public virtual TaskStateInternal Transition(TaskImpl task, TaskEvent @event)
			{
				TaskRecoverEvent tre = (TaskRecoverEvent)@event;
				return task.Recover(tre.GetTaskInfo(), tre.GetOutputCommitter(), tre.GetRecoverTaskOutput
					());
			}
		}

		private class InitialScheduleTransition : SingleArcTransition<TaskImpl, TaskEvent
			>
		{
			public virtual void Transition(TaskImpl task, TaskEvent @event)
			{
				task.AddAndScheduleAttempt(Avataar.Virgin);
				task.scheduledTime = task.clock.GetTime();
				task.SendTaskStartedEvent();
			}
		}

		private class RedundantScheduleTransition : SingleArcTransition<TaskImpl, TaskEvent
			>
		{
			// Used when creating a new attempt while one is already running.
			//  Currently we do this for speculation.  In the future we may do this
			//  for tasks that failed in a way that might indicate application code
			//  problems, so we can take later failures in parallel and flush the
			//  job quickly when this happens.
			public virtual void Transition(TaskImpl task, TaskEvent @event)
			{
				Log.Info("Scheduling a redundant attempt for task " + task.taskId);
				task.AddAndScheduleAttempt(Avataar.Speculative);
			}
		}

		private class AttemptCommitPendingTransition : SingleArcTransition<TaskImpl, TaskEvent
			>
		{
			public virtual void Transition(TaskImpl task, TaskEvent @event)
			{
				TaskTAttemptEvent ev = (TaskTAttemptEvent)@event;
				// The nextAttemptNumber is commit pending, decide on set the commitAttempt
				TaskAttemptId attemptID = ev.GetTaskAttemptID();
				if (task.commitAttempt == null)
				{
					// TODO: validate attemptID
					task.commitAttempt = attemptID;
					Log.Info(attemptID + " given a go for committing the task output.");
				}
				else
				{
					// Don't think this can be a pluggable decision, so simply raise an
					// event for the TaskAttempt to delete its output.
					Log.Info(task.commitAttempt + " already given a go for committing the task output, so killing "
						 + attemptID);
					task.eventHandler.Handle(new TaskAttemptKillEvent(attemptID, Speculation + task.commitAttempt
						 + " committed first!"));
				}
			}
		}

		private class AttemptSucceededTransition : SingleArcTransition<TaskImpl, TaskEvent
			>
		{
			public virtual void Transition(TaskImpl task, TaskEvent @event)
			{
				TaskTAttemptEvent taskTAttemptEvent = (TaskTAttemptEvent)@event;
				TaskAttemptId taskAttemptId = taskTAttemptEvent.GetTaskAttemptID();
				task.HandleTaskAttemptCompletion(taskAttemptId, TaskAttemptCompletionEventStatus.
					Succeeded);
				task.finishedAttempts.AddItem(taskAttemptId);
				task.inProgressAttempts.Remove(taskAttemptId);
				task.successfulAttempt = taskAttemptId;
				task.SendTaskSucceededEvents();
				foreach (TaskAttempt attempt in task.attempts.Values)
				{
					if (attempt.GetID() != task.successfulAttempt && !attempt.IsFinished())
					{
						// This is okay because it can only talk us out of sending a
						//  TA_KILL message to an attempt that doesn't need one for
						//  other reasons.
						Log.Info("Issuing kill to other attempt " + attempt.GetID());
						task.eventHandler.Handle(new TaskAttemptKillEvent(attempt.GetID(), Speculation + 
							task.successfulAttempt + " succeeded first!"));
					}
				}
				task.Finished(TaskStateInternal.Succeeded);
			}
		}

		private class AttemptKilledTransition : SingleArcTransition<TaskImpl, TaskEvent>
		{
			public virtual void Transition(TaskImpl task, TaskEvent @event)
			{
				TaskAttemptId taskAttemptId = ((TaskTAttemptEvent)@event).GetTaskAttemptID();
				task.HandleTaskAttemptCompletion(taskAttemptId, TaskAttemptCompletionEventStatus.
					Killed);
				task.finishedAttempts.AddItem(taskAttemptId);
				task.inProgressAttempts.Remove(taskAttemptId);
				if (task.successfulAttempt == null)
				{
					task.AddAndScheduleAttempt(Avataar.Virgin);
				}
				if ((task.commitAttempt != null) && (task.commitAttempt == taskAttemptId))
				{
					task.commitAttempt = null;
				}
			}
		}

		private class KillWaitAttemptKilledTransition : MultipleArcTransition<TaskImpl, TaskEvent
			, TaskStateInternal>
		{
			protected internal TaskStateInternal finalState = TaskStateInternal.Killed;

			protected internal readonly TaskAttemptCompletionEventStatus taCompletionEventStatus;

			public KillWaitAttemptKilledTransition()
				: this(TaskAttemptCompletionEventStatus.Killed)
			{
			}

			public KillWaitAttemptKilledTransition(TaskAttemptCompletionEventStatus taCompletionEventStatus
				)
			{
				this.taCompletionEventStatus = taCompletionEventStatus;
			}

			public virtual TaskStateInternal Transition(TaskImpl task, TaskEvent @event)
			{
				TaskAttemptId taskAttemptId = ((TaskTAttemptEvent)@event).GetTaskAttemptID();
				task.HandleTaskAttemptCompletion(taskAttemptId, taCompletionEventStatus);
				task.finishedAttempts.AddItem(taskAttemptId);
				// check whether all attempts are finished
				if (task.finishedAttempts.Count == task.attempts.Count)
				{
					if (task.historyTaskStartGenerated)
					{
						TaskFailedEvent taskFailedEvent = CreateTaskFailedEvent(task, null, finalState, null
							);
						// TODO JH verify failedAttempt null
						task.eventHandler.Handle(new JobHistoryEvent(task.taskId.GetJobId(), taskFailedEvent
							));
					}
					else
					{
						Log.Debug("Not generating HistoryFinish event since start event not" + " generated for task: "
							 + task.GetID());
					}
					task.eventHandler.Handle(new JobTaskEvent(task.taskId, GetExternalState(finalState
						)));
					return finalState;
				}
				return task.GetInternalState();
			}
		}

		private class KillWaitAttemptSucceededTransition : TaskImpl.KillWaitAttemptKilledTransition
		{
			public KillWaitAttemptSucceededTransition()
				: base(TaskAttemptCompletionEventStatus.Succeeded)
			{
			}
		}

		private class KillWaitAttemptFailedTransition : TaskImpl.KillWaitAttemptKilledTransition
		{
			public KillWaitAttemptFailedTransition()
				: base(TaskAttemptCompletionEventStatus.Failed)
			{
			}
		}

		private class AttemptFailedTransition : MultipleArcTransition<TaskImpl, TaskEvent
			, TaskStateInternal>
		{
			public virtual TaskStateInternal Transition(TaskImpl task, TaskEvent @event)
			{
				TaskTAttemptEvent castEvent = (TaskTAttemptEvent)@event;
				TaskAttemptId taskAttemptId = castEvent.GetTaskAttemptID();
				task.failedAttempts.AddItem(taskAttemptId);
				if (taskAttemptId.Equals(task.commitAttempt))
				{
					task.commitAttempt = null;
				}
				TaskAttempt attempt = task.attempts[taskAttemptId];
				if (attempt.GetAssignedContainerMgrAddress() != null)
				{
					//container was assigned
					task.eventHandler.Handle(new ContainerFailedEvent(attempt.GetID(), attempt.GetAssignedContainerMgrAddress
						()));
				}
				task.finishedAttempts.AddItem(taskAttemptId);
				if (task.failedAttempts.Count < task.maxAttempts)
				{
					task.HandleTaskAttemptCompletion(taskAttemptId, TaskAttemptCompletionEventStatus.
						Failed);
					// we don't need a new event if we already have a spare
					task.inProgressAttempts.Remove(taskAttemptId);
					if (task.inProgressAttempts.Count == 0 && task.successfulAttempt == null)
					{
						task.AddAndScheduleAttempt(Avataar.Virgin);
					}
				}
				else
				{
					task.HandleTaskAttemptCompletion(taskAttemptId, TaskAttemptCompletionEventStatus.
						Tipfailed);
					// issue kill to all non finished attempts
					foreach (TaskAttempt taskAttempt in task.attempts.Values)
					{
						task.KillUnfinishedAttempt(taskAttempt, "Task has failed. Killing attempt!");
					}
					task.inProgressAttempts.Clear();
					if (task.historyTaskStartGenerated)
					{
						TaskFailedEvent taskFailedEvent = CreateTaskFailedEvent(task, attempt.GetDiagnostics
							(), TaskStateInternal.Failed, taskAttemptId);
						task.eventHandler.Handle(new JobHistoryEvent(task.taskId.GetJobId(), taskFailedEvent
							));
					}
					else
					{
						Log.Debug("Not generating HistoryFinish event since start event not" + " generated for task: "
							 + task.GetID());
					}
					task.eventHandler.Handle(new JobTaskEvent(task.taskId, TaskState.Failed));
					return task.Finished(TaskStateInternal.Failed);
				}
				return GetDefaultState(task);
			}

			protected internal virtual TaskStateInternal GetDefaultState(TaskImpl task)
			{
				return task.GetInternalState();
			}
		}

		private class RetroactiveFailureTransition : TaskImpl.AttemptFailedTransition
		{
			public override TaskStateInternal Transition(TaskImpl task, TaskEvent @event)
			{
				TaskTAttemptEvent castEvent = (TaskTAttemptEvent)@event;
				if (task.GetInternalState() == TaskStateInternal.Succeeded && !castEvent.GetTaskAttemptID
					().Equals(task.successfulAttempt))
				{
					// don't allow a different task attempt to override a previous
					// succeeded state
					task.finishedAttempts.AddItem(castEvent.GetTaskAttemptID());
					task.inProgressAttempts.Remove(castEvent.GetTaskAttemptID());
					return TaskStateInternal.Succeeded;
				}
				// a successful REDUCE task should not be overridden
				//TODO: consider moving it to MapTaskImpl
				if (!TaskType.Map.Equals(task.GetType()))
				{
					Log.Error("Unexpected event for REDUCE task " + @event.GetType());
					task.InternalError(@event.GetType());
				}
				// tell the job about the rescheduling
				task.eventHandler.Handle(new JobMapTaskRescheduledEvent(task.taskId));
				// super.transition is mostly coded for the case where an
				//  UNcompleted task failed.  When a COMPLETED task retroactively
				//  fails, we have to let AttemptFailedTransition.transition
				//  believe that there's no redundancy.
				UnSucceed(task);
				// fake increase in Uncomplete attempts for super.transition
				task.inProgressAttempts.AddItem(castEvent.GetTaskAttemptID());
				return base.Transition(task, @event);
			}

			protected internal override TaskStateInternal GetDefaultState(TaskImpl task)
			{
				return TaskStateInternal.Scheduled;
			}
		}

		private class RetroactiveKilledTransition : MultipleArcTransition<TaskImpl, TaskEvent
			, TaskStateInternal>
		{
			public virtual TaskStateInternal Transition(TaskImpl task, TaskEvent @event)
			{
				TaskAttemptId attemptId = null;
				if (@event is TaskTAttemptEvent)
				{
					TaskTAttemptEvent castEvent = (TaskTAttemptEvent)@event;
					attemptId = castEvent.GetTaskAttemptID();
					if (task.GetInternalState() == TaskStateInternal.Succeeded && !attemptId.Equals(task
						.successfulAttempt))
					{
						// don't allow a different task attempt to override a previous
						// succeeded state
						task.finishedAttempts.AddItem(castEvent.GetTaskAttemptID());
						task.inProgressAttempts.Remove(castEvent.GetTaskAttemptID());
						return TaskStateInternal.Succeeded;
					}
				}
				// a successful REDUCE task should not be overridden
				// TODO: consider moving it to MapTaskImpl
				if (!TaskType.Map.Equals(task.GetType()))
				{
					Log.Error("Unexpected event for REDUCE task " + @event.GetType());
					task.InternalError(@event.GetType());
				}
				// successful attempt is now killed. reschedule
				// tell the job about the rescheduling
				UnSucceed(task);
				task.HandleTaskAttemptCompletion(attemptId, TaskAttemptCompletionEventStatus.Killed
					);
				task.eventHandler.Handle(new JobMapTaskRescheduledEvent(task.taskId));
				// typically we are here because this map task was run on a bad node and
				// we want to reschedule it on a different node.
				// Depending on whether there are previous failed attempts or not this
				// can SCHEDULE or RESCHEDULE the container allocate request. If this
				// SCHEDULE's then the dataLocal hosts of this taskAttempt will be used
				// from the map splitInfo. So the bad node might be sent as a location
				// to the RM. But the RM would ignore that just like it would ignore
				// currently pending container requests affinitized to bad nodes.
				task.AddAndScheduleAttempt(Avataar.Virgin);
				return TaskStateInternal.Scheduled;
			}
		}

		private class AttemptSucceededAtSucceededTransition : SingleArcTransition<TaskImpl
			, TaskEvent>
		{
			public virtual void Transition(TaskImpl task, TaskEvent @event)
			{
				TaskTAttemptEvent castEvent = (TaskTAttemptEvent)@event;
				task.finishedAttempts.AddItem(castEvent.GetTaskAttemptID());
				task.inProgressAttempts.Remove(castEvent.GetTaskAttemptID());
			}
		}

		private class KillNewTransition : SingleArcTransition<TaskImpl, TaskEvent>
		{
			public virtual void Transition(TaskImpl task, TaskEvent @event)
			{
				if (task.historyTaskStartGenerated)
				{
					TaskFailedEvent taskFailedEvent = CreateTaskFailedEvent(task, null, TaskStateInternal
						.Killed, null);
					// TODO Verify failedAttemptId is null
					task.eventHandler.Handle(new JobHistoryEvent(task.taskId.GetJobId(), taskFailedEvent
						));
				}
				else
				{
					Log.Debug("Not generating HistoryFinish event since start event not" + " generated for task: "
						 + task.GetID());
				}
				task.eventHandler.Handle(new JobTaskEvent(task.taskId, GetExternalState(TaskStateInternal
					.Killed)));
				task.metrics.EndWaitingTask(task);
			}
		}

		private void KillUnfinishedAttempt(TaskAttempt attempt, string logMsg)
		{
			if (attempt != null && !attempt.IsFinished())
			{
				eventHandler.Handle(new TaskAttemptKillEvent(attempt.GetID(), logMsg));
			}
		}

		private class KillTransition : SingleArcTransition<TaskImpl, TaskEvent>
		{
			public virtual void Transition(TaskImpl task, TaskEvent @event)
			{
				// issue kill to all non finished attempts
				foreach (TaskAttempt attempt in task.attempts.Values)
				{
					task.KillUnfinishedAttempt(attempt, "Task KILL is received. Killing attempt!");
				}
				task.inProgressAttempts.Clear();
			}
		}

		internal class LaunchTransition : SingleArcTransition<TaskImpl, TaskEvent>
		{
			public virtual void Transition(TaskImpl task, TaskEvent @event)
			{
				task.metrics.LaunchedTask(task);
				task.metrics.RunningTask(task);
			}
		}

		public abstract TaskType GetType();
	}
}
