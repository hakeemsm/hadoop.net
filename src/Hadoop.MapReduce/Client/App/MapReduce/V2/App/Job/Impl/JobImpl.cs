using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.Lib.Chain;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Commit;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Metrics;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.State;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	/// <summary>Implementation of Job interface.</summary>
	/// <remarks>
	/// Implementation of Job interface. Maintains the state machines of Job.
	/// The read and write calls use ReadWriteLock for concurrency.
	/// </remarks>
	public class JobImpl : Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job, EventHandler<JobEvent
		>
	{
		private static readonly TaskAttemptCompletionEvent[] EmptyTaskAttemptCompletionEvents
			 = new TaskAttemptCompletionEvent[0];

		private static readonly TaskCompletionEvent[] EmptyTaskCompletionEvents = new TaskCompletionEvent
			[0];

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.JobImpl
			));

		private float maxAllowedFetchFailuresFraction;

		private int maxFetchFailuresNotifications;

		public const string JobKilledDiag = "Job received Kill while in RUNNING state.";

		private readonly ApplicationAttemptId applicationAttemptId;

		private readonly Clock clock;

		private readonly JobACLsManager aclsManager;

		private readonly string username;

		private readonly IDictionary<JobACL, AccessControlList> jobACLs;

		private float setupWeight = 0.05f;

		private float cleanupWeight = 0.05f;

		private float mapWeight = 0.0f;

		private float reduceWeight = 0.0f;

		private readonly IDictionary<TaskId, JobHistoryParser.TaskInfo> completedTasksFromPreviousRun;

		private readonly IList<AMInfo> amInfos;

		private readonly Lock readLock;

		private readonly Lock writeLock;

		private readonly JobId jobId;

		private readonly string jobName;

		private readonly OutputCommitter committer;

		private readonly bool newApiCommitter;

		private readonly JobID oldJobId;

		private readonly TaskAttemptListener taskAttemptListener;

		private readonly object tasksSyncHandle = new object();

		private readonly ICollection<TaskId> mapTasks = new LinkedHashSet<TaskId>();

		private readonly ICollection<TaskId> reduceTasks = new LinkedHashSet<TaskId>();

		/// <summary>maps nodes to tasks that have run on those nodes</summary>
		private readonly Dictionary<NodeId, IList<TaskAttemptId>> nodesToSucceededTaskAttempts
			 = new Dictionary<NodeId, IList<TaskAttemptId>>();

		private readonly EventHandler eventHandler;

		private readonly MRAppMetrics metrics;

		private readonly string userName;

		private string queueName;

		private readonly long appSubmitTime;

		private readonly AppContext appContext;

		private bool lazyTasksCopyNeeded = false;

		internal volatile IDictionary<TaskId, Task> tasks = new LinkedHashMap<TaskId, Task
			>();

		private Counters jobCounters = new Counters();

		private object fullCountersLock = new object();

		private Counters fullCounters = null;

		private Counters finalMapCounters = null;

		private Counters finalReduceCounters = null;

		public JobConf conf;

		private FileSystem fs;

		private Path remoteJobSubmitDir;

		public Path remoteJobConfFile;

		private JobContext jobContext;

		private int allowedMapFailuresPercent = 0;

		private int allowedReduceFailuresPercent = 0;

		private IList<TaskAttemptCompletionEvent> taskAttemptCompletionEvents;

		private IList<TaskCompletionEvent> mapAttemptCompletionEvents;

		private IList<int> taskCompletionIdxToMapCompletionIdx;

		private readonly IList<string> diagnostics = new AList<string>();

		private readonly IDictionary<TaskId, int> successAttemptCompletionEventNoMap = new 
			Dictionary<TaskId, int>();

		private readonly IDictionary<TaskAttemptId, int> fetchFailuresMapping = new Dictionary
			<TaskAttemptId, int>();

		private static readonly JobImpl.DiagnosticsUpdateTransition DiagnosticUpdateTransition
			 = new JobImpl.DiagnosticsUpdateTransition();

		private static readonly JobImpl.InternalErrorTransition InternalErrorTransition = 
			new JobImpl.InternalErrorTransition();

		private static readonly JobImpl.InternalRebootTransition InternalRebootTransition
			 = new JobImpl.InternalRebootTransition();

		private static readonly JobImpl.TaskAttemptCompletedEventTransition TaskAttemptCompletedEventTransition
			 = new JobImpl.TaskAttemptCompletedEventTransition();

		private static readonly JobImpl.CounterUpdateTransition CounterUpdateTransition = 
			new JobImpl.CounterUpdateTransition();

		private static readonly JobImpl.UpdatedNodesTransition UpdatedNodesTransition = new 
			JobImpl.UpdatedNodesTransition();

		protected internal static readonly StateMachineFactory<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.JobImpl
			, JobStateInternal, JobEventType, JobEvent> stateMachineFactory = new StateMachineFactory
			<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.JobImpl, JobStateInternal, JobEventType
			, JobEvent>(JobStateInternal.New).AddTransition(JobStateInternal.New, JobStateInternal
			.New, JobEventType.JobDiagnosticUpdate, DiagnosticUpdateTransition).AddTransition
			(JobStateInternal.New, JobStateInternal.New, JobEventType.JobCounterUpdate, CounterUpdateTransition
			).AddTransition(JobStateInternal.New, EnumSet.Of(JobStateInternal.Inited, JobStateInternal
			.New), JobEventType.JobInit, new JobImpl.InitTransition()).AddTransition(JobStateInternal
			.New, JobStateInternal.FailAbort, JobEventType.JobInitFailed, new JobImpl.InitFailedTransition
			()).AddTransition(JobStateInternal.New, JobStateInternal.Killed, JobEventType.JobKill
			, new JobImpl.KillNewJobTransition()).AddTransition(JobStateInternal.New, JobStateInternal
			.Error, JobEventType.InternalError, InternalErrorTransition).AddTransition(JobStateInternal
			.New, JobStateInternal.Reboot, JobEventType.JobAmReboot, InternalRebootTransition
			).AddTransition(JobStateInternal.New, JobStateInternal.New, JobEventType.JobUpdatedNodes
			).AddTransition(JobStateInternal.Inited, JobStateInternal.Inited, JobEventType.JobDiagnosticUpdate
			, DiagnosticUpdateTransition).AddTransition(JobStateInternal.Inited, JobStateInternal
			.Inited, JobEventType.JobCounterUpdate, CounterUpdateTransition).AddTransition(JobStateInternal
			.Inited, JobStateInternal.Setup, JobEventType.JobStart, new JobImpl.StartTransition
			()).AddTransition(JobStateInternal.Inited, JobStateInternal.Killed, JobEventType
			.JobKill, new JobImpl.KillInitedJobTransition()).AddTransition(JobStateInternal.
			Inited, JobStateInternal.Error, JobEventType.InternalError, InternalErrorTransition
			).AddTransition(JobStateInternal.Inited, JobStateInternal.Reboot, JobEventType.JobAmReboot
			, InternalRebootTransition).AddTransition(JobStateInternal.Inited, JobStateInternal
			.Inited, JobEventType.JobUpdatedNodes).AddTransition(JobStateInternal.Setup, JobStateInternal
			.Setup, JobEventType.JobDiagnosticUpdate, DiagnosticUpdateTransition).AddTransition
			(JobStateInternal.Setup, JobStateInternal.Setup, JobEventType.JobCounterUpdate, 
			CounterUpdateTransition).AddTransition(JobStateInternal.Setup, JobStateInternal.
			Running, JobEventType.JobSetupCompleted, new JobImpl.SetupCompletedTransition())
			.AddTransition(JobStateInternal.Setup, JobStateInternal.FailAbort, JobEventType.
			JobSetupFailed, new JobImpl.SetupFailedTransition()).AddTransition(JobStateInternal
			.Setup, JobStateInternal.KillAbort, JobEventType.JobKill, new JobImpl.KilledDuringSetupTransition
			()).AddTransition(JobStateInternal.Setup, JobStateInternal.Error, JobEventType.InternalError
			, InternalErrorTransition).AddTransition(JobStateInternal.Setup, JobStateInternal
			.Reboot, JobEventType.JobAmReboot, InternalRebootTransition).AddTransition(JobStateInternal
			.Setup, JobStateInternal.Setup, JobEventType.JobUpdatedNodes).AddTransition(JobStateInternal
			.Running, JobStateInternal.Running, JobEventType.JobTaskAttemptCompleted, TaskAttemptCompletedEventTransition
			).AddTransition(JobStateInternal.Running, EnumSet.Of(JobStateInternal.Running, JobStateInternal
			.Committing, JobStateInternal.FailWait, JobStateInternal.FailAbort), JobEventType
			.JobTaskCompleted, new JobImpl.TaskCompletedTransition()).AddTransition(JobStateInternal
			.Running, EnumSet.Of(JobStateInternal.Running, JobStateInternal.Committing), JobEventType
			.JobCompleted, new JobImpl.JobNoTasksCompletedTransition()).AddTransition(JobStateInternal
			.Running, JobStateInternal.KillWait, JobEventType.JobKill, new JobImpl.KillTasksTransition
			()).AddTransition(JobStateInternal.Running, JobStateInternal.Running, JobEventType
			.JobUpdatedNodes, UpdatedNodesTransition).AddTransition(JobStateInternal.Running
			, JobStateInternal.Running, JobEventType.JobMapTaskRescheduled, new JobImpl.MapTaskRescheduledTransition
			()).AddTransition(JobStateInternal.Running, JobStateInternal.Running, JobEventType
			.JobDiagnosticUpdate, DiagnosticUpdateTransition).AddTransition(JobStateInternal
			.Running, JobStateInternal.Running, JobEventType.JobCounterUpdate, CounterUpdateTransition
			).AddTransition(JobStateInternal.Running, JobStateInternal.Running, JobEventType
			.JobTaskAttemptFetchFailure, new JobImpl.TaskAttemptFetchFailureTransition()).AddTransition
			(JobStateInternal.Running, JobStateInternal.Error, JobEventType.InternalError, InternalErrorTransition
			).AddTransition(JobStateInternal.Running, JobStateInternal.Reboot, JobEventType.
			JobAmReboot, InternalRebootTransition).AddTransition(JobStateInternal.KillWait, 
			EnumSet.Of(JobStateInternal.KillWait, JobStateInternal.KillAbort), JobEventType.
			JobTaskCompleted, new JobImpl.KillWaitTaskCompletedTransition()).AddTransition(JobStateInternal
			.KillWait, JobStateInternal.KillWait, JobEventType.JobTaskAttemptCompleted, TaskAttemptCompletedEventTransition
			).AddTransition(JobStateInternal.KillWait, JobStateInternal.KillWait, JobEventType
			.JobDiagnosticUpdate, DiagnosticUpdateTransition).AddTransition(JobStateInternal
			.KillWait, JobStateInternal.KillWait, JobEventType.JobCounterUpdate, CounterUpdateTransition
			).AddTransition(JobStateInternal.KillWait, JobStateInternal.Error, JobEventType.
			InternalError, InternalErrorTransition).AddTransition(JobStateInternal.KillWait, 
			JobStateInternal.KillWait, EnumSet.Of(JobEventType.JobKill, JobEventType.JobUpdatedNodes
			, JobEventType.JobMapTaskRescheduled, JobEventType.JobTaskAttemptFetchFailure, JobEventType
			.JobAmReboot)).AddTransition(JobStateInternal.Committing, JobStateInternal.Succeeded
			, JobEventType.JobCommitCompleted, new JobImpl.CommitSucceededTransition()).AddTransition
			(JobStateInternal.Committing, JobStateInternal.FailAbort, JobEventType.JobCommitFailed
			, new JobImpl.CommitFailedTransition()).AddTransition(JobStateInternal.Committing
			, JobStateInternal.KillAbort, JobEventType.JobKill, new JobImpl.KilledDuringCommitTransition
			()).AddTransition(JobStateInternal.Committing, JobStateInternal.Committing, JobEventType
			.JobDiagnosticUpdate, DiagnosticUpdateTransition).AddTransition(JobStateInternal
			.Committing, JobStateInternal.Committing, JobEventType.JobCounterUpdate, CounterUpdateTransition
			).AddTransition(JobStateInternal.Committing, JobStateInternal.Error, JobEventType
			.InternalError, InternalErrorTransition).AddTransition(JobStateInternal.Committing
			, JobStateInternal.Reboot, JobEventType.JobAmReboot, InternalRebootTransition).AddTransition
			(JobStateInternal.Committing, JobStateInternal.Committing, EnumSet.Of(JobEventType
			.JobUpdatedNodes, JobEventType.JobTaskAttemptFetchFailure)).AddTransition(JobStateInternal
			.Succeeded, JobStateInternal.Succeeded, JobEventType.JobDiagnosticUpdate, DiagnosticUpdateTransition
			).AddTransition(JobStateInternal.Succeeded, JobStateInternal.Succeeded, JobEventType
			.JobCounterUpdate, CounterUpdateTransition).AddTransition(JobStateInternal.Succeeded
			, JobStateInternal.Error, JobEventType.InternalError, InternalErrorTransition).AddTransition
			(JobStateInternal.Succeeded, JobStateInternal.Succeeded, EnumSet.Of(JobEventType
			.JobKill, JobEventType.JobUpdatedNodes, JobEventType.JobTaskAttemptFetchFailure, 
			JobEventType.JobAmReboot, JobEventType.JobTaskAttemptCompleted, JobEventType.JobMapTaskRescheduled
			)).AddTransition(JobStateInternal.FailWait, JobStateInternal.FailWait, JobEventType
			.JobDiagnosticUpdate, DiagnosticUpdateTransition).AddTransition(JobStateInternal
			.FailWait, JobStateInternal.FailWait, JobEventType.JobCounterUpdate, CounterUpdateTransition
			).AddTransition(JobStateInternal.FailWait, EnumSet.Of(JobStateInternal.FailWait, 
			JobStateInternal.FailAbort), JobEventType.JobTaskCompleted, new JobImpl.JobFailWaitTransition
			()).AddTransition(JobStateInternal.FailWait, JobStateInternal.FailAbort, JobEventType
			.JobFailWaitTimedout, new JobImpl.JobFailWaitTimedOutTransition()).AddTransition
			(JobStateInternal.FailWait, JobStateInternal.Killed, JobEventType.JobKill, new JobImpl.KilledDuringAbortTransition
			()).AddTransition(JobStateInternal.FailWait, JobStateInternal.Error, JobEventType
			.InternalError, InternalErrorTransition).AddTransition(JobStateInternal.FailWait
			, JobStateInternal.FailWait, EnumSet.Of(JobEventType.JobUpdatedNodes, JobEventType
			.JobTaskAttemptCompleted, JobEventType.JobMapTaskRescheduled, JobEventType.JobTaskAttemptFetchFailure
			, JobEventType.JobAmReboot)).AddTransition(JobStateInternal.FailAbort, JobStateInternal
			.FailAbort, JobEventType.JobDiagnosticUpdate, DiagnosticUpdateTransition).AddTransition
			(JobStateInternal.FailAbort, JobStateInternal.FailAbort, JobEventType.JobCounterUpdate
			, CounterUpdateTransition).AddTransition(JobStateInternal.FailAbort, JobStateInternal
			.Failed, JobEventType.JobAbortCompleted, new JobImpl.JobAbortCompletedTransition
			()).AddTransition(JobStateInternal.FailAbort, JobStateInternal.Killed, JobEventType
			.JobKill, new JobImpl.KilledDuringAbortTransition()).AddTransition(JobStateInternal
			.FailAbort, JobStateInternal.Error, JobEventType.InternalError, InternalErrorTransition
			).AddTransition(JobStateInternal.FailAbort, JobStateInternal.FailAbort, EnumSet.
			Of(JobEventType.JobUpdatedNodes, JobEventType.JobTaskCompleted, JobEventType.JobTaskAttemptCompleted
			, JobEventType.JobMapTaskRescheduled, JobEventType.JobTaskAttemptFetchFailure, JobEventType
			.JobCommitCompleted, JobEventType.JobCommitFailed, JobEventType.JobAmReboot, JobEventType
			.JobFailWaitTimedout)).AddTransition(JobStateInternal.KillAbort, JobStateInternal
			.KillAbort, JobEventType.JobDiagnosticUpdate, DiagnosticUpdateTransition).AddTransition
			(JobStateInternal.KillAbort, JobStateInternal.KillAbort, JobEventType.JobCounterUpdate
			, CounterUpdateTransition).AddTransition(JobStateInternal.KillAbort, JobStateInternal
			.Killed, JobEventType.JobAbortCompleted, new JobImpl.JobAbortCompletedTransition
			()).AddTransition(JobStateInternal.KillAbort, JobStateInternal.Killed, JobEventType
			.JobKill, new JobImpl.KilledDuringAbortTransition()).AddTransition(JobStateInternal
			.KillAbort, JobStateInternal.Error, JobEventType.InternalError, InternalErrorTransition
			).AddTransition(JobStateInternal.KillAbort, JobStateInternal.KillAbort, EnumSet.
			Of(JobEventType.JobUpdatedNodes, JobEventType.JobTaskAttemptFetchFailure, JobEventType
			.JobSetupCompleted, JobEventType.JobSetupFailed, JobEventType.JobCommitCompleted
			, JobEventType.JobCommitFailed, JobEventType.JobAmReboot)).AddTransition(JobStateInternal
			.Failed, JobStateInternal.Failed, JobEventType.JobDiagnosticUpdate, DiagnosticUpdateTransition
			).AddTransition(JobStateInternal.Failed, JobStateInternal.Failed, JobEventType.JobCounterUpdate
			, CounterUpdateTransition).AddTransition(JobStateInternal.Failed, JobStateInternal
			.Error, JobEventType.InternalError, InternalErrorTransition).AddTransition(JobStateInternal
			.Failed, JobStateInternal.Failed, EnumSet.Of(JobEventType.JobKill, JobEventType.
			JobUpdatedNodes, JobEventType.JobTaskCompleted, JobEventType.JobTaskAttemptCompleted
			, JobEventType.JobMapTaskRescheduled, JobEventType.JobTaskAttemptFetchFailure, JobEventType
			.JobSetupCompleted, JobEventType.JobSetupFailed, JobEventType.JobCommitCompleted
			, JobEventType.JobCommitFailed, JobEventType.JobAbortCompleted, JobEventType.JobAmReboot
			)).AddTransition(JobStateInternal.Killed, JobStateInternal.Killed, JobEventType.
			JobDiagnosticUpdate, DiagnosticUpdateTransition).AddTransition(JobStateInternal.
			Killed, JobStateInternal.Killed, JobEventType.JobCounterUpdate, CounterUpdateTransition
			).AddTransition(JobStateInternal.Killed, JobStateInternal.Error, JobEventType.InternalError
			, InternalErrorTransition).AddTransition(JobStateInternal.Killed, JobStateInternal
			.Killed, EnumSet.Of(JobEventType.JobKill, JobEventType.JobStart, JobEventType.JobUpdatedNodes
			, JobEventType.JobTaskAttemptFetchFailure, JobEventType.JobSetupCompleted, JobEventType
			.JobSetupFailed, JobEventType.JobCommitCompleted, JobEventType.JobCommitFailed, 
			JobEventType.JobAbortCompleted, JobEventType.JobAmReboot)).AddTransition(JobStateInternal
			.Error, JobStateInternal.Error, EnumSet.Of(JobEventType.JobInit, JobEventType.JobKill
			, JobEventType.JobTaskCompleted, JobEventType.JobTaskAttemptCompleted, JobEventType
			.JobMapTaskRescheduled, JobEventType.JobDiagnosticUpdate, JobEventType.JobUpdatedNodes
			, JobEventType.JobTaskAttemptFetchFailure, JobEventType.JobSetupCompleted, JobEventType
			.JobSetupFailed, JobEventType.JobCommitCompleted, JobEventType.JobCommitFailed, 
			JobEventType.JobAbortCompleted, JobEventType.InternalError, JobEventType.JobAmReboot
			)).AddTransition(JobStateInternal.Error, JobStateInternal.Error, JobEventType.JobCounterUpdate
			, CounterUpdateTransition).AddTransition(JobStateInternal.Reboot, JobStateInternal
			.Reboot, EnumSet.Of(JobEventType.JobInit, JobEventType.JobKill, JobEventType.JobTaskCompleted
			, JobEventType.JobTaskAttemptCompleted, JobEventType.JobMapTaskRescheduled, JobEventType
			.JobDiagnosticUpdate, JobEventType.JobUpdatedNodes, JobEventType.JobTaskAttemptFetchFailure
			, JobEventType.JobSetupCompleted, JobEventType.JobSetupFailed, JobEventType.JobCommitCompleted
			, JobEventType.JobCommitFailed, JobEventType.JobAbortCompleted, JobEventType.InternalError
			, JobEventType.JobAmReboot)).AddTransition(JobStateInternal.Reboot, JobStateInternal
			.Reboot, JobEventType.JobCounterUpdate, CounterUpdateTransition).InstallTopology
			();

		private readonly StateMachine<JobStateInternal, JobEventType, JobEvent> stateMachine;

		private int numMapTasks;

		private int numReduceTasks;

		private int completedTaskCount = 0;

		private int succeededMapTaskCount = 0;

		private int succeededReduceTaskCount = 0;

		private int failedMapTaskCount = 0;

		private int failedReduceTaskCount = 0;

		private int killedMapTaskCount = 0;

		private int killedReduceTaskCount = 0;

		private long startTime;

		private long finishTime;

		private float setupProgress;

		private float mapProgress;

		private float reduceProgress;

		private float cleanupProgress;

		private bool isUber = false;

		private Credentials jobCredentials;

		private Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken;

		private JobTokenSecretManager jobTokenSecretManager;

		private JobStateInternal forcedState = null;

		private ScheduledThreadPoolExecutor executor;

		private ScheduledFuture failWaitTriggerScheduledFuture;

		private JobState lastNonFinalState = JobState.New;

		public JobImpl(JobId jobId, ApplicationAttemptId applicationAttemptId, Configuration
			 conf, EventHandler eventHandler, TaskAttemptListener taskAttemptListener, JobTokenSecretManager
			 jobTokenSecretManager, Credentials jobCredentials, Clock clock, IDictionary<TaskId
			, JobHistoryParser.TaskInfo> completedTasksFromPreviousRun, MRAppMetrics metrics
			, OutputCommitter committer, bool newApiCommitter, string userName, long appSubmitTime
			, IList<AMInfo> amInfos, AppContext appContext, JobStateInternal forcedState, string
			 forcedDiagnostic)
		{
			//The maximum fraction of fetch failures allowed for a map
			//Maximum no. of fetch-failure notifications after which map task is failed
			//final fields
			// FIXME:  
			//
			// Can then replace task-level uber counters (MR-2424) with job-level ones
			// sent from LocalContainerLauncher, and eventually including a count of
			// of uber-AM attempts (probably sent from MRAppMaster).
			//fields initialized in init
			//task/attempt related datastructures
			// Transitions from NEW state
			// Ignore-able events
			// Transitions from INITED state
			// Ignore-able events
			// Transitions from SETUP state
			// Ignore-able events
			// Transitions from RUNNING state
			// Transitions from KILL_WAIT state.
			// Ignore-able events
			// Transitions from COMMITTING state
			// Ignore-able events
			// Transitions from SUCCEEDED state
			// Ignore-able events
			// Transitions from FAIL_WAIT state
			// Ignore-able events
			//Transitions from FAIL_ABORT state
			// Ignore-able events
			// Transitions from KILL_ABORT state
			// Ignore-able events
			// Transitions from FAILED state
			// Ignore-able events
			// Transitions from KILLED state
			// Ignore-able events
			// No transitions from INTERNAL_ERROR state. Ignore all.
			// No transitions from AM_REBOOT state. Ignore all.
			// create the topology tables
			//changing fields while the job is running
			//Executor used for running future tasks.
			this.applicationAttemptId = applicationAttemptId;
			this.jobId = jobId;
			this.jobName = conf.Get(JobContext.JobName, "<missing job name>");
			this.conf = new JobConf(conf);
			this.metrics = metrics;
			this.clock = clock;
			this.completedTasksFromPreviousRun = completedTasksFromPreviousRun;
			this.amInfos = amInfos;
			this.appContext = appContext;
			this.userName = userName;
			this.queueName = conf.Get(MRJobConfig.QueueName, "default");
			this.appSubmitTime = appSubmitTime;
			this.oldJobId = TypeConverter.FromYarn(jobId);
			this.committer = committer;
			this.newApiCommitter = newApiCommitter;
			this.taskAttemptListener = taskAttemptListener;
			this.eventHandler = eventHandler;
			ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
			this.readLock = readWriteLock.ReadLock();
			this.writeLock = readWriteLock.WriteLock();
			this.jobCredentials = jobCredentials;
			this.jobTokenSecretManager = jobTokenSecretManager;
			this.aclsManager = new JobACLsManager(conf);
			this.username = Runtime.GetProperty("user.name");
			this.jobACLs = aclsManager.ConstructJobACLs(conf);
			ThreadFactory threadFactory = new ThreadFactoryBuilder().SetNameFormat("Job Fail Wait Timeout Monitor #%d"
				).SetDaemon(true).Build();
			this.executor = new ScheduledThreadPoolExecutor(1, threadFactory);
			// This "this leak" is okay because the retained pointer is in an
			//  instance variable.
			stateMachine = stateMachineFactory.Make(this);
			this.forcedState = forcedState;
			if (forcedDiagnostic != null)
			{
				this.diagnostics.AddItem(forcedDiagnostic);
			}
			this.maxAllowedFetchFailuresFraction = conf.GetFloat(MRJobConfig.MaxAllowedFetchFailuresFraction
				, MRJobConfig.DefaultMaxAllowedFetchFailuresFraction);
			this.maxFetchFailuresNotifications = conf.GetInt(MRJobConfig.MaxFetchFailuresNotifications
				, MRJobConfig.DefaultMaxFetchFailuresNotifications);
		}

		protected internal virtual StateMachine<JobStateInternal, JobEventType, JobEvent>
			 GetStateMachine()
		{
			return stateMachine;
		}

		public virtual JobId GetID()
		{
			return jobId;
		}

		internal virtual EventHandler GetEventHandler()
		{
			return this.eventHandler;
		}

		internal virtual JobContext GetJobContext()
		{
			return this.jobContext;
		}

		public virtual bool CheckAccess(UserGroupInformation callerUGI, JobACL jobOperation
			)
		{
			AccessControlList jobACL = jobACLs[jobOperation];
			if (jobACL == null)
			{
				return true;
			}
			return aclsManager.CheckAccess(callerUGI, jobOperation, userName, jobACL);
		}

		public virtual Task GetTask(TaskId taskID)
		{
			readLock.Lock();
			try
			{
				return tasks[taskID];
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual int GetCompletedMaps()
		{
			readLock.Lock();
			try
			{
				return succeededMapTaskCount + failedMapTaskCount + killedMapTaskCount;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual int GetCompletedReduces()
		{
			readLock.Lock();
			try
			{
				return succeededReduceTaskCount + failedReduceTaskCount + killedReduceTaskCount;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual bool IsUber()
		{
			return isUber;
		}

		public virtual Counters GetAllCounters()
		{
			readLock.Lock();
			try
			{
				JobStateInternal state = GetInternalState();
				if (state == JobStateInternal.Error || state == JobStateInternal.Failed || state 
					== JobStateInternal.Killed || state == JobStateInternal.Succeeded)
				{
					this.MayBeConstructFinalFullCounters();
					return fullCounters;
				}
				Counters counters = new Counters();
				counters.IncrAllCounters(jobCounters);
				return IncrTaskCounters(counters, tasks.Values);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public static Counters IncrTaskCounters(Counters counters, ICollection<Task> tasks
			)
		{
			foreach (Task task in tasks)
			{
				counters.IncrAllCounters(task.GetCounters());
			}
			return counters;
		}

		public virtual TaskAttemptCompletionEvent[] GetTaskAttemptCompletionEvents(int fromEventId
			, int maxEvents)
		{
			TaskAttemptCompletionEvent[] events = EmptyTaskAttemptCompletionEvents;
			readLock.Lock();
			try
			{
				if (taskAttemptCompletionEvents.Count > fromEventId)
				{
					int actualMax = Math.Min(maxEvents, (taskAttemptCompletionEvents.Count - fromEventId
						));
					events = Sharpen.Collections.ToArray(taskAttemptCompletionEvents.SubList(fromEventId
						, actualMax + fromEventId), events);
				}
				return events;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual TaskCompletionEvent[] GetMapAttemptCompletionEvents(int startIndex
			, int maxEvents)
		{
			TaskCompletionEvent[] events = EmptyTaskCompletionEvents;
			readLock.Lock();
			try
			{
				if (mapAttemptCompletionEvents.Count > startIndex)
				{
					int actualMax = Math.Min(maxEvents, (mapAttemptCompletionEvents.Count - startIndex
						));
					events = Sharpen.Collections.ToArray(mapAttemptCompletionEvents.SubList(startIndex
						, actualMax + startIndex), events);
				}
				return events;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual IList<string> GetDiagnostics()
		{
			readLock.Lock();
			try
			{
				return diagnostics;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual JobReport GetReport()
		{
			readLock.Lock();
			try
			{
				JobState state = GetState();
				// jobFile can be null if the job is not yet inited.
				string jobFile = remoteJobConfFile == null ? string.Empty : remoteJobConfFile.ToString
					();
				StringBuilder diagsb = new StringBuilder();
				foreach (string s in GetDiagnostics())
				{
					diagsb.Append(s).Append("\n");
				}
				if (GetInternalState() == JobStateInternal.New)
				{
					return MRBuilderUtils.NewJobReport(jobId, jobName, username, state, appSubmitTime
						, startTime, finishTime, setupProgress, 0.0f, 0.0f, cleanupProgress, jobFile, amInfos
						, isUber, diagsb.ToString());
				}
				ComputeProgress();
				JobReport report = MRBuilderUtils.NewJobReport(jobId, jobName, username, state, appSubmitTime
					, startTime, finishTime, setupProgress, this.mapProgress, this.reduceProgress, cleanupProgress
					, jobFile, amInfos, isUber, diagsb.ToString());
				return report;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual float GetProgress()
		{
			this.readLock.Lock();
			try
			{
				ComputeProgress();
				return (this.setupProgress * this.setupWeight + this.cleanupProgress * this.cleanupWeight
					 + this.mapProgress * this.mapWeight + this.reduceProgress * this.reduceWeight);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		private void ComputeProgress()
		{
			this.readLock.Lock();
			try
			{
				float mapProgress = 0f;
				float reduceProgress = 0f;
				foreach (Task task in this.tasks.Values)
				{
					if (task.GetType() == TaskType.Map)
					{
						mapProgress += (task.IsFinished() ? 1f : task.GetProgress());
					}
					else
					{
						reduceProgress += (task.IsFinished() ? 1f : task.GetProgress());
					}
				}
				if (this.numMapTasks != 0)
				{
					mapProgress = mapProgress / this.numMapTasks;
				}
				if (this.numReduceTasks != 0)
				{
					reduceProgress = reduceProgress / this.numReduceTasks;
				}
				this.mapProgress = mapProgress;
				this.reduceProgress = reduceProgress;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual IDictionary<TaskId, Task> GetTasks()
		{
			lock (tasksSyncHandle)
			{
				lazyTasksCopyNeeded = true;
				return Sharpen.Collections.UnmodifiableMap(tasks);
			}
		}

		public virtual IDictionary<TaskId, Task> GetTasks(TaskType taskType)
		{
			IDictionary<TaskId, Task> localTasksCopy = tasks;
			IDictionary<TaskId, Task> result = new Dictionary<TaskId, Task>();
			ICollection<TaskId> tasksOfGivenType = null;
			readLock.Lock();
			try
			{
				if (TaskType.Map == taskType)
				{
					tasksOfGivenType = mapTasks;
				}
				else
				{
					tasksOfGivenType = reduceTasks;
				}
				foreach (TaskId taskID in tasksOfGivenType)
				{
					result[taskID] = localTasksCopy[taskID];
				}
				return result;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual JobState GetState()
		{
			readLock.Lock();
			try
			{
				JobState state = GetExternalState(GetInternalState());
				if (!appContext.HasSuccessfullyUnregistered() && (state == JobState.Succeeded || 
					state == JobState.Failed || state == JobState.Killed || state == JobState.Error))
				{
					return lastNonFinalState;
				}
				else
				{
					return state;
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		protected internal virtual void ScheduleTasks(ICollection<TaskId> taskIDs, bool recoverTaskOutput
			)
		{
			foreach (TaskId taskID in taskIDs)
			{
				JobHistoryParser.TaskInfo taskInfo = Sharpen.Collections.Remove(completedTasksFromPreviousRun
					, taskID);
				if (taskInfo != null)
				{
					eventHandler.Handle(new TaskRecoverEvent(taskID, taskInfo, committer, recoverTaskOutput
						));
				}
				else
				{
					eventHandler.Handle(new TaskEvent(taskID, TaskEventType.TSchedule));
				}
			}
		}

		public virtual void Handle(JobEvent @event)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Processing " + @event.GetJobId() + " of type " + @event.GetType());
			}
			try
			{
				writeLock.Lock();
				JobStateInternal oldState = GetInternalState();
				try
				{
					GetStateMachine().DoTransition(@event.GetType(), @event);
				}
				catch (InvalidStateTransitonException e)
				{
					Log.Error("Can't handle this event at current state", e);
					AddDiagnostic("Invalid event " + @event.GetType() + " on Job " + this.jobId);
					eventHandler.Handle(new JobEvent(this.jobId, JobEventType.InternalError));
				}
				//notify the eventhandler of state change
				if (oldState != GetInternalState())
				{
					Log.Info(jobId + "Job Transitioned from " + oldState + " to " + GetInternalState(
						));
					RememberLastNonFinalState(oldState);
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		private void RememberLastNonFinalState(JobStateInternal stateInternal)
		{
			JobState state = GetExternalState(stateInternal);
			// if state is not the final state, set lastNonFinalState
			if (state != JobState.Succeeded && state != JobState.Failed && state != JobState.
				Killed && state != JobState.Error)
			{
				lastNonFinalState = state;
			}
		}

		[InterfaceAudience.Private]
		public virtual JobStateInternal GetInternalState()
		{
			readLock.Lock();
			try
			{
				if (forcedState != null)
				{
					return forcedState;
				}
				return GetStateMachine().GetCurrentState();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		private JobState GetExternalState(JobStateInternal smState)
		{
			switch (smState)
			{
				case JobStateInternal.KillWait:
				case JobStateInternal.KillAbort:
				{
					return JobState.Killed;
				}

				case JobStateInternal.Setup:
				case JobStateInternal.Committing:
				{
					return JobState.Running;
				}

				case JobStateInternal.FailWait:
				case JobStateInternal.FailAbort:
				{
					return JobState.Failed;
				}

				case JobStateInternal.Reboot:
				{
					if (appContext.IsLastAMRetry())
					{
						return JobState.Error;
					}
					else
					{
						// In case of not last retry, return the external state as RUNNING since
						// otherwise JobClient will exit when it polls the AM for job state
						return JobState.Running;
					}
					goto default;
				}

				default:
				{
					return JobState.ValueOf(smState.ToString());
				}
			}
		}

		//helpful in testing
		protected internal virtual void AddTask(Task task)
		{
			lock (tasksSyncHandle)
			{
				if (lazyTasksCopyNeeded)
				{
					IDictionary<TaskId, Task> newTasks = new LinkedHashMap<TaskId, Task>();
					newTasks.PutAll(tasks);
					tasks = newTasks;
					lazyTasksCopyNeeded = false;
				}
			}
			tasks[task.GetID()] = task;
			if (task.GetType() == TaskType.Map)
			{
				mapTasks.AddItem(task.GetID());
			}
			else
			{
				if (task.GetType() == TaskType.Reduce)
				{
					reduceTasks.AddItem(task.GetID());
				}
			}
			metrics.WaitingTask(task);
		}

		internal virtual void SetFinishTime()
		{
			finishTime = clock.GetTime();
		}

		internal virtual void LogJobHistoryFinishedEvent()
		{
			this.SetFinishTime();
			JobFinishedEvent jfe = CreateJobFinishedEvent(this);
			Log.Info("Calling handler for JobFinishedEvent ");
			this.GetEventHandler().Handle(new JobHistoryEvent(this.jobId, jfe));
		}

		/// <summary>Create the default file System for this job.</summary>
		/// <param name="conf">the conf object</param>
		/// <returns>the default filesystem for this job</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual FileSystem GetFileSystem(Configuration conf)
		{
			return FileSystem.Get(conf);
		}

		protected internal virtual JobStateInternal CheckReadyForCommit()
		{
			JobStateInternal currentState = GetInternalState();
			if (completedTaskCount == tasks.Count && currentState == JobStateInternal.Running)
			{
				eventHandler.Handle(new CommitterJobCommitEvent(jobId, GetJobContext()));
				return JobStateInternal.Committing;
			}
			// return the current state as job not ready to commit yet
			return GetInternalState();
		}

		internal virtual JobStateInternal Finished(JobStateInternal finalState)
		{
			if (GetInternalState() == JobStateInternal.Running)
			{
				metrics.EndRunningJob(this);
			}
			if (finishTime == 0)
			{
				SetFinishTime();
			}
			eventHandler.Handle(new JobFinishEvent(jobId));
			switch (finalState)
			{
				case JobStateInternal.Killed:
				{
					metrics.KilledJob(this);
					break;
				}

				case JobStateInternal.Reboot:
				case JobStateInternal.Error:
				case JobStateInternal.Failed:
				{
					metrics.FailedJob(this);
					break;
				}

				case JobStateInternal.Succeeded:
				{
					metrics.CompletedJob(this);
					break;
				}

				default:
				{
					throw new ArgumentException("Illegal job state: " + finalState);
				}
			}
			return finalState;
		}

		public virtual string GetUserName()
		{
			return userName;
		}

		public virtual string GetQueueName()
		{
			return queueName;
		}

		public virtual void SetQueueName(string queueName)
		{
			this.queueName = queueName;
			JobQueueChangeEvent jqce = new JobQueueChangeEvent(oldJobId, queueName);
			eventHandler.Handle(new JobHistoryEvent(jobId, jqce));
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.job.Job#getConfFile()
		*/
		public virtual Path GetConfFile()
		{
			return remoteJobConfFile;
		}

		public virtual string GetName()
		{
			return jobName;
		}

		public virtual int GetTotalMaps()
		{
			return mapTasks.Count;
		}

		//FIXME: why indirection? return numMapTasks...
		// unless race?  how soon can this get called?
		public virtual int GetTotalReduces()
		{
			return reduceTasks.Count;
		}

		//FIXME: why indirection? return numReduceTasks
		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.job.Job#getJobACLs()
		*/
		public virtual IDictionary<JobACL, AccessControlList> GetJobACLs()
		{
			return Sharpen.Collections.UnmodifiableMap(jobACLs);
		}

		public virtual IList<AMInfo> GetAMInfos()
		{
			return amInfos;
		}

		/// <summary>Decide whether job can be run in uber mode based on various criteria.</summary>
		/// <param name="dataInputLength">Total length for all splits</param>
		private void MakeUberDecision(long dataInputLength)
		{
			//FIXME:  need new memory criterion for uber-decision (oops, too late here;
			// until AM-resizing supported,
			// must depend on job client to pass fat-slot needs)
			// these are no longer "system" settings, necessarily; user may override
			int sysMaxMaps = conf.GetInt(MRJobConfig.JobUbertaskMaxmaps, 9);
			int sysMaxReduces = conf.GetInt(MRJobConfig.JobUbertaskMaxreduces, 1);
			long sysMaxBytes = conf.GetLong(MRJobConfig.JobUbertaskMaxbytes, fs.GetDefaultBlockSize
				(this.remoteJobSubmitDir));
			// FIXME: this is wrong; get FS from
			// [File?]InputFormat and default block size
			// from that
			long sysMemSizeForUberSlot = conf.GetInt(MRJobConfig.MrAmVmemMb, MRJobConfig.DefaultMrAmVmemMb
				);
			long sysCPUSizeForUberSlot = conf.GetInt(MRJobConfig.MrAmCpuVcores, MRJobConfig.DefaultMrAmCpuVcores
				);
			bool uberEnabled = conf.GetBoolean(MRJobConfig.JobUbertaskEnable, false);
			bool smallNumMapTasks = (numMapTasks <= sysMaxMaps);
			bool smallNumReduceTasks = (numReduceTasks <= sysMaxReduces);
			bool smallInput = (dataInputLength <= sysMaxBytes);
			// ignoring overhead due to UberAM and statics as negligible here:
			long requiredMapMB = conf.GetLong(MRJobConfig.MapMemoryMb, 0);
			long requiredReduceMB = conf.GetLong(MRJobConfig.ReduceMemoryMb, 0);
			long requiredMB = Math.Max(requiredMapMB, requiredReduceMB);
			int requiredMapCores = conf.GetInt(MRJobConfig.MapCpuVcores, MRJobConfig.DefaultMapCpuVcores
				);
			int requiredReduceCores = conf.GetInt(MRJobConfig.ReduceCpuVcores, MRJobConfig.DefaultReduceCpuVcores
				);
			int requiredCores = Math.Max(requiredMapCores, requiredReduceCores);
			if (numReduceTasks == 0)
			{
				requiredMB = requiredMapMB;
				requiredCores = requiredMapCores;
			}
			bool smallMemory = (requiredMB <= sysMemSizeForUberSlot) || (sysMemSizeForUberSlot
				 == JobConf.DisabledMemoryLimit);
			bool smallCpu = requiredCores <= sysCPUSizeForUberSlot;
			bool notChainJob = !IsChainJob(conf);
			// User has overall veto power over uberization, or user can modify
			// limits (overriding system settings and potentially shooting
			// themselves in the head).  Note that ChainMapper/Reducer are
			// fundamentally incompatible with MR-1220; they employ a blocking
			// queue between the maps/reduces and thus require parallel execution,
			// while "uber-AM" (MR AM + LocalContainerLauncher) loops over tasks
			// and thus requires sequential execution.
			isUber = uberEnabled && smallNumMapTasks && smallNumReduceTasks && smallInput && 
				smallMemory && smallCpu && notChainJob;
			if (isUber)
			{
				Log.Info("Uberizing job " + jobId + ": " + numMapTasks + "m+" + numReduceTasks + 
					"r tasks (" + dataInputLength + " input bytes) will run sequentially on single node."
					);
				// make sure reduces are scheduled only after all map are completed
				conf.SetFloat(MRJobConfig.CompletedMapsForReduceSlowstart, 1.0f);
				// uber-subtask attempts all get launched on same node; if one fails,
				// probably should retry elsewhere, i.e., move entire uber-AM:  ergo,
				// limit attempts to 1 (or at most 2?  probably not...)
				conf.SetInt(MRJobConfig.MapMaxAttempts, 1);
				conf.SetInt(MRJobConfig.ReduceMaxAttempts, 1);
				// disable speculation
				conf.SetBoolean(MRJobConfig.MapSpeculative, false);
				conf.SetBoolean(MRJobConfig.ReduceSpeculative, false);
			}
			else
			{
				StringBuilder msg = new StringBuilder();
				msg.Append("Not uberizing ").Append(jobId).Append(" because:");
				if (!uberEnabled)
				{
					msg.Append(" not enabled;");
				}
				if (!smallNumMapTasks)
				{
					msg.Append(" too many maps;");
				}
				if (!smallNumReduceTasks)
				{
					msg.Append(" too many reduces;");
				}
				if (!smallInput)
				{
					msg.Append(" too much input;");
				}
				if (!smallCpu)
				{
					msg.Append(" too much CPU;");
				}
				if (!smallMemory)
				{
					msg.Append(" too much RAM;");
				}
				if (!notChainJob)
				{
					msg.Append(" chainjob;");
				}
				Log.Info(msg.ToString());
			}
		}

		/// <summary>
		/// ChainMapper and ChainReducer must execute in parallel, so they're not
		/// compatible with uberization/LocalContainerLauncher (100% sequential).
		/// </summary>
		private bool IsChainJob(Configuration conf)
		{
			bool isChainJob = false;
			try
			{
				string mapClassName = conf.Get(MRJobConfig.MapClassAttr);
				if (mapClassName != null)
				{
					Type mapClass = Sharpen.Runtime.GetType(mapClassName);
					if (typeof(ChainMapper).IsAssignableFrom(mapClass))
					{
						isChainJob = true;
					}
				}
			}
			catch (TypeLoadException)
			{
			}
			catch (NoClassDefFoundError)
			{
			}
			// don't care; assume it's not derived from ChainMapper
			try
			{
				string reduceClassName = conf.Get(MRJobConfig.ReduceClassAttr);
				if (reduceClassName != null)
				{
					Type reduceClass = Sharpen.Runtime.GetType(reduceClassName);
					if (typeof(ChainReducer).IsAssignableFrom(reduceClass))
					{
						isChainJob = true;
					}
				}
			}
			catch (TypeLoadException)
			{
			}
			catch (NoClassDefFoundError)
			{
			}
			// don't care; assume it's not derived from ChainReducer
			return isChainJob;
		}

		private void ActOnUnusableNode(NodeId nodeId, NodeState nodeState)
		{
			// rerun previously successful map tasks
			IList<TaskAttemptId> taskAttemptIdList = nodesToSucceededTaskAttempts[nodeId];
			if (taskAttemptIdList != null)
			{
				string mesg = "TaskAttempt killed because it ran on unusable node " + nodeId;
				foreach (TaskAttemptId id in taskAttemptIdList)
				{
					if (TaskType.Map == id.GetTaskId().GetTaskType())
					{
						// reschedule only map tasks because their outputs maybe unusable
						Log.Info(mesg + ". AttemptId:" + id);
						eventHandler.Handle(new TaskAttemptKillEvent(id, mesg));
					}
				}
			}
		}

		// currently running task attempts on unusable nodes are handled in
		// RMContainerAllocator
		/*
		private int getBlockSize() {
		String inputClassName = conf.get(MRJobConfig.INPUT_FORMAT_CLASS_ATTR);
		if (inputClassName != null) {
		Class<?> inputClass - Class.forName(inputClassName);
		if (FileInputFormat<K, V>)
		}
		}
		*/
		/// <summary>
		/// Get the workflow adjacencies from the job conf
		/// The string returned is of the form "key"="value" "key"="value" ...
		/// </summary>
		private static string GetWorkflowAdjacencies(Configuration conf)
		{
			int prefixLen = MRJobConfig.WorkflowAdjacencyPrefixString.Length;
			IDictionary<string, string> adjacencies = conf.GetValByRegex(MRJobConfig.WorkflowAdjacencyPrefixPattern
				);
			if (adjacencies.IsEmpty())
			{
				return string.Empty;
			}
			int size = 0;
			foreach (KeyValuePair<string, string> entry in adjacencies)
			{
				int keyLen = entry.Key.Length;
				size += keyLen - prefixLen;
				size += entry.Value.Length + 6;
			}
			StringBuilder sb = new StringBuilder(size);
			foreach (KeyValuePair<string, string> entry_1 in adjacencies)
			{
				int keyLen = entry_1.Key.Length;
				sb.Append("\"");
				sb.Append(EscapeString(Sharpen.Runtime.Substring(entry_1.Key, prefixLen, keyLen))
					);
				sb.Append("\"=\"");
				sb.Append(EscapeString(entry_1.Value));
				sb.Append("\" ");
			}
			return sb.ToString();
		}

		public static string EscapeString(string data)
		{
			return StringUtils.EscapeString(data, StringUtils.EscapeChar, new char[] { '"', '='
				, '.' });
		}

		public class InitTransition : MultipleArcTransition<JobImpl, JobEvent, JobStateInternal
			>
		{
			/// <summary>
			/// Note that this transition method is called directly (and synchronously)
			/// by MRAppMaster's init() method (i.e., no RPC, no thread-switching;
			/// just plain sequential call within AM context), so we can trigger
			/// modifications in AM state from here (at least, if AM is written that
			/// way; MR version is).
			/// </summary>
			public virtual JobStateInternal Transition(JobImpl job, JobEvent @event)
			{
				job.metrics.SubmittedJob(job);
				job.metrics.PreparingJob(job);
				if (job.newApiCommitter)
				{
					job.jobContext = new JobContextImpl(job.conf, job.oldJobId);
				}
				else
				{
					job.jobContext = new JobContextImpl(job.conf, job.oldJobId);
				}
				try
				{
					Setup(job);
					job.fs = job.GetFileSystem(job.conf);
					//log to job history
					JobSubmittedEvent jse = new JobSubmittedEvent(job.oldJobId, job.conf.Get(MRJobConfig
						.JobName, "test"), job.conf.Get(MRJobConfig.UserName, "mapred"), job.appSubmitTime
						, job.remoteJobConfFile.ToString(), job.jobACLs, job.queueName, job.conf.Get(MRJobConfig
						.WorkflowId, string.Empty), job.conf.Get(MRJobConfig.WorkflowName, string.Empty)
						, job.conf.Get(MRJobConfig.WorkflowNodeName, string.Empty), GetWorkflowAdjacencies
						(job.conf), job.conf.Get(MRJobConfig.WorkflowTags, string.Empty));
					job.eventHandler.Handle(new JobHistoryEvent(job.jobId, jse));
					//TODO JH Verify jobACLs, UserName via UGI?
					JobSplit.TaskSplitMetaInfo[] taskSplitMetaInfo = CreateSplits(job, job.jobId);
					job.numMapTasks = taskSplitMetaInfo.Length;
					job.numReduceTasks = job.conf.GetInt(MRJobConfig.NumReduces, 0);
					if (job.numMapTasks == 0 && job.numReduceTasks == 0)
					{
						job.AddDiagnostic("No of maps and reduces are 0 " + job.jobId);
					}
					else
					{
						if (job.numMapTasks == 0)
						{
							job.reduceWeight = 0.9f;
						}
						else
						{
							if (job.numReduceTasks == 0)
							{
								job.mapWeight = 0.9f;
							}
							else
							{
								job.mapWeight = job.reduceWeight = 0.45f;
							}
						}
					}
					CheckTaskLimits();
					long inputLength = 0;
					for (int i = 0; i < job.numMapTasks; ++i)
					{
						inputLength += taskSplitMetaInfo[i].GetInputDataLength();
					}
					job.MakeUberDecision(inputLength);
					job.taskAttemptCompletionEvents = new AList<TaskAttemptCompletionEvent>(job.numMapTasks
						 + job.numReduceTasks + 10);
					job.mapAttemptCompletionEvents = new AList<TaskCompletionEvent>(job.numMapTasks +
						 10);
					job.taskCompletionIdxToMapCompletionIdx = new AList<int>(job.numMapTasks + job.numReduceTasks
						 + 10);
					job.allowedMapFailuresPercent = job.conf.GetInt(MRJobConfig.MapFailuresMaxPercent
						, 0);
					job.allowedReduceFailuresPercent = job.conf.GetInt(MRJobConfig.ReduceFailuresMaxpercent
						, 0);
					// create the Tasks but don't start them yet
					CreateMapTasks(job, inputLength, taskSplitMetaInfo);
					CreateReduceTasks(job);
					job.metrics.EndPreparingJob(job);
					return JobStateInternal.Inited;
				}
				catch (Exception e)
				{
					Log.Warn("Job init failed", e);
					job.metrics.EndPreparingJob(job);
					job.AddDiagnostic("Job init failed : " + StringUtils.StringifyException(e));
					// Leave job in the NEW state. The MR AM will detect that the state is
					// not INITED and send a JOB_INIT_FAILED event.
					return JobStateInternal.New;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual void Setup(JobImpl job)
			{
				string oldJobIDString = job.oldJobId.ToString();
				string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
				Path path = MRApps.GetStagingAreaDir(job.conf, user);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("startJobs: parent=" + path + " child=" + oldJobIDString);
				}
				job.remoteJobSubmitDir = FileSystem.Get(job.conf).MakeQualified(new Path(path, oldJobIDString
					));
				job.remoteJobConfFile = new Path(job.remoteJobSubmitDir, MRJobConfig.JobConfFile);
				// Prepare the TaskAttemptListener server for authentication of Containers
				// TaskAttemptListener gets the information via jobTokenSecretManager.
				JobTokenIdentifier identifier = new JobTokenIdentifier(new Org.Apache.Hadoop.IO.Text
					(oldJobIDString));
				job.jobToken = new Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier>(identifier
					, job.jobTokenSecretManager);
				job.jobToken.SetService(identifier.GetJobId());
				// Add it to the jobTokenSecretManager so that TaskAttemptListener server
				// can authenticate containers(tasks)
				job.jobTokenSecretManager.AddTokenForJob(oldJobIDString, job.jobToken);
				Log.Info("Adding job token for " + oldJobIDString + " to jobTokenSecretManager");
				// If the job client did not setup the shuffle secret then reuse
				// the job token secret for the shuffle.
				if (TokenCache.GetShuffleSecretKey(job.jobCredentials) == null)
				{
					Log.Warn("Shuffle secret key missing from job credentials." + " Using job token secret as shuffle secret."
						);
					TokenCache.SetShuffleSecretKey(job.jobToken.GetPassword(), job.jobCredentials);
				}
			}

			private void CreateMapTasks(JobImpl job, long inputLength, JobSplit.TaskSplitMetaInfo
				[] splits)
			{
				for (int i = 0; i < job.numMapTasks; ++i)
				{
					TaskImpl task = new MapTaskImpl(job.jobId, i, job.eventHandler, job.remoteJobConfFile
						, job.conf, splits[i], job.taskAttemptListener, job.jobToken, job.jobCredentials
						, job.clock, job.applicationAttemptId.GetAttemptId(), job.metrics, job.appContext
						);
					job.AddTask(task);
				}
				Log.Info("Input size for job " + job.jobId + " = " + inputLength + ". Number of splits = "
					 + splits.Length);
			}

			private void CreateReduceTasks(JobImpl job)
			{
				for (int i = 0; i < job.numReduceTasks; i++)
				{
					TaskImpl task = new ReduceTaskImpl(job.jobId, i, job.eventHandler, job.remoteJobConfFile
						, job.conf, job.numMapTasks, job.taskAttemptListener, job.jobToken, job.jobCredentials
						, job.clock, job.applicationAttemptId.GetAttemptId(), job.metrics, job.appContext
						);
					job.AddTask(task);
				}
				Log.Info("Number of reduces for job " + job.jobId + " = " + job.numReduceTasks);
			}

			protected internal virtual JobSplit.TaskSplitMetaInfo[] CreateSplits(JobImpl job, 
				JobId jobId)
			{
				JobSplit.TaskSplitMetaInfo[] allTaskSplitMetaInfo;
				try
				{
					allTaskSplitMetaInfo = SplitMetaInfoReader.ReadSplitMetaInfo(job.oldJobId, job.fs
						, job.conf, job.remoteJobSubmitDir);
				}
				catch (IOException e)
				{
					throw new YarnRuntimeException(e);
				}
				return allTaskSplitMetaInfo;
			}

			/// <summary>
			/// If the number of tasks are greater than the configured value
			/// throw an exception that will fail job initialization
			/// </summary>
			private void CheckTaskLimits()
			{
			}
			// no code, for now
		}

		private class InitFailedTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			// end of InitTransition
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.eventHandler.Handle(new CommitterJobAbortEvent(job.jobId, job.jobContext, JobStatus.State
					.Failed));
			}
		}

		private class SetupCompletedTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.setupProgress = 1.0f;
				job.ScheduleTasks(job.mapTasks, job.numReduceTasks == 0);
				job.ScheduleTasks(job.reduceTasks, true);
				// If we have no tasks, just transition to job completed
				if (job.numReduceTasks == 0 && job.numMapTasks == 0)
				{
					job.eventHandler.Handle(new JobEvent(job.jobId, JobEventType.JobCompleted));
				}
			}
		}

		private class SetupFailedTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.metrics.EndRunningJob(job);
				job.AddDiagnostic("Job setup failed : " + ((JobSetupFailedEvent)@event).GetMessage
					());
				job.eventHandler.Handle(new CommitterJobAbortEvent(job.jobId, job.jobContext, JobStatus.State
					.Failed));
			}
		}

		public class StartTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			/// <summary>
			/// This transition executes in the event-dispatcher thread, though it's
			/// triggered in MRAppMaster's startJobs() method.
			/// </summary>
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				JobStartEvent jse = (JobStartEvent)@event;
				if (jse.GetRecoveredJobStartTime() != 0)
				{
					job.startTime = jse.GetRecoveredJobStartTime();
				}
				else
				{
					job.startTime = job.clock.GetTime();
				}
				JobInitedEvent jie = new JobInitedEvent(job.oldJobId, job.startTime, job.numMapTasks
					, job.numReduceTasks, job.GetState().ToString(), job.IsUber());
				job.eventHandler.Handle(new JobHistoryEvent(job.jobId, jie));
				JobInfoChangeEvent jice = new JobInfoChangeEvent(job.oldJobId, job.appSubmitTime, 
					job.startTime);
				job.eventHandler.Handle(new JobHistoryEvent(job.jobId, jice));
				job.metrics.RunningJob(job);
				job.eventHandler.Handle(new CommitterJobSetupEvent(job.jobId, job.jobContext));
			}
		}

		private void UnsuccessfulFinish(JobStateInternal finalState)
		{
			if (finishTime == 0)
			{
				SetFinishTime();
			}
			cleanupProgress = 1.0f;
			JobUnsuccessfulCompletionEvent unsuccessfulJobEvent = new JobUnsuccessfulCompletionEvent
				(oldJobId, finishTime, succeededMapTaskCount, succeededReduceTaskCount, finalState
				.ToString(), diagnostics);
			eventHandler.Handle(new JobHistoryEvent(jobId, unsuccessfulJobEvent));
			Finished(finalState);
		}

		private class JobAbortCompletedTransition : SingleArcTransition<JobImpl, JobEvent
			>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				JobStateInternal finalState = JobStateInternal.ValueOf(((JobAbortCompletedEvent)@event
					).GetFinalState().ToString());
				job.UnsuccessfulFinish(finalState);
			}
		}

		private class JobFailWaitTransition : MultipleArcTransition<JobImpl, JobEvent, JobStateInternal
			>
		{
			//This transition happens when a job is to be failed. It waits for all the
			//tasks to finish / be killed.
			public virtual JobStateInternal Transition(JobImpl job, JobEvent @event)
			{
				if (!job.failWaitTriggerScheduledFuture.IsCancelled())
				{
					foreach (Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task task in job.tasks.Values)
					{
						if (!task.IsFinished())
						{
							return JobStateInternal.FailWait;
						}
					}
				}
				//Finished waiting. All tasks finished / were killed
				job.failWaitTriggerScheduledFuture.Cancel(false);
				job.eventHandler.Handle(new CommitterJobAbortEvent(job.jobId, job.jobContext, JobStatus.State
					.Failed));
				return JobStateInternal.FailAbort;
			}
		}

		private class JobFailWaitTimedOutTransition : SingleArcTransition<JobImpl, JobEvent
			>
		{
			//This transition happens when a job to be failed times out while waiting on
			//tasks that had been sent the KILL signal. It is triggered by a
			//ScheduledFuture task queued in the executor.
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				Log.Info("Timeout expired in FAIL_WAIT waiting for tasks to get killed." + " Going to fail job anyway"
					);
				job.failWaitTriggerScheduledFuture.Cancel(false);
				job.eventHandler.Handle(new CommitterJobAbortEvent(job.jobId, job.jobContext, JobStatus.State
					.Failed));
			}
		}

		// JobFinishedEvent triggers the move of the history file out of the staging
		// area. May need to create a new event type for this if JobFinished should 
		// not be generated for KilledJobs, etc.
		private static JobFinishedEvent CreateJobFinishedEvent(JobImpl job)
		{
			job.MayBeConstructFinalFullCounters();
			JobFinishedEvent jfe = new JobFinishedEvent(job.oldJobId, job.finishTime, job.succeededMapTaskCount
				, job.succeededReduceTaskCount, job.failedMapTaskCount, job.failedReduceTaskCount
				, job.finalMapCounters, job.finalReduceCounters, job.fullCounters);
			return jfe;
		}

		private void MayBeConstructFinalFullCounters()
		{
			// Calculating full-counters. This should happen only once for the job.
			lock (this.fullCountersLock)
			{
				if (this.fullCounters != null)
				{
					// Already constructed. Just return.
					return;
				}
				this.ConstructFinalFullcounters();
			}
		}

		[InterfaceAudience.Private]
		public virtual void ConstructFinalFullcounters()
		{
			this.fullCounters = new Counters();
			this.finalMapCounters = new Counters();
			this.finalReduceCounters = new Counters();
			this.fullCounters.IncrAllCounters(jobCounters);
			foreach (Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task t in this.tasks.Values)
			{
				Counters counters = t.GetCounters();
				switch (t.GetType())
				{
					case TaskType.Map:
					{
						this.finalMapCounters.IncrAllCounters(counters);
						break;
					}

					case TaskType.Reduce:
					{
						this.finalReduceCounters.IncrAllCounters(counters);
						break;
					}

					default:
					{
						throw new InvalidOperationException("Task type neither map nor reduce: " + t.GetType
							());
					}
				}
				this.fullCounters.IncrAllCounters(counters);
			}
		}

		private class KillNewJobTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			// Task-start has been moved out of InitTransition, so this arc simply
			// hardcodes 0 for both map and reduce finished tasks.
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.SetFinishTime();
				JobUnsuccessfulCompletionEvent failedEvent = new JobUnsuccessfulCompletionEvent(job
					.oldJobId, job.finishTime, 0, 0, JobStateInternal.Killed.ToString(), job.diagnostics
					);
				job.eventHandler.Handle(new JobHistoryEvent(job.jobId, failedEvent));
				job.Finished(JobStateInternal.Killed);
			}
		}

		private class KillInitedJobTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.AddDiagnostic("Job received Kill in INITED state.");
				job.eventHandler.Handle(new CommitterJobAbortEvent(job.jobId, job.jobContext, JobStatus.State
					.Killed));
			}
		}

		private class KilledDuringSetupTransition : SingleArcTransition<JobImpl, JobEvent
			>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.metrics.EndRunningJob(job);
				job.AddDiagnostic("Job received kill in SETUP state.");
				job.eventHandler.Handle(new CommitterJobAbortEvent(job.jobId, job.jobContext, JobStatus.State
					.Killed));
			}
		}

		private class KillTasksTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.AddDiagnostic(JobKilledDiag);
				foreach (Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task task in job.tasks.Values)
				{
					job.eventHandler.Handle(new TaskEvent(task.GetID(), TaskEventType.TKill));
				}
				job.metrics.EndRunningJob(job);
			}
		}

		private class TaskAttemptCompletedEventTransition : SingleArcTransition<JobImpl, 
			JobEvent>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				TaskAttemptCompletionEvent tce = ((JobTaskAttemptCompletedEvent)@event).GetCompletionEvent
					();
				// Add the TaskAttemptCompletionEvent
				//eventId is equal to index in the arraylist
				tce.SetEventId(job.taskAttemptCompletionEvents.Count);
				job.taskAttemptCompletionEvents.AddItem(tce);
				int mapEventIdx = -1;
				if (TaskType.Map.Equals(tce.GetAttemptId().GetTaskId().GetTaskType()))
				{
					// we track map completions separately from task completions because
					// - getMapAttemptCompletionEvents uses index ranges specific to maps
					// - type converting the same events over and over is expensive
					mapEventIdx = job.mapAttemptCompletionEvents.Count;
					job.mapAttemptCompletionEvents.AddItem(TypeConverter.FromYarn(tce));
				}
				job.taskCompletionIdxToMapCompletionIdx.AddItem(mapEventIdx);
				TaskAttemptId attemptId = tce.GetAttemptId();
				TaskId taskId = attemptId.GetTaskId();
				//make the previous completion event as obsolete if it exists
				int successEventNo = Sharpen.Collections.Remove(job.successAttemptCompletionEventNoMap
					, taskId);
				if (successEventNo != null)
				{
					TaskAttemptCompletionEvent successEvent = job.taskAttemptCompletionEvents[successEventNo
						];
					successEvent.SetStatus(TaskAttemptCompletionEventStatus.Obsolete);
					int mapCompletionIdx = job.taskCompletionIdxToMapCompletionIdx[successEventNo];
					if (mapCompletionIdx >= 0)
					{
						// update the corresponding TaskCompletionEvent for the map
						TaskCompletionEvent mapEvent = job.mapAttemptCompletionEvents[mapCompletionIdx];
						job.mapAttemptCompletionEvents.Set(mapCompletionIdx, new TaskCompletionEvent(mapEvent
							.GetEventId(), ((TaskAttemptID)mapEvent.GetTaskAttemptId()), mapEvent.IdWithinJob
							(), mapEvent.IsMapTask(), TaskCompletionEvent.Status.Obsolete, mapEvent.GetTaskTrackerHttp
							()));
					}
				}
				// if this attempt is not successful then why is the previous successful 
				// attempt being removed above - MAPREDUCE-4330
				if (TaskAttemptCompletionEventStatus.Succeeded.Equals(tce.GetStatus()))
				{
					job.successAttemptCompletionEventNoMap[taskId] = tce.GetEventId();
					// here we could have simply called Task.getSuccessfulAttempt() but
					// the event that triggers this code is sent before
					// Task.successfulAttempt is set and so there is no guarantee that it
					// will be available now
					Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task task = job.tasks[taskId];
					TaskAttempt attempt = task.GetAttempt(attemptId);
					NodeId nodeId = attempt.GetNodeId();
					System.Diagnostics.Debug.Assert((nodeId != null));
					// node must exist for a successful event
					IList<TaskAttemptId> taskAttemptIdList = job.nodesToSucceededTaskAttempts[nodeId];
					if (taskAttemptIdList == null)
					{
						taskAttemptIdList = new AList<TaskAttemptId>();
						job.nodesToSucceededTaskAttempts[nodeId] = taskAttemptIdList;
					}
					taskAttemptIdList.AddItem(attempt.GetID());
				}
			}
		}

		private class TaskAttemptFetchFailureTransition : SingleArcTransition<JobImpl, JobEvent
			>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				//get number of shuffling reduces
				int shufflingReduceTasks = 0;
				foreach (TaskId taskId in job.reduceTasks)
				{
					Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task task = job.tasks[taskId];
					if (TaskState.Running.Equals(task.GetState()))
					{
						foreach (TaskAttempt attempt in task.GetAttempts().Values)
						{
							if (attempt.GetPhase() == Phase.Shuffle)
							{
								shufflingReduceTasks++;
								break;
							}
						}
					}
				}
				JobTaskAttemptFetchFailureEvent fetchfailureEvent = (JobTaskAttemptFetchFailureEvent
					)@event;
				foreach (TaskAttemptId mapId in fetchfailureEvent.GetMaps())
				{
					int fetchFailures = job.fetchFailuresMapping[mapId];
					fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures + 1);
					job.fetchFailuresMapping[mapId] = fetchFailures;
					float failureRate = shufflingReduceTasks == 0 ? 1.0f : (float)fetchFailures / shufflingReduceTasks;
					// declare faulty if fetch-failures >= max-allowed-failures
					if (fetchFailures >= job.GetMaxFetchFailuresNotifications() && failureRate >= job
						.GetMaxAllowedFetchFailuresFraction())
					{
						Log.Info("Too many fetch-failures for output of task attempt: " + mapId + " ... raising fetch failure to map"
							);
						job.eventHandler.Handle(new TaskAttemptEvent(mapId, TaskAttemptEventType.TaTooManyFetchFailure
							));
						Sharpen.Collections.Remove(job.fetchFailuresMapping, mapId);
					}
				}
			}
		}

		private class TaskCompletedTransition : MultipleArcTransition<JobImpl, JobEvent, 
			JobStateInternal>
		{
			public virtual JobStateInternal Transition(JobImpl job, JobEvent @event)
			{
				job.completedTaskCount++;
				Log.Info("Num completed Tasks: " + job.completedTaskCount);
				JobTaskEvent taskEvent = (JobTaskEvent)@event;
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task task = job.tasks[taskEvent.GetTaskID(
					)];
				if (taskEvent.GetState() == TaskState.Succeeded)
				{
					TaskSucceeded(job, task);
				}
				else
				{
					if (taskEvent.GetState() == TaskState.Failed)
					{
						TaskFailed(job, task);
					}
					else
					{
						if (taskEvent.GetState() == TaskState.Killed)
						{
							TaskKilled(job, task);
						}
					}
				}
				return CheckJobAfterTaskCompletion(job);
			}

			internal class TriggerScheduledFuture : Runnable
			{
				internal JobEvent toSend;

				internal JobImpl job;

				internal TriggerScheduledFuture(JobImpl job, JobEvent toSend)
				{
					//This class is used to queue a ScheduledFuture to send an event to a job
					//after some delay. This can be used to wait for maximum amount of time
					//before proceeding anyway. e.g. When a job is waiting in FAIL_WAIT for
					//all tasks to be killed.
					this.toSend = toSend;
					this.job = job;
				}

				public virtual void Run()
				{
					Log.Info("Sending event " + toSend + " to " + job.GetID());
					job.GetEventHandler().Handle(toSend);
				}
			}

			protected internal virtual JobStateInternal CheckJobAfterTaskCompletion(JobImpl job
				)
			{
				//check for Job failure
				if (job.failedMapTaskCount * 100 > job.allowedMapFailuresPercent * job.numMapTasks
					 || job.failedReduceTaskCount * 100 > job.allowedReduceFailuresPercent * job.numReduceTasks)
				{
					job.SetFinishTime();
					string diagnosticMsg = "Job failed as tasks failed. " + "failedMaps:" + job.failedMapTaskCount
						 + " failedReduces:" + job.failedReduceTaskCount;
					Log.Info(diagnosticMsg);
					job.AddDiagnostic(diagnosticMsg);
					//Send kill signal to all unfinished tasks here.
					bool allDone = true;
					foreach (Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task task in job.tasks.Values)
					{
						if (!task.IsFinished())
						{
							allDone = false;
							job.eventHandler.Handle(new TaskEvent(task.GetID(), TaskEventType.TKill));
						}
					}
					//If all tasks are already done, we should go directly to FAIL_ABORT
					if (allDone)
					{
						job.eventHandler.Handle(new CommitterJobAbortEvent(job.jobId, job.jobContext, JobStatus.State
							.Failed));
						return JobStateInternal.FailAbort;
					}
					//Set max timeout to wait for the tasks to get killed
					job.failWaitTriggerScheduledFuture = job.executor.Schedule(new JobImpl.TaskCompletedTransition.TriggerScheduledFuture
						(job, new JobEvent(job.GetID(), JobEventType.JobFailWaitTimedout)), job.conf.GetInt
						(MRJobConfig.MrAmCommitterCancelTimeoutMs, MRJobConfig.DefaultMrAmCommitterCancelTimeoutMs
						), TimeUnit.Milliseconds);
					return JobStateInternal.FailWait;
				}
				return job.CheckReadyForCommit();
			}

			private void TaskSucceeded(JobImpl job, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task
				 task)
			{
				if (task.GetType() == TaskType.Map)
				{
					job.succeededMapTaskCount++;
				}
				else
				{
					job.succeededReduceTaskCount++;
				}
				job.metrics.CompletedTask(task);
			}

			private void TaskFailed(JobImpl job, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task 
				task)
			{
				if (task.GetType() == TaskType.Map)
				{
					job.failedMapTaskCount++;
				}
				else
				{
					if (task.GetType() == TaskType.Reduce)
					{
						job.failedReduceTaskCount++;
					}
				}
				job.AddDiagnostic("Task failed " + task.GetID());
				job.metrics.FailedTask(task);
			}

			private void TaskKilled(JobImpl job, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task 
				task)
			{
				if (task.GetType() == TaskType.Map)
				{
					job.killedMapTaskCount++;
				}
				else
				{
					if (task.GetType() == TaskType.Reduce)
					{
						job.killedReduceTaskCount++;
					}
				}
				job.metrics.KilledTask(task);
			}
		}

		private class JobNoTasksCompletedTransition : MultipleArcTransition<JobImpl, JobEvent
			, JobStateInternal>
		{
			// Transition class for handling jobs with no tasks
			public virtual JobStateInternal Transition(JobImpl job, JobEvent @event)
			{
				return job.CheckReadyForCommit();
			}
		}

		private class CommitSucceededTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.LogJobHistoryFinishedEvent();
				job.Finished(JobStateInternal.Succeeded);
			}
		}

		private class CommitFailedTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				JobCommitFailedEvent jcfe = (JobCommitFailedEvent)@event;
				job.AddDiagnostic("Job commit failed: " + jcfe.GetMessage());
				job.eventHandler.Handle(new CommitterJobAbortEvent(job.jobId, job.jobContext, JobStatus.State
					.Failed));
			}
		}

		private class KilledDuringCommitTransition : SingleArcTransition<JobImpl, JobEvent
			>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.SetFinishTime();
				job.eventHandler.Handle(new CommitterJobAbortEvent(job.jobId, job.jobContext, JobStatus.State
					.Killed));
			}
		}

		private class KilledDuringAbortTransition : SingleArcTransition<JobImpl, JobEvent
			>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.UnsuccessfulFinish(JobStateInternal.Killed);
			}
		}

		private class MapTaskRescheduledTransition : SingleArcTransition<JobImpl, JobEvent
			>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				//succeeded map task is restarted back
				job.completedTaskCount--;
				job.succeededMapTaskCount--;
			}
		}

		private class KillWaitTaskCompletedTransition : JobImpl.TaskCompletedTransition
		{
			protected internal override JobStateInternal CheckJobAfterTaskCompletion(JobImpl 
				job)
			{
				if (job.completedTaskCount == job.tasks.Count)
				{
					job.SetFinishTime();
					job.eventHandler.Handle(new CommitterJobAbortEvent(job.jobId, job.jobContext, JobStatus.State
						.Killed));
					return JobStateInternal.KillAbort;
				}
				//return the current state, Job not finished yet
				return job.GetInternalState();
			}
		}

		protected internal virtual void AddDiagnostic(string diag)
		{
			diagnostics.AddItem(diag);
		}

		private class DiagnosticsUpdateTransition : SingleArcTransition<JobImpl, JobEvent
			>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				job.AddDiagnostic(((JobDiagnosticsUpdateEvent)@event).GetDiagnosticUpdate());
			}
		}

		private class CounterUpdateTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				JobCounterUpdateEvent jce = (JobCounterUpdateEvent)@event;
				foreach (JobCounterUpdateEvent.CounterIncrementalUpdate ci in jce.GetCounterUpdates
					())
				{
					job.jobCounters.FindCounter(ci.GetCounterKey()).Increment(ci.GetIncrementValue());
				}
			}
		}

		private class UpdatedNodesTransition : SingleArcTransition<JobImpl, JobEvent>
		{
			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				JobUpdatedNodesEvent updateEvent = (JobUpdatedNodesEvent)@event;
				foreach (NodeReport nr in updateEvent.GetUpdatedNodes())
				{
					NodeState nodeState = nr.GetNodeState();
					if (nodeState.IsUnusable())
					{
						// act on the updates
						job.ActOnUnusableNode(nr.GetNodeId(), nodeState);
					}
				}
			}
		}

		private class InternalTerminationTransition : SingleArcTransition<JobImpl, JobEvent
			>
		{
			internal JobStateInternal terminationState = null;

			internal string jobHistoryString = null;

			public InternalTerminationTransition(JobStateInternal stateInternal, string jobHistoryString
				)
			{
				this.terminationState = stateInternal;
				//mostly a hack for jbhistoryserver
				this.jobHistoryString = jobHistoryString;
			}

			public virtual void Transition(JobImpl job, JobEvent @event)
			{
				//TODO Is this JH event required.
				job.SetFinishTime();
				JobUnsuccessfulCompletionEvent failedEvent = new JobUnsuccessfulCompletionEvent(job
					.oldJobId, job.finishTime, 0, 0, jobHistoryString, job.diagnostics);
				job.eventHandler.Handle(new JobHistoryEvent(job.jobId, failedEvent));
				job.Finished(terminationState);
			}
		}

		private class InternalErrorTransition : JobImpl.InternalTerminationTransition
		{
			public InternalErrorTransition()
				: base(JobStateInternal.Error, JobStateInternal.Error.ToString())
			{
			}
		}

		private class InternalRebootTransition : JobImpl.InternalTerminationTransition
		{
			public InternalRebootTransition()
				: base(JobStateInternal.Reboot, JobStateInternal.Error.ToString())
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Configuration LoadConfFile()
		{
			Path confPath = GetConfFile();
			FileContext fc = FileContext.GetFileContext(confPath.ToUri(), conf);
			Configuration jobConf = new Configuration(false);
			jobConf.AddResource(fc.Open(confPath), confPath.ToString());
			return jobConf;
		}

		public virtual float GetMaxAllowedFetchFailuresFraction()
		{
			return maxAllowedFetchFailuresFraction;
		}

		public virtual int GetMaxFetchFailuresNotifications()
		{
			return maxFetchFailuresNotifications;
		}
	}
}
