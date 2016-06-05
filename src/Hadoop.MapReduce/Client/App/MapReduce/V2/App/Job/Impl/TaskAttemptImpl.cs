using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Commit;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Launcher;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.App.Speculate;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.State;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	/// <summary>Implementation of TaskAttempt interface.</summary>
	public abstract class TaskAttemptImpl : TaskAttempt, EventHandler<TaskAttemptEvent
		>
	{
		internal static readonly Counters EmptyCounters = new Counters();

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskAttemptImpl
			));

		private const long MemorySplitsResolution = 1024;

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		protected internal readonly JobConf conf;

		protected internal readonly Path jobFile;

		protected internal readonly int partition;

		protected internal EventHandler eventHandler;

		private readonly TaskAttemptId attemptId;

		private readonly Clock clock;

		private readonly JobID oldJobId;

		private readonly TaskAttemptListener taskAttemptListener;

		private readonly Resource resourceCapability;

		protected internal ICollection<string> dataLocalHosts;

		protected internal ICollection<string> dataLocalRacks;

		private readonly IList<string> diagnostics = new AList<string>();

		private readonly Lock readLock;

		private readonly Lock writeLock;

		private readonly AppContext appContext;

		private Credentials credentials;

		private Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken;

		private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();

		private static string initialClasspath = null;

		private static string initialAppClasspath = null;

		private static string initialHadoopClasspath = null;

		private static object commonContainerSpecLock = new object();

		private static ContainerLaunchContext commonContainerSpec = null;

		private static readonly object classpathLock = new object();

		private long launchTime;

		private long finishTime;

		private WrappedProgressSplitsBlock progressSplitBlock;

		private int shufflePort = -1;

		private string trackerName;

		private int httpPort;

		private Locality locality;

		private Avataar avataar;

		private static readonly TaskAttemptImpl.CleanupContainerTransition CleanupContainerTransition
			 = new TaskAttemptImpl.CleanupContainerTransition();

		private static readonly TaskAttemptImpl.DiagnosticInformationUpdater DiagnosticInformationUpdateTransition
			 = new TaskAttemptImpl.DiagnosticInformationUpdater();

		private static readonly EnumSet<TaskAttemptEventType> FailedKilledStateIgnoredEvents
			 = EnumSet.Of(TaskAttemptEventType.TaKill, TaskAttemptEventType.TaAssigned, TaskAttemptEventType
			.TaContainerCompleted, TaskAttemptEventType.TaUpdate, TaskAttemptEventType.TaContainerLaunched
			, TaskAttemptEventType.TaContainerLaunchFailed, TaskAttemptEventType.TaContainerCleaned
			, TaskAttemptEventType.TaCommitPending, TaskAttemptEventType.TaDone, TaskAttemptEventType
			.TaFailmsg, TaskAttemptEventType.TaTooManyFetchFailure);

		private static readonly StateMachineFactory<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskAttemptImpl
			, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent> stateMachineFactory
			 = new StateMachineFactory<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskAttemptImpl
			, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>(TaskAttemptStateInternal
			.New).AddTransition(TaskAttemptStateInternal.New, TaskAttemptStateInternal.Unassigned
			, TaskAttemptEventType.TaSchedule, new TaskAttemptImpl.RequestContainerTransition
			(false)).AddTransition(TaskAttemptStateInternal.New, TaskAttemptStateInternal.Unassigned
			, TaskAttemptEventType.TaReschedule, new TaskAttemptImpl.RequestContainerTransition
			(true)).AddTransition(TaskAttemptStateInternal.New, TaskAttemptStateInternal.Killed
			, TaskAttemptEventType.TaKill, new TaskAttemptImpl.KilledTransition()).AddTransition
			(TaskAttemptStateInternal.New, TaskAttemptStateInternal.Failed, TaskAttemptEventType
			.TaFailmsg, new TaskAttemptImpl.FailedTransition()).AddTransition(TaskAttemptStateInternal
			.New, EnumSet.Of(TaskAttemptStateInternal.Failed, TaskAttemptStateInternal.Killed
			, TaskAttemptStateInternal.Succeeded), TaskAttemptEventType.TaRecover, new TaskAttemptImpl.RecoverTransition
			()).AddTransition(TaskAttemptStateInternal.New, TaskAttemptStateInternal.New, TaskAttemptEventType
			.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition).AddTransition(TaskAttemptStateInternal
			.Unassigned, TaskAttemptStateInternal.Assigned, TaskAttemptEventType.TaAssigned, 
			new TaskAttemptImpl.ContainerAssignedTransition()).AddTransition(TaskAttemptStateInternal
			.Unassigned, TaskAttemptStateInternal.Killed, TaskAttemptEventType.TaKill, new TaskAttemptImpl.DeallocateContainerTransition
			(TaskAttemptStateInternal.Killed, true)).AddTransition(TaskAttemptStateInternal.
			Unassigned, TaskAttemptStateInternal.Failed, TaskAttemptEventType.TaFailmsg, new 
			TaskAttemptImpl.DeallocateContainerTransition(TaskAttemptStateInternal.Failed, true
			)).AddTransition(TaskAttemptStateInternal.Unassigned, TaskAttemptStateInternal.Unassigned
			, TaskAttemptEventType.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition
			).AddTransition(TaskAttemptStateInternal.Assigned, TaskAttemptStateInternal.Running
			, TaskAttemptEventType.TaContainerLaunched, new TaskAttemptImpl.LaunchedContainerTransition
			()).AddTransition(TaskAttemptStateInternal.Assigned, TaskAttemptStateInternal.Assigned
			, TaskAttemptEventType.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition
			).AddTransition(TaskAttemptStateInternal.Assigned, TaskAttemptStateInternal.Failed
			, TaskAttemptEventType.TaContainerLaunchFailed, new TaskAttemptImpl.DeallocateContainerTransition
			(TaskAttemptStateInternal.Failed, false)).AddTransition(TaskAttemptStateInternal
			.Assigned, TaskAttemptStateInternal.FailContainerCleanup, TaskAttemptEventType.TaContainerCompleted
			, CleanupContainerTransition).AddTransition(TaskAttemptStateInternal.Assigned, TaskAttemptStateInternal
			.KillContainerCleanup, TaskAttemptEventType.TaKill, CleanupContainerTransition).
			AddTransition(TaskAttemptStateInternal.Assigned, TaskAttemptStateInternal.FailContainerCleanup
			, TaskAttemptEventType.TaFailmsg, CleanupContainerTransition).AddTransition(TaskAttemptStateInternal
			.Running, TaskAttemptStateInternal.Running, TaskAttemptEventType.TaUpdate, new TaskAttemptImpl.StatusUpdater
			()).AddTransition(TaskAttemptStateInternal.Running, TaskAttemptStateInternal.Running
			, TaskAttemptEventType.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition
			).AddTransition(TaskAttemptStateInternal.Running, TaskAttemptStateInternal.SuccessContainerCleanup
			, TaskAttemptEventType.TaDone, CleanupContainerTransition).AddTransition(TaskAttemptStateInternal
			.Running, TaskAttemptStateInternal.CommitPending, TaskAttemptEventType.TaCommitPending
			, new TaskAttemptImpl.CommitPendingTransition()).AddTransition(TaskAttemptStateInternal
			.Running, TaskAttemptStateInternal.FailContainerCleanup, TaskAttemptEventType.TaFailmsg
			, CleanupContainerTransition).AddTransition(TaskAttemptStateInternal.Running, TaskAttemptStateInternal
			.FailContainerCleanup, TaskAttemptEventType.TaContainerCompleted, CleanupContainerTransition
			).AddTransition(TaskAttemptStateInternal.Running, TaskAttemptStateInternal.FailContainerCleanup
			, TaskAttemptEventType.TaTimedOut, CleanupContainerTransition).AddTransition(TaskAttemptStateInternal
			.Running, TaskAttemptStateInternal.Killed, TaskAttemptEventType.TaContainerCleaned
			, new TaskAttemptImpl.KilledTransition()).AddTransition(TaskAttemptStateInternal
			.Running, TaskAttemptStateInternal.KillContainerCleanup, TaskAttemptEventType.TaKill
			, CleanupContainerTransition).AddTransition(TaskAttemptStateInternal.CommitPending
			, TaskAttemptStateInternal.CommitPending, TaskAttemptEventType.TaUpdate, new TaskAttemptImpl.StatusUpdater
			()).AddTransition(TaskAttemptStateInternal.CommitPending, TaskAttemptStateInternal
			.CommitPending, TaskAttemptEventType.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition
			).AddTransition(TaskAttemptStateInternal.CommitPending, TaskAttemptStateInternal
			.SuccessContainerCleanup, TaskAttemptEventType.TaDone, CleanupContainerTransition
			).AddTransition(TaskAttemptStateInternal.CommitPending, TaskAttemptStateInternal
			.KillContainerCleanup, TaskAttemptEventType.TaKill, CleanupContainerTransition).
			AddTransition(TaskAttemptStateInternal.CommitPending, TaskAttemptStateInternal.Killed
			, TaskAttemptEventType.TaContainerCleaned, new TaskAttemptImpl.KilledTransition(
			)).AddTransition(TaskAttemptStateInternal.CommitPending, TaskAttemptStateInternal
			.FailContainerCleanup, TaskAttemptEventType.TaFailmsg, CleanupContainerTransition
			).AddTransition(TaskAttemptStateInternal.CommitPending, TaskAttemptStateInternal
			.FailContainerCleanup, TaskAttemptEventType.TaContainerCompleted, CleanupContainerTransition
			).AddTransition(TaskAttemptStateInternal.CommitPending, TaskAttemptStateInternal
			.FailContainerCleanup, TaskAttemptEventType.TaTimedOut, CleanupContainerTransition
			).AddTransition(TaskAttemptStateInternal.CommitPending, TaskAttemptStateInternal
			.CommitPending, TaskAttemptEventType.TaCommitPending).AddTransition(TaskAttemptStateInternal
			.SuccessContainerCleanup, TaskAttemptStateInternal.Succeeded, TaskAttemptEventType
			.TaContainerCleaned, new TaskAttemptImpl.SucceededTransition()).AddTransition(TaskAttemptStateInternal
			.SuccessContainerCleanup, TaskAttemptStateInternal.SuccessContainerCleanup, TaskAttemptEventType
			.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition).AddTransition(TaskAttemptStateInternal
			.SuccessContainerCleanup, TaskAttemptStateInternal.SuccessContainerCleanup, EnumSet
			.Of(TaskAttemptEventType.TaKill, TaskAttemptEventType.TaFailmsg, TaskAttemptEventType
			.TaTimedOut, TaskAttemptEventType.TaContainerCompleted)).AddTransition(TaskAttemptStateInternal
			.FailContainerCleanup, TaskAttemptStateInternal.FailTaskCleanup, TaskAttemptEventType
			.TaContainerCleaned, new TaskAttemptImpl.TaskCleanupTransition()).AddTransition(
			TaskAttemptStateInternal.FailContainerCleanup, TaskAttemptStateInternal.FailContainerCleanup
			, TaskAttemptEventType.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition
			).AddTransition(TaskAttemptStateInternal.FailContainerCleanup, TaskAttemptStateInternal
			.FailContainerCleanup, EnumSet.Of(TaskAttemptEventType.TaKill, TaskAttemptEventType
			.TaContainerCompleted, TaskAttemptEventType.TaUpdate, TaskAttemptEventType.TaCommitPending
			, TaskAttemptEventType.TaContainerLaunched, TaskAttemptEventType.TaContainerLaunchFailed
			, TaskAttemptEventType.TaDone, TaskAttemptEventType.TaFailmsg, TaskAttemptEventType
			.TaTimedOut)).AddTransition(TaskAttemptStateInternal.KillContainerCleanup, TaskAttemptStateInternal
			.KillTaskCleanup, TaskAttemptEventType.TaContainerCleaned, new TaskAttemptImpl.TaskCleanupTransition
			()).AddTransition(TaskAttemptStateInternal.KillContainerCleanup, TaskAttemptStateInternal
			.KillContainerCleanup, TaskAttemptEventType.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition
			).AddTransition(TaskAttemptStateInternal.KillContainerCleanup, TaskAttemptStateInternal
			.KillContainerCleanup, EnumSet.Of(TaskAttemptEventType.TaKill, TaskAttemptEventType
			.TaContainerCompleted, TaskAttemptEventType.TaUpdate, TaskAttemptEventType.TaCommitPending
			, TaskAttemptEventType.TaContainerLaunched, TaskAttemptEventType.TaContainerLaunchFailed
			, TaskAttemptEventType.TaDone, TaskAttemptEventType.TaFailmsg, TaskAttemptEventType
			.TaTimedOut)).AddTransition(TaskAttemptStateInternal.FailTaskCleanup, TaskAttemptStateInternal
			.Failed, TaskAttemptEventType.TaCleanupDone, new TaskAttemptImpl.FailedTransition
			()).AddTransition(TaskAttemptStateInternal.FailTaskCleanup, TaskAttemptStateInternal
			.FailTaskCleanup, TaskAttemptEventType.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition
			).AddTransition(TaskAttemptStateInternal.FailTaskCleanup, TaskAttemptStateInternal
			.FailTaskCleanup, EnumSet.Of(TaskAttemptEventType.TaKill, TaskAttemptEventType.TaContainerCompleted
			, TaskAttemptEventType.TaUpdate, TaskAttemptEventType.TaCommitPending, TaskAttemptEventType
			.TaDone, TaskAttemptEventType.TaFailmsg, TaskAttemptEventType.TaContainerCleaned
			, TaskAttemptEventType.TaContainerLaunched, TaskAttemptEventType.TaContainerLaunchFailed
			)).AddTransition(TaskAttemptStateInternal.KillTaskCleanup, TaskAttemptStateInternal
			.Killed, TaskAttemptEventType.TaCleanupDone, new TaskAttemptImpl.KilledTransition
			()).AddTransition(TaskAttemptStateInternal.KillTaskCleanup, TaskAttemptStateInternal
			.KillTaskCleanup, TaskAttemptEventType.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition
			).AddTransition(TaskAttemptStateInternal.KillTaskCleanup, TaskAttemptStateInternal
			.KillTaskCleanup, EnumSet.Of(TaskAttemptEventType.TaKill, TaskAttemptEventType.TaContainerCompleted
			, TaskAttemptEventType.TaUpdate, TaskAttemptEventType.TaCommitPending, TaskAttemptEventType
			.TaDone, TaskAttemptEventType.TaFailmsg, TaskAttemptEventType.TaContainerCleaned
			, TaskAttemptEventType.TaContainerLaunched, TaskAttemptEventType.TaContainerLaunchFailed
			)).AddTransition(TaskAttemptStateInternal.Succeeded, TaskAttemptStateInternal.Failed
			, TaskAttemptEventType.TaTooManyFetchFailure, new TaskAttemptImpl.TooManyFetchFailureTransition
			()).AddTransition(TaskAttemptStateInternal.Succeeded, EnumSet.Of(TaskAttemptStateInternal
			.Succeeded, TaskAttemptStateInternal.Killed), TaskAttemptEventType.TaKill, new TaskAttemptImpl.KilledAfterSuccessTransition
			()).AddTransition(TaskAttemptStateInternal.Succeeded, TaskAttemptStateInternal.Succeeded
			, TaskAttemptEventType.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition
			).AddTransition(TaskAttemptStateInternal.Succeeded, TaskAttemptStateInternal.Succeeded
			, EnumSet.Of(TaskAttemptEventType.TaFailmsg, TaskAttemptEventType.TaContainerCleaned
			, TaskAttemptEventType.TaContainerCompleted)).AddTransition(TaskAttemptStateInternal
			.Failed, TaskAttemptStateInternal.Failed, TaskAttemptEventType.TaDiagnosticsUpdate
			, DiagnosticInformationUpdateTransition).AddTransition(TaskAttemptStateInternal.
			Failed, TaskAttemptStateInternal.Failed, FailedKilledStateIgnoredEvents).AddTransition
			(TaskAttemptStateInternal.Killed, TaskAttemptStateInternal.Killed, TaskAttemptEventType
			.TaDiagnosticsUpdate, DiagnosticInformationUpdateTransition).AddTransition(TaskAttemptStateInternal
			.Killed, TaskAttemptStateInternal.Killed, FailedKilledStateIgnoredEvents).InstallTopology
			();

		private readonly StateMachine<TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent
			> stateMachine;

		[VisibleForTesting]
		public Container container;

		private string nodeRackName;

		private WrappedJvmID jvmID;

		private Task remoteTask;

		private TaskAttemptStatusUpdateEvent.TaskAttemptStatus reportedStatus;

		private static readonly string LineSeparator = Runtime.GetProperty("line.separator"
			);

		public TaskAttemptImpl(TaskId taskId, int i, EventHandler eventHandler, TaskAttemptListener
			 taskAttemptListener, Path jobFile, int partition, JobConf conf, string[] dataLocalHosts
			, Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken, Credentials
			 credentials, Clock clock, AppContext appContext)
		{
			//TODO Make configurable?
			// Container launch events can arrive late
			// Transitions from the NEW state.
			// Transitions from the UNASSIGNED state.
			// Transitions from the ASSIGNED state.
			// Transitions from RUNNING state.
			// If no commit is required, task directly goes to success
			// If commit is required, task goes through commit pending state.
			// Failure handling while RUNNING
			//for handling container exit without sending the done or fail msg
			// Timeout handling while RUNNING
			// if container killed by AM shutting down
			// Kill handling
			// Transitions from COMMIT_PENDING state
			// if container killed by AM shutting down
			// AM is likely to receive duplicate TA_COMMIT_PENDINGs as the task attempt
			// will re-send the commit message until it doesn't encounter any
			// IOException and succeeds in delivering the commit message.
			// Ignoring the duplicate commit message is a short-term fix. In long term,
			// we need to make use of retry cache to help this and other MR protocol
			// APIs that can be considered as @AtMostOnce.
			// Transitions from SUCCESS_CONTAINER_CLEANUP state
			// kill and cleanup the container
			// Ignore-able events
			// Transitions from FAIL_CONTAINER_CLEANUP state.
			// Ignore-able events
			// Container launch events can arrive late
			// Transitions from KILL_CONTAINER_CLEANUP
			// Ignore-able events
			// Transitions from FAIL_TASK_CLEANUP
			// run the task cleanup
			// Ignore-able events
			// Container launch events can arrive late
			// Transitions from KILL_TASK_CLEANUP
			// Ignore-able events
			// Container launch events can arrive late
			// Transitions from SUCCEEDED
			//only possible for map attempts
			// Ignore-able events for SUCCEEDED state
			// Transitions from FAILED state
			// Ignore-able events for FAILED state
			// Transitions from KILLED state
			// Ignore-able events for KILLED state
			// create the topology tables
			//this takes good amount of memory ~ 30KB. Instantiate it lazily
			//and make it null once task is launched.
			//this is the last status reported by the REMOTE running attempt
			oldJobId = TypeConverter.FromYarn(taskId.GetJobId());
			this.conf = conf;
			this.clock = clock;
			attemptId = recordFactory.NewRecordInstance<TaskAttemptId>();
			attemptId.SetTaskId(taskId);
			attemptId.SetId(i);
			this.taskAttemptListener = taskAttemptListener;
			this.appContext = appContext;
			// Initialize reportedStatus
			reportedStatus = new TaskAttemptStatusUpdateEvent.TaskAttemptStatus();
			InitTaskAttemptStatus(reportedStatus);
			ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
			readLock = readWriteLock.ReadLock();
			writeLock = readWriteLock.WriteLock();
			this.credentials = credentials;
			this.jobToken = jobToken;
			this.eventHandler = eventHandler;
			this.jobFile = jobFile;
			this.partition = partition;
			//TODO:create the resource reqt for this Task attempt
			this.resourceCapability = recordFactory.NewRecordInstance<Resource>();
			this.resourceCapability.SetMemory(GetMemoryRequired(conf, taskId.GetTaskType()));
			this.resourceCapability.SetVirtualCores(GetCpuRequired(conf, taskId.GetTaskType()
				));
			this.dataLocalHosts = ResolveHosts(dataLocalHosts);
			RackResolver.Init(conf);
			this.dataLocalRacks = new HashSet<string>();
			foreach (string host in this.dataLocalHosts)
			{
				this.dataLocalRacks.AddItem(RackResolver.Resolve(host).GetNetworkLocation());
			}
			locality = Locality.OffSwitch;
			avataar = Avataar.Virgin;
			// This "this leak" is okay because the retained pointer is in an
			//  instance variable.
			stateMachine = stateMachineFactory.Make(this);
		}

		private int GetMemoryRequired(Configuration conf, TaskType taskType)
		{
			int memory = 1024;
			if (taskType == TaskType.Map)
			{
				memory = conf.GetInt(MRJobConfig.MapMemoryMb, MRJobConfig.DefaultMapMemoryMb);
			}
			else
			{
				if (taskType == TaskType.Reduce)
				{
					memory = conf.GetInt(MRJobConfig.ReduceMemoryMb, MRJobConfig.DefaultReduceMemoryMb
						);
				}
			}
			return memory;
		}

		private int GetCpuRequired(Configuration conf, TaskType taskType)
		{
			int vcores = 1;
			if (taskType == TaskType.Map)
			{
				vcores = conf.GetInt(MRJobConfig.MapCpuVcores, MRJobConfig.DefaultMapCpuVcores);
			}
			else
			{
				if (taskType == TaskType.Reduce)
				{
					vcores = conf.GetInt(MRJobConfig.ReduceCpuVcores, MRJobConfig.DefaultReduceCpuVcores
						);
				}
			}
			return vcores;
		}

		/// <summary>
		/// Create a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.LocalResource"/>
		/// record with all the given parameters.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static LocalResource CreateLocalResource(FileSystem fc, Path file, LocalResourceType
			 type, LocalResourceVisibility visibility)
		{
			FileStatus fstat = fc.GetFileStatus(file);
			URL resourceURL = ConverterUtils.GetYarnUrlFromPath(fc.ResolvePath(fstat.GetPath(
				)));
			long resourceSize = fstat.GetLen();
			long resourceModificationTime = fstat.GetModificationTime();
			return LocalResource.NewInstance(resourceURL, type, visibility, resourceSize, resourceModificationTime
				);
		}

		/// <summary>
		/// Lock this on initialClasspath so that there is only one fork in the AM for
		/// getting the initial class-path.
		/// </summary>
		/// <remarks>
		/// Lock this on initialClasspath so that there is only one fork in the AM for
		/// getting the initial class-path. TODO: We already construct
		/// a parent CLC and use it for all the containers, so this should go away
		/// once the mr-generated-classpath stuff is gone.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static string GetInitialClasspath(Configuration conf)
		{
			lock (classpathLock)
			{
				if (initialClasspathFlag.Get())
				{
					return initialClasspath;
				}
				IDictionary<string, string> env = new Dictionary<string, string>();
				MRApps.SetClasspath(env, conf);
				initialClasspath = env[ApplicationConstants.Environment.Classpath.ToString()];
				initialAppClasspath = env[ApplicationConstants.Environment.AppClasspath.ToString(
					)];
				initialHadoopClasspath = env[ApplicationConstants.Environment.HadoopClasspath.ToString
					()];
				initialClasspathFlag.Set(true);
				return initialClasspath;
			}
		}

		/// <summary>
		/// Create the common
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext"/>
		/// for all attempts.
		/// </summary>
		/// <param name="applicationACLs"></param>
		private static ContainerLaunchContext CreateCommonContainerLaunchContext(IDictionary
			<ApplicationAccessType, string> applicationACLs, Configuration conf, Org.Apache.Hadoop.Security.Token.Token
			<JobTokenIdentifier> jobToken, JobID oldJobId, Credentials credentials)
		{
			// Application resources
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			// Application environment
			IDictionary<string, string> environment = new Dictionary<string, string>();
			// Service data
			IDictionary<string, ByteBuffer> serviceData = new Dictionary<string, ByteBuffer>(
				);
			// Tokens
			ByteBuffer taskCredentialsBuffer = ByteBuffer.Wrap(new byte[] {  });
			try
			{
				FileSystem remoteFS = FileSystem.Get(conf);
				// //////////// Set up JobJar to be localized properly on the remote NM.
				string jobJar = conf.Get(MRJobConfig.Jar);
				if (jobJar != null)
				{
					Path jobJarPath = new Path(jobJar);
					FileSystem jobJarFs = FileSystem.Get(jobJarPath.ToUri(), conf);
					Path remoteJobJar = jobJarPath.MakeQualified(jobJarFs.GetUri(), jobJarFs.GetWorkingDirectory
						());
					LocalResource rc = CreateLocalResource(jobJarFs, remoteJobJar, LocalResourceType.
						Pattern, LocalResourceVisibility.Application);
					string pattern = conf.GetPattern(JobContext.JarUnpackPattern, JobConf.UnpackJarPatternDefault
						).Pattern();
					rc.SetPattern(pattern);
					localResources[MRJobConfig.JobJar] = rc;
					Log.Info("The job-jar file on the remote FS is " + remoteJobJar.ToUri().ToASCIIString
						());
				}
				else
				{
					// Job jar may be null. For e.g, for pipes, the job jar is the hadoop
					// mapreduce jar itself which is already on the classpath.
					Log.Info("Job jar is not present. " + "Not adding any jar to the list of resources."
						);
				}
				// //////////// End of JobJar setup
				// //////////// Set up JobConf to be localized properly on the remote NM.
				Path path = MRApps.GetStagingAreaDir(conf, UserGroupInformation.GetCurrentUser().
					GetShortUserName());
				Path remoteJobSubmitDir = new Path(path, oldJobId.ToString());
				Path remoteJobConfPath = new Path(remoteJobSubmitDir, MRJobConfig.JobConfFile);
				localResources[MRJobConfig.JobConfFile] = CreateLocalResource(remoteFS, remoteJobConfPath
					, LocalResourceType.File, LocalResourceVisibility.Application);
				Log.Info("The job-conf file on the remote FS is " + remoteJobConfPath.ToUri().ToASCIIString
					());
				// //////////// End of JobConf setup
				// Setup DistributedCache
				MRApps.SetupDistributedCache(conf, localResources);
				// Setup up task credentials buffer
				Log.Info("Adding #" + credentials.NumberOfTokens() + " tokens and #" + credentials
					.NumberOfSecretKeys() + " secret keys for NM use for launching container");
				Credentials taskCredentials = new Credentials(credentials);
				// LocalStorageToken is needed irrespective of whether security is enabled
				// or not.
				TokenCache.SetJobToken(jobToken, taskCredentials);
				DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
				Log.Info("Size of containertokens_dob is " + taskCredentials.NumberOfTokens());
				taskCredentials.WriteTokenStorageToStream(containerTokens_dob);
				taskCredentialsBuffer = ByteBuffer.Wrap(containerTokens_dob.GetData(), 0, containerTokens_dob
					.GetLength());
				// Add shuffle secret key
				// The secret key is converted to a JobToken to preserve backwards
				// compatibility with an older ShuffleHandler running on an NM.
				Log.Info("Putting shuffle token in serviceData");
				byte[] shuffleSecret = TokenCache.GetShuffleSecretKey(credentials);
				if (shuffleSecret == null)
				{
					Log.Warn("Cannot locate shuffle secret in credentials." + " Using job token as shuffle secret."
						);
					shuffleSecret = jobToken.GetPassword();
				}
				Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> shuffleToken = new Org.Apache.Hadoop.Security.Token.Token
					<JobTokenIdentifier>(jobToken.GetIdentifier(), shuffleSecret, jobToken.GetKind()
					, jobToken.GetService());
				serviceData[ShuffleHandler.MapreduceShuffleServiceid] = ShuffleHandler.SerializeServiceData
					(shuffleToken);
				// add external shuffle-providers - if any
				ICollection<string> shuffleProviders = conf.GetStringCollection(MRJobConfig.MapreduceJobShuffleProviderServices
					);
				if (!shuffleProviders.IsEmpty())
				{
					ICollection<string> auxNames = conf.GetStringCollection(YarnConfiguration.NmAuxServices
						);
					foreach (string shuffleProvider in shuffleProviders)
					{
						if (shuffleProvider.Equals(ShuffleHandler.MapreduceShuffleServiceid))
						{
							continue;
						}
						// skip built-in shuffle-provider that was already inserted with shuffle secret key
						if (auxNames.Contains(shuffleProvider))
						{
							Log.Info("Adding ShuffleProvider Service: " + shuffleProvider + " to serviceData"
								);
							// This only serves for INIT_APP notifications
							// The shuffle service needs to be able to work with the host:port information provided by the AM
							// (i.e. shuffle services which require custom location / other configuration are not supported)
							serviceData[shuffleProvider] = ByteBuffer.Allocate(0);
						}
						else
						{
							throw new YarnRuntimeException("ShuffleProvider Service: " + shuffleProvider + " was NOT found in the list of aux-services that are available in this NM."
								 + " You may need to specify this ShuffleProvider as an aux-service in your yarn-site.xml"
								);
						}
					}
				}
				MRApps.AddToEnvironment(environment, ApplicationConstants.Environment.Classpath.ToString
					(), GetInitialClasspath(conf), conf);
				if (initialHadoopClasspath != null)
				{
					MRApps.AddToEnvironment(environment, ApplicationConstants.Environment.HadoopClasspath
						.ToString(), initialHadoopClasspath, conf);
				}
				if (initialAppClasspath != null)
				{
					MRApps.AddToEnvironment(environment, ApplicationConstants.Environment.AppClasspath
						.ToString(), initialAppClasspath, conf);
				}
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
			// Shell
			environment[ApplicationConstants.Environment.Shell.ToString()] = conf.Get(MRJobConfig
				.MapredAdminUserShell, MRJobConfig.DefaultShell);
			// Add pwd to LD_LIBRARY_PATH, add this before adding anything else
			MRApps.AddToEnvironment(environment, ApplicationConstants.Environment.LdLibraryPath
				.ToString(), MRApps.CrossPlatformifyMREnv(conf, ApplicationConstants.Environment
				.Pwd), conf);
			// Add the env variables passed by the admin
			MRApps.SetEnvFromInputString(environment, conf.Get(MRJobConfig.MapredAdminUserEnv
				, MRJobConfig.DefaultMapredAdminUserEnv), conf);
			// Construct the actual Container
			// The null fields are per-container and will be constructed for each
			// container separately.
			ContainerLaunchContext container = ContainerLaunchContext.NewInstance(localResources
				, environment, null, serviceData, taskCredentialsBuffer, applicationACLs);
			return container;
		}

		internal static ContainerLaunchContext CreateContainerLaunchContext(IDictionary<ApplicationAccessType
			, string> applicationACLs, Configuration conf, Org.Apache.Hadoop.Security.Token.Token
			<JobTokenIdentifier> jobToken, Task remoteTask, JobID oldJobId, WrappedJvmID jvmID
			, TaskAttemptListener taskAttemptListener, Credentials credentials)
		{
			lock (commonContainerSpecLock)
			{
				if (commonContainerSpec == null)
				{
					commonContainerSpec = CreateCommonContainerLaunchContext(applicationACLs, conf, jobToken
						, oldJobId, credentials);
				}
			}
			// Fill in the fields needed per-container that are missing in the common
			// spec.
			bool userClassesTakesPrecedence = conf.GetBoolean(MRJobConfig.MapreduceJobUserClasspathFirst
				, false);
			// Setup environment by cloning from common env.
			IDictionary<string, string> env = commonContainerSpec.GetEnvironment();
			IDictionary<string, string> myEnv = new Dictionary<string, string>(env.Count);
			myEnv.PutAll(env);
			if (userClassesTakesPrecedence)
			{
				myEnv[ApplicationConstants.Environment.ClasspathPrependDistcache.ToString()] = "true";
			}
			MapReduceChildJVM.SetVMEnv(myEnv, remoteTask);
			// Set up the launch command
			IList<string> commands = MapReduceChildJVM.GetVMCommand(taskAttemptListener.GetAddress
				(), remoteTask, jvmID);
			// Duplicate the ByteBuffers for access by multiple containers.
			IDictionary<string, ByteBuffer> myServiceData = new Dictionary<string, ByteBuffer
				>();
			foreach (KeyValuePair<string, ByteBuffer> entry in commonContainerSpec.GetServiceData
				())
			{
				myServiceData[entry.Key] = entry.Value.Duplicate();
			}
			// Construct the actual Container
			ContainerLaunchContext container = ContainerLaunchContext.NewInstance(commonContainerSpec
				.GetLocalResources(), myEnv, commands, myServiceData, commonContainerSpec.GetTokens
				().Duplicate(), applicationACLs);
			return container;
		}

		public virtual ContainerId GetAssignedContainerID()
		{
			readLock.Lock();
			try
			{
				return container == null ? null : container.GetId();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual string GetAssignedContainerMgrAddress()
		{
			readLock.Lock();
			try
			{
				return container == null ? null : StringInterner.WeakIntern(container.GetNodeId()
					.ToString());
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual long GetLaunchTime()
		{
			readLock.Lock();
			try
			{
				return launchTime;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual long GetFinishTime()
		{
			readLock.Lock();
			try
			{
				return finishTime;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual long GetShuffleFinishTime()
		{
			readLock.Lock();
			try
			{
				return this.reportedStatus.shuffleFinishTime;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual long GetSortFinishTime()
		{
			readLock.Lock();
			try
			{
				return this.reportedStatus.sortFinishTime;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual int GetShufflePort()
		{
			readLock.Lock();
			try
			{
				return shufflePort;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual NodeId GetNodeId()
		{
			readLock.Lock();
			try
			{
				return container == null ? null : container.GetNodeId();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <summary>If container Assigned then return the node's address, otherwise null.</summary>
		public virtual string GetNodeHttpAddress()
		{
			readLock.Lock();
			try
			{
				return container == null ? null : container.GetNodeHttpAddress();
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <summary>If container Assigned then return the node's rackname, otherwise null.</summary>
		public virtual string GetNodeRackName()
		{
			this.readLock.Lock();
			try
			{
				return this.nodeRackName;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		protected internal abstract Task CreateRemoteTask();

		public virtual TaskAttemptId GetID()
		{
			return attemptId;
		}

		public virtual bool IsFinished()
		{
			readLock.Lock();
			try
			{
				// TODO: Use stateMachine level method?
				return (GetInternalState() == TaskAttemptStateInternal.Succeeded || GetInternalState
					() == TaskAttemptStateInternal.Failed || GetInternalState() == TaskAttemptStateInternal
					.Killed);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual TaskAttemptReport GetReport()
		{
			TaskAttemptReport result = recordFactory.NewRecordInstance<TaskAttemptReport>();
			readLock.Lock();
			try
			{
				result.SetTaskAttemptId(attemptId);
				//take the LOCAL state of attempt
				//DO NOT take from reportedStatus
				result.SetTaskAttemptState(GetState());
				result.SetProgress(reportedStatus.progress);
				result.SetStartTime(launchTime);
				result.SetFinishTime(finishTime);
				result.SetShuffleFinishTime(this.reportedStatus.shuffleFinishTime);
				result.SetDiagnosticInfo(StringUtils.Join(LineSeparator, GetDiagnostics()));
				result.SetPhase(reportedStatus.phase);
				result.SetStateString(reportedStatus.stateString);
				result.SetCounters(TypeConverter.ToYarn(GetCounters()));
				result.SetContainerId(this.GetAssignedContainerID());
				result.SetNodeManagerHost(trackerName);
				result.SetNodeManagerHttpPort(httpPort);
				if (this.container != null)
				{
					result.SetNodeManagerPort(this.container.GetNodeId().GetPort());
				}
				return result;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual IList<string> GetDiagnostics()
		{
			IList<string> result = new AList<string>();
			readLock.Lock();
			try
			{
				Sharpen.Collections.AddAll(result, diagnostics);
				return result;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual Counters GetCounters()
		{
			readLock.Lock();
			try
			{
				Counters counters = reportedStatus.counters;
				if (counters == null)
				{
					counters = EmptyCounters;
				}
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
				return reportedStatus.progress;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual Phase GetPhase()
		{
			readLock.Lock();
			try
			{
				return reportedStatus.phase;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual TaskAttemptState GetState()
		{
			readLock.Lock();
			try
			{
				return GetExternalState(stateMachine.GetCurrentState());
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual void Handle(TaskAttemptEvent @event)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Processing " + @event.GetTaskAttemptID() + " of type " + @event.GetType
					());
			}
			writeLock.Lock();
			try
			{
				TaskAttemptStateInternal oldState = GetInternalState();
				try
				{
					stateMachine.DoTransition(@event.GetType(), @event);
				}
				catch (InvalidStateTransitonException e)
				{
					Log.Error("Can't handle this event at current state for " + this.attemptId, e);
					eventHandler.Handle(new JobDiagnosticsUpdateEvent(this.attemptId.GetTaskId().GetJobId
						(), "Invalid event " + @event.GetType() + " on TaskAttempt " + this.attemptId));
					eventHandler.Handle(new JobEvent(this.attemptId.GetTaskId().GetJobId(), JobEventType
						.InternalError));
				}
				if (oldState != GetInternalState())
				{
					Log.Info(attemptId + " TaskAttempt Transitioned from " + oldState + " to " + GetInternalState
						());
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		[VisibleForTesting]
		public virtual TaskAttemptStateInternal GetInternalState()
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

		public virtual Locality GetLocality()
		{
			return locality;
		}

		public virtual void SetLocality(Locality locality)
		{
			this.locality = locality;
		}

		public virtual Avataar GetAvataar()
		{
			return avataar;
		}

		public virtual void SetAvataar(Avataar avataar)
		{
			this.avataar = avataar;
		}

		public virtual TaskAttemptStateInternal Recover(JobHistoryParser.TaskAttemptInfo 
			taInfo, OutputCommitter committer, bool recoverOutput)
		{
			ContainerId containerId = taInfo.GetContainerId();
			NodeId containerNodeId = ConverterUtils.ToNodeId(taInfo.GetHostname() + ":" + taInfo
				.GetPort());
			string nodeHttpAddress = StringInterner.WeakIntern(taInfo.GetHostname() + ":" + taInfo
				.GetHttpPort());
			// Resource/Priority/Tokens are only needed while launching the container on
			// an NM, these are already completed tasks, so setting them to null
			container = Container.NewInstance(containerId, containerNodeId, nodeHttpAddress, 
				null, null, null);
			ComputeRackAndLocality();
			launchTime = taInfo.GetStartTime();
			finishTime = (taInfo.GetFinishTime() != -1) ? taInfo.GetFinishTime() : clock.GetTime
				();
			shufflePort = taInfo.GetShufflePort();
			trackerName = taInfo.GetHostname();
			httpPort = taInfo.GetHttpPort();
			SendLaunchedEvents();
			reportedStatus.id = attemptId;
			reportedStatus.progress = 1.0f;
			reportedStatus.counters = taInfo.GetCounters();
			reportedStatus.stateString = taInfo.GetState();
			reportedStatus.phase = Phase.Cleanup;
			reportedStatus.mapFinishTime = taInfo.GetMapFinishTime();
			reportedStatus.shuffleFinishTime = taInfo.GetShuffleFinishTime();
			reportedStatus.sortFinishTime = taInfo.GetSortFinishTime();
			AddDiagnosticInfo(taInfo.GetError());
			bool needToClean = false;
			string recoveredState = taInfo.GetTaskStatus();
			if (recoverOutput && TaskAttemptState.Succeeded.ToString().Equals(recoveredState))
			{
				TaskAttemptContext tac = new TaskAttemptContextImpl(conf, TypeConverter.FromYarn(
					attemptId));
				try
				{
					committer.RecoverTask(tac);
					Log.Info("Recovered output from task attempt " + attemptId);
				}
				catch (Exception e)
				{
					Log.Error("Unable to recover task attempt " + attemptId, e);
					Log.Info("Task attempt " + attemptId + " will be recovered as KILLED");
					recoveredState = TaskAttemptState.Killed.ToString();
					needToClean = true;
				}
			}
			TaskAttemptStateInternal attemptState;
			if (TaskAttemptState.Succeeded.ToString().Equals(recoveredState))
			{
				attemptState = TaskAttemptStateInternal.Succeeded;
				reportedStatus.taskState = TaskAttemptState.Succeeded;
				eventHandler.Handle(CreateJobCounterUpdateEventTASucceeded(this));
				LogAttemptFinishedEvent(attemptState);
			}
			else
			{
				if (TaskAttemptState.Failed.ToString().Equals(recoveredState))
				{
					attemptState = TaskAttemptStateInternal.Failed;
					reportedStatus.taskState = TaskAttemptState.Failed;
					eventHandler.Handle(CreateJobCounterUpdateEventTAFailed(this, false));
					TaskAttemptUnsuccessfulCompletionEvent tauce = CreateTaskAttemptUnsuccessfulCompletionEvent
						(this, TaskAttemptStateInternal.Failed);
					eventHandler.Handle(new JobHistoryEvent(attemptId.GetTaskId().GetJobId(), tauce));
				}
				else
				{
					if (!TaskAttemptState.Killed.ToString().Equals(recoveredState))
					{
						if (recoveredState.ToString().IsEmpty())
						{
							Log.Info("TaskAttempt" + attemptId + " had not completed, recovering as KILLED");
						}
						else
						{
							Log.Warn("TaskAttempt " + attemptId + " found in unexpected state " + recoveredState
								 + ", recovering as KILLED");
						}
						AddDiagnosticInfo("Killed during application recovery");
						needToClean = true;
					}
					attemptState = TaskAttemptStateInternal.Killed;
					reportedStatus.taskState = TaskAttemptState.Killed;
					eventHandler.Handle(CreateJobCounterUpdateEventTAKilled(this, false));
					TaskAttemptUnsuccessfulCompletionEvent tauce = CreateTaskAttemptUnsuccessfulCompletionEvent
						(this, TaskAttemptStateInternal.Killed);
					eventHandler.Handle(new JobHistoryEvent(attemptId.GetTaskId().GetJobId(), tauce));
				}
			}
			if (needToClean)
			{
				TaskAttemptContext tac = new TaskAttemptContextImpl(conf, TypeConverter.FromYarn(
					attemptId));
				try
				{
					committer.AbortTask(tac);
				}
				catch (Exception e)
				{
					Log.Warn("Task cleanup failed for attempt " + attemptId, e);
				}
			}
			return attemptState;
		}

		private static TaskAttemptState GetExternalState(TaskAttemptStateInternal smState
			)
		{
			switch (smState)
			{
				case TaskAttemptStateInternal.Assigned:
				case TaskAttemptStateInternal.Unassigned:
				{
					return TaskAttemptState.Starting;
				}

				case TaskAttemptStateInternal.CommitPending:
				{
					return TaskAttemptState.CommitPending;
				}

				case TaskAttemptStateInternal.Failed:
				{
					return TaskAttemptState.Failed;
				}

				case TaskAttemptStateInternal.Killed:
				{
					return TaskAttemptState.Killed;
				}

				case TaskAttemptStateInternal.FailContainerCleanup:
				case TaskAttemptStateInternal.FailTaskCleanup:
				case TaskAttemptStateInternal.KillContainerCleanup:
				case TaskAttemptStateInternal.KillTaskCleanup:
				case TaskAttemptStateInternal.SuccessContainerCleanup:
				case TaskAttemptStateInternal.Running:
				{
					// All CLEANUP states considered as RUNNING since events have not gone out
					// to the Task yet. May be possible to consider them as a Finished state.
					return TaskAttemptState.Running;
				}

				case TaskAttemptStateInternal.New:
				{
					return TaskAttemptState.New;
				}

				case TaskAttemptStateInternal.Succeeded:
				{
					return TaskAttemptState.Succeeded;
				}

				default:
				{
					throw new YarnRuntimeException("Attempt to convert invalid " + "stateMachineTaskAttemptState to externalTaskAttemptState: "
						 + smState);
				}
			}
		}

		//always called in write lock
		private void SetFinishTime()
		{
			//set the finish time only if launch time is set
			if (launchTime != 0)
			{
				finishTime = clock.GetTime();
			}
		}

		private void ComputeRackAndLocality()
		{
			NodeId containerNodeId = container.GetNodeId();
			nodeRackName = RackResolver.Resolve(containerNodeId.GetHost()).GetNetworkLocation
				();
			locality = Locality.OffSwitch;
			if (dataLocalHosts.Count > 0)
			{
				string cHost = ResolveHost(containerNodeId.GetHost());
				if (dataLocalHosts.Contains(cHost))
				{
					locality = Locality.NodeLocal;
				}
			}
			if (locality == Locality.OffSwitch)
			{
				if (dataLocalRacks.Contains(nodeRackName))
				{
					locality = Locality.RackLocal;
				}
			}
		}

		private static void UpdateMillisCounters(JobCounterUpdateEvent jce, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskAttemptImpl
			 taskAttempt)
		{
			TaskType taskType = taskAttempt.GetID().GetTaskId().GetTaskType();
			long duration = (taskAttempt.GetFinishTime() - taskAttempt.GetLaunchTime());
			int mbRequired = taskAttempt.GetMemoryRequired(taskAttempt.conf, taskType);
			int vcoresRequired = taskAttempt.GetCpuRequired(taskAttempt.conf, taskType);
			int minSlotMemSize = taskAttempt.conf.GetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb
				, YarnConfiguration.DefaultRmSchedulerMinimumAllocationMb);
			int simSlotsRequired = minSlotMemSize == 0 ? 0 : (int)Math.Ceil((float)mbRequired
				 / minSlotMemSize);
			if (taskType == TaskType.Map)
			{
				jce.AddCounterUpdate(JobCounter.SlotsMillisMaps, simSlotsRequired * duration);
				jce.AddCounterUpdate(JobCounter.MbMillisMaps, duration * mbRequired);
				jce.AddCounterUpdate(JobCounter.VcoresMillisMaps, duration * vcoresRequired);
				jce.AddCounterUpdate(JobCounter.MillisMaps, duration);
			}
			else
			{
				jce.AddCounterUpdate(JobCounter.SlotsMillisReduces, simSlotsRequired * duration);
				jce.AddCounterUpdate(JobCounter.MbMillisReduces, duration * mbRequired);
				jce.AddCounterUpdate(JobCounter.VcoresMillisReduces, duration * vcoresRequired);
				jce.AddCounterUpdate(JobCounter.MillisReduces, duration);
			}
		}

		private static JobCounterUpdateEvent CreateJobCounterUpdateEventTASucceeded(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskAttemptImpl
			 taskAttempt)
		{
			TaskId taskId = taskAttempt.attemptId.GetTaskId();
			JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskId.GetJobId());
			UpdateMillisCounters(jce, taskAttempt);
			return jce;
		}

		private static JobCounterUpdateEvent CreateJobCounterUpdateEventTAFailed(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskAttemptImpl
			 taskAttempt, bool taskAlreadyCompleted)
		{
			TaskType taskType = taskAttempt.GetID().GetTaskId().GetTaskType();
			JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskAttempt.GetID().GetTaskId
				().GetJobId());
			if (taskType == TaskType.Map)
			{
				jce.AddCounterUpdate(JobCounter.NumFailedMaps, 1);
			}
			else
			{
				jce.AddCounterUpdate(JobCounter.NumFailedReduces, 1);
			}
			if (!taskAlreadyCompleted)
			{
				UpdateMillisCounters(jce, taskAttempt);
			}
			return jce;
		}

		private static JobCounterUpdateEvent CreateJobCounterUpdateEventTAKilled(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskAttemptImpl
			 taskAttempt, bool taskAlreadyCompleted)
		{
			TaskType taskType = taskAttempt.GetID().GetTaskId().GetTaskType();
			JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskAttempt.GetID().GetTaskId
				().GetJobId());
			if (taskType == TaskType.Map)
			{
				jce.AddCounterUpdate(JobCounter.NumKilledMaps, 1);
			}
			else
			{
				jce.AddCounterUpdate(JobCounter.NumKilledReduces, 1);
			}
			if (!taskAlreadyCompleted)
			{
				UpdateMillisCounters(jce, taskAttempt);
			}
			return jce;
		}

		private static TaskAttemptUnsuccessfulCompletionEvent CreateTaskAttemptUnsuccessfulCompletionEvent
			(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskAttemptImpl taskAttempt, TaskAttemptStateInternal
			 attemptState)
		{
			TaskAttemptUnsuccessfulCompletionEvent tauce = new TaskAttemptUnsuccessfulCompletionEvent
				(TypeConverter.FromYarn(taskAttempt.attemptId), TypeConverter.FromYarn(taskAttempt
				.attemptId.GetTaskId().GetTaskType()), attemptState.ToString(), taskAttempt.finishTime
				, taskAttempt.container == null ? "UNKNOWN" : taskAttempt.container.GetNodeId().
				GetHost(), taskAttempt.container == null ? -1 : taskAttempt.container.GetNodeId(
				).GetPort(), taskAttempt.nodeRackName == null ? "UNKNOWN" : taskAttempt.nodeRackName
				, StringUtils.Join(LineSeparator, taskAttempt.GetDiagnostics()), taskAttempt.GetCounters
				(), taskAttempt.GetProgressSplitBlock().Burst());
			return tauce;
		}

		private static void SendJHStartEventForAssignedFailTask(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl.TaskAttemptImpl
			 taskAttempt)
		{
			if (null == taskAttempt.container)
			{
				return;
			}
			taskAttempt.launchTime = taskAttempt.clock.GetTime();
			IPEndPoint nodeHttpInetAddr = NetUtils.CreateSocketAddr(taskAttempt.container.GetNodeHttpAddress
				());
			taskAttempt.trackerName = nodeHttpInetAddr.GetHostName();
			taskAttempt.httpPort = nodeHttpInetAddr.Port;
			taskAttempt.SendLaunchedEvents();
		}

		private void SendLaunchedEvents()
		{
			JobCounterUpdateEvent jce = new JobCounterUpdateEvent(attemptId.GetTaskId().GetJobId
				());
			jce.AddCounterUpdate(attemptId.GetTaskId().GetTaskType() == TaskType.Map ? JobCounter
				.TotalLaunchedMaps : JobCounter.TotalLaunchedReduces, 1);
			eventHandler.Handle(jce);
			Log.Info("TaskAttempt: [" + attemptId + "] using containerId: [" + container.GetId
				() + " on NM: [" + StringInterner.WeakIntern(container.GetNodeId().ToString()) +
				 "]");
			TaskAttemptStartedEvent tase = new TaskAttemptStartedEvent(TypeConverter.FromYarn
				(attemptId), TypeConverter.FromYarn(attemptId.GetTaskId().GetTaskType()), launchTime
				, trackerName, httpPort, shufflePort, container.GetId(), locality.ToString(), avataar
				.ToString());
			eventHandler.Handle(new JobHistoryEvent(attemptId.GetTaskId().GetJobId(), tase));
		}

		private WrappedProgressSplitsBlock GetProgressSplitBlock()
		{
			readLock.Lock();
			try
			{
				if (progressSplitBlock == null)
				{
					progressSplitBlock = new WrappedProgressSplitsBlock(conf.GetInt(MRJobConfig.MrAmNumProgressSplits
						, MRJobConfig.DefaultMrAmNumProgressSplits));
				}
				return progressSplitBlock;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		private void UpdateProgressSplits()
		{
			double newProgress = reportedStatus.progress;
			newProgress = Math.Max(Math.Min(newProgress, 1.0D), 0.0D);
			Counters counters = reportedStatus.counters;
			if (counters == null)
			{
				return;
			}
			WrappedProgressSplitsBlock splitsBlock = GetProgressSplitBlock();
			if (splitsBlock != null)
			{
				long now = clock.GetTime();
				long start = GetLaunchTime();
				// TODO Ensure not 0
				if (start != 0 && now - start <= int.MaxValue)
				{
					splitsBlock.GetProgressWallclockTime().Extend(newProgress, (int)(now - start));
				}
				Counter cpuCounter = counters.FindCounter(TaskCounter.CpuMilliseconds);
				if (cpuCounter != null && cpuCounter.GetValue() <= int.MaxValue)
				{
					splitsBlock.GetProgressCPUTime().Extend(newProgress, (int)cpuCounter.GetValue());
				}
				// long to int? TODO: FIX. Same below
				Counter virtualBytes = counters.FindCounter(TaskCounter.VirtualMemoryBytes);
				if (virtualBytes != null)
				{
					splitsBlock.GetProgressVirtualMemoryKbytes().Extend(newProgress, (int)(virtualBytes
						.GetValue() / (MemorySplitsResolution)));
				}
				Counter physicalBytes = counters.FindCounter(TaskCounter.PhysicalMemoryBytes);
				if (physicalBytes != null)
				{
					splitsBlock.GetProgressPhysicalMemoryKbytes().Extend(newProgress, (int)(physicalBytes
						.GetValue() / (MemorySplitsResolution)));
				}
			}
		}

		internal class RequestContainerTransition : SingleArcTransition<TaskAttemptImpl, 
			TaskAttemptEvent>
		{
			private readonly bool rescheduled;

			public RequestContainerTransition(bool rescheduled)
			{
				this.rescheduled = rescheduled;
			}

			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				// Tell any speculator that we're requesting a container
				taskAttempt.eventHandler.Handle(new SpeculatorEvent(taskAttempt.GetID().GetTaskId
					(), +1));
				//request for container
				if (rescheduled)
				{
					taskAttempt.eventHandler.Handle(ContainerRequestEvent.CreateContainerRequestEventForFailedContainer
						(taskAttempt.attemptId, taskAttempt.resourceCapability));
				}
				else
				{
					taskAttempt.eventHandler.Handle(new ContainerRequestEvent(taskAttempt.attemptId, 
						taskAttempt.resourceCapability, Sharpen.Collections.ToArray(taskAttempt.dataLocalHosts
						, new string[taskAttempt.dataLocalHosts.Count]), Sharpen.Collections.ToArray(taskAttempt
						.dataLocalRacks, new string[taskAttempt.dataLocalRacks.Count])));
				}
			}
		}

		protected internal virtual ICollection<string> ResolveHosts(string[] src)
		{
			ICollection<string> result = new HashSet<string>();
			if (src != null)
			{
				for (int i = 0; i < src.Length; i++)
				{
					if (src[i] == null)
					{
						continue;
					}
					else
					{
						if (IsIP(src[i]))
						{
							result.AddItem(ResolveHost(src[i]));
						}
						else
						{
							result.AddItem(src[i]);
						}
					}
				}
			}
			return result;
		}

		protected internal virtual string ResolveHost(string src)
		{
			string result = src;
			// Fallback in case of failure.
			try
			{
				IPAddress addr = Sharpen.Extensions.GetAddressByName(src);
				result = addr.GetHostName();
			}
			catch (UnknownHostException)
			{
				Log.Warn("Failed to resolve address: " + src + ". Continuing to use the same.");
			}
			return result;
		}

		private static readonly Sharpen.Pattern ipPattern = Sharpen.Pattern.Compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}"
			);

		// Pattern for matching ip
		protected internal virtual bool IsIP(string src)
		{
			return ipPattern.Matcher(src).Matches();
		}

		private class ContainerAssignedTransition : SingleArcTransition<TaskAttemptImpl, 
			TaskAttemptEvent>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				TaskAttemptContainerAssignedEvent cEvent = (TaskAttemptContainerAssignedEvent)@event;
				Container container = cEvent.GetContainer();
				taskAttempt.container = container;
				// this is a _real_ Task (classic Hadoop mapred flavor):
				taskAttempt.remoteTask = taskAttempt.CreateRemoteTask();
				taskAttempt.jvmID = new WrappedJvmID(((JobID)taskAttempt.remoteTask.GetTaskID().GetJobID
					()), taskAttempt.remoteTask.IsMapTask(), taskAttempt.container.GetId().GetContainerId
					());
				taskAttempt.taskAttemptListener.RegisterPendingTask(taskAttempt.remoteTask, taskAttempt
					.jvmID);
				taskAttempt.ComputeRackAndLocality();
				//launch the container
				//create the container object to be launched for a given Task attempt
				ContainerLaunchContext launchContext = CreateContainerLaunchContext(cEvent.GetApplicationACLs
					(), taskAttempt.conf, taskAttempt.jobToken, taskAttempt.remoteTask, taskAttempt.
					oldJobId, taskAttempt.jvmID, taskAttempt.taskAttemptListener, taskAttempt.credentials
					);
				taskAttempt.eventHandler.Handle(new ContainerRemoteLaunchEvent(taskAttempt.attemptId
					, launchContext, container, taskAttempt.remoteTask));
				// send event to speculator that our container needs are satisfied
				taskAttempt.eventHandler.Handle(new SpeculatorEvent(taskAttempt.GetID().GetTaskId
					(), -1));
			}
		}

		private class DeallocateContainerTransition : SingleArcTransition<TaskAttemptImpl
			, TaskAttemptEvent>
		{
			private readonly TaskAttemptStateInternal finalState;

			private readonly bool withdrawsContainerRequest;

			internal DeallocateContainerTransition(TaskAttemptStateInternal finalState, bool 
				withdrawsContainerRequest)
			{
				this.finalState = finalState;
				this.withdrawsContainerRequest = withdrawsContainerRequest;
			}

			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				if (taskAttempt.GetLaunchTime() == 0)
				{
					SendJHStartEventForAssignedFailTask(taskAttempt);
				}
				//set the finish time
				taskAttempt.SetFinishTime();
				if (@event is TaskAttemptKillEvent)
				{
					taskAttempt.AddDiagnosticInfo(((TaskAttemptKillEvent)@event).GetMessage());
				}
				//send the deallocate event to ContainerAllocator
				taskAttempt.eventHandler.Handle(new ContainerAllocatorEvent(taskAttempt.attemptId
					, ContainerAllocator.EventType.ContainerDeallocate));
				// send event to speculator that we withdraw our container needs, if
				//  we're transitioning out of UNASSIGNED
				if (withdrawsContainerRequest)
				{
					taskAttempt.eventHandler.Handle(new SpeculatorEvent(taskAttempt.GetID().GetTaskId
						(), -1));
				}
				switch (finalState)
				{
					case TaskAttemptStateInternal.Failed:
					{
						taskAttempt.eventHandler.Handle(new TaskTAttemptEvent(taskAttempt.attemptId, TaskEventType
							.TAttemptFailed));
						break;
					}

					case TaskAttemptStateInternal.Killed:
					{
						taskAttempt.eventHandler.Handle(new TaskTAttemptEvent(taskAttempt.attemptId, TaskEventType
							.TAttemptKilled));
						break;
					}

					default:
					{
						Log.Error("Task final state is not FAILED or KILLED: " + finalState);
						break;
					}
				}
				TaskAttemptUnsuccessfulCompletionEvent tauce = CreateTaskAttemptUnsuccessfulCompletionEvent
					(taskAttempt, finalState);
				if (finalState == TaskAttemptStateInternal.Failed)
				{
					taskAttempt.eventHandler.Handle(CreateJobCounterUpdateEventTAFailed(taskAttempt, 
						false));
				}
				else
				{
					if (finalState == TaskAttemptStateInternal.Killed)
					{
						taskAttempt.eventHandler.Handle(CreateJobCounterUpdateEventTAKilled(taskAttempt, 
							false));
					}
				}
				taskAttempt.eventHandler.Handle(new JobHistoryEvent(taskAttempt.attemptId.GetTaskId
					().GetJobId(), tauce));
			}
		}

		private class LaunchedContainerTransition : SingleArcTransition<TaskAttemptImpl, 
			TaskAttemptEvent>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent evnt
				)
			{
				TaskAttemptContainerLaunchedEvent @event = (TaskAttemptContainerLaunchedEvent)evnt;
				//set the launch time
				taskAttempt.launchTime = taskAttempt.clock.GetTime();
				taskAttempt.shufflePort = @event.GetShufflePort();
				// register it to TaskAttemptListener so that it can start monitoring it.
				taskAttempt.taskAttemptListener.RegisterLaunchedTask(taskAttempt.attemptId, taskAttempt
					.jvmID);
				//TODO Resolve to host / IP in case of a local address.
				IPEndPoint nodeHttpInetAddr = NetUtils.CreateSocketAddr(taskAttempt.container.GetNodeHttpAddress
					());
				// TODO: Costly to create sock-addr?
				taskAttempt.trackerName = nodeHttpInetAddr.GetHostName();
				taskAttempt.httpPort = nodeHttpInetAddr.Port;
				taskAttempt.SendLaunchedEvents();
				taskAttempt.eventHandler.Handle(new SpeculatorEvent(taskAttempt.attemptId, true, 
					taskAttempt.clock.GetTime()));
				//make remoteTask reference as null as it is no more needed
				//and free up the memory
				taskAttempt.remoteTask = null;
				//tell the Task that attempt has started
				taskAttempt.eventHandler.Handle(new TaskTAttemptEvent(taskAttempt.attemptId, TaskEventType
					.TAttemptLaunched));
			}
		}

		private class CommitPendingTransition : SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent
			>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				taskAttempt.eventHandler.Handle(new TaskTAttemptEvent(taskAttempt.attemptId, TaskEventType
					.TAttemptCommitPending));
			}
		}

		private class TaskCleanupTransition : SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent
			>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				TaskAttemptContext taskContext = new TaskAttemptContextImpl(taskAttempt.conf, TypeConverter
					.FromYarn(taskAttempt.attemptId));
				taskAttempt.eventHandler.Handle(new CommitterTaskAbortEvent(taskAttempt.attemptId
					, taskContext));
			}
		}

		private class SucceededTransition : SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent
			>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				//set the finish time
				taskAttempt.SetFinishTime();
				taskAttempt.eventHandler.Handle(CreateJobCounterUpdateEventTASucceeded(taskAttempt
					));
				taskAttempt.LogAttemptFinishedEvent(TaskAttemptStateInternal.Succeeded);
				taskAttempt.eventHandler.Handle(new TaskTAttemptEvent(taskAttempt.attemptId, TaskEventType
					.TAttemptSucceeded));
				taskAttempt.eventHandler.Handle(new SpeculatorEvent(taskAttempt.reportedStatus, taskAttempt
					.clock.GetTime()));
			}
		}

		private class FailedTransition : SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent
			>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				if (taskAttempt.GetLaunchTime() == 0)
				{
					SendJHStartEventForAssignedFailTask(taskAttempt);
				}
				// set the finish time
				taskAttempt.SetFinishTime();
				taskAttempt.eventHandler.Handle(CreateJobCounterUpdateEventTAFailed(taskAttempt, 
					false));
				TaskAttemptUnsuccessfulCompletionEvent tauce = CreateTaskAttemptUnsuccessfulCompletionEvent
					(taskAttempt, TaskAttemptStateInternal.Failed);
				taskAttempt.eventHandler.Handle(new JobHistoryEvent(taskAttempt.attemptId.GetTaskId
					().GetJobId(), tauce));
				taskAttempt.eventHandler.Handle(new TaskTAttemptEvent(taskAttempt.attemptId, TaskEventType
					.TAttemptFailed));
			}
		}

		private class RecoverTransition : MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent
			, TaskAttemptStateInternal>
		{
			public virtual TaskAttemptStateInternal Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent
				 @event)
			{
				TaskAttemptRecoverEvent tare = (TaskAttemptRecoverEvent)@event;
				return taskAttempt.Recover(tare.GetTaskAttemptInfo(), tare.GetCommitter(), tare.GetRecoverOutput
					());
			}
		}

		private void LogAttemptFinishedEvent(TaskAttemptStateInternal state)
		{
			//Log finished events only if an attempt started.
			if (GetLaunchTime() == 0)
			{
				return;
			}
			string containerHostName = this.container == null ? "UNKNOWN" : this.container.GetNodeId
				().GetHost();
			int containerNodePort = this.container == null ? -1 : this.container.GetNodeId().
				GetPort();
			if (attemptId.GetTaskId().GetTaskType() == TaskType.Map)
			{
				MapAttemptFinishedEvent mfe = new MapAttemptFinishedEvent(TypeConverter.FromYarn(
					attemptId), TypeConverter.FromYarn(attemptId.GetTaskId().GetTaskType()), state.ToString
					(), this.reportedStatus.mapFinishTime, finishTime, containerHostName, containerNodePort
					, this.nodeRackName == null ? "UNKNOWN" : this.nodeRackName, this.reportedStatus
					.stateString, GetCounters(), GetProgressSplitBlock().Burst());
				eventHandler.Handle(new JobHistoryEvent(attemptId.GetTaskId().GetJobId(), mfe));
			}
			else
			{
				ReduceAttemptFinishedEvent rfe = new ReduceAttemptFinishedEvent(TypeConverter.FromYarn
					(attemptId), TypeConverter.FromYarn(attemptId.GetTaskId().GetTaskType()), state.
					ToString(), this.reportedStatus.shuffleFinishTime, this.reportedStatus.sortFinishTime
					, finishTime, containerHostName, containerNodePort, this.nodeRackName == null ? 
					"UNKNOWN" : this.nodeRackName, this.reportedStatus.stateString, GetCounters(), GetProgressSplitBlock
					().Burst());
				eventHandler.Handle(new JobHistoryEvent(attemptId.GetTaskId().GetJobId(), rfe));
			}
		}

		private class TooManyFetchFailureTransition : SingleArcTransition<TaskAttemptImpl
			, TaskAttemptEvent>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				// too many fetch failure can only happen for map tasks
				Preconditions.CheckArgument(taskAttempt.GetID().GetTaskId().GetTaskType() == TaskType
					.Map);
				//add to diagnostic
				taskAttempt.AddDiagnosticInfo("Too Many fetch failures.Failing the attempt");
				if (taskAttempt.GetLaunchTime() != 0)
				{
					taskAttempt.eventHandler.Handle(CreateJobCounterUpdateEventTAFailed(taskAttempt, 
						true));
					TaskAttemptUnsuccessfulCompletionEvent tauce = CreateTaskAttemptUnsuccessfulCompletionEvent
						(taskAttempt, TaskAttemptStateInternal.Failed);
					taskAttempt.eventHandler.Handle(new JobHistoryEvent(taskAttempt.attemptId.GetTaskId
						().GetJobId(), tauce));
				}
				else
				{
					Log.Debug("Not generating HistoryFinish event since start event not " + "generated for taskAttempt: "
						 + taskAttempt.GetID());
				}
				taskAttempt.eventHandler.Handle(new TaskTAttemptEvent(taskAttempt.attemptId, TaskEventType
					.TAttemptFailed));
			}
		}

		private class KilledAfterSuccessTransition : MultipleArcTransition<TaskAttemptImpl
			, TaskAttemptEvent, TaskAttemptStateInternal>
		{
			public virtual TaskAttemptStateInternal Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent
				 @event)
			{
				if (taskAttempt.GetID().GetTaskId().GetTaskType() == TaskType.Reduce)
				{
					// after a reduce task has succeeded, its outputs are in safe in HDFS.
					// logically such a task should not be killed. we only come here when
					// there is a race condition in the event queue. E.g. some logic sends
					// a kill request to this attempt when the successful completion event
					// for this task is already in the event queue. so the kill event will
					// get executed immediately after the attempt is marked successful and 
					// result in this transition being exercised.
					// ignore this for reduce tasks
					Log.Info("Ignoring killed event for successful reduce task attempt" + taskAttempt
						.GetID().ToString());
					return TaskAttemptStateInternal.Succeeded;
				}
				if (@event is TaskAttemptKillEvent)
				{
					TaskAttemptKillEvent msgEvent = (TaskAttemptKillEvent)@event;
					//add to diagnostic
					taskAttempt.AddDiagnosticInfo(msgEvent.GetMessage());
				}
				// not setting a finish time since it was set on success
				System.Diagnostics.Debug.Assert((taskAttempt.GetFinishTime() != 0));
				System.Diagnostics.Debug.Assert((taskAttempt.GetLaunchTime() != 0));
				taskAttempt.eventHandler.Handle(CreateJobCounterUpdateEventTAKilled(taskAttempt, 
					true));
				TaskAttemptUnsuccessfulCompletionEvent tauce = CreateTaskAttemptUnsuccessfulCompletionEvent
					(taskAttempt, TaskAttemptStateInternal.Killed);
				taskAttempt.eventHandler.Handle(new JobHistoryEvent(taskAttempt.attemptId.GetTaskId
					().GetJobId(), tauce));
				taskAttempt.eventHandler.Handle(new TaskTAttemptEvent(taskAttempt.attemptId, TaskEventType
					.TAttemptKilled));
				return TaskAttemptStateInternal.Killed;
			}
		}

		private class KilledTransition : SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent
			>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				if (taskAttempt.GetLaunchTime() == 0)
				{
					SendJHStartEventForAssignedFailTask(taskAttempt);
				}
				//set the finish time
				taskAttempt.SetFinishTime();
				taskAttempt.eventHandler.Handle(CreateJobCounterUpdateEventTAKilled(taskAttempt, 
					false));
				TaskAttemptUnsuccessfulCompletionEvent tauce = CreateTaskAttemptUnsuccessfulCompletionEvent
					(taskAttempt, TaskAttemptStateInternal.Killed);
				taskAttempt.eventHandler.Handle(new JobHistoryEvent(taskAttempt.attemptId.GetTaskId
					().GetJobId(), tauce));
				if (@event is TaskAttemptKillEvent)
				{
					taskAttempt.AddDiagnosticInfo(((TaskAttemptKillEvent)@event).GetMessage());
				}
				taskAttempt.eventHandler.Handle(new TaskTAttemptEvent(taskAttempt.attemptId, TaskEventType
					.TAttemptKilled));
			}
		}

		private class CleanupContainerTransition : SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent
			>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				// unregister it to TaskAttemptListener so that it stops listening
				// for it
				taskAttempt.taskAttemptListener.Unregister(taskAttempt.attemptId, taskAttempt.jvmID
					);
				if (@event is TaskAttemptKillEvent)
				{
					taskAttempt.AddDiagnosticInfo(((TaskAttemptKillEvent)@event).GetMessage());
				}
				taskAttempt.reportedStatus.progress = 1.0f;
				taskAttempt.UpdateProgressSplits();
				//send the cleanup event to containerLauncher
				taskAttempt.eventHandler.Handle(new ContainerLauncherEvent(taskAttempt.attemptId, 
					taskAttempt.container.GetId(), StringInterner.WeakIntern(taskAttempt.container.GetNodeId
					().ToString()), taskAttempt.container.GetContainerToken(), ContainerLauncher.EventType
					.ContainerRemoteCleanup));
			}
		}

		private void AddDiagnosticInfo(string diag)
		{
			if (diag != null && !diag.Equals(string.Empty))
			{
				diagnostics.AddItem(diag);
			}
		}

		private class StatusUpdater : SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent
			>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				// Status update calls don't really change the state of the attempt.
				TaskAttemptStatusUpdateEvent.TaskAttemptStatus newReportedStatus = ((TaskAttemptStatusUpdateEvent
					)@event).GetReportedTaskAttemptStatus();
				// Now switch the information in the reportedStatus
				taskAttempt.reportedStatus = newReportedStatus;
				taskAttempt.reportedStatus.taskState = taskAttempt.GetState();
				// send event to speculator about the reported status
				taskAttempt.eventHandler.Handle(new SpeculatorEvent(taskAttempt.reportedStatus, taskAttempt
					.clock.GetTime()));
				taskAttempt.UpdateProgressSplits();
				//if fetch failures are present, send the fetch failure event to job
				//this only will happen in reduce attempt type
				if (taskAttempt.reportedStatus.fetchFailedMaps != null && taskAttempt.reportedStatus
					.fetchFailedMaps.Count > 0)
				{
					taskAttempt.eventHandler.Handle(new JobTaskAttemptFetchFailureEvent(taskAttempt.attemptId
						, taskAttempt.reportedStatus.fetchFailedMaps));
				}
			}
		}

		private class DiagnosticInformationUpdater : SingleArcTransition<TaskAttemptImpl, 
			TaskAttemptEvent>
		{
			public virtual void Transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent @event
				)
			{
				TaskAttemptDiagnosticsUpdateEvent diagEvent = (TaskAttemptDiagnosticsUpdateEvent)
					@event;
				Log.Info("Diagnostics report from " + taskAttempt.attemptId + ": " + diagEvent.GetDiagnosticInfo
					());
				taskAttempt.AddDiagnosticInfo(diagEvent.GetDiagnosticInfo());
			}
		}

		private void InitTaskAttemptStatus(TaskAttemptStatusUpdateEvent.TaskAttemptStatus
			 result)
		{
			result.progress = 0.0f;
			result.phase = Phase.Starting;
			result.stateString = "NEW";
			result.taskState = TaskAttemptState.New;
			Counters counters = EmptyCounters;
			result.counters = counters;
		}
	}
}
