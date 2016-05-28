using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Commit;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.App.Launcher;
using Org.Apache.Hadoop.Mapreduce.V2.App.Local;
using Org.Apache.Hadoop.Mapreduce.V2.App.Metrics;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.App.Speculate;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Log4j;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary>The Map-Reduce Application Master.</summary>
	/// <remarks>
	/// The Map-Reduce Application Master.
	/// The state machine is encapsulated in the implementation of Job interface.
	/// All state changes happens via Job interface. Each event
	/// results in a Finite State Transition in Job.
	/// MR AppMaster is the composition of loosely coupled services. The services
	/// interact with each other via events. The components resembles the
	/// Actors model. The component acts on received event and send out the
	/// events to other components.
	/// This keeps it highly concurrent with no or minimal synchronization needs.
	/// The events are dispatched by a central Dispatch mechanism. All components
	/// register to the Dispatcher.
	/// The information is shared across different components using AppContext.
	/// </remarks>
	public class MRAppMaster : CompositeService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.MRAppMaster
			));

		/// <summary>Priority of the MRAppMaster shutdown hook.</summary>
		public const int ShutdownHookPriority = 30;

		public const string IntermediateDataEncryptionAlgo = "HmacSHA1";

		private Clock clock;

		private readonly long startTime;

		private readonly long appSubmitTime;

		private string appName;

		private readonly ApplicationAttemptId appAttemptID;

		private readonly ContainerId containerID;

		private readonly string nmHost;

		private readonly int nmPort;

		private readonly int nmHttpPort;

		protected internal readonly MRAppMetrics metrics;

		private IDictionary<TaskId, JobHistoryParser.TaskInfo> completedTasksFromPreviousRun;

		private IList<AMInfo> amInfos;

		private AppContext context;

		private Dispatcher dispatcher;

		private ClientService clientService;

		private ContainerAllocator containerAllocator;

		private ContainerLauncher containerLauncher;

		private EventHandler<CommitterEvent> committerEventHandler;

		private Speculator speculator;

		private TaskAttemptListener taskAttemptListener;

		private JobTokenSecretManager jobTokenSecretManager = new JobTokenSecretManager();

		private JobId jobId;

		private bool newApiCommitter;

		private ClassLoader jobClassLoader;

		private OutputCommitter committer;

		private MRAppMaster.JobEventDispatcher jobEventDispatcher;

		private JobHistoryEventHandler jobHistoryEventHandler;

		private MRAppMaster.SpeculatorEventDispatcher speculatorEventDispatcher;

		private byte[] encryptedSpillKey;

		private Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job;

		private Credentials jobCredentials = new Credentials();

		protected internal UserGroupInformation currentUser;

		[VisibleForTesting]
		protected internal volatile bool isLastAMRetry = false;

		internal bool errorHappenedShutDown = false;

		private string shutDownMessage = null;

		internal JobStateInternal forcedState = null;

		private readonly ScheduledExecutorService logSyncer;

		private long recoveredJobStartTime = 0;

		[VisibleForTesting]
		protected internal AtomicBoolean successfullyUnregistered = new AtomicBoolean(false
			);

		public MRAppMaster(ApplicationAttemptId applicationAttemptId, ContainerId containerId
			, string nmHost, int nmPort, int nmHttpPort, long appSubmitTime)
			: this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort, new SystemClock
				(), appSubmitTime)
		{
		}

		public MRAppMaster(ApplicationAttemptId applicationAttemptId, ContainerId containerId
			, string nmHost, int nmPort, int nmHttpPort, Clock clock, long appSubmitTime)
			: base(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.MRAppMaster).FullName)
		{
			// Filled during init
			// Will be setup during init
			//Something happened and we should shut down right after we start up.
			this.clock = clock;
			this.startTime = clock.GetTime();
			this.appSubmitTime = appSubmitTime;
			this.appAttemptID = applicationAttemptId;
			this.containerID = containerId;
			this.nmHost = nmHost;
			this.nmPort = nmPort;
			this.nmHttpPort = nmHttpPort;
			this.metrics = MRAppMetrics.Create();
			logSyncer = TaskLog.CreateLogSyncer();
			Log.Info("Created MRAppMaster for application " + applicationAttemptId);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			// create the job classloader if enabled
			CreateJobClassLoader(conf);
			conf.SetBoolean(Dispatcher.DispatcherExitOnErrorKey, true);
			InitJobCredentialsAndUGI(conf);
			context = new MRAppMaster.RunningAppContext(this, conf);
			// Job name is the same as the app name util we support DAG of jobs
			// for an app later
			appName = conf.Get(MRJobConfig.JobName, "<missing app name>");
			conf.SetInt(MRJobConfig.ApplicationAttemptId, appAttemptID.GetAttemptId());
			newApiCommitter = false;
			jobId = MRBuilderUtils.NewJobId(appAttemptID.GetApplicationId(), appAttemptID.GetApplicationId
				().GetId());
			int numReduceTasks = conf.GetInt(MRJobConfig.NumReduces, 0);
			if ((numReduceTasks > 0 && conf.GetBoolean("mapred.reducer.new-api", false)) || (
				numReduceTasks == 0 && conf.GetBoolean("mapred.mapper.new-api", false)))
			{
				newApiCommitter = true;
				Log.Info("Using mapred newApiCommitter.");
			}
			bool copyHistory = false;
			try
			{
				string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
				Path stagingDir = MRApps.GetStagingAreaDir(conf, user);
				FileSystem fs = GetFileSystem(conf);
				bool stagingExists = fs.Exists(stagingDir);
				Path startCommitFile = MRApps.GetStartJobCommitFile(conf, user, jobId);
				bool commitStarted = fs.Exists(startCommitFile);
				Path endCommitSuccessFile = MRApps.GetEndJobCommitSuccessFile(conf, user, jobId);
				bool commitSuccess = fs.Exists(endCommitSuccessFile);
				Path endCommitFailureFile = MRApps.GetEndJobCommitFailureFile(conf, user, jobId);
				bool commitFailure = fs.Exists(endCommitFailureFile);
				if (!stagingExists)
				{
					isLastAMRetry = true;
					Log.Info("Attempt num: " + appAttemptID.GetAttemptId() + " is last retry: " + isLastAMRetry
						 + " because the staging dir doesn't exist.");
					errorHappenedShutDown = true;
					forcedState = JobStateInternal.Error;
					shutDownMessage = "Staging dir does not exist " + stagingDir;
					Log.Fatal(shutDownMessage);
				}
				else
				{
					if (commitStarted)
					{
						//A commit was started so this is the last time, we just need to know
						// what result we will use to notify, and how we will unregister
						errorHappenedShutDown = true;
						isLastAMRetry = true;
						Log.Info("Attempt num: " + appAttemptID.GetAttemptId() + " is last retry: " + isLastAMRetry
							 + " because a commit was started.");
						copyHistory = true;
						if (commitSuccess)
						{
							shutDownMessage = "We crashed after successfully committing. Recovering.";
							forcedState = JobStateInternal.Succeeded;
						}
						else
						{
							if (commitFailure)
							{
								shutDownMessage = "We crashed after a commit failure.";
								forcedState = JobStateInternal.Failed;
							}
							else
							{
								//The commit is still pending, commit error
								shutDownMessage = "We crashed durring a commit";
								forcedState = JobStateInternal.Error;
							}
						}
					}
				}
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException("Error while initializing", e);
			}
			if (errorHappenedShutDown)
			{
				dispatcher = CreateDispatcher();
				AddIfService(dispatcher);
				MRAppMaster.NoopEventHandler eater = new MRAppMaster.NoopEventHandler();
				//We do not have a JobEventDispatcher in this path
				dispatcher.Register(typeof(JobEventType), eater);
				EventHandler<JobHistoryEvent> historyService = null;
				if (copyHistory)
				{
					historyService = CreateJobHistoryHandler(context);
					dispatcher.Register(typeof(EventType), historyService);
				}
				else
				{
					dispatcher.Register(typeof(EventType), eater);
				}
				if (copyHistory)
				{
					// Now that there's a FINISHING state for application on RM to give AMs
					// plenty of time to clean up after unregister it's safe to clean staging
					// directory after unregistering with RM. So, we start the staging-dir
					// cleaner BEFORE the ContainerAllocator so that on shut-down,
					// ContainerAllocator unregisters first and then the staging-dir cleaner
					// deletes staging directory.
					AddService(CreateStagingDirCleaningService());
				}
				// service to allocate containers from RM (if non-uber) or to fake it (uber)
				containerAllocator = CreateContainerAllocator(null, context);
				AddIfService(containerAllocator);
				dispatcher.Register(typeof(ContainerAllocator.EventType), containerAllocator);
				if (copyHistory)
				{
					// Add the JobHistoryEventHandler last so that it is properly stopped first.
					// This will guarantee that all history-events are flushed before AM goes
					// ahead with shutdown.
					// Note: Even though JobHistoryEventHandler is started last, if any
					// component creates a JobHistoryEvent in the meanwhile, it will be just be
					// queued inside the JobHistoryEventHandler 
					AddIfService(historyService);
					JobHistoryCopyService cpHist = new JobHistoryCopyService(appAttemptID, dispatcher
						.GetEventHandler());
					AddIfService(cpHist);
				}
			}
			else
			{
				committer = CreateOutputCommitter(conf);
				dispatcher = CreateDispatcher();
				AddIfService(dispatcher);
				//service to handle requests from JobClient
				clientService = CreateClientService(context);
				// Init ClientService separately so that we stop it separately, since this
				// service needs to wait some time before it stops so clients can know the
				// final states
				clientService.Init(conf);
				containerAllocator = CreateContainerAllocator(clientService, context);
				//service to handle the output committer
				committerEventHandler = CreateCommitterEventHandler(context, committer);
				AddIfService(committerEventHandler);
				//service to handle requests to TaskUmbilicalProtocol
				taskAttemptListener = CreateTaskAttemptListener(context);
				AddIfService(taskAttemptListener);
				//service to log job history events
				EventHandler<JobHistoryEvent> historyService = CreateJobHistoryHandler(context);
				dispatcher.Register(typeof(EventType), historyService);
				this.jobEventDispatcher = new MRAppMaster.JobEventDispatcher(this);
				//register the event dispatchers
				dispatcher.Register(typeof(JobEventType), jobEventDispatcher);
				dispatcher.Register(typeof(TaskEventType), new MRAppMaster.TaskEventDispatcher(this
					));
				dispatcher.Register(typeof(TaskAttemptEventType), new MRAppMaster.TaskAttemptEventDispatcher
					(this));
				dispatcher.Register(typeof(CommitterEventType), committerEventHandler);
				if (conf.GetBoolean(MRJobConfig.MapSpeculative, false) || conf.GetBoolean(MRJobConfig
					.ReduceSpeculative, false))
				{
					//optional service to speculate on task attempts' progress
					speculator = CreateSpeculator(conf, context);
					AddIfService(speculator);
				}
				speculatorEventDispatcher = new MRAppMaster.SpeculatorEventDispatcher(this, conf);
				dispatcher.Register(typeof(Speculator.EventType), speculatorEventDispatcher);
				// Now that there's a FINISHING state for application on RM to give AMs
				// plenty of time to clean up after unregister it's safe to clean staging
				// directory after unregistering with RM. So, we start the staging-dir
				// cleaner BEFORE the ContainerAllocator so that on shut-down,
				// ContainerAllocator unregisters first and then the staging-dir cleaner
				// deletes staging directory.
				AddService(CreateStagingDirCleaningService());
				// service to allocate containers from RM (if non-uber) or to fake it (uber)
				AddIfService(containerAllocator);
				dispatcher.Register(typeof(ContainerAllocator.EventType), containerAllocator);
				// corresponding service to launch allocated containers via NodeManager
				containerLauncher = CreateContainerLauncher(context);
				AddIfService(containerLauncher);
				dispatcher.Register(typeof(ContainerLauncher.EventType), containerLauncher);
				// Add the JobHistoryEventHandler last so that it is properly stopped first.
				// This will guarantee that all history-events are flushed before AM goes
				// ahead with shutdown.
				// Note: Even though JobHistoryEventHandler is started last, if any
				// component creates a JobHistoryEvent in the meanwhile, it will be just be
				// queued inside the JobHistoryEventHandler 
				AddIfService(historyService);
			}
			base.ServiceInit(conf);
		}

		// end of init()
		protected internal virtual Dispatcher CreateDispatcher()
		{
			return new AsyncDispatcher();
		}

		private OutputCommitter CreateOutputCommitter(Configuration conf)
		{
			return CallWithJobClassLoader(conf, new _Action_458(this));
		}

		private sealed class _Action_458 : MRAppMaster.Action<OutputCommitter>
		{
			public _Action_458(MRAppMaster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public OutputCommitter Call(Configuration conf)
			{
				OutputCommitter committer = null;
				Org.Apache.Hadoop.Mapreduce.V2.App.MRAppMaster.Log.Info("OutputCommitter set in config "
					 + conf.Get("mapred.output.committer.class"));
				if (this._enclosing.newApiCommitter)
				{
					TaskId taskID = MRBuilderUtils.NewTaskId(this._enclosing.jobId, 0, TaskType.Map);
					TaskAttemptId attemptID = MRBuilderUtils.NewTaskAttemptId(taskID, 0);
					TaskAttemptContext taskContext = new TaskAttemptContextImpl(conf, TypeConverter.FromYarn
						(attemptID));
					OutputFormat outputFormat;
					try
					{
						outputFormat = ReflectionUtils.NewInstance(taskContext.GetOutputFormatClass(), conf
							);
						committer = outputFormat.GetOutputCommitter(taskContext);
					}
					catch (Exception e)
					{
						throw new YarnRuntimeException(e);
					}
				}
				else
				{
					committer = ReflectionUtils.NewInstance(conf.GetClass<OutputCommitter>("mapred.output.committer.class"
						, typeof(FileOutputCommitter)), conf);
				}
				Org.Apache.Hadoop.Mapreduce.V2.App.MRAppMaster.Log.Info("OutputCommitter is " + committer
					.GetType().FullName);
				return committer;
			}

			private readonly MRAppMaster _enclosing;
		}

		protected internal virtual bool KeepJobFiles(JobConf conf)
		{
			return (conf.GetKeepTaskFilesPattern() != null || conf.GetKeepFailedTaskFiles());
		}

		/// <summary>Create the default file System for this job.</summary>
		/// <param name="conf">the conf object</param>
		/// <returns>the default filesystem for this job</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual FileSystem GetFileSystem(Configuration conf)
		{
			return FileSystem.Get(conf);
		}

		protected internal virtual Credentials GetCredentials()
		{
			return jobCredentials;
		}

		/// <summary>clean up staging directories for the job.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CleanupStagingDir()
		{
			/* make sure we clean the staging files */
			string jobTempDir = null;
			FileSystem fs = GetFileSystem(GetConfig());
			try
			{
				if (!KeepJobFiles(new JobConf(GetConfig())))
				{
					jobTempDir = GetConfig().Get(MRJobConfig.MapreduceJobDir);
					if (jobTempDir == null)
					{
						Log.Warn("Job Staging directory is null");
						return;
					}
					Path jobTempDirPath = new Path(jobTempDir);
					Log.Info("Deleting staging directory " + FileSystem.GetDefaultUri(GetConfig()) + 
						" " + jobTempDir);
					fs.Delete(jobTempDirPath, true);
				}
			}
			catch (IOException io)
			{
				Log.Error("Failed to cleanup staging dir " + jobTempDir, io);
			}
		}

		/// <summary>Exit call.</summary>
		/// <remarks>Exit call. Just in a function call to enable testing.</remarks>
		protected internal virtual void Sysexit()
		{
			System.Environment.Exit(0);
		}

		[VisibleForTesting]
		public virtual void ShutDownJob()
		{
			// job has finished
			// this is the only job, so shut down the Appmaster
			// note in a workflow scenario, this may lead to creation of a new
			// job (FIXME?)
			try
			{
				//if isLastAMRetry comes as true, should never set it to false
				if (!isLastAMRetry)
				{
					if (((JobImpl)job).GetInternalState() != JobStateInternal.Reboot)
					{
						Log.Info("We are finishing cleanly so this is the last retry");
						isLastAMRetry = true;
					}
				}
				NotifyIsLastAMRetry(isLastAMRetry);
				// Stop all services
				// This will also send the final report to the ResourceManager
				Log.Info("Calling stop for all the services");
				this.Stop();
				if (isLastAMRetry)
				{
					// Send job-end notification when it is safe to report termination to
					// users and it is the last AM retry
					if (GetConfig().Get(MRJobConfig.MrJobEndNotificationUrl) != null)
					{
						try
						{
							Log.Info("Job end notification started for jobID : " + job.GetReport().GetJobId()
								);
							JobEndNotifier notifier = new JobEndNotifier();
							notifier.SetConf(GetConfig());
							JobReport report = job.GetReport();
							// If unregistration fails, the final state is unavailable. However,
							// at the last AM Retry, the client will finally be notified FAILED
							// from RM, so we should let users know FAILED via notifier as well
							if (!context.HasSuccessfullyUnregistered())
							{
								report.SetJobState(JobState.Failed);
							}
							notifier.Notify(report);
						}
						catch (Exception ie)
						{
							Log.Warn("Job end notification interrupted for jobID : " + job.GetReport().GetJobId
								(), ie);
						}
					}
				}
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
				clientService.Stop();
			}
			catch (Exception t)
			{
				Log.Warn("Graceful stop failed. Exiting.. ", t);
				ExitUtil.Terminate(1, t);
			}
		}

		private class JobFinishEventHandler : EventHandler<JobFinishEvent>
		{
			public virtual void Handle(JobFinishEvent @event)
			{
				// Create a new thread to shutdown the AM. We should not do it in-line
				// to avoid blocking the dispatcher itself.
				new _Thread_605(this).Start();
			}

			private sealed class _Thread_605 : Sharpen.Thread
			{
				public _Thread_605(JobFinishEventHandler _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override void Run()
				{
					this._enclosing._enclosing.ShutDownJob();
				}

				private readonly JobFinishEventHandler _enclosing;
			}

			internal JobFinishEventHandler(MRAppMaster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MRAppMaster _enclosing;
		}

		/// <summary>create an event handler that handles the job finish event.</summary>
		/// <returns>the job finish event handler.</returns>
		protected internal virtual EventHandler<JobFinishEvent> CreateJobFinishEventHandler
			()
		{
			return new MRAppMaster.JobFinishEventHandler(this);
		}

		/// <summary>Create and initialize (but don't start) a single job.</summary>
		/// <param name="forcedState">a state to force the job into or null for normal operation.
		/// 	</param>
		/// <param name="diagnostic">a diagnostic message to include with the job.</param>
		protected internal virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job CreateJob(Configuration
			 conf, JobStateInternal forcedState, string diagnostic)
		{
			// create single job
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job newJob = new JobImpl(jobId, appAttemptID
				, conf, dispatcher.GetEventHandler(), taskAttemptListener, jobTokenSecretManager
				, jobCredentials, clock, completedTasksFromPreviousRun, metrics, committer, newApiCommitter
				, currentUser.GetUserName(), appSubmitTime, amInfos, context, forcedState, diagnostic
				);
			((MRAppMaster.RunningAppContext)context).jobs[newJob.GetID()] = newJob;
			dispatcher.Register(typeof(JobFinishEvent.Type), CreateJobFinishEventHandler());
			return newJob;
		}

		// end createJob()
		/// <summary>Obtain the tokens needed by the job and put them in the UGI</summary>
		/// <param name="conf"/>
		protected internal virtual void InitJobCredentialsAndUGI(Configuration conf)
		{
			try
			{
				this.currentUser = UserGroupInformation.GetCurrentUser();
				this.jobCredentials = ((JobConf)conf).GetCredentials();
				if (CryptoUtils.IsEncryptedSpillEnabled(conf))
				{
					int keyLen = conf.GetInt(MRJobConfig.MrEncryptedIntermediateDataKeySizeBits, MRJobConfig
						.DefaultMrEncryptedIntermediateDataKeySizeBits);
					KeyGenerator keyGen = KeyGenerator.GetInstance(IntermediateDataEncryptionAlgo);
					keyGen.Init(keyLen);
					encryptedSpillKey = keyGen.GenerateKey().GetEncoded();
				}
				else
				{
					encryptedSpillKey = new byte[] { 0 };
				}
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (NoSuchAlgorithmException e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		protected internal virtual EventHandler<JobHistoryEvent> CreateJobHistoryHandler(
			AppContext context)
		{
			this.jobHistoryEventHandler = new JobHistoryEventHandler(context, GetStartCount()
				);
			return this.jobHistoryEventHandler;
		}

		protected internal virtual AbstractService CreateStagingDirCleaningService()
		{
			return new MRAppMaster.StagingDirCleaningService(this);
		}

		protected internal virtual Speculator CreateSpeculator(Configuration conf, AppContext
			 context)
		{
			return CallWithJobClassLoader(conf, new _Action_687(context));
		}

		private sealed class _Action_687 : MRAppMaster.Action<Speculator>
		{
			public _Action_687(AppContext context)
			{
				this.context = context;
			}

			public Speculator Call(Configuration conf)
			{
				Type speculatorClass;
				try
				{
					speculatorClass = conf.GetClass<Speculator>(MRJobConfig.MrAmJobSpeculator, typeof(
						DefaultSpeculator));
					// "yarn.mapreduce.job.speculator.class"
					Constructor<Speculator> speculatorConstructor = speculatorClass.GetConstructor(typeof(
						Configuration), typeof(AppContext));
					Speculator result = speculatorConstructor.NewInstance(conf, context);
					return result;
				}
				catch (InstantiationException ex)
				{
					MRAppMaster.Log.Error("Can't make a speculator -- check " + MRJobConfig.MrAmJobSpeculator
						, ex);
					throw new YarnRuntimeException(ex);
				}
				catch (MemberAccessException ex)
				{
					MRAppMaster.Log.Error("Can't make a speculator -- check " + MRJobConfig.MrAmJobSpeculator
						, ex);
					throw new YarnRuntimeException(ex);
				}
				catch (TargetInvocationException ex)
				{
					MRAppMaster.Log.Error("Can't make a speculator -- check " + MRJobConfig.MrAmJobSpeculator
						, ex);
					throw new YarnRuntimeException(ex);
				}
				catch (MissingMethodException ex)
				{
					MRAppMaster.Log.Error("Can't make a speculator -- check " + MRJobConfig.MrAmJobSpeculator
						, ex);
					throw new YarnRuntimeException(ex);
				}
			}

			private readonly AppContext context;
		}

		protected internal virtual TaskAttemptListener CreateTaskAttemptListener(AppContext
			 context)
		{
			TaskAttemptListener lis = new TaskAttemptListenerImpl(context, jobTokenSecretManager
				, GetRMHeartbeatHandler(), encryptedSpillKey);
			return lis;
		}

		protected internal virtual EventHandler<CommitterEvent> CreateCommitterEventHandler
			(AppContext context, OutputCommitter committer)
		{
			return new CommitterEventHandler(context, committer, GetRMHeartbeatHandler(), jobClassLoader
				);
		}

		protected internal virtual ContainerAllocator CreateContainerAllocator(ClientService
			 clientService, AppContext context)
		{
			return new MRAppMaster.ContainerAllocatorRouter(this, clientService, context);
		}

		protected internal virtual RMHeartbeatHandler GetRMHeartbeatHandler()
		{
			return (RMHeartbeatHandler)containerAllocator;
		}

		protected internal virtual ContainerLauncher CreateContainerLauncher(AppContext context
			)
		{
			return new MRAppMaster.ContainerLauncherRouter(this, context);
		}

		//TODO:should have an interface for MRClientService
		protected internal virtual ClientService CreateClientService(AppContext context)
		{
			return new MRClientService(context);
		}

		public virtual ApplicationId GetAppID()
		{
			return appAttemptID.GetApplicationId();
		}

		public virtual ApplicationAttemptId GetAttemptID()
		{
			return appAttemptID;
		}

		public virtual JobId GetJobId()
		{
			return jobId;
		}

		public virtual OutputCommitter GetCommitter()
		{
			return committer;
		}

		public virtual bool IsNewApiCommitter()
		{
			return newApiCommitter;
		}

		public virtual int GetStartCount()
		{
			return appAttemptID.GetAttemptId();
		}

		public virtual AppContext GetContext()
		{
			return context;
		}

		public virtual Dispatcher GetDispatcher()
		{
			return dispatcher;
		}

		public virtual IDictionary<TaskId, JobHistoryParser.TaskInfo> GetCompletedTaskFromPreviousRun
			()
		{
			return completedTasksFromPreviousRun;
		}

		public virtual IList<AMInfo> GetAllAMInfos()
		{
			return amInfos;
		}

		public virtual ContainerAllocator GetContainerAllocator()
		{
			return containerAllocator;
		}

		public virtual ContainerLauncher GetContainerLauncher()
		{
			return containerLauncher;
		}

		public virtual TaskAttemptListener GetTaskAttemptListener()
		{
			return taskAttemptListener;
		}

		public virtual bool IsLastAMRetry()
		{
			return isLastAMRetry;
		}

		/// <summary>
		/// By the time life-cycle of this router starts, job-init would have already
		/// happened.
		/// </summary>
		private sealed class ContainerAllocatorRouter : AbstractService, ContainerAllocator
			, RMHeartbeatHandler
		{
			private readonly ClientService clientService;

			private readonly AppContext context;

			private ContainerAllocator containerAllocator;

			internal ContainerAllocatorRouter(MRAppMaster _enclosing, ClientService clientService
				, AppContext context)
				: base(typeof(MRAppMaster.ContainerAllocatorRouter).FullName)
			{
				this._enclosing = _enclosing;
				this.clientService = clientService;
				this.context = context;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				if (this._enclosing.job.IsUber())
				{
					MRApps.SetupDistributedCacheLocal(this.GetConfig());
					this.containerAllocator = new LocalContainerAllocator(this.clientService, this.context
						, this._enclosing.nmHost, this._enclosing.nmPort, this._enclosing.nmHttpPort, this
						._enclosing.containerID);
				}
				else
				{
					this.containerAllocator = new RMContainerAllocator(this.clientService, this.context
						);
				}
				((Org.Apache.Hadoop.Service.Service)this.containerAllocator).Init(this.GetConfig(
					));
				((Org.Apache.Hadoop.Service.Service)this.containerAllocator).Start();
				base.ServiceStart();
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				ServiceOperations.Stop((Org.Apache.Hadoop.Service.Service)this.containerAllocator
					);
				base.ServiceStop();
			}

			public void Handle(ContainerAllocatorEvent @event)
			{
				this.containerAllocator.Handle(@event);
			}

			public void SetSignalled(bool isSignalled)
			{
				((RMCommunicator)this.containerAllocator).SetSignalled(isSignalled);
			}

			public void SetShouldUnregister(bool shouldUnregister)
			{
				((RMCommunicator)this.containerAllocator).SetShouldUnregister(shouldUnregister);
			}

			public long GetLastHeartbeatTime()
			{
				return ((RMCommunicator)this.containerAllocator).GetLastHeartbeatTime();
			}

			public void RunOnNextHeartbeat(Runnable callback)
			{
				((RMCommunicator)this.containerAllocator).RunOnNextHeartbeat(callback);
			}

			private readonly MRAppMaster _enclosing;
		}

		/// <summary>
		/// By the time life-cycle of this router starts, job-init would have already
		/// happened.
		/// </summary>
		private sealed class ContainerLauncherRouter : AbstractService, ContainerLauncher
		{
			private readonly AppContext context;

			private ContainerLauncher containerLauncher;

			internal ContainerLauncherRouter(MRAppMaster _enclosing, AppContext context)
				: base(typeof(MRAppMaster.ContainerLauncherRouter).FullName)
			{
				this._enclosing = _enclosing;
				this.context = context;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				if (this._enclosing.job.IsUber())
				{
					this.containerLauncher = new LocalContainerLauncher(this.context, (TaskUmbilicalProtocol
						)this._enclosing.taskAttemptListener);
					((LocalContainerLauncher)this.containerLauncher).SetEncryptedSpillKey(this._enclosing
						.encryptedSpillKey);
				}
				else
				{
					this.containerLauncher = new ContainerLauncherImpl(this.context);
				}
				((Org.Apache.Hadoop.Service.Service)this.containerLauncher).Init(this.GetConfig()
					);
				((Org.Apache.Hadoop.Service.Service)this.containerLauncher).Start();
				base.ServiceStart();
			}

			public void Handle(ContainerLauncherEvent @event)
			{
				this.containerLauncher.Handle(@event);
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				ServiceOperations.Stop((Org.Apache.Hadoop.Service.Service)this.containerLauncher);
				base.ServiceStop();
			}

			private readonly MRAppMaster _enclosing;
		}

		private sealed class StagingDirCleaningService : AbstractService
		{
			internal StagingDirCleaningService(MRAppMaster _enclosing)
				: base(typeof(MRAppMaster.StagingDirCleaningService).FullName)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				try
				{
					if (this._enclosing.isLastAMRetry)
					{
						this._enclosing.CleanupStagingDir();
					}
					else
					{
						MRAppMaster.Log.Info("Skipping cleaning up the staging dir. " + "assuming AM will be retried."
							);
					}
				}
				catch (IOException io)
				{
					MRAppMaster.Log.Error("Failed to cleanup staging dir: ", io);
				}
				base.ServiceStop();
			}

			private readonly MRAppMaster _enclosing;
		}

		public class RunningAppContext : AppContext
		{
			private readonly IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobs
				 = new ConcurrentHashMap<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job>();

			private readonly Configuration conf;

			private readonly ClusterInfo clusterInfo = new ClusterInfo();

			private readonly ClientToAMTokenSecretManager clientToAMTokenSecretManager;

			public RunningAppContext(MRAppMaster _enclosing, Configuration config)
			{
				this._enclosing = _enclosing;
				this.conf = config;
				this.clientToAMTokenSecretManager = new ClientToAMTokenSecretManager(this._enclosing
					.appAttemptID, null);
			}

			public virtual ApplicationAttemptId GetApplicationAttemptId()
			{
				return this._enclosing.appAttemptID;
			}

			public virtual ApplicationId GetApplicationID()
			{
				return this._enclosing.appAttemptID.GetApplicationId();
			}

			public virtual string GetApplicationName()
			{
				return this._enclosing.appName;
			}

			public virtual long GetStartTime()
			{
				return this._enclosing.startTime;
			}

			public virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetJob(JobId jobID)
			{
				return this.jobs[jobID];
			}

			public virtual IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllJobs
				()
			{
				return this.jobs;
			}

			public virtual EventHandler GetEventHandler()
			{
				return this._enclosing.dispatcher.GetEventHandler();
			}

			public virtual CharSequence GetUser()
			{
				return this.conf.Get(MRJobConfig.UserName);
			}

			public virtual Clock GetClock()
			{
				return this._enclosing.clock;
			}

			public virtual ClusterInfo GetClusterInfo()
			{
				return this.clusterInfo;
			}

			public virtual ICollection<string> GetBlacklistedNodes()
			{
				return ((RMContainerRequestor)this._enclosing.containerAllocator).GetBlacklistedNodes
					();
			}

			public virtual ClientToAMTokenSecretManager GetClientToAMTokenSecretManager()
			{
				return this.clientToAMTokenSecretManager;
			}

			public virtual bool IsLastAMRetry()
			{
				return this._enclosing.isLastAMRetry;
			}

			public virtual bool HasSuccessfullyUnregistered()
			{
				return this._enclosing.successfullyUnregistered.Get();
			}

			public virtual void MarkSuccessfulUnregistration()
			{
				this._enclosing.successfullyUnregistered.Set(true);
			}

			public virtual void ResetIsLastAMRetry()
			{
				this._enclosing.isLastAMRetry = false;
			}

			public virtual string GetNMHostname()
			{
				return this._enclosing.nmHost;
			}

			private readonly MRAppMaster _enclosing;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			amInfos = new List<AMInfo>();
			completedTasksFromPreviousRun = new Dictionary<TaskId, JobHistoryParser.TaskInfo>
				();
			ProcessRecovery();
			// Current an AMInfo for the current AM generation.
			AMInfo amInfo = MRBuilderUtils.NewAMInfo(appAttemptID, startTime, containerID, nmHost
				, nmPort, nmHttpPort);
			// /////////////////// Create the job itself.
			job = CreateJob(GetConfig(), forcedState, shutDownMessage);
			// End of creating the job.
			// Send out an MR AM inited event for all previous AMs.
			foreach (AMInfo info in amInfos)
			{
				dispatcher.GetEventHandler().Handle(new JobHistoryEvent(job.GetID(), new AMStartedEvent
					(info.GetAppAttemptId(), info.GetStartTime(), info.GetContainerId(), info.GetNodeManagerHost
					(), info.GetNodeManagerPort(), info.GetNodeManagerHttpPort(), appSubmitTime)));
			}
			// Send out an MR AM inited event for this AM.
			dispatcher.GetEventHandler().Handle(new JobHistoryEvent(job.GetID(), new AMStartedEvent
				(amInfo.GetAppAttemptId(), amInfo.GetStartTime(), amInfo.GetContainerId(), amInfo
				.GetNodeManagerHost(), amInfo.GetNodeManagerPort(), amInfo.GetNodeManagerHttpPort
				(), this.forcedState == null ? null : this.forcedState.ToString(), appSubmitTime
				)));
			amInfos.AddItem(amInfo);
			// metrics system init is really init & start.
			// It's more test friendly to put it here.
			DefaultMetricsSystem.Initialize("MRAppMaster");
			bool initFailed = false;
			if (!errorHappenedShutDown)
			{
				// create a job event for job intialization
				JobEvent initJobEvent = new JobEvent(job.GetID(), JobEventType.JobInit);
				// Send init to the job (this does NOT trigger job execution)
				// This is a synchronous call, not an event through dispatcher. We want
				// job-init to be done completely here.
				jobEventDispatcher.Handle(initJobEvent);
				// If job is still not initialized, an error happened during
				// initialization. Must complete starting all of the services so failure
				// events can be processed.
				initFailed = (((JobImpl)job).GetInternalState() != JobStateInternal.Inited);
				// JobImpl's InitTransition is done (call above is synchronous), so the
				// "uber-decision" (MR-1220) has been made.  Query job and switch to
				// ubermode if appropriate (by registering different container-allocator
				// and container-launcher services/event-handlers).
				if (job.IsUber())
				{
					speculatorEventDispatcher.DisableSpeculation();
					Log.Info("MRAppMaster uberizing job " + job.GetID() + " in local container (\"uber-AM\") on node "
						 + nmHost + ":" + nmPort + ".");
				}
				else
				{
					// send init to speculator only for non-uber jobs. 
					// This won't yet start as dispatcher isn't started yet.
					dispatcher.GetEventHandler().Handle(new SpeculatorEvent(job.GetID(), clock.GetTime
						()));
					Log.Info("MRAppMaster launching normal, non-uberized, multi-container " + "job " 
						+ job.GetID() + ".");
				}
				// Start ClientService here, since it's not initialized if
				// errorHappenedShutDown is true
				clientService.Start();
			}
			//start all the components
			base.ServiceStart();
			// finally set the job classloader
			MRApps.SetClassLoader(jobClassLoader, GetConfig());
			if (initFailed)
			{
				JobEvent initFailedEvent = new JobEvent(job.GetID(), JobEventType.JobInitFailed);
				jobEventDispatcher.Handle(initFailedEvent);
			}
			else
			{
				// All components have started, start the job.
				StartJobs();
			}
		}

		public override void Stop()
		{
			base.Stop();
			TaskLog.SyncLogsShutdown(logSyncer);
		}

		/// <exception cref="System.IO.IOException"/>
		private bool IsRecoverySupported()
		{
			bool isSupported = false;
			Configuration conf = GetConfig();
			if (committer != null)
			{
				JobContext _jobContext;
				if (newApiCommitter)
				{
					_jobContext = new JobContextImpl(conf, TypeConverter.FromYarn(GetJobId()));
				}
				else
				{
					_jobContext = new JobContextImpl(new JobConf(conf), TypeConverter.FromYarn(GetJobId
						()));
				}
				isSupported = CallWithJobClassLoader(conf, new _ExceptionAction_1143(this, _jobContext
					));
			}
			return isSupported;
		}

		private sealed class _ExceptionAction_1143 : MRAppMaster.ExceptionAction<bool>
		{
			public _ExceptionAction_1143(MRAppMaster _enclosing, JobContext _jobContext)
			{
				this._enclosing = _enclosing;
				this._jobContext = _jobContext;
			}

			/// <exception cref="System.IO.IOException"/>
			public bool Call(Configuration conf)
			{
				return this._enclosing.committer.IsRecoverySupported(_jobContext);
			}

			private readonly MRAppMaster _enclosing;

			private readonly JobContext _jobContext;
		}

		/// <exception cref="System.IO.IOException"/>
		private void ProcessRecovery()
		{
			if (appAttemptID.GetAttemptId() == 1)
			{
				return;
			}
			// no need to recover on the first attempt
			bool recoveryEnabled = GetConfig().GetBoolean(MRJobConfig.MrAmJobRecoveryEnable, 
				MRJobConfig.MrAmJobRecoveryEnableDefault);
			bool recoverySupportedByCommitter = IsRecoverySupported();
			// If a shuffle secret was not provided by the job client then this app
			// attempt will generate one.  However that disables recovery if there
			// are reducers as the shuffle secret would be app attempt specific.
			int numReduceTasks = GetConfig().GetInt(MRJobConfig.NumReduces, 0);
			bool shuffleKeyValidForRecovery = TokenCache.GetShuffleSecretKey(jobCredentials) 
				!= null;
			if (recoveryEnabled && recoverySupportedByCommitter && (numReduceTasks <= 0 || shuffleKeyValidForRecovery
				))
			{
				Log.Info("Recovery is enabled. " + "Will try to recover from previous life on best effort basis."
					);
				try
				{
					ParsePreviousJobHistory();
				}
				catch (IOException e)
				{
					Log.Warn("Unable to parse prior job history, aborting recovery", e);
					// try to get just the AMInfos
					Sharpen.Collections.AddAll(amInfos, ReadJustAMInfos());
				}
			}
			else
			{
				Log.Info("Will not try to recover. recoveryEnabled: " + recoveryEnabled + " recoverySupportedByCommitter: "
					 + recoverySupportedByCommitter + " numReduceTasks: " + numReduceTasks + " shuffleKeyValidForRecovery: "
					 + shuffleKeyValidForRecovery + " ApplicationAttemptID: " + appAttemptID.GetAttemptId
					());
				// Get the amInfos anyways whether recovery is enabled or not
				Sharpen.Collections.AddAll(amInfos, ReadJustAMInfos());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static FSDataInputStream GetPreviousJobHistoryStream(Configuration conf, 
			ApplicationAttemptId appAttemptId)
		{
			Path historyFile = JobHistoryUtils.GetPreviousJobHistoryPath(conf, appAttemptId);
			Log.Info("Previous history file is at " + historyFile);
			return historyFile.GetFileSystem(conf).Open(historyFile);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ParsePreviousJobHistory()
		{
			FSDataInputStream @in = GetPreviousJobHistoryStream(GetConfig(), appAttemptID);
			JobHistoryParser parser = new JobHistoryParser(@in);
			JobHistoryParser.JobInfo jobInfo = parser.Parse();
			Exception parseException = parser.GetParseException();
			if (parseException != null)
			{
				Log.Info("Got an error parsing job-history file" + ", ignoring incomplete events."
					, parseException);
			}
			IDictionary<TaskID, JobHistoryParser.TaskInfo> taskInfos = jobInfo.GetAllTasks();
			foreach (JobHistoryParser.TaskInfo taskInfo in taskInfos.Values)
			{
				if (TaskState.Succeeded.ToString().Equals(taskInfo.GetTaskStatus()))
				{
					IEnumerator<KeyValuePair<TaskAttemptID, JobHistoryParser.TaskAttemptInfo>> taskAttemptIterator
						 = taskInfo.GetAllTaskAttempts().GetEnumerator();
					while (taskAttemptIterator.HasNext())
					{
						KeyValuePair<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> currentEntry = taskAttemptIterator
							.Next();
						if (!jobInfo.GetAllCompletedTaskAttempts().Contains(currentEntry.Key))
						{
							taskAttemptIterator.Remove();
						}
					}
					completedTasksFromPreviousRun[TypeConverter.ToYarn(taskInfo.GetTaskId())] = taskInfo;
					Log.Info("Read from history task " + TypeConverter.ToYarn(taskInfo.GetTaskId()));
				}
			}
			Log.Info("Read completed tasks from history " + completedTasksFromPreviousRun.Count
				);
			recoveredJobStartTime = jobInfo.GetLaunchTime();
			// recover AMInfos
			IList<JobHistoryParser.AMInfo> jhAmInfoList = jobInfo.GetAMInfos();
			if (jhAmInfoList != null)
			{
				foreach (JobHistoryParser.AMInfo jhAmInfo in jhAmInfoList)
				{
					AMInfo amInfo = MRBuilderUtils.NewAMInfo(jhAmInfo.GetAppAttemptId(), jhAmInfo.GetStartTime
						(), jhAmInfo.GetContainerId(), jhAmInfo.GetNodeManagerHost(), jhAmInfo.GetNodeManagerPort
						(), jhAmInfo.GetNodeManagerHttpPort());
					amInfos.AddItem(amInfo);
				}
			}
		}

		private IList<AMInfo> ReadJustAMInfos()
		{
			IList<AMInfo> amInfos = new AList<AMInfo>();
			FSDataInputStream inputStream = null;
			try
			{
				inputStream = GetPreviousJobHistoryStream(GetConfig(), appAttemptID);
				EventReader jobHistoryEventReader = new EventReader(inputStream);
				// All AMInfos are contiguous. Track when the first AMStartedEvent
				// appears.
				bool amStartedEventsBegan = false;
				HistoryEvent @event;
				while ((@event = jobHistoryEventReader.GetNextEvent()) != null)
				{
					if (@event.GetEventType() == EventType.AmStarted)
					{
						if (!amStartedEventsBegan)
						{
							// First AMStartedEvent.
							amStartedEventsBegan = true;
						}
						AMStartedEvent amStartedEvent = (AMStartedEvent)@event;
						amInfos.AddItem(MRBuilderUtils.NewAMInfo(amStartedEvent.GetAppAttemptId(), amStartedEvent
							.GetStartTime(), amStartedEvent.GetContainerId(), StringInterner.WeakIntern(amStartedEvent
							.GetNodeManagerHost()), amStartedEvent.GetNodeManagerPort(), amStartedEvent.GetNodeManagerHttpPort
							()));
					}
					else
					{
						if (amStartedEventsBegan)
						{
							// This means AMStartedEvents began and this event is a
							// non-AMStarted event.
							// No need to continue reading all the other events.
							break;
						}
					}
				}
			}
			catch (IOException e)
			{
				Log.Warn("Could not parse the old history file. " + "Will not have old AMinfos ", 
					e);
			}
			finally
			{
				if (inputStream != null)
				{
					IOUtils.CloseQuietly(inputStream);
				}
			}
			return amInfos;
		}

		/// <summary>
		/// This can be overridden to instantiate multiple jobs and create a
		/// workflow.
		/// </summary>
		/// <remarks>
		/// This can be overridden to instantiate multiple jobs and create a
		/// workflow.
		/// TODO:  Rework the design to actually support this.  Currently much of the
		/// job stuff has been moved to init() above to support uberization (MR-1220).
		/// In a typical workflow, one presumably would want to uberize only a subset
		/// of the jobs (the "small" ones), which is awkward with the current design.
		/// </remarks>
		protected internal virtual void StartJobs()
		{
			JobEvent startJobEvent = new JobStartEvent(job.GetID(), recoveredJobStartTime);
			dispatcher.GetEventHandler().Handle(startJobEvent);
		}

		private class JobEventDispatcher : EventHandler<JobEvent>
		{
			public virtual void Handle(JobEvent @event)
			{
				((EventHandler<JobEvent>)this._enclosing.context.GetJob(@event.GetJobId())).Handle
					(@event);
			}

			internal JobEventDispatcher(MRAppMaster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MRAppMaster _enclosing;
		}

		private class TaskEventDispatcher : EventHandler<TaskEvent>
		{
			public virtual void Handle(TaskEvent @event)
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task task = this._enclosing.context.GetJob
					(@event.GetTaskID().GetJobId()).GetTask(@event.GetTaskID());
				((EventHandler<TaskEvent>)task).Handle(@event);
			}

			internal TaskEventDispatcher(MRAppMaster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MRAppMaster _enclosing;
		}

		private class TaskAttemptEventDispatcher : EventHandler<TaskAttemptEvent>
		{
			public virtual void Handle(TaskAttemptEvent @event)
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this._enclosing.context.GetJob(@event
					.GetTaskAttemptID().GetTaskId().GetJobId());
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Task task = job.GetTask(@event.GetTaskAttemptID
					().GetTaskId());
				TaskAttempt attempt = task.GetAttempt(@event.GetTaskAttemptID());
				((EventHandler<TaskAttemptEvent>)attempt).Handle(@event);
			}

			internal TaskAttemptEventDispatcher(MRAppMaster _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MRAppMaster _enclosing;
		}

		private class SpeculatorEventDispatcher : EventHandler<SpeculatorEvent>
		{
			private readonly Configuration conf;

			private volatile bool disabled;

			public SpeculatorEventDispatcher(MRAppMaster _enclosing, Configuration config)
			{
				this._enclosing = _enclosing;
				this.conf = config;
			}

			public virtual void Handle(SpeculatorEvent @event)
			{
				if (this.disabled)
				{
					return;
				}
				TaskId tId = @event.GetTaskID();
				TaskType tType = null;
				/* event's TaskId will be null if the event type is JOB_CREATE or
				* ATTEMPT_STATUS_UPDATE
				*/
				if (tId != null)
				{
					tType = tId.GetTaskType();
				}
				bool shouldMapSpec = this.conf.GetBoolean(MRJobConfig.MapSpeculative, false);
				bool shouldReduceSpec = this.conf.GetBoolean(MRJobConfig.ReduceSpeculative, false
					);
				/* The point of the following is to allow the MAP and REDUCE speculative
				* config values to be independent:
				* IF spec-exec is turned on for maps AND the task is a map task
				* OR IF spec-exec is turned on for reduces AND the task is a reduce task
				* THEN call the speculator to handle the event.
				*/
				if ((shouldMapSpec && (tType == null || tType == TaskType.Map)) || (shouldReduceSpec
					 && (tType == null || tType == TaskType.Reduce)))
				{
					// Speculator IS enabled, direct the event to there.
					this._enclosing.CallWithJobClassLoader(this.conf, new _Action_1373(this, @event));
				}
			}

			private sealed class _Action_1373 : MRAppMaster.Action<Void>
			{
				public _Action_1373(SpeculatorEventDispatcher _enclosing, SpeculatorEvent @event)
				{
					this._enclosing = _enclosing;
					this.@event = @event;
				}

				public Void Call(Configuration conf)
				{
					this._enclosing._enclosing.speculator.Handle(@event);
					return null;
				}

				private readonly SpeculatorEventDispatcher _enclosing;

				private readonly SpeculatorEvent @event;
			}

			public virtual void DisableSpeculation()
			{
				this.disabled = true;
			}

			private readonly MRAppMaster _enclosing;
		}

		/// <summary>Eats events that are not needed in some error cases.</summary>
		private class NoopEventHandler : EventHandler<Org.Apache.Hadoop.Yarn.Event.Event>
		{
			public virtual void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
			}
			//Empty
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ValidateInputParam(string value, string param)
		{
			if (value == null)
			{
				string msg = param + " is null";
				Log.Error(msg);
				throw new IOException(msg);
			}
		}

		public static void Main(string[] args)
		{
			try
			{
				Sharpen.Thread.SetDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler
					());
				string containerIdStr = Runtime.Getenv(ApplicationConstants.Environment.ContainerId
					.ToString());
				string nodeHostString = Runtime.Getenv(ApplicationConstants.Environment.NmHost.ToString
					());
				string nodePortString = Runtime.Getenv(ApplicationConstants.Environment.NmPort.ToString
					());
				string nodeHttpPortString = Runtime.Getenv(ApplicationConstants.Environment.NmHttpPort
					.ToString());
				string appSubmitTimeStr = Runtime.Getenv(ApplicationConstants.AppSubmitTimeEnv);
				ValidateInputParam(containerIdStr, ApplicationConstants.Environment.ContainerId.ToString
					());
				ValidateInputParam(nodeHostString, ApplicationConstants.Environment.NmHost.ToString
					());
				ValidateInputParam(nodePortString, ApplicationConstants.Environment.NmPort.ToString
					());
				ValidateInputParam(nodeHttpPortString, ApplicationConstants.Environment.NmHttpPort
					.ToString());
				ValidateInputParam(appSubmitTimeStr, ApplicationConstants.AppSubmitTimeEnv);
				ContainerId containerId = ConverterUtils.ToContainerId(containerIdStr);
				ApplicationAttemptId applicationAttemptId = containerId.GetApplicationAttemptId();
				long appSubmitTime = long.Parse(appSubmitTimeStr);
				MRAppMaster appMaster = new MRAppMaster(applicationAttemptId, containerId, nodeHostString
					, System.Convert.ToInt32(nodePortString), System.Convert.ToInt32(nodeHttpPortString
					), appSubmitTime);
				ShutdownHookManager.Get().AddShutdownHook(new MRAppMaster.MRAppMasterShutdownHook
					(appMaster), ShutdownHookPriority);
				JobConf conf = new JobConf(new YarnConfiguration());
				conf.AddResource(new Path(MRJobConfig.JobConfFile));
				MRWebAppUtil.Initialize(conf);
				string jobUserName = Runtime.Getenv(ApplicationConstants.Environment.User.ToString
					());
				conf.Set(MRJobConfig.UserName, jobUserName);
				InitAndStartAppMaster(appMaster, conf, jobUserName);
			}
			catch (Exception t)
			{
				Log.Fatal("Error starting MRAppMaster", t);
				ExitUtil.Terminate(1, t);
			}
		}

		internal class MRAppMasterShutdownHook : Runnable
		{
			internal MRAppMaster appMaster;

			internal MRAppMasterShutdownHook(MRAppMaster appMaster)
			{
				// The shutdown hook that runs when a signal is received AND during normal
				// close of the JVM.
				this.appMaster = appMaster;
			}

			public virtual void Run()
			{
				Log.Info("MRAppMaster received a signal. Signaling RMCommunicator and " + "JobHistoryEventHandler."
					);
				// Notify the JHEH and RMCommunicator that a SIGTERM has been received so
				// that they don't take too long in shutting down
				if (appMaster.containerAllocator is MRAppMaster.ContainerAllocatorRouter)
				{
					((MRAppMaster.ContainerAllocatorRouter)appMaster.containerAllocator).SetSignalled
						(true);
				}
				appMaster.NotifyIsLastAMRetry(appMaster.isLastAMRetry);
				appMaster.Stop();
			}
		}

		public virtual void NotifyIsLastAMRetry(bool isLastAMRetry)
		{
			if (containerAllocator is MRAppMaster.ContainerAllocatorRouter)
			{
				Log.Info("Notify RMCommunicator isAMLastRetry: " + isLastAMRetry);
				((MRAppMaster.ContainerAllocatorRouter)containerAllocator).SetShouldUnregister(isLastAMRetry
					);
			}
			if (jobHistoryEventHandler != null)
			{
				Log.Info("Notify JHEH isAMLastRetry: " + isLastAMRetry);
				jobHistoryEventHandler.SetForcejobCompletion(isLastAMRetry);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal static void InitAndStartAppMaster(MRAppMaster appMaster, JobConf
			 conf, string jobUserName)
		{
			UserGroupInformation.SetConfiguration(conf);
			// Security framework already loaded the tokens into current UGI, just use
			// them
			Credentials credentials = UserGroupInformation.GetCurrentUser().GetCredentials();
			Log.Info("Executing with tokens:");
			foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in credentials.GetAllTokens
				())
			{
				Log.Info(token);
			}
			UserGroupInformation appMasterUgi = UserGroupInformation.CreateRemoteUser(jobUserName
				);
			appMasterUgi.AddCredentials(credentials);
			// Now remove the AM->RM token so tasks don't have it
			IEnumerator<Org.Apache.Hadoop.Security.Token.Token<object>> iter = credentials.GetAllTokens
				().GetEnumerator();
			while (iter.HasNext())
			{
				Org.Apache.Hadoop.Security.Token.Token<object> token_1 = iter.Next();
				if (token_1.GetKind().Equals(AMRMTokenIdentifier.KindName))
				{
					iter.Remove();
				}
			}
			conf.GetCredentials().AddAll(credentials);
			appMasterUgi.DoAs(new _PrivilegedExceptionAction_1515(appMaster, conf));
		}

		private sealed class _PrivilegedExceptionAction_1515 : PrivilegedExceptionAction<
			object>
		{
			public _PrivilegedExceptionAction_1515(MRAppMaster appMaster, JobConf conf)
			{
				this.appMaster = appMaster;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				appMaster.Init(conf);
				appMaster.Start();
				if (appMaster.errorHappenedShutDown)
				{
					throw new IOException("Was asked to shut down.");
				}
				return null;
			}

			private readonly MRAppMaster appMaster;

			private readonly JobConf conf;
		}

		/// <summary>
		/// Creates a job classloader based on the configuration if the job classloader
		/// is enabled.
		/// </summary>
		/// <remarks>
		/// Creates a job classloader based on the configuration if the job classloader
		/// is enabled. It is a no-op if the job classloader is not enabled.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void CreateJobClassLoader(Configuration conf)
		{
			jobClassLoader = MRApps.CreateJobClassLoader(conf);
		}

		/// <summary>
		/// Executes the given action with the job classloader set as the configuration
		/// classloader as well as the thread context class loader if the job
		/// classloader is enabled.
		/// </summary>
		/// <remarks>
		/// Executes the given action with the job classloader set as the configuration
		/// classloader as well as the thread context class loader if the job
		/// classloader is enabled. After the call, the original classloader is
		/// restored.
		/// If the job classloader is enabled and the code needs to load user-supplied
		/// classes via configuration or thread context classloader, this method should
		/// be used in order to load them.
		/// </remarks>
		/// <param name="conf">the configuration on which the classloader will be set</param>
		/// <param name="action">the callable action to be executed</param>
		internal virtual T CallWithJobClassLoader<T>(Configuration conf, MRAppMaster.Action
			<T> action)
		{
			// if the job classloader is enabled, we may need it to load the (custom)
			// classes; we make the job classloader available and unset it once it is
			// done
			ClassLoader currentClassLoader = conf.GetClassLoader();
			bool setJobClassLoader = jobClassLoader != null && currentClassLoader != jobClassLoader;
			if (setJobClassLoader)
			{
				MRApps.SetClassLoader(jobClassLoader, conf);
			}
			try
			{
				return action.Call(conf);
			}
			finally
			{
				if (setJobClassLoader)
				{
					// restore the original classloader
					MRApps.SetClassLoader(currentClassLoader, conf);
				}
			}
		}

		/// <summary>
		/// Executes the given action that can throw a checked exception with the job
		/// classloader set as the configuration classloader as well as the thread
		/// context class loader if the job classloader is enabled.
		/// </summary>
		/// <remarks>
		/// Executes the given action that can throw a checked exception with the job
		/// classloader set as the configuration classloader as well as the thread
		/// context class loader if the job classloader is enabled. After the call, the
		/// original classloader is restored.
		/// If the job classloader is enabled and the code needs to load user-supplied
		/// classes via configuration or thread context classloader, this method should
		/// be used in order to load them.
		/// </remarks>
		/// <param name="conf">the configuration on which the classloader will be set</param>
		/// <param name="action">the callable action to be executed</param>
		/// <exception cref="System.IO.IOException">if the underlying action throws an IOException
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnRuntimeException">
		/// if the underlying action throws an exception
		/// other than an IOException
		/// </exception>
		internal virtual T CallWithJobClassLoader<T>(Configuration conf, MRAppMaster.ExceptionAction
			<T> action)
		{
			// if the job classloader is enabled, we may need it to load the (custom)
			// classes; we make the job classloader available and unset it once it is
			// done
			ClassLoader currentClassLoader = conf.GetClassLoader();
			bool setJobClassLoader = jobClassLoader != null && currentClassLoader != jobClassLoader;
			if (setJobClassLoader)
			{
				MRApps.SetClassLoader(jobClassLoader, conf);
			}
			try
			{
				return action.Call(conf);
			}
			catch (IOException e)
			{
				throw;
			}
			catch (YarnRuntimeException e)
			{
				throw;
			}
			catch (Exception e)
			{
				// wrap it with a YarnRuntimeException
				throw new YarnRuntimeException(e);
			}
			finally
			{
				if (setJobClassLoader)
				{
					// restore the original classloader
					MRApps.SetClassLoader(currentClassLoader, conf);
				}
			}
		}

		/// <summary>Action to be wrapped with setting and unsetting the job classloader</summary>
		private interface Action<T>
		{
			T Call(Configuration conf);
		}

		private interface ExceptionAction<T>
		{
			/// <exception cref="System.Exception"/>
			T Call(Configuration conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			base.ServiceStop();
			LogManager.Shutdown();
		}

		public virtual ClientService GetClientService()
		{
			return clientService;
		}
	}
}
