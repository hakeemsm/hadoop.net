using System;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Commit;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.App.Launcher;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.State;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary>Mock MRAppMaster.</summary>
	/// <remarks>
	/// Mock MRAppMaster. Doesn't start RPC servers.
	/// No threads are started except of the event Dispatcher thread.
	/// </remarks>
	public class MRApp : MRAppMaster
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.MRApp
			));

		internal int maps;

		internal int reduces;

		private FilePath testWorkDir;

		private Path testAbsPath;

		private ClusterInfo clusterInfo;

		private string assignedQueue;

		public static string NmHost = "localhost";

		public static int NmPort = 1234;

		public static int NmHttpPort = 8042;

		protected internal bool autoComplete = false;

		internal static ApplicationId applicationId;

		static MRApp()
		{
			// Queue to pretend the RM assigned us
			//if true, tasks complete automatically as soon as they are launched
			applicationId = ApplicationId.NewInstance(0, 0);
		}

		public MRApp(int maps, int reduces, bool autoComplete, string testName, bool cleanOnStart
			, Clock clock)
			: this(maps, reduces, autoComplete, testName, cleanOnStart, 1, clock, null)
		{
		}

		public MRApp(int maps, int reduces, bool autoComplete, string testName, bool cleanOnStart
			, Clock clock, bool unregistered)
			: this(maps, reduces, autoComplete, testName, cleanOnStart, 1, clock, unregistered
				)
		{
		}

		public MRApp(int maps, int reduces, bool autoComplete, string testName, bool cleanOnStart
			)
			: this(maps, reduces, autoComplete, testName, cleanOnStart, 1)
		{
		}

		public MRApp(int maps, int reduces, bool autoComplete, string testName, bool cleanOnStart
			, string assignedQueue)
			: this(maps, reduces, autoComplete, testName, cleanOnStart, 1, new SystemClock(), 
				assignedQueue)
		{
		}

		public MRApp(int maps, int reduces, bool autoComplete, string testName, bool cleanOnStart
			, bool unregistered)
			: this(maps, reduces, autoComplete, testName, cleanOnStart, 1, unregistered)
		{
		}

		protected internal override void InitJobCredentialsAndUGI(Configuration conf)
		{
			// Fake a shuffle secret that normally is provided by the job client.
			string shuffleSecret = "fake-shuffle-secret";
			TokenCache.SetShuffleSecretKey(Sharpen.Runtime.GetBytesForString(shuffleSecret), 
				GetCredentials());
		}

		private static ApplicationAttemptId GetApplicationAttemptId(ApplicationId applicationId
			, int startCount)
		{
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, startCount);
			return applicationAttemptId;
		}

		private static ContainerId GetContainerId(ApplicationId applicationId, int startCount
			)
		{
			ApplicationAttemptId appAttemptId = GetApplicationAttemptId(applicationId, startCount
				);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, startCount);
			return containerId;
		}

		public MRApp(int maps, int reduces, bool autoComplete, string testName, bool cleanOnStart
			, int startCount)
			: this(maps, reduces, autoComplete, testName, cleanOnStart, startCount, new SystemClock
				(), null)
		{
		}

		public MRApp(int maps, int reduces, bool autoComplete, string testName, bool cleanOnStart
			, int startCount, bool unregistered)
			: this(maps, reduces, autoComplete, testName, cleanOnStart, startCount, new SystemClock
				(), unregistered)
		{
		}

		public MRApp(int maps, int reduces, bool autoComplete, string testName, bool cleanOnStart
			, int startCount, Clock clock, bool unregistered)
			: this(GetApplicationAttemptId(applicationId, startCount), GetContainerId(applicationId
				, startCount), maps, reduces, autoComplete, testName, cleanOnStart, startCount, 
				clock, unregistered, null)
		{
		}

		public MRApp(int maps, int reduces, bool autoComplete, string testName, bool cleanOnStart
			, int startCount, Clock clock, string assignedQueue)
			: this(GetApplicationAttemptId(applicationId, startCount), GetContainerId(applicationId
				, startCount), maps, reduces, autoComplete, testName, cleanOnStart, startCount, 
				clock, true, assignedQueue)
		{
		}

		public MRApp(ApplicationAttemptId appAttemptId, ContainerId amContainerId, int maps
			, int reduces, bool autoComplete, string testName, bool cleanOnStart, int startCount
			, bool unregistered)
			: this(appAttemptId, amContainerId, maps, reduces, autoComplete, testName, cleanOnStart
				, startCount, new SystemClock(), unregistered, null)
		{
		}

		public MRApp(ApplicationAttemptId appAttemptId, ContainerId amContainerId, int maps
			, int reduces, bool autoComplete, string testName, bool cleanOnStart, int startCount
			)
			: this(appAttemptId, amContainerId, maps, reduces, autoComplete, testName, cleanOnStart
				, startCount, new SystemClock(), true, null)
		{
		}

		public MRApp(ApplicationAttemptId appAttemptId, ContainerId amContainerId, int maps
			, int reduces, bool autoComplete, string testName, bool cleanOnStart, int startCount
			, Clock clock, bool unregistered, string assignedQueue)
			: base(appAttemptId, amContainerId, NmHost, NmPort, NmHttpPort, clock, Runtime.CurrentTimeMillis
				())
		{
			this.testWorkDir = new FilePath("target", testName);
			testAbsPath = new Path(testWorkDir.GetAbsolutePath());
			Log.Info("PathUsed: " + testAbsPath);
			if (cleanOnStart)
			{
				testAbsPath = new Path(testWorkDir.GetAbsolutePath());
				try
				{
					FileContext.GetLocalFSFileContext().Delete(testAbsPath, true);
				}
				catch (Exception e)
				{
					Log.Warn("COULD NOT CLEANUP: " + testAbsPath, e);
					throw new YarnRuntimeException("could not cleanup test dir", e);
				}
			}
			this.maps = maps;
			this.reduces = reduces;
			this.autoComplete = autoComplete;
			// If safeToReportTerminationToUser is set to true, we can verify whether
			// the job can reaches the final state when MRAppMaster shuts down.
			this.successfullyUnregistered.Set(unregistered);
			this.assignedQueue = assignedQueue;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			try
			{
				//Create the staging directory if it does not exist
				string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
				Path stagingDir = MRApps.GetStagingAreaDir(conf, user);
				FileSystem fs = GetFileSystem(conf);
				fs.Mkdirs(stagingDir);
			}
			catch (Exception e)
			{
				throw new YarnRuntimeException("Error creating staging dir", e);
			}
			base.ServiceInit(conf);
			if (this.clusterInfo != null)
			{
				GetContext().GetClusterInfo().SetMaxContainerCapability(this.clusterInfo.GetMaxContainerCapability
					());
			}
			else
			{
				GetContext().GetClusterInfo().SetMaxContainerCapability(Resource.NewInstance(10240
					, 1));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job Submit(Configuration conf
			)
		{
			//TODO: fix the bug where the speculator gets events with 
			//not-fully-constructed objects. For now, disable speculative exec
			return Submit(conf, false, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job Submit(Configuration conf
			, bool mapSpeculative, bool reduceSpeculative)
		{
			string user = conf.Get(MRJobConfig.UserName, UserGroupInformation.GetCurrentUser(
				).GetShortUserName());
			conf.Set(MRJobConfig.UserName, user);
			conf.Set(MRJobConfig.MrAmStagingDir, testAbsPath.ToString());
			conf.SetBoolean(MRJobConfig.MrAmCreateJhIntermediateBaseDir, true);
			// TODO: fix the bug where the speculator gets events with
			// not-fully-constructed objects. For now, disable speculative exec
			conf.SetBoolean(MRJobConfig.MapSpeculative, mapSpeculative);
			conf.SetBoolean(MRJobConfig.ReduceSpeculative, reduceSpeculative);
			Init(conf);
			Start();
			DefaultMetricsSystem.Shutdown();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetContext().GetAllJobs().Values
				.GetEnumerator().Next();
			if (assignedQueue != null)
			{
				job.SetQueueName(assignedQueue);
			}
			// Write job.xml
			string jobFile = MRApps.GetJobFile(conf, user, TypeConverter.FromYarn(job.GetID()
				));
			Log.Info("Writing job conf to " + jobFile);
			new FilePath(jobFile).GetParentFile().Mkdirs();
			conf.WriteXml(new FileOutputStream(jobFile));
			return job;
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForInternalState(JobImpl job, JobStateInternal finalState
			)
		{
			int timeoutSecs = 0;
			JobStateInternal iState = job.GetInternalState();
			while (!finalState.Equals(iState) && timeoutSecs++ < 20)
			{
				System.Console.Out.WriteLine("Job Internal State is : " + iState + " Waiting for Internal state : "
					 + finalState);
				Sharpen.Thread.Sleep(500);
				iState = job.GetInternalState();
			}
			System.Console.Out.WriteLine("Task Internal State is : " + iState);
			NUnit.Framework.Assert.AreEqual("Task Internal state is not correct (timedout)", 
				finalState, iState);
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForInternalState(TaskImpl task, TaskStateInternal finalState
			)
		{
			int timeoutSecs = 0;
			TaskReport report = task.GetReport();
			TaskStateInternal iState = task.GetInternalState();
			while (!finalState.Equals(iState) && timeoutSecs++ < 20)
			{
				System.Console.Out.WriteLine("Task Internal State is : " + iState + " Waiting for Internal state : "
					 + finalState + "   progress : " + report.GetProgress());
				Sharpen.Thread.Sleep(500);
				report = task.GetReport();
				iState = task.GetInternalState();
			}
			System.Console.Out.WriteLine("Task Internal State is : " + iState);
			NUnit.Framework.Assert.AreEqual("Task Internal state is not correct (timedout)", 
				finalState, iState);
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForInternalState(TaskAttemptImpl attempt, TaskAttemptStateInternal
			 finalState)
		{
			int timeoutSecs = 0;
			TaskAttemptReport report = attempt.GetReport();
			TaskAttemptStateInternal iState = attempt.GetInternalState();
			while (!finalState.Equals(iState) && timeoutSecs++ < 20)
			{
				System.Console.Out.WriteLine("TaskAttempt Internal State is : " + iState + " Waiting for Internal state : "
					 + finalState + "   progress : " + report.GetProgress());
				Sharpen.Thread.Sleep(500);
				report = attempt.GetReport();
				iState = attempt.GetInternalState();
			}
			System.Console.Out.WriteLine("TaskAttempt Internal State is : " + iState);
			NUnit.Framework.Assert.AreEqual("TaskAttempt Internal state is not correct (timedout)"
				, finalState, iState);
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForState(TaskAttempt attempt, TaskAttemptState finalState
			)
		{
			int timeoutSecs = 0;
			TaskAttemptReport report = attempt.GetReport();
			while (!finalState.Equals(report.GetTaskAttemptState()) && timeoutSecs++ < 20)
			{
				System.Console.Out.WriteLine("TaskAttempt State is : " + report.GetTaskAttemptState
					() + " Waiting for state : " + finalState + "   progress : " + report.GetProgress
					());
				report = attempt.GetReport();
				Sharpen.Thread.Sleep(500);
			}
			System.Console.Out.WriteLine("TaskAttempt State is : " + report.GetTaskAttemptState
				());
			NUnit.Framework.Assert.AreEqual("TaskAttempt state is not correct (timedout)", finalState
				, report.GetTaskAttemptState());
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForState(Task task, TaskState finalState)
		{
			int timeoutSecs = 0;
			TaskReport report = task.GetReport();
			while (!finalState.Equals(report.GetTaskState()) && timeoutSecs++ < 20)
			{
				System.Console.Out.WriteLine("Task State for " + task.GetID() + " is : " + report
					.GetTaskState() + " Waiting for state : " + finalState + "   progress : " + report
					.GetProgress());
				report = task.GetReport();
				Sharpen.Thread.Sleep(500);
			}
			System.Console.Out.WriteLine("Task State is : " + report.GetTaskState());
			NUnit.Framework.Assert.AreEqual("Task state is not correct (timedout)", finalState
				, report.GetTaskState());
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForState(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job, 
			JobState finalState)
		{
			int timeoutSecs = 0;
			JobReport report = job.GetReport();
			while (!finalState.Equals(report.GetJobState()) && timeoutSecs++ < 20)
			{
				System.Console.Out.WriteLine("Job State is : " + report.GetJobState() + " Waiting for state : "
					 + finalState + "   map progress : " + report.GetMapProgress() + "   reduce progress : "
					 + report.GetReduceProgress());
				report = job.GetReport();
				Sharpen.Thread.Sleep(500);
			}
			System.Console.Out.WriteLine("Job State is : " + report.GetJobState());
			NUnit.Framework.Assert.AreEqual("Job state is not correct (timedout)", finalState
				, job.GetState());
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForState(Service.STATE finalState)
		{
			if (finalState == Service.STATE.Stopped)
			{
				NUnit.Framework.Assert.IsTrue("Timeout while waiting for MRApp to stop", WaitForServiceToStop
					(20 * 1000));
			}
			else
			{
				int timeoutSecs = 0;
				while (!finalState.Equals(GetServiceState()) && timeoutSecs++ < 20)
				{
					System.Console.Out.WriteLine("MRApp State is : " + GetServiceState() + " Waiting for state : "
						 + finalState);
					Sharpen.Thread.Sleep(500);
				}
				System.Console.Out.WriteLine("MRApp State is : " + GetServiceState());
				NUnit.Framework.Assert.AreEqual("MRApp state is not correct (timedout)", finalState
					, GetServiceState());
			}
		}

		public virtual void VerifyCompleted()
		{
			foreach (Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job in GetContext().GetAllJobs
				().Values)
			{
				JobReport jobReport = job.GetReport();
				System.Console.Out.WriteLine("Job start time :" + jobReport.GetStartTime());
				System.Console.Out.WriteLine("Job finish time :" + jobReport.GetFinishTime());
				NUnit.Framework.Assert.IsTrue("Job start time is not less than finish time", jobReport
					.GetStartTime() <= jobReport.GetFinishTime());
				NUnit.Framework.Assert.IsTrue("Job finish time is in future", jobReport.GetFinishTime
					() <= Runtime.CurrentTimeMillis());
				foreach (Task task in job.GetTasks().Values)
				{
					TaskReport taskReport = task.GetReport();
					System.Console.Out.WriteLine("Task start time : " + taskReport.GetStartTime());
					System.Console.Out.WriteLine("Task finish time : " + taskReport.GetFinishTime());
					NUnit.Framework.Assert.IsTrue("Task start time is not less than finish time", taskReport
						.GetStartTime() <= taskReport.GetFinishTime());
					foreach (TaskAttempt attempt in task.GetAttempts().Values)
					{
						TaskAttemptReport attemptReport = attempt.GetReport();
						NUnit.Framework.Assert.IsTrue("Attempt start time is not less than finish time", 
							attemptReport.GetStartTime() <= attemptReport.GetFinishTime());
					}
				}
			}
		}

		protected internal override Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job CreateJob(
			Configuration conf, JobStateInternal forcedState, string diagnostic)
		{
			UserGroupInformation currentUser = null;
			try
			{
				currentUser = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job newJob = new MRApp.TestJob(this, GetJobId
				(), GetAttemptID(), conf, GetDispatcher().GetEventHandler(), GetTaskAttemptListener
				(), GetContext().GetClock(), GetCommitter(), IsNewApiCommitter(), currentUser.GetUserName
				(), GetContext(), forcedState, diagnostic);
			((AppContext)GetContext()).GetAllJobs()[newJob.GetID()] = newJob;
			GetDispatcher().Register(typeof(JobFinishEvent.Type), new _EventHandler_473(this)
				);
			return newJob;
		}

		private sealed class _EventHandler_473 : EventHandler<JobFinishEvent>
		{
			public _EventHandler_473(MRApp _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Handle(JobFinishEvent @event)
			{
				this._enclosing.Stop();
			}

			private readonly MRApp _enclosing;
		}

		protected internal override TaskAttemptListener CreateTaskAttemptListener(AppContext
			 context)
		{
			return new _TaskAttemptListener_485();
		}

		private sealed class _TaskAttemptListener_485 : TaskAttemptListener
		{
			public _TaskAttemptListener_485()
			{
			}

			public IPEndPoint GetAddress()
			{
				return NetUtils.CreateSocketAddr("localhost:54321");
			}

			public void RegisterLaunchedTask(TaskAttemptId attemptID, WrappedJvmID jvmID)
			{
			}

			public void Unregister(TaskAttemptId attemptID, WrappedJvmID jvmID)
			{
			}

			public void RegisterPendingTask(Task task, WrappedJvmID jvmID)
			{
			}
		}

		protected internal override EventHandler<JobHistoryEvent> CreateJobHistoryHandler
			(AppContext context)
		{
			//disable history
			return new _EventHandler_507();
		}

		private sealed class _EventHandler_507 : EventHandler<JobHistoryEvent>
		{
			public _EventHandler_507()
			{
			}

			public void Handle(JobHistoryEvent @event)
			{
			}
		}

		protected internal override ContainerLauncher CreateContainerLauncher(AppContext 
			context)
		{
			return new MRApp.MockContainerLauncher(this);
		}

		protected internal class MockContainerLauncher : ContainerLauncher
		{
			internal int shufflePort = -1;

			public MockContainerLauncher(MRApp _enclosing)
			{
				this._enclosing = _enclosing;
			}

			//We are running locally so set the shuffle port to -1 
			public virtual void Handle(ContainerLauncherEvent @event)
			{
				switch (@event.GetType())
				{
					case ContainerLauncher.EventType.ContainerRemoteLaunch:
					{
						this._enclosing.ContainerLaunched(@event.GetTaskAttemptID(), this.shufflePort);
						this._enclosing.AttemptLaunched(@event.GetTaskAttemptID());
						break;
					}

					case ContainerLauncher.EventType.ContainerRemoteCleanup:
					{
						this._enclosing.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(@event
							.GetTaskAttemptID(), TaskAttemptEventType.TaContainerCleaned));
						break;
					}
				}
			}

			private readonly MRApp _enclosing;
		}

		protected internal virtual void ContainerLaunched(TaskAttemptId attemptID, int shufflePort
			)
		{
			GetContext().GetEventHandler().Handle(new TaskAttemptContainerLaunchedEvent(attemptID
				, shufflePort));
		}

		protected internal virtual void AttemptLaunched(TaskAttemptId attemptID)
		{
			if (autoComplete)
			{
				// send the done event
				GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
					.TaDone));
			}
		}

		protected internal override ContainerAllocator CreateContainerAllocator(ClientService
			 clientService, AppContext context)
		{
			return new MRApp.MRAppContainerAllocator(this);
		}

		protected internal class MRAppContainerAllocator : ContainerAllocator, RMHeartbeatHandler
		{
			private int containerCount;

			public virtual void Handle(ContainerAllocatorEvent @event)
			{
				ContainerId cId = ContainerId.NewContainerId(this._enclosing.GetContext().GetApplicationAttemptId
					(), this.containerCount++);
				NodeId nodeId = NodeId.NewInstance(MRApp.NmHost, MRApp.NmPort);
				Resource resource = Resource.NewInstance(1234, 2);
				ContainerTokenIdentifier containerTokenIdentifier = new ContainerTokenIdentifier(
					cId, nodeId.ToString(), "user", resource, Runtime.CurrentTimeMillis() + 10000, 42
					, 42, Priority.NewInstance(0), 0);
				Token containerToken = MRApp.NewContainerToken(nodeId, Sharpen.Runtime.GetBytesForString
					("password"), containerTokenIdentifier);
				Container container = Container.NewInstance(cId, nodeId, MRApp.NmHost + ":" + MRApp
					.NmHttpPort, resource, null, containerToken);
				JobID id = TypeConverter.FromYarn(MRApp.applicationId);
				JobId jobId = TypeConverter.ToYarn(id);
				this._enclosing.GetContext().GetEventHandler().Handle(new JobHistoryEvent(jobId, 
					new NormalizedResourceEvent(TaskType.Reduce, 100)));
				this._enclosing.GetContext().GetEventHandler().Handle(new JobHistoryEvent(jobId, 
					new NormalizedResourceEvent(TaskType.Map, 100)));
				this._enclosing.GetContext().GetEventHandler().Handle(new TaskAttemptContainerAssignedEvent
					(@event.GetAttemptID(), container, null));
			}

			public virtual long GetLastHeartbeatTime()
			{
				return this._enclosing.GetContext().GetClock().GetTime();
			}

			public virtual void RunOnNextHeartbeat(Runnable callback)
			{
				callback.Run();
			}

			internal MRAppContainerAllocator(MRApp _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MRApp _enclosing;
		}

		protected internal override EventHandler<CommitterEvent> CreateCommitterEventHandler
			(AppContext context, OutputCommitter committer)
		{
			// create an output committer with the task methods stubbed out
			OutputCommitter stubbedCommitter = new _OutputCommitter_613(committer);
			return new CommitterEventHandler(context, stubbedCommitter, GetRMHeartbeatHandler
				());
		}

		private sealed class _OutputCommitter_613 : OutputCommitter
		{
			public _OutputCommitter_613(OutputCommitter committer)
			{
				this.committer = committer;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetupJob(JobContext jobContext)
			{
				committer.SetupJob(jobContext);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CleanupJob(JobContext jobContext)
			{
				committer.CleanupJob(jobContext);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CommitJob(JobContext jobContext)
			{
				committer.CommitJob(jobContext);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AbortJob(JobContext jobContext, JobStatus.State state)
			{
				committer.AbortJob(jobContext, state);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool IsRecoverySupported(JobContext jobContext)
			{
				return committer.IsRecoverySupported(jobContext);
			}

			public override bool IsRecoverySupported()
			{
				return committer.IsRecoverySupported();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetupTask(TaskAttemptContext taskContext)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool NeedsTaskCommit(TaskAttemptContext taskContext)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CommitTask(TaskAttemptContext taskContext)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AbortTask(TaskAttemptContext taskContext)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RecoverTask(TaskAttemptContext taskContext)
			{
			}

			private readonly OutputCommitter committer;
		}

		protected internal override ClientService CreateClientService(AppContext context)
		{
			return new _MRClientService_673(context);
		}

		private sealed class _MRClientService_673 : MRClientService
		{
			public _MRClientService_673(AppContext baseArg1)
				: base(baseArg1)
			{
			}

			public override IPEndPoint GetBindAddress()
			{
				return NetUtils.CreateSocketAddr("localhost:9876");
			}

			public override int GetHttpPort()
			{
				return -1;
			}
		}

		public virtual void SetClusterInfo(ClusterInfo clusterInfo)
		{
			// Only useful if set before a job is started.
			if (GetServiceState() == Service.STATE.Notinited || GetServiceState() == Service.STATE
				.Inited)
			{
				this.clusterInfo = clusterInfo;
			}
			else
			{
				throw new InvalidOperationException("ClusterInfo can only be set before the App is STARTED"
					);
			}
		}

		internal class TestJob : JobImpl
		{
			private readonly MRApp.TestInitTransition initTransition = new MRApp.TestInitTransition
				(this._enclosing.maps, this._enclosing.reduces);

			internal StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent> localFactory
				 = JobImpl.stateMachineFactory.AddTransition(JobStateInternal.New, EnumSet.Of(JobStateInternal
				.Inited, JobStateInternal.Failed), JobEventType.JobInit, this.initTransition);

			private readonly StateMachine<JobStateInternal, JobEventType, JobEvent> localStateMachine;

			//override the init transition
			// This is abusive.
			protected internal override StateMachine<JobStateInternal, JobEventType, JobEvent
				> GetStateMachine()
			{
				return this.localStateMachine;
			}

			public TestJob(MRApp _enclosing, JobId jobId, ApplicationAttemptId applicationAttemptId
				, Configuration conf, EventHandler eventHandler, TaskAttemptListener taskAttemptListener
				, Clock clock, OutputCommitter committer, bool newApiCommitter, string user, AppContext
				 appContext, JobStateInternal forcedState, string diagnostic)
				: base(jobId, MRApp.GetApplicationAttemptId(MRApp.applicationId, this._enclosing.
					GetStartCount()), conf, eventHandler, taskAttemptListener, new JobTokenSecretManager
					(), new Credentials(), clock, this._enclosing.GetCompletedTaskFromPreviousRun(), 
					this._enclosing.metrics, committer, newApiCommitter, user, Runtime.CurrentTimeMillis
					(), this._enclosing.GetAllAMInfos(), appContext, forcedState, diagnostic)
			{
				this._enclosing = _enclosing;
				// This "this leak" is okay because the retained pointer is in an
				//  instance variable.
				this.localStateMachine = this.localFactory.Make(this);
			}

			private readonly MRApp _enclosing;
		}

		internal class TestInitTransition : JobImpl.InitTransition
		{
			private int maps;

			private int reduces;

			internal TestInitTransition(int maps, int reduces)
			{
				//Override InitTransition to not look for split files etc
				this.maps = maps;
				this.reduces = reduces;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void Setup(JobImpl job)
			{
				base.Setup(job);
				job.conf.SetInt(MRJobConfig.NumReduces, reduces);
				job.remoteJobConfFile = new Path("test");
			}

			protected internal override JobSplit.TaskSplitMetaInfo[] CreateSplits(JobImpl job
				, JobId jobId)
			{
				JobSplit.TaskSplitMetaInfo[] splits = new JobSplit.TaskSplitMetaInfo[maps];
				for (int i = 0; i < maps; i++)
				{
					splits[i] = new JobSplit.TaskSplitMetaInfo();
				}
				return splits;
			}
		}

		public static Org.Apache.Hadoop.Yarn.Api.Records.Token NewContainerToken(NodeId nodeId
			, byte[] password, ContainerTokenIdentifier tokenIdentifier)
		{
			// RPC layer client expects ip:port as service for tokens
			IPEndPoint addr = NetUtils.CreateSocketAddrForHost(nodeId.GetHost(), nodeId.GetPort
				());
			// NOTE: use SecurityUtil.setTokenService if this becomes a "real" token
			Org.Apache.Hadoop.Yarn.Api.Records.Token containerToken = Org.Apache.Hadoop.Yarn.Api.Records.Token
				.NewInstance(tokenIdentifier.GetBytes(), ContainerTokenIdentifier.Kind.ToString(
				), password, SecurityUtil.BuildTokenService(addr).ToString());
			return containerToken;
		}

		public static ContainerId NewContainerId(int appId, int appAttemptId, long timestamp
			, int containerId)
		{
			ApplicationId applicationId = ApplicationId.NewInstance(timestamp, appId);
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, appAttemptId);
			return ContainerId.NewContainerId(applicationAttemptId, containerId);
		}

		/// <exception cref="System.IO.IOException"/>
		public static ContainerTokenIdentifier NewContainerTokenIdentifier(Org.Apache.Hadoop.Yarn.Api.Records.Token
			 containerToken)
		{
			Org.Apache.Hadoop.Security.Token.Token<ContainerTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<ContainerTokenIdentifier>(((byte[])containerToken.GetIdentifier().Array()), ((byte
				[])containerToken.GetPassword().Array()), new Text(containerToken.GetKind()), new 
				Text(containerToken.GetService()));
			return token.DecodeIdentifier();
		}
	}
}
