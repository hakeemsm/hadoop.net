using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary>Make sure that the job staging directory clean up happens.</summary>
	public class TestStagingCleanup
	{
		private Configuration conf = new Configuration();

		private FileSystem fs;

		private string stagingJobDir = "tmpJobDir";

		private Path stagingJobPath;

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletionofStagingOnUnregistrationFailure()
		{
			TestDeletionofStagingOnUnregistrationFailure(2, false);
			TestDeletionofStagingOnUnregistrationFailure(1, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestDeletionofStagingOnUnregistrationFailure(int maxAttempts, bool shouldHaveDeleted
			)
		{
			conf.Set(MRJobConfig.MapreduceJobDir, stagingJobDir);
			fs = Org.Mockito.Mockito.Mock<FileSystem>();
			Org.Mockito.Mockito.When(fs.Delete(Matchers.Any<Path>(), Matchers.AnyBoolean())).
				ThenReturn(true);
			//Staging Dir exists
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			Path stagingDir = MRApps.GetStagingAreaDir(conf, user);
			Org.Mockito.Mockito.When(fs.Exists(stagingDir)).ThenReturn(true);
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
			JobId jobid = recordFactory.NewRecordInstance<JobId>();
			jobid.SetAppId(appId);
			TestStagingCleanup.TestMRApp appMaster = new TestStagingCleanup.TestMRApp(this, attemptId
				, null, JobStateInternal.Running, maxAttempts);
			appMaster.crushUnregistration = true;
			appMaster.Init(conf);
			appMaster.Start();
			appMaster.ShutDownJob();
			((MRAppMaster.RunningAppContext)appMaster.GetContext()).ResetIsLastAMRetry();
			if (shouldHaveDeleted)
			{
				NUnit.Framework.Assert.AreEqual(true, appMaster.IsLastAMRetry());
				Org.Mockito.Mockito.Verify(fs).Delete(stagingJobPath, true);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(false, appMaster.IsLastAMRetry());
				Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).Delete(stagingJobPath
					, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDeletionofStaging()
		{
			conf.Set(MRJobConfig.MapreduceJobDir, stagingJobDir);
			fs = Org.Mockito.Mockito.Mock<FileSystem>();
			Org.Mockito.Mockito.When(fs.Delete(Matchers.Any<Path>(), Matchers.AnyBoolean())).
				ThenReturn(true);
			//Staging Dir exists
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			Path stagingDir = MRApps.GetStagingAreaDir(conf, user);
			Org.Mockito.Mockito.When(fs.Exists(stagingDir)).ThenReturn(true);
			ApplicationId appId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 0);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
			JobId jobid = recordFactory.NewRecordInstance<JobId>();
			jobid.SetAppId(appId);
			ContainerAllocator mockAlloc = Org.Mockito.Mockito.Mock<ContainerAllocator>();
			NUnit.Framework.Assert.IsTrue(MRJobConfig.DefaultMrAmMaxAttempts > 1);
			MRAppMaster appMaster = new TestStagingCleanup.TestMRApp(this, attemptId, mockAlloc
				, JobStateInternal.Running, MRJobConfig.DefaultMrAmMaxAttempts);
			appMaster.Init(conf);
			appMaster.Start();
			appMaster.ShutDownJob();
			//test whether notifyIsLastAMRetry called
			NUnit.Framework.Assert.AreEqual(true, ((TestStagingCleanup.TestMRApp)appMaster).GetTestIsLastAMRetry
				());
			Org.Mockito.Mockito.Verify(fs).Delete(stagingJobPath, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNoDeletionofStagingOnReboot()
		{
			conf.Set(MRJobConfig.MapreduceJobDir, stagingJobDir);
			fs = Org.Mockito.Mockito.Mock<FileSystem>();
			Org.Mockito.Mockito.When(fs.Delete(Matchers.Any<Path>(), Matchers.AnyBoolean())).
				ThenReturn(true);
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			Path stagingDir = MRApps.GetStagingAreaDir(conf, user);
			Org.Mockito.Mockito.When(fs.Exists(stagingDir)).ThenReturn(true);
			ApplicationId appId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 0);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerAllocator mockAlloc = Org.Mockito.Mockito.Mock<ContainerAllocator>();
			NUnit.Framework.Assert.IsTrue(MRJobConfig.DefaultMrAmMaxAttempts > 1);
			MRAppMaster appMaster = new TestStagingCleanup.TestMRApp(this, attemptId, mockAlloc
				, JobStateInternal.Reboot, MRJobConfig.DefaultMrAmMaxAttempts);
			appMaster.Init(conf);
			appMaster.Start();
			//shutdown the job, not the lastRetry
			appMaster.ShutDownJob();
			//test whether notifyIsLastAMRetry called
			NUnit.Framework.Assert.AreEqual(false, ((TestStagingCleanup.TestMRApp)appMaster).
				GetTestIsLastAMRetry());
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(0)).Delete(stagingJobPath
				, true);
		}

		// FIXME:
		// Disabled this test because currently, when job state=REBOOT at shutdown 
		// when lastRetry = true in RM view, cleanup will not do. 
		// This will be supported after YARN-2261 completed
		//   @Test (timeout = 30000)
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDeletionofStagingOnReboot()
		{
			conf.Set(MRJobConfig.MapreduceJobDir, stagingJobDir);
			fs = Org.Mockito.Mockito.Mock<FileSystem>();
			Org.Mockito.Mockito.When(fs.Delete(Matchers.Any<Path>(), Matchers.AnyBoolean())).
				ThenReturn(true);
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			Path stagingDir = MRApps.GetStagingAreaDir(conf, user);
			Org.Mockito.Mockito.When(fs.Exists(stagingDir)).ThenReturn(true);
			ApplicationId appId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 0);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerAllocator mockAlloc = Org.Mockito.Mockito.Mock<ContainerAllocator>();
			MRAppMaster appMaster = new TestStagingCleanup.TestMRApp(this, attemptId, mockAlloc
				, JobStateInternal.Reboot, 1);
			//no retry
			appMaster.Init(conf);
			appMaster.Start();
			//shutdown the job, is lastRetry
			appMaster.ShutDownJob();
			//test whether notifyIsLastAMRetry called
			NUnit.Framework.Assert.AreEqual(true, ((TestStagingCleanup.TestMRApp)appMaster).GetTestIsLastAMRetry
				());
			Org.Mockito.Mockito.Verify(fs).Delete(stagingJobPath, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDeletionofStagingOnKill()
		{
			conf.Set(MRJobConfig.MapreduceJobDir, stagingJobDir);
			fs = Org.Mockito.Mockito.Mock<FileSystem>();
			Org.Mockito.Mockito.When(fs.Delete(Matchers.Any<Path>(), Matchers.AnyBoolean())).
				ThenReturn(true);
			//Staging Dir exists
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			Path stagingDir = MRApps.GetStagingAreaDir(conf, user);
			Org.Mockito.Mockito.When(fs.Exists(stagingDir)).ThenReturn(true);
			ApplicationId appId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 0);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 0);
			JobId jobid = recordFactory.NewRecordInstance<JobId>();
			jobid.SetAppId(appId);
			ContainerAllocator mockAlloc = Org.Mockito.Mockito.Mock<ContainerAllocator>();
			MRAppMaster appMaster = new TestStagingCleanup.TestMRApp(this, attemptId, mockAlloc
				);
			appMaster.Init(conf);
			//simulate the process being killed
			MRAppMaster.MRAppMasterShutdownHook hook = new MRAppMaster.MRAppMasterShutdownHook
				(appMaster);
			hook.Run();
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Times(0)).Delete(stagingJobPath
				, true);
		}

		// FIXME:
		// Disabled this test because currently, when shutdown hook triggered at
		// lastRetry in RM view, cleanup will not do. This should be supported after
		// YARN-2261 completed
		//   @Test (timeout = 30000)
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDeletionofStagingOnKillLastTry()
		{
			conf.Set(MRJobConfig.MapreduceJobDir, stagingJobDir);
			fs = Org.Mockito.Mockito.Mock<FileSystem>();
			Org.Mockito.Mockito.When(fs.Delete(Matchers.Any<Path>(), Matchers.AnyBoolean())).
				ThenReturn(true);
			//Staging Dir exists
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			Path stagingDir = MRApps.GetStagingAreaDir(conf, user);
			Org.Mockito.Mockito.When(fs.Exists(stagingDir)).ThenReturn(true);
			ApplicationId appId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 0);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
			JobId jobid = recordFactory.NewRecordInstance<JobId>();
			jobid.SetAppId(appId);
			ContainerAllocator mockAlloc = Org.Mockito.Mockito.Mock<ContainerAllocator>();
			MRAppMaster appMaster = new TestStagingCleanup.TestMRApp(this, attemptId, mockAlloc
				);
			//no retry
			appMaster.Init(conf);
			NUnit.Framework.Assert.IsTrue("appMaster.isLastAMRetry() is false", appMaster.IsLastAMRetry
				());
			//simulate the process being killed
			MRAppMaster.MRAppMasterShutdownHook hook = new MRAppMaster.MRAppMasterShutdownHook
				(appMaster);
			hook.Run();
			NUnit.Framework.Assert.IsTrue("MRAppMaster isn't stopped", appMaster.IsInState(Service.STATE
				.Stopped));
			Org.Mockito.Mockito.Verify(fs).Delete(stagingJobPath, true);
		}

		private class TestMRApp : MRAppMaster
		{
			internal ContainerAllocator allocator;

			internal bool testIsLastAMRetry = false;

			internal JobStateInternal jobStateInternal;

			internal bool crushUnregistration = false;

			public TestMRApp(TestStagingCleanup _enclosing, ApplicationAttemptId applicationAttemptId
				, ContainerAllocator allocator)
				: base(applicationAttemptId, ContainerId.NewContainerId(applicationAttemptId, 1), 
					"testhost", 2222, 3333, Runtime.CurrentTimeMillis())
			{
				this._enclosing = _enclosing;
				this.allocator = allocator;
				this.successfullyUnregistered.Set(true);
			}

			public TestMRApp(TestStagingCleanup _enclosing, ApplicationAttemptId applicationAttemptId
				, ContainerAllocator allocator, JobStateInternal jobStateInternal, int maxAppAttempts
				)
				: this(applicationAttemptId, allocator)
			{
				this._enclosing = _enclosing;
				this.jobStateInternal = jobStateInternal;
			}

			protected internal override FileSystem GetFileSystem(Configuration conf)
			{
				return this._enclosing.fs;
			}

			protected internal override ContainerAllocator CreateContainerAllocator(ClientService
				 clientService, AppContext context)
			{
				if (this.allocator == null)
				{
					if (this.crushUnregistration)
					{
						return new TestStagingCleanup.TestMRApp.CustomContainerAllocator(this, context);
					}
					else
					{
						return base.CreateContainerAllocator(clientService, context);
					}
				}
				return this.allocator;
			}

			protected internal override Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job CreateJob(
				Configuration conf, JobStateInternal forcedState, string diagnostic)
			{
				JobImpl jobImpl = Org.Mockito.Mockito.Mock<JobImpl>();
				Org.Mockito.Mockito.When(jobImpl.GetInternalState()).ThenReturn(this.jobStateInternal
					);
				Org.Mockito.Mockito.When(jobImpl.GetAllCounters()).ThenReturn(new Counters());
				JobID jobID = JobID.ForName("job_1234567890000_0001");
				JobId jobId = TypeConverter.ToYarn(jobID);
				Org.Mockito.Mockito.When(jobImpl.GetID()).ThenReturn(jobId);
				((AppContext)this.GetContext()).GetAllJobs()[jobImpl.GetID()] = jobImpl;
				return jobImpl;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				base.ServiceStart();
				DefaultMetricsSystem.Shutdown();
			}

			public override void NotifyIsLastAMRetry(bool isLastAMRetry)
			{
				this.testIsLastAMRetry = isLastAMRetry;
				base.NotifyIsLastAMRetry(isLastAMRetry);
			}

			protected internal override RMHeartbeatHandler GetRMHeartbeatHandler()
			{
				return TestStagingCleanup.GetStubbedHeartbeatHandler(this.GetContext());
			}

			protected internal override void Sysexit()
			{
			}

			public override Configuration GetConfig()
			{
				return this._enclosing.conf;
			}

			protected internal override void InitJobCredentialsAndUGI(Configuration conf)
			{
			}

			public virtual bool GetTestIsLastAMRetry()
			{
				return this.testIsLastAMRetry;
			}

			private class CustomContainerAllocator : RMCommunicator, ContainerAllocator
			{
				public CustomContainerAllocator(TestMRApp _enclosing, AppContext context)
					: base(null, context)
				{
					this._enclosing = _enclosing;
				}

				protected override void ServiceInit(Configuration conf)
				{
				}

				protected override void ServiceStart()
				{
				}

				protected override void ServiceStop()
				{
					this.Unregister();
				}

				/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				protected internal override void DoUnregistration()
				{
					throw new YarnException("test exception");
				}

				/// <exception cref="System.Exception"/>
				protected internal override void Heartbeat()
				{
				}

				public virtual void Handle(ContainerAllocatorEvent @event)
				{
				}

				private readonly TestMRApp _enclosing;
			}

			private readonly TestStagingCleanup _enclosing;
		}

		private sealed class MRAppTestCleanup : MRApp
		{
			internal int stagingDirCleanedup;

			internal int ContainerAllocatorStopped;

			internal int numStops;

			public MRAppTestCleanup(TestStagingCleanup _enclosing, int maps, int reduces, bool
				 autoComplete, string testName, bool cleanOnStart)
				: base(maps, reduces, autoComplete, testName, cleanOnStart)
			{
				this._enclosing = _enclosing;
				this.stagingDirCleanedup = 0;
				this.ContainerAllocatorStopped = 0;
				this.numStops = 0;
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
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job newJob = new MRApp.TestJob(this, this.
					GetJobId(), this.GetAttemptID(), conf, this.GetDispatcher().GetEventHandler(), this
					.GetTaskAttemptListener(), this.GetContext().GetClock(), this.GetCommitter(), this
					.IsNewApiCommitter(), currentUser.GetUserName(), this.GetContext(), forcedState, 
					diagnostic);
				((AppContext)this.GetContext()).GetAllJobs()[newJob.GetID()] = newJob;
				this.GetDispatcher().Register(typeof(JobFinishEvent.Type), this.CreateJobFinishEventHandler
					());
				return newJob;
			}

			protected internal override ContainerAllocator CreateContainerAllocator(ClientService
				 clientService, AppContext context)
			{
				return new TestStagingCleanup.MRAppTestCleanup.TestCleanupContainerAllocator(this
					);
			}

			private class TestCleanupContainerAllocator : AbstractService, ContainerAllocator
			{
				private MRApp.MRAppContainerAllocator allocator;

				internal TestCleanupContainerAllocator(MRAppTestCleanup _enclosing)
					: base(typeof(TestStagingCleanup.MRAppTestCleanup.TestCleanupContainerAllocator).
						FullName)
				{
					this._enclosing = _enclosing;
					this.allocator = new MRApp.MRAppContainerAllocator(this);
				}

				public virtual void Handle(ContainerAllocatorEvent @event)
				{
					this.allocator.Handle(@event);
				}

				/// <exception cref="System.Exception"/>
				protected override void ServiceStop()
				{
					this._enclosing.numStops++;
					this._enclosing.ContainerAllocatorStopped = this._enclosing.numStops;
					base.ServiceStop();
				}

				private readonly MRAppTestCleanup _enclosing;
			}

			protected internal override RMHeartbeatHandler GetRMHeartbeatHandler()
			{
				return TestStagingCleanup.GetStubbedHeartbeatHandler(this.GetContext());
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CleanupStagingDir()
			{
				this.numStops++;
				this.stagingDirCleanedup = this.numStops;
			}

			protected internal override void Sysexit()
			{
			}

			private readonly TestStagingCleanup _enclosing;
		}

		private static RMHeartbeatHandler GetStubbedHeartbeatHandler(AppContext appContext
			)
		{
			return new _RMHeartbeatHandler_453(appContext);
		}

		private sealed class _RMHeartbeatHandler_453 : RMHeartbeatHandler
		{
			public _RMHeartbeatHandler_453(AppContext appContext)
			{
				this.appContext = appContext;
			}

			public long GetLastHeartbeatTime()
			{
				return appContext.GetClock().GetTime();
			}

			public void RunOnNextHeartbeat(Runnable callback)
			{
				callback.Run();
			}

			private readonly AppContext appContext;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStagingCleanupOrder()
		{
			TestStagingCleanup.MRAppTestCleanup app = new TestStagingCleanup.MRAppTestCleanup
				(this, 1, 1, true, this.GetType().FullName, true);
			JobImpl job = (JobImpl)app.Submit(new Configuration());
			app.WaitForState(job, JobState.Succeeded);
			app.VerifyCompleted();
			int waitTime = 20 * 1000;
			while (waitTime > 0 && app.numStops < 2)
			{
				Sharpen.Thread.Sleep(100);
				waitTime -= 100;
			}
			// assert ContainerAllocatorStopped and then tagingDirCleanedup
			NUnit.Framework.Assert.AreEqual(1, app.ContainerAllocatorStopped);
			NUnit.Framework.Assert.AreEqual(2, app.stagingDirCleanedup);
		}

		public TestStagingCleanup()
		{
			stagingJobPath = new Path(stagingJobDir);
		}
	}
}
