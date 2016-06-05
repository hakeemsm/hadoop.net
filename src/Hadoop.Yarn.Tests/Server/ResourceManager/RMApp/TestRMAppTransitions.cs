using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class TestRMAppTransitions
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.TestRMAppTransitions
			));

		private bool isSecurityEnabled;

		private Configuration conf;

		private RMContext rmContext;

		private static int maxAppAttempts = YarnConfiguration.DefaultRmAmMaxAttempts;

		private static int appId = 1;

		private DrainDispatcher rmDispatcher;

		private RMStateStore store;

		private RMApplicationHistoryWriter writer;

		private SystemMetricsPublisher publisher;

		private YarnScheduler scheduler;

		private TestRMAppTransitions.TestSchedulerEventDispatcher schedulerDispatcher;

		private sealed class TestApplicationAttemptEventDispatcher : EventHandler<RMAppAttemptEvent
			>
		{
			private readonly RMContext rmContext;

			public TestApplicationAttemptEventDispatcher(RMContext rmContext)
			{
				// ignore all the RM application attempt events
				this.rmContext = rmContext;
			}

			public void Handle(RMAppAttemptEvent @event)
			{
				ApplicationId appId = @event.GetApplicationAttemptId().GetApplicationId();
				RMApp rmApp = this.rmContext.GetRMApps()[appId];
				if (rmApp != null)
				{
					try
					{
						rmApp.GetRMAppAttempt(@event.GetApplicationAttemptId()).Handle(@event);
					}
					catch (Exception t)
					{
						Log.Error("Error in handling event type " + @event.GetType() + " for application "
							 + appId, t);
					}
				}
			}
		}

		private sealed class TestApplicationEventDispatcher : EventHandler<RMAppEvent>
		{
			private readonly RMContext rmContext;

			public TestApplicationEventDispatcher(RMContext rmContext)
			{
				// handle all the RM application events - same as in ResourceManager.java
				this.rmContext = rmContext;
			}

			public void Handle(RMAppEvent @event)
			{
				ApplicationId appID = @event.GetApplicationId();
				RMApp rmApp = this.rmContext.GetRMApps()[appID];
				if (rmApp != null)
				{
					try
					{
						rmApp.Handle(@event);
					}
					catch (Exception t)
					{
						Log.Error("Error in handling event type " + @event.GetType() + " for application "
							 + appID, t);
					}
				}
			}
		}

		private sealed class TestApplicationManagerEventDispatcher : EventHandler<RMAppManagerEvent
			>
		{
			// handle all the RM application manager events - same as in
			// ResourceManager.java
			public void Handle(RMAppManagerEvent @event)
			{
			}
		}

		private sealed class TestSchedulerEventDispatcher : EventHandler<SchedulerEvent>
		{
			public SchedulerEvent lastSchedulerEvent;

			// handle all the scheduler events - same as in ResourceManager.java
			public void Handle(SchedulerEvent @event)
			{
				lastSchedulerEvent = @event;
			}
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> GetTestParameters()
		{
			return Arrays.AsList(new object[][] { new object[] { false }, new object[] { true
				 } });
		}

		public TestRMAppTransitions(bool isSecurityEnabled)
		{
			this.isSecurityEnabled = isSecurityEnabled;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			conf = new YarnConfiguration();
			UserGroupInformation.AuthenticationMethod authMethod = UserGroupInformation.AuthenticationMethod
				.Simple;
			if (isSecurityEnabled)
			{
				authMethod = UserGroupInformation.AuthenticationMethod.Kerberos;
			}
			SecurityUtil.SetAuthenticationMethod(authMethod, conf);
			UserGroupInformation.SetConfiguration(conf);
			rmDispatcher = new DrainDispatcher();
			ContainerAllocationExpirer containerAllocationExpirer = Org.Mockito.Mockito.Mock<
				ContainerAllocationExpirer>();
			AMLivelinessMonitor amLivelinessMonitor = Org.Mockito.Mockito.Mock<AMLivelinessMonitor
				>();
			AMLivelinessMonitor amFinishingMonitor = Org.Mockito.Mockito.Mock<AMLivelinessMonitor
				>();
			store = Org.Mockito.Mockito.Mock<RMStateStore>();
			writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter>();
			DelegationTokenRenewer renewer = Org.Mockito.Mockito.Mock<DelegationTokenRenewer>
				();
			RMContext realRMContext = new RMContextImpl(rmDispatcher, containerAllocationExpirer
				, amLivelinessMonitor, amFinishingMonitor, renewer, new AMRMTokenSecretManager(conf
				, this.rmContext), new RMContainerTokenSecretManager(conf), new NMTokenSecretManagerInRM
				(conf), new ClientToAMTokenSecretManagerInRM());
			((RMContextImpl)realRMContext).SetStateStore(store);
			publisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher>();
			realRMContext.SetSystemMetricsPublisher(publisher);
			realRMContext.SetRMApplicationHistoryWriter(writer);
			this.rmContext = Org.Mockito.Mockito.Spy(realRMContext);
			ResourceScheduler resourceScheduler = Org.Mockito.Mockito.Mock<ResourceScheduler>
				();
			Org.Mockito.Mockito.DoReturn(null).When(resourceScheduler).GetAppResourceUsageReport
				((ApplicationAttemptId)Matchers.Any());
			Org.Mockito.Mockito.DoReturn(resourceScheduler).When(rmContext).GetScheduler();
			rmDispatcher.Register(typeof(RMAppAttemptEventType), new TestRMAppTransitions.TestApplicationAttemptEventDispatcher
				(this.rmContext));
			rmDispatcher.Register(typeof(RMAppEventType), new TestRMAppTransitions.TestApplicationEventDispatcher
				(rmContext));
			rmDispatcher.Register(typeof(RMAppManagerEventType), new TestRMAppTransitions.TestApplicationManagerEventDispatcher
				());
			schedulerDispatcher = new TestRMAppTransitions.TestSchedulerEventDispatcher();
			rmDispatcher.Register(typeof(SchedulerEventType), schedulerDispatcher);
			rmDispatcher.Init(conf);
			rmDispatcher.Start();
		}

		protected internal virtual RMApp CreateNewTestApp(ApplicationSubmissionContext submissionContext
			)
		{
			ApplicationId applicationId = MockApps.NewAppID(appId++);
			string user = MockApps.NewUserName();
			string name = MockApps.NewAppName();
			string queue = MockApps.NewQueue();
			// ensure max application attempts set to known value
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, maxAppAttempts);
			scheduler = Org.Mockito.Mockito.Mock<YarnScheduler>();
			ApplicationMasterService masterService = new ApplicationMasterService(rmContext, 
				scheduler);
			if (submissionContext == null)
			{
				submissionContext = new ApplicationSubmissionContextPBImpl();
			}
			// applicationId will not be used because RMStateStore is mocked,
			// but applicationId is still set for safety
			submissionContext.SetApplicationId(applicationId);
			RMApp application = new RMAppImpl(applicationId, rmContext, conf, name, user, queue
				, submissionContext, scheduler, masterService, Runtime.CurrentTimeMillis(), "YARN"
				, null, null);
			TestAppStartState(applicationId, user, name, queue, application);
			this.rmContext.GetRMApps().PutIfAbsent(application.GetApplicationId(), application
				);
			return application;
		}

		// Test expected newly created app state
		private static void TestAppStartState(ApplicationId applicationId, string user, string
			 name, string queue, RMApp application)
		{
			NUnit.Framework.Assert.IsTrue("application start time is not greater then 0", application
				.GetStartTime() > 0);
			NUnit.Framework.Assert.IsTrue("application start time is before currentTime", application
				.GetStartTime() <= Runtime.CurrentTimeMillis());
			NUnit.Framework.Assert.AreEqual("application user is not correct", user, application
				.GetUser());
			NUnit.Framework.Assert.AreEqual("application id is not correct", applicationId, application
				.GetApplicationId());
			NUnit.Framework.Assert.AreEqual("application progress is not correct", (float)0.0
				, application.GetProgress(), (float)0.0);
			NUnit.Framework.Assert.AreEqual("application queue is not correct", queue, application
				.GetQueue());
			NUnit.Framework.Assert.AreEqual("application name is not correct", name, application
				.GetName());
			NUnit.Framework.Assert.AreEqual("application finish time is not 0 and should be", 
				0, application.GetFinishTime());
			NUnit.Framework.Assert.AreEqual("application tracking url is not correct", null, 
				application.GetTrackingUrl());
			StringBuilder diag = application.GetDiagnostics();
			NUnit.Framework.Assert.AreEqual("application diagnostics is not correct", 0, diag
				.Length);
		}

		// test to make sure times are set when app finishes
		private static void AssertStartTimeSet(RMApp application)
		{
			NUnit.Framework.Assert.IsTrue("application start time is not greater then 0", application
				.GetStartTime() > 0);
			NUnit.Framework.Assert.IsTrue("application start time is before currentTime", application
				.GetStartTime() <= Runtime.CurrentTimeMillis());
		}

		private static void AssertAppState(RMAppState state, RMApp application)
		{
			NUnit.Framework.Assert.AreEqual("application state should have been " + state, state
				, application.GetState());
		}

		private static void AssertFinalAppStatus(FinalApplicationStatus status, RMApp application
			)
		{
			NUnit.Framework.Assert.AreEqual("Final application status should have been " + status
				, status, application.GetFinalApplicationStatus());
		}

		// test to make sure times are set when app finishes
		private void AssertTimesAtFinish(RMApp application)
		{
			AssertStartTimeSet(application);
			NUnit.Framework.Assert.IsTrue("application finish time is not greater then 0", (application
				.GetFinishTime() > 0));
			NUnit.Framework.Assert.IsTrue("application finish time is not >= then start time"
				, (application.GetFinishTime() >= application.GetStartTime()));
		}

		private void AssertAppFinalStateSaved(RMApp application)
		{
			Org.Mockito.Mockito.Verify(store, Org.Mockito.Mockito.Times(1)).UpdateApplicationState
				(Matchers.Any<ApplicationStateData>());
		}

		private void AssertAppFinalStateNotSaved(RMApp application)
		{
			Org.Mockito.Mockito.Verify(store, Org.Mockito.Mockito.Times(0)).UpdateApplicationState
				(Matchers.Any<ApplicationStateData>());
		}

		private void AssertKilled(RMApp application)
		{
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Killed, application);
			AssertFinalAppStatus(FinalApplicationStatus.Killed, application);
			StringBuilder diag = application.GetDiagnostics();
			NUnit.Framework.Assert.AreEqual("application diagnostics is not correct", "Application killed by user."
				, diag.ToString());
		}

		private void AssertFailed(RMApp application, string regex)
		{
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Failed, application);
			AssertFinalAppStatus(FinalApplicationStatus.Failed, application);
			StringBuilder diag = application.GetDiagnostics();
			NUnit.Framework.Assert.IsTrue("application diagnostics is not correct", diag.ToString
				().Matches(regex));
		}

		private void SendAppUpdateSavedEvent(RMApp application)
		{
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AppUpdateSaved);
			application.Handle(@event);
			rmDispatcher.Await();
		}

		private void SendAttemptUpdateSavedEvent(RMApp application)
		{
			application.GetCurrentAppAttempt().Handle(new RMAppAttemptEvent(application.GetCurrentAppAttempt
				().GetAppAttemptId(), RMAppAttemptEventType.AttemptUpdateSaved));
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual RMApp TestCreateAppNewSaving(ApplicationSubmissionContext
			 submissionContext)
		{
			RMApp application = CreateNewTestApp(submissionContext);
			Org.Mockito.Mockito.Verify(writer).ApplicationStarted(Matchers.Any<RMApp>());
			Org.Mockito.Mockito.Verify(publisher).AppCreated(Matchers.Any<RMApp>(), Matchers.AnyLong
				());
			// NEW => NEW_SAVING event RMAppEventType.START
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.Start);
			application.Handle(@event);
			AssertStartTimeSet(application);
			AssertAppState(RMAppState.NewSaving, application);
			return application;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual RMApp TestCreateAppSubmittedNoRecovery(ApplicationSubmissionContext
			 submissionContext)
		{
			RMApp application = TestCreateAppNewSaving(submissionContext);
			// NEW_SAVING => SUBMITTED event RMAppEventType.APP_SAVED
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AppNewSaved);
			application.Handle(@event);
			AssertStartTimeSet(application);
			AssertAppState(RMAppState.Submitted, application);
			return application;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual RMApp TestCreateAppSubmittedRecovery(ApplicationSubmissionContext
			 submissionContext)
		{
			RMApp application = CreateNewTestApp(submissionContext);
			// NEW => SUBMITTED event RMAppEventType.RECOVER
			RMStateStore.RMState state = new RMStateStore.RMState();
			ApplicationStateData appState = ApplicationStateData.NewInstance(123, 123, null, 
				"user");
			state.GetApplicationState()[application.GetApplicationId()] = appState;
			RMAppEvent @event = new RMAppRecoverEvent(application.GetApplicationId(), state);
			application.Handle(@event);
			AssertStartTimeSet(application);
			AssertAppState(RMAppState.Submitted, application);
			return application;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual RMApp TestCreateAppAccepted(ApplicationSubmissionContext
			 submissionContext)
		{
			RMApp application = TestCreateAppSubmittedNoRecovery(submissionContext);
			// SUBMITTED => ACCEPTED event RMAppEventType.APP_ACCEPTED
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AppAccepted);
			application.Handle(@event);
			AssertStartTimeSet(application);
			AssertAppState(RMAppState.Accepted, application);
			return application;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual RMApp TestCreateAppRunning(ApplicationSubmissionContext
			 submissionContext)
		{
			RMApp application = TestCreateAppAccepted(submissionContext);
			// ACCEPTED => RUNNING event RMAppEventType.ATTEMPT_REGISTERED
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AttemptRegistered);
			application.Handle(@event);
			AssertStartTimeSet(application);
			AssertAppState(RMAppState.Running, application);
			AssertFinalAppStatus(FinalApplicationStatus.Undefined, application);
			return application;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual RMApp TestCreateAppFinalSaving(ApplicationSubmissionContext
			 submissionContext)
		{
			RMApp application = TestCreateAppRunning(submissionContext);
			RMAppEvent finishingEvent = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AttemptUnregistered);
			application.Handle(finishingEvent);
			AssertAppState(RMAppState.FinalSaving, application);
			AssertAppFinalStateSaved(application);
			return application;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual RMApp TestCreateAppFinishing(ApplicationSubmissionContext
			 submissionContext)
		{
			// unmanaged AMs don't use the FINISHING state
			System.Diagnostics.Debug.Assert(submissionContext == null || !submissionContext.GetUnmanagedAM
				());
			RMApp application = TestCreateAppFinalSaving(submissionContext);
			// FINAL_SAVING => FINISHING event RMAppEventType.APP_UPDATED
			RMAppEvent appUpdated = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AppUpdateSaved);
			application.Handle(appUpdated);
			AssertAppState(RMAppState.Finishing, application);
			AssertTimesAtFinish(application);
			return application;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual RMApp TestCreateAppFinished(ApplicationSubmissionContext
			 submissionContext, string diagnostics)
		{
			// unmanaged AMs don't use the FINISHING state
			RMApp application = null;
			if (submissionContext != null && submissionContext.GetUnmanagedAM())
			{
				application = TestCreateAppRunning(submissionContext);
			}
			else
			{
				application = TestCreateAppFinishing(submissionContext);
			}
			// RUNNING/FINISHING => FINISHED event RMAppEventType.ATTEMPT_FINISHED
			RMAppEvent finishedEvent = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AttemptFinished, diagnostics);
			application.Handle(finishedEvent);
			AssertAppState(RMAppState.Finished, application);
			AssertTimesAtFinish(application);
			// finished without a proper unregister implies failed
			AssertFinalAppStatus(FinalApplicationStatus.Failed, application);
			NUnit.Framework.Assert.IsTrue("Finished app missing diagnostics", application.GetDiagnostics
				().IndexOf(diagnostics) != -1);
			return application;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUnmanagedApp()
		{
			ApplicationSubmissionContext subContext = new ApplicationSubmissionContextPBImpl(
				);
			subContext.SetUnmanagedAM(true);
			// test success path
			Log.Info("--- START: testUnmanagedAppSuccessPath ---");
			string diagMsg = "some diagnostics";
			RMApp application = TestCreateAppFinished(subContext, diagMsg);
			NUnit.Framework.Assert.IsTrue("Finished app missing diagnostics", application.GetDiagnostics
				().IndexOf(diagMsg) != -1);
			// reset the counter of Mockito.verify
			Org.Mockito.Mockito.Reset(writer);
			Org.Mockito.Mockito.Reset(publisher);
			// test app fails after 1 app attempt failure
			Log.Info("--- START: testUnmanagedAppFailPath ---");
			application = TestCreateAppRunning(subContext);
			RMAppEvent @event = new RMAppFailedAttemptEvent(application.GetApplicationId(), RMAppEventType
				.AttemptFailed, string.Empty, false);
			application.Handle(@event);
			rmDispatcher.Await();
			RMAppAttempt appAttempt = application.GetCurrentAppAttempt();
			NUnit.Framework.Assert.AreEqual(1, appAttempt.GetAppAttemptId().GetAttemptId());
			SendAppUpdateSavedEvent(application);
			AssertFailed(application, ".*Unmanaged application.*Failing the application.*");
			AssertAppFinalStateSaved(application);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppSuccessPath()
		{
			Log.Info("--- START: testAppSuccessPath ---");
			string diagMsg = "some diagnostics";
			RMApp application = TestCreateAppFinished(null, diagMsg);
			NUnit.Framework.Assert.IsTrue("Finished application missing diagnostics", application
				.GetDiagnostics().IndexOf(diagMsg) != -1);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAppRecoverPath()
		{
			Log.Info("--- START: testAppRecoverPath ---");
			ApplicationSubmissionContext sub = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				ApplicationSubmissionContext>();
			ContainerLaunchContext clc = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ContainerLaunchContext
				>();
			Credentials credentials = new Credentials();
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.WriteTokenStorageToStream(dob);
			ByteBuffer securityTokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
			clc.SetTokens(securityTokens);
			sub.SetAMContainerSpec(clc);
			TestCreateAppSubmittedRecovery(sub);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAppNewKill()
		{
			Log.Info("--- START: testAppNewKill ---");
			RMApp application = CreateNewTestApp(null);
			// NEW => KILLED event RMAppEventType.KILL
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.Kill, "Application killed by user.");
			application.Handle(@event);
			rmDispatcher.Await();
			SendAppUpdateSavedEvent(application);
			AssertKilled(application);
			AssertAppFinalStateNotSaved(application);
			VerifyApplicationFinished(RMAppState.Killed);
			VerifyAppRemovedSchedulerEvent(RMAppState.Killed);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppNewReject()
		{
			Log.Info("--- START: testAppNewReject ---");
			RMApp application = CreateNewTestApp(null);
			// NEW => FAILED event RMAppEventType.APP_REJECTED
			string rejectedText = "Test Application Rejected";
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AppRejected, rejectedText);
			application.Handle(@event);
			rmDispatcher.Await();
			SendAppUpdateSavedEvent(application);
			AssertFailed(application, rejectedText);
			AssertAppFinalStateSaved(application);
			VerifyApplicationFinished(RMAppState.Failed);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAppNewRejectAddToStore()
		{
			Log.Info("--- START: testAppNewRejectAddToStore ---");
			RMApp application = CreateNewTestApp(null);
			// NEW => FAILED event RMAppEventType.APP_REJECTED
			string rejectedText = "Test Application Rejected";
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AppRejected, rejectedText);
			application.Handle(@event);
			rmDispatcher.Await();
			SendAppUpdateSavedEvent(application);
			AssertFailed(application, rejectedText);
			AssertAppFinalStateSaved(application);
			VerifyApplicationFinished(RMAppState.Failed);
			rmContext.GetStateStore().RemoveApplication(application);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAppNewSavingKill()
		{
			Log.Info("--- START: testAppNewSavingKill ---");
			RMApp application = TestCreateAppNewSaving(null);
			// NEW_SAVING => KILLED event RMAppEventType.KILL
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.Kill, "Application killed by user.");
			application.Handle(@event);
			rmDispatcher.Await();
			SendAppUpdateSavedEvent(application);
			AssertKilled(application);
			VerifyApplicationFinished(RMAppState.Killed);
			VerifyAppRemovedSchedulerEvent(RMAppState.Killed);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAppNewSavingReject()
		{
			Log.Info("--- START: testAppNewSavingReject ---");
			RMApp application = TestCreateAppNewSaving(null);
			// NEW_SAVING => FAILED event RMAppEventType.APP_REJECTED
			string rejectedText = "Test Application Rejected";
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AppRejected, rejectedText);
			application.Handle(@event);
			rmDispatcher.Await();
			SendAppUpdateSavedEvent(application);
			AssertFailed(application, rejectedText);
			AssertAppFinalStateSaved(application);
			VerifyApplicationFinished(RMAppState.Failed);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAppSubmittedRejected()
		{
			Log.Info("--- START: testAppSubmittedRejected ---");
			RMApp application = TestCreateAppSubmittedNoRecovery(null);
			// SUBMITTED => FAILED event RMAppEventType.APP_REJECTED
			string rejectedText = "app rejected";
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AppRejected, rejectedText);
			application.Handle(@event);
			rmDispatcher.Await();
			SendAppUpdateSavedEvent(application);
			AssertFailed(application, rejectedText);
			AssertAppFinalStateSaved(application);
			VerifyApplicationFinished(RMAppState.Failed);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppSubmittedKill()
		{
			Log.Info("--- START: testAppSubmittedKill---");
			RMApp application = TestCreateAppSubmittedNoRecovery(null);
			// SUBMITTED => KILLED event RMAppEventType.KILL
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.Kill, "Application killed by user.");
			application.Handle(@event);
			rmDispatcher.Await();
			SendAppUpdateSavedEvent(application);
			AssertKilled(application);
			AssertAppFinalStateSaved(application);
			VerifyApplicationFinished(RMAppState.Killed);
			VerifyAppRemovedSchedulerEvent(RMAppState.Killed);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAcceptedFailed()
		{
			Log.Info("--- START: testAppAcceptedFailed ---");
			RMApp application = TestCreateAppAccepted(null);
			// ACCEPTED => ACCEPTED event RMAppEventType.RMAppEventType.ATTEMPT_FAILED
			NUnit.Framework.Assert.IsTrue(maxAppAttempts > 1);
			for (int i = 1; i < maxAppAttempts; i++)
			{
				RMAppEvent @event = new RMAppFailedAttemptEvent(application.GetApplicationId(), RMAppEventType
					.AttemptFailed, string.Empty, false);
				application.Handle(@event);
				AssertAppState(RMAppState.Accepted, application);
				@event = new RMAppEvent(application.GetApplicationId(), RMAppEventType.AppAccepted
					);
				application.Handle(@event);
				rmDispatcher.Await();
				AssertAppState(RMAppState.Accepted, application);
			}
			// ACCEPTED => FAILED event RMAppEventType.RMAppEventType.ATTEMPT_FAILED 
			// after max application attempts
			string message = "Test fail";
			RMAppEvent event_1 = new RMAppFailedAttemptEvent(application.GetApplicationId(), 
				RMAppEventType.AttemptFailed, message, false);
			application.Handle(event_1);
			rmDispatcher.Await();
			SendAppUpdateSavedEvent(application);
			AssertFailed(application, ".*" + message + ".*Failing the application.*");
			AssertAppFinalStateSaved(application);
			VerifyApplicationFinished(RMAppState.Failed);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAcceptedKill()
		{
			Log.Info("--- START: testAppAcceptedKill ---");
			RMApp application = TestCreateAppAccepted(null);
			// ACCEPTED => KILLED event RMAppEventType.KILL
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.Kill, "Application killed by user.");
			application.Handle(@event);
			rmDispatcher.Await();
			AssertAppState(RMAppState.Killing, application);
			RMAppEvent appAttemptKilled = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AttemptKilled, "Application killed by user.");
			application.Handle(appAttemptKilled);
			AssertAppState(RMAppState.FinalSaving, application);
			SendAppUpdateSavedEvent(application);
			AssertKilled(application);
			AssertAppFinalStateSaved(application);
			VerifyApplicationFinished(RMAppState.Killed);
			VerifyAppRemovedSchedulerEvent(RMAppState.Killed);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAcceptedAttemptKilled()
		{
			Log.Info("--- START: testAppAcceptedAttemptKilled ---");
			RMApp application = TestCreateAppAccepted(null);
			// ACCEPTED => FINAL_SAVING event RMAppEventType.ATTEMPT_KILLED
			// When application recovery happens for attempt is KILLED but app is
			// RUNNING.
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AttemptKilled, "Application killed by user.");
			application.Handle(@event);
			rmDispatcher.Await();
			AssertAppState(RMAppState.FinalSaving, application);
			SendAppUpdateSavedEvent(application);
			AssertKilled(application);
			AssertAppFinalStateSaved(application);
			VerifyApplicationFinished(RMAppState.Killed);
			VerifyAppRemovedSchedulerEvent(RMAppState.Killed);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppRunningKill()
		{
			Log.Info("--- START: testAppRunningKill ---");
			RMApp application = TestCreateAppRunning(null);
			// RUNNING => KILLED event RMAppEventType.KILL
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.Kill, "Application killed by user.");
			application.Handle(@event);
			rmDispatcher.Await();
			SendAttemptUpdateSavedEvent(application);
			SendAppUpdateSavedEvent(application);
			AssertKilled(application);
			VerifyApplicationFinished(RMAppState.Killed);
			VerifyAppRemovedSchedulerEvent(RMAppState.Killed);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppRunningFailed()
		{
			Log.Info("--- START: testAppRunningFailed ---");
			RMApp application = TestCreateAppRunning(null);
			RMAppAttempt appAttempt = application.GetCurrentAppAttempt();
			int expectedAttemptId = 1;
			NUnit.Framework.Assert.AreEqual(expectedAttemptId, appAttempt.GetAppAttemptId().GetAttemptId
				());
			// RUNNING => FAILED/RESTARTING event RMAppEventType.ATTEMPT_FAILED
			NUnit.Framework.Assert.IsTrue(maxAppAttempts > 1);
			for (int i = 1; i < maxAppAttempts; i++)
			{
				RMAppEvent @event = new RMAppFailedAttemptEvent(application.GetApplicationId(), RMAppEventType
					.AttemptFailed, string.Empty, false);
				application.Handle(@event);
				rmDispatcher.Await();
				AssertAppState(RMAppState.Accepted, application);
				appAttempt = application.GetCurrentAppAttempt();
				NUnit.Framework.Assert.AreEqual(++expectedAttemptId, appAttempt.GetAppAttemptId()
					.GetAttemptId());
				@event = new RMAppEvent(application.GetApplicationId(), RMAppEventType.AppAccepted
					);
				application.Handle(@event);
				rmDispatcher.Await();
				AssertAppState(RMAppState.Accepted, application);
				@event = new RMAppEvent(application.GetApplicationId(), RMAppEventType.AttemptRegistered
					);
				application.Handle(@event);
				rmDispatcher.Await();
				AssertAppState(RMAppState.Running, application);
			}
			// RUNNING => FAILED/RESTARTING event RMAppEventType.ATTEMPT_FAILED 
			// after max application attempts
			RMAppEvent event_1 = new RMAppFailedAttemptEvent(application.GetApplicationId(), 
				RMAppEventType.AttemptFailed, string.Empty, false);
			application.Handle(event_1);
			rmDispatcher.Await();
			SendAppUpdateSavedEvent(application);
			AssertFailed(application, ".*Failing the application.*");
			AssertAppFinalStateSaved(application);
			// FAILED => FAILED event RMAppEventType.KILL
			event_1 = new RMAppEvent(application.GetApplicationId(), RMAppEventType.Kill, "Application killed by user."
				);
			application.Handle(event_1);
			rmDispatcher.Await();
			AssertFailed(application, ".*Failing the application.*");
			AssertAppFinalStateSaved(application);
			VerifyApplicationFinished(RMAppState.Failed);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAtFinishingIgnoreKill()
		{
			Log.Info("--- START: testAppAtFinishingIgnoreKill ---");
			RMApp application = TestCreateAppFinishing(null);
			// FINISHING => FINISHED event RMAppEventType.KILL
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.Kill, "Application killed by user.");
			application.Handle(@event);
			rmDispatcher.Await();
			AssertAppState(RMAppState.Finishing, application);
		}

		// While App is at FINAL_SAVING, Attempt_Finished event may come before
		// App_Saved event, we stay on FINAL_SAVING on Attempt_Finished event
		// and then directly jump from FINAL_SAVING to FINISHED state on App_Saved
		// event
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppFinalSavingToFinished()
		{
			Log.Info("--- START: testAppFinalSavingToFinished ---");
			RMApp application = TestCreateAppFinalSaving(null);
			string diagMsg = "some diagnostics";
			// attempt_finished event comes before attempt_saved event
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AttemptFinished, diagMsg);
			application.Handle(@event);
			AssertAppState(RMAppState.FinalSaving, application);
			RMAppEvent appUpdated = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AppUpdateSaved);
			application.Handle(appUpdated);
			AssertAppState(RMAppState.Finished, application);
			AssertTimesAtFinish(application);
			// finished without a proper unregister implies failed
			AssertFinalAppStatus(FinalApplicationStatus.Failed, application);
			NUnit.Framework.Assert.IsTrue("Finished app missing diagnostics", application.GetDiagnostics
				().IndexOf(diagMsg) != -1);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAppFinishedFinished()
		{
			Log.Info("--- START: testAppFinishedFinished ---");
			RMApp application = TestCreateAppFinished(null, string.Empty);
			// FINISHED => FINISHED event RMAppEventType.KILL
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.Kill, "Application killed by user.");
			application.Handle(@event);
			rmDispatcher.Await();
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Finished, application);
			StringBuilder diag = application.GetDiagnostics();
			NUnit.Framework.Assert.AreEqual("application diagnostics is not correct", string.Empty
				, diag.ToString());
			VerifyApplicationFinished(RMAppState.Finished);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAppFailedFailed()
		{
			Log.Info("--- START: testAppFailedFailed ---");
			RMApp application = TestCreateAppNewSaving(null);
			// NEW_SAVING => FAILED event RMAppEventType.APP_REJECTED
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.AppRejected, string.Empty);
			application.Handle(@event);
			rmDispatcher.Await();
			SendAppUpdateSavedEvent(application);
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Failed, application);
			// FAILED => FAILED event RMAppEventType.KILL
			@event = new RMAppEvent(application.GetApplicationId(), RMAppEventType.Kill, "Application killed by user."
				);
			application.Handle(@event);
			rmDispatcher.Await();
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Failed, application);
			VerifyApplicationFinished(RMAppState.Failed);
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Failed, application);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAppKilledKilled()
		{
			Log.Info("--- START: testAppKilledKilled ---");
			RMApp application = TestCreateAppRunning(null);
			// RUNNING => KILLED event RMAppEventType.KILL
			RMAppEvent @event = new RMAppEvent(application.GetApplicationId(), RMAppEventType
				.Kill, "Application killed by user.");
			application.Handle(@event);
			rmDispatcher.Await();
			SendAttemptUpdateSavedEvent(application);
			SendAppUpdateSavedEvent(application);
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Killed, application);
			// KILLED => KILLED event RMAppEventType.ATTEMPT_FINISHED
			@event = new RMAppEvent(application.GetApplicationId(), RMAppEventType.AttemptFinished
				, string.Empty);
			application.Handle(@event);
			rmDispatcher.Await();
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Killed, application);
			// KILLED => KILLED event RMAppEventType.ATTEMPT_FAILED
			@event = new RMAppFailedAttemptEvent(application.GetApplicationId(), RMAppEventType
				.AttemptFailed, string.Empty, false);
			application.Handle(@event);
			rmDispatcher.Await();
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Killed, application);
			// KILLED => KILLED event RMAppEventType.KILL
			@event = new RMAppEvent(application.GetApplicationId(), RMAppEventType.Kill, "Application killed by user."
				);
			application.Handle(@event);
			rmDispatcher.Await();
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Killed, application);
			VerifyApplicationFinished(RMAppState.Killed);
			AssertTimesAtFinish(application);
			AssertAppState(RMAppState.Killed, application);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppsRecoveringStates()
		{
			RMStateStore.RMState state = new RMStateStore.RMState();
			IDictionary<ApplicationId, ApplicationStateData> applicationState = state.GetApplicationState
				();
			CreateRMStateForApplications(applicationState, RMAppState.Finished);
			CreateRMStateForApplications(applicationState, RMAppState.Killed);
			CreateRMStateForApplications(applicationState, RMAppState.Failed);
			foreach (ApplicationStateData appState in applicationState.Values)
			{
				TestRecoverApplication(appState, state);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecoverApplication(ApplicationStateData appState, RMStateStore.RMState
			 rmState)
		{
			ApplicationSubmissionContext submissionContext = appState.GetApplicationSubmissionContext
				();
			RMAppImpl application = new RMAppImpl(appState.GetApplicationSubmissionContext().
				GetApplicationId(), rmContext, conf, submissionContext.GetApplicationName(), null
				, submissionContext.GetQueue(), submissionContext, null, null, appState.GetSubmitTime
				(), submissionContext.GetApplicationType(), submissionContext.GetApplicationTags
				(), BuilderUtils.NewResourceRequest(RMAppAttemptImpl.AmContainerPriority, ResourceRequest
				.Any, submissionContext.GetResource(), 1));
			NUnit.Framework.Assert.AreEqual(RMAppState.New, application.GetState());
			RMAppEvent recoverEvent = new RMAppRecoverEvent(application.GetApplicationId(), rmState
				);
			// Trigger RECOVER event.
			application.Handle(recoverEvent);
			// Application final status looked from recoveredFinalStatus
			NUnit.Framework.Assert.IsTrue("Application is not in recoveredFinalStatus.", RMAppImpl
				.IsAppInFinalState(application));
			rmDispatcher.Await();
			RMAppState finalState = appState.GetState();
			NUnit.Framework.Assert.AreEqual("Application is not in finalState.", finalState, 
				application.GetState());
		}

		public virtual void CreateRMStateForApplications(IDictionary<ApplicationId, ApplicationStateData
			> applicationState, RMAppState rmAppState)
		{
			RMApp app = CreateNewTestApp(null);
			ApplicationStateData appState = ApplicationStateData.NewInstance(app.GetSubmitTime
				(), app.GetStartTime(), app.GetUser(), app.GetApplicationSubmissionContext(), rmAppState
				, null, app.GetFinishTime());
			applicationState[app.GetApplicationId()] = appState;
		}

		[NUnit.Framework.Test]
		public virtual void TestGetAppReport()
		{
			RMApp app = CreateNewTestApp(null);
			AssertAppState(RMAppState.New, app);
			ApplicationReport report = app.CreateAndGetApplicationReport(null, true);
			NUnit.Framework.Assert.IsNotNull(report.GetApplicationResourceUsageReport());
			NUnit.Framework.Assert.AreEqual(report.GetApplicationResourceUsageReport(), RMServerUtils
				.DummyApplicationResourceUsageReport);
			report = app.CreateAndGetApplicationReport("clientuser", true);
			NUnit.Framework.Assert.IsNotNull(report.GetApplicationResourceUsageReport());
			NUnit.Framework.Assert.IsTrue("bad proxy url for app", report.GetTrackingUrl().EndsWith
				("/proxy/" + app.GetApplicationId() + "/"));
		}

		private void VerifyApplicationFinished(RMAppState state)
		{
			ArgumentCaptor<RMAppState> finalState = ArgumentCaptor.ForClass<RMAppState>();
			Org.Mockito.Mockito.Verify(writer).ApplicationFinished(Matchers.Any<RMApp>(), finalState
				.Capture());
			NUnit.Framework.Assert.AreEqual(state, finalState.GetValue());
			finalState = ArgumentCaptor.ForClass<RMAppState>();
			Org.Mockito.Mockito.Verify(publisher).AppFinished(Matchers.Any<RMApp>(), finalState
				.Capture(), Matchers.AnyLong());
			NUnit.Framework.Assert.AreEqual(state, finalState.GetValue());
		}

		private void VerifyAppRemovedSchedulerEvent(RMAppState finalState)
		{
			NUnit.Framework.Assert.AreEqual(SchedulerEventType.AppRemoved, schedulerDispatcher
				.lastSchedulerEvent.GetType());
			if (schedulerDispatcher.lastSchedulerEvent is AppRemovedSchedulerEvent)
			{
				AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)schedulerDispatcher
					.lastSchedulerEvent;
				NUnit.Framework.Assert.AreEqual(finalState, appRemovedEvent.GetFinalState());
			}
		}
	}
}
