using System;
using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Server.Webproxy;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	public class TestRMAppAttemptTransitions
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.TestRMAppAttemptTransitions
			));

		private const string EmptyDiagnostics = string.Empty;

		private static readonly string RmWebappAddr = WebAppUtils.GetResolvedRMWebAppURLWithScheme
			(new Configuration());

		private bool isSecurityEnabled;

		private RMContext rmContext;

		private RMContext spyRMContext;

		private YarnScheduler scheduler;

		private ResourceScheduler resourceScheduler;

		private ApplicationMasterService masterService;

		private ApplicationMasterLauncher applicationMasterLauncher;

		private AMLivelinessMonitor amLivelinessMonitor;

		private AMLivelinessMonitor amFinishingMonitor;

		private RMApplicationHistoryWriter writer;

		private SystemMetricsPublisher publisher;

		private RMStateStore store;

		private RMAppImpl application;

		private RMAppAttempt applicationAttempt;

		private Configuration conf = new Configuration();

		private AMRMTokenSecretManager amRMTokenManager;

		private ClientToAMTokenSecretManagerInRM clientToAMTokenManager = Org.Mockito.Mockito.Spy
			(new ClientToAMTokenSecretManagerInRM());

		private NMTokenSecretManagerInRM nmTokenManager;

		private bool transferStateFromPreviousAttempt = false;

		private EventHandler<RMNodeEvent> rmnodeEventHandler;

		private sealed class TestApplicationAttemptEventDispatcher : EventHandler<RMAppAttemptEvent
			>
		{
			public void Handle(RMAppAttemptEvent @event)
			{
				ApplicationAttemptId appID = @event.GetApplicationAttemptId();
				NUnit.Framework.Assert.AreEqual(this._enclosing.applicationAttempt.GetAppAttemptId
					(), appID);
				try
				{
					this._enclosing.applicationAttempt.Handle(@event);
				}
				catch (Exception t)
				{
					TestRMAppAttemptTransitions.Log.Error("Error in handling event type " + @event.GetType
						() + " for application " + appID, t);
				}
			}

			internal TestApplicationAttemptEventDispatcher(TestRMAppAttemptTransitions _enclosing
				)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMAppAttemptTransitions _enclosing;
		}

		private sealed class TestApplicationEventDispatcher : EventHandler<RMAppEvent>
		{
			// handle all the RM application events - same as in ResourceManager.java
			public void Handle(RMAppEvent @event)
			{
				NUnit.Framework.Assert.AreEqual(this._enclosing.application.GetApplicationId(), @event
					.GetApplicationId());
				if (@event is RMAppFailedAttemptEvent)
				{
					this._enclosing.transferStateFromPreviousAttempt = ((RMAppFailedAttemptEvent)@event
						).GetTransferStateFromPreviousAttempt();
				}
				try
				{
					this._enclosing.application.Handle(@event);
				}
				catch (Exception t)
				{
					TestRMAppAttemptTransitions.Log.Error("Error in handling event type " + @event.GetType
						() + " for application " + this._enclosing.application.GetApplicationId(), t);
				}
			}

			internal TestApplicationEventDispatcher(TestRMAppAttemptTransitions _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMAppAttemptTransitions _enclosing;
		}

		private sealed class TestSchedulerEventDispatcher : EventHandler<SchedulerEvent>
		{
			public void Handle(SchedulerEvent @event)
			{
				this._enclosing.scheduler.Handle(@event);
			}

			internal TestSchedulerEventDispatcher(TestRMAppAttemptTransitions _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMAppAttemptTransitions _enclosing;
		}

		private sealed class TestAMLauncherEventDispatcher : EventHandler<AMLauncherEvent
			>
		{
			public void Handle(AMLauncherEvent @event)
			{
				this._enclosing.applicationMasterLauncher.Handle(@event);
			}

			internal TestAMLauncherEventDispatcher(TestRMAppAttemptTransitions _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMAppAttemptTransitions _enclosing;
		}

		private static int appId = 1;

		private ApplicationSubmissionContext submissionContext = null;

		private bool unmanagedAM;

		[Parameterized.Parameters]
		public static ICollection<object[]> GetTestParameters()
		{
			return Arrays.AsList(new object[][] { new object[] { false }, new object[] { true
				 } });
		}

		public TestRMAppAttemptTransitions(bool isSecurityEnabled)
		{
			amRMTokenManager = Org.Mockito.Mockito.Spy(new AMRMTokenSecretManager(conf, rmContext
				));
			nmTokenManager = Org.Mockito.Mockito.Spy(new NMTokenSecretManagerInRM(conf));
			this.isSecurityEnabled = isSecurityEnabled;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			UserGroupInformation.AuthenticationMethod authMethod = UserGroupInformation.AuthenticationMethod
				.Simple;
			if (isSecurityEnabled)
			{
				authMethod = UserGroupInformation.AuthenticationMethod.Kerberos;
			}
			SecurityUtil.SetAuthenticationMethod(authMethod, conf);
			UserGroupInformation.SetConfiguration(conf);
			InlineDispatcher rmDispatcher = new InlineDispatcher();
			ContainerAllocationExpirer containerAllocationExpirer = Org.Mockito.Mockito.Mock<
				ContainerAllocationExpirer>();
			amLivelinessMonitor = Org.Mockito.Mockito.Mock<AMLivelinessMonitor>();
			amFinishingMonitor = Org.Mockito.Mockito.Mock<AMLivelinessMonitor>();
			writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter>();
			MasterKeyData masterKeyData = amRMTokenManager.CreateNewMasterKey();
			Org.Mockito.Mockito.When(amRMTokenManager.GetMasterKey()).ThenReturn(masterKeyData
				);
			rmContext = new RMContextImpl(rmDispatcher, containerAllocationExpirer, amLivelinessMonitor
				, amFinishingMonitor, null, amRMTokenManager, new RMContainerTokenSecretManager(
				conf), nmTokenManager, clientToAMTokenManager);
			store = Org.Mockito.Mockito.Mock<RMStateStore>();
			((RMContextImpl)rmContext).SetStateStore(store);
			publisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher>();
			rmContext.SetSystemMetricsPublisher(publisher);
			rmContext.SetRMApplicationHistoryWriter(writer);
			scheduler = Org.Mockito.Mockito.Mock<YarnScheduler>();
			masterService = Org.Mockito.Mockito.Mock<ApplicationMasterService>();
			applicationMasterLauncher = Org.Mockito.Mockito.Mock<ApplicationMasterLauncher>();
			rmDispatcher.Register(typeof(RMAppAttemptEventType), new TestRMAppAttemptTransitions.TestApplicationAttemptEventDispatcher
				(this));
			rmDispatcher.Register(typeof(RMAppEventType), new TestRMAppAttemptTransitions.TestApplicationEventDispatcher
				(this));
			rmDispatcher.Register(typeof(SchedulerEventType), new TestRMAppAttemptTransitions.TestSchedulerEventDispatcher
				(this));
			rmDispatcher.Register(typeof(AMLauncherEventType), new TestRMAppAttemptTransitions.TestAMLauncherEventDispatcher
				(this));
			rmnodeEventHandler = Org.Mockito.Mockito.Mock<RMNodeImpl>();
			rmDispatcher.Register(typeof(RMNodeEventType), rmnodeEventHandler);
			rmDispatcher.Init(conf);
			rmDispatcher.Start();
			ApplicationId applicationId = MockApps.NewAppID(appId++);
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, 0);
			resourceScheduler = Org.Mockito.Mockito.Mock<ResourceScheduler>();
			ApplicationResourceUsageReport appResUsgRpt = Org.Mockito.Mockito.Mock<ApplicationResourceUsageReport
				>();
			Org.Mockito.Mockito.When(appResUsgRpt.GetMemorySeconds()).ThenReturn(0L);
			Org.Mockito.Mockito.When(appResUsgRpt.GetVcoreSeconds()).ThenReturn(0L);
			Org.Mockito.Mockito.When(resourceScheduler.GetAppResourceUsageReport((ApplicationAttemptId
				)Matchers.Any())).ThenReturn(appResUsgRpt);
			spyRMContext = Org.Mockito.Mockito.Spy(rmContext);
			Org.Mockito.Mockito.DoReturn(resourceScheduler).When(spyRMContext).GetScheduler();
			string user = MockApps.NewUserName();
			string queue = MockApps.NewQueue();
			submissionContext = Org.Mockito.Mockito.Mock<ApplicationSubmissionContext>();
			Org.Mockito.Mockito.When(submissionContext.GetQueue()).ThenReturn(queue);
			Resource resource = BuilderUtils.NewResource(1536, 1);
			ContainerLaunchContext amContainerSpec = BuilderUtils.NewContainerLaunchContext(null
				, null, null, null, null, null);
			Org.Mockito.Mockito.When(submissionContext.GetAMContainerSpec()).ThenReturn(amContainerSpec
				);
			Org.Mockito.Mockito.When(submissionContext.GetResource()).ThenReturn(resource);
			unmanagedAM = false;
			application = Org.Mockito.Mockito.Mock<RMAppImpl>();
			applicationAttempt = new RMAppAttemptImpl(applicationAttemptId, spyRMContext, scheduler
				, masterService, submissionContext, new Configuration(), false, BuilderUtils.NewResourceRequest
				(RMAppAttemptImpl.AmContainerPriority, ResourceRequest.Any, submissionContext.GetResource
				(), 1));
			Org.Mockito.Mockito.When(application.GetCurrentAppAttempt()).ThenReturn(applicationAttempt
				);
			Org.Mockito.Mockito.When(application.GetApplicationId()).ThenReturn(applicationId
				);
			spyRMContext.GetRMApps()[application.GetApplicationId()] = application;
			TestAppAttemptNewState();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDown()
		{
			((AsyncDispatcher)this.spyRMContext.GetDispatcher()).Stop();
		}

		private string GetProxyUrl(RMAppAttempt appAttempt)
		{
			string url = null;
			string scheme = WebAppUtils.GetHttpSchemePrefix(conf);
			try
			{
				string proxy = WebAppUtils.GetProxyHostAndPort(conf);
				URI proxyUri = ProxyUriUtils.GetUriFromAMUrl(scheme, proxy);
				URI result = ProxyUriUtils.GetProxyUri(null, proxyUri, appAttempt.GetAppAttemptId
					().GetApplicationId());
				url = result.ToASCIIString();
			}
			catch (URISyntaxException)
			{
				NUnit.Framework.Assert.Fail();
			}
			return url;
		}

		/// <summary><see cref="RMAppAttemptState.New"/></summary>
		private void TestAppAttemptNewState()
		{
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.New, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetDiagnostics().Length);
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetJustFinishedContainers()
				.Count);
			NUnit.Framework.Assert.IsNull(applicationAttempt.GetMasterContainer());
			NUnit.Framework.Assert.AreEqual(0.0, (double)applicationAttempt.GetProgress(), 0.0001
				);
			NUnit.Framework.Assert.AreEqual(0, application.GetRanNodes().Count);
			NUnit.Framework.Assert.IsNull(applicationAttempt.GetFinalApplicationStatus());
			NUnit.Framework.Assert.IsNotNull(applicationAttempt.GetTrackingUrl());
			NUnit.Framework.Assert.IsFalse("N/A".Equals(applicationAttempt.GetTrackingUrl()));
		}

		/// <summary><see cref="RMAppAttemptState.Submitted"/></summary>
		private void TestAppAttemptSubmittedState()
		{
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Submitted, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetDiagnostics().Length);
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetJustFinishedContainers()
				.Count);
			NUnit.Framework.Assert.IsNull(applicationAttempt.GetMasterContainer());
			NUnit.Framework.Assert.AreEqual(0.0, (double)applicationAttempt.GetProgress(), 0.0001
				);
			NUnit.Framework.Assert.AreEqual(0, application.GetRanNodes().Count);
			NUnit.Framework.Assert.IsNull(applicationAttempt.GetFinalApplicationStatus());
			if (UserGroupInformation.IsSecurityEnabled())
			{
				Org.Mockito.Mockito.Verify(clientToAMTokenManager).CreateMasterKey(applicationAttempt
					.GetAppAttemptId());
				// can't create ClientToken as at this time ClientTokenMasterKey has
				// not been registered in the SecretManager
				NUnit.Framework.Assert.IsNull(applicationAttempt.CreateClientToken("some client")
					);
			}
			NUnit.Framework.Assert.IsNull(applicationAttempt.CreateClientToken(null));
			// Check events
			Org.Mockito.Mockito.Verify(masterService).RegisterAppAttempt(applicationAttempt.GetAppAttemptId
				());
			Org.Mockito.Mockito.Verify(scheduler).Handle(Matchers.Any<AppAttemptAddedSchedulerEvent
				>());
		}

		/// <summary>
		/// <see cref="RMAppAttemptState.Submitted"/>
		/// -&gt;
		/// <see cref="RMAppAttemptState.Failed"/>
		/// </summary>
		private void TestAppAttemptSubmittedToFailedState(string diagnostics)
		{
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(diagnostics, applicationAttempt.GetDiagnostics());
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetJustFinishedContainers()
				.Count);
			NUnit.Framework.Assert.IsNull(applicationAttempt.GetMasterContainer());
			NUnit.Framework.Assert.AreEqual(0.0, (double)applicationAttempt.GetProgress(), 0.0001
				);
			NUnit.Framework.Assert.AreEqual(0, application.GetRanNodes().Count);
			NUnit.Framework.Assert.IsNull(applicationAttempt.GetFinalApplicationStatus());
			// Check events
			Org.Mockito.Mockito.Verify(masterService).UnregisterAttempt(applicationAttempt.GetAppAttemptId
				());
			// ATTEMPT_FAILED should be notified to app if app attempt is submitted to
			// failed state.
			ArgumentMatcher<RMAppEvent> matcher = new _ArgumentMatcher_418();
			Org.Mockito.Mockito.Verify(application).Handle(Matchers.ArgThat(matcher));
			VerifyTokenCount(applicationAttempt.GetAppAttemptId(), 1);
			VerifyApplicationAttemptFinished(RMAppAttemptState.Failed);
		}

		private sealed class _ArgumentMatcher_418 : ArgumentMatcher<RMAppEvent>
		{
			public _ArgumentMatcher_418()
			{
			}

			public override bool Matches(object o)
			{
				RMAppEvent @event = (RMAppEvent)o;
				return @event.GetType() == RMAppEventType.AttemptFailed;
			}
		}

		/// <summary><see cref="RMAppAttemptState.Killed"/></summary>
		private void TestAppAttemptKilledState(Container amContainer, string diagnostics)
		{
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Killed, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(diagnostics, applicationAttempt.GetDiagnostics());
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetJustFinishedContainers()
				.Count);
			NUnit.Framework.Assert.AreEqual(amContainer, applicationAttempt.GetMasterContainer
				());
			NUnit.Framework.Assert.AreEqual(0.0, (double)applicationAttempt.GetProgress(), 0.0001
				);
			NUnit.Framework.Assert.AreEqual(0, application.GetRanNodes().Count);
			NUnit.Framework.Assert.IsNull(applicationAttempt.GetFinalApplicationStatus());
			VerifyTokenCount(applicationAttempt.GetAppAttemptId(), 1);
			VerifyAttemptFinalStateSaved();
			NUnit.Framework.Assert.IsFalse(transferStateFromPreviousAttempt);
			VerifyApplicationAttemptFinished(RMAppAttemptState.Killed);
		}

		/// <summary><see cref="RMAppAttemptState.Launched"/></summary>
		private void TestAppAttemptRecoveredState()
		{
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Launched, applicationAttempt.GetAppAttemptState
				());
		}

		/// <summary><see cref="RMAppAttemptState.Scheduled"/></summary>
		private void TestAppAttemptScheduledState()
		{
			RMAppAttemptState expectedState;
			int expectedAllocateCount;
			if (unmanagedAM)
			{
				expectedState = RMAppAttemptState.Launched;
				expectedAllocateCount = 0;
			}
			else
			{
				expectedState = RMAppAttemptState.Scheduled;
				expectedAllocateCount = 1;
			}
			NUnit.Framework.Assert.AreEqual(expectedState, applicationAttempt.GetAppAttemptState
				());
			Org.Mockito.Mockito.Verify(scheduler, Org.Mockito.Mockito.Times(expectedAllocateCount
				)).Allocate(Matchers.Any<ApplicationAttemptId>(), Matchers.Any<IList>(), Matchers.Any
				<IList>(), Matchers.Any<IList>(), Matchers.Any<IList>());
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetJustFinishedContainers()
				.Count);
			NUnit.Framework.Assert.IsNull(applicationAttempt.GetMasterContainer());
			NUnit.Framework.Assert.AreEqual(0.0, (double)applicationAttempt.GetProgress(), 0.0001
				);
			NUnit.Framework.Assert.AreEqual(0, application.GetRanNodes().Count);
			NUnit.Framework.Assert.IsNull(applicationAttempt.GetFinalApplicationStatus());
		}

		/// <summary><see cref="RMAppAttemptState.Allocated"/></summary>
		private void TestAppAttemptAllocatedState(Container amContainer)
		{
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Allocated, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(amContainer, applicationAttempt.GetMasterContainer
				());
			// Check events
			Org.Mockito.Mockito.Verify(applicationMasterLauncher).Handle(Matchers.Any<AMLauncherEvent
				>());
			Org.Mockito.Mockito.Verify(scheduler, Org.Mockito.Mockito.Times(2)).Allocate(Matchers.Any
				<ApplicationAttemptId>(), Matchers.Any<IList>(), Matchers.Any<IList>(), Matchers.Any
				<IList>(), Matchers.Any<IList>());
			Org.Mockito.Mockito.Verify(nmTokenManager).ClearNodeSetForAttempt(applicationAttempt
				.GetAppAttemptId());
		}

		/// <summary><see cref="RMAppAttemptState.Failed"/></summary>
		private void TestAppAttemptFailedState(Container container, string diagnostics)
		{
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(diagnostics, applicationAttempt.GetDiagnostics());
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetJustFinishedContainers()
				.Count);
			NUnit.Framework.Assert.AreEqual(container, applicationAttempt.GetMasterContainer(
				));
			NUnit.Framework.Assert.AreEqual(0.0, (double)applicationAttempt.GetProgress(), 0.0001
				);
			NUnit.Framework.Assert.AreEqual(0, application.GetRanNodes().Count);
			// Check events
			Org.Mockito.Mockito.Verify(application, Org.Mockito.Mockito.Times(1)).Handle(Matchers.Any
				<RMAppFailedAttemptEvent>());
			VerifyTokenCount(applicationAttempt.GetAppAttemptId(), 1);
			VerifyAttemptFinalStateSaved();
			VerifyApplicationAttemptFinished(RMAppAttemptState.Failed);
		}

		/// <summary><see cref="RMAppAttemptState#LAUNCH"/></summary>
		private void TestAppAttemptLaunchedState(Container container)
		{
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Launched, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(container, applicationAttempt.GetMasterContainer(
				));
			if (UserGroupInformation.IsSecurityEnabled())
			{
				// ClientTokenMasterKey has been registered in SecretManager, it's able to
				// create ClientToken now
				NUnit.Framework.Assert.IsNotNull(applicationAttempt.CreateClientToken("some client"
					));
			}
		}

		// TODO - need to add more checks relevant to this state
		/// <summary><see cref="RMAppAttemptState.Running"/></summary>
		private void TestAppAttemptRunningState(Container container, string host, int rpcPort
			, string trackingUrl, bool unmanagedAM)
		{
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Running, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(container, applicationAttempt.GetMasterContainer(
				));
			NUnit.Framework.Assert.AreEqual(host, applicationAttempt.GetHost());
			NUnit.Framework.Assert.AreEqual(rpcPort, applicationAttempt.GetRpcPort());
			VerifyUrl(trackingUrl, applicationAttempt.GetOriginalTrackingUrl());
			if (unmanagedAM)
			{
				VerifyUrl(trackingUrl, applicationAttempt.GetTrackingUrl());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(GetProxyUrl(applicationAttempt), applicationAttempt
					.GetTrackingUrl());
			}
		}

		// TODO - need to add more checks relevant to this state
		/// <summary><see cref="RMAppAttemptState.Finishing"/></summary>
		private void TestAppAttemptFinishingState(Container container, FinalApplicationStatus
			 finalStatus, string trackingUrl, string diagnostics)
		{
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Finishing, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(diagnostics, applicationAttempt.GetDiagnostics());
			VerifyUrl(trackingUrl, applicationAttempt.GetOriginalTrackingUrl());
			NUnit.Framework.Assert.AreEqual(GetProxyUrl(applicationAttempt), applicationAttempt
				.GetTrackingUrl());
			NUnit.Framework.Assert.AreEqual(container, applicationAttempt.GetMasterContainer(
				));
			NUnit.Framework.Assert.AreEqual(finalStatus, applicationAttempt.GetFinalApplicationStatus
				());
			VerifyTokenCount(applicationAttempt.GetAppAttemptId(), 0);
			VerifyAttemptFinalStateSaved();
		}

		/// <summary><see cref="RMAppAttemptState.Finished"/></summary>
		private void TestAppAttemptFinishedState(Container container, FinalApplicationStatus
			 finalStatus, string trackingUrl, string diagnostics, int finishedContainerCount
			, bool unmanagedAM)
		{
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Finished, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(diagnostics, applicationAttempt.GetDiagnostics());
			VerifyUrl(trackingUrl, applicationAttempt.GetOriginalTrackingUrl());
			if (unmanagedAM)
			{
				VerifyUrl(trackingUrl, applicationAttempt.GetTrackingUrl());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(GetProxyUrl(applicationAttempt), applicationAttempt
					.GetTrackingUrl());
				VerifyAttemptFinalStateSaved();
			}
			NUnit.Framework.Assert.AreEqual(finishedContainerCount, applicationAttempt.GetJustFinishedContainers
				().Count);
			NUnit.Framework.Assert.AreEqual(0, GetFinishedContainersSentToAM(applicationAttempt
				).Count);
			NUnit.Framework.Assert.AreEqual(container, applicationAttempt.GetMasterContainer(
				));
			NUnit.Framework.Assert.AreEqual(finalStatus, applicationAttempt.GetFinalApplicationStatus
				());
			VerifyTokenCount(applicationAttempt.GetAppAttemptId(), 1);
			NUnit.Framework.Assert.IsFalse(transferStateFromPreviousAttempt);
			VerifyApplicationAttemptFinished(RMAppAttemptState.Finished);
		}

		private void SubmitApplicationAttempt()
		{
			ApplicationAttemptId appAttemptId = applicationAttempt.GetAppAttemptId();
			applicationAttempt.Handle(new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType
				.Start));
			TestAppAttemptSubmittedState();
		}

		private void ScheduleApplicationAttempt()
		{
			SubmitApplicationAttempt();
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.AttemptAdded));
			if (unmanagedAM)
			{
				NUnit.Framework.Assert.AreEqual(RMAppAttemptState.LaunchedUnmanagedSaving, applicationAttempt
					.GetAppAttemptState());
				applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
					(), RMAppAttemptEventType.AttemptNewSaved));
			}
			TestAppAttemptScheduledState();
		}

		private Container AllocateApplicationAttempt()
		{
			ScheduleApplicationAttempt();
			// Mock the allocation of AM container 
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Resource resource = BuilderUtils.NewResource(2048, 1);
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(BuilderUtils.NewContainerId
				(applicationAttempt.GetAppAttemptId(), 1));
			Org.Mockito.Mockito.When(container.GetResource()).ThenReturn(resource);
			Allocation allocation = Org.Mockito.Mockito.Mock<Allocation>();
			Org.Mockito.Mockito.When(allocation.GetContainers()).ThenReturn(Sharpen.Collections
				.SingletonList(container));
			Org.Mockito.Mockito.When(scheduler.Allocate(Matchers.Any<ApplicationAttemptId>(), 
				Matchers.Any<IList>(), Matchers.Any<IList>(), Matchers.Any<IList>(), Matchers.Any
				<IList>())).ThenReturn(allocation);
			RMContainer rmContainer = Org.Mockito.Mockito.Mock<RMContainerImpl>();
			Org.Mockito.Mockito.When(scheduler.GetRMContainer(container.GetId())).ThenReturn(
				rmContainer);
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.ContainerAllocated));
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.AllocatedSaving, applicationAttempt
				.GetAppAttemptState());
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.AttemptNewSaved));
			TestAppAttemptAllocatedState(container);
			return container;
		}

		private void LaunchApplicationAttempt(Container container)
		{
			if (UserGroupInformation.IsSecurityEnabled())
			{
				// Before LAUNCHED state, can't create ClientToken as at this time
				// ClientTokenMasterKey has not been registered in the SecretManager
				NUnit.Framework.Assert.IsNull(applicationAttempt.CreateClientToken("some client")
					);
			}
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Launched));
			TestAppAttemptLaunchedState(container);
		}

		private void RunApplicationAttempt(Container container, string host, int rpcPort, 
			string trackingUrl, bool unmanagedAM)
		{
			applicationAttempt.Handle(new RMAppAttemptRegistrationEvent(applicationAttempt.GetAppAttemptId
				(), host, rpcPort, trackingUrl));
			TestAppAttemptRunningState(container, host, rpcPort, trackingUrl, unmanagedAM);
		}

		private void UnregisterApplicationAttempt(Container container, FinalApplicationStatus
			 finalStatus, string trackingUrl, string diagnostics)
		{
			applicationAttempt.Handle(new RMAppAttemptUnregistrationEvent(applicationAttempt.
				GetAppAttemptId(), trackingUrl, finalStatus, diagnostics));
			SendAttemptUpdateSavedEvent(applicationAttempt);
			TestAppAttemptFinishingState(container, finalStatus, trackingUrl, diagnostics);
		}

		private void TestUnmanagedAMSuccess(string url)
		{
			unmanagedAM = true;
			Org.Mockito.Mockito.When(submissionContext.GetUnmanagedAM()).ThenReturn(true);
			// submit AM and check it goes to LAUNCHED state
			ScheduleApplicationAttempt();
			TestAppAttemptLaunchedState(null);
			Org.Mockito.Mockito.Verify(amLivelinessMonitor, Org.Mockito.Mockito.Times(1)).Register
				(applicationAttempt.GetAppAttemptId());
			// launch AM
			RunApplicationAttempt(null, "host", 8042, url, true);
			// complete a container
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(NodeId.NewInstance("host"
				, 1234));
			application.Handle(new RMAppRunningOnNodeEvent(application.GetApplicationId(), container
				.GetNodeId()));
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(applicationAttempt
				.GetAppAttemptId(), Org.Mockito.Mockito.Mock<ContainerStatus>(), container.GetNodeId
				()));
			// complete AM
			string diagnostics = "Successful";
			FinalApplicationStatus finalStatus = FinalApplicationStatus.Succeeded;
			applicationAttempt.Handle(new RMAppAttemptUnregistrationEvent(applicationAttempt.
				GetAppAttemptId(), url, finalStatus, diagnostics));
			TestAppAttemptFinishedState(null, finalStatus, url, diagnostics, 1, true);
			NUnit.Framework.Assert.IsFalse(transferStateFromPreviousAttempt);
		}

		private void SendAttemptUpdateSavedEvent(RMAppAttempt applicationAttempt)
		{
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.AttemptUpdateSaved));
		}

		[NUnit.Framework.Test]
		public virtual void TestUsageReport()
		{
			// scheduler has info on running apps
			ApplicationAttemptId attemptId = applicationAttempt.GetAppAttemptId();
			ApplicationResourceUsageReport appResUsgRpt = Org.Mockito.Mockito.Mock<ApplicationResourceUsageReport
				>();
			Org.Mockito.Mockito.When(appResUsgRpt.GetMemorySeconds()).ThenReturn(123456L);
			Org.Mockito.Mockito.When(appResUsgRpt.GetVcoreSeconds()).ThenReturn(55544L);
			Org.Mockito.Mockito.When(scheduler.GetAppResourceUsageReport(Matchers.Any<ApplicationAttemptId
				>())).ThenReturn(appResUsgRpt);
			// start and finish the attempt
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			applicationAttempt.Handle(new RMAppAttemptUnregistrationEvent(attemptId, string.Empty
				, FinalApplicationStatus.Succeeded, string.Empty));
			// expect usage stats to come from the scheduler report
			ApplicationResourceUsageReport report = applicationAttempt.GetApplicationResourceUsageReport
				();
			NUnit.Framework.Assert.AreEqual(123456L, report.GetMemorySeconds());
			NUnit.Framework.Assert.AreEqual(55544L, report.GetVcoreSeconds());
			// finish app attempt and remove it from scheduler 
			Org.Mockito.Mockito.When(appResUsgRpt.GetMemorySeconds()).ThenReturn(223456L);
			Org.Mockito.Mockito.When(appResUsgRpt.GetVcoreSeconds()).ThenReturn(75544L);
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NodeId anyNodeId = NodeId.NewInstance("host", 1234);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(attemptId, ContainerStatus
				.NewInstance(amContainer.GetId(), ContainerState.Complete, string.Empty, 0), anyNodeId
				));
			Org.Mockito.Mockito.When(scheduler.GetSchedulerAppInfo(Matchers.Eq(attemptId))).ThenReturn
				(null);
			report = applicationAttempt.GetApplicationResourceUsageReport();
			NUnit.Framework.Assert.AreEqual(223456, report.GetMemorySeconds());
			NUnit.Framework.Assert.AreEqual(75544, report.GetVcoreSeconds());
		}

		[NUnit.Framework.Test]
		public virtual void TestUnmanagedAMUnexpectedRegistration()
		{
			unmanagedAM = true;
			Org.Mockito.Mockito.When(submissionContext.GetUnmanagedAM()).ThenReturn(true);
			// submit AM and check it goes to SUBMITTED state
			SubmitApplicationAttempt();
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Submitted, applicationAttempt.GetAppAttemptState
				());
			// launch AM and verify attempt failed
			applicationAttempt.Handle(new RMAppAttemptRegistrationEvent(applicationAttempt.GetAppAttemptId
				(), "host", 8042, "oldtrackingurl"));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Submitted, applicationAttempt
				.CreateApplicationAttemptState());
			TestAppAttemptSubmittedToFailedState("Unmanaged AM must register after AM attempt reaches LAUNCHED state."
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestUnmanagedAMContainersCleanup()
		{
			unmanagedAM = true;
			Org.Mockito.Mockito.When(submissionContext.GetUnmanagedAM()).ThenReturn(true);
			Org.Mockito.Mockito.When(submissionContext.GetKeepContainersAcrossApplicationAttempts
				()).ThenReturn(true);
			// submit AM and check it goes to SUBMITTED state
			SubmitApplicationAttempt();
			// launch AM and verify attempt failed
			applicationAttempt.Handle(new RMAppAttemptRegistrationEvent(applicationAttempt.GetAppAttemptId
				(), "host", 8042, "oldtrackingurl"));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Submitted, applicationAttempt
				.CreateApplicationAttemptState());
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.IsFalse(transferStateFromPreviousAttempt);
		}

		[NUnit.Framework.Test]
		public virtual void TestNewToKilled()
		{
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Kill));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.New, applicationAttempt
				.CreateApplicationAttemptState());
			TestAppAttemptKilledState(null, EmptyDiagnostics);
			VerifyTokenCount(applicationAttempt.GetAppAttemptId(), 1);
		}

		[NUnit.Framework.Test]
		public virtual void TestNewToRecovered()
		{
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Recover));
			TestAppAttemptRecoveredState();
		}

		[NUnit.Framework.Test]
		public virtual void TestSubmittedToKilled()
		{
			SubmitApplicationAttempt();
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Kill));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Submitted, applicationAttempt
				.CreateApplicationAttemptState());
			TestAppAttemptKilledState(null, EmptyDiagnostics);
		}

		[NUnit.Framework.Test]
		public virtual void TestScheduledToKilled()
		{
			ScheduleApplicationAttempt();
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Kill));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Scheduled, applicationAttempt
				.CreateApplicationAttemptState());
			TestAppAttemptKilledState(null, EmptyDiagnostics);
		}

		[NUnit.Framework.Test]
		public virtual void TestAMCrashAtScheduled()
		{
			// This is to test sending CONTAINER_FINISHED event at SCHEDULED state.
			// Verify the state transition is correct.
			ScheduleApplicationAttempt();
			ContainerStatus cs = SchedulerUtils.CreateAbnormalContainerStatus(BuilderUtils.NewContainerId
				(applicationAttempt.GetAppAttemptId(), 1), SchedulerUtils.LostContainer);
			// send CONTAINER_FINISHED event at SCHEDULED state,
			// The state should be FINAL_SAVING with previous state SCHEDULED
			NodeId anyNodeId = NodeId.NewInstance("host", 1234);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(applicationAttempt
				.GetAppAttemptId(), cs, anyNodeId));
			// createApplicationAttemptState will return previous state (SCHEDULED),
			// if the current state is FINAL_SAVING.
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Scheduled, applicationAttempt
				.CreateApplicationAttemptState());
			// send ATTEMPT_UPDATE_SAVED event,
			// verify the state is changed to state FAILED.
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, applicationAttempt.GetAppAttemptState
				());
			VerifyApplicationAttemptFinished(RMAppAttemptState.Failed);
		}

		[NUnit.Framework.Test]
		public virtual void TestAllocatedToKilled()
		{
			Container amContainer = AllocateApplicationAttempt();
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Kill));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Allocated, applicationAttempt
				.CreateApplicationAttemptState());
			TestAppAttemptKilledState(amContainer, EmptyDiagnostics);
		}

		[NUnit.Framework.Test]
		public virtual void TestAllocatedToFailed()
		{
			Container amContainer = AllocateApplicationAttempt();
			string diagnostics = "Launch Failed";
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.LaunchFailed, diagnostics));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Allocated, applicationAttempt
				.CreateApplicationAttemptState());
			TestAppAttemptFailedState(amContainer, diagnostics);
		}

		public virtual void TestLaunchedAtFinalSaving()
		{
			Container amContainer = AllocateApplicationAttempt();
			// ALLOCATED->FINAL_SAVING
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Kill));
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			// verify for both launched and launch_failed transitions in final_saving
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Launched));
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.LaunchFailed, "Launch Failed"));
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			TestAppAttemptKilledState(amContainer, EmptyDiagnostics);
			// verify for both launched and launch_failed transitions in killed
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Launched));
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.LaunchFailed, "Launch Failed"));
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Killed, applicationAttempt.GetAppAttemptState
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestAMCrashAtAllocated()
		{
			Container amContainer = AllocateApplicationAttempt();
			string containerDiagMsg = "some error";
			int exitCode = 123;
			ContainerStatus cs = BuilderUtils.NewContainerStatus(amContainer.GetId(), ContainerState
				.Complete, containerDiagMsg, exitCode);
			NodeId anyNodeId = NodeId.NewInstance("host", 1234);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(applicationAttempt
				.GetAppAttemptId(), cs, anyNodeId));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Allocated, applicationAttempt
				.CreateApplicationAttemptState());
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, applicationAttempt.GetAppAttemptState
				());
			VerifyTokenCount(applicationAttempt.GetAppAttemptId(), 1);
			VerifyApplicationAttemptFinished(RMAppAttemptState.Failed);
			bool shouldCheckURL = (applicationAttempt.GetTrackingUrl() != null);
			VerifyAMCrashAtAllocatedDiagnosticInfo(applicationAttempt.GetDiagnostics(), exitCode
				, shouldCheckURL);
		}

		[NUnit.Framework.Test]
		public virtual void TestRunningToFailed()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			string containerDiagMsg = "some error";
			int exitCode = 123;
			ContainerStatus cs = BuilderUtils.NewContainerStatus(amContainer.GetId(), ContainerState
				.Complete, containerDiagMsg, exitCode);
			ApplicationAttemptId appAttemptId = applicationAttempt.GetAppAttemptId();
			NodeId anyNodeId = NodeId.NewInstance("host", 1234);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(appAttemptId, cs
				, anyNodeId));
			// ignored ContainerFinished and Expire at FinalSaving if we were supposed
			// to Failed state.
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(applicationAttempt
				.GetAppAttemptId(), BuilderUtils.NewContainerStatus(amContainer.GetId(), ContainerState
				.Complete, string.Empty, 0), anyNodeId));
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Expire));
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Running, applicationAttempt
				.CreateApplicationAttemptState());
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetJustFinishedContainers()
				.Count);
			NUnit.Framework.Assert.AreEqual(amContainer, applicationAttempt.GetMasterContainer
				());
			NUnit.Framework.Assert.AreEqual(0, application.GetRanNodes().Count);
			string rmAppPageUrl = StringHelper.Pjoin(RmWebappAddr, "cluster", "app", applicationAttempt
				.GetAppAttemptId().GetApplicationId());
			NUnit.Framework.Assert.AreEqual(rmAppPageUrl, applicationAttempt.GetOriginalTrackingUrl
				());
			NUnit.Framework.Assert.AreEqual(rmAppPageUrl, applicationAttempt.GetTrackingUrl()
				);
			VerifyAMHostAndPortInvalidated();
			VerifyApplicationAttemptFinished(RMAppAttemptState.Failed);
		}

		[NUnit.Framework.Test]
		public virtual void TestRunningToKilled()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Kill));
			// ignored ContainerFinished and Expire at FinalSaving if we were supposed
			// to Killed state.
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			NodeId anyNodeId = NodeId.NewInstance("host", 1234);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(applicationAttempt
				.GetAppAttemptId(), BuilderUtils.NewContainerStatus(amContainer.GetId(), ContainerState
				.Complete, string.Empty, 0), anyNodeId));
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Expire));
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Running, applicationAttempt
				.CreateApplicationAttemptState());
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Killed, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetJustFinishedContainers()
				.Count);
			NUnit.Framework.Assert.AreEqual(amContainer, applicationAttempt.GetMasterContainer
				());
			NUnit.Framework.Assert.AreEqual(0, application.GetRanNodes().Count);
			string rmAppPageUrl = StringHelper.Pjoin(RmWebappAddr, "cluster", "app", applicationAttempt
				.GetAppAttemptId().GetApplicationId());
			NUnit.Framework.Assert.AreEqual(rmAppPageUrl, applicationAttempt.GetOriginalTrackingUrl
				());
			NUnit.Framework.Assert.AreEqual(rmAppPageUrl, applicationAttempt.GetTrackingUrl()
				);
			VerifyTokenCount(applicationAttempt.GetAppAttemptId(), 1);
			VerifyAMHostAndPortInvalidated();
			VerifyApplicationAttemptFinished(RMAppAttemptState.Killed);
		}

		public virtual void TestLaunchedExpire()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Expire));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Launched, applicationAttempt
				.CreateApplicationAttemptState());
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.IsTrue("expire diagnostics missing", applicationAttempt.GetDiagnostics
				().Contains("timed out"));
			string rmAppPageUrl = StringHelper.Pjoin(RmWebappAddr, "cluster", "app", applicationAttempt
				.GetAppAttemptId().GetApplicationId());
			NUnit.Framework.Assert.AreEqual(rmAppPageUrl, applicationAttempt.GetOriginalTrackingUrl
				());
			NUnit.Framework.Assert.AreEqual(rmAppPageUrl, applicationAttempt.GetTrackingUrl()
				);
			VerifyTokenCount(applicationAttempt.GetAppAttemptId(), 1);
			VerifyApplicationAttemptFinished(RMAppAttemptState.Failed);
		}

		public virtual void TestRunningExpire()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Expire));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Running, applicationAttempt
				.CreateApplicationAttemptState());
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.IsTrue("expire diagnostics missing", applicationAttempt.GetDiagnostics
				().Contains("timed out"));
			string rmAppPageUrl = StringHelper.Pjoin(RmWebappAddr, "cluster", "app", applicationAttempt
				.GetAppAttemptId().GetApplicationId());
			NUnit.Framework.Assert.AreEqual(rmAppPageUrl, applicationAttempt.GetOriginalTrackingUrl
				());
			NUnit.Framework.Assert.AreEqual(rmAppPageUrl, applicationAttempt.GetTrackingUrl()
				);
			VerifyTokenCount(applicationAttempt.GetAppAttemptId(), 1);
			VerifyAMHostAndPortInvalidated();
			VerifyApplicationAttemptFinished(RMAppAttemptState.Failed);
		}

		[NUnit.Framework.Test]
		public virtual void TestUnregisterToKilledFinishing()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			UnregisterApplicationAttempt(amContainer, FinalApplicationStatus.Killed, "newtrackingurl"
				, "Killed by user");
		}

		[NUnit.Framework.Test]
		public virtual void TestTrackingUrlUnmanagedAM()
		{
			TestUnmanagedAMSuccess("oldTrackingUrl");
		}

		[NUnit.Framework.Test]
		public virtual void TestEmptyTrackingUrlUnmanagedAM()
		{
			TestUnmanagedAMSuccess(string.Empty);
		}

		[NUnit.Framework.Test]
		public virtual void TestNullTrackingUrlUnmanagedAM()
		{
			TestUnmanagedAMSuccess(null);
		}

		[NUnit.Framework.Test]
		public virtual void TestManagedAMWithTrackingUrl()
		{
			TestTrackingUrlManagedAM("theTrackingUrl");
		}

		[NUnit.Framework.Test]
		public virtual void TestManagedAMWithEmptyTrackingUrl()
		{
			TestTrackingUrlManagedAM(string.Empty);
		}

		[NUnit.Framework.Test]
		public virtual void TestManagedAMWithNullTrackingUrl()
		{
			TestTrackingUrlManagedAM(null);
		}

		private void TestTrackingUrlManagedAM(string url)
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, url, false);
			UnregisterApplicationAttempt(amContainer, FinalApplicationStatus.Succeeded, url, 
				"Successful");
		}

		[NUnit.Framework.Test]
		public virtual void TestUnregisterToSuccessfulFinishing()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			UnregisterApplicationAttempt(amContainer, FinalApplicationStatus.Succeeded, "mytrackingurl"
				, "Successful");
		}

		[NUnit.Framework.Test]
		public virtual void TestFinishingKill()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			FinalApplicationStatus finalStatus = FinalApplicationStatus.Failed;
			string trackingUrl = "newtrackingurl";
			string diagnostics = "Job failed";
			UnregisterApplicationAttempt(amContainer, finalStatus, trackingUrl, diagnostics);
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Kill));
			TestAppAttemptFinishingState(amContainer, finalStatus, trackingUrl, diagnostics);
		}

		[NUnit.Framework.Test]
		public virtual void TestFinishingExpire()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			FinalApplicationStatus finalStatus = FinalApplicationStatus.Succeeded;
			string trackingUrl = "mytrackingurl";
			string diagnostics = "Successful";
			UnregisterApplicationAttempt(amContainer, finalStatus, trackingUrl, diagnostics);
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Expire));
			TestAppAttemptFinishedState(amContainer, finalStatus, trackingUrl, diagnostics, 0
				, false);
		}

		[NUnit.Framework.Test]
		public virtual void TestFinishingToFinishing()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			FinalApplicationStatus finalStatus = FinalApplicationStatus.Succeeded;
			string trackingUrl = "mytrackingurl";
			string diagnostics = "Successful";
			UnregisterApplicationAttempt(amContainer, finalStatus, trackingUrl, diagnostics);
			// container must be AM container to move from FINISHING to FINISHED
			NodeId anyNodeId = NodeId.NewInstance("host", 1234);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(applicationAttempt
				.GetAppAttemptId(), BuilderUtils.NewContainerStatus(BuilderUtils.NewContainerId(
				applicationAttempt.GetAppAttemptId(), 42), ContainerState.Complete, string.Empty
				, 0), anyNodeId));
			TestAppAttemptFinishingState(amContainer, finalStatus, trackingUrl, diagnostics);
		}

		[NUnit.Framework.Test]
		public virtual void TestSuccessfulFinishingToFinished()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			FinalApplicationStatus finalStatus = FinalApplicationStatus.Succeeded;
			string trackingUrl = "mytrackingurl";
			string diagnostics = "Successful";
			UnregisterApplicationAttempt(amContainer, finalStatus, trackingUrl, diagnostics);
			NodeId anyNodeId = NodeId.NewInstance("host", 1234);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(applicationAttempt
				.GetAppAttemptId(), BuilderUtils.NewContainerStatus(amContainer.GetId(), ContainerState
				.Complete, string.Empty, 0), anyNodeId));
			TestAppAttemptFinishedState(amContainer, finalStatus, trackingUrl, diagnostics, 0
				, false);
		}

		// While attempt is at FINAL_SAVING, Contaienr_Finished event may come before
		// Attempt_Saved event, we stay on FINAL_SAVING on Container_Finished event
		// and then directly jump from FINAL_SAVING to FINISHED state on Attempt_Saved
		// event
		[NUnit.Framework.Test]
		public virtual void TestFinalSavingToFinishedWithContainerFinished()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			FinalApplicationStatus finalStatus = FinalApplicationStatus.Succeeded;
			string trackingUrl = "mytrackingurl";
			string diagnostics = "Successful";
			applicationAttempt.Handle(new RMAppAttemptUnregistrationEvent(applicationAttempt.
				GetAppAttemptId(), trackingUrl, finalStatus, diagnostics));
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Running, applicationAttempt
				.CreateApplicationAttemptState());
			// Container_finished event comes before Attempt_Saved event.
			NodeId anyNodeId = NodeId.NewInstance("host", 1234);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(applicationAttempt
				.GetAppAttemptId(), BuilderUtils.NewContainerStatus(amContainer.GetId(), ContainerState
				.Complete, string.Empty, 0), anyNodeId));
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			// send attempt_saved
			SendAttemptUpdateSavedEvent(applicationAttempt);
			TestAppAttemptFinishedState(amContainer, finalStatus, trackingUrl, diagnostics, 0
				, false);
		}

		// While attempt is at FINAL_SAVING, Expire event may come before
		// Attempt_Saved event, we stay on FINAL_SAVING on Expire event and then
		// directly jump from FINAL_SAVING to FINISHED state on Attempt_Saved event.
		[NUnit.Framework.Test]
		public virtual void TestFinalSavingToFinishedWithExpire()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			FinalApplicationStatus finalStatus = FinalApplicationStatus.Succeeded;
			string trackingUrl = "mytrackingurl";
			string diagnostics = "Successssseeeful";
			applicationAttempt.Handle(new RMAppAttemptUnregistrationEvent(applicationAttempt.
				GetAppAttemptId(), trackingUrl, finalStatus, diagnostics));
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Running, applicationAttempt
				.CreateApplicationAttemptState());
			// Expire event comes before Attempt_saved event.
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Expire));
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.FinalSaving, applicationAttempt
				.GetAppAttemptState());
			// send attempt_saved
			SendAttemptUpdateSavedEvent(applicationAttempt);
			TestAppAttemptFinishedState(amContainer, finalStatus, trackingUrl, diagnostics, 0
				, false);
		}

		[NUnit.Framework.Test]
		public virtual void TestFinishedContainer()
		{
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			// Complete one container
			ContainerId containerId1 = BuilderUtils.NewContainerId(applicationAttempt.GetAppAttemptId
				(), 2);
			Container container1 = Org.Mockito.Mockito.Mock<Container>();
			ContainerStatus containerStatus1 = Org.Mockito.Mockito.Mock<ContainerStatus>();
			Org.Mockito.Mockito.When(container1.GetId()).ThenReturn(containerId1);
			Org.Mockito.Mockito.When(containerStatus1.GetContainerId()).ThenReturn(containerId1
				);
			Org.Mockito.Mockito.When(container1.GetNodeId()).ThenReturn(NodeId.NewInstance("host"
				, 1234));
			application.Handle(new RMAppRunningOnNodeEvent(application.GetApplicationId(), container1
				.GetNodeId()));
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(applicationAttempt
				.GetAppAttemptId(), containerStatus1, container1.GetNodeId()));
			ArgumentCaptor<RMNodeFinishedContainersPulledByAMEvent> captor = ArgumentCaptor.ForClass
				<RMNodeFinishedContainersPulledByAMEvent>();
			// Verify justFinishedContainers
			NUnit.Framework.Assert.AreEqual(1, applicationAttempt.GetJustFinishedContainers()
				.Count);
			NUnit.Framework.Assert.AreEqual(container1.GetId(), applicationAttempt.GetJustFinishedContainers
				()[0].GetContainerId());
			NUnit.Framework.Assert.AreEqual(0, GetFinishedContainersSentToAM(applicationAttempt
				).Count);
			// Verify finishedContainersSentToAM gets container after pull
			IList<ContainerStatus> containerStatuses = applicationAttempt.PullJustFinishedContainers
				();
			NUnit.Framework.Assert.AreEqual(1, containerStatuses.Count);
			Org.Mockito.Mockito.Verify(rmnodeEventHandler, Org.Mockito.Mockito.Never()).Handle
				(Org.Mockito.Mockito.Any<RMNodeEvent>());
			NUnit.Framework.Assert.IsTrue(applicationAttempt.GetJustFinishedContainers().IsEmpty
				());
			NUnit.Framework.Assert.AreEqual(1, GetFinishedContainersSentToAM(applicationAttempt
				).Count);
			// Verify container is acked to NM via the RMNodeEvent after second pull
			containerStatuses = applicationAttempt.PullJustFinishedContainers();
			NUnit.Framework.Assert.AreEqual(0, containerStatuses.Count);
			Org.Mockito.Mockito.Verify(rmnodeEventHandler).Handle(captor.Capture());
			NUnit.Framework.Assert.AreEqual(container1.GetId(), captor.GetValue().GetContainers
				()[0]);
			NUnit.Framework.Assert.IsTrue(applicationAttempt.GetJustFinishedContainers().IsEmpty
				());
			NUnit.Framework.Assert.AreEqual(0, GetFinishedContainersSentToAM(applicationAttempt
				).Count);
		}

		private static IList<ContainerStatus> GetFinishedContainersSentToAM(RMAppAttempt 
			applicationAttempt)
		{
			IList<ContainerStatus> containers = new AList<ContainerStatus>();
			foreach (IList<ContainerStatus> containerStatuses in applicationAttempt.GetFinishedContainersSentToAMReference
				().Values)
			{
				Sharpen.Collections.AddAll(containers, containerStatuses);
			}
			return containers;
		}

		// this is to test user can get client tokens only after the client token
		// master key is saved in the state store and also registered in
		// ClientTokenSecretManager
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClientToken()
		{
			Assume.AssumeTrue(isSecurityEnabled);
			Container amContainer = AllocateApplicationAttempt();
			// before attempt is launched, can not get ClientToken
			Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> token = applicationAttempt
				.CreateClientToken(null);
			NUnit.Framework.Assert.IsNull(token);
			token = applicationAttempt.CreateClientToken("clientuser");
			NUnit.Framework.Assert.IsNull(token);
			LaunchApplicationAttempt(amContainer);
			// after attempt is launched , can get ClientToken
			token = applicationAttempt.CreateClientToken(null);
			NUnit.Framework.Assert.IsNull(token);
			token = applicationAttempt.CreateClientToken("clientuser");
			NUnit.Framework.Assert.IsNotNull(token);
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Kill));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Launched, applicationAttempt
				.CreateApplicationAttemptState());
			SendAttemptUpdateSavedEvent(applicationAttempt);
			// after attempt is killed, can not get Client Token
			token = applicationAttempt.CreateClientToken(null);
			NUnit.Framework.Assert.IsNull(token);
			token = applicationAttempt.CreateClientToken("clientuser");
			NUnit.Framework.Assert.IsNull(token);
		}

		// this is to test master key is saved in the secret manager only after
		// attempt is launched and in secure-mode
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationAttemptMasterKey()
		{
			Container amContainer = AllocateApplicationAttempt();
			ApplicationAttemptId appid = applicationAttempt.GetAppAttemptId();
			bool isMasterKeyExisted = false;
			// before attempt is launched, can not get MasterKey
			isMasterKeyExisted = clientToAMTokenManager.HasMasterKey(appid);
			NUnit.Framework.Assert.IsFalse(isMasterKeyExisted);
			LaunchApplicationAttempt(amContainer);
			// after attempt is launched and in secure mode, can get MasterKey
			isMasterKeyExisted = clientToAMTokenManager.HasMasterKey(appid);
			if (isSecurityEnabled)
			{
				NUnit.Framework.Assert.IsTrue(isMasterKeyExisted);
				NUnit.Framework.Assert.IsNotNull(clientToAMTokenManager.GetMasterKey(appid));
			}
			else
			{
				NUnit.Framework.Assert.IsFalse(isMasterKeyExisted);
			}
			applicationAttempt.Handle(new RMAppAttemptEvent(applicationAttempt.GetAppAttemptId
				(), RMAppAttemptEventType.Kill));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Launched, applicationAttempt
				.CreateApplicationAttemptState());
			SendAttemptUpdateSavedEvent(applicationAttempt);
			// after attempt is killed, can not get MasterKey
			isMasterKeyExisted = clientToAMTokenManager.HasMasterKey(appid);
			NUnit.Framework.Assert.IsFalse(isMasterKeyExisted);
		}

		[NUnit.Framework.Test]
		public virtual void TestFailedToFailed()
		{
			// create a failed attempt.
			Org.Mockito.Mockito.When(submissionContext.GetKeepContainersAcrossApplicationAttempts
				()).ThenReturn(true);
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			ContainerStatus cs1 = ContainerStatus.NewInstance(amContainer.GetId(), ContainerState
				.Complete, "some error", 123);
			ApplicationAttemptId appAttemptId = applicationAttempt.GetAppAttemptId();
			NodeId anyNodeId = NodeId.NewInstance("host", 1234);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(appAttemptId, cs1
				, anyNodeId));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Running, applicationAttempt
				.CreateApplicationAttemptState());
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, applicationAttempt.GetAppAttemptState
				());
			// should not kill containers when attempt fails.
			NUnit.Framework.Assert.IsTrue(transferStateFromPreviousAttempt);
			VerifyApplicationAttemptFinished(RMAppAttemptState.Failed);
			// failed attempt captured the container finished event.
			NUnit.Framework.Assert.AreEqual(0, applicationAttempt.GetJustFinishedContainers()
				.Count);
			ContainerStatus cs2 = ContainerStatus.NewInstance(ContainerId.NewContainerId(appAttemptId
				, 2), ContainerState.Complete, string.Empty, 0);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(appAttemptId, cs2
				, anyNodeId));
			NUnit.Framework.Assert.AreEqual(1, applicationAttempt.GetJustFinishedContainers()
				.Count);
			bool found = false;
			foreach (ContainerStatus containerStatus in applicationAttempt.GetJustFinishedContainers
				())
			{
				if (cs2.GetContainerId().Equals(containerStatus.GetContainerId()))
				{
					found = true;
				}
			}
			NUnit.Framework.Assert.IsTrue(found);
		}

		[NUnit.Framework.Test]
		public virtual void TestContainersCleanupForLastAttempt()
		{
			// create a failed attempt.
			applicationAttempt = new RMAppAttemptImpl(applicationAttempt.GetAppAttemptId(), spyRMContext
				, scheduler, masterService, submissionContext, new Configuration(), true, BuilderUtils
				.NewResourceRequest(RMAppAttemptImpl.AmContainerPriority, ResourceRequest.Any, submissionContext
				.GetResource(), 1));
			Org.Mockito.Mockito.When(submissionContext.GetKeepContainersAcrossApplicationAttempts
				()).ThenReturn(true);
			Org.Mockito.Mockito.When(submissionContext.GetMaxAppAttempts()).ThenReturn(1);
			Container amContainer = AllocateApplicationAttempt();
			LaunchApplicationAttempt(amContainer);
			RunApplicationAttempt(amContainer, "host", 8042, "oldtrackingurl", false);
			ContainerStatus cs1 = ContainerStatus.NewInstance(amContainer.GetId(), ContainerState
				.Complete, "some error", 123);
			ApplicationAttemptId appAttemptId = applicationAttempt.GetAppAttemptId();
			NodeId anyNodeId = NodeId.NewInstance("host", 1234);
			applicationAttempt.Handle(new RMAppAttemptContainerFinishedEvent(appAttemptId, cs1
				, anyNodeId));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Running, applicationAttempt
				.CreateApplicationAttemptState());
			SendAttemptUpdateSavedEvent(applicationAttempt);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, applicationAttempt.GetAppAttemptState
				());
			NUnit.Framework.Assert.IsFalse(transferStateFromPreviousAttempt);
			VerifyApplicationAttemptFinished(RMAppAttemptState.Failed);
		}

		[NUnit.Framework.Test]
		public virtual void TestScheduleTransitionReplaceAMContainerRequestWithDefaults()
		{
			YarnScheduler mockScheduler = Org.Mockito.Mockito.Mock<YarnScheduler>();
			Org.Mockito.Mockito.When(mockScheduler.Allocate(Matchers.Any<ApplicationAttemptId
				>(), Matchers.Any<IList>(), Matchers.Any<IList>(), Matchers.Any<IList>(), Matchers.Any
				<IList>())).ThenAnswer(new _Answer_1516());
			// capacity shouldn't changed
			// priority, #container, relax-locality will be changed
			// just return an empty allocation
			// create an attempt.
			applicationAttempt = new RMAppAttemptImpl(applicationAttempt.GetAppAttemptId(), spyRMContext
				, scheduler, masterService, submissionContext, new Configuration(), true, ResourceRequest
				.NewInstance(Priority.Undefined, "host1", Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(3333, 1), 3, false, "label-expression"));
			new RMAppAttemptImpl.ScheduleTransition().Transition((RMAppAttemptImpl)applicationAttempt
				, null);
		}

		private sealed class _Answer_1516 : Answer<Allocation>
		{
			public _Answer_1516()
			{
			}

			/// <exception cref="System.Exception"/>
			public Allocation Answer(InvocationOnMock invocation)
			{
				ResourceRequest rr = (ResourceRequest)((IList)invocation.GetArguments()[1])[0];
				NUnit.Framework.Assert.AreEqual(Resource.NewInstance(3333, 1), rr.GetCapability()
					);
				NUnit.Framework.Assert.AreEqual("label-expression", rr.GetNodeLabelExpression());
				NUnit.Framework.Assert.AreEqual(RMAppAttemptImpl.AmContainerPriority, rr.GetPriority
					());
				NUnit.Framework.Assert.AreEqual(1, rr.GetNumContainers());
				NUnit.Framework.Assert.AreEqual(ResourceRequest.Any, rr.GetResourceName());
				IList l = new ArrayList();
				Set s = new HashSet();
				return new Allocation(l, Resources.None(), s, s, l);
			}
		}

		private void VerifyAMCrashAtAllocatedDiagnosticInfo(string diagnostics, int exitCode
			, bool shouldCheckURL)
		{
			NUnit.Framework.Assert.IsTrue("Diagnostic information does not point the logs to the users"
				, diagnostics.Contains("logs"));
			NUnit.Framework.Assert.IsTrue("Diagnostic information does not contain application attempt id"
				, diagnostics.Contains(applicationAttempt.GetAppAttemptId().ToString()));
			NUnit.Framework.Assert.IsTrue("Diagnostic information does not contain application exit code"
				, diagnostics.Contains("exitCode: " + exitCode));
			if (shouldCheckURL)
			{
				NUnit.Framework.Assert.IsTrue("Diagnostic information does not contain application proxy URL"
					, diagnostics.Contains(applicationAttempt.GetTrackingUrl()));
			}
		}

		private void VerifyTokenCount(ApplicationAttemptId appAttemptId, int count)
		{
			Org.Mockito.Mockito.Verify(amRMTokenManager, Org.Mockito.Mockito.Times(count)).ApplicationMasterFinished
				(appAttemptId);
			if (UserGroupInformation.IsSecurityEnabled())
			{
				Org.Mockito.Mockito.Verify(clientToAMTokenManager, Org.Mockito.Mockito.Times(count
					)).UnRegisterApplication(appAttemptId);
				if (count > 0)
				{
					NUnit.Framework.Assert.IsNull(applicationAttempt.CreateClientToken("client"));
				}
			}
		}

		private void VerifyUrl(string url1, string url2)
		{
			if (url1 == null || url1.Trim().IsEmpty())
			{
				NUnit.Framework.Assert.AreEqual("N/A", url2);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(url1, url2);
			}
		}

		private void VerifyAttemptFinalStateSaved()
		{
			Org.Mockito.Mockito.Verify(store, Org.Mockito.Mockito.Times(1)).UpdateApplicationAttemptState
				(Matchers.Any<ApplicationAttemptStateData>());
		}

		private void VerifyAMHostAndPortInvalidated()
		{
			NUnit.Framework.Assert.AreEqual("N/A", applicationAttempt.GetHost());
			NUnit.Framework.Assert.AreEqual(-1, applicationAttempt.GetRpcPort());
		}

		private void VerifyApplicationAttemptFinished(RMAppAttemptState state)
		{
			ArgumentCaptor<RMAppAttemptState> finalState = ArgumentCaptor.ForClass<RMAppAttemptState
				>();
			Org.Mockito.Mockito.Verify(writer).ApplicationAttemptFinished(Matchers.Any<RMAppAttempt
				>(), finalState.Capture());
			NUnit.Framework.Assert.AreEqual(state, finalState.GetValue());
			finalState = ArgumentCaptor.ForClass<RMAppAttemptState>();
			Org.Mockito.Mockito.Verify(publisher).AppAttemptFinished(Matchers.Any<RMAppAttempt
				>(), finalState.Capture(), Matchers.Any<RMApp>(), Matchers.AnyLong());
			NUnit.Framework.Assert.AreEqual(state, finalState.GetValue());
		}
	}
}
