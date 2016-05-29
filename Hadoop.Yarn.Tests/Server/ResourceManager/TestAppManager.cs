using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>Testing applications being retired from RM.</summary>
	public class TestAppManager
	{
		private Log Log = LogFactory.GetLog(typeof(TestAppManager));

		private static RMAppEventType appEventType = RMAppEventType.Kill;

		public virtual RMAppEventType GetAppEventType()
		{
			lock (this)
			{
				return appEventType;
			}
		}

		public virtual void SetAppEventType(RMAppEventType newType)
		{
			lock (this)
			{
				appEventType = newType;
			}
		}

		public static IList<RMApp> NewRMApps(int n, long time, RMAppState state)
		{
			IList<RMApp> list = Lists.NewArrayList();
			for (int i = 0; i < n; ++i)
			{
				list.AddItem(new MockRMApp(i, time, state));
			}
			return list;
		}

		public virtual RMContext MockRMContext(int n, long time)
		{
			IList<RMApp> apps = NewRMApps(n, time, RMAppState.Finished);
			ConcurrentMap<ApplicationId, RMApp> map = Maps.NewConcurrentMap();
			foreach (RMApp app in apps)
			{
				map[app.GetApplicationId()] = app;
			}
			Dispatcher rmDispatcher = new AsyncDispatcher();
			ContainerAllocationExpirer containerAllocationExpirer = new ContainerAllocationExpirer
				(rmDispatcher);
			AMLivelinessMonitor amLivelinessMonitor = new AMLivelinessMonitor(rmDispatcher);
			AMLivelinessMonitor amFinishingMonitor = new AMLivelinessMonitor(rmDispatcher);
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			RMContext context = new _RMContextImpl_121(map, rmDispatcher, containerAllocationExpirer
				, amLivelinessMonitor, amFinishingMonitor, null, null, null, null, null);
			((RMContextImpl)context).SetStateStore(Org.Mockito.Mockito.Mock<RMStateStore>());
			metricsPublisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher>();
			context.SetSystemMetricsPublisher(metricsPublisher);
			context.SetRMApplicationHistoryWriter(writer);
			return context;
		}

		private sealed class _RMContextImpl_121 : RMContextImpl
		{
			public _RMContextImpl_121(ConcurrentMap<ApplicationId, RMApp> map, Dispatcher baseArg1
				, ContainerAllocationExpirer baseArg2, AMLivelinessMonitor baseArg3, AMLivelinessMonitor
				 baseArg4, DelegationTokenRenewer baseArg5, AMRMTokenSecretManager baseArg6, RMContainerTokenSecretManager
				 baseArg7, NMTokenSecretManagerInRM baseArg8, ClientToAMTokenSecretManagerInRM baseArg9
				)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					, baseArg9)
			{
				this.map = map;
			}

			public override ConcurrentMap<ApplicationId, RMApp> GetRMApps()
			{
				return map;
			}

			private readonly ConcurrentMap<ApplicationId, RMApp> map;
		}

		public class TestAppManagerDispatcher : EventHandler<RMAppManagerEvent>
		{
			public TestAppManagerDispatcher(TestAppManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public virtual void Handle(RMAppManagerEvent @event)
			{
			}

			private readonly TestAppManager _enclosing;
			// do nothing
		}

		public class TestDispatcher : EventHandler<RMAppEvent>
		{
			public TestDispatcher(TestAppManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public virtual void Handle(RMAppEvent @event)
			{
				//RMApp rmApp = this.rmContext.getRMApps().get(appID);
				this._enclosing.SetAppEventType(@event.GetType());
				System.Console.Out.WriteLine("in handle routine " + this._enclosing.GetAppEventType
					().ToString());
			}

			private readonly TestAppManager _enclosing;
		}

		public class TestRMAppManager : RMAppManager
		{
			public TestRMAppManager(TestAppManager _enclosing, RMContext context, Configuration
				 conf)
				: base(context, null, null, new ApplicationACLsManager(conf), conf)
			{
				this._enclosing = _enclosing;
			}

			public TestRMAppManager(TestAppManager _enclosing, RMContext context, ClientToAMTokenSecretManagerInRM
				 clientToAMSecretManager, YarnScheduler scheduler, ApplicationMasterService masterService
				, ApplicationACLsManager applicationACLsManager, Configuration conf)
				: base(context, scheduler, masterService, applicationACLsManager, conf)
			{
				this._enclosing = _enclosing;
			}

			// Extend and make the functions we want to test public
			protected internal override void CheckAppNumCompletedLimit()
			{
				base.CheckAppNumCompletedLimit();
			}

			protected internal override void FinishApplication(ApplicationId appId)
			{
				base.FinishApplication(appId);
			}

			protected internal override int GetCompletedAppsListSize()
			{
				return base.GetCompletedAppsListSize();
			}

			public virtual int GetCompletedAppsInStateStore()
			{
				return this.completedAppsInStateStore;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public virtual void SubmitApplication(ApplicationSubmissionContext submissionContext
				, string user)
			{
				base.SubmitApplication(submissionContext, Runtime.CurrentTimeMillis(), user);
			}

			private readonly TestAppManager _enclosing;
		}

		protected internal virtual void AddToCompletedApps(TestAppManager.TestRMAppManager
			 appMonitor, RMContext rmContext)
		{
			foreach (RMApp app in rmContext.GetRMApps().Values)
			{
				if (app.GetState() == RMAppState.Finished || app.GetState() == RMAppState.Killed 
					|| app.GetState() == RMAppState.Failed)
				{
					appMonitor.FinishApplication(app.GetApplicationId());
				}
			}
		}

		private RMContext rmContext;

		private SystemMetricsPublisher metricsPublisher;

		private TestAppManager.TestRMAppManager appMonitor;

		private ApplicationSubmissionContext asContext;

		private ApplicationId appId;

		[SetUp]
		public virtual void SetUp()
		{
			long now = Runtime.CurrentTimeMillis();
			rmContext = MockRMContext(1, now - 10);
			ResourceScheduler scheduler = MockResourceScheduler();
			Configuration conf = new Configuration();
			ApplicationMasterService masterService = new ApplicationMasterService(rmContext, 
				scheduler);
			appMonitor = new TestAppManager.TestRMAppManager(this, rmContext, new ClientToAMTokenSecretManagerInRM
				(), scheduler, masterService, new ApplicationACLsManager(conf), conf);
			appId = MockApps.NewAppID(1);
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			asContext = recordFactory.NewRecordInstance<ApplicationSubmissionContext>();
			asContext.SetApplicationId(appId);
			asContext.SetAMContainerSpec(MockContainerLaunchContext(recordFactory));
			asContext.SetResource(MockResource());
			SetupDispatcher(rmContext, conf);
		}

		[TearDown]
		public virtual void TearDown()
		{
			SetAppEventType(RMAppEventType.Kill);
			((Org.Apache.Hadoop.Service.Service)rmContext.GetDispatcher()).Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMAppRetireNone()
		{
			long now = Runtime.CurrentTimeMillis();
			// Create such that none of the applications will retire since
			// haven't hit max #
			RMContext rmContext = MockRMContext(10, now - 10);
			Configuration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmMaxCompletedApplications, 10);
			TestAppManager.TestRMAppManager appMonitor = new TestAppManager.TestRMAppManager(
				this, rmContext, conf);
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect before checkAppTimeLimit"
				, 10, rmContext.GetRMApps().Count);
			// add them to completed apps list
			AddToCompletedApps(appMonitor, rmContext);
			// shouldn't  have to many apps
			appMonitor.CheckAppNumCompletedLimit();
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect after # completed check"
				, 10, rmContext.GetRMApps().Count);
			NUnit.Framework.Assert.AreEqual("Number of completed apps incorrect after check", 
				10, appMonitor.GetCompletedAppsListSize());
			Org.Mockito.Mockito.Verify(rmContext.GetStateStore(), Org.Mockito.Mockito.Never()
				).RemoveApplication(Matchers.IsA<RMApp>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMAppRetireSome()
		{
			long now = Runtime.CurrentTimeMillis();
			RMContext rmContext = MockRMContext(10, now - 20000);
			Configuration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmStateStoreMaxCompletedApplications, 3);
			conf.SetInt(YarnConfiguration.RmMaxCompletedApplications, 3);
			TestAppManager.TestRMAppManager appMonitor = new TestAppManager.TestRMAppManager(
				this, rmContext, conf);
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect before", 10, rmContext.
				GetRMApps().Count);
			// add them to completed apps list
			AddToCompletedApps(appMonitor, rmContext);
			// shouldn't  have to many apps
			appMonitor.CheckAppNumCompletedLimit();
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect after # completed check"
				, 3, rmContext.GetRMApps().Count);
			NUnit.Framework.Assert.AreEqual("Number of completed apps incorrect after check", 
				3, appMonitor.GetCompletedAppsListSize());
			Org.Mockito.Mockito.Verify(rmContext.GetStateStore(), Org.Mockito.Mockito.Times(7
				)).RemoveApplication(Matchers.IsA<RMApp>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMAppRetireSomeDifferentStates()
		{
			long now = Runtime.CurrentTimeMillis();
			// these parameters don't matter, override applications below
			RMContext rmContext = MockRMContext(10, now - 20000);
			Configuration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmStateStoreMaxCompletedApplications, 2);
			conf.SetInt(YarnConfiguration.RmMaxCompletedApplications, 2);
			TestAppManager.TestRMAppManager appMonitor = new TestAppManager.TestRMAppManager(
				this, rmContext, conf);
			// clear out applications map
			rmContext.GetRMApps().Clear();
			NUnit.Framework.Assert.AreEqual("map isn't empty", 0, rmContext.GetRMApps().Count
				);
			// 6 applications are in final state, 4 are not in final state.
			// / set with various finished states
			RMApp app = new MockRMApp(0, now - 20000, RMAppState.Killed);
			rmContext.GetRMApps()[app.GetApplicationId()] = app;
			app = new MockRMApp(1, now - 200000, RMAppState.Failed);
			rmContext.GetRMApps()[app.GetApplicationId()] = app;
			app = new MockRMApp(2, now - 30000, RMAppState.Finished);
			rmContext.GetRMApps()[app.GetApplicationId()] = app;
			app = new MockRMApp(3, now - 20000, RMAppState.Running);
			rmContext.GetRMApps()[app.GetApplicationId()] = app;
			app = new MockRMApp(4, now - 20000, RMAppState.New);
			rmContext.GetRMApps()[app.GetApplicationId()] = app;
			// make sure it doesn't expire these since still running
			app = new MockRMApp(5, now - 10001, RMAppState.Killed);
			rmContext.GetRMApps()[app.GetApplicationId()] = app;
			app = new MockRMApp(6, now - 30000, RMAppState.Accepted);
			rmContext.GetRMApps()[app.GetApplicationId()] = app;
			app = new MockRMApp(7, now - 20000, RMAppState.Submitted);
			rmContext.GetRMApps()[app.GetApplicationId()] = app;
			app = new MockRMApp(8, now - 10001, RMAppState.Failed);
			rmContext.GetRMApps()[app.GetApplicationId()] = app;
			app = new MockRMApp(9, now - 20000, RMAppState.Failed);
			rmContext.GetRMApps()[app.GetApplicationId()] = app;
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect before", 10, rmContext.
				GetRMApps().Count);
			// add them to completed apps list
			AddToCompletedApps(appMonitor, rmContext);
			// shouldn't  have to many apps
			appMonitor.CheckAppNumCompletedLimit();
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect after # completed check"
				, 6, rmContext.GetRMApps().Count);
			NUnit.Framework.Assert.AreEqual("Number of completed apps incorrect after check", 
				2, appMonitor.GetCompletedAppsListSize());
			// 6 applications in final state, 4 of them are removed
			Org.Mockito.Mockito.Verify(rmContext.GetStateStore(), Org.Mockito.Mockito.Times(4
				)).RemoveApplication(Matchers.IsA<RMApp>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMAppRetireNullApp()
		{
			long now = Runtime.CurrentTimeMillis();
			RMContext rmContext = MockRMContext(10, now - 20000);
			TestAppManager.TestRMAppManager appMonitor = new TestAppManager.TestRMAppManager(
				this, rmContext, new Configuration());
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect before", 10, rmContext.
				GetRMApps().Count);
			appMonitor.FinishApplication(null);
			NUnit.Framework.Assert.AreEqual("Number of completed apps incorrect after check", 
				0, appMonitor.GetCompletedAppsListSize());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMAppRetireZeroSetting()
		{
			long now = Runtime.CurrentTimeMillis();
			RMContext rmContext = MockRMContext(10, now - 20000);
			Configuration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmStateStoreMaxCompletedApplications, 0);
			conf.SetInt(YarnConfiguration.RmMaxCompletedApplications, 0);
			TestAppManager.TestRMAppManager appMonitor = new TestAppManager.TestRMAppManager(
				this, rmContext, conf);
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect before", 10, rmContext.
				GetRMApps().Count);
			AddToCompletedApps(appMonitor, rmContext);
			NUnit.Framework.Assert.AreEqual("Number of completed apps incorrect", 10, appMonitor
				.GetCompletedAppsListSize());
			appMonitor.CheckAppNumCompletedLimit();
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect after # completed check"
				, 0, rmContext.GetRMApps().Count);
			NUnit.Framework.Assert.AreEqual("Number of completed apps incorrect after check", 
				0, appMonitor.GetCompletedAppsListSize());
			Org.Mockito.Mockito.Verify(rmContext.GetStateStore(), Org.Mockito.Mockito.Times(10
				)).RemoveApplication(Matchers.IsA<RMApp>());
		}

		[NUnit.Framework.Test]
		public virtual void TestStateStoreAppLimitLessThanMemoryAppLimit()
		{
			long now = Runtime.CurrentTimeMillis();
			RMContext rmContext = MockRMContext(10, now - 20000);
			Configuration conf = new YarnConfiguration();
			int maxAppsInMemory = 8;
			int maxAppsInStateStore = 4;
			conf.SetInt(YarnConfiguration.RmMaxCompletedApplications, maxAppsInMemory);
			conf.SetInt(YarnConfiguration.RmStateStoreMaxCompletedApplications, maxAppsInStateStore
				);
			TestAppManager.TestRMAppManager appMonitor = new TestAppManager.TestRMAppManager(
				this, rmContext, conf);
			AddToCompletedApps(appMonitor, rmContext);
			NUnit.Framework.Assert.AreEqual("Number of completed apps incorrect", 10, appMonitor
				.GetCompletedAppsListSize());
			appMonitor.CheckAppNumCompletedLimit();
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect after # completed check"
				, maxAppsInMemory, rmContext.GetRMApps().Count);
			NUnit.Framework.Assert.AreEqual("Number of completed apps incorrect after check", 
				maxAppsInMemory, appMonitor.GetCompletedAppsListSize());
			int numRemoveAppsFromStateStore = 10 - maxAppsInStateStore;
			Org.Mockito.Mockito.Verify(rmContext.GetStateStore(), Org.Mockito.Mockito.Times(numRemoveAppsFromStateStore
				)).RemoveApplication(Matchers.IsA<RMApp>());
			NUnit.Framework.Assert.AreEqual(maxAppsInStateStore, appMonitor.GetCompletedAppsInStateStore
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestStateStoreAppLimitLargerThanMemoryAppLimit()
		{
			long now = Runtime.CurrentTimeMillis();
			RMContext rmContext = MockRMContext(10, now - 20000);
			Configuration conf = new YarnConfiguration();
			int maxAppsInMemory = 8;
			conf.SetInt(YarnConfiguration.RmMaxCompletedApplications, maxAppsInMemory);
			// larger than maxCompletedAppsInMemory, reset to RM_MAX_COMPLETED_APPLICATIONS.
			conf.SetInt(YarnConfiguration.RmStateStoreMaxCompletedApplications, 1000);
			TestAppManager.TestRMAppManager appMonitor = new TestAppManager.TestRMAppManager(
				this, rmContext, conf);
			AddToCompletedApps(appMonitor, rmContext);
			NUnit.Framework.Assert.AreEqual("Number of completed apps incorrect", 10, appMonitor
				.GetCompletedAppsListSize());
			appMonitor.CheckAppNumCompletedLimit();
			int numRemoveApps = 10 - maxAppsInMemory;
			NUnit.Framework.Assert.AreEqual("Number of apps incorrect after # completed check"
				, maxAppsInMemory, rmContext.GetRMApps().Count);
			NUnit.Framework.Assert.AreEqual("Number of completed apps incorrect after check", 
				maxAppsInMemory, appMonitor.GetCompletedAppsListSize());
			Org.Mockito.Mockito.Verify(rmContext.GetStateStore(), Org.Mockito.Mockito.Times(numRemoveApps
				)).RemoveApplication(Matchers.IsA<RMApp>());
			NUnit.Framework.Assert.AreEqual(maxAppsInMemory, appMonitor.GetCompletedAppsInStateStore
				());
		}

		protected internal virtual void SetupDispatcher(RMContext rmContext, Configuration
			 conf)
		{
			TestAppManager.TestDispatcher testDispatcher = new TestAppManager.TestDispatcher(
				this);
			TestAppManager.TestAppManagerDispatcher testAppManagerDispatcher = new TestAppManager.TestAppManagerDispatcher
				(this);
			rmContext.GetDispatcher().Register(typeof(RMAppEventType), testDispatcher);
			rmContext.GetDispatcher().Register(typeof(RMAppManagerEventType), testAppManagerDispatcher
				);
			((Org.Apache.Hadoop.Service.Service)rmContext.GetDispatcher()).Init(conf);
			((Org.Apache.Hadoop.Service.Service)rmContext.GetDispatcher()).Start();
			NUnit.Framework.Assert.AreEqual("app event type is wrong before", RMAppEventType.
				Kill, appEventType);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMAppSubmit()
		{
			appMonitor.SubmitApplication(asContext, "test");
			RMApp app = rmContext.GetRMApps()[appId];
			NUnit.Framework.Assert.IsNotNull("app is null", app);
			NUnit.Framework.Assert.AreEqual("app id doesn't match", appId, app.GetApplicationId
				());
			NUnit.Framework.Assert.AreEqual("app state doesn't match", RMAppState.New, app.GetState
				());
			Org.Mockito.Mockito.Verify(metricsPublisher).AppACLsUpdated(Matchers.Any<RMApp>()
				, Matchers.Any<string>(), Matchers.AnyLong());
			// wait for event to be processed
			int timeoutSecs = 0;
			while ((GetAppEventType() == RMAppEventType.Kill) && timeoutSecs++ < 20)
			{
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.AreEqual("app event type sent is wrong", RMAppEventType.Start
				, GetAppEventType());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMAppSubmitMaxAppAttempts()
		{
			int[] globalMaxAppAttempts = new int[] { 10, 1 };
			int[][] individualMaxAppAttempts = new int[][] { new int[] { 9, 10, 11, 0 }, new 
				int[] { 1, 10, 0, -1 } };
			int[][] expectedNums = new int[][] { new int[] { 9, 10, 10, 10 }, new int[] { 1, 
				1, 1, 1 } };
			for (int i = 0; i < globalMaxAppAttempts.Length; ++i)
			{
				for (int j = 0; j < individualMaxAppAttempts.Length; ++j)
				{
					ResourceScheduler scheduler = MockResourceScheduler();
					Configuration conf = new Configuration();
					conf.SetInt(YarnConfiguration.RmAmMaxAttempts, globalMaxAppAttempts[i]);
					ApplicationMasterService masterService = new ApplicationMasterService(rmContext, 
						scheduler);
					TestAppManager.TestRMAppManager appMonitor = new TestAppManager.TestRMAppManager(
						this, rmContext, new ClientToAMTokenSecretManagerInRM(), scheduler, masterService
						, new ApplicationACLsManager(conf), conf);
					ApplicationId appID = MockApps.NewAppID(i * 4 + j + 1);
					asContext.SetApplicationId(appID);
					if (individualMaxAppAttempts[i][j] != 0)
					{
						asContext.SetMaxAppAttempts(individualMaxAppAttempts[i][j]);
					}
					appMonitor.SubmitApplication(asContext, "test");
					RMApp app = rmContext.GetRMApps()[appID];
					NUnit.Framework.Assert.AreEqual("max application attempts doesn't match", expectedNums
						[i][j], app.GetMaxAppAttempts());
					// wait for event to be processed
					int timeoutSecs = 0;
					while ((GetAppEventType() == RMAppEventType.Kill) && timeoutSecs++ < 20)
					{
						Sharpen.Thread.Sleep(1000);
					}
					SetAppEventType(RMAppEventType.Kill);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMAppSubmitDuplicateApplicationId()
		{
			ApplicationId appId = MockApps.NewAppID(0);
			asContext.SetApplicationId(appId);
			RMApp appOrig = rmContext.GetRMApps()[appId];
			NUnit.Framework.Assert.IsTrue("app name matches but shouldn't", "testApp1" != appOrig
				.GetName());
			// our testApp1 should be rejected and original app with same id should be left in place
			try
			{
				appMonitor.SubmitApplication(asContext, "test");
				NUnit.Framework.Assert.Fail("Exception is expected when applicationId is duplicate."
					);
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue("The thrown exception is not the expectd one.", e.Message
					.Contains("Cannot add a duplicate!"));
			}
			// make sure original app didn't get removed
			RMApp app = rmContext.GetRMApps()[appId];
			NUnit.Framework.Assert.IsNotNull("app is null", app);
			NUnit.Framework.Assert.AreEqual("app id doesn't match", appId, app.GetApplicationId
				());
			NUnit.Framework.Assert.AreEqual("app state doesn't match", RMAppState.Finished, app
				.GetState());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMAppSubmitInvalidResourceRequest()
		{
			asContext.SetResource(Resources.CreateResource(YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb
				 + 1));
			// submit an app
			try
			{
				appMonitor.SubmitApplication(asContext, "test");
				NUnit.Framework.Assert.Fail("Application submission should fail because resource"
					 + " request is invalid.");
			}
			catch (YarnException e)
			{
				// Exception is expected
				// TODO Change this to assert the expected exception type - post YARN-142
				// sub-task related to specialized exceptions.
				NUnit.Framework.Assert.IsTrue("The thrown exception is not" + " InvalidResourceRequestException"
					, e.Message.Contains("Invalid resource request"));
			}
		}

		public virtual void TestEscapeApplicationSummary()
		{
			RMApp app = Org.Mockito.Mockito.Mock<RMAppImpl>();
			Org.Mockito.Mockito.When(app.GetApplicationId()).ThenReturn(ApplicationId.NewInstance
				(100L, 1));
			Org.Mockito.Mockito.When(app.GetName()).ThenReturn("Multiline\n\n\r\rAppName");
			Org.Mockito.Mockito.When(app.GetUser()).ThenReturn("Multiline\n\n\r\rUserName");
			Org.Mockito.Mockito.When(app.GetQueue()).ThenReturn("Multiline\n\n\r\rQueueName");
			Org.Mockito.Mockito.When(app.GetState()).ThenReturn(RMAppState.Running);
			Org.Mockito.Mockito.When(app.GetApplicationType()).ThenReturn("MAPREDUCE");
			RMAppMetrics metrics = new RMAppMetrics(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1234, 56), 10, 1, 16384, 64);
			Org.Mockito.Mockito.When(app.GetRMAppMetrics()).ThenReturn(metrics);
			RMAppManager.ApplicationSummary.SummaryBuilder summary = RMAppManager.ApplicationSummary
				.CreateAppSummary(app);
			string msg = summary.ToString();
			Log.Info("summary: " + msg);
			NUnit.Framework.Assert.IsFalse(msg.Contains("\n"));
			NUnit.Framework.Assert.IsFalse(msg.Contains("\r"));
			string escaped = "\\n\\n\\r\\r";
			NUnit.Framework.Assert.IsTrue(msg.Contains("Multiline" + escaped + "AppName"));
			NUnit.Framework.Assert.IsTrue(msg.Contains("Multiline" + escaped + "UserName"));
			NUnit.Framework.Assert.IsTrue(msg.Contains("Multiline" + escaped + "QueueName"));
			NUnit.Framework.Assert.IsTrue(msg.Contains("memorySeconds=16384"));
			NUnit.Framework.Assert.IsTrue(msg.Contains("vcoreSeconds=64"));
			NUnit.Framework.Assert.IsTrue(msg.Contains("preemptedAMContainers=1"));
			NUnit.Framework.Assert.IsTrue(msg.Contains("preemptedNonAMContainers=10"));
			NUnit.Framework.Assert.IsTrue(msg.Contains("preemptedResources=<memory:1234\\, vCores:56>"
				));
			NUnit.Framework.Assert.IsTrue(msg.Contains("applicationType=MAPREDUCE"));
		}

		private static ResourceScheduler MockResourceScheduler()
		{
			ResourceScheduler scheduler = Org.Mockito.Mockito.Mock<ResourceScheduler>();
			Org.Mockito.Mockito.When(scheduler.GetMinimumResourceCapability()).ThenReturn(Resources
				.CreateResource(YarnConfiguration.DefaultRmSchedulerMinimumAllocationMb));
			Org.Mockito.Mockito.When(scheduler.GetMaximumResourceCapability()).ThenReturn(Resources
				.CreateResource(YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb));
			ResourceCalculator rc = new DefaultResourceCalculator();
			Org.Mockito.Mockito.When(scheduler.GetResourceCalculator()).ThenReturn(rc);
			return scheduler;
		}

		private static ContainerLaunchContext MockContainerLaunchContext(RecordFactory recordFactory
			)
		{
			ContainerLaunchContext amContainer = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			amContainer.SetApplicationACLs(new Dictionary<ApplicationAccessType, string>());
			return amContainer;
		}

		private static Org.Apache.Hadoop.Yarn.Api.Records.Resource MockResource()
		{
			return Resources.CreateResource(YarnConfiguration.DefaultRmSchedulerMinimumAllocationMb
				);
		}
	}
}
