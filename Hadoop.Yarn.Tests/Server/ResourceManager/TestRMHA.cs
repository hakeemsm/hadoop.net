using System;
using System.IO;
using System.Net;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Api.Client.Config;
using Javax.WS.RS.Core;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Codehaus.Jettison.Json;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestRMHA
	{
		private Log Log = LogFactory.GetLog(typeof(TestRMHA));

		private Configuration configuration;

		private MockRM rm = null;

		private RMApp app = null;

		private RMAppAttempt attempt = null;

		private const string StateErr = "ResourceManager is in wrong HA state";

		private const string Rm1Address = "1.1.1.1:1";

		private const string Rm1NodeId = "rm1";

		private const string Rm2Address = "0.0.0.0:0";

		private const string Rm2NodeId = "rm2";

		private const string Rm3Address = "2.2.2.2:2";

		private const string Rm3NodeId = "rm3";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			configuration = new Configuration();
			UserGroupInformation.SetConfiguration(configuration);
			configuration.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			configuration.Set(YarnConfiguration.RmHaIds, Rm1NodeId + "," + Rm2NodeId);
			foreach (string confKey in YarnConfiguration.GetServiceAddressConfKeys(configuration
				))
			{
				configuration.Set(HAUtil.AddSuffix(confKey, Rm1NodeId), Rm1Address);
				configuration.Set(HAUtil.AddSuffix(confKey, Rm2NodeId), Rm2Address);
				configuration.Set(HAUtil.AddSuffix(confKey, Rm3NodeId), Rm3Address);
			}
			// Enable webapp to test web-services also
			configuration.SetBoolean(MockRM.EnableWebapp, true);
			configuration.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			ClusterMetrics.Destroy();
			QueueMetrics.ClearQueueMetrics();
			DefaultMetricsSystem.Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckMonitorHealth()
		{
			try
			{
				rm.adminService.MonitorHealth();
			}
			catch (HealthCheckFailedException)
			{
				NUnit.Framework.Assert.Fail("The RM is in bad health: it is Active, but the active services "
					 + "are not running");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckStandbyRMFunctionality()
		{
			NUnit.Framework.Assert.AreEqual(StateErr, HAServiceProtocol.HAServiceState.Standby
				, rm.adminService.GetServiceStatus().GetState());
			NUnit.Framework.Assert.IsFalse("Active RM services are started", rm.AreActiveServicesRunning
				());
			NUnit.Framework.Assert.IsTrue("RM is not ready to become active", rm.adminService
				.GetServiceStatus().IsReadyToBecomeActive());
		}

		/// <exception cref="System.Exception"/>
		private void CheckActiveRMFunctionality()
		{
			NUnit.Framework.Assert.AreEqual(StateErr, HAServiceProtocol.HAServiceState.Active
				, rm.adminService.GetServiceStatus().GetState());
			NUnit.Framework.Assert.IsTrue("Active RM services aren't started", rm.AreActiveServicesRunning
				());
			NUnit.Framework.Assert.IsTrue("RM is not ready to become active", rm.adminService
				.GetServiceStatus().IsReadyToBecomeActive());
			try
			{
				rm.GetNewAppId();
				rm.RegisterNode("127.0.0.1:1", 2048);
				app = rm.SubmitApp(1024);
				attempt = app.GetCurrentAppAttempt();
				rm.WaitForState(attempt.GetAppAttemptId(), RMAppAttemptState.Scheduled);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Unable to perform Active RM functions");
				Log.Error("ActiveRM check failed", e);
			}
			CheckActiveRMWebServices();
		}

		// Do some sanity testing of the web-services after fail-over.
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		private void CheckActiveRMWebServices()
		{
			// Validate web-service
			Com.Sun.Jersey.Api.Client.Client webServiceClient = Com.Sun.Jersey.Api.Client.Client
				.Create(new DefaultClientConfig());
			IPEndPoint rmWebappAddr = NetUtils.GetConnectAddress(rm.GetWebapp().GetListenerAddress
				());
			string webappURL = "http://" + rmWebappAddr.GetHostName() + ":" + rmWebappAddr.Port;
			WebResource webResource = webServiceClient.Resource(webappURL);
			string path = app.GetApplicationId().ToString();
			ClientResponse response = webResource.Path("ws").Path("v1").Path("cluster").Path(
				"apps").Path(path).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject appJson = json.GetJSONObject("app");
			NUnit.Framework.Assert.AreEqual("ACCEPTED", appJson.GetString("state"));
		}

		// Other stuff is verified in the regular web-services related tests
		/// <summary>Test to verify the following RM HA transitions to the following states.</summary>
		/// <remarks>
		/// Test to verify the following RM HA transitions to the following states.
		/// 1. Standby: Should be a no-op
		/// 2. Active: Active services should start
		/// 3. Active: Should be a no-op.
		/// While active, submit a couple of jobs
		/// 4. Standby: Active services should stop
		/// 5. Active: Active services should start
		/// 6. Stop the RM: All services should stop and RM should not be ready to
		/// become Active
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestFailoverAndTransitions()
		{
			configuration.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			Configuration conf = new YarnConfiguration(configuration);
			rm = new MockRM(conf);
			rm.Init(conf);
			HAServiceProtocol.StateChangeRequestInfo requestInfo = new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser);
			NUnit.Framework.Assert.AreEqual(StateErr, HAServiceProtocol.HAServiceState.Initializing
				, rm.adminService.GetServiceStatus().GetState());
			NUnit.Framework.Assert.IsFalse("RM is ready to become active before being started"
				, rm.adminService.GetServiceStatus().IsReadyToBecomeActive());
			CheckMonitorHealth();
			rm.Start();
			CheckMonitorHealth();
			CheckStandbyRMFunctionality();
			VerifyClusterMetrics(0, 0, 0, 0, 0, 0);
			// 1. Transition to Standby - must be a no-op
			rm.adminService.TransitionToStandby(requestInfo);
			CheckMonitorHealth();
			CheckStandbyRMFunctionality();
			VerifyClusterMetrics(0, 0, 0, 0, 0, 0);
			// 2. Transition to active
			rm.adminService.TransitionToActive(requestInfo);
			CheckMonitorHealth();
			CheckActiveRMFunctionality();
			VerifyClusterMetrics(1, 1, 1, 1, 2048, 1);
			// 3. Transition to active - no-op
			rm.adminService.TransitionToActive(requestInfo);
			CheckMonitorHealth();
			CheckActiveRMFunctionality();
			VerifyClusterMetrics(1, 2, 2, 2, 2048, 2);
			// 4. Transition to standby
			rm.adminService.TransitionToStandby(requestInfo);
			CheckMonitorHealth();
			CheckStandbyRMFunctionality();
			VerifyClusterMetrics(0, 0, 0, 0, 0, 0);
			// 5. Transition to active to check Active->Standby->Active works
			rm.adminService.TransitionToActive(requestInfo);
			CheckMonitorHealth();
			CheckActiveRMFunctionality();
			VerifyClusterMetrics(1, 1, 1, 1, 2048, 1);
			// 6. Stop the RM. All services should stop and RM should not be ready to
			// become active
			rm.Stop();
			NUnit.Framework.Assert.AreEqual(StateErr, HAServiceProtocol.HAServiceState.Stopping
				, rm.adminService.GetServiceStatus().GetState());
			NUnit.Framework.Assert.IsFalse("RM is ready to become active even after it is stopped"
				, rm.adminService.GetServiceStatus().IsReadyToBecomeActive());
			NUnit.Framework.Assert.IsFalse("Active RM services are started", rm.AreActiveServicesRunning
				());
			CheckMonitorHealth();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTransitionsWhenAutomaticFailoverEnabled()
		{
			string ErrUnforcedRequest = "User request succeeded even when " + "automatic failover is enabled";
			Configuration conf = new YarnConfiguration(configuration);
			rm = new MockRM(conf);
			rm.Init(conf);
			rm.Start();
			HAServiceProtocol.StateChangeRequestInfo requestInfo = new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser);
			// Transition to standby
			try
			{
				rm.adminService.TransitionToStandby(requestInfo);
				NUnit.Framework.Assert.Fail(ErrUnforcedRequest);
			}
			catch (AccessControlException)
			{
			}
			// expected
			CheckMonitorHealth();
			CheckStandbyRMFunctionality();
			// Transition to active
			try
			{
				rm.adminService.TransitionToActive(requestInfo);
				NUnit.Framework.Assert.Fail(ErrUnforcedRequest);
			}
			catch (AccessControlException)
			{
			}
			// expected
			CheckMonitorHealth();
			CheckStandbyRMFunctionality();
			string ErrForcedRequest = "Forced request by user should work " + "even if automatic failover is enabled";
			requestInfo = new HAServiceProtocol.StateChangeRequestInfo(HAServiceProtocol.RequestSource
				.RequestByUserForced);
			// Transition to standby
			try
			{
				rm.adminService.TransitionToStandby(requestInfo);
			}
			catch (AccessControlException)
			{
				NUnit.Framework.Assert.Fail(ErrForcedRequest);
			}
			CheckMonitorHealth();
			CheckStandbyRMFunctionality();
			// Transition to active
			try
			{
				rm.adminService.TransitionToActive(requestInfo);
			}
			catch (AccessControlException)
			{
				NUnit.Framework.Assert.Fail(ErrForcedRequest);
			}
			CheckMonitorHealth();
			CheckActiveRMFunctionality();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRMDispatcherForHA()
		{
			string errorMessageForEventHandler = "Expect to get the same number of handlers";
			string errorMessageForService = "Expect to get the same number of services";
			configuration.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			Configuration conf = new YarnConfiguration(configuration);
			rm = new _MockRM_313(conf);
			rm.Init(conf);
			int expectedEventHandlerCount = ((TestRMHA.MyCountingDispatcher)rm.GetRMContext()
				.GetDispatcher()).GetEventHandlerCount();
			int expectedServiceCount = rm.GetServices().Count;
			NUnit.Framework.Assert.IsTrue(expectedEventHandlerCount != 0);
			HAServiceProtocol.StateChangeRequestInfo requestInfo = new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser);
			NUnit.Framework.Assert.AreEqual(StateErr, HAServiceProtocol.HAServiceState.Initializing
				, rm.adminService.GetServiceStatus().GetState());
			NUnit.Framework.Assert.IsFalse("RM is ready to become active before being started"
				, rm.adminService.GetServiceStatus().IsReadyToBecomeActive());
			rm.Start();
			//call transitions to standby and active a couple of times
			rm.adminService.TransitionToStandby(requestInfo);
			rm.adminService.TransitionToActive(requestInfo);
			rm.adminService.TransitionToStandby(requestInfo);
			rm.adminService.TransitionToActive(requestInfo);
			rm.adminService.TransitionToStandby(requestInfo);
			TestRMHA.MyCountingDispatcher dispatcher = (TestRMHA.MyCountingDispatcher)rm.GetRMContext
				().GetDispatcher();
			NUnit.Framework.Assert.IsTrue(!dispatcher.IsStopped());
			rm.adminService.TransitionToActive(requestInfo);
			NUnit.Framework.Assert.AreEqual(errorMessageForEventHandler, expectedEventHandlerCount
				, ((TestRMHA.MyCountingDispatcher)rm.GetRMContext().GetDispatcher()).GetEventHandlerCount
				());
			NUnit.Framework.Assert.AreEqual(errorMessageForService, expectedServiceCount, rm.
				GetServices().Count);
			// Keep the dispatcher reference before transitioning to standby
			dispatcher = (TestRMHA.MyCountingDispatcher)rm.GetRMContext().GetDispatcher();
			rm.adminService.TransitionToStandby(requestInfo);
			NUnit.Framework.Assert.AreEqual(errorMessageForEventHandler, expectedEventHandlerCount
				, ((TestRMHA.MyCountingDispatcher)rm.GetRMContext().GetDispatcher()).GetEventHandlerCount
				());
			NUnit.Framework.Assert.AreEqual(errorMessageForService, expectedServiceCount, rm.
				GetServices().Count);
			NUnit.Framework.Assert.IsTrue(dispatcher.IsStopped());
			rm.Stop();
		}

		private sealed class _MockRM_313 : MockRM
		{
			public _MockRM_313(Configuration baseArg1)
				: base(baseArg1)
			{
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return new TestRMHA.MyCountingDispatcher(this);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestHAIDLookup()
		{
			//test implicitly lookup HA-ID
			Configuration conf = new YarnConfiguration(configuration);
			rm = new MockRM(conf);
			rm.Init(conf);
			NUnit.Framework.Assert.AreEqual(conf.Get(YarnConfiguration.RmHaId), Rm2NodeId);
			//test explicitly lookup HA-ID
			configuration.Set(YarnConfiguration.RmHaId, Rm1NodeId);
			conf = new YarnConfiguration(configuration);
			rm = new MockRM(conf);
			rm.Init(conf);
			NUnit.Framework.Assert.AreEqual(conf.Get(YarnConfiguration.RmHaId), Rm1NodeId);
			//test if RM_HA_ID can not be found
			configuration.Set(YarnConfiguration.RmHaIds, Rm1NodeId + "," + Rm3NodeId);
			configuration.Unset(YarnConfiguration.RmHaId);
			conf = new YarnConfiguration(configuration);
			try
			{
				rm = new MockRM(conf);
				rm.Init(conf);
				NUnit.Framework.Assert.Fail("Should get an exception here.");
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.IsTrue(ex.Message.Contains("Invalid configuration! Can not find valid RM_HA_ID."
					));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHAWithRMHostName()
		{
			InnerTestHAWithRMHostName(false);
			configuration.Clear();
			SetUp();
			InnerTestHAWithRMHostName(true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFailoverWhenTransitionToActiveThrowException()
		{
			configuration.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			Configuration conf = new YarnConfiguration(configuration);
			MemoryRMStateStore memStore = new _MemoryRMStateStore_414();
			// first time throw exception
			// start RM
			memStore.Init(conf);
			rm = new MockRM(conf, memStore);
			rm.Init(conf);
			HAServiceProtocol.StateChangeRequestInfo requestInfo = new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser);
			NUnit.Framework.Assert.AreEqual(StateErr, HAServiceProtocol.HAServiceState.Initializing
				, rm.adminService.GetServiceStatus().GetState());
			NUnit.Framework.Assert.IsFalse("RM is ready to become active before being started"
				, rm.adminService.GetServiceStatus().IsReadyToBecomeActive());
			CheckMonitorHealth();
			rm.Start();
			CheckMonitorHealth();
			CheckStandbyRMFunctionality();
			// 2. Try Transition to active, throw exception
			try
			{
				rm.adminService.TransitionToActive(requestInfo);
				NUnit.Framework.Assert.Fail("Transitioned to Active should throw exception.");
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("Error when transitioning to Active mode".Contains(
					e.Message));
			}
			// 3. Transition to active, success
			rm.adminService.TransitionToActive(requestInfo);
			CheckMonitorHealth();
			CheckActiveRMFunctionality();
		}

		private sealed class _MemoryRMStateStore_414 : MemoryRMStateStore
		{
			public _MemoryRMStateStore_414()
			{
				this.count = 0;
			}

			internal int count;

			/// <exception cref="System.Exception"/>
			protected internal override void StartInternal()
			{
				lock (this)
				{
					if (this.count++ == 0)
					{
						throw new Exception("Session Expired");
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTransitionedToStandbyShouldNotHang()
		{
			configuration.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			Configuration conf = new YarnConfiguration(configuration);
			MemoryRMStateStore memStore = new _MemoryRMStateStore_464();
			memStore.Init(conf);
			rm = new _MockRM_472(conf, memStore);
			rm.Init(conf);
			HAServiceProtocol.StateChangeRequestInfo requestInfo = new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser);
			NUnit.Framework.Assert.AreEqual(StateErr, HAServiceProtocol.HAServiceState.Initializing
				, rm.adminService.GetServiceStatus().GetState());
			NUnit.Framework.Assert.IsFalse("RM is ready to become active before being started"
				, rm.adminService.GetServiceStatus().IsReadyToBecomeActive());
			CheckMonitorHealth();
			rm.Start();
			CheckMonitorHealth();
			CheckStandbyRMFunctionality();
			// 2. Transition to Active.
			rm.adminService.TransitionToActive(requestInfo);
			// 3. Try Transition to standby
			Sharpen.Thread t = new Sharpen.Thread(new _Runnable_498(this));
			// TODO Auto-generated catch block
			t.Start();
			rm.GetRMContext().GetStateStore().UpdateApplicationState(null);
			t.Join();
			// wait for thread to finish
			rm.adminService.TransitionToStandby(requestInfo);
			CheckStandbyRMFunctionality();
			rm.Stop();
		}

		private sealed class _MemoryRMStateStore_464 : MemoryRMStateStore
		{
			public _MemoryRMStateStore_464()
			{
			}

			public override void UpdateApplicationState(ApplicationStateData appState)
			{
				lock (this)
				{
					this.NotifyStoreOperationFailed(new StoreFencedException());
				}
			}
		}

		private sealed class _MockRM_472 : MockRM
		{
			public _MockRM_472(Configuration baseArg1, RMStateStore baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			/// <exception cref="System.Exception"/>
			internal override void StopActiveServices()
			{
				Sharpen.Thread.Sleep(10000);
				base.StopActiveServices();
			}
		}

		private sealed class _Runnable_498 : Runnable
		{
			public _Runnable_498(TestRMHA _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				try
				{
					this._enclosing.rm.TransitionToStandby(true);
				}
				catch (IOException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}

			private readonly TestRMHA _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverClearsRMContext()
		{
			configuration.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			configuration.SetBoolean(YarnConfiguration.RecoveryEnabled, true);
			Configuration conf = new YarnConfiguration(configuration);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// 1. start RM
			rm = new MockRM(conf, memStore);
			rm.Init(conf);
			rm.Start();
			HAServiceProtocol.StateChangeRequestInfo requestInfo = new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser);
			CheckMonitorHealth();
			CheckStandbyRMFunctionality();
			// 2. Transition to active
			rm.adminService.TransitionToActive(requestInfo);
			CheckMonitorHealth();
			CheckActiveRMFunctionality();
			VerifyClusterMetrics(1, 1, 1, 1, 2048, 1);
			NUnit.Framework.Assert.AreEqual(1, rm.GetRMContext().GetRMNodes().Count);
			NUnit.Framework.Assert.AreEqual(1, rm.GetRMContext().GetRMApps().Count);
			// 3. Create new RM
			rm = new _MockRM_550(conf, memStore);
			rm.Init(conf);
			rm.Start();
			CheckMonitorHealth();
			CheckStandbyRMFunctionality();
			// 4. Try Transition to active, throw exception
			try
			{
				rm.adminService.TransitionToActive(requestInfo);
				NUnit.Framework.Assert.Fail("Transitioned to Active should throw exception.");
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("Error when transitioning to Active mode".Contains(
					e.Message));
			}
			// 5. Clears the metrics
			VerifyClusterMetrics(0, 0, 0, 0, 0, 0);
			NUnit.Framework.Assert.AreEqual(0, rm.GetRMContext().GetRMNodes().Count);
			NUnit.Framework.Assert.AreEqual(0, rm.GetRMContext().GetRMApps().Count);
		}

		private sealed class _MockRM_550 : MockRM
		{
			public _MockRM_550(Configuration baseArg1, RMStateStore baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			protected internal override ResourceTrackerService CreateResourceTrackerService()
			{
				return new _ResourceTrackerService_556(this.rmContext, this.nodesListManager, this
					.nmLivelinessMonitor, this.rmContext.GetContainerTokenSecretManager(), this.rmContext
					.GetNMTokenSecretManager());
			}

			private sealed class _ResourceTrackerService_556 : ResourceTrackerService
			{
				public _ResourceTrackerService_556(RMContext baseArg1, NodesListManager baseArg2, 
					NMLivelinessMonitor baseArg3, RMContainerTokenSecretManager baseArg4, NMTokenSecretManagerInRM
					 baseArg5)
					: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
				{
				}

				/// <exception cref="System.Exception"/>
				protected override void ServiceStart()
				{
					throw new Exception("ResourceTracker service failed");
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTransitionedToActiveRefreshFail()
		{
			configuration.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			YarnConfiguration conf = new YarnConfiguration(configuration);
			configuration = new CapacitySchedulerConfiguration(conf);
			rm = new _MockRM_588(this, configuration);
			rm.Init(configuration);
			rm.Start();
			HAServiceProtocol.StateChangeRequestInfo requestInfo = new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser);
			configuration.Set("yarn.scheduler.capacity.root.default.capacity", "100");
			rm.adminService.TransitionToStandby(requestInfo);
			NUnit.Framework.Assert.AreEqual(HAServiceProtocol.HAServiceState.Standby, rm.GetRMContext
				().GetHAServiceState());
			configuration.Set("yarn.scheduler.capacity.root.default.capacity", "200");
			try
			{
				rm.adminService.TransitionToActive(requestInfo);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue("Error on refreshAll during transistion to Active".
					Contains(e.Message));
			}
			TestRMHA.FailFastDispatcher dispatcher = ((TestRMHA.FailFastDispatcher)rm.rmContext
				.GetDispatcher());
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual(1, dispatcher.GetEventCount());
			// Making correct conf and check the state
			configuration.Set("yarn.scheduler.capacity.root.default.capacity", "100");
			rm.adminService.TransitionToActive(requestInfo);
			NUnit.Framework.Assert.AreEqual(HAServiceProtocol.HAServiceState.Active, rm.GetRMContext
				().GetHAServiceState());
			rm.adminService.TransitionToStandby(requestInfo);
			NUnit.Framework.Assert.AreEqual(HAServiceProtocol.HAServiceState.Standby, rm.GetRMContext
				().GetHAServiceState());
		}

		private sealed class _MockRM_588 : MockRM
		{
			public _MockRM_588(TestRMHA _enclosing, Configuration baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
			}

			protected internal override AdminService CreateAdminService()
			{
				return new _AdminService_591(this, this, this.GetRMContext());
			}

			private sealed class _AdminService_591 : AdminService
			{
				public _AdminService_591(_MockRM_588 _enclosing, ResourceManager baseArg1, RMContext
					 baseArg2)
					: base(baseArg1, baseArg2)
				{
					this._enclosing = _enclosing;
				}

				protected override void SetConfig(Configuration conf)
				{
					base.SetConfig(this._enclosing._enclosing.configuration);
				}

				private readonly _MockRM_588 _enclosing;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return new TestRMHA.FailFastDispatcher(this);
			}

			private readonly TestRMHA _enclosing;
		}

		public virtual void InnerTestHAWithRMHostName(bool includeBindHost)
		{
			//this is run two times, with and without a bind host configured
			if (includeBindHost)
			{
				configuration.Set(YarnConfiguration.RmBindHost, "9.9.9.9");
			}
			//test if both RM_HOSTBANE_{rm_id} and RM_RPCADDRESS_{rm_id} are set
			//We should only read rpc addresses from RM_RPCADDRESS_{rm_id} configuration
			configuration.Set(HAUtil.AddSuffix(YarnConfiguration.RmHostname, Rm1NodeId), "1.1.1.1"
				);
			configuration.Set(HAUtil.AddSuffix(YarnConfiguration.RmHostname, Rm2NodeId), "0.0.0.0"
				);
			configuration.Set(HAUtil.AddSuffix(YarnConfiguration.RmHostname, Rm3NodeId), "2.2.2.2"
				);
			try
			{
				Configuration conf = new YarnConfiguration(configuration);
				rm = new MockRM(conf);
				rm.Init(conf);
				foreach (string confKey in YarnConfiguration.GetServiceAddressConfKeys(conf))
				{
					NUnit.Framework.Assert.AreEqual("RPC address not set for " + confKey, Rm1Address, 
						conf.Get(HAUtil.AddSuffix(confKey, Rm1NodeId)));
					NUnit.Framework.Assert.AreEqual("RPC address not set for " + confKey, Rm2Address, 
						conf.Get(HAUtil.AddSuffix(confKey, Rm2NodeId)));
					NUnit.Framework.Assert.AreEqual("RPC address not set for " + confKey, Rm3Address, 
						conf.Get(HAUtil.AddSuffix(confKey, Rm3NodeId)));
					if (includeBindHost)
					{
						NUnit.Framework.Assert.AreEqual("Web address misconfigured WITH bind-host", Sharpen.Runtime.Substring
							(rm.webAppAddress, 0, 7), "9.9.9.9");
					}
					else
					{
						//YarnConfiguration tries to figure out which rm host it's on by binding to it,
						//which doesn't happen for any of these fake addresses, so we end up with 0.0.0.0
						NUnit.Framework.Assert.AreEqual("Web address misconfigured WITHOUT bind-host", Sharpen.Runtime.Substring
							(rm.webAppAddress, 0, 7), "0.0.0.0");
					}
				}
			}
			catch (YarnRuntimeException)
			{
				NUnit.Framework.Assert.Fail("Should not throw any exceptions.");
			}
			//test if only RM_HOSTBANE_{rm_id} is set
			configuration.Clear();
			configuration.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			configuration.Set(YarnConfiguration.RmHaIds, Rm1NodeId + "," + Rm2NodeId);
			configuration.Set(HAUtil.AddSuffix(YarnConfiguration.RmHostname, Rm1NodeId), "1.1.1.1"
				);
			configuration.Set(HAUtil.AddSuffix(YarnConfiguration.RmHostname, Rm2NodeId), "0.0.0.0"
				);
			try
			{
				Configuration conf = new YarnConfiguration(configuration);
				rm = new MockRM(conf);
				rm.Init(conf);
				NUnit.Framework.Assert.AreEqual("RPC address not set for " + YarnConfiguration.RmAddress
					, "1.1.1.1:8032", conf.Get(HAUtil.AddSuffix(YarnConfiguration.RmAddress, Rm1NodeId
					)));
				NUnit.Framework.Assert.AreEqual("RPC address not set for " + YarnConfiguration.RmAddress
					, "0.0.0.0:8032", conf.Get(HAUtil.AddSuffix(YarnConfiguration.RmAddress, Rm2NodeId
					)));
			}
			catch (YarnRuntimeException)
			{
				NUnit.Framework.Assert.Fail("Should not throw any exceptions.");
			}
		}

		/// <exception cref="System.Exception"/>
		private void VerifyClusterMetrics(int activeNodes, int appsSubmitted, int appsPending
			, int containersPending, int availableMB, int activeApplications)
		{
			int timeoutSecs = 0;
			QueueMetrics metrics = rm.GetResourceScheduler().GetRootQueueMetrics();
			ClusterMetrics clusterMetrics = ClusterMetrics.GetMetrics();
			bool isAllMetricAssertionDone = false;
			string message = null;
			while (timeoutSecs++ < 5)
			{
				try
				{
					// verify queue metrics
					AssertMetric("appsSubmitted", appsSubmitted, metrics.GetAppsSubmitted());
					AssertMetric("appsPending", appsPending, metrics.GetAppsPending());
					AssertMetric("containersPending", containersPending, metrics.GetPendingContainers
						());
					AssertMetric("availableMB", availableMB, metrics.GetAvailableMB());
					AssertMetric("activeApplications", activeApplications, metrics.GetActiveApps());
					// verify node metric
					AssertMetric("activeNodes", activeNodes, clusterMetrics.GetNumActiveNMs());
					isAllMetricAssertionDone = true;
					break;
				}
				catch (Exception e)
				{
					message = e.Message;
					System.Console.Out.WriteLine("Waiting for metrics assertion to complete");
					Sharpen.Thread.Sleep(1000);
				}
			}
			NUnit.Framework.Assert.IsTrue(message, isAllMetricAssertionDone);
		}

		private void AssertMetric(string metricName, int expected, int actual)
		{
			NUnit.Framework.Assert.AreEqual("Incorrect value for metric " + metricName, expected
				, actual);
		}

		internal class MyCountingDispatcher : AbstractService, Dispatcher
		{
			private int eventHandlerCount;

			private volatile bool stopped = false;

			public MyCountingDispatcher(TestRMHA _enclosing)
				: base("MyCountingDispatcher")
			{
				this._enclosing = _enclosing;
				this.eventHandlerCount = 0;
			}

			public override EventHandler GetEventHandler()
			{
				return null;
			}

			public override void Register(Type eventType, EventHandler handler)
			{
				this.eventHandlerCount++;
			}

			public virtual int GetEventHandlerCount()
			{
				return this.eventHandlerCount;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				this.stopped = true;
				base.ServiceStop();
			}

			public virtual bool IsStopped()
			{
				return this.stopped;
			}

			private readonly TestRMHA _enclosing;
		}

		internal class FailFastDispatcher : DrainDispatcher
		{
			internal int eventreceived = 0;

			protected override void Dispatch(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				if (@event.GetType() == RMFatalEventType.TransitionToActiveFailed)
				{
					this.eventreceived++;
				}
				else
				{
					base.Dispatch(@event);
				}
			}

			public virtual int GetEventCount()
			{
				return this.eventreceived;
			}

			internal FailFastDispatcher(TestRMHA _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMHA _enclosing;
		}
	}
}
