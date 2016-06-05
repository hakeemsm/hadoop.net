using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Log4j;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestRM : ParameterizedSchedulerTestBase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestRM
			));

		private const int WaitSleepMs = 100;

		private YarnConfiguration conf;

		public TestRM(ParameterizedSchedulerTestBase.SchedulerType type)
			: base(type)
		{
		}

		// Milliseconds to sleep for when waiting for something to happen
		[SetUp]
		public virtual void Setup()
		{
			conf = GetConf();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			ClusterMetrics.Destroy();
			QueueMetrics.ClearQueueMetrics();
			DefaultMetricsSystem.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNewAppId()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			MockRM rm = new MockRM(conf);
			rm.Start();
			GetNewApplicationResponse resp = rm.GetNewAppId();
			System.Diagnostics.Debug.Assert((resp.GetApplicationId().GetId() != 0));
			System.Diagnostics.Debug.Assert((resp.GetMaximumResourceCapability().GetMemory() 
				> 0));
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppWithNoContainers()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			MockRM rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			RMApp app = rm.SubmitApp(2000);
			//kick the scheduling
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			am.RegisterAppAttempt();
			am.UnregisterAppAttempt();
			nm1.NodeHeartbeat(attempt.GetAppAttemptId(), 1, ContainerState.Complete);
			am.WaitForState(RMAppAttemptState.Finished);
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppOnMultiNode()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			conf.Set("yarn.scheduler.capacity.node-locality-delay", "-1");
			MockRM rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("h2:5678", 10240);
			RMApp app = rm.SubmitApp(2000);
			//kick the scheduling
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			am.RegisterAppAttempt();
			//request for containers
			int request = 13;
			am.Allocate("h1", 1000, request, new AList<ContainerId>());
			//kick the scheduler
			IList<Container> conts = am.Allocate(new AList<ResourceRequest>(), new AList<ContainerId
				>()).GetAllocatedContainers();
			int contReceived = conts.Count;
			while (contReceived < 3)
			{
				//only 3 containers are available on node1
				nm1.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(conts, am.Allocate(new AList<ResourceRequest>(), new AList
					<ContainerId>()).GetAllocatedContainers());
				contReceived = conts.Count;
				Log.Info("Got " + contReceived + " containers. Waiting to get " + 3);
				Sharpen.Thread.Sleep(WaitSleepMs);
			}
			NUnit.Framework.Assert.AreEqual(3, conts.Count);
			//send node2 heartbeat
			conts = am.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>()).GetAllocatedContainers
				();
			contReceived = conts.Count;
			while (contReceived < 10)
			{
				nm2.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(conts, am.Allocate(new AList<ResourceRequest>(), new AList
					<ContainerId>()).GetAllocatedContainers());
				contReceived = conts.Count;
				Log.Info("Got " + contReceived + " containers. Waiting to get " + 10);
				Sharpen.Thread.Sleep(WaitSleepMs);
			}
			NUnit.Framework.Assert.AreEqual(10, conts.Count);
			am.UnregisterAppAttempt();
			nm1.NodeHeartbeat(attempt.GetAppAttemptId(), 1, ContainerState.Complete);
			am.WaitForState(RMAppAttemptState.Finished);
			rm.Stop();
		}

		// Test even if AM container is allocated with containerId not equal to 1, the
		// following allocate requests from AM should be able to retrieve the
		// corresponding NM Token.
		/// <exception cref="System.Exception"/>
		public virtual void TestNMTokenSentForNormalContainer()
		{
			conf.Set(YarnConfiguration.RmScheduler, typeof(CapacityScheduler).GetCanonicalName
				());
			MockRM rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			RMApp app = rm.SubmitApp(2000);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			// Call getNewContainerId to increase container Id so that the AM container
			// Id doesn't equal to one.
			CapacityScheduler cs = (CapacityScheduler)rm.GetResourceScheduler();
			cs.GetApplicationAttempt(attempt.GetAppAttemptId()).GetNewContainerId();
			// kick the scheduling
			nm1.NodeHeartbeat(true);
			MockAM am = MockRM.LaunchAM(app, rm, nm1);
			// am container Id not equal to 1.
			NUnit.Framework.Assert.IsTrue(attempt.GetMasterContainer().GetId().GetContainerId
				() != 1);
			// NMSecretManager doesn't record the node on which the am is allocated.
			NUnit.Framework.Assert.IsFalse(rm.GetRMContext().GetNMTokenSecretManager().IsApplicationAttemptNMTokenPresent
				(attempt.GetAppAttemptId(), nm1.GetNodeId()));
			am.RegisterAppAttempt();
			rm.WaitForState(app.GetApplicationId(), RMAppState.Running);
			int NumContainers = 1;
			IList<Container> containers = new AList<Container>();
			// nmTokens keeps track of all the nmTokens issued in the allocate call.
			IList<NMToken> expectedNMTokens = new AList<NMToken>();
			// am1 allocate 1 container on nm1.
			while (true)
			{
				AllocateResponse response = am.Allocate("127.0.0.1", 2000, NumContainers, new AList
					<ContainerId>());
				nm1.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(containers, response.GetAllocatedContainers());
				Sharpen.Collections.AddAll(expectedNMTokens, response.GetNMTokens());
				if (containers.Count == NumContainers)
				{
					break;
				}
				Sharpen.Thread.Sleep(200);
				System.Console.Out.WriteLine("Waiting for container to be allocated.");
			}
			NodeId nodeId = expectedNMTokens[0].GetNodeId();
			// NMToken is sent for the allocated container.
			NUnit.Framework.Assert.AreEqual(nm1.GetNodeId(), nodeId);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNMToken()
		{
			MockRM rm = new MockRM(conf);
			try
			{
				rm.Start();
				MockNM nm1 = rm.RegisterNode("h1:1234", 10000);
				NMTokenSecretManagerInRM nmTokenSecretManager = rm.GetRMContext().GetNMTokenSecretManager
					();
				// submitting new application
				RMApp app = rm.SubmitApp(1000);
				// start scheduling.
				nm1.NodeHeartbeat(true);
				// Starting application attempt and launching
				// It should get registered with NMTokenSecretManager.
				RMAppAttempt attempt = app.GetCurrentAppAttempt();
				MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
				NUnit.Framework.Assert.IsTrue(nmTokenSecretManager.IsApplicationAttemptRegistered
					(attempt.GetAppAttemptId()));
				// This will register application master.
				am.RegisterAppAttempt();
				AList<Container> containersReceivedForNM1 = new AList<Container>();
				IList<ContainerId> releaseContainerList = new AList<ContainerId>();
				Dictionary<string, Token> nmTokens = new Dictionary<string, Token>();
				// initially requesting 2 containers.
				AllocateResponse response = am.Allocate("h1", 1000, 2, releaseContainerList);
				NUnit.Framework.Assert.AreEqual(0, response.GetAllocatedContainers().Count);
				AllocateContainersAndValidateNMTokens(am, containersReceivedForNM1, 2, nmTokens, 
					nm1);
				NUnit.Framework.Assert.AreEqual(1, nmTokens.Count);
				// requesting 2 more containers.
				response = am.Allocate("h1", 1000, 2, releaseContainerList);
				NUnit.Framework.Assert.AreEqual(0, response.GetAllocatedContainers().Count);
				AllocateContainersAndValidateNMTokens(am, containersReceivedForNM1, 4, nmTokens, 
					nm1);
				NUnit.Framework.Assert.AreEqual(1, nmTokens.Count);
				// We will be simulating NM restart so restarting newly added h2:1234
				// NM 2 now registers.
				MockNM nm2 = rm.RegisterNode("h2:1234", 10000);
				nm2.NodeHeartbeat(true);
				AList<Container> containersReceivedForNM2 = new AList<Container>();
				response = am.Allocate("h2", 1000, 2, releaseContainerList);
				NUnit.Framework.Assert.AreEqual(0, response.GetAllocatedContainers().Count);
				AllocateContainersAndValidateNMTokens(am, containersReceivedForNM2, 2, nmTokens, 
					nm2);
				NUnit.Framework.Assert.AreEqual(2, nmTokens.Count);
				// Simulating NM-2 restart.
				nm2 = rm.RegisterNode("h2:1234", 10000);
				// Wait for reconnect to make it through the RM and create a new RMNode
				IDictionary<NodeId, RMNode> nodes = rm.GetRMContext().GetRMNodes();
				while (nodes[nm2.GetNodeId()].GetLastNodeHeartBeatResponse().GetResponseId() > 0)
				{
					Sharpen.Thread.Sleep(WaitSleepMs);
				}
				int interval = 40;
				// Wait for nm Token to be cleared.
				while (nmTokenSecretManager.IsApplicationAttemptNMTokenPresent(attempt.GetAppAttemptId
					(), nm2.GetNodeId()) && interval-- > 0)
				{
					Log.Info("waiting for nmToken to be cleared for : " + nm2.GetNodeId());
					Sharpen.Thread.Sleep(WaitSleepMs);
				}
				NUnit.Framework.Assert.IsTrue(nmTokenSecretManager.IsApplicationAttemptRegistered
					(attempt.GetAppAttemptId()));
				// removing NMToken for h2:1234
				Sharpen.Collections.Remove(nmTokens, nm2.GetNodeId().ToString());
				NUnit.Framework.Assert.AreEqual(1, nmTokens.Count);
				// We should again receive the NMToken.
				response = am.Allocate("h2", 1000, 2, releaseContainerList);
				NUnit.Framework.Assert.AreEqual(0, response.GetAllocatedContainers().Count);
				AllocateContainersAndValidateNMTokens(am, containersReceivedForNM2, 4, nmTokens, 
					nm2);
				NUnit.Framework.Assert.AreEqual(2, nmTokens.Count);
				// Now rolling over NMToken masterKey. it should resend the NMToken in
				// next allocate call.
				NUnit.Framework.Assert.IsTrue(nmTokenSecretManager.IsApplicationAttemptNMTokenPresent
					(attempt.GetAppAttemptId(), nm1.GetNodeId()));
				NUnit.Framework.Assert.IsTrue(nmTokenSecretManager.IsApplicationAttemptNMTokenPresent
					(attempt.GetAppAttemptId(), nm2.GetNodeId()));
				nmTokenSecretManager.RollMasterKey();
				nmTokenSecretManager.ActivateNextMasterKey();
				NUnit.Framework.Assert.IsFalse(nmTokenSecretManager.IsApplicationAttemptNMTokenPresent
					(attempt.GetAppAttemptId(), nm1.GetNodeId()));
				NUnit.Framework.Assert.IsFalse(nmTokenSecretManager.IsApplicationAttemptNMTokenPresent
					(attempt.GetAppAttemptId(), nm2.GetNodeId()));
				// It should not remove application attempt entry.
				NUnit.Framework.Assert.IsTrue(nmTokenSecretManager.IsApplicationAttemptRegistered
					(attempt.GetAppAttemptId()));
				nmTokens.Clear();
				NUnit.Framework.Assert.AreEqual(0, nmTokens.Count);
				// We should again receive the NMToken.
				response = am.Allocate("h2", 1000, 1, releaseContainerList);
				NUnit.Framework.Assert.AreEqual(0, response.GetAllocatedContainers().Count);
				AllocateContainersAndValidateNMTokens(am, containersReceivedForNM2, 5, nmTokens, 
					nm2);
				NUnit.Framework.Assert.AreEqual(1, nmTokens.Count);
				NUnit.Framework.Assert.IsTrue(nmTokenSecretManager.IsApplicationAttemptNMTokenPresent
					(attempt.GetAppAttemptId(), nm2.GetNodeId()));
				// After AM is finished making sure that nmtoken entry for app
				NUnit.Framework.Assert.IsTrue(nmTokenSecretManager.IsApplicationAttemptRegistered
					(attempt.GetAppAttemptId()));
				am.UnregisterAppAttempt();
				// marking all the containers as finished.
				foreach (Container container in containersReceivedForNM1)
				{
					nm1.NodeHeartbeat(attempt.GetAppAttemptId(), container.GetId().GetContainerId(), 
						ContainerState.Complete);
				}
				foreach (Container container_1 in containersReceivedForNM2)
				{
					nm2.NodeHeartbeat(attempt.GetAppAttemptId(), container_1.GetId().GetContainerId()
						, ContainerState.Complete);
				}
				nm1.NodeHeartbeat(am.GetApplicationAttemptId(), 1, ContainerState.Complete);
				am.WaitForState(RMAppAttemptState.Finished);
				NUnit.Framework.Assert.IsFalse(nmTokenSecretManager.IsApplicationAttemptRegistered
					(attempt.GetAppAttemptId()));
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void AllocateContainersAndValidateNMTokens(MockAM am, 
			AList<Container> containersReceived, int totalContainerRequested, Dictionary<string
			, Token> nmTokens, MockNM nm)
		{
			AList<ContainerId> releaseContainerList = new AList<ContainerId>();
			AllocateResponse response;
			AList<ResourceRequest> resourceRequest = new AList<ResourceRequest>();
			while (containersReceived.Count < totalContainerRequested)
			{
				nm.NodeHeartbeat(true);
				Log.Info("requesting containers..");
				response = am.Allocate(resourceRequest, releaseContainerList);
				Sharpen.Collections.AddAll(containersReceived, response.GetAllocatedContainers());
				if (!response.GetNMTokens().IsEmpty())
				{
					foreach (NMToken nmToken in response.GetNMTokens())
					{
						string nodeId = nmToken.GetNodeId().ToString();
						if (nmTokens.Contains(nodeId))
						{
							NUnit.Framework.Assert.Fail("Duplicate NMToken received for : " + nodeId);
						}
						nmTokens[nodeId] = nmToken.GetToken();
					}
				}
				Log.Info("Got " + containersReceived.Count + " containers. Waiting to get " + totalContainerRequested
					);
				Sharpen.Thread.Sleep(WaitSleepMs);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestActivatingApplicationAfterAddingNM()
		{
			MockRM rm1 = new MockRM(conf);
			// start like normal because state is empty
			rm1.Start();
			// app that gets launched
			RMApp app1 = rm1.SubmitApp(200);
			// app that does not get launched
			RMApp app2 = rm1.SubmitApp(200);
			// app1 and app2 should be scheduled, but because no resource is available,
			// they are not activated.
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			ApplicationAttemptId attemptId1 = attempt1.GetAppAttemptId();
			rm1.WaitForState(attemptId1, RMAppAttemptState.Scheduled);
			RMAppAttempt attempt2 = app2.GetCurrentAppAttempt();
			ApplicationAttemptId attemptId2 = attempt2.GetAppAttemptId();
			rm1.WaitForState(attemptId2, RMAppAttemptState.Scheduled);
			MockNM nm1 = new MockNM("h1:1234", 15120, rm1.GetResourceTrackerService());
			MockNM nm2 = new MockNM("h2:5678", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			nm2.RegisterNode();
			//kick the scheduling
			nm1.NodeHeartbeat(true);
			// app1 should be allocated now
			rm1.WaitForState(attemptId1, RMAppAttemptState.Allocated);
			rm1.WaitForState(attemptId2, RMAppAttemptState.Scheduled);
			nm2.NodeHeartbeat(true);
			// app2 should be allocated now
			rm1.WaitForState(attemptId1, RMAppAttemptState.Allocated);
			rm1.WaitForState(attemptId2, RMAppAttemptState.Allocated);
			rm1.Stop();
		}

		// This is to test AM Host and rpc port are invalidated after the am attempt
		// is killed or failed, so that client doesn't get the wrong information.
		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidateAMHostPortWhenAMFailedOrKilled()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			// a succeeded app
			RMApp app1 = rm1.SubmitApp(200);
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			MockRM.FinishAMAndVerifyAppState(app1, rm1, nm1, am1);
			// a failed app
			RMApp app2 = rm1.SubmitApp(200);
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm1, nm1);
			nm1.NodeHeartbeat(am2.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am2.WaitForState(RMAppAttemptState.Failed);
			rm1.WaitForState(app2.GetApplicationId(), RMAppState.Failed);
			// a killed app
			RMApp app3 = rm1.SubmitApp(200);
			MockAM am3 = MockRM.LaunchAndRegisterAM(app3, rm1, nm1);
			rm1.KillApp(app3.GetApplicationId());
			rm1.WaitForState(app3.GetApplicationId(), RMAppState.Killed);
			rm1.WaitForState(am3.GetApplicationAttemptId(), RMAppAttemptState.Killed);
			GetApplicationsRequest request1 = GetApplicationsRequest.NewInstance(EnumSet.Of(YarnApplicationState
				.Finished, YarnApplicationState.Killed, YarnApplicationState.Failed));
			GetApplicationsResponse response1 = rm1.GetClientRMService().GetApplications(request1
				);
			IList<ApplicationReport> appList1 = response1.GetApplicationList();
			NUnit.Framework.Assert.AreEqual(3, appList1.Count);
			foreach (ApplicationReport report in appList1)
			{
				// killed/failed apps host and rpc port are invalidated.
				if (report.GetApplicationId().Equals(app2.GetApplicationId()) || report.GetApplicationId
					().Equals(app3.GetApplicationId()))
				{
					NUnit.Framework.Assert.AreEqual("N/A", report.GetHost());
					NUnit.Framework.Assert.AreEqual(-1, report.GetRpcPort());
				}
				// succeeded app's host and rpc port is not invalidated
				if (report.GetApplicationId().Equals(app1.GetApplicationId()))
				{
					NUnit.Framework.Assert.IsFalse(report.GetHost().Equals("N/A"));
					NUnit.Framework.Assert.IsTrue(report.GetRpcPort() != -1);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidatedAMHostPortOnAMRestart()
		{
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// a failed app
			RMApp app2 = rm1.SubmitApp(200);
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm1, nm1);
			nm1.NodeHeartbeat(am2.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am2.WaitForState(RMAppAttemptState.Failed);
			rm1.WaitForState(app2.GetApplicationId(), RMAppState.Accepted);
			// before new attempt is launched, the app report returns the invalid AM
			// host and port.
			GetApplicationReportRequest request1 = GetApplicationReportRequest.NewInstance(app2
				.GetApplicationId());
			ApplicationReport report1 = rm1.GetClientRMService().GetApplicationReport(request1
				).GetApplicationReport();
			NUnit.Framework.Assert.AreEqual("N/A", report1.GetHost());
			NUnit.Framework.Assert.AreEqual(-1, report1.GetRpcPort());
		}

		/// <summary>Validate killing an application when it is at accepted state.</summary>
		/// <exception cref="System.Exception">exception</exception>
		public virtual void TestApplicationKillAtAcceptedState()
		{
			Dispatcher dispatcher = new _AsyncDispatcher_573();
			MockRM rm = new _MockRM_596(dispatcher, conf);
			// test metrics
			QueueMetrics metrics = rm.GetResourceScheduler().GetRootQueueMetrics();
			int appsKilled = metrics.GetAppsKilled();
			int appsSubmitted = metrics.GetAppsSubmitted();
			rm.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm.GetResourceTrackerService());
			nm1.RegisterNode();
			// a failed app
			RMApp application = rm.SubmitApp(200);
			MockAM am = MockRM.LaunchAM(application, rm, nm1);
			am.WaitForState(RMAppAttemptState.Launched);
			nm1.NodeHeartbeat(am.GetApplicationAttemptId(), 1, ContainerState.Running);
			rm.WaitForState(application.GetApplicationId(), RMAppState.Accepted);
			// Now kill the application before new attempt is launched, the app report
			// returns the invalid AM host and port.
			KillApplicationRequest request = KillApplicationRequest.NewInstance(application.GetApplicationId
				());
			rm.GetClientRMService().ForceKillApplication(request);
			// Specific test for YARN-1689 follows
			// Now let's say a race causes AM to register now. This should not crash RM.
			am.RegisterAppAttempt(false);
			// We explicitly intercepted the kill-event to RMAppAttempt, so app should
			// still be in KILLING state.
			rm.WaitForState(application.GetApplicationId(), RMAppState.Killing);
			// AM should now be in running
			rm.WaitForState(am.GetApplicationAttemptId(), RMAppAttemptState.Running);
			// Simulate that appAttempt is killed.
			rm.GetRMContext().GetDispatcher().GetEventHandler().Handle(new RMAppEvent(application
				.GetApplicationId(), RMAppEventType.AttemptKilled));
			rm.WaitForState(application.GetApplicationId(), RMAppState.Killed);
			// test metrics
			metrics = rm.GetResourceScheduler().GetRootQueueMetrics();
			NUnit.Framework.Assert.AreEqual(appsKilled + 1, metrics.GetAppsKilled());
			NUnit.Framework.Assert.AreEqual(appsSubmitted + 1, metrics.GetAppsSubmitted());
		}

		private sealed class _AsyncDispatcher_573 : AsyncDispatcher
		{
			public _AsyncDispatcher_573()
			{
			}

			public override EventHandler GetEventHandler()
			{
				EventHandler handler = Org.Mockito.Mockito.Spy(base.GetEventHandler());
				Org.Mockito.Mockito.DoNothing().When(handler).Handle(Matchers.ArgThat(new _T946821583
					(this)));
				return handler;
			}

			internal class _T946821583 : ArgumentMatcher<AbstractEvent>
			{
				public override bool Matches(object argument)
				{
					if (argument is RMAppAttemptEvent)
					{
						if (((RMAppAttemptEvent)argument).GetType().Equals(RMAppAttemptEventType.Kill))
						{
							return true;
						}
					}
					return false;
				}

				internal _T946821583(_AsyncDispatcher_573 _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly _AsyncDispatcher_573 _enclosing;
			}
		}

		private sealed class _MockRM_596 : MockRM
		{
			public _MockRM_596(Dispatcher dispatcher, Configuration baseArg1)
				: base(baseArg1)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly Dispatcher dispatcher;
		}

		// Test Kill an app while the app is finishing in the meanwhile.
		/// <exception cref="System.Exception"/>
		public virtual void TestKillFinishingApp()
		{
			// this dispatcher ignores RMAppAttemptEventType.KILL event
			Dispatcher dispatcher = new _AsyncDispatcher_654();
			MockRM rm1 = new _MockRM_677(dispatcher, conf);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			rm1.KillApp(app1.GetApplicationId());
			FinishApplicationMasterRequest req = FinishApplicationMasterRequest.NewInstance(FinalApplicationStatus
				.Succeeded, string.Empty, string.Empty);
			am1.UnregisterAppAttempt(req, true);
			rm1.WaitForState(am1.GetApplicationAttemptId(), RMAppAttemptState.Finishing);
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Complete);
			rm1.WaitForState(am1.GetApplicationAttemptId(), RMAppAttemptState.Finished);
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Finished);
		}

		private sealed class _AsyncDispatcher_654 : AsyncDispatcher
		{
			public _AsyncDispatcher_654()
			{
			}

			public override EventHandler GetEventHandler()
			{
				EventHandler handler = Org.Mockito.Mockito.Spy(base.GetEventHandler());
				Org.Mockito.Mockito.DoNothing().When(handler).Handle(Matchers.ArgThat(new _T1869621103
					(this)));
				return handler;
			}

			internal class _T1869621103 : ArgumentMatcher<AbstractEvent>
			{
				public override bool Matches(object argument)
				{
					if (argument is RMAppAttemptEvent)
					{
						if (((RMAppAttemptEvent)argument).GetType().Equals(RMAppAttemptEventType.Kill))
						{
							return true;
						}
					}
					return false;
				}

				internal _T1869621103(_AsyncDispatcher_654 _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly _AsyncDispatcher_654 _enclosing;
			}
		}

		private sealed class _MockRM_677 : MockRM
		{
			public _MockRM_677(Dispatcher dispatcher, Configuration baseArg1)
				: base(baseArg1)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly Dispatcher dispatcher;
		}

		// Test Kill an app while the app is failing
		/// <exception cref="System.Exception"/>
		public virtual void TestKillFailingApp()
		{
			// this dispatcher ignores RMAppAttemptEventType.KILL event
			Dispatcher dispatcher = new _AsyncDispatcher_708();
			MockRM rm1 = new _MockRM_731(dispatcher, conf);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			rm1.KillApp(app1.GetApplicationId());
			// fail the app by sending container_finished event.
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Complete);
			rm1.WaitForState(am1.GetApplicationAttemptId(), RMAppAttemptState.Failed);
			// app is killed, not launching a new attempt
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Killed);
		}

		private sealed class _AsyncDispatcher_708 : AsyncDispatcher
		{
			public _AsyncDispatcher_708()
			{
			}

			public override EventHandler GetEventHandler()
			{
				EventHandler handler = Org.Mockito.Mockito.Spy(base.GetEventHandler());
				Org.Mockito.Mockito.DoNothing().When(handler).Handle(Matchers.ArgThat(new _T866489717
					(this)));
				return handler;
			}

			internal class _T866489717 : ArgumentMatcher<AbstractEvent>
			{
				public override bool Matches(object argument)
				{
					if (argument is RMAppAttemptEvent)
					{
						if (((RMAppAttemptEvent)argument).GetType().Equals(RMAppAttemptEventType.Kill))
						{
							return true;
						}
					}
					return false;
				}

				internal _T866489717(_AsyncDispatcher_708 _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly _AsyncDispatcher_708 _enclosing;
			}
		}

		private sealed class _MockRM_731 : MockRM
		{
			public _MockRM_731(Dispatcher dispatcher, Configuration baseArg1)
				: base(baseArg1)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly Dispatcher dispatcher;
		}
	}
}
