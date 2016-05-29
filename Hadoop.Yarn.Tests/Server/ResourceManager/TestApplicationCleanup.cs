using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestApplicationCleanup
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestApplicationCleanup
			));

		private YarnConfiguration conf;

		/// <exception cref="Sharpen.UnknownHostException"/>
		[SetUp]
		public virtual void Setup()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			conf = new YarnConfiguration();
			UserGroupInformation.SetConfiguration(conf);
			conf.Set(YarnConfiguration.RecoveryEnabled, "true");
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			NUnit.Framework.Assert.IsTrue(YarnConfiguration.DefaultRmAmMaxAttempts > 1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppCleanup()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			MockRM rm = new MockRM();
			rm.Start();
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 5000);
			RMApp app = rm.SubmitApp(2000);
			//kick the scheduling
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			am.RegisterAppAttempt();
			//request for containers
			int request = 2;
			am.Allocate("127.0.0.1", 1000, request, new AList<ContainerId>());
			//kick the scheduler
			nm1.NodeHeartbeat(true);
			IList<Container> conts = am.Allocate(new AList<ResourceRequest>(), new AList<ContainerId
				>()).GetAllocatedContainers();
			int contReceived = conts.Count;
			int waitCount = 0;
			while (contReceived < request && waitCount++ < 200)
			{
				Log.Info("Got " + contReceived + " containers. Waiting to get " + request);
				Sharpen.Thread.Sleep(100);
				conts = am.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>()).GetAllocatedContainers
					();
				contReceived += conts.Count;
				nm1.NodeHeartbeat(true);
			}
			NUnit.Framework.Assert.AreEqual(request, contReceived);
			am.UnregisterAppAttempt();
			NodeHeartbeatResponse resp = nm1.NodeHeartbeat(attempt.GetAppAttemptId(), 1, ContainerState
				.Complete);
			am.WaitForState(RMAppAttemptState.Finished);
			//currently only containers are cleaned via this
			//AM container is cleaned via container launcher
			resp = nm1.NodeHeartbeat(true);
			IList<ContainerId> containersToCleanup = resp.GetContainersToCleanup();
			IList<ApplicationId> appsToCleanup = resp.GetApplicationsToCleanup();
			int numCleanedContainers = containersToCleanup.Count;
			int numCleanedApps = appsToCleanup.Count;
			waitCount = 0;
			while ((numCleanedContainers < 2 || numCleanedApps < 1) && waitCount++ < 200)
			{
				Log.Info("Waiting to get cleanup events.. cleanedConts: " + numCleanedContainers 
					+ " cleanedApps: " + numCleanedApps);
				Sharpen.Thread.Sleep(100);
				resp = nm1.NodeHeartbeat(true);
				IList<ContainerId> deltaContainersToCleanup = resp.GetContainersToCleanup();
				IList<ApplicationId> deltaAppsToCleanup = resp.GetApplicationsToCleanup();
				// Add the deltas to the global list
				Sharpen.Collections.AddAll(containersToCleanup, deltaContainersToCleanup);
				Sharpen.Collections.AddAll(appsToCleanup, deltaAppsToCleanup);
				// Update counts now
				numCleanedContainers = containersToCleanup.Count;
				numCleanedApps = appsToCleanup.Count;
			}
			NUnit.Framework.Assert.AreEqual(1, appsToCleanup.Count);
			NUnit.Framework.Assert.AreEqual(app.GetApplicationId(), appsToCleanup[0]);
			NUnit.Framework.Assert.AreEqual(1, numCleanedApps);
			NUnit.Framework.Assert.AreEqual(2, numCleanedContainers);
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerCleanup()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			DrainDispatcher dispatcher = new DrainDispatcher();
			MockRM rm = new _MockRM_167(this, dispatcher);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 5000);
			RMApp app = rm.SubmitApp(2000);
			//kick the scheduling
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			am.RegisterAppAttempt();
			//request for containers
			int request = 2;
			am.Allocate("127.0.0.1", 1000, request, new AList<ContainerId>());
			dispatcher.Await();
			//kick the scheduler
			nm1.NodeHeartbeat(true);
			IList<Container> conts = am.Allocate(new AList<ResourceRequest>(), new AList<ContainerId
				>()).GetAllocatedContainers();
			int contReceived = conts.Count;
			int waitCount = 0;
			while (contReceived < request && waitCount++ < 200)
			{
				Log.Info("Got " + contReceived + " containers. Waiting to get " + request);
				Sharpen.Thread.Sleep(100);
				conts = am.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>()).GetAllocatedContainers
					();
				dispatcher.Await();
				contReceived += conts.Count;
				nm1.NodeHeartbeat(true);
			}
			NUnit.Framework.Assert.AreEqual(request, contReceived);
			// Release a container.
			AList<ContainerId> release = new AList<ContainerId>();
			release.AddItem(conts[0].GetId());
			am.Allocate(new AList<ResourceRequest>(), release);
			dispatcher.Await();
			// Send one more heartbeat with a fake running container. This is to
			// simulate the situation that can happen if the NM reports that container
			// is running in the same heartbeat when the RM asks it to clean it up.
			IDictionary<ApplicationId, IList<ContainerStatus>> containerStatuses = new Dictionary
				<ApplicationId, IList<ContainerStatus>>();
			AList<ContainerStatus> containerStatusList = new AList<ContainerStatus>();
			containerStatusList.AddItem(BuilderUtils.NewContainerStatus(conts[0].GetId(), ContainerState
				.Running, "nothing", 0));
			containerStatuses[app.GetApplicationId()] = containerStatusList;
			NodeHeartbeatResponse resp = nm1.NodeHeartbeat(containerStatuses, true);
			WaitForContainerCleanup(dispatcher, nm1, resp);
			// Now to test the case when RM already gave cleanup, and NM suddenly
			// realizes that the container is running.
			Log.Info("Testing container launch much after release and " + "NM getting cleanup"
				);
			containerStatuses.Clear();
			containerStatusList.Clear();
			containerStatusList.AddItem(BuilderUtils.NewContainerStatus(conts[0].GetId(), ContainerState
				.Running, "nothing", 0));
			containerStatuses[app.GetApplicationId()] = containerStatusList;
			resp = nm1.NodeHeartbeat(containerStatuses, true);
			// The cleanup list won't be instantaneous as it is given out by scheduler
			// and not RMNodeImpl.
			WaitForContainerCleanup(dispatcher, nm1, resp);
			rm.Stop();
		}

		private sealed class _MockRM_167 : MockRM
		{
			public _MockRM_167(TestApplicationCleanup _enclosing, DrainDispatcher dispatcher)
			{
				this._enclosing = _enclosing;
				this.dispatcher = dispatcher;
			}

			protected internal override EventHandler<SchedulerEvent> CreateSchedulerEventDispatcher
				()
			{
				return new _SchedulerEventDispatcher_170(this, this.scheduler);
			}

			private sealed class _SchedulerEventDispatcher_170 : ResourceManager.SchedulerEventDispatcher
			{
				public _SchedulerEventDispatcher_170(_MockRM_167 _enclosing, ResourceScheduler baseArg1
					)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				public override void Handle(SchedulerEvent @event)
				{
					this._enclosing.scheduler.Handle(@event);
				}

				private readonly _MockRM_167 _enclosing;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly TestApplicationCleanup _enclosing;

			private readonly DrainDispatcher dispatcher;
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void WaitForContainerCleanup(DrainDispatcher dispatcher
			, MockNM nm, NodeHeartbeatResponse resp)
		{
			int waitCount = 0;
			int cleanedConts = 0;
			IList<ContainerId> contsToClean;
			do
			{
				dispatcher.Await();
				contsToClean = resp.GetContainersToCleanup();
				cleanedConts += contsToClean.Count;
				if (cleanedConts >= 1)
				{
					break;
				}
				Sharpen.Thread.Sleep(100);
				resp = nm.NodeHeartbeat(true);
			}
			while (waitCount++ < 200);
			if (contsToClean.IsEmpty())
			{
				Log.Error("Failed to get any containers to cleanup");
			}
			else
			{
				Log.Info("Got cleanup for " + contsToClean[0]);
			}
			NUnit.Framework.Assert.AreEqual(1, cleanedConts);
		}

		/// <exception cref="System.Exception"/>
		private void WaitForAppCleanupMessageRecved(MockNM nm, ApplicationId appId)
		{
			while (true)
			{
				NodeHeartbeatResponse response = nm.NodeHeartbeat(true);
				if (response.GetApplicationsToCleanup() != null && response.GetApplicationsToCleanup
					().Count == 1 && appId.Equals(response.GetApplicationsToCleanup()[0]))
				{
					return;
				}
				Log.Info("Haven't got application=" + appId.ToString() + " in cleanup list from node heartbeat response, "
					 + "sleep for a while before next heartbeat");
				Sharpen.Thread.Sleep(1000);
			}
		}

		/// <exception cref="System.Exception"/>
		private MockAM LaunchAM(RMApp app, MockRM rm, MockNM nm)
		{
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			nm.NodeHeartbeat(true);
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			am.RegisterAppAttempt();
			rm.WaitForState(app.GetApplicationId(), RMAppState.Running);
			return am;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppCleanupWhenRMRestartedAfterAppFinished()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// start RM
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			nm1.NodeHeartbeat(am0.GetApplicationAttemptId(), 1, ContainerState.Complete);
			rm1.WaitForState(app0.GetApplicationId(), RMAppState.Failed);
			// start new RM
			MockRM rm2 = new MockRM(conf, memStore);
			rm2.Start();
			// nm1 register to rm2, and do a heartbeat
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			nm1.RegisterNode(Arrays.AsList(app0.GetApplicationId()));
			rm2.WaitForState(app0.GetApplicationId(), RMAppState.Failed);
			// wait for application cleanup message received
			WaitForAppCleanupMessageRecved(nm1, app0.GetApplicationId());
			rm1.Stop();
			rm2.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppCleanupWhenRMRestartedBeforeAppFinished()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// start RM
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 1024, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			MockNM nm2 = new MockNM("127.0.0.1:5678", 1024, rm1.GetResourceTrackerService());
			nm2.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			// alloc another container on nm2
			AllocateResponse allocResponse = am0.Allocate(Arrays.AsList(ResourceRequest.NewInstance
				(Priority.NewInstance(1), "*", Resource.NewInstance(1024, 0), 1)), null);
			while (null == allocResponse.GetAllocatedContainers() || allocResponse.GetAllocatedContainers
				().IsEmpty())
			{
				nm2.NodeHeartbeat(true);
				allocResponse = am0.Allocate(null, null);
				Sharpen.Thread.Sleep(1000);
			}
			// start new RM
			MockRM rm2 = new MockRM(conf, memStore);
			rm2.Start();
			// nm1/nm2 register to rm2, and do a heartbeat
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			nm1.RegisterNode(Arrays.AsList(NMContainerStatus.NewInstance(ContainerId.NewContainerId
				(am0.GetApplicationAttemptId(), 1), ContainerState.Complete, Resource.NewInstance
				(1024, 1), string.Empty, 0, Priority.NewInstance(0), 1234)), Arrays.AsList(app0.
				GetApplicationId()));
			nm2.SetResourceTrackerService(rm2.GetResourceTrackerService());
			nm2.RegisterNode(Arrays.AsList(app0.GetApplicationId()));
			// assert app state has been saved.
			rm2.WaitForState(app0.GetApplicationId(), RMAppState.Failed);
			// wait for application cleanup message received on NM1
			WaitForAppCleanupMessageRecved(nm1, app0.GetApplicationId());
			// wait for application cleanup message received on NM2
			WaitForAppCleanupMessageRecved(nm2, app0.GetApplicationId());
			rm1.Stop();
			rm2.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerCleanupWhenRMRestartedAppNotRegistered()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// start RM
			DrainDispatcher dispatcher = new DrainDispatcher();
			MockRM rm1 = new _MockRM_413(dispatcher, conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			nm1.NodeHeartbeat(am0.GetApplicationAttemptId(), 1, ContainerState.Running);
			rm1.WaitForState(app0.GetApplicationId(), RMAppState.Running);
			// start new RM
			DrainDispatcher dispatcher2 = new DrainDispatcher();
			MockRM rm2 = new _MockRM_432(dispatcher2, conf, memStore);
			rm2.Start();
			// nm1 register to rm2, and do a heartbeat
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			nm1.RegisterNode(Arrays.AsList(app0.GetApplicationId()));
			rm2.WaitForState(app0.GetApplicationId(), RMAppState.Accepted);
			// Add unknown container for application unknown to scheduler
			NodeHeartbeatResponse response = nm1.NodeHeartbeat(am0.GetApplicationAttemptId(), 
				2, ContainerState.Running);
			WaitForContainerCleanup(dispatcher2, nm1, response);
			rm1.Stop();
			rm2.Stop();
		}

		private sealed class _MockRM_413 : MockRM
		{
			public _MockRM_413(DrainDispatcher dispatcher, Configuration baseArg1, RMStateStore
				 baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly DrainDispatcher dispatcher;
		}

		private sealed class _MockRM_432 : MockRM
		{
			public _MockRM_432(DrainDispatcher dispatcher2, Configuration baseArg1, RMStateStore
				 baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.dispatcher2 = dispatcher2;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher2;
			}

			private readonly DrainDispatcher dispatcher2;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppCleanupWhenNMReconnects()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// start RM
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			nm1.NodeHeartbeat(am0.GetApplicationAttemptId(), 1, ContainerState.Complete);
			rm1.WaitForState(app0.GetApplicationId(), RMAppState.Failed);
			// wait for application cleanup message received
			WaitForAppCleanupMessageRecved(nm1, app0.GetApplicationId());
			// reconnect NM with application still active
			nm1.RegisterNode(Arrays.AsList(app0.GetApplicationId()));
			WaitForAppCleanupMessageRecved(nm1, app0.GetApplicationId());
			rm1.Stop();
		}

		// The test verifies processing of NMContainerStatuses which are sent during
		// NM registration.
		// 1. Start the cluster-RM,NM,Submit app with 1024MB,Launch & register AM
		// 2. AM sends ResourceRequest for 1 container with memory 2048MB.
		// 3. Verify for number of container allocated by RM
		// 4. Verify Memory Usage by cluster, it should be 3072. AM memory + requested
		// memory. 1024 + 2048=3072
		// 5. Re-register NM by sending completed container status
		// 6. Verify for Memory Used, it should be 1024
		// 7. Send AM heatbeat to RM. Allocated response should contain completed
		// container.
		/// <exception cref="System.Exception"/>
		public virtual void TestProcessingNMContainerStatusesOnNMRestart()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// 1. Start the cluster-RM,NM,Submit app with 1024MB,Launch & register AM
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			int nmMemory = 8192;
			int amMemory = 1024;
			int containerMemory = 2048;
			MockNM nm1 = new MockNM("127.0.0.1:1234", nmMemory, rm1.GetResourceTrackerService
				());
			nm1.RegisterNode();
			RMApp app0 = rm1.SubmitApp(amMemory);
			MockAM am0 = MockRM.LaunchAndRegisterAM(app0, rm1, nm1);
			// 2. AM sends ResourceRequest for 1 container with memory 2048MB.
			int noOfContainers = 1;
			IList<Container> allocateContainers = am0.AllocateAndWaitForContainers(noOfContainers
				, containerMemory, nm1);
			// 3. Verify for number of container allocated by RM
			NUnit.Framework.Assert.AreEqual(noOfContainers, allocateContainers.Count);
			Container container = allocateContainers[0];
			nm1.NodeHeartbeat(am0.GetApplicationAttemptId(), 1, ContainerState.Running);
			nm1.NodeHeartbeat(am0.GetApplicationAttemptId(), container.GetId().GetContainerId
				(), ContainerState.Running);
			rm1.WaitForState(app0.GetApplicationId(), RMAppState.Running);
			// 4. Verify Memory Usage by cluster, it should be 3072. AM memory +
			// requested memory. 1024 + 2048=3072
			ResourceScheduler rs = rm1.GetRMContext().GetScheduler();
			int allocatedMB = rs.GetRootQueueMetrics().GetAllocatedMB();
			NUnit.Framework.Assert.AreEqual(amMemory + containerMemory, allocatedMB);
			// 5. Re-register NM by sending completed container status
			IList<NMContainerStatus> nMContainerStatusForApp = CreateNMContainerStatusForApp(
				am0);
			nm1.RegisterNode(nMContainerStatusForApp, Arrays.AsList(app0.GetApplicationId()));
			WaitForClusterMemory(nm1, rs, amMemory);
			// 6. Verify for Memory Used, it should be 1024
			NUnit.Framework.Assert.AreEqual(amMemory, rs.GetRootQueueMetrics().GetAllocatedMB
				());
			// 7. Send AM heatbeat to RM. Allocated response should contain completed
			// container
			AllocateRequest req = AllocateRequest.NewInstance(0, 0F, new AList<ResourceRequest
				>(), new AList<ContainerId>(), null);
			AllocateResponse allocate = am0.Allocate(req);
			IList<ContainerStatus> completedContainersStatuses = allocate.GetCompletedContainersStatuses
				();
			NUnit.Framework.Assert.AreEqual(noOfContainers, completedContainersStatuses.Count
				);
			// Application clean up should happen Cluster memory used is 0
			nm1.NodeHeartbeat(am0.GetApplicationAttemptId(), 1, ContainerState.Complete);
			WaitForClusterMemory(nm1, rs, 0);
			rm1.Stop();
		}

		/// <exception cref="System.Exception"/>
		private void WaitForClusterMemory(MockNM nm1, ResourceScheduler rs, int clusterMemory
			)
		{
			int counter = 0;
			while (rs.GetRootQueueMetrics().GetAllocatedMB() != clusterMemory)
			{
				nm1.NodeHeartbeat(true);
				Sharpen.Thread.Sleep(100);
				if (counter++ == 50)
				{
					NUnit.Framework.Assert.Fail("Wait for cluster memory is timed out.Expected=" + clusterMemory
						 + " Actual=" + rs.GetRootQueueMetrics().GetAllocatedMB());
				}
			}
		}

		public static IList<NMContainerStatus> CreateNMContainerStatusForApp(MockAM am)
		{
			IList<NMContainerStatus> list = new AList<NMContainerStatus>();
			NMContainerStatus amContainer = CreateNMContainerStatus(am.GetApplicationAttemptId
				(), 1, ContainerState.Running, 1024);
			NMContainerStatus completedContainer = CreateNMContainerStatus(am.GetApplicationAttemptId
				(), 2, ContainerState.Complete, 2048);
			list.AddItem(amContainer);
			list.AddItem(completedContainer);
			return list;
		}

		public static NMContainerStatus CreateNMContainerStatus(ApplicationAttemptId appAttemptId
			, int id, ContainerState containerState, int memory)
		{
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, id);
			NMContainerStatus containerReport = NMContainerStatus.NewInstance(containerId, containerState
				, Resource.NewInstance(memory, 1), "recover container", 0, Priority.NewInstance(
				0), 0);
			return containerReport;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestApplicationCleanup t = new TestApplicationCleanup();
			t.TestAppCleanup();
		}
	}
}
