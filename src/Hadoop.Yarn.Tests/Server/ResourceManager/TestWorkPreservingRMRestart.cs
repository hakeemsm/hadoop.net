using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestWorkPreservingRMRestart : ParameterizedSchedulerTestBase
	{
		private YarnConfiguration conf;

		internal MockRM rm1 = null;

		internal MockRM rm2 = null;

		public TestWorkPreservingRMRestart(ParameterizedSchedulerTestBase.SchedulerType type
			)
			: base(type)
		{
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		[SetUp]
		public virtual void Setup()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			conf = GetConf();
			UserGroupInformation.SetConfiguration(conf);
			conf.Set(YarnConfiguration.RecoveryEnabled, "true");
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, true);
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 0);
			DefaultMetricsSystem.SetMiniClusterMode(true);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (rm1 != null)
			{
				rm1.Stop();
			}
			if (rm2 != null)
			{
				rm2.Stop();
			}
		}

		// Test common scheduler state including SchedulerAttempt, SchedulerNode,
		// AppSchedulingInfo can be reconstructed via the container recovery reports
		// on NM re-registration.
		// Also test scheduler specific changes: i.e. Queue recovery-
		// CSQueue/FSQueue/FifoQueue recovery respectively.
		// Test Strategy: send 3 container recovery reports(AMContainer, running
		// container, completed container) on NM re-registration, check the states of
		// SchedulerAttempt, SchedulerNode etc. are updated accordingly.
		/// <exception cref="System.Exception"/>
		public virtual void TestSchedulerRecovery()
		{
			conf.SetBoolean(CapacitySchedulerConfiguration.EnableUserMetrics, true);
			conf.Set(CapacitySchedulerConfiguration.ResourceCalculatorClass, typeof(DominantResourceCalculator
				).FullName);
			int containerMemory = 1024;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource containerResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(containerMemory, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			// clear queue metrics
			rm1.ClearQueueMetrics(app1);
			// Re-start RM
			rm2 = new MockRM(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			// recover app
			RMApp recoveredApp1 = rm2.GetRMContext().GetRMApps()[app1.GetApplicationId()];
			RMAppAttempt loadedAttempt1 = recoveredApp1.GetCurrentAppAttempt();
			NMContainerStatus amContainer = TestRMRestart.CreateNMContainerStatus(am1.GetApplicationAttemptId
				(), 1, ContainerState.Running);
			NMContainerStatus runningContainer = TestRMRestart.CreateNMContainerStatus(am1.GetApplicationAttemptId
				(), 2, ContainerState.Running);
			NMContainerStatus completedContainer = TestRMRestart.CreateNMContainerStatus(am1.
				GetApplicationAttemptId(), 3, ContainerState.Complete);
			nm1.RegisterNode(Arrays.AsList(amContainer, runningContainer, completedContainer)
				, null);
			// Wait for RM to settle down on recovering containers;
			WaitForNumContainersToRecover(2, rm2, am1.GetApplicationAttemptId());
			ICollection<ContainerId> launchedContainers = ((RMNodeImpl)rm2.GetRMContext().GetRMNodes
				()[nm1.GetNodeId()]).GetLaunchedContainers();
			NUnit.Framework.Assert.IsTrue(launchedContainers.Contains(amContainer.GetContainerId
				()));
			NUnit.Framework.Assert.IsTrue(launchedContainers.Contains(runningContainer.GetContainerId
				()));
			// check RMContainers are re-recreated and the container state is correct.
			rm2.WaitForState(nm1, amContainer.GetContainerId(), RMContainerState.Running);
			rm2.WaitForState(nm1, runningContainer.GetContainerId(), RMContainerState.Running
				);
			rm2.WaitForContainerToComplete(loadedAttempt1, completedContainer);
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm2.GetResourceScheduler
				();
			SchedulerNode schedulerNode1 = scheduler.GetSchedulerNode(nm1.GetNodeId());
			NUnit.Framework.Assert.IsTrue("SchedulerNode#toString is not in expected format", 
				schedulerNode1.ToString().Contains(schedulerNode1.GetAvailableResource().ToString
				()));
			NUnit.Framework.Assert.IsTrue("SchedulerNode#toString is not in expected format", 
				schedulerNode1.ToString().Contains(schedulerNode1.GetUsedResource().ToString()));
			// ********* check scheduler node state.*******
			// 2 running containers.
			Org.Apache.Hadoop.Yarn.Api.Records.Resource usedResources = Resources.Multiply(containerResource
				, 2);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource nmResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(nm1.GetMemory(), nm1.GetvCores());
			NUnit.Framework.Assert.IsTrue(schedulerNode1.IsValidContainer(amContainer.GetContainerId
				()));
			NUnit.Framework.Assert.IsTrue(schedulerNode1.IsValidContainer(runningContainer.GetContainerId
				()));
			NUnit.Framework.Assert.IsFalse(schedulerNode1.IsValidContainer(completedContainer
				.GetContainerId()));
			// 2 launched containers, 1 completed container
			NUnit.Framework.Assert.AreEqual(2, schedulerNode1.GetNumContainers());
			NUnit.Framework.Assert.AreEqual(Resources.Subtract(nmResource, usedResources), schedulerNode1
				.GetAvailableResource());
			NUnit.Framework.Assert.AreEqual(usedResources, schedulerNode1.GetUsedResource());
			Org.Apache.Hadoop.Yarn.Api.Records.Resource availableResources = Resources.Subtract
				(nmResource, usedResources);
			// ***** check queue state based on the underlying scheduler ********
			IDictionary<ApplicationId, SchedulerApplication> schedulerApps = ((AbstractYarnScheduler
				)rm2.GetResourceScheduler()).GetSchedulerApplications();
			SchedulerApplication schedulerApp = schedulerApps[recoveredApp1.GetApplicationId(
				)];
			if (GetSchedulerType() == ParameterizedSchedulerTestBase.SchedulerType.Capacity)
			{
				CheckCSQueue(rm2, schedulerApp, nmResource, nmResource, usedResources, 2);
			}
			else
			{
				CheckFSQueue(rm2, schedulerApp, usedResources, availableResources);
			}
			// *********** check scheduler attempt state.********
			SchedulerApplicationAttempt schedulerAttempt = schedulerApp.GetCurrentAppAttempt(
				);
			NUnit.Framework.Assert.IsTrue(schedulerAttempt.GetLiveContainers().Contains(scheduler
				.GetRMContainer(amContainer.GetContainerId())));
			NUnit.Framework.Assert.IsTrue(schedulerAttempt.GetLiveContainers().Contains(scheduler
				.GetRMContainer(runningContainer.GetContainerId())));
			NUnit.Framework.Assert.AreEqual(schedulerAttempt.GetCurrentConsumption(), usedResources
				);
			// *********** check appSchedulingInfo state ***********
			NUnit.Framework.Assert.AreEqual((1L << 40) + 1L, schedulerAttempt.GetNewContainerId
				());
		}

		/// <exception cref="System.Exception"/>
		private void CheckCSQueue(MockRM rm, SchedulerApplication<SchedulerApplicationAttempt
			> app, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 queueResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource usedResource, int numContainers
			)
		{
			CheckCSLeafQueue(rm, app, clusterResource, queueResource, usedResource, numContainers
				);
			LeafQueue queue = (LeafQueue)app.GetQueue();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource availableResources = Resources.Subtract
				(queueResource, usedResource);
			// ************ check app headroom ****************
			SchedulerApplicationAttempt schedulerAttempt = app.GetCurrentAppAttempt();
			NUnit.Framework.Assert.AreEqual(availableResources, schedulerAttempt.GetHeadroom(
				));
			// ************* check Queue metrics ************
			QueueMetrics queueMetrics = queue.GetMetrics();
			AssertMetrics(queueMetrics, 1, 0, 1, 0, 2, availableResources.GetMemory(), availableResources
				.GetVirtualCores(), usedResource.GetMemory(), usedResource.GetVirtualCores());
			// ************ check user metrics ***********
			QueueMetrics userMetrics = queueMetrics.GetUserMetrics(app.GetUser());
			AssertMetrics(userMetrics, 1, 0, 1, 0, 2, availableResources.GetMemory(), availableResources
				.GetVirtualCores(), usedResource.GetMemory(), usedResource.GetVirtualCores());
		}

		private void CheckCSLeafQueue(MockRM rm, SchedulerApplication<SchedulerApplicationAttempt
			> app, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 queueResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource usedResource, int numContainers
			)
		{
			LeafQueue leafQueue = (LeafQueue)app.GetQueue();
			// assert queue used resources.
			NUnit.Framework.Assert.AreEqual(usedResource, leafQueue.GetUsedResources());
			NUnit.Framework.Assert.AreEqual(numContainers, leafQueue.GetNumContainers());
			ResourceCalculator calc = ((CapacityScheduler)rm.GetResourceScheduler()).GetResourceCalculator
				();
			float usedCapacity = Resources.Divide(calc, clusterResource, usedResource, queueResource
				);
			// assert queue used capacity
			NUnit.Framework.Assert.AreEqual(usedCapacity, leafQueue.GetUsedCapacity(), 1e-8);
			float absoluteUsedCapacity = Resources.Divide(calc, clusterResource, usedResource
				, clusterResource);
			// assert queue absolute capacity
			NUnit.Framework.Assert.AreEqual(absoluteUsedCapacity, leafQueue.GetAbsoluteUsedCapacity
				(), 1e-8);
			// assert user consumed resources.
			NUnit.Framework.Assert.AreEqual(usedResource, leafQueue.GetUser(app.GetUser()).GetUsed
				());
		}

		/// <exception cref="System.Exception"/>
		private void CheckFSQueue(ResourceManager rm, SchedulerApplication schedulerApp, 
			Org.Apache.Hadoop.Yarn.Api.Records.Resource usedResources, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 availableResources)
		{
			// waiting for RM's scheduling apps
			int retry = 0;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource assumedFairShare = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(8192, 8);
			while (true)
			{
				Sharpen.Thread.Sleep(100);
				if (assumedFairShare.Equals(((FairScheduler)rm.GetResourceScheduler()).GetQueueManager
					().GetRootQueue().GetFairShare()))
				{
					break;
				}
				retry++;
				if (retry > 30)
				{
					NUnit.Framework.Assert.Fail("Apps are not scheduled within assumed timeout");
				}
			}
			FairScheduler scheduler = (FairScheduler)rm.GetResourceScheduler();
			FSParentQueue root = scheduler.GetQueueManager().GetRootQueue();
			// ************ check cluster used Resources ********
			NUnit.Framework.Assert.IsTrue(root.GetPolicy() is DominantResourceFairnessPolicy);
			NUnit.Framework.Assert.AreEqual(usedResources, root.GetResourceUsage());
			// ************ check app headroom ****************
			FSAppAttempt schedulerAttempt = (FSAppAttempt)schedulerApp.GetCurrentAppAttempt();
			NUnit.Framework.Assert.AreEqual(availableResources, schedulerAttempt.GetHeadroom(
				));
			// ************ check queue metrics ****************
			QueueMetrics queueMetrics = scheduler.GetRootQueueMetrics();
			AssertMetrics(queueMetrics, 1, 0, 1, 0, 2, availableResources.GetMemory(), availableResources
				.GetVirtualCores(), usedResources.GetMemory(), usedResources.GetVirtualCores());
		}

		// create 3 container reports for AM
		public static IList<NMContainerStatus> CreateNMContainerStatusForApp(MockAM am)
		{
			IList<NMContainerStatus> list = new AList<NMContainerStatus>();
			NMContainerStatus amContainer = TestRMRestart.CreateNMContainerStatus(am.GetApplicationAttemptId
				(), 1, ContainerState.Running);
			NMContainerStatus runningContainer = TestRMRestart.CreateNMContainerStatus(am.GetApplicationAttemptId
				(), 2, ContainerState.Running);
			NMContainerStatus completedContainer = TestRMRestart.CreateNMContainerStatus(am.GetApplicationAttemptId
				(), 3, ContainerState.Complete);
			list.AddItem(amContainer);
			list.AddItem(runningContainer);
			list.AddItem(completedContainer);
			return list;
		}

		private const string R = "Default";

		private const string A = "QueueA";

		private const string B = "QueueB";

		private const string B1 = "QueueB1";

		private const string B2 = "QueueB2";

		private const string QueueDoesntExist = "NoSuchQueue";

		private const string User1 = "user1";

		private const string User2 = "user2";

		//don't ever create the below queue ;-)
		private void SetupQueueConfiguration(CapacitySchedulerConfiguration conf)
		{
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { R });
			string QR = CapacitySchedulerConfiguration.Root + "." + R;
			conf.SetCapacity(QR, 100);
			string QA = QR + "." + A;
			string QB = QR + "." + B;
			conf.SetQueues(QR, new string[] { A, B });
			conf.SetCapacity(QA, 50);
			conf.SetCapacity(QB, 50);
			conf.SetDouble(CapacitySchedulerConfiguration.MaximumApplicationMastersResourcePercent
				, 0.5f);
		}

		private void SetupQueueConfigurationOnlyA(CapacitySchedulerConfiguration conf)
		{
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { R });
			string QR = CapacitySchedulerConfiguration.Root + "." + R;
			conf.SetCapacity(QR, 100);
			string QA = QR + "." + A;
			conf.SetQueues(QR, new string[] { A });
			conf.SetCapacity(QA, 100);
			conf.SetDouble(CapacitySchedulerConfiguration.MaximumApplicationMastersResourcePercent
				, 1.0f);
		}

		private void SetupQueueConfigurationChildOfB(CapacitySchedulerConfiguration conf)
		{
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { R });
			string QR = CapacitySchedulerConfiguration.Root + "." + R;
			conf.SetCapacity(QR, 100);
			string QA = QR + "." + A;
			string QB = QR + "." + B;
			string QB1 = QB + "." + B1;
			string QB2 = QB + "." + B2;
			conf.SetQueues(QR, new string[] { A, B });
			conf.SetCapacity(QA, 50);
			conf.SetCapacity(QB, 50);
			conf.SetQueues(QB, new string[] { B1, B2 });
			conf.SetCapacity(QB1, 50);
			conf.SetCapacity(QB2, 50);
			conf.SetDouble(CapacitySchedulerConfiguration.MaximumApplicationMastersResourcePercent
				, 0.5f);
		}

		// Test CS recovery with multi-level queues and multi-users:
		// 1. setup 2 NMs each with 8GB memory;
		// 2. setup 2 level queues: Default -> (QueueA, QueueB)
		// 3. User1 submits 2 apps on QueueA
		// 4. User2 submits 1 app  on QueueB
		// 5. AM and each container has 1GB memory
		// 6. Restart RM.
		// 7. nm1 re-syncs back containers belong to user1
		// 8. nm2 re-syncs back containers belong to user2.
		// 9. Assert the parent queue and 2 leaf queues state and the metrics.
		// 10. Assert each user's consumption inside the queue.
		/// <exception cref="System.Exception"/>
		public virtual void TestCapacitySchedulerRecovery()
		{
			if (GetSchedulerType() != ParameterizedSchedulerTestBase.SchedulerType.Capacity)
			{
				return;
			}
			conf.SetBoolean(CapacitySchedulerConfiguration.EnableUserMetrics, true);
			conf.Set(CapacitySchedulerConfiguration.ResourceCalculatorClass, typeof(DominantResourceCalculator
				).FullName);
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfiguration(csConf);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(csConf);
			rm1 = new MockRM(csConf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			MockNM nm2 = new MockNM("127.1.1.1:4321", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			nm2.RegisterNode();
			RMApp app1_1 = rm1.SubmitApp(1024, "app1_1", User1, null, A);
			MockAM am1_1 = MockRM.LaunchAndRegisterAM(app1_1, rm1, nm1);
			RMApp app1_2 = rm1.SubmitApp(1024, "app1_2", User1, null, A);
			MockAM am1_2 = MockRM.LaunchAndRegisterAM(app1_2, rm1, nm2);
			RMApp app2 = rm1.SubmitApp(1024, "app2", User2, null, B);
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm1, nm2);
			// clear queue metrics
			rm1.ClearQueueMetrics(app1_1);
			rm1.ClearQueueMetrics(app1_2);
			rm1.ClearQueueMetrics(app2);
			csConf.Set("yarn.scheduler.capacity.root.Default.QueueB.state", "STOPPED");
			// Re-start RM
			rm2 = new MockRM(csConf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			nm2.SetResourceTrackerService(rm2.GetResourceTrackerService());
			IList<NMContainerStatus> am1_1Containers = CreateNMContainerStatusForApp(am1_1);
			IList<NMContainerStatus> am1_2Containers = CreateNMContainerStatusForApp(am1_2);
			Sharpen.Collections.AddAll(am1_1Containers, am1_2Containers);
			nm1.RegisterNode(am1_1Containers, null);
			IList<NMContainerStatus> am2Containers = CreateNMContainerStatusForApp(am2);
			nm2.RegisterNode(am2Containers, null);
			// Wait for RM to settle down on recovering containers;
			WaitForNumContainersToRecover(2, rm2, am1_1.GetApplicationAttemptId());
			WaitForNumContainersToRecover(2, rm2, am1_2.GetApplicationAttemptId());
			WaitForNumContainersToRecover(2, rm2, am1_2.GetApplicationAttemptId());
			// Calculate each queue's resource usage.
			Org.Apache.Hadoop.Yarn.Api.Records.Resource containerResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource nmResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(nm1.GetMemory(), nm1.GetvCores());
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.Multiply(
				nmResource, 2);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource q1Resource = Resources.Multiply(clusterResource
				, 0.5);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource q2Resource = Resources.Multiply(clusterResource
				, 0.5);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource q1UsedResource = Resources.Multiply(containerResource
				, 4);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource q2UsedResource = Resources.Multiply(containerResource
				, 2);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totalUsedResource = Resources.Add(q1UsedResource
				, q2UsedResource);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource q1availableResources = Resources.Subtract
				(q1Resource, q1UsedResource);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource q2availableResources = Resources.Subtract
				(q2Resource, q2UsedResource);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totalAvailableResource = Resources.Add
				(q1availableResources, q2availableResources);
			IDictionary<ApplicationId, SchedulerApplication> schedulerApps = ((AbstractYarnScheduler
				)rm2.GetResourceScheduler()).GetSchedulerApplications();
			SchedulerApplication schedulerApp1_1 = schedulerApps[app1_1.GetApplicationId()];
			// assert queue A state.
			CheckCSLeafQueue(rm2, schedulerApp1_1, clusterResource, q1Resource, q1UsedResource
				, 4);
			QueueMetrics queue1Metrics = schedulerApp1_1.GetQueue().GetMetrics();
			AssertMetrics(queue1Metrics, 2, 0, 2, 0, 4, q1availableResources.GetMemory(), q1availableResources
				.GetVirtualCores(), q1UsedResource.GetMemory(), q1UsedResource.GetVirtualCores()
				);
			// assert queue B state.
			SchedulerApplication schedulerApp2 = schedulerApps[app2.GetApplicationId()];
			CheckCSLeafQueue(rm2, schedulerApp2, clusterResource, q2Resource, q2UsedResource, 
				2);
			QueueMetrics queue2Metrics = schedulerApp2.GetQueue().GetMetrics();
			AssertMetrics(queue2Metrics, 1, 0, 1, 0, 2, q2availableResources.GetMemory(), q2availableResources
				.GetVirtualCores(), q2UsedResource.GetMemory(), q2UsedResource.GetVirtualCores()
				);
			// assert parent queue state.
			LeafQueue leafQueue = (LeafQueue)schedulerApp2.GetQueue();
			ParentQueue parentQueue = (ParentQueue)leafQueue.GetParent();
			CheckParentQueue(parentQueue, 6, totalUsedResource, (float)6 / 16, (float)6 / 16);
			AssertMetrics(parentQueue.GetMetrics(), 3, 0, 3, 0, 6, totalAvailableResource.GetMemory
				(), totalAvailableResource.GetVirtualCores(), totalUsedResource.GetMemory(), totalUsedResource
				.GetVirtualCores());
		}

		/// <exception cref="System.Exception"/>
		private void VerifyAppRecoveryWithWrongQueueConfig(CapacitySchedulerConfiguration
			 csConf, RMApp app, string diagnostics, MemoryRMStateStore memStore, RMStateStore.RMState
			 state)
		{
			// Restart RM with fail-fast as false. App should be killed.
			csConf.SetBoolean(YarnConfiguration.RmFailFast, false);
			rm2 = new MockRM(csConf, memStore);
			rm2.Start();
			// Wait for app to be killed.
			rm2.WaitForState(app.GetApplicationId(), RMAppState.Killed);
			ApplicationReport report = rm2.GetApplicationReport(app.GetApplicationId());
			NUnit.Framework.Assert.AreEqual(report.GetFinalApplicationStatus(), FinalApplicationStatus
				.Killed);
			NUnit.Framework.Assert.AreEqual(report.GetYarnApplicationState(), YarnApplicationState
				.Killed);
			NUnit.Framework.Assert.AreEqual(report.GetDiagnostics(), diagnostics);
			// Remove updated app info(app being KILLED) from state store and reinstate
			// state store to previous state i.e. which indicates app is RUNNING.
			// This is to simulate app recovery with fail fast config as true.
			foreach (KeyValuePair<ApplicationId, ApplicationStateData> entry in state.GetApplicationState
				())
			{
				ApplicationStateData appState = Org.Mockito.Mockito.Mock<ApplicationStateData>();
				ApplicationSubmissionContext ctxt = Org.Mockito.Mockito.Mock<ApplicationSubmissionContext
					>();
				Org.Mockito.Mockito.When(appState.GetApplicationSubmissionContext()).ThenReturn(ctxt
					);
				Org.Mockito.Mockito.When(ctxt.GetApplicationId()).ThenReturn(entry.Key);
				memStore.RemoveApplicationStateInternal(appState);
				memStore.StoreApplicationStateInternal(entry.Key, entry.Value);
			}
			// Now restart RM with fail-fast as true. QueueException should be thrown.
			csConf.SetBoolean(YarnConfiguration.RmFailFast, true);
			MockRM rm = new MockRM(csConf, memStore);
			try
			{
				rm.Start();
				NUnit.Framework.Assert.Fail("QueueException must have been thrown");
			}
			catch (QueueInvalidException)
			{
			}
			finally
			{
				rm.Close();
			}
		}

		//Test behavior of an app if queue is changed from leaf to parent during
		//recovery. Test case does following:
		//1. Add an app to QueueB and start the attempt.
		//2. Add 2 subqueues(QueueB1 and QueueB2) to QueueB, restart the RM, once with
		//   fail fast config as false and once with fail fast as true.
		//3. Verify that app was killed if fail fast is false.
		//4. Verify that QueueException was thrown if fail fast is true.
		/// <exception cref="System.Exception"/>
		public virtual void TestCapacityLeafQueueBecomesParentOnRecovery()
		{
			if (GetSchedulerType() != ParameterizedSchedulerTestBase.SchedulerType.Capacity)
			{
				return;
			}
			conf.SetBoolean(CapacitySchedulerConfiguration.EnableUserMetrics, true);
			conf.Set(CapacitySchedulerConfiguration.ResourceCalculatorClass, typeof(DominantResourceCalculator
				).FullName);
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfiguration(csConf);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(csConf);
			rm1 = new MockRM(csConf, memStore);
			rm1.Start();
			MockNM nm = new MockNM("127.1.1.1:4321", 8192, rm1.GetResourceTrackerService());
			nm.RegisterNode();
			// Submit an app to QueueB.
			RMApp app = rm1.SubmitApp(1024, "app", User2, null, B);
			MockRM.LaunchAndRegisterAM(app, rm1, nm);
			NUnit.Framework.Assert.AreEqual(rm1.GetApplicationReport(app.GetApplicationId()).
				GetYarnApplicationState(), YarnApplicationState.Running);
			// Take a copy of state store so that it can be reset to this state.
			RMStateStore.RMState state = memStore.LoadState();
			// Change scheduler config with child queues added to QueueB.
			csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfigurationChildOfB(csConf);
			string diags = "Application killed on recovery as it was submitted to " + "queue QueueB which is no longer a leaf queue after restart.";
			VerifyAppRecoveryWithWrongQueueConfig(csConf, app, diags, memStore, state);
		}

		//Test behavior of an app if queue is removed during recovery. Test case does
		//following:
		//1. Add some apps to two queues, attempt to add an app to a non-existant
		//   queue to verify that the new logic is not in effect during normal app
		//   submission
		//2. Remove one of the queues, restart the RM, once with fail fast config as
		//   false and once with fail fast as true.
		//3. Verify that app was killed if fail fast is false.
		//4. Verify that QueueException was thrown if fail fast is true.
		/// <exception cref="System.Exception"/>
		public virtual void TestCapacitySchedulerQueueRemovedRecovery()
		{
			if (GetSchedulerType() != ParameterizedSchedulerTestBase.SchedulerType.Capacity)
			{
				return;
			}
			conf.SetBoolean(CapacitySchedulerConfiguration.EnableUserMetrics, true);
			conf.Set(CapacitySchedulerConfiguration.ResourceCalculatorClass, typeof(DominantResourceCalculator
				).FullName);
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfiguration(csConf);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(csConf);
			rm1 = new MockRM(csConf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			MockNM nm2 = new MockNM("127.1.1.1:4321", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			nm2.RegisterNode();
			RMApp app1_1 = rm1.SubmitApp(1024, "app1_1", User1, null, A);
			MockAM am1_1 = MockRM.LaunchAndRegisterAM(app1_1, rm1, nm1);
			RMApp app1_2 = rm1.SubmitApp(1024, "app1_2", User1, null, A);
			MockAM am1_2 = MockRM.LaunchAndRegisterAM(app1_2, rm1, nm2);
			RMApp app2 = rm1.SubmitApp(1024, "app2", User2, null, B);
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm1, nm2);
			NUnit.Framework.Assert.AreEqual(rm1.GetApplicationReport(app2.GetApplicationId())
				.GetYarnApplicationState(), YarnApplicationState.Running);
			//Submit an app with a non existant queue to make sure it does not
			//cause a fatal failure in the non-recovery case
			RMApp appNA = rm1.SubmitApp(1024, "app1_2", User1, null, QueueDoesntExist, false);
			// clear queue metrics
			rm1.ClearQueueMetrics(app1_1);
			rm1.ClearQueueMetrics(app1_2);
			rm1.ClearQueueMetrics(app2);
			// Take a copy of state store so that it can be reset to this state.
			RMStateStore.RMState state = memStore.LoadState();
			// Set new configuration with QueueB removed.
			csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfigurationOnlyA(csConf);
			string diags = "Application killed on recovery as it was submitted to " + "queue QueueB which no longer exists after restart.";
			VerifyAppRecoveryWithWrongQueueConfig(csConf, app2, diags, memStore, state);
		}

		private void CheckParentQueue(ParentQueue parentQueue, int numContainers, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 usedResource, float UsedCapacity, float absoluteUsedCapacity)
		{
			NUnit.Framework.Assert.AreEqual(numContainers, parentQueue.GetNumContainers());
			NUnit.Framework.Assert.AreEqual(usedResource, parentQueue.GetUsedResources());
			NUnit.Framework.Assert.AreEqual(UsedCapacity, parentQueue.GetUsedCapacity(), 1e-8
				);
			NUnit.Framework.Assert.AreEqual(absoluteUsedCapacity, parentQueue.GetAbsoluteUsedCapacity
				(), 1e-8);
		}

		// Test RM shuts down, in the meanwhile, AM fails. Restarted RM scheduler
		// should not recover the containers that belong to the failed AM.
		/// <exception cref="System.Exception"/>
		public virtual void TestAMfailedBetweenRMRestart()
		{
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 0);
			memStore.Init(conf);
			rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			rm2 = new MockRM(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			NMContainerStatus amContainer = TestRMRestart.CreateNMContainerStatus(am1.GetApplicationAttemptId
				(), 1, ContainerState.Complete);
			NMContainerStatus runningContainer = TestRMRestart.CreateNMContainerStatus(am1.GetApplicationAttemptId
				(), 2, ContainerState.Running);
			NMContainerStatus completedContainer = TestRMRestart.CreateNMContainerStatus(am1.
				GetApplicationAttemptId(), 3, ContainerState.Complete);
			nm1.RegisterNode(Arrays.AsList(amContainer, runningContainer, completedContainer)
				, null);
			rm2.WaitForState(am1.GetApplicationAttemptId(), RMAppAttemptState.Failed);
			// Wait for RM to settle down on recovering containers;
			Sharpen.Thread.Sleep(3000);
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm2.GetResourceScheduler
				();
			// Previous AM failed, The failed AM should once again release the
			// just-recovered containers.
			NUnit.Framework.Assert.IsNull(scheduler.GetRMContainer(runningContainer.GetContainerId
				()));
			NUnit.Framework.Assert.IsNull(scheduler.GetRMContainer(completedContainer.GetContainerId
				()));
			rm2.WaitForNewAMToLaunchAndRegister(app1.GetApplicationId(), 2, nm1);
			MockNM nm2 = new MockNM("127.1.1.1:4321", 8192, rm2.GetResourceTrackerService());
			NMContainerStatus previousAttemptContainer = TestRMRestart.CreateNMContainerStatus
				(am1.GetApplicationAttemptId(), 4, ContainerState.Running);
			nm2.RegisterNode(Arrays.AsList(previousAttemptContainer), null);
			// Wait for RM to settle down on recovering containers;
			Sharpen.Thread.Sleep(3000);
			// check containers from previous failed attempt should not be recovered.
			NUnit.Framework.Assert.IsNull(scheduler.GetRMContainer(previousAttemptContainer.GetContainerId
				()));
		}

		// Apps already completed before RM restart. Restarted RM scheduler should not
		// recover containers for completed apps.
		/// <exception cref="System.Exception"/>
		public virtual void TestContainersNotRecoveredForCompletedApps()
		{
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			MockRM.FinishAMAndVerifyAppState(app1, rm1, nm1, am1);
			rm2 = new MockRM(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			NMContainerStatus runningContainer = TestRMRestart.CreateNMContainerStatus(am1.GetApplicationAttemptId
				(), 2, ContainerState.Running);
			NMContainerStatus completedContainer = TestRMRestart.CreateNMContainerStatus(am1.
				GetApplicationAttemptId(), 3, ContainerState.Complete);
			nm1.RegisterNode(Arrays.AsList(runningContainer, completedContainer), null);
			RMApp recoveredApp1 = rm2.GetRMContext().GetRMApps()[app1.GetApplicationId()];
			NUnit.Framework.Assert.AreEqual(RMAppState.Finished, recoveredApp1.GetState());
			// Wait for RM to settle down on recovering containers;
			Sharpen.Thread.Sleep(3000);
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm2.GetResourceScheduler
				();
			// scheduler should not recover containers for finished apps.
			NUnit.Framework.Assert.IsNull(scheduler.GetRMContainer(runningContainer.GetContainerId
				()));
			NUnit.Framework.Assert.IsNull(scheduler.GetRMContainer(completedContainer.GetContainerId
				()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppReregisterOnRMWorkPreservingRestart()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// start RM
			rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = MockRM.LaunchAM(app0, rm1, nm1);
			// Issuing registerAppAttempt() before and after RM restart to confirm
			// registerApplicationMaster() is idempotent.
			am0.RegisterAppAttempt();
			// start new RM
			rm2 = new MockRM(conf, memStore);
			rm2.Start();
			rm2.WaitForState(app0.GetApplicationId(), RMAppState.Accepted);
			rm2.WaitForState(am0.GetApplicationAttemptId(), RMAppAttemptState.Launched);
			am0.SetAMRMProtocol(rm2.GetApplicationMasterService(), rm2.GetRMContext());
			// retry registerApplicationMaster() after RM restart.
			am0.RegisterAppAttempt(true);
			rm2.WaitForState(app0.GetApplicationId(), RMAppState.Running);
			rm2.WaitForState(am0.GetApplicationAttemptId(), RMAppAttemptState.Running);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMContainerStatusWithRMRestart()
		{
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1_1 = rm1.SubmitApp(1024);
			MockAM am1_1 = MockRM.LaunchAndRegisterAM(app1_1, rm1, nm1);
			RMAppAttempt attempt0 = app1_1.GetCurrentAppAttempt();
			AbstractYarnScheduler scheduler = ((AbstractYarnScheduler)rm1.GetResourceScheduler
				());
			NUnit.Framework.Assert.IsTrue(scheduler.GetRMContainer(attempt0.GetMasterContainer
				().GetId()).IsAMContainer());
			// Re-start RM
			rm2 = new MockRM(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			IList<NMContainerStatus> am1_1Containers = CreateNMContainerStatusForApp(am1_1);
			nm1.RegisterNode(am1_1Containers, null);
			// Wait for RM to settle down on recovering containers;
			WaitForNumContainersToRecover(2, rm2, am1_1.GetApplicationAttemptId());
			scheduler = ((AbstractYarnScheduler)rm2.GetResourceScheduler());
			NUnit.Framework.Assert.IsTrue(scheduler.GetRMContainer(attempt0.GetMasterContainer
				().GetId()).IsAMContainer());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecoverSchedulerAppAndAttemptSynchronously()
		{
			// start RM
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = MockRM.LaunchAndRegisterAM(app0, rm1, nm1);
			rm2 = new MockRM(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			// scheduler app/attempt is immediately available after RM is re-started.
			NUnit.Framework.Assert.IsNotNull(rm2.GetResourceScheduler().GetSchedulerAppInfo(am0
				.GetApplicationAttemptId()));
			// getTransferredContainers should not throw NPE.
			((AbstractYarnScheduler)rm2.GetResourceScheduler()).GetTransferredContainers(am0.
				GetApplicationAttemptId());
			IList<NMContainerStatus> containers = CreateNMContainerStatusForApp(am0);
			nm1.RegisterNode(containers, null);
			WaitForNumContainersToRecover(2, rm2, am0.GetApplicationAttemptId());
		}

		// Test if RM on recovery receives the container release request from AM
		// before it receives the container status reported by NM for recovery. this
		// container should not be recovered.
		/// <exception cref="System.Exception"/>
		public virtual void TestReleasedContainerNotRecovered()
		{
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			rm1 = new MockRM(conf, memStore);
			MockNM nm1 = new MockNM("h1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			rm1.Start();
			RMApp app1 = rm1.SubmitApp(1024);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			// Re-start RM
			conf.SetInt(YarnConfiguration.RmNmExpiryIntervalMs, 8000);
			rm2 = new MockRM(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			am1.SetAMRMProtocol(rm2.GetApplicationMasterService(), rm2.GetRMContext());
			am1.RegisterAppAttempt(true);
			// try to release a container before the container is actually recovered.
			ContainerId runningContainer = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 2);
			am1.Allocate(null, Arrays.AsList(runningContainer));
			// send container statuses to recover the containers
			IList<NMContainerStatus> containerStatuses = CreateNMContainerStatusForApp(am1);
			nm1.RegisterNode(containerStatuses, null);
			// only the am container should be recovered.
			WaitForNumContainersToRecover(1, rm2, am1.GetApplicationAttemptId());
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm2.GetResourceScheduler
				();
			// cached release request is cleaned.
			// assertFalse(scheduler.getPendingRelease().contains(runningContainer));
			AllocateResponse response = am1.Allocate(null, null);
			// AM gets notified of the completed container.
			bool receivedCompletedContainer = false;
			foreach (ContainerStatus status in response.GetCompletedContainersStatuses())
			{
				if (status.GetContainerId().Equals(runningContainer))
				{
					receivedCompletedContainer = true;
				}
			}
			NUnit.Framework.Assert.IsTrue(receivedCompletedContainer);
			GenericTestUtils.WaitFor(new _Supplier_955(scheduler, am1, runningContainer), 1000
				, 20000);
		}

		private sealed class _Supplier_955 : Supplier<bool>
		{
			public _Supplier_955(AbstractYarnScheduler scheduler, MockAM am1, ContainerId runningContainer
				)
			{
				this.scheduler = scheduler;
				this.am1 = am1;
				this.runningContainer = runningContainer;
			}

			public bool Get()
			{
				// release cache is cleaned up and previous running container is not
				// recovered
				return scheduler.GetApplicationAttempt(am1.GetApplicationAttemptId()).GetPendingRelease
					().IsEmpty() && scheduler.GetRMContainer(runningContainer) == null;
			}

			private readonly AbstractYarnScheduler scheduler;

			private readonly MockAM am1;

			private readonly ContainerId runningContainer;
		}

		private void AssertMetrics(QueueMetrics qm, int appsSubmitted, int appsPending, int
			 appsRunning, int appsCompleted, int allocatedContainers, int availableMB, int availableVirtualCores
			, int allocatedMB, int allocatedVirtualCores)
		{
			NUnit.Framework.Assert.AreEqual(appsSubmitted, qm.GetAppsSubmitted());
			NUnit.Framework.Assert.AreEqual(appsPending, qm.GetAppsPending());
			NUnit.Framework.Assert.AreEqual(appsRunning, qm.GetAppsRunning());
			NUnit.Framework.Assert.AreEqual(appsCompleted, qm.GetAppsCompleted());
			NUnit.Framework.Assert.AreEqual(allocatedContainers, qm.GetAllocatedContainers());
			NUnit.Framework.Assert.AreEqual(availableMB, qm.GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(availableVirtualCores, qm.GetAvailableVirtualCores
				());
			NUnit.Framework.Assert.AreEqual(allocatedMB, qm.GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(allocatedVirtualCores, qm.GetAllocatedVirtualCores
				());
		}

		/// <exception cref="System.Exception"/>
		public static void WaitForNumContainersToRecover(int num, MockRM rm, ApplicationAttemptId
			 attemptId)
		{
			AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm.GetResourceScheduler(
				);
			SchedulerApplicationAttempt attempt = scheduler.GetApplicationAttempt(attemptId);
			while (attempt == null)
			{
				System.Console.Out.WriteLine("Wait for scheduler attempt " + attemptId + " to be created"
					);
				Sharpen.Thread.Sleep(200);
				attempt = scheduler.GetApplicationAttempt(attemptId);
			}
			while (attempt.GetLiveContainers().Count < num)
			{
				System.Console.Out.WriteLine("Wait for " + num + " containers to recover. currently: "
					 + attempt.GetLiveContainers().Count);
				Sharpen.Thread.Sleep(200);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNewContainersNotAllocatedDuringSchedulerRecovery()
		{
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 4000);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			// Restart RM
			rm2 = new MockRM(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			nm1.RegisterNode();
			ControlledClock clock = new ControlledClock(new SystemClock());
			long startTime = Runtime.CurrentTimeMillis();
			((RMContextImpl)rm2.GetRMContext()).SetSystemClock(clock);
			am1.SetAMRMProtocol(rm2.GetApplicationMasterService(), rm2.GetRMContext());
			am1.RegisterAppAttempt(true);
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Running);
			// AM request for new containers
			am1.Allocate("127.0.0.1", 1000, 1, new AList<ContainerId>());
			IList<Container> containers = new AList<Container>();
			clock.SetTime(startTime + 2000);
			nm1.NodeHeartbeat(true);
			// sleep some time as allocation happens asynchronously.
			Sharpen.Thread.Sleep(3000);
			Sharpen.Collections.AddAll(containers, am1.Allocate(new AList<ResourceRequest>(), 
				new AList<ContainerId>()).GetAllocatedContainers());
			// container is not allocated during scheduling recovery.
			NUnit.Framework.Assert.IsTrue(containers.IsEmpty());
			clock.SetTime(startTime + 8000);
			nm1.NodeHeartbeat(true);
			// Container is created after recovery is done.
			while (containers.IsEmpty())
			{
				Sharpen.Collections.AddAll(containers, am1.Allocate(new AList<ResourceRequest>(), 
					new AList<ContainerId>()).GetAllocatedContainers());
				Sharpen.Thread.Sleep(500);
			}
		}

		/// <summary>
		/// Testing to confirm that retried finishApplicationMaster() doesn't throw
		/// InvalidApplicationMasterRequest before and after RM restart.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRetriedFinishApplicationMasterRequest()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// start RM
			rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = MockRM.LaunchAM(app0, rm1, nm1);
			am0.RegisterAppAttempt();
			// Emulating following a scenario:
			// RM1 saves the app in RMStateStore and then crashes,
			// FinishApplicationMasterResponse#isRegistered still return false,
			// so AM still retry the 2nd RM
			MockRM.FinishAMAndVerifyAppState(app0, rm1, nm1, am0);
			// start new RM
			rm2 = new MockRM(conf, memStore);
			rm2.Start();
			am0.SetAMRMProtocol(rm2.GetApplicationMasterService(), rm2.GetRMContext());
			am0.UnregisterAppAttempt(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppFailedToRenewTokenOnRecovery()
		{
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			UserGroupInformation.SetConfiguration(conf);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			MockRM rm1 = new TestRMRestart.TestSecurityMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			MockRM rm2 = new _TestSecurityMockRM_1107(conf, memStore);
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			rm2.Start();
			NMContainerStatus containerStatus = TestRMRestart.CreateNMContainerStatus(am1.GetApplicationAttemptId
				(), 1, ContainerState.Running);
			nm1.RegisterNode(Arrays.AsList(containerStatus), null);
			// am re-register
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			am1.SetAMRMProtocol(rm2.GetApplicationMasterService(), rm2.GetRMContext());
			am1.RegisterAppAttempt(true);
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Running);
			// Because the token expired, am could crash.
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Complete);
			rm2.WaitForState(am1.GetApplicationAttemptId(), RMAppAttemptState.Failed);
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Failed);
		}

		private sealed class _TestSecurityMockRM_1107 : TestRMRestart.TestSecurityMockRM
		{
			public _TestSecurityMockRM_1107(Configuration baseArg1, RMStateStore baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			protected internal override DelegationTokenRenewer CreateDelegationTokenRenewer()
			{
				return new _DelegationTokenRenewer_1109();
			}

			private sealed class _DelegationTokenRenewer_1109 : DelegationTokenRenewer
			{
				public _DelegationTokenRenewer_1109()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public override void AddApplicationSync(ApplicationId applicationId, Credentials 
					ts, bool shouldCancelAtEnd, string user)
				{
					throw new IOException("Token renew failed !!");
				}
			}
		}

		/// <summary>
		/// Test validateAndCreateResourceRequest fails on recovery, app should ignore
		/// this Exception and continue
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAppFailToValidateResourceRequestOnRecovery()
		{
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8192, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			// Change the config so that validateAndCreateResourceRequest throws
			// exception on recovery
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, 50);
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, 100);
			rm2 = new MockRM(conf, memStore);
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			rm2.Start();
		}
	}
}
