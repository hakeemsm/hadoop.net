using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Applicationsmanager
{
	public class TestAMRestart
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestAMRestartWithExistingContainers()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 2);
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			RMApp app1 = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", -1, null, "MAPREDUCE", false, true);
			MockNM nm1 = new MockNM("127.0.0.1:1234", 10240, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			MockNM nm2 = new MockNM("127.0.0.1:2351", 4089, rm1.GetResourceTrackerService());
			nm2.RegisterNode();
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			int NumContainers = 3;
			// allocate NUM_CONTAINERS containers
			am1.Allocate("127.0.0.1", 1024, NumContainers, new AList<ContainerId>());
			nm1.NodeHeartbeat(true);
			// wait for containers to be allocated.
			IList<Container> containers = am1.Allocate(new AList<ResourceRequest>(), new AList
				<ContainerId>()).GetAllocatedContainers();
			while (containers.Count != NumContainers)
			{
				nm1.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(containers, am1.Allocate(new AList<ResourceRequest>(), 
					new AList<ContainerId>()).GetAllocatedContainers());
				Sharpen.Thread.Sleep(200);
			}
			// launch the 2nd container, for testing running container transferred.
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 2, ContainerState.Running);
			ContainerId containerId2 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 2);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Running);
			// launch the 3rd container, for testing container allocated by previous
			// attempt is completed by the next new attempt/
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 3, ContainerState.Running);
			ContainerId containerId3 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 3);
			rm1.WaitForState(nm1, containerId3, RMContainerState.Running);
			// 4th container still in AQUIRED state. for testing Acquired container is
			// always killed.
			ContainerId containerId4 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 4);
			rm1.WaitForState(nm1, containerId4, RMContainerState.Acquired);
			// 5th container is in Allocated state. for testing allocated container is
			// always killed.
			am1.Allocate("127.0.0.1", 1024, 1, new AList<ContainerId>());
			nm1.NodeHeartbeat(true);
			ContainerId containerId5 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 5);
			rm1.WaitForContainerAllocated(nm1, containerId5);
			rm1.WaitForState(nm1, containerId5, RMContainerState.Allocated);
			// 6th container is in Reserved state.
			am1.Allocate("127.0.0.1", 6000, 1, new AList<ContainerId>());
			ContainerId containerId6 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 6);
			nm1.NodeHeartbeat(true);
			SchedulerApplicationAttempt schedulerAttempt = ((AbstractYarnScheduler)rm1.GetResourceScheduler
				()).GetCurrentAttemptForContainer(containerId6);
			while (schedulerAttempt.GetReservedContainers().IsEmpty())
			{
				System.Console.Out.WriteLine("Waiting for container " + containerId6 + " to be reserved."
					);
				nm1.NodeHeartbeat(true);
				Sharpen.Thread.Sleep(200);
			}
			// assert containerId6 is reserved.
			NUnit.Framework.Assert.AreEqual(containerId6, schedulerAttempt.GetReservedContainers
				()[0].GetContainerId());
			// fail the AM by sending CONTAINER_FINISHED event without registering.
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am1.WaitForState(RMAppAttemptState.Failed);
			// wait for some time. previous AM's running containers should still remain
			// in scheduler even though am failed
			Sharpen.Thread.Sleep(3000);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Running);
			// acquired/allocated containers are cleaned up.
			NUnit.Framework.Assert.IsNull(rm1.GetResourceScheduler().GetRMContainer(containerId4
				));
			NUnit.Framework.Assert.IsNull(rm1.GetResourceScheduler().GetRMContainer(containerId5
				));
			// wait for app to start a new attempt.
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			// assert this is a new AM.
			ApplicationAttemptId newAttemptId = app1.GetCurrentAppAttempt().GetAppAttemptId();
			NUnit.Framework.Assert.IsFalse(newAttemptId.Equals(am1.GetApplicationAttemptId())
				);
			// launch the new AM
			RMAppAttempt attempt2 = app1.GetCurrentAppAttempt();
			nm1.NodeHeartbeat(true);
			MockAM am2 = rm1.SendAMLaunched(attempt2.GetAppAttemptId());
			RegisterApplicationMasterResponse registerResponse = am2.RegisterAppAttempt();
			// Assert two containers are running: container2 and container3;
			NUnit.Framework.Assert.AreEqual(2, registerResponse.GetContainersFromPreviousAttempts
				().Count);
			bool containerId2Exists = false;
			bool containerId3Exists = false;
			foreach (Container container in registerResponse.GetContainersFromPreviousAttempts
				())
			{
				if (container.GetId().Equals(containerId2))
				{
					containerId2Exists = true;
				}
				if (container.GetId().Equals(containerId3))
				{
					containerId3Exists = true;
				}
			}
			NUnit.Framework.Assert.IsTrue(containerId2Exists && containerId3Exists);
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Running);
			// complete container by sending the container complete event which has earlier
			// attempt's attemptId
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 3, ContainerState.Complete);
			// Even though the completed container containerId3 event was sent to the
			// earlier failed attempt, new RMAppAttempt can also capture this container
			// info.
			// completed containerId4 is also transferred to the new attempt.
			RMAppAttempt newAttempt = app1.GetRMAppAttempt(am2.GetApplicationAttemptId());
			// 4 containers finished, acquired/allocated/reserved/completed.
			WaitForContainersToFinish(4, newAttempt);
			bool container3Exists = false;
			bool container4Exists = false;
			bool container5Exists = false;
			bool container6Exists = false;
			foreach (ContainerStatus status in newAttempt.GetJustFinishedContainers())
			{
				if (status.GetContainerId().Equals(containerId3))
				{
					// containerId3 is the container ran by previous attempt but finished by the
					// new attempt.
					container3Exists = true;
				}
				if (status.GetContainerId().Equals(containerId4))
				{
					// containerId4 is the Acquired Container killed by the previous attempt,
					// it's now inside new attempt's finished container list.
					container4Exists = true;
				}
				if (status.GetContainerId().Equals(containerId5))
				{
					// containerId5 is the Allocated container killed by previous failed attempt.
					container5Exists = true;
				}
				if (status.GetContainerId().Equals(containerId6))
				{
					// containerId6 is the reserved container killed by previous failed attempt.
					container6Exists = true;
				}
			}
			NUnit.Framework.Assert.IsTrue(container3Exists && container4Exists && container5Exists
				 && container6Exists);
			// New SchedulerApplicationAttempt also has the containers info.
			rm1.WaitForState(nm1, containerId2, RMContainerState.Running);
			// record the scheduler attempt for testing.
			SchedulerApplicationAttempt schedulerNewAttempt = ((AbstractYarnScheduler)rm1.GetResourceScheduler
				()).GetCurrentAttemptForContainer(containerId2);
			// finish this application
			MockRM.FinishAMAndVerifyAppState(app1, rm1, nm1, am2);
			// the 2nd attempt released the 1st attempt's running container, when the
			// 2nd attempt finishes.
			NUnit.Framework.Assert.IsFalse(schedulerNewAttempt.GetLiveContainers().Contains(containerId2
				));
			// all 4 normal containers finished.
			System.Console.Out.WriteLine("New attempt's just finished containers: " + newAttempt
				.GetJustFinishedContainers());
			WaitForContainersToFinish(5, newAttempt);
			rm1.Stop();
		}

		/// <exception cref="System.Exception"/>
		private void WaitForContainersToFinish(int expectedNum, RMAppAttempt attempt)
		{
			int count = 0;
			while (attempt.GetJustFinishedContainers().Count < expectedNum && count < 500)
			{
				Sharpen.Thread.Sleep(100);
				count++;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNMTokensRebindOnAMRestart()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 3);
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			RMApp app1 = rm1.SubmitApp(200, "myname", "myuser", new Dictionary<ApplicationAccessType
				, string>(), false, "default", -1, null, "MAPREDUCE", false, true);
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8000, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			MockNM nm2 = new MockNM("127.1.1.1:4321", 8000, rm1.GetResourceTrackerService());
			nm2.RegisterNode();
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			IList<Container> containers = new AList<Container>();
			// nmTokens keeps track of all the nmTokens issued in the allocate call.
			IList<NMToken> expectedNMTokens = new AList<NMToken>();
			// am1 allocate 2 container on nm1.
			// first container
			while (true)
			{
				AllocateResponse response = am1.Allocate("127.0.0.1", 2000, 2, new AList<ContainerId
					>());
				nm1.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(containers, response.GetAllocatedContainers());
				Sharpen.Collections.AddAll(expectedNMTokens, response.GetNMTokens());
				if (containers.Count == 2)
				{
					break;
				}
				Sharpen.Thread.Sleep(200);
				System.Console.Out.WriteLine("Waiting for container to be allocated.");
			}
			// launch the container-2
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 2, ContainerState.Running);
			ContainerId containerId2 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 2);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Running);
			// launch the container-3
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 3, ContainerState.Running);
			ContainerId containerId3 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 3);
			rm1.WaitForState(nm1, containerId3, RMContainerState.Running);
			// fail am1
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am1.WaitForState(RMAppAttemptState.Failed);
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			// restart the am
			MockAM am2 = MockRM.LaunchAM(app1, rm1, nm1);
			RegisterApplicationMasterResponse registerResponse = am2.RegisterAppAttempt();
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Running);
			// check am2 get the nm token from am1.
			NUnit.Framework.Assert.AreEqual(expectedNMTokens, registerResponse.GetNMTokensFromPreviousAttempts
				());
			// am2 allocate 1 container on nm2
			containers = new AList<Container>();
			while (true)
			{
				AllocateResponse allocateResponse = am2.Allocate("127.1.1.1", 4000, 1, new AList<
					ContainerId>());
				nm2.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(containers, allocateResponse.GetAllocatedContainers());
				Sharpen.Collections.AddAll(expectedNMTokens, allocateResponse.GetNMTokens());
				if (containers.Count == 1)
				{
					break;
				}
				Sharpen.Thread.Sleep(200);
				System.Console.Out.WriteLine("Waiting for container to be allocated.");
			}
			nm1.NodeHeartbeat(am2.GetApplicationAttemptId(), 2, ContainerState.Running);
			ContainerId am2ContainerId2 = ContainerId.NewContainerId(am2.GetApplicationAttemptId
				(), 2);
			rm1.WaitForState(nm1, am2ContainerId2, RMContainerState.Running);
			// fail am2.
			nm1.NodeHeartbeat(am2.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am2.WaitForState(RMAppAttemptState.Failed);
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			// restart am
			MockAM am3 = MockRM.LaunchAM(app1, rm1, nm1);
			registerResponse = am3.RegisterAppAttempt();
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Running);
			// check am3 get the NM token from both am1 and am2;
			IList<NMToken> transferredTokens = registerResponse.GetNMTokensFromPreviousAttempts
				();
			NUnit.Framework.Assert.AreEqual(2, transferredTokens.Count);
			NUnit.Framework.Assert.IsTrue(transferredTokens.ContainsAll(expectedNMTokens));
			rm1.Stop();
		}

		// AM container preempted, nm disk failure
		// should not be counted towards AM max retry count.
		/// <exception cref="System.Exception"/>
		public virtual void TestShouldNotCountFailureToMaxAttemptRetry()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			// explicitly set max-am-retry count as 1.
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			conf.SetBoolean(YarnConfiguration.RecoveryEnabled, true);
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8000, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			CapacityScheduler scheduler = (CapacityScheduler)rm1.GetResourceScheduler();
			ContainerId amContainer = ContainerId.NewContainerId(am1.GetApplicationAttemptId(
				), 1);
			// Preempt the first attempt;
			scheduler.KillContainer(scheduler.GetRMContainer(amContainer));
			am1.WaitForState(RMAppAttemptState.Failed);
			NUnit.Framework.Assert.IsTrue(!attempt1.ShouldCountTowardsMaxAttemptRetry());
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			ApplicationStateData appState = memStore.GetState().GetApplicationState()[app1.GetApplicationId
				()];
			// AM should be restarted even though max-am-attempt is 1.
			MockAM am2 = rm1.WaitForNewAMToLaunchAndRegister(app1.GetApplicationId(), 2, nm1);
			RMAppAttempt attempt2 = app1.GetCurrentAppAttempt();
			NUnit.Framework.Assert.IsTrue(((RMAppAttemptImpl)attempt2).MayBeLastAttempt());
			// Preempt the second attempt.
			ContainerId amContainer2 = ContainerId.NewContainerId(am2.GetApplicationAttemptId
				(), 1);
			scheduler.KillContainer(scheduler.GetRMContainer(amContainer2));
			am2.WaitForState(RMAppAttemptState.Failed);
			NUnit.Framework.Assert.IsTrue(!attempt2.ShouldCountTowardsMaxAttemptRetry());
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			MockAM am3 = rm1.WaitForNewAMToLaunchAndRegister(app1.GetApplicationId(), 3, nm1);
			RMAppAttempt attempt3 = app1.GetCurrentAppAttempt();
			NUnit.Framework.Assert.IsTrue(((RMAppAttemptImpl)attempt3).MayBeLastAttempt());
			// mimic NM disk_failure
			ContainerStatus containerStatus = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ContainerStatus
				>();
			containerStatus.SetContainerId(attempt3.GetMasterContainer().GetId());
			containerStatus.SetDiagnostics("mimic NM disk_failure");
			containerStatus.SetState(ContainerState.Complete);
			containerStatus.SetExitStatus(ContainerExitStatus.DisksFailed);
			IDictionary<ApplicationId, IList<ContainerStatus>> conts = new Dictionary<ApplicationId
				, IList<ContainerStatus>>();
			conts[app1.GetApplicationId()] = Sharpen.Collections.SingletonList(containerStatus
				);
			nm1.NodeHeartbeat(conts, true);
			am3.WaitForState(RMAppAttemptState.Failed);
			NUnit.Framework.Assert.IsTrue(!attempt3.ShouldCountTowardsMaxAttemptRetry());
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.DisksFailed, appState.GetAttempt
				(am3.GetApplicationAttemptId()).GetAMContainerExitStatus());
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			MockAM am4 = rm1.WaitForNewAMToLaunchAndRegister(app1.GetApplicationId(), 4, nm1);
			RMAppAttempt attempt4 = app1.GetCurrentAppAttempt();
			NUnit.Framework.Assert.IsTrue(((RMAppAttemptImpl)attempt4).MayBeLastAttempt());
			// create second NM, and register to rm1
			MockNM nm2 = new MockNM("127.0.0.1:2234", 8000, rm1.GetResourceTrackerService());
			nm2.RegisterNode();
			// nm1 heartbeats to report unhealthy
			// This will mimic ContainerExitStatus.ABORT
			nm1.NodeHeartbeat(false);
			am4.WaitForState(RMAppAttemptState.Failed);
			NUnit.Framework.Assert.IsTrue(!attempt4.ShouldCountTowardsMaxAttemptRetry());
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Aborted, appState.GetAttempt(
				am4.GetApplicationAttemptId()).GetAMContainerExitStatus());
			// launch next AM in nm2
			nm2.NodeHeartbeat(true);
			MockAM am5 = rm1.WaitForNewAMToLaunchAndRegister(app1.GetApplicationId(), 5, nm2);
			RMAppAttempt attempt5 = app1.GetCurrentAppAttempt();
			NUnit.Framework.Assert.IsTrue(((RMAppAttemptImpl)attempt5).MayBeLastAttempt());
			// fail the AM normally
			nm2.NodeHeartbeat(am5.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am5.WaitForState(RMAppAttemptState.Failed);
			NUnit.Framework.Assert.IsTrue(attempt5.ShouldCountTowardsMaxAttemptRetry());
			// AM should not be restarted.
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Failed);
			NUnit.Framework.Assert.AreEqual(5, app1.GetAppAttempts().Count);
			rm1.Stop();
		}

		// Test RM restarts after AM container is preempted, new RM should not count
		// AM preemption failure towards the max-retry-account and should be able to
		// re-launch the AM.
		/// <exception cref="System.Exception"/>
		public virtual void TestPreemptedAMRestartOnRMRestart()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			conf.SetBoolean(YarnConfiguration.RecoveryEnabled, true);
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, false);
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			// explicitly set max-am-retry count as 1.
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8000, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			CapacityScheduler scheduler = (CapacityScheduler)rm1.GetResourceScheduler();
			ContainerId amContainer = ContainerId.NewContainerId(am1.GetApplicationAttemptId(
				), 1);
			// Forcibly preempt the am container;
			scheduler.KillContainer(scheduler.GetRMContainer(amContainer));
			am1.WaitForState(RMAppAttemptState.Failed);
			NUnit.Framework.Assert.IsTrue(!attempt1.ShouldCountTowardsMaxAttemptRetry());
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			// state store has 1 attempt stored.
			ApplicationStateData appState = memStore.GetState().GetApplicationState()[app1.GetApplicationId
				()];
			NUnit.Framework.Assert.AreEqual(1, appState.GetAttemptCount());
			// attempt stored has the preempted container exit status.
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Preempted, appState.GetAttempt
				(am1.GetApplicationAttemptId()).GetAMContainerExitStatus());
			// Restart rm.
			MockRM rm2 = new MockRM(conf, memStore);
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			nm1.RegisterNode();
			rm2.Start();
			// Restarted RM should re-launch the am.
			MockAM am2 = rm2.WaitForNewAMToLaunchAndRegister(app1.GetApplicationId(), 2, nm1);
			MockRM.FinishAMAndVerifyAppState(app1, rm2, nm1, am2);
			RMAppAttempt attempt2 = rm2.GetRMContext().GetRMApps()[app1.GetApplicationId()].GetCurrentAppAttempt
				();
			NUnit.Framework.Assert.IsTrue(attempt2.ShouldCountTowardsMaxAttemptRetry());
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Invalid, appState.GetAttempt(
				am2.GetApplicationAttemptId()).GetAMContainerExitStatus());
			rm1.Stop();
			rm2.Stop();
		}

		// Test regular RM restart/failover, new RM should not count
		// AM failure towards the max-retry-account and should be able to
		// re-launch the AM.
		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartOrFailoverNotCountedForAMFailures()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			conf.SetBoolean(YarnConfiguration.RecoveryEnabled, true);
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, false);
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			// explicitly set max-am-retry count as 1.
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8000, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200);
			// AM should be restarted even though max-am-attempt is 1.
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			NUnit.Framework.Assert.IsTrue(((RMAppAttemptImpl)attempt1).MayBeLastAttempt());
			// Restart rm.
			MockRM rm2 = new MockRM(conf, memStore);
			rm2.Start();
			ApplicationStateData appState = memStore.GetState().GetApplicationState()[app1.GetApplicationId
				()];
			// re-register the NM
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			NMContainerStatus status = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NMContainerStatus
				>();
			status.SetContainerExitStatus(ContainerExitStatus.KilledByResourcemanager);
			status.SetContainerId(attempt1.GetMasterContainer().GetId());
			status.SetContainerState(ContainerState.Complete);
			status.SetDiagnostics(string.Empty);
			nm1.RegisterNode(Sharpen.Collections.SingletonList(status), null);
			rm2.WaitForState(attempt1.GetAppAttemptId(), RMAppAttemptState.Failed);
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.KilledByResourcemanager, appState
				.GetAttempt(am1.GetApplicationAttemptId()).GetAMContainerExitStatus());
			// Will automatically start a new AppAttempt in rm2
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			MockAM am2 = rm2.WaitForNewAMToLaunchAndRegister(app1.GetApplicationId(), 2, nm1);
			MockRM.FinishAMAndVerifyAppState(app1, rm2, nm1, am2);
			RMAppAttempt attempt3 = rm2.GetRMContext().GetRMApps()[app1.GetApplicationId()].GetCurrentAppAttempt
				();
			NUnit.Framework.Assert.IsTrue(attempt3.ShouldCountTowardsMaxAttemptRetry());
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Invalid, appState.GetAttempt(
				am2.GetApplicationAttemptId()).GetAMContainerExitStatus());
			rm1.Stop();
			rm2.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMAppAttemptFailuresValidityInterval()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			conf.SetBoolean(YarnConfiguration.RecoveryEnabled, true);
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, false);
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			// explicitly set max-am-retry count as 2.
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 2);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 8000, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// set window size to a larger number : 20s
			// we will verify the app should be failed if
			// two continuous attempts failed in 20s.
			RMApp app = rm1.SubmitApp(200, 20000);
			MockAM am = MockRM.LaunchAM(app, rm1, nm1);
			// Fail current attempt normally
			nm1.NodeHeartbeat(am.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am.WaitForState(RMAppAttemptState.Failed);
			// launch the second attempt
			rm1.WaitForState(app.GetApplicationId(), RMAppState.Accepted);
			NUnit.Framework.Assert.AreEqual(2, app.GetAppAttempts().Count);
			NUnit.Framework.Assert.IsTrue(((RMAppAttemptImpl)app.GetCurrentAppAttempt()).MayBeLastAttempt
				());
			MockAM am_2 = MockRM.LaunchAndRegisterAM(app, rm1, nm1);
			am_2.WaitForState(RMAppAttemptState.Running);
			nm1.NodeHeartbeat(am_2.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am_2.WaitForState(RMAppAttemptState.Failed);
			// current app should be failed.
			rm1.WaitForState(app.GetApplicationId(), RMAppState.Failed);
			ControlledClock clock = new ControlledClock(new SystemClock());
			// set window size to 6s
			RMAppImpl app1 = (RMAppImpl)rm1.SubmitApp(200, 6000);
			app1.SetSystemClock(clock);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			// Fail attempt1 normally
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am1.WaitForState(RMAppAttemptState.Failed);
			// launch the second attempt
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			NUnit.Framework.Assert.AreEqual(2, app1.GetAppAttempts().Count);
			RMAppAttempt attempt2 = app1.GetCurrentAppAttempt();
			NUnit.Framework.Assert.IsTrue(((RMAppAttemptImpl)attempt2).MayBeLastAttempt());
			MockAM am2 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			am2.WaitForState(RMAppAttemptState.Running);
			// wait for 6 seconds
			clock.SetTime(Runtime.CurrentTimeMillis() + 6 * 1000);
			// Fail attempt2 normally
			nm1.NodeHeartbeat(am2.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am2.WaitForState(RMAppAttemptState.Failed);
			// can launch the third attempt successfully
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			NUnit.Framework.Assert.AreEqual(3, app1.GetAppAttempts().Count);
			RMAppAttempt attempt3 = app1.GetCurrentAppAttempt();
			clock.Reset();
			MockAM am3 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			am3.WaitForState(RMAppAttemptState.Running);
			// Restart rm.
			MockRM rm2 = new MockRM(conf, memStore);
			rm2.Start();
			// re-register the NM
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			NMContainerStatus status = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NMContainerStatus
				>();
			status.SetContainerExitStatus(ContainerExitStatus.KilledByResourcemanager);
			status.SetContainerId(attempt3.GetMasterContainer().GetId());
			status.SetContainerState(ContainerState.Complete);
			status.SetDiagnostics(string.Empty);
			nm1.RegisterNode(Sharpen.Collections.SingletonList(status), null);
			rm2.WaitForState(attempt3.GetAppAttemptId(), RMAppAttemptState.Failed);
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			// Lauch Attempt 4
			MockAM am4 = rm2.WaitForNewAMToLaunchAndRegister(app1.GetApplicationId(), 4, nm1);
			// wait for 6 seconds
			clock.SetTime(Runtime.CurrentTimeMillis() + 6 * 1000);
			// Fail attempt4 normally
			nm1.NodeHeartbeat(am4.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am4.WaitForState(RMAppAttemptState.Failed);
			// can launch the 5th attempt successfully
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			MockAM am5 = rm2.WaitForNewAMToLaunchAndRegister(app1.GetApplicationId(), 5, nm1);
			clock.Reset();
			am5.WaitForState(RMAppAttemptState.Running);
			// Fail attempt5 normally
			nm1.NodeHeartbeat(am5.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am5.WaitForState(RMAppAttemptState.Failed);
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Failed);
			rm1.Stop();
			rm2.Stop();
		}
	}
}
