/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Lang.Time;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestContainerResourceUsage
	{
		private YarnConfiguration conf;

		/// <exception cref="Sharpen.UnknownHostException"/>
		[SetUp]
		public virtual void Setup()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			conf = new YarnConfiguration();
			UserGroupInformation.SetConfiguration(conf);
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUsageWithOneAttemptAndOneContainer()
		{
			MockRM rm = new MockRM(conf);
			rm.Start();
			MockNM nm = new MockNM("127.0.0.1:1234", 15120, rm.GetResourceTrackerService());
			nm.RegisterNode();
			RMApp app0 = rm.SubmitApp(200);
			RMAppMetrics rmAppMetrics = app0.GetRMAppMetrics();
			NUnit.Framework.Assert.IsTrue("Before app submittion, memory seconds should have been 0 but was "
				 + rmAppMetrics.GetMemorySeconds(), rmAppMetrics.GetMemorySeconds() == 0);
			NUnit.Framework.Assert.IsTrue("Before app submission, vcore seconds should have been 0 but was "
				 + rmAppMetrics.GetVcoreSeconds(), rmAppMetrics.GetVcoreSeconds() == 0);
			RMAppAttempt attempt0 = app0.GetCurrentAppAttempt();
			nm.NodeHeartbeat(true);
			MockAM am0 = rm.SendAMLaunched(attempt0.GetAppAttemptId());
			am0.RegisterAppAttempt();
			RMContainer rmContainer = rm.GetResourceScheduler().GetRMContainer(attempt0.GetMasterContainer
				().GetId());
			// Allow metrics to accumulate.
			int sleepInterval = 1000;
			int cumulativeSleepTime = 0;
			while (rmAppMetrics.GetMemorySeconds() <= 0 && cumulativeSleepTime < 5000)
			{
				Sharpen.Thread.Sleep(sleepInterval);
				cumulativeSleepTime += sleepInterval;
			}
			rmAppMetrics = app0.GetRMAppMetrics();
			NUnit.Framework.Assert.IsTrue("While app is running, memory seconds should be >0 but is "
				 + rmAppMetrics.GetMemorySeconds(), rmAppMetrics.GetMemorySeconds() > 0);
			NUnit.Framework.Assert.IsTrue("While app is running, vcore seconds should be >0 but is "
				 + rmAppMetrics.GetVcoreSeconds(), rmAppMetrics.GetVcoreSeconds() > 0);
			MockRM.FinishAMAndVerifyAppState(app0, rm, nm, am0);
			AggregateAppResourceUsage ru = CalculateContainerResourceMetrics(rmContainer);
			rmAppMetrics = app0.GetRMAppMetrics();
			NUnit.Framework.Assert.AreEqual("Unexcpected MemorySeconds value", ru.GetMemorySeconds
				(), rmAppMetrics.GetMemorySeconds());
			NUnit.Framework.Assert.AreEqual("Unexpected VcoreSeconds value", ru.GetVcoreSeconds
				(), rmAppMetrics.GetVcoreSeconds());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUsageWithMultipleContainersAndRMRestart()
		{
			// Set max attempts to 1 so that when the first attempt fails, the app
			// won't try to start a new one.
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			conf.SetBoolean(YarnConfiguration.RecoveryEnabled, true);
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, false);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			MockRM rm0 = new MockRM(conf, memStore);
			rm0.Start();
			MockNM nm = new MockNM("127.0.0.1:1234", 65536, rm0.GetResourceTrackerService());
			nm.RegisterNode();
			RMApp app0 = rm0.SubmitApp(200);
			rm0.WaitForState(app0.GetApplicationId(), RMAppState.Accepted);
			RMAppAttempt attempt0 = app0.GetCurrentAppAttempt();
			ApplicationAttemptId attemptId0 = attempt0.GetAppAttemptId();
			rm0.WaitForState(attemptId0, RMAppAttemptState.Scheduled);
			nm.NodeHeartbeat(true);
			rm0.WaitForState(attemptId0, RMAppAttemptState.Allocated);
			MockAM am0 = rm0.SendAMLaunched(attempt0.GetAppAttemptId());
			am0.RegisterAppAttempt();
			int NumContainers = 2;
			am0.Allocate("127.0.0.1", 1000, NumContainers, new AList<ContainerId>());
			nm.NodeHeartbeat(true);
			IList<Container> conts = am0.Allocate(new AList<ResourceRequest>(), new AList<ContainerId
				>()).GetAllocatedContainers();
			while (conts.Count != NumContainers)
			{
				nm.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(conts, am0.Allocate(new AList<ResourceRequest>(), new 
					AList<ContainerId>()).GetAllocatedContainers());
				Sharpen.Thread.Sleep(500);
			}
			// launch the 2nd and 3rd containers.
			foreach (Container c in conts)
			{
				nm.NodeHeartbeat(attempt0.GetAppAttemptId(), c.GetId().GetContainerId(), ContainerState
					.Running);
				rm0.WaitForState(nm, c.GetId(), RMContainerState.Running);
			}
			// Get the RMContainers for all of the live containers, to be used later
			// for metrics calculations and comparisons.
			ICollection<RMContainer> rmContainers = rm0.scheduler.GetSchedulerAppInfo(attempt0
				.GetAppAttemptId()).GetLiveContainers();
			// Allow metrics to accumulate.
			int sleepInterval = 1000;
			int cumulativeSleepTime = 0;
			while (app0.GetRMAppMetrics().GetMemorySeconds() <= 0 && cumulativeSleepTime < 5000
				)
			{
				Sharpen.Thread.Sleep(sleepInterval);
				cumulativeSleepTime += sleepInterval;
			}
			// Stop all non-AM containers
			foreach (Container c_1 in conts)
			{
				if (c_1.GetId().GetContainerId() == 1)
				{
					continue;
				}
				nm.NodeHeartbeat(attempt0.GetAppAttemptId(), c_1.GetId().GetContainerId(), ContainerState
					.Complete);
				rm0.WaitForState(nm, c_1.GetId(), RMContainerState.Completed);
			}
			// After all other containers have completed, manually complete the master
			// container in order to trigger a save to the state store of the resource
			// usage metrics. This will cause the attempt to fail, and, since the max
			// attempt retries is 1, the app will also fail. This is intentional so
			// that all containers will complete prior to saving.
			ContainerId cId = ContainerId.NewContainerId(attempt0.GetAppAttemptId(), 1);
			nm.NodeHeartbeat(attempt0.GetAppAttemptId(), cId.GetContainerId(), ContainerState
				.Complete);
			rm0.WaitForState(nm, cId, RMContainerState.Completed);
			// Check that the container metrics match those from the app usage report.
			long memorySeconds = 0;
			long vcoreSeconds = 0;
			foreach (RMContainer c_2 in rmContainers)
			{
				AggregateAppResourceUsage ru = CalculateContainerResourceMetrics(c_2);
				memorySeconds += ru.GetMemorySeconds();
				vcoreSeconds += ru.GetVcoreSeconds();
			}
			RMAppMetrics metricsBefore = app0.GetRMAppMetrics();
			NUnit.Framework.Assert.AreEqual("Unexcpected MemorySeconds value", memorySeconds, 
				metricsBefore.GetMemorySeconds());
			NUnit.Framework.Assert.AreEqual("Unexpected VcoreSeconds value", vcoreSeconds, metricsBefore
				.GetVcoreSeconds());
			// create new RM to represent RM restart. Load up the state store.
			MockRM rm1 = new MockRM(conf, memStore);
			rm1.Start();
			RMApp app0After = rm1.GetRMContext().GetRMApps()[app0.GetApplicationId()];
			// Compare container resource usage metrics from before and after restart.
			RMAppMetrics metricsAfter = app0After.GetRMAppMetrics();
			NUnit.Framework.Assert.AreEqual("Vcore seconds were not the same after RM Restart"
				, metricsBefore.GetVcoreSeconds(), metricsAfter.GetVcoreSeconds());
			NUnit.Framework.Assert.AreEqual("Memory seconds were not the same after RM Restart"
				, metricsBefore.GetMemorySeconds(), metricsAfter.GetMemorySeconds());
			rm0.Stop();
			rm0.Close();
			rm1.Stop();
			rm1.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUsageAfterAMRestartWithMultipleContainers()
		{
			AmRestartTests(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUsageAfterAMRestartKeepContainers()
		{
			AmRestartTests(true);
		}

		/// <exception cref="System.Exception"/>
		private void AmRestartTests(bool keepRunningContainers)
		{
			MockRM rm = new MockRM(conf);
			rm.Start();
			RMApp app = rm.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", -1, null, "MAPREDUCE", false, keepRunningContainers
				);
			MockNM nm = new MockNM("127.0.0.1:1234", 10240, rm.GetResourceTrackerService());
			nm.RegisterNode();
			MockAM am0 = MockRM.LaunchAndRegisterAM(app, rm, nm);
			int NumContainers = 1;
			// allocate NUM_CONTAINERS containers
			am0.Allocate("127.0.0.1", 1024, NumContainers, new AList<ContainerId>());
			nm.NodeHeartbeat(true);
			// wait for containers to be allocated.
			IList<Container> containers = am0.Allocate(new AList<ResourceRequest>(), new AList
				<ContainerId>()).GetAllocatedContainers();
			while (containers.Count != NumContainers)
			{
				nm.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(containers, am0.Allocate(new AList<ResourceRequest>(), 
					new AList<ContainerId>()).GetAllocatedContainers());
				Sharpen.Thread.Sleep(200);
			}
			// launch the 2nd container.
			ContainerId containerId2 = ContainerId.NewContainerId(am0.GetApplicationAttemptId
				(), 2);
			nm.NodeHeartbeat(am0.GetApplicationAttemptId(), containerId2.GetContainerId(), ContainerState
				.Running);
			rm.WaitForState(nm, containerId2, RMContainerState.Running);
			// Capture the containers here so the metrics can be calculated after the
			// app has completed.
			ICollection<RMContainer> rmContainers = rm.scheduler.GetSchedulerAppInfo(am0.GetApplicationAttemptId
				()).GetLiveContainers();
			// fail the first app attempt by sending CONTAINER_FINISHED event without
			// registering.
			ContainerId amContainerId = app.GetCurrentAppAttempt().GetMasterContainer().GetId
				();
			nm.NodeHeartbeat(am0.GetApplicationAttemptId(), amContainerId.GetContainerId(), ContainerState
				.Complete);
			am0.WaitForState(RMAppAttemptState.Failed);
			long memorySeconds = 0;
			long vcoreSeconds = 0;
			// Calculate container usage metrics for first attempt.
			if (keepRunningContainers)
			{
				// Only calculate the usage for the one container that has completed.
				foreach (RMContainer c in rmContainers)
				{
					if (c.GetContainerId().Equals(amContainerId))
					{
						AggregateAppResourceUsage ru = CalculateContainerResourceMetrics(c);
						memorySeconds += ru.GetMemorySeconds();
						vcoreSeconds += ru.GetVcoreSeconds();
					}
					else
					{
						// The remaining container should be RUNNING.
						NUnit.Framework.Assert.IsTrue("After first attempt failed, remaining container " 
							+ "should still be running. ", c.GetContainerState().Equals(ContainerState.Running
							));
					}
				}
			}
			else
			{
				// If keepRunningContainers is false, all live containers should now
				// be completed. Calculate the resource usage metrics for all of them.
				foreach (RMContainer c in rmContainers)
				{
					AggregateAppResourceUsage ru = CalculateContainerResourceMetrics(c);
					memorySeconds += ru.GetMemorySeconds();
					vcoreSeconds += ru.GetVcoreSeconds();
				}
			}
			// wait for app to start a new attempt.
			rm.WaitForState(app.GetApplicationId(), RMAppState.Accepted);
			// assert this is a new AM.
			RMAppAttempt attempt2 = app.GetCurrentAppAttempt();
			NUnit.Framework.Assert.IsFalse(attempt2.GetAppAttemptId().Equals(am0.GetApplicationAttemptId
				()));
			// launch the new AM
			nm.NodeHeartbeat(true);
			MockAM am1 = rm.SendAMLaunched(attempt2.GetAppAttemptId());
			am1.RegisterAppAttempt();
			// allocate NUM_CONTAINERS containers
			am1.Allocate("127.0.0.1", 1024, NumContainers, new AList<ContainerId>());
			nm.NodeHeartbeat(true);
			// wait for containers to be allocated.
			containers = am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>())
				.GetAllocatedContainers();
			while (containers.Count != NumContainers)
			{
				nm.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(containers, am1.Allocate(new AList<ResourceRequest>(), 
					new AList<ContainerId>()).GetAllocatedContainers());
				Sharpen.Thread.Sleep(200);
			}
			rm.WaitForState(app.GetApplicationId(), RMAppState.Running);
			// Capture running containers for later use by metrics calculations.
			rmContainers = rm.scheduler.GetSchedulerAppInfo(attempt2.GetAppAttemptId()).GetLiveContainers
				();
			// complete container by sending the container complete event which has
			// earlier attempt's attemptId
			amContainerId = app.GetCurrentAppAttempt().GetMasterContainer().GetId();
			nm.NodeHeartbeat(am0.GetApplicationAttemptId(), amContainerId.GetContainerId(), ContainerState
				.Complete);
			MockRM.FinishAMAndVerifyAppState(app, rm, nm, am1);
			// Calculate container usage metrics for second attempt.
			foreach (RMContainer c_1 in rmContainers)
			{
				AggregateAppResourceUsage ru = CalculateContainerResourceMetrics(c_1);
				memorySeconds += ru.GetMemorySeconds();
				vcoreSeconds += ru.GetVcoreSeconds();
			}
			RMAppMetrics rmAppMetrics = app.GetRMAppMetrics();
			NUnit.Framework.Assert.AreEqual("Unexcpected MemorySeconds value", memorySeconds, 
				rmAppMetrics.GetMemorySeconds());
			NUnit.Framework.Assert.AreEqual("Unexpected VcoreSeconds value", vcoreSeconds, rmAppMetrics
				.GetVcoreSeconds());
			rm.Stop();
			return;
		}

		private AggregateAppResourceUsage CalculateContainerResourceMetrics(RMContainer rmContainer
			)
		{
			Resource resource = rmContainer.GetContainer().GetResource();
			long usedMillis = rmContainer.GetFinishTime() - rmContainer.GetCreationTime();
			long memorySeconds = resource.GetMemory() * usedMillis / DateUtils.MillisPerSecond;
			long vcoreSeconds = resource.GetVirtualCores() * usedMillis / DateUtils.MillisPerSecond;
			return new AggregateAppResourceUsage(memorySeconds, vcoreSeconds);
		}
	}
}
