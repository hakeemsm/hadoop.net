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
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestRMRestart : ParameterizedSchedulerTestBase
	{
		private static readonly FilePath TempDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), "decommision");

		private FilePath hostFile = new FilePath(TempDir + FilePath.separator + "hostFile.txt"
			);

		private YarnConfiguration conf;

		private static IPEndPoint rmAddr;

		private IList<MockRM> rms = new AList<MockRM>();

		public TestRMRestart(ParameterizedSchedulerTestBase.SchedulerType type)
			: base(type)
		{
		}

		// Fake rmAddr for token-renewal
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = GetConf();
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			UserGroupInformation.SetConfiguration(conf);
			conf.SetBoolean(YarnConfiguration.RecoveryEnabled, true);
			conf.SetBoolean(YarnConfiguration.RmWorkPreservingRecoveryEnabled, false);
			conf.Set(YarnConfiguration.RmStore, typeof(MemoryRMStateStore).FullName);
			rmAddr = new IPEndPoint("localhost", 8032);
			NUnit.Framework.Assert.IsTrue(YarnConfiguration.DefaultRmAmMaxAttempts > 1);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			foreach (MockRM rm in rms)
			{
				rm.Stop();
			}
			rms.Clear();
			TempDir.Delete();
		}

		/// <returns>a new MockRM that will be stopped at the end of the test.</returns>
		private MockRM CreateMockRM(YarnConfiguration conf, RMStateStore store)
		{
			MockRM rm = new MockRM(conf, store);
			rms.AddItem(rm);
			return rm;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestart()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			// PHASE 1: create state in an RM
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			// start like normal because state is empty
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			MockNM nm2 = new MockNM("127.0.0.2:5678", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			nm2.RegisterNode();
			// nm2 will not heartbeat with RM1
			// create app that will finish and the final state should be saved.
			RMApp app0 = rm1.SubmitApp(200);
			RMAppAttempt attempt0 = app0.GetCurrentAppAttempt();
			// spot check that app is saved
			NUnit.Framework.Assert.AreEqual(1, rmAppState.Count);
			nm1.NodeHeartbeat(true);
			MockAM am0 = rm1.SendAMLaunched(attempt0.GetAppAttemptId());
			am0.RegisterAppAttempt();
			FinishApplicationMaster(app0, rm1, nm1, am0);
			// create app that gets launched and does allocate before RM restart
			RMApp app1 = rm1.SubmitApp(200);
			// assert app1 info is saved
			ApplicationStateData appState = rmAppState[app1.GetApplicationId()];
			NUnit.Framework.Assert.IsNotNull(appState);
			NUnit.Framework.Assert.AreEqual(0, appState.GetAttemptCount());
			NUnit.Framework.Assert.AreEqual(appState.GetApplicationSubmissionContext().GetApplicationId
				(), app1.GetApplicationSubmissionContext().GetApplicationId());
			//kick the scheduling to allocate AM container
			nm1.NodeHeartbeat(true);
			// assert app1 attempt is saved
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			ApplicationAttemptId attemptId1 = attempt1.GetAppAttemptId();
			rm1.WaitForState(attemptId1, RMAppAttemptState.Allocated);
			NUnit.Framework.Assert.AreEqual(1, appState.GetAttemptCount());
			ApplicationAttemptStateData attemptState = appState.GetAttempt(attemptId1);
			NUnit.Framework.Assert.IsNotNull(attemptState);
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewContainerId(attemptId1, 1), attemptState
				.GetMasterContainer().GetId());
			// launch the AM
			MockAM am1 = rm1.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			// AM request for containers
			am1.Allocate("127.0.0.1", 1000, 1, new AList<ContainerId>());
			// kick the scheduler
			nm1.NodeHeartbeat(true);
			IList<Container> conts = am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId
				>()).GetAllocatedContainers();
			while (conts.Count == 0)
			{
				nm1.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(conts, am1.Allocate(new AList<ResourceRequest>(), new 
					AList<ContainerId>()).GetAllocatedContainers());
				Sharpen.Thread.Sleep(500);
			}
			// create app that does not get launched by RM before RM restart
			RMApp app2 = rm1.SubmitApp(200);
			// assert app2 info is saved
			appState = rmAppState[app2.GetApplicationId()];
			NUnit.Framework.Assert.IsNotNull(appState);
			NUnit.Framework.Assert.AreEqual(0, appState.GetAttemptCount());
			NUnit.Framework.Assert.AreEqual(appState.GetApplicationSubmissionContext().GetApplicationId
				(), app2.GetApplicationSubmissionContext().GetApplicationId());
			// create unmanaged app
			RMApp appUnmanaged = rm1.SubmitApp(200, "someApp", "someUser", null, true, null, 
				conf.GetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				), null);
			ApplicationAttemptId unmanagedAttemptId = appUnmanaged.GetCurrentAppAttempt().GetAppAttemptId
				();
			// assert appUnmanaged info is saved
			ApplicationId unmanagedAppId = appUnmanaged.GetApplicationId();
			appState = rmAppState[unmanagedAppId];
			NUnit.Framework.Assert.IsNotNull(appState);
			// wait for attempt to reach LAUNCHED state 
			rm1.WaitForState(unmanagedAttemptId, RMAppAttemptState.Launched);
			rm1.WaitForState(unmanagedAppId, RMAppState.Accepted);
			// assert unmanaged attempt info is saved
			NUnit.Framework.Assert.AreEqual(1, appState.GetAttemptCount());
			NUnit.Framework.Assert.AreEqual(appState.GetApplicationSubmissionContext().GetApplicationId
				(), appUnmanaged.GetApplicationSubmissionContext().GetApplicationId());
			// PHASE 2: create new RM and start from old state
			// create new RM to represent restart and recover state
			MockRM rm2 = CreateMockRM(conf, memStore);
			// start new RM
			rm2.Start();
			// change NM to point to new RM
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			nm2.SetResourceTrackerService(rm2.GetResourceTrackerService());
			// verify load of old state
			// 4 apps are loaded.
			// FINISHED app and attempt is also loaded back.
			// Unmanaged app state is still loaded back but it cannot be restarted by
			// the RM. this will change with work preserving RM restart in which AMs/NMs
			// are not rebooted.
			NUnit.Framework.Assert.AreEqual(4, rm2.GetRMContext().GetRMApps().Count);
			// check that earlier finished app and attempt is also loaded back and move
			// to finished state.
			rm2.WaitForState(app0.GetApplicationId(), RMAppState.Finished);
			rm2.WaitForState(am0.GetApplicationAttemptId(), RMAppAttemptState.Finished);
			// verify correct number of attempts and other data
			RMApp loadedApp1 = rm2.GetRMContext().GetRMApps()[app1.GetApplicationId()];
			NUnit.Framework.Assert.IsNotNull(loadedApp1);
			NUnit.Framework.Assert.AreEqual(1, loadedApp1.GetAppAttempts().Count);
			NUnit.Framework.Assert.AreEqual(app1.GetApplicationSubmissionContext().GetApplicationId
				(), loadedApp1.GetApplicationSubmissionContext().GetApplicationId());
			RMApp loadedApp2 = rm2.GetRMContext().GetRMApps()[app2.GetApplicationId()];
			NUnit.Framework.Assert.IsNotNull(loadedApp2);
			//Assert.assertEquals(0, loadedApp2.getAppAttempts().size());
			NUnit.Framework.Assert.AreEqual(app2.GetApplicationSubmissionContext().GetApplicationId
				(), loadedApp2.GetApplicationSubmissionContext().GetApplicationId());
			// verify state machine kicked into expected states
			rm2.WaitForState(loadedApp1.GetApplicationId(), RMAppState.Accepted);
			rm2.WaitForState(loadedApp2.GetApplicationId(), RMAppState.Accepted);
			// verify attempts for apps
			// The app for which AM was started will wait for previous am
			// container finish event to arrive. However for an application for which
			// no am container was running will start new application attempt.
			NUnit.Framework.Assert.AreEqual(1, loadedApp1.GetAppAttempts().Count);
			NUnit.Framework.Assert.AreEqual(1, loadedApp2.GetAppAttempts().Count);
			// verify old AM is not accepted
			// change running AM to talk to new RM
			am1.SetAMRMProtocol(rm2.GetApplicationMasterService(), rm2.GetRMContext());
			try
			{
				am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>());
				NUnit.Framework.Assert.Fail();
			}
			catch (ApplicationAttemptNotFoundException e)
			{
				NUnit.Framework.Assert.IsTrue(e is ApplicationAttemptNotFoundException);
			}
			// NM should be rebooted on heartbeat, even first heartbeat for nm2
			NodeHeartbeatResponse hbResponse = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(NodeAction.Resync, hbResponse.GetNodeAction());
			hbResponse = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(NodeAction.Resync, hbResponse.GetNodeAction());
			// new NM to represent NM re-register
			nm1 = new MockNM("127.0.0.1:1234", 15120, rm2.GetResourceTrackerService());
			nm2 = new MockNM("127.0.0.2:5678", 15120, rm2.GetResourceTrackerService());
			NMContainerStatus status = Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestRMRestart
				.CreateNMContainerStatus(loadedApp1.GetCurrentAppAttempt().GetAppAttemptId(), 1, 
				ContainerState.Complete);
			nm1.RegisterNode(Arrays.AsList(status), null);
			nm2.RegisterNode();
			rm2.WaitForState(loadedApp1.GetApplicationId(), RMAppState.Accepted);
			// wait for the 2nd attempt to be started.
			int timeoutSecs = 0;
			while (loadedApp1.GetAppAttempts().Count != 2 && timeoutSecs++ < 40)
			{
				Sharpen.Thread.Sleep(200);
			}
			// verify no more reboot response sent
			hbResponse = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Resync != hbResponse.GetNodeAction());
			hbResponse = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Resync != hbResponse.GetNodeAction());
			// assert app1 attempt is saved
			attempt1 = loadedApp1.GetCurrentAppAttempt();
			attemptId1 = attempt1.GetAppAttemptId();
			rm2.WaitForState(attemptId1, RMAppAttemptState.Allocated);
			appState = rmAppState[loadedApp1.GetApplicationId()];
			attemptState = appState.GetAttempt(attemptId1);
			NUnit.Framework.Assert.IsNotNull(attemptState);
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewContainerId(attemptId1, 1), attemptState
				.GetMasterContainer().GetId());
			// Nodes on which the AM's run 
			MockNM am1Node = nm1;
			if (attemptState.GetMasterContainer().GetNodeId().ToString().Contains("127.0.0.2"
				))
			{
				am1Node = nm2;
			}
			// assert app2 attempt is saved
			RMAppAttempt attempt2 = loadedApp2.GetCurrentAppAttempt();
			ApplicationAttemptId attemptId2 = attempt2.GetAppAttemptId();
			rm2.WaitForState(attemptId2, RMAppAttemptState.Allocated);
			appState = rmAppState[loadedApp2.GetApplicationId()];
			attemptState = appState.GetAttempt(attemptId2);
			NUnit.Framework.Assert.IsNotNull(attemptState);
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewContainerId(attemptId2, 1), attemptState
				.GetMasterContainer().GetId());
			MockNM am2Node = nm1;
			if (attemptState.GetMasterContainer().GetNodeId().ToString().Contains("127.0.0.2"
				))
			{
				am2Node = nm2;
			}
			// start the AM's
			am1 = rm2.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			MockAM am2 = rm2.SendAMLaunched(attempt2.GetAppAttemptId());
			am2.RegisterAppAttempt();
			//request for containers
			am1.Allocate("127.0.0.1", 1000, 3, new AList<ContainerId>());
			am2.Allocate("127.0.0.2", 1000, 1, new AList<ContainerId>());
			// verify container allocate continues to work
			nm1.NodeHeartbeat(true);
			nm2.NodeHeartbeat(true);
			conts = am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>()).GetAllocatedContainers
				();
			while (conts.Count == 0)
			{
				nm1.NodeHeartbeat(true);
				nm2.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(conts, am1.Allocate(new AList<ResourceRequest>(), new 
					AList<ContainerId>()).GetAllocatedContainers());
				Sharpen.Thread.Sleep(500);
			}
			// finish the AMs
			FinishApplicationMaster(loadedApp1, rm2, am1Node, am1);
			FinishApplicationMaster(loadedApp2, rm2, am2Node, am2);
			// stop RM's
			rm2.Stop();
			rm1.Stop();
			// completed apps are not removed immediately after app finish
			// And finished app is also loaded back.
			NUnit.Framework.Assert.AreEqual(4, rmAppState.Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartAppRunningAMFailed()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", -1, null, "MAPREDUCE", true, true);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			// fail the AM by sending CONTAINER_FINISHED event without registering.
			nm1.NodeHeartbeat(am0.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am0.WaitForState(RMAppAttemptState.Failed);
			ApplicationStateData appState = rmAppState[app0.GetApplicationId()];
			// assert the AM failed state is saved.
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, appState.GetAttempt(am0
				.GetApplicationAttemptId()).GetState());
			// assert app state has not been saved.
			NUnit.Framework.Assert.IsNull(rmAppState[app0.GetApplicationId()].GetState());
			// new AM started but not registered, app still stays at ACCECPTED state.
			rm1.WaitForState(app0.GetApplicationId(), RMAppState.Accepted);
			// start new RM
			MockRM rm2 = CreateMockRM(conf, memStore);
			rm2.Start();
			// assert the previous AM state is loaded back on RM recovery.
			rm2.WaitForState(am0.GetApplicationAttemptId(), RMAppAttemptState.Failed);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartWaitForPreviousAMToFinish()
		{
			// testing 3 cases
			// After RM restarts
			// 1) New application attempt is not started until previous AM container
			// finish event is reported back to RM as a part of nm registration.
			// 2) If previous AM container finish event is never reported back (i.e.
			// node manager on which this AM container was running also went down) in
			// that case AMLivenessMonitor should time out previous attempt and start
			// new attempt.
			// 3) If all the stored attempts had finished then new attempt should
			// be started immediately.
			YarnConfiguration conf = new YarnConfiguration(this.conf);
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 40);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 16382, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// submitting app
			RMApp app1 = rm1.SubmitApp(200);
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			MockAM am1 = LaunchAM(app1, rm1, nm1);
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Complete);
			// Fail first AM.
			am1.WaitForState(RMAppAttemptState.Failed);
			// launch another AM.
			MockAM am2 = LaunchAM(app1, rm1, nm1);
			NUnit.Framework.Assert.AreEqual(1, rmAppState.Count);
			NUnit.Framework.Assert.AreEqual(app1.GetState(), RMAppState.Running);
			NUnit.Framework.Assert.AreEqual(app1.GetAppAttempts()[app1.GetCurrentAppAttempt()
				.GetAppAttemptId()].GetAppAttemptState(), RMAppAttemptState.Running);
			//  start new RM.
			MockRM rm2 = CreateMockRM(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			NodeHeartbeatResponse res = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(NodeAction.Resync, res.GetNodeAction());
			RMApp rmApp = rm2.GetRMContext().GetRMApps()[app1.GetApplicationId()];
			// application should be in ACCEPTED state
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			NUnit.Framework.Assert.AreEqual(RMAppState.Accepted, rmApp.GetState());
			// new attempt should not be started
			NUnit.Framework.Assert.AreEqual(2, rmApp.GetAppAttempts().Count);
			// am1 attempt should be in FAILED state where as am2 attempt should be in
			// LAUNCHED state
			rm2.WaitForState(am1.GetApplicationAttemptId(), RMAppAttemptState.Failed);
			rm2.WaitForState(am2.GetApplicationAttemptId(), RMAppAttemptState.Launched);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, rmApp.GetAppAttempts()[
				am1.GetApplicationAttemptId()].GetAppAttemptState());
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Launched, rmApp.GetAppAttempts(
				)[am2.GetApplicationAttemptId()].GetAppAttemptState());
			NMContainerStatus status = Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestRMRestart
				.CreateNMContainerStatus(am2.GetApplicationAttemptId(), 1, ContainerState.Complete
				);
			nm1.RegisterNode(Arrays.AsList(status), null);
			rm2.WaitForState(am2.GetApplicationAttemptId(), RMAppAttemptState.Failed);
			LaunchAM(rmApp, rm2, nm1);
			NUnit.Framework.Assert.AreEqual(3, rmApp.GetAppAttempts().Count);
			rm2.WaitForState(rmApp.GetCurrentAppAttempt().GetAppAttemptId(), RMAppAttemptState
				.Running);
			// Now restart RM ...
			// Setting AMLivelinessMonitor interval to be 10 Secs. 
			conf.SetInt(YarnConfiguration.RmAmExpiryIntervalMs, 10000);
			MockRM rm3 = CreateMockRM(conf, memStore);
			rm3.Start();
			// Wait for RM to process all the events as a part of rm recovery.
			nm1.SetResourceTrackerService(rm3.GetResourceTrackerService());
			rmApp = rm3.GetRMContext().GetRMApps()[app1.GetApplicationId()];
			// application should be in ACCEPTED state
			rm3.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			NUnit.Framework.Assert.AreEqual(rmApp.GetState(), RMAppState.Accepted);
			// new attempt should not be started
			NUnit.Framework.Assert.AreEqual(3, rmApp.GetAppAttempts().Count);
			// am1 and am2 attempts should be in FAILED state where as am3 should be
			// in LAUNCHED state
			rm3.WaitForState(am1.GetApplicationAttemptId(), RMAppAttemptState.Failed);
			rm3.WaitForState(am2.GetApplicationAttemptId(), RMAppAttemptState.Failed);
			ApplicationAttemptId latestAppAttemptId = rmApp.GetCurrentAppAttempt().GetAppAttemptId
				();
			rm3.WaitForState(latestAppAttemptId, RMAppAttemptState.Launched);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, rmApp.GetAppAttempts()[
				am1.GetApplicationAttemptId()].GetAppAttemptState());
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, rmApp.GetAppAttempts()[
				am2.GetApplicationAttemptId()].GetAppAttemptState());
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Launched, rmApp.GetAppAttempts(
				)[latestAppAttemptId].GetAppAttemptState());
			rm3.WaitForState(latestAppAttemptId, RMAppAttemptState.Failed);
			rm3.WaitForState(rmApp.GetApplicationId(), RMAppState.Accepted);
			NUnit.Framework.Assert.AreEqual(4, rmApp.GetAppAttempts().Count);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, rmApp.GetAppAttempts()[
				latestAppAttemptId].GetAppAttemptState());
			latestAppAttemptId = rmApp.GetCurrentAppAttempt().GetAppAttemptId();
			// The 4th attempt has started but is not yet saved into RMStateStore
			// It will be saved only when we launch AM.
			// submitting app but not starting AM for it.
			RMApp app2 = rm3.SubmitApp(200);
			rm3.WaitForState(app2.GetApplicationId(), RMAppState.Accepted);
			NUnit.Framework.Assert.AreEqual(1, app2.GetAppAttempts().Count);
			NUnit.Framework.Assert.AreEqual(0, memStore.GetState().GetApplicationState()[app2
				.GetApplicationId()].GetAttemptCount());
			MockRM rm4 = CreateMockRM(conf, memStore);
			rm4.Start();
			rmApp = rm4.GetRMContext().GetRMApps()[app1.GetApplicationId()];
			rm4.WaitForState(rmApp.GetApplicationId(), RMAppState.Accepted);
			// wait for the attempt to be created.
			int timeoutSecs = 0;
			while (rmApp.GetAppAttempts().Count != 2 && timeoutSecs++ < 40)
			{
				Sharpen.Thread.Sleep(200);
			}
			NUnit.Framework.Assert.AreEqual(4, rmApp.GetAppAttempts().Count);
			NUnit.Framework.Assert.AreEqual(RMAppState.Accepted, rmApp.GetState());
			rm4.WaitForState(latestAppAttemptId, RMAppAttemptState.Scheduled);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Scheduled, rmApp.GetAppAttempts
				()[latestAppAttemptId].GetAppAttemptState());
			// The initial application for which an AM was not started should be in
			// ACCEPTED state with one application attempt started.
			app2 = rm4.GetRMContext().GetRMApps()[app2.GetApplicationId()];
			rm4.WaitForState(app2.GetApplicationId(), RMAppState.Accepted);
			NUnit.Framework.Assert.AreEqual(RMAppState.Accepted, app2.GetState());
			NUnit.Framework.Assert.AreEqual(1, app2.GetAppAttempts().Count);
			rm4.WaitForState(app2.GetCurrentAppAttempt().GetAppAttemptId(), RMAppAttemptState
				.Scheduled);
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Scheduled, app2.GetCurrentAppAttempt
				().GetAppAttemptState());
		}

		// Test RM restarts after previous attempt succeeded and was saved into state
		// store but before the RMAppAttempt notifies RMApp that it has succeeded. On
		// recovery, RMAppAttempt should send the AttemptFinished event to RMApp so
		// that RMApp can recover its state.
		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartWaitForPreviousSucceededAttempt()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 2);
			MemoryRMStateStore memStore = new _MemoryRMStateStore_644();
			// do nothing; simulate app final state is not saved.
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("127.0.0.1:1234", 15120);
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = MockRM.LaunchAndRegisterAM(app0, rm1, nm1);
			FinishApplicationMasterRequest req = FinishApplicationMasterRequest.NewInstance(FinalApplicationStatus
				.Succeeded, string.Empty, string.Empty);
			am0.UnregisterAppAttempt(req, true);
			am0.WaitForState(RMAppAttemptState.Finishing);
			// app final state is not saved. This guarantees that RMApp cannot be
			// recovered via its own saved state, but only via the event notification
			// from the RMAppAttempt on recovery.
			NUnit.Framework.Assert.IsNull(rmAppState[app0.GetApplicationId()].GetState());
			// start RM
			MockRM rm2 = CreateMockRM(conf, memStore);
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			rm2.Start();
			rm2.WaitForState(app0.GetCurrentAppAttempt().GetAppAttemptId(), RMAppAttemptState
				.Finished);
			rm2.WaitForState(app0.GetApplicationId(), RMAppState.Finished);
			// app final state is saved via the finish event from attempt.
			NUnit.Framework.Assert.AreEqual(RMAppState.Finished, rmAppState[app0.GetApplicationId
				()].GetState());
		}

		private sealed class _MemoryRMStateStore_644 : MemoryRMStateStore
		{
			public _MemoryRMStateStore_644()
			{
				this.count = 0;
			}

			internal int count;

			/// <exception cref="System.Exception"/>
			protected internal override void UpdateApplicationStateInternal(ApplicationId appId
				, ApplicationStateData appStateData)
			{
				if (this.count == 0)
				{
					RMStateStore.Log.Info(appId + " final state is not saved.");
					this.count++;
				}
				else
				{
					base.UpdateApplicationStateInternal(appId, appStateData);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartFailedApp()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			// fail the AM by sending CONTAINER_FINISHED event without registering.
			nm1.NodeHeartbeat(am0.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am0.WaitForState(RMAppAttemptState.Failed);
			rm1.WaitForState(app0.GetApplicationId(), RMAppState.Failed);
			// assert the app/attempt failed state is saved.
			ApplicationStateData appState = rmAppState[app0.GetApplicationId()];
			NUnit.Framework.Assert.AreEqual(RMAppState.Failed, appState.GetState());
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Failed, appState.GetAttempt(am0
				.GetApplicationAttemptId()).GetState());
			// start new RM
			MockRM rm2 = CreateMockRM(conf, memStore);
			rm2.Start();
			RMApp loadedApp0 = rm2.GetRMContext().GetRMApps()[app0.GetApplicationId()];
			rm2.WaitForState(app0.GetApplicationId(), RMAppState.Failed);
			rm2.WaitForState(am0.GetApplicationAttemptId(), RMAppAttemptState.Failed);
			// no new attempt is created.
			NUnit.Framework.Assert.AreEqual(1, loadedApp0.GetAppAttempts().Count);
			VerifyAppReportAfterRMRestart(app0, rm2);
			NUnit.Framework.Assert.IsTrue(app0.GetDiagnostics().ToString().Contains("Failing the application."
				));
		}

		// failed diagnostics from attempt is lost because the diagnostics from
		// attempt is not yet available by the time app is saving the app state.
		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartKilledApp()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			// kill the app.
			rm1.KillApp(app0.GetApplicationId());
			rm1.WaitForState(app0.GetApplicationId(), RMAppState.Killed);
			rm1.WaitForState(am0.GetApplicationAttemptId(), RMAppAttemptState.Killed);
			// killed state is saved.
			ApplicationStateData appState = rmAppState[app0.GetApplicationId()];
			NUnit.Framework.Assert.AreEqual(RMAppState.Killed, appState.GetState());
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Killed, appState.GetAttempt(am0
				.GetApplicationAttemptId()).GetState());
			string trackingUrl = app0.GetCurrentAppAttempt().GetOriginalTrackingUrl();
			NUnit.Framework.Assert.IsNotNull(trackingUrl);
			// restart rm
			MockRM rm2 = CreateMockRM(conf, memStore);
			rm2.Start();
			RMApp loadedApp0 = rm2.GetRMContext().GetRMApps()[app0.GetApplicationId()];
			rm2.WaitForState(app0.GetApplicationId(), RMAppState.Killed);
			rm2.WaitForState(am0.GetApplicationAttemptId(), RMAppAttemptState.Killed);
			// no new attempt is created.
			NUnit.Framework.Assert.AreEqual(1, loadedApp0.GetAppAttempts().Count);
			ApplicationReport appReport = VerifyAppReportAfterRMRestart(app0, rm2);
			NUnit.Framework.Assert.AreEqual(app0.GetDiagnostics().ToString(), appReport.GetDiagnostics
				());
			NUnit.Framework.Assert.AreEqual(trackingUrl, loadedApp0.GetCurrentAppAttempt().GetOriginalTrackingUrl
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartKilledAppWithNoAttempts()
		{
			MemoryRMStateStore memStore = new _MemoryRMStateStore_793();
			// ignore attempt saving request.
			// ignore attempt saving request.
			memStore.Init(conf);
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			// create app
			RMApp app0 = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", -1, null, "MAPREDUCE", false);
			// kill the app.
			rm1.KillApp(app0.GetApplicationId());
			rm1.WaitForState(app0.GetApplicationId(), RMAppState.Killed);
			// restart rm
			MockRM rm2 = CreateMockRM(conf, memStore);
			rm2.Start();
			RMApp loadedApp0 = rm2.GetRMContext().GetRMApps()[app0.GetApplicationId()];
			rm2.WaitForState(loadedApp0.GetApplicationId(), RMAppState.Killed);
			NUnit.Framework.Assert.IsTrue(loadedApp0.GetAppAttempts().Count == 0);
		}

		private sealed class _MemoryRMStateStore_793 : MemoryRMStateStore
		{
			public _MemoryRMStateStore_793()
			{
			}

			/// <exception cref="System.Exception"/>
			protected internal override void StoreApplicationAttemptStateInternal(ApplicationAttemptId
				 attemptId, ApplicationAttemptStateData attemptStateData)
			{
				lock (this)
				{
				}
			}

			/// <exception cref="System.Exception"/>
			protected internal override void UpdateApplicationAttemptStateInternal(ApplicationAttemptId
				 attemptId, ApplicationAttemptStateData attemptStateData)
			{
				lock (this)
				{
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartSucceededApp()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create an app and finish the app.
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			// unregister am
			FinishApplicationMasterRequest req = FinishApplicationMasterRequest.NewInstance(FinalApplicationStatus
				.Succeeded, "diagnostics", "trackingUrl");
			FinishApplicationMaster(app0, rm1, nm1, am0, req);
			// check the state store about the unregistered info.
			ApplicationStateData appState = rmAppState[app0.GetApplicationId()];
			ApplicationAttemptStateData attemptState0 = appState.GetAttempt(am0.GetApplicationAttemptId
				());
			NUnit.Framework.Assert.AreEqual("diagnostics", attemptState0.GetDiagnostics());
			NUnit.Framework.Assert.AreEqual(FinalApplicationStatus.Succeeded, attemptState0.GetFinalApplicationStatus
				());
			NUnit.Framework.Assert.AreEqual("trackingUrl", attemptState0.GetFinalTrackingUrl(
				));
			NUnit.Framework.Assert.AreEqual(app0.GetFinishTime(), appState.GetFinishTime());
			// restart rm
			MockRM rm2 = CreateMockRM(conf, memStore);
			rm2.Start();
			// verify application report returns the same app info as the app info
			// before RM restarts.
			ApplicationReport appReport = VerifyAppReportAfterRMRestart(app0, rm2);
			NUnit.Framework.Assert.AreEqual(FinalApplicationStatus.Succeeded, appReport.GetFinalApplicationStatus
				());
			NUnit.Framework.Assert.AreEqual("trackingUrl", appReport.GetOriginalTrackingUrl()
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartGetApplicationList()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 1);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// a succeeded app.
			RMApp app0 = rm1.SubmitApp(200, "name", "user", null, false, "default", 1, null, 
				"myType");
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			FinishApplicationMaster(app0, rm1, nm1, am0);
			// a failed app.
			RMApp app1 = rm1.SubmitApp(200, "name", "user", null, false, "default", 1, null, 
				"myType");
			MockAM am1 = LaunchAM(app1, rm1, nm1);
			// fail the AM by sending CONTAINER_FINISHED event without registering.
			nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am1.WaitForState(RMAppAttemptState.Failed);
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Failed);
			// a killed app.
			RMApp app2 = rm1.SubmitApp(200, "name", "user", null, false, "default", 1, null, 
				"myType");
			MockAM am2 = LaunchAM(app2, rm1, nm1);
			rm1.KillApp(app2.GetApplicationId());
			rm1.WaitForState(app2.GetApplicationId(), RMAppState.Killed);
			rm1.WaitForState(am2.GetApplicationAttemptId(), RMAppAttemptState.Killed);
			// restart rm
			MockRM rm2 = new _MockRM_918(conf, memStore);
			rms.AddItem(rm2);
			rm2.Start();
			GetApplicationsRequest request1 = GetApplicationsRequest.NewInstance(EnumSet.Of(YarnApplicationState
				.Finished, YarnApplicationState.Killed, YarnApplicationState.Failed));
			GetApplicationsResponse response1 = rm2.GetClientRMService().GetApplications(request1
				);
			IList<ApplicationReport> appList1 = response1.GetApplicationList();
			// assert all applications exist according to application state after RM
			// restarts.
			bool forApp0 = false;
			bool forApp1 = false;
			bool forApp2 = false;
			foreach (ApplicationReport report in appList1)
			{
				if (report.GetApplicationId().Equals(app0.GetApplicationId()))
				{
					NUnit.Framework.Assert.AreEqual(YarnApplicationState.Finished, report.GetYarnApplicationState
						());
					forApp0 = true;
				}
				if (report.GetApplicationId().Equals(app1.GetApplicationId()))
				{
					NUnit.Framework.Assert.AreEqual(YarnApplicationState.Failed, report.GetYarnApplicationState
						());
					forApp1 = true;
				}
				if (report.GetApplicationId().Equals(app2.GetApplicationId()))
				{
					NUnit.Framework.Assert.AreEqual(YarnApplicationState.Killed, report.GetYarnApplicationState
						());
					forApp2 = true;
				}
			}
			NUnit.Framework.Assert.IsTrue(forApp0 && forApp1 && forApp2);
			// assert all applications exist according to application type after RM
			// restarts.
			ICollection<string> appTypes = new HashSet<string>();
			appTypes.AddItem("myType");
			GetApplicationsRequest request2 = GetApplicationsRequest.NewInstance(appTypes);
			GetApplicationsResponse response2 = rm2.GetClientRMService().GetApplications(request2
				);
			IList<ApplicationReport> appList2 = response2.GetApplicationList();
			NUnit.Framework.Assert.IsTrue(3 == appList2.Count);
			// check application summary is logged for the completed apps after RM restart.
			Org.Mockito.Mockito.Verify(rm2.GetRMAppManager(), Org.Mockito.Mockito.Times(3)).LogApplicationSummary
				(Matchers.IsA<ApplicationId>());
		}

		private sealed class _MockRM_918 : MockRM
		{
			public _MockRM_918(Configuration baseArg1, RMStateStore baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			protected internal override RMAppManager CreateRMAppManager()
			{
				return Org.Mockito.Mockito.Spy(base.CreateRMAppManager());
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
		private ApplicationReport VerifyAppReportAfterRMRestart(RMApp app, MockRM rm)
		{
			GetApplicationReportRequest reportRequest = GetApplicationReportRequest.NewInstance
				(app.GetApplicationId());
			GetApplicationReportResponse response = rm.GetClientRMService().GetApplicationReport
				(reportRequest);
			ApplicationReport report = response.GetApplicationReport();
			NUnit.Framework.Assert.AreEqual(app.GetStartTime(), report.GetStartTime());
			NUnit.Framework.Assert.AreEqual(app.GetFinishTime(), report.GetFinishTime());
			NUnit.Framework.Assert.AreEqual(app.CreateApplicationState(), report.GetYarnApplicationState
				());
			NUnit.Framework.Assert.IsTrue(1 == report.GetProgress());
			return response.GetApplicationReport();
		}

		/// <exception cref="System.Exception"/>
		private void FinishApplicationMaster(RMApp rmApp, MockRM rm, MockNM nm, MockAM am
			)
		{
			FinishApplicationMasterRequest req = FinishApplicationMasterRequest.NewInstance(FinalApplicationStatus
				.Succeeded, string.Empty, string.Empty);
			FinishApplicationMaster(rmApp, rm, nm, am, req);
		}

		/// <exception cref="System.Exception"/>
		private void FinishApplicationMaster(RMApp rmApp, MockRM rm, MockNM nm, MockAM am
			, FinishApplicationMasterRequest req)
		{
			RMStateStore.RMState rmState = ((MemoryRMStateStore)rm.GetRMContext().GetStateStore
				()).GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			am.UnregisterAppAttempt(req, true);
			am.WaitForState(RMAppAttemptState.Finishing);
			nm.NodeHeartbeat(am.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am.WaitForState(RMAppAttemptState.Finished);
			rm.WaitForState(rmApp.GetApplicationId(), RMAppState.Finished);
			// check that app/attempt is saved with the final state
			ApplicationStateData appState = rmAppState[rmApp.GetApplicationId()];
			NUnit.Framework.Assert.AreEqual(RMAppState.Finished, appState.GetState());
			NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Finished, appState.GetAttempt(am
				.GetApplicationAttemptId()).GetState());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartOnMaxAppAttempts()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// submit an app with maxAppAttempts equals to 1
			RMApp app1 = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", 1, null);
			// submit an app with maxAppAttempts equals to -1
			RMApp app2 = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", -1, null);
			// assert app1 info is saved
			ApplicationStateData appState = rmAppState[app1.GetApplicationId()];
			NUnit.Framework.Assert.IsNotNull(appState);
			NUnit.Framework.Assert.AreEqual(0, appState.GetAttemptCount());
			NUnit.Framework.Assert.AreEqual(appState.GetApplicationSubmissionContext().GetApplicationId
				(), app1.GetApplicationSubmissionContext().GetApplicationId());
			// Allocate the AM
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt = app1.GetCurrentAppAttempt();
			ApplicationAttemptId attemptId1 = attempt.GetAppAttemptId();
			rm1.WaitForState(attemptId1, RMAppAttemptState.Allocated);
			NUnit.Framework.Assert.AreEqual(1, appState.GetAttemptCount());
			ApplicationAttemptStateData attemptState = appState.GetAttempt(attemptId1);
			NUnit.Framework.Assert.IsNotNull(attemptState);
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewContainerId(attemptId1, 1), attemptState
				.GetMasterContainer().GetId());
			// Setting AMLivelinessMonitor interval to be 3 Secs.
			conf.SetInt(YarnConfiguration.RmAmExpiryIntervalMs, 3000);
			// start new RM   
			MockRM rm2 = CreateMockRM(conf, memStore);
			rm2.Start();
			// verify that maxAppAttempts is set to global value
			NUnit.Framework.Assert.AreEqual(2, rm2.GetRMContext().GetRMApps()[app2.GetApplicationId
				()].GetMaxAppAttempts());
			// app1 and app2 are loaded back, but app1 failed because it's
			// hitting max-retry.
			NUnit.Framework.Assert.AreEqual(2, rm2.GetRMContext().GetRMApps().Count);
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Failed);
			rm2.WaitForState(app2.GetApplicationId(), RMAppState.Accepted);
			// app1 failed state is saved in state store. app2 final saved state is not
			// determined yet.
			NUnit.Framework.Assert.AreEqual(RMAppState.Failed, rmAppState[app1.GetApplicationId
				()].GetState());
			NUnit.Framework.Assert.IsNull(rmAppState[app2.GetApplicationId()].GetState());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDelegationTokenRestoredInDelegationTokenRenewer()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 2);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			MockRM rm1 = new TestRMRestart.TestSecurityMockRM(conf, memStore);
			rm1.Start();
			HashSet<Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>> tokenSet
				 = new HashSet<Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier
				>>();
			// create an empty credential
			Credentials ts = new Credentials();
			// create tokens and add into credential
			Text userText1 = new Text("user1");
			RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(userText1, new 
				Text("renewer1"), userText1);
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token1 = new 
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>(dtId1, rm1.GetRMContext
				().GetRMDelegationTokenSecretManager());
			SecurityUtil.SetTokenService(token1, rmAddr);
			ts.AddToken(userText1, token1);
			tokenSet.AddItem(token1);
			Text userText2 = new Text("user2");
			RMDelegationTokenIdentifier dtId2 = new RMDelegationTokenIdentifier(userText2, new 
				Text("renewer2"), userText2);
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token2 = new 
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier>(dtId2, rm1.GetRMContext
				().GetRMDelegationTokenSecretManager());
			SecurityUtil.SetTokenService(token2, rmAddr);
			ts.AddToken(userText2, token2);
			tokenSet.AddItem(token2);
			// submit an app with customized credential
			RMApp app = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", 1, ts);
			// assert app info is saved
			ApplicationStateData appState = rmAppState[app.GetApplicationId()];
			NUnit.Framework.Assert.IsNotNull(appState);
			// assert delegation tokens exist in rm1 DelegationTokenRenewr
			NUnit.Framework.Assert.AreEqual(tokenSet, rm1.GetRMContext().GetDelegationTokenRenewer
				().GetDelegationTokens());
			// assert delegation tokens are saved
			DataOutputBuffer dob = new DataOutputBuffer();
			ts.WriteTokenStorageToStream(dob);
			ByteBuffer securityTokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
			securityTokens.Rewind();
			NUnit.Framework.Assert.AreEqual(securityTokens, appState.GetApplicationSubmissionContext
				().GetAMContainerSpec().GetTokens());
			// start new RM
			MockRM rm2 = new TestRMRestart.TestSecurityMockRM(conf, memStore);
			rm2.Start();
			// Need to wait for a while as now token renewal happens on another thread
			// and is asynchronous in nature.
			WaitForTokensToBeRenewed(rm2, tokenSet);
			// verify tokens are properly populated back to rm2 DelegationTokenRenewer
			NUnit.Framework.Assert.AreEqual(tokenSet, rm2.GetRMContext().GetDelegationTokenRenewer
				().GetDelegationTokens());
		}

		/// <exception cref="System.Exception"/>
		private void WaitForTokensToBeRenewed(MockRM rm2, HashSet<Org.Apache.Hadoop.Security.Token.Token
			<RMDelegationTokenIdentifier>> tokenSet)
		{
			// Max wait time to get the token renewal can be kept as 1sec (100 * 10ms)
			int waitCnt = 100;
			while (waitCnt-- > 0)
			{
				if (tokenSet.Equals(rm2.GetRMContext().GetDelegationTokenRenewer().GetDelegationTokens
					()))
				{
					// Stop waiting as tokens are populated to DelegationTokenRenewer.
					break;
				}
				else
				{
					Sharpen.Thread.Sleep(10);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppAttemptTokensRestoredOnRMRestart()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 2);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			MockRM rm1 = new TestRMRestart.TestSecurityMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("0.0.0.0:4321", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// submit an app
			RMApp app1 = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), "default");
			// assert app info is saved
			ApplicationStateData appState = rmAppState[app1.GetApplicationId()];
			NUnit.Framework.Assert.IsNotNull(appState);
			// Allocate the AM
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			ApplicationAttemptId attemptId1 = attempt1.GetAppAttemptId();
			rm1.WaitForState(attemptId1, RMAppAttemptState.Allocated);
			// assert attempt info is saved
			ApplicationAttemptStateData attemptState = appState.GetAttempt(attemptId1);
			NUnit.Framework.Assert.IsNotNull(attemptState);
			NUnit.Framework.Assert.AreEqual(BuilderUtils.NewContainerId(attemptId1, 1), attemptState
				.GetMasterContainer().GetId());
			// the clientTokenMasterKey that are generated when
			// RMAppAttempt is created,
			byte[] clientTokenMasterKey = attempt1.GetClientTokenMasterKey().GetEncoded();
			// assert application credentials are saved
			Credentials savedCredentials = attemptState.GetAppAttemptTokens();
			Assert.AssertArrayEquals("client token master key not saved", clientTokenMasterKey
				, savedCredentials.GetSecretKey(RMStateStore.AmClientTokenMasterKeyName));
			// start new RM
			MockRM rm2 = new TestRMRestart.TestSecurityMockRM(conf, memStore);
			rm2.Start();
			RMApp loadedApp1 = rm2.GetRMContext().GetRMApps()[app1.GetApplicationId()];
			RMAppAttempt loadedAttempt1 = loadedApp1.GetRMAppAttempt(attemptId1);
			// assert loaded attempt recovered
			NUnit.Framework.Assert.IsNotNull(loadedAttempt1);
			// assert client token master key is recovered back to api-versioned
			// client token master key
			NUnit.Framework.Assert.AreEqual("client token master key not restored", attempt1.
				GetClientTokenMasterKey(), loadedAttempt1.GetClientTokenMasterKey());
			// assert ClientTokenSecretManager also knows about the key
			Assert.AssertArrayEquals(clientTokenMasterKey, rm2.GetClientToAMTokenSecretManager
				().GetMasterKey(attemptId1).GetEncoded());
			// assert AMRMTokenSecretManager also knows about the AMRMToken password
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = loadedAttempt1
				.GetAMRMToken();
			Assert.AssertArrayEquals(amrmToken.GetPassword(), rm2.GetRMContext().GetAMRMTokenSecretManager
				().RetrievePassword(amrmToken.DecodeIdentifier()));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMDelegationTokenRestoredOnRMRestart()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 2);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			conf.Set(YarnConfiguration.RmAddress, "localhost:8032");
			UserGroupInformation.SetConfiguration(conf);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			IDictionary<RMDelegationTokenIdentifier, long> rmDTState = rmState.GetRMDTSecretManagerState
				().GetTokenState();
			ICollection<DelegationKey> rmDTMasterKeyState = rmState.GetRMDTSecretManagerState
				().GetMasterKeyState();
			MockRM rm1 = new TestRMRestart.TestSecurityMockRM(conf, memStore);
			rm1.Start();
			// create an empty credential
			Credentials ts = new Credentials();
			// request a token and add into credential
			GetDelegationTokenRequest request1 = GetDelegationTokenRequest.NewInstance("renewer1"
				);
			UserGroupInformation.GetCurrentUser().SetAuthenticationMethod(SaslRpcServer.AuthMethod
				.Kerberos);
			GetDelegationTokenResponse response1 = rm1.GetClientRMService().GetDelegationToken
				(request1);
			Org.Apache.Hadoop.Yarn.Api.Records.Token delegationToken1 = response1.GetRMDelegationToken
				();
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token1 = ConverterUtils
				.ConvertFromYarn(delegationToken1, rmAddr);
			RMDelegationTokenIdentifier dtId1 = token1.DecodeIdentifier();
			HashSet<RMDelegationTokenIdentifier> tokenIdentSet = new HashSet<RMDelegationTokenIdentifier
				>();
			ts.AddToken(token1.GetService(), token1);
			tokenIdentSet.AddItem(dtId1);
			// submit an app with customized credential
			RMApp app = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", 1, ts);
			// assert app info is saved
			ApplicationStateData appState = rmAppState[app.GetApplicationId()];
			NUnit.Framework.Assert.IsNotNull(appState);
			// assert all master keys are saved
			ICollection<DelegationKey> allKeysRM1 = rm1.GetRMContext().GetRMDelegationTokenSecretManager
				().GetAllMasterKeys();
			NUnit.Framework.Assert.AreEqual(allKeysRM1, rmDTMasterKeyState);
			// assert all tokens are saved
			IDictionary<RMDelegationTokenIdentifier, long> allTokensRM1 = rm1.GetRMContext().
				GetRMDelegationTokenSecretManager().GetAllTokens();
			NUnit.Framework.Assert.AreEqual(tokenIdentSet, allTokensRM1.Keys);
			NUnit.Framework.Assert.AreEqual(allTokensRM1, rmDTState);
			// assert sequence number is saved
			NUnit.Framework.Assert.AreEqual(rm1.GetRMContext().GetRMDelegationTokenSecretManager
				().GetLatestDTSequenceNumber(), rmState.GetRMDTSecretManagerState().GetDTSequenceNumber
				());
			// request one more token
			GetDelegationTokenRequest request2 = GetDelegationTokenRequest.NewInstance("renewer2"
				);
			GetDelegationTokenResponse response2 = rm1.GetClientRMService().GetDelegationToken
				(request2);
			Org.Apache.Hadoop.Yarn.Api.Records.Token delegationToken2 = response2.GetRMDelegationToken
				();
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token2 = ConverterUtils
				.ConvertFromYarn(delegationToken2, rmAddr);
			RMDelegationTokenIdentifier dtId2 = token2.DecodeIdentifier();
			// cancel token2
			try
			{
				rm1.GetRMContext().GetRMDelegationTokenSecretManager().CancelToken(token2, UserGroupInformation
					.GetCurrentUser().GetUserName());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			// Assert the token which has the latest delegationTokenSequenceNumber is removed
			NUnit.Framework.Assert.AreEqual(rm1.GetRMContext().GetRMDelegationTokenSecretManager
				().GetLatestDTSequenceNumber(), dtId2.GetSequenceNumber());
			NUnit.Framework.Assert.IsFalse(rmDTState.Contains(dtId2));
			// start new RM
			MockRM rm2 = new TestRMRestart.TestSecurityMockRM(conf, memStore);
			rm2.Start();
			// assert master keys and tokens are populated back to DTSecretManager
			IDictionary<RMDelegationTokenIdentifier, long> allTokensRM2 = rm2.GetRMContext().
				GetRMDelegationTokenSecretManager().GetAllTokens();
			NUnit.Framework.Assert.AreEqual(allTokensRM2.Keys, allTokensRM1.Keys);
			// rm2 has its own master keys when it starts, we use containsAll here
			NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetRMDelegationTokenSecretManager
				().GetAllMasterKeys().ContainsAll(allKeysRM1));
			// assert sequenceNumber is properly recovered,
			// even though the token which has max sequenceNumber is not stored
			NUnit.Framework.Assert.AreEqual(rm1.GetRMContext().GetRMDelegationTokenSecretManager
				().GetLatestDTSequenceNumber(), rm2.GetRMContext().GetRMDelegationTokenSecretManager
				().GetLatestDTSequenceNumber());
			// renewDate before renewing
			long renewDateBeforeRenew = allTokensRM2[dtId1];
			try
			{
				// Sleep for one millisecond to make sure renewDataAfterRenew is greater
				Sharpen.Thread.Sleep(1);
				// renew recovered token
				rm2.GetRMContext().GetRMDelegationTokenSecretManager().RenewToken(token1, "renewer1"
					);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			allTokensRM2 = rm2.GetRMContext().GetRMDelegationTokenSecretManager().GetAllTokens
				();
			long renewDateAfterRenew = allTokensRM2[dtId1];
			// assert token is renewed
			NUnit.Framework.Assert.IsTrue(renewDateAfterRenew > renewDateBeforeRenew);
			// assert new token is added into state store
			NUnit.Framework.Assert.IsTrue(rmDTState.ContainsValue(renewDateAfterRenew));
			// assert old token is removed from state store
			NUnit.Framework.Assert.IsFalse(rmDTState.ContainsValue(renewDateBeforeRenew));
			try
			{
				rm2.GetRMContext().GetRMDelegationTokenSecretManager().CancelToken(token1, UserGroupInformation
					.GetCurrentUser().GetUserName());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			// assert token is removed from state after its cancelled
			allTokensRM2 = rm2.GetRMContext().GetRMDelegationTokenSecretManager().GetAllTokens
				();
			NUnit.Framework.Assert.IsFalse(allTokensRM2.Contains(dtId1));
			NUnit.Framework.Assert.IsFalse(rmDTState.Contains(dtId1));
		}

		// This is to test submit an application to the new RM with the old delegation
		// token got from previous RM.
		/// <exception cref="System.Exception"/>
		public virtual void TestAppSubmissionWithOldDelegationTokenAfterRMRestart()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 2);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			conf.Set(YarnConfiguration.RmAddress, "localhost:8032");
			UserGroupInformation.SetConfiguration(conf);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			MockRM rm1 = new TestRMRestart.TestSecurityMockRM(conf, memStore);
			rm1.Start();
			GetDelegationTokenRequest request1 = GetDelegationTokenRequest.NewInstance("renewer1"
				);
			UserGroupInformation.GetCurrentUser().SetAuthenticationMethod(SaslRpcServer.AuthMethod
				.Kerberos);
			GetDelegationTokenResponse response1 = rm1.GetClientRMService().GetDelegationToken
				(request1);
			Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token1 = ConverterUtils
				.ConvertFromYarn(response1.GetRMDelegationToken(), rmAddr);
			// start new RM
			MockRM rm2 = new TestRMRestart.TestSecurityMockRM(conf, memStore);
			rm2.Start();
			// submit an app with the old delegation token got from previous RM.
			Credentials ts = new Credentials();
			ts.AddToken(token1.GetService(), token1);
			RMApp app = rm2.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", 1, ts);
			rm2.WaitForState(app.GetApplicationId(), RMAppState.Accepted);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRMStateStoreDispatcherDrainedOnRMStop()
		{
			MemoryRMStateStore memStore = new _MemoryRMStateStore_1457();
			// Unblock app saving request.
			// Block app saving request.
			// Skip if synchronous updation of DTToken
			memStore.Init(conf);
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			// create apps.
			AList<RMApp> appList = new AList<RMApp>();
			int NumApps = 5;
			for (int i = 0; i < NumApps; i++)
			{
				RMApp app = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
					, string>(), false, "default", -1, null, "MAPREDUCE", false);
				appList.AddItem(app);
				rm1.WaitForState(app.GetApplicationId(), RMAppState.NewSaving);
			}
			// all apps's saving request are now enqueued to RMStateStore's dispatcher
			// queue, and will be processed once rm.stop() is called.
			// Nothing exist in state store before stop is called.
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = memStore.GetState()
				.GetApplicationState();
			NUnit.Framework.Assert.IsTrue(rmAppState.Count == 0);
			// stop rm
			rm1.Stop();
			// Assert app info is still saved even if stop is called with pending saving
			// request on dispatcher.
			foreach (RMApp app_1 in appList)
			{
				ApplicationStateData appState = rmAppState[app_1.GetApplicationId()];
				NUnit.Framework.Assert.IsNotNull(appState);
				NUnit.Framework.Assert.AreEqual(0, appState.GetAttemptCount());
				NUnit.Framework.Assert.AreEqual(appState.GetApplicationSubmissionContext().GetApplicationId
					(), app_1.GetApplicationSubmissionContext().GetApplicationId());
			}
			NUnit.Framework.Assert.IsTrue(rmAppState.Count == NumApps);
		}

		private sealed class _MemoryRMStateStore_1457 : MemoryRMStateStore
		{
			public _MemoryRMStateStore_1457()
			{
				this.wait = true;
			}

			internal volatile bool wait;

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				this.wait = false;
				base.ServiceStop();
			}

			protected internal override void HandleStoreEvent(RMStateStoreEvent @event)
			{
				if (!(@event is RMStateStoreAMRMTokenEvent) && !(@event is RMStateStoreRMDTEvent)
					 && !(@event is RMStateStoreRMDTMasterKeyEvent))
				{
					while (this.wait)
					{
					}
				}
				base.HandleStoreEvent(@event);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFinishedAppRemovalAfterRMRestart()
		{
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			conf.SetInt(YarnConfiguration.RmMaxCompletedApplications, 1);
			memStore.Init(conf);
			RMStateStore.RMState rmState = memStore.GetState();
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create an app and finish the app.
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			FinishApplicationMaster(app0, rm1, nm1, am0);
			MockRM rm2 = CreateMockRM(conf, memStore);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			nm1 = rm2.RegisterNode("127.0.0.1:1234", 15120);
			IDictionary<ApplicationId, ApplicationStateData> rmAppState = rmState.GetApplicationState
				();
			// app0 exits in both state store and rmContext
			NUnit.Framework.Assert.AreEqual(RMAppState.Finished, rmAppState[app0.GetApplicationId
				()].GetState());
			rm2.WaitForState(app0.GetApplicationId(), RMAppState.Finished);
			// create one more app and finish the app.
			RMApp app1 = rm2.SubmitApp(200);
			MockAM am1 = LaunchAM(app1, rm2, nm1);
			FinishApplicationMaster(app1, rm2, nm1, am1);
			// the first app0 get kicked out from both rmContext and state store
			NUnit.Framework.Assert.IsNull(rm2.GetRMContext().GetRMApps()[app0.GetApplicationId
				()]);
			NUnit.Framework.Assert.IsNull(rmAppState[app0.GetApplicationId()]);
		}

		// This is to test RM does not get hang on shutdown.
		/// <exception cref="System.Exception"/>
		public virtual void TestRMShutdown()
		{
			MemoryRMStateStore memStore = new _MemoryRMStateStore_1565();
			// start RM
			memStore.Init(conf);
			MockRM rm1 = null;
			try
			{
				rm1 = CreateMockRM(conf, memStore);
				rm1.Start();
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Invalid version."));
			}
			NUnit.Framework.Assert.IsTrue(rm1.GetServiceState() == Service.STATE.Stopped);
		}

		private sealed class _MemoryRMStateStore_1565 : MemoryRMStateStore
		{
			public _MemoryRMStateStore_1565()
			{
			}

			/// <exception cref="System.Exception"/>
			public override void CheckVersion()
			{
				lock (this)
				{
					throw new Exception("Invalid version.");
				}
			}
		}

		// This is to test Killing application should be able to wait until app
		// reaches killed state and also check that attempt state is saved before app
		// state is saved.
		/// <exception cref="System.Exception"/>
		public virtual void TestClientRetryOnKillingApplication()
		{
			MemoryRMStateStore memStore = new TestRMRestart.TestMemoryRMStateStore(this);
			memStore.Init(conf);
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app1 = rm1.SubmitApp(200, "name", "user", null, false, "default", 1, null, 
				"myType");
			MockAM am1 = LaunchAM(app1, rm1, nm1);
			KillApplicationResponse response;
			int count = 0;
			while (true)
			{
				response = rm1.KillApp(app1.GetApplicationId());
				if (response.GetIsKillCompleted())
				{
					break;
				}
				Sharpen.Thread.Sleep(100);
				count++;
			}
			// we expect at least 2 calls for killApp as the first killApp always return
			// false.
			NUnit.Framework.Assert.IsTrue(count >= 1);
			rm1.WaitForState(am1.GetApplicationAttemptId(), RMAppAttemptState.Killed);
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Killed);
			NUnit.Framework.Assert.AreEqual(1, ((TestRMRestart.TestMemoryRMStateStore)memStore
				).updateAttempt);
			NUnit.Framework.Assert.AreEqual(2, ((TestRMRestart.TestMemoryRMStateStore)memStore
				).updateApp);
		}

		// Test Application that fails on submission is saved in state store.
		/// <exception cref="System.Exception"/>
		public virtual void TestAppFailedOnSubmissionSavedInStateStore()
		{
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			MockRM rm1 = new _TestSecurityMockRM_1634(this, conf, memStore);
			rm1.Start();
			RMApp app1 = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
				, string>(), false, "default", -1, null, "MAPREDUCE", false);
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Failed);
			// Check app staet is saved in state store.
			NUnit.Framework.Assert.AreEqual(RMAppState.Failed, memStore.GetState().GetApplicationState
				()[app1.GetApplicationId()].GetState());
			MockRM rm2 = new TestRMRestart.TestSecurityMockRM(conf, memStore);
			rm2.Start();
			// Restarted RM has the failed app info too.
			rm2.WaitForState(app1.GetApplicationId(), RMAppState.Failed);
		}

		private sealed class _TestSecurityMockRM_1634 : TestRMRestart.TestSecurityMockRM
		{
			public _TestSecurityMockRM_1634(TestRMRestart _enclosing, Configuration baseArg1, 
				RMStateStore baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
			}

			protected internal override RMAppManager CreateRMAppManager()
			{
				return new _T444274087(this, this.rmContext, this.scheduler, this.masterService, 
					this.applicationACLsManager, this._enclosing.conf);
			}

			internal class _T444274087 : RMAppManager
			{
				public _T444274087(_TestSecurityMockRM_1634 _enclosing, RMContext context, YarnScheduler
					 scheduler, ApplicationMasterService masterService, ApplicationACLsManager applicationACLsManager
					, Configuration conf)
					: base(context, scheduler, masterService, applicationACLsManager, conf)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override Credentials ParseCredentials(ApplicationSubmissionContext
					 application)
				{
					throw new IOException("Parsing credential error.");
				}

				private readonly _TestSecurityMockRM_1634 _enclosing;
			}

			private readonly TestRMRestart _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppRecoveredInOrderOnRMRestart()
		{
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			for (int i = 10; i > 0; i--)
			{
				ApplicationStateData appState = Org.Mockito.Mockito.Mock<ApplicationStateData>();
				ApplicationSubmissionContext context = Org.Mockito.Mockito.Mock<ApplicationSubmissionContext
					>();
				Org.Mockito.Mockito.When(appState.GetApplicationSubmissionContext()).ThenReturn(context
					);
				Org.Mockito.Mockito.When(context.GetApplicationId()).ThenReturn(ApplicationId.NewInstance
					(1234, i));
				memStore.GetState().GetApplicationState()[appState.GetApplicationSubmissionContext
					().GetApplicationId()] = appState;
			}
			MockRM rm1 = new _MockRM_1689(this, conf, memStore);
			// check application is recovered in order.
			try
			{
				rm1.Start();
			}
			finally
			{
				rm1.Stop();
			}
		}

		private sealed class _MockRM_1689 : MockRM
		{
			public _MockRM_1689(TestRMRestart _enclosing, Configuration baseArg1, RMStateStore
				 baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
			}

			protected internal override RMAppManager CreateRMAppManager()
			{
				return new _T1107907694(this, this.rmContext, this.scheduler, this.masterService, 
					this.applicationACLsManager, this._enclosing.conf);
			}

			internal class _T1107907694 : RMAppManager
			{
				internal ApplicationId prevId = ApplicationId.NewInstance(1234, 0);

				public _T1107907694(_MockRM_1689 _enclosing, RMContext context, YarnScheduler scheduler
					, ApplicationMasterService masterService, ApplicationACLsManager applicationACLsManager
					, Configuration conf)
					: base(context, scheduler, masterService, applicationACLsManager, conf)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				protected internal override void RecoverApplication(ApplicationStateData appState
					, RMStateStore.RMState rmState)
				{
					NUnit.Framework.Assert.IsTrue(rmState.GetApplicationState().Count > 0);
					NUnit.Framework.Assert.IsTrue(appState.GetApplicationSubmissionContext().GetApplicationId
						().CompareTo(this.prevId) > 0);
					this.prevId = appState.GetApplicationSubmissionContext().GetApplicationId();
				}

				private readonly _MockRM_1689 _enclosing;
			}

			private readonly TestRMRestart _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQueueMetricsOnRMRestart()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// PHASE 1: create state in an RM
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			QueueMetrics qm1 = rm1.GetResourceScheduler().GetRootQueueMetrics();
			ResetQueueMetrics(qm1);
			AssertQueueMetrics(qm1, 0, 0, 0, 0);
			// create app that gets launched and does allocate before RM restart
			RMApp app1 = rm1.SubmitApp(200);
			// Need to wait first for AppAttempt to be started (RMAppState.ACCEPTED)
			// and then for it to reach RMAppAttemptState.SCHEDULED
			// inorder to ensure appsPending metric is incremented
			rm1.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			ApplicationAttemptId attemptId1 = attempt1.GetAppAttemptId();
			rm1.WaitForState(attemptId1, RMAppAttemptState.Scheduled);
			AssertQueueMetrics(qm1, 1, 1, 0, 0);
			nm1.NodeHeartbeat(true);
			rm1.WaitForState(attemptId1, RMAppAttemptState.Allocated);
			MockAM am1 = rm1.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			am1.Allocate("127.0.0.1", 1000, 1, new AList<ContainerId>());
			nm1.NodeHeartbeat(true);
			IList<Container> conts = am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId
				>()).GetAllocatedContainers();
			while (conts.Count == 0)
			{
				nm1.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(conts, am1.Allocate(new AList<ResourceRequest>(), new 
					AList<ContainerId>()).GetAllocatedContainers());
				Sharpen.Thread.Sleep(500);
			}
			AssertQueueMetrics(qm1, 1, 0, 1, 0);
			// PHASE 2: create new RM and start from old state
			// create new RM to represent restart and recover state
			MockRM rm2 = CreateMockRM(conf, memStore);
			QueueMetrics qm2 = rm2.GetResourceScheduler().GetRootQueueMetrics();
			ResetQueueMetrics(qm2);
			AssertQueueMetrics(qm2, 0, 0, 0, 0);
			rm2.Start();
			nm1.SetResourceTrackerService(rm2.GetResourceTrackerService());
			// recover app
			RMApp loadedApp1 = rm2.GetRMContext().GetRMApps()[app1.GetApplicationId()];
			nm1.NodeHeartbeat(true);
			nm1 = new MockNM("127.0.0.1:1234", 15120, rm2.GetResourceTrackerService());
			NMContainerStatus status = TestRMRestart.CreateNMContainerStatus(loadedApp1.GetCurrentAppAttempt
				().GetAppAttemptId(), 1, ContainerState.Complete);
			nm1.RegisterNode(Arrays.AsList(status), null);
			while (loadedApp1.GetAppAttempts().Count != 2)
			{
				Sharpen.Thread.Sleep(200);
			}
			attempt1 = loadedApp1.GetCurrentAppAttempt();
			attemptId1 = attempt1.GetAppAttemptId();
			rm2.WaitForState(attemptId1, RMAppAttemptState.Scheduled);
			AssertQueueMetrics(qm2, 1, 1, 0, 0);
			nm1.NodeHeartbeat(true);
			rm2.WaitForState(attemptId1, RMAppAttemptState.Allocated);
			AssertQueueMetrics(qm2, 1, 0, 1, 0);
			am1 = rm2.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			am1.Allocate("127.0.0.1", 1000, 3, new AList<ContainerId>());
			nm1.NodeHeartbeat(true);
			conts = am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>()).GetAllocatedContainers
				();
			while (conts.Count == 0)
			{
				nm1.NodeHeartbeat(true);
				Sharpen.Collections.AddAll(conts, am1.Allocate(new AList<ResourceRequest>(), new 
					AList<ContainerId>()).GetAllocatedContainers());
				Sharpen.Thread.Sleep(500);
			}
			// finish the AMs
			FinishApplicationMaster(loadedApp1, rm2, nm1, am1);
			AssertQueueMetrics(qm2, 1, 0, 0, 1);
		}

		private int appsSubmittedCarryOn = 0;

		private int appsPendingCarryOn = 0;

		private int appsRunningCarryOn = 0;

		private int appsCompletedCarryOn = 0;

		// The metrics has some carry-on value from the previous RM, because the
		// test case is in-memory, for the same queue name (e.g. root), there's
		// always a singleton QueueMetrics object.
		private void ResetQueueMetrics(QueueMetrics qm)
		{
			appsSubmittedCarryOn = qm.GetAppsSubmitted();
			appsPendingCarryOn = qm.GetAppsPending();
			appsRunningCarryOn = qm.GetAppsRunning();
			appsCompletedCarryOn = qm.GetAppsCompleted();
		}

		private void AssertQueueMetrics(QueueMetrics qm, int appsSubmitted, int appsPending
			, int appsRunning, int appsCompleted)
		{
			NUnit.Framework.Assert.AreEqual(qm.GetAppsSubmitted(), appsSubmitted + appsSubmittedCarryOn
				);
			NUnit.Framework.Assert.AreEqual(qm.GetAppsPending(), appsPending + appsPendingCarryOn
				);
			NUnit.Framework.Assert.AreEqual(qm.GetAppsRunning(), appsRunning + appsRunningCarryOn
				);
			NUnit.Framework.Assert.AreEqual(qm.GetAppsCompleted(), appsCompleted + appsCompletedCarryOn
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDecomissionedNMsMetricsOnRMRestart()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.RmNodesExcludeFilePath, hostFile.GetAbsolutePath());
			WriteToHostsFile(string.Empty);
			DrainDispatcher dispatcher = new DrainDispatcher();
			MockRM rm1 = null;
			MockRM rm2 = null;
			try
			{
				rm1 = new _MockRM_1856(dispatcher, conf);
				rm1.Start();
				MockNM nm1 = rm1.RegisterNode("localhost:1234", 8000);
				MockNM nm2 = rm1.RegisterNode("host2:1234", 8000);
				NUnit.Framework.Assert.AreEqual(0, ClusterMetrics.GetMetrics().GetNumDecommisionedNMs
					());
				string ip = NetUtils.NormalizeHostName("localhost");
				// Add 2 hosts to exclude list.
				WriteToHostsFile("host2", ip);
				// refresh nodes
				rm1.GetNodesListManager().RefreshNodes(conf);
				NodeHeartbeatResponse nodeHeartbeat = nm1.NodeHeartbeat(true);
				NUnit.Framework.Assert.IsTrue(NodeAction.Shutdown.Equals(nodeHeartbeat.GetNodeAction
					()));
				nodeHeartbeat = nm2.NodeHeartbeat(true);
				NUnit.Framework.Assert.IsTrue("The decommisioned metrics are not updated", NodeAction
					.Shutdown.Equals(nodeHeartbeat.GetNodeAction()));
				dispatcher.Await();
				NUnit.Framework.Assert.AreEqual(2, ClusterMetrics.GetMetrics().GetNumDecommisionedNMs
					());
				rm1.Stop();
				rm1 = null;
				NUnit.Framework.Assert.AreEqual(0, ClusterMetrics.GetMetrics().GetNumDecommisionedNMs
					());
				// restart RM.
				rm2 = new MockRM(conf);
				rm2.Start();
				NUnit.Framework.Assert.AreEqual(2, ClusterMetrics.GetMetrics().GetNumDecommisionedNMs
					());
			}
			finally
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
		}

		private sealed class _MockRM_1856 : MockRM
		{
			public _MockRM_1856(DrainDispatcher dispatcher, Configuration baseArg1)
				: base(baseArg1)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly DrainDispatcher dispatcher;
		}

		// Test Delegation token is renewed synchronously so that recover events
		// can be processed before any other external incoming events, specifically
		// the ContainerFinished event on NM re-registraton.
		/// <exception cref="System.Exception"/>
		public virtual void TestSynchronouslyRenewDTOnRecovery()
		{
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, 2);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			// start RM
			MockRM rm1 = CreateMockRM(conf, memStore);
			rm1.Start();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = MockRM.LaunchAndRegisterAM(app0, rm1, nm1);
			MockRM rm2 = new _MockRM_1928(this, nm1, am0, conf, memStore);
			// send the container_finished event as soon as the
			// ResourceTrackerService is started.
			try
			{
				// Re-start RM
				rm2.Start();
				// wait for the 2nd attempt to be started.
				RMApp loadedApp0 = rm2.GetRMContext().GetRMApps()[app0.GetApplicationId()];
				int timeoutSecs = 0;
				while (loadedApp0.GetAppAttempts().Count != 2 && timeoutSecs++ < 40)
				{
					Sharpen.Thread.Sleep(200);
				}
				MockAM am1 = MockRM.LaunchAndRegisterAM(loadedApp0, rm2, nm1);
				MockRM.FinishAMAndVerifyAppState(loadedApp0, rm2, nm1, am1);
			}
			finally
			{
				rm2.Stop();
			}
		}

		private sealed class _MockRM_1928 : MockRM
		{
			public _MockRM_1928(TestRMRestart _enclosing, MockNM nm1, MockAM am0, Configuration
				 baseArg1, RMStateStore baseArg2)
				: base(baseArg1, baseArg2)
			{
				this._enclosing = _enclosing;
				this.nm1 = nm1;
				this.am0 = am0;
			}

			protected internal override ResourceTrackerService CreateResourceTrackerService()
			{
				return new _ResourceTrackerService_1934(this, nm1, am0, this.rmContext, this.nodesListManager
					, this.nmLivelinessMonitor, this.rmContext.GetContainerTokenSecretManager(), this
					.rmContext.GetNMTokenSecretManager());
			}

			private sealed class _ResourceTrackerService_1934 : ResourceTrackerService
			{
				public _ResourceTrackerService_1934(_MockRM_1928 _enclosing, MockNM nm1, MockAM am0
					, RMContext baseArg1, NodesListManager baseArg2, NMLivelinessMonitor baseArg3, RMContainerTokenSecretManager
					 baseArg4, NMTokenSecretManagerInRM baseArg5)
					: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
				{
					this._enclosing = _enclosing;
					this.nm1 = nm1;
					this.am0 = am0;
				}

				/// <exception cref="System.Exception"/>
				protected override void ServiceStart()
				{
					base.ServiceStart();
					nm1.SetResourceTrackerService(this._enclosing.GetResourceTrackerService());
					NMContainerStatus status = TestRMRestart.CreateNMContainerStatus(am0.GetApplicationAttemptId
						(), 1, ContainerState.Complete);
					nm1.RegisterNode(Arrays.AsList(status), null);
				}

				private readonly _MockRM_1928 _enclosing;

				private readonly MockNM nm1;

				private readonly MockAM am0;
			}

			private readonly TestRMRestart _enclosing;

			private readonly MockNM nm1;

			private readonly MockAM am0;
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteToHostsFile(params string[] hosts)
		{
			if (!hostFile.Exists())
			{
				TempDir.Mkdirs();
				hostFile.CreateNewFile();
			}
			FileOutputStream fStream = null;
			try
			{
				fStream = new FileOutputStream(hostFile);
				for (int i = 0; i < hosts.Length; i++)
				{
					fStream.Write(Sharpen.Runtime.GetBytesForString(hosts[i]));
					fStream.Write(Sharpen.Runtime.GetBytesForString(Runtime.GetProperty("line.separator"
						)));
				}
			}
			finally
			{
				if (fStream != null)
				{
					IOUtils.CloseStream(fStream);
					fStream = null;
				}
			}
		}

		public static NMContainerStatus CreateNMContainerStatus(ApplicationAttemptId appAttemptId
			, int id, ContainerState containerState)
		{
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, id);
			NMContainerStatus containerReport = NMContainerStatus.NewInstance(containerId, containerState
				, Resource.NewInstance(1024, 1), "recover container", 0, Priority.NewInstance(0)
				, 0);
			return containerReport;
		}

		public class TestMemoryRMStateStore : MemoryRMStateStore
		{
			internal int count = 0;

			public int updateApp = 0;

			public int updateAttempt = 0;

			/// <exception cref="System.Exception"/>
			protected internal override void UpdateApplicationStateInternal(ApplicationId appId
				, ApplicationStateData appStateData)
			{
				this.updateApp = ++this.count;
				base.UpdateApplicationStateInternal(appId, appStateData);
			}

			/// <exception cref="System.Exception"/>
			protected internal override void UpdateApplicationAttemptStateInternal(ApplicationAttemptId
				 attemptId, ApplicationAttemptStateData attemptStateData)
			{
				lock (this)
				{
					this.updateAttempt = ++this.count;
					base.UpdateApplicationAttemptStateInternal(attemptId, attemptStateData);
				}
			}

			internal TestMemoryRMStateStore(TestRMRestart _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMRestart _enclosing;
		}

		public class TestSecurityMockRM : MockRM
		{
			public TestSecurityMockRM(Configuration conf, RMStateStore store)
				: base(conf, store)
			{
			}

			public override void Init(Configuration conf)
			{
				// reset localServiceAddress.
				RMDelegationTokenIdentifier.Renewer.SetSecretManager(null, null);
				base.Init(conf);
			}

			protected internal override ClientRMService CreateClientRMService()
			{
				return new _ClientRMService_2039(GetRMContext(), GetResourceScheduler(), rmAppManager
					, applicationACLsManager, null, GetRMContext().GetRMDelegationTokenSecretManager
					());
			}

			private sealed class _ClientRMService_2039 : ClientRMService
			{
				public _ClientRMService_2039(RMContext baseArg1, YarnScheduler baseArg2, RMAppManager
					 baseArg3, ApplicationACLsManager baseArg4, QueueACLsManager baseArg5, RMDelegationTokenSecretManager
					 baseArg6)
					: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6)
				{
				}

				/// <exception cref="System.Exception"/>
				protected override void ServiceStart()
				{
				}

				// do nothing
				/// <exception cref="System.Exception"/>
				protected override void ServiceStop()
				{
				}
			}

			//do nothing
			/// <exception cref="System.IO.IOException"/>
			protected internal override void DoSecureLogin()
			{
			}
			// Do nothing.
		}

		// Test does following verification
		// 1. Start RM1 with store patch /tmp
		// 2. Add/remove/replace labels to cluster and node lable and verify
		// 3. Start RM2 with store patch /tmp only
		// 4. Get cluster and node lobel, it should be present by recovering it
		/// <exception cref="System.Exception"/>
		public virtual void TestRMRestartRecoveringNodeLabelManager()
		{
			// Initial FS node label store root dir to a random tmp dir
			FilePath nodeLabelFsStoreDir = new FilePath("target", this.GetType().Name + "-testRMRestartRecoveringNodeLabelManager"
				);
			if (nodeLabelFsStoreDir.Exists())
			{
				FileUtils.DeleteDirectory(nodeLabelFsStoreDir);
			}
			nodeLabelFsStoreDir.DeleteOnExit();
			string nodeLabelFsStoreDirURI = nodeLabelFsStoreDir.ToURI().ToString();
			conf.Set(YarnConfiguration.FsNodeLabelsStoreRootDir, nodeLabelFsStoreDirURI);
			MemoryRMStateStore memStore = new MemoryRMStateStore();
			memStore.Init(conf);
			conf.SetBoolean(YarnConfiguration.NodeLabelsEnabled, true);
			MockRM rm1 = new _MockRM_2081(conf, memStore);
			rm1.Init(conf);
			rm1.Start();
			RMNodeLabelsManager nodeLabelManager = rm1.GetRMContext().GetNodeLabelManager();
			ICollection<string> clusterNodeLabels = new HashSet<string>();
			clusterNodeLabels.AddItem("x");
			clusterNodeLabels.AddItem("y");
			clusterNodeLabels.AddItem("z");
			// Add node label x,y,z
			nodeLabelManager.AddToCluserNodeLabels(clusterNodeLabels);
			// Add node Label to Node h1->x
			NodeId n1 = NodeId.NewInstance("h1", 0);
			nodeLabelManager.AddLabelsToNode(ImmutableMap.Of(n1, ToSet("x")));
			clusterNodeLabels.Remove("z");
			// Remove cluster label z
			nodeLabelManager.RemoveFromClusterNodeLabels(ToSet("z"));
			// Replace nodelabel h1->x,y
			nodeLabelManager.ReplaceLabelsOnNode(ImmutableMap.Of(n1, ToSet("y")));
			// Wait for updating store.It is expected NodeStore update should happen
			// very fast since it has separate dispatcher. So waiting for max 5 seconds,
			// which is sufficient time to update NodeStore.
			int count = 10;
			while (count-- > 0)
			{
				if (nodeLabelManager.GetNodeLabels().Count > 0)
				{
					break;
				}
				Sharpen.Thread.Sleep(500);
			}
			NUnit.Framework.Assert.AreEqual(clusterNodeLabels.Count, nodeLabelManager.GetClusterNodeLabels
				().Count);
			IDictionary<NodeId, ICollection<string>> nodeLabels = nodeLabelManager.GetNodeLabels
				();
			NUnit.Framework.Assert.AreEqual(1, nodeLabelManager.GetNodeLabels().Count);
			NUnit.Framework.Assert.IsTrue(nodeLabels[n1].Equals(ToSet("y")));
			MockRM rm2 = new _MockRM_2131(conf, memStore);
			rm2.Init(conf);
			rm2.Start();
			nodeLabelManager = rm2.GetRMContext().GetNodeLabelManager();
			NUnit.Framework.Assert.AreEqual(clusterNodeLabels.Count, nodeLabelManager.GetClusterNodeLabels
				().Count);
			nodeLabels = nodeLabelManager.GetNodeLabels();
			NUnit.Framework.Assert.AreEqual(1, nodeLabelManager.GetNodeLabels().Count);
			NUnit.Framework.Assert.IsTrue(nodeLabels[n1].Equals(ToSet("y")));
			rm1.Stop();
			rm2.Stop();
		}

		private sealed class _MockRM_2081 : MockRM
		{
			public _MockRM_2081(Configuration baseArg1, RMStateStore baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			protected internal override RMNodeLabelsManager CreateNodeLabelManager()
			{
				RMNodeLabelsManager mgr = new RMNodeLabelsManager();
				mgr.Init(this.GetConfig());
				return mgr;
			}
		}

		private sealed class _MockRM_2131 : MockRM
		{
			public _MockRM_2131(Configuration baseArg1, RMStateStore baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			protected internal override RMNodeLabelsManager CreateNodeLabelManager()
			{
				RMNodeLabelsManager mgr = new RMNodeLabelsManager();
				mgr.Init(this.GetConfig());
				return mgr;
			}
		}

		private ICollection<E> ToSet<E>(params E[] elements)
		{
			ICollection<E> set = Sets.NewHashSet(elements);
			return set;
		}
	}
}
