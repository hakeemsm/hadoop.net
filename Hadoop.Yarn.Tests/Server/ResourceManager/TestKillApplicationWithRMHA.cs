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
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestKillApplicationWithRMHA : RMHATestBase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestKillApplicationWithRMHA
			));

		/// <exception cref="System.Exception"/>
		public virtual void TestKillAppWhenFailoverHappensAtNewState()
		{
			// create a customized RMAppManager
			// During the process of Application submission,
			// the RMAppState will always be NEW.
			// The ApplicationState will not be saved in RMStateStore.
			StartRMsWithCustomizedRMAppManager();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// Submit the application
			RMApp app0 = rm1.SubmitApp(200, string.Empty, UserGroupInformation.GetCurrentUser
				().GetShortUserName(), null, false, null, configuration.GetInt(YarnConfiguration
				.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null, null, false, 
				false);
			// failover and kill application
			// When FailOver happens, the state of this application is NEW,
			// and ApplicationState is not saved in RMStateStore. The active RM
			// can not load the ApplicationState of this application.
			// Expected to get ApplicationNotFoundException
			// when receives the KillApplicationRequest
			try
			{
				FailOverAndKillApp(app0.GetApplicationId(), RMAppState.New);
				NUnit.Framework.Assert.Fail("Should get an exception here");
			}
			catch (ApplicationNotFoundException ex)
			{
				NUnit.Framework.Assert.IsTrue(ex.Message.Contains("Trying to kill an absent application "
					 + app0.GetApplicationId()));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestKillAppWhenFailoverHappensAtRunningState()
		{
			StartRMs();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			// failover and kill application
			// The application is at RUNNING State when failOver happens.
			// Since RMStateStore has already saved ApplicationState, the active RM
			// will load the ApplicationState. After that, the application will be at
			// ACCEPTED State. Because the application is not at Final State,
			// KillApplicationResponse.getIsKillCompleted is expected to return false.
			FailOverAndKillApp(app0.GetApplicationId(), am0.GetApplicationAttemptId(), RMAppState
				.Running, RMAppAttemptState.Running, RMAppState.Accepted);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestKillAppWhenFailoverHappensAtFinalState()
		{
			StartRMs();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			// kill the app.
			rm1.KillApp(app0.GetApplicationId());
			rm1.WaitForState(app0.GetApplicationId(), RMAppState.Killed);
			rm1.WaitForState(am0.GetApplicationAttemptId(), RMAppAttemptState.Killed);
			// failover and kill application
			// The application is at Killed State and RMStateStore has already
			// saved this applicationState. After failover happens, the current
			// active RM will load the ApplicationState whose RMAppState is killed.
			// Because this application is at Final State,
			// KillApplicationResponse.getIsKillCompleted is expected to return true.
			FailOverAndKillApp(app0.GetApplicationId(), am0.GetApplicationAttemptId(), RMAppState
				.Killed, RMAppAttemptState.Killed, RMAppState.Killed);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestKillAppWhenFailOverHappensDuringApplicationKill()
		{
			// create a customized ClientRMService
			// When receives the killApplicationRequest, simply return the response
			// and make sure the application will not be KILLED State
			StartRMsWithCustomizedClientRMService();
			MockNM nm1 = new MockNM("127.0.0.1:1234", 15120, rm1.GetResourceTrackerService());
			nm1.RegisterNode();
			// create app and launch the AM
			RMApp app0 = rm1.SubmitApp(200);
			MockAM am0 = LaunchAM(app0, rm1, nm1);
			// ensure that the app is in running state
			NUnit.Framework.Assert.AreEqual(app0.GetState(), RMAppState.Running);
			// kill the app.
			rm1.KillApp(app0.GetApplicationId());
			// failover happens before this application goes to final state.
			// The RMAppState that will be loaded by the active rm
			// should be ACCEPTED.
			FailOverAndKillApp(app0.GetApplicationId(), am0.GetApplicationAttemptId(), RMAppState
				.Running, RMAppAttemptState.Running, RMAppState.Accepted);
		}

		/// <exception cref="System.Exception"/>
		private void FailOverAndKillApp(ApplicationId appId, ApplicationAttemptId appAttemptId
			, RMAppState initialRMAppState, RMAppAttemptState initialRMAppAttemptState, RMAppState
			 expectedAppStateBeforeKillApp)
		{
			NUnit.Framework.Assert.AreEqual(initialRMAppState, rm1.GetRMContext().GetRMApps()
				[appId].GetState());
			NUnit.Framework.Assert.AreEqual(initialRMAppAttemptState, rm1.GetRMContext().GetRMApps
				()[appId].GetAppAttempts()[appAttemptId].GetState());
			ExplicitFailover();
			NUnit.Framework.Assert.AreEqual(expectedAppStateBeforeKillApp, rm2.GetRMContext()
				.GetRMApps()[appId].GetState());
			KillApplication(rm2, appId, appAttemptId, initialRMAppState);
		}

		/// <exception cref="System.Exception"/>
		private void FailOverAndKillApp(ApplicationId appId, RMAppState initialRMAppState
			)
		{
			NUnit.Framework.Assert.AreEqual(initialRMAppState, rm1.GetRMContext().GetRMApps()
				[appId].GetState());
			ExplicitFailover();
			NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetRMApps()[appId] == null);
			KillApplication(rm2, appId, null, initialRMAppState);
		}

		/// <exception cref="System.IO.IOException"/>
		private void StartRMsWithCustomizedClientRMService()
		{
			Configuration conf1 = new Configuration(confForRM1);
			rm1 = new _MockRM_193(conf1);
			rm2 = new MockRM(confForRM2);
			StartRMs(rm1, conf1, rm2, confForRM2);
		}

		private sealed class _MockRM_193 : MockRM
		{
			public _MockRM_193(Configuration baseArg1)
				: base(baseArg1)
			{
			}

			protected internal override ClientRMService CreateClientRMService()
			{
				return new TestKillApplicationWithRMHA.MyClientRMService(this.rmContext, this.scheduler
					, this.rmAppManager, this.applicationACLsManager, this.queueACLsManager, this.GetRMContext
					().GetRMDelegationTokenSecretManager());
			}
		}

		private class MyClientRMService : ClientRMService
		{
			private RMContext rmContext;

			public MyClientRMService(RMContext rmContext, YarnScheduler scheduler, RMAppManager
				 rmAppManager, ApplicationACLsManager applicationACLsManager, QueueACLsManager queueACLsManager
				, RMDelegationTokenSecretManager rmDTSecretManager)
				: base(rmContext, scheduler, rmAppManager, applicationACLsManager, queueACLsManager
					, rmDTSecretManager)
			{
				this.rmContext = rmContext;
			}

			protected override void ServiceStart()
			{
			}

			// override to not start rpc handler
			protected override void ServiceStop()
			{
			}

			// don't do anything
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public override KillApplicationResponse ForceKillApplication(KillApplicationRequest
				 request)
			{
				ApplicationId applicationId = request.GetApplicationId();
				RMApp application = this.rmContext.GetRMApps()[applicationId];
				if (application.IsAppFinalStateStored())
				{
					return KillApplicationResponse.NewInstance(true);
				}
				else
				{
					return KillApplicationResponse.NewInstance(false);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void KillApplication(MockRM rm, ApplicationId appId, ApplicationAttemptId
			 appAttemptId, RMAppState rmAppState)
		{
			KillApplicationResponse response = rm.KillApp(appId);
			NUnit.Framework.Assert.IsTrue(response.GetIsKillCompleted() == IsFinalState(rmAppState
				));
			RMApp loadedApp0 = rm.GetRMContext().GetRMApps()[appId];
			rm.WaitForState(appId, RMAppState.Killed);
			if (appAttemptId != null)
			{
				rm.WaitForState(appAttemptId, RMAppAttemptState.Killed);
			}
			// no new attempt is created.
			NUnit.Framework.Assert.AreEqual(1, loadedApp0.GetAppAttempts().Count);
		}
	}
}
