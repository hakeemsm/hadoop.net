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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class RMHATestBase : ClientBaseWithFixes
	{
		private const int ZkTimeoutMs = 5000;

		private static HAServiceProtocol.StateChangeRequestInfo requestInfo = new HAServiceProtocol.StateChangeRequestInfo
			(HAServiceProtocol.RequestSource.RequestByUser);

		protected internal Configuration configuration = new YarnConfiguration();

		internal static MockRM rm1 = null;

		internal static MockRM rm2 = null;

		internal Configuration confForRM1;

		internal Configuration confForRM2;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			configuration.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			configuration.Set(YarnConfiguration.RmHaIds, "rm1,rm2");
			configuration.SetBoolean(YarnConfiguration.RecoveryEnabled, true);
			configuration.Set(YarnConfiguration.RmStore, typeof(ZKRMStateStore).FullName);
			configuration.Set(YarnConfiguration.RmZkAddress, hostPort);
			configuration.SetInt(YarnConfiguration.RmZkTimeoutMs, ZkTimeoutMs);
			configuration.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			configuration.Set(YarnConfiguration.RmClusterId, "test-yarn-cluster");
			int @base = 100;
			foreach (string confKey in YarnConfiguration.GetServiceAddressConfKeys(configuration
				))
			{
				configuration.Set(HAUtil.AddSuffix(confKey, "rm1"), "0.0.0.0:" + (@base + 20));
				configuration.Set(HAUtil.AddSuffix(confKey, "rm2"), "0.0.0.0:" + (@base + 40));
				@base = @base * 2;
			}
			confForRM1 = new Configuration(configuration);
			confForRM1.Set(YarnConfiguration.RmHaId, "rm1");
			confForRM2 = new Configuration(configuration);
			confForRM2.Set(YarnConfiguration.RmHaId, "rm2");
		}

		[TearDown]
		public virtual void Teardown()
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

		/// <exception cref="System.Exception"/>
		protected internal virtual MockAM LaunchAM(RMApp app, MockRM rm, MockNM nm)
		{
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			nm.NodeHeartbeat(true);
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			am.RegisterAppAttempt();
			rm.WaitForState(app.GetApplicationId(), RMAppState.Running);
			rm.WaitForState(app.GetCurrentAppAttempt().GetAppAttemptId(), RMAppAttemptState.Running
				);
			return am;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void StartRMs()
		{
			rm1 = new MockRM(confForRM1, null, false);
			rm2 = new MockRM(confForRM2, null, false);
			StartRMs(rm1, confForRM1, rm2, confForRM2);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void StartRMsWithCustomizedRMAppManager()
		{
			Configuration conf1 = new Configuration(confForRM1);
			rm1 = new _MockRM_118(conf1, conf1);
			rm2 = new MockRM(confForRM2);
			StartRMs(rm1, conf1, rm2, confForRM2);
		}

		private sealed class _MockRM_118 : MockRM
		{
			public _MockRM_118(Configuration conf1, Configuration baseArg1)
				: base(baseArg1)
			{
				this.conf1 = conf1;
			}

			protected internal override RMAppManager CreateRMAppManager()
			{
				return new RMHATestBase.MyRMAppManager(this.rmContext, this.scheduler, this.masterService
					, this.applicationACLsManager, conf1);
			}

			private readonly Configuration conf1;
		}

		private class MyRMAppManager : RMAppManager
		{
			private Configuration conf;

			private RMContext rmContext;

			public MyRMAppManager(RMContext context, YarnScheduler scheduler, ApplicationMasterService
				 masterService, ApplicationACLsManager applicationACLsManager, Configuration conf
				)
				: base(context, scheduler, masterService, applicationACLsManager, conf)
			{
				this.conf = conf;
				this.rmContext = context;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			protected internal override void SubmitApplication(ApplicationSubmissionContext submissionContext
				, long submitTime, string user)
			{
				//Do nothing, just add the application to RMContext
				RMAppImpl application = new RMAppImpl(submissionContext.GetApplicationId(), this.
					rmContext, this.conf, submissionContext.GetApplicationName(), user, submissionContext
					.GetQueue(), submissionContext, this.rmContext.GetScheduler(), this.rmContext.GetApplicationMasterService
					(), submitTime, submissionContext.GetApplicationType(), submissionContext.GetApplicationTags
					(), null);
				this.rmContext.GetRMApps()[submissionContext.GetApplicationId()] = application;
			}
			//Do not send RMAppEventType.START event
			//so the state of Application will not reach to NEW_SAVING state.
		}

		protected internal virtual bool IsFinalState(RMAppState state)
		{
			return state.Equals(RMAppState.Finishing) || state.Equals(RMAppState.Finished) ||
				 state.Equals(RMAppState.Failed) || state.Equals(RMAppState.Killed);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void ExplicitFailover()
		{
			rm1.adminService.TransitionToStandby(requestInfo);
			rm2.adminService.TransitionToActive(requestInfo);
			NUnit.Framework.Assert.IsTrue(rm1.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
				.Standby);
			NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
				.Active);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void StartRMs(MockRM rm1, Configuration confForRM1, MockRM
			 rm2, Configuration confForRM2)
		{
			rm1.Init(confForRM1);
			rm1.Start();
			NUnit.Framework.Assert.IsTrue(rm1.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
				.Standby);
			rm2.Init(confForRM2);
			rm2.Start();
			NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
				.Standby);
			rm1.adminService.TransitionToActive(requestInfo);
			NUnit.Framework.Assert.IsTrue(rm1.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
				.Active);
		}
	}
}
