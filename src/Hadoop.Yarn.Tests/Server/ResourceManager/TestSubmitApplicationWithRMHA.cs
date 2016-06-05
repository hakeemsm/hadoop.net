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
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestSubmitApplicationWithRMHA : RMHATestBase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestSubmitApplicationWithRMHA
			));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHandleRMHABeforeSubmitApplicationCallWithSavedApplicationState
			()
		{
			// start two RMs, and transit rm1 to active, rm2 to standby
			StartRMs();
			// get a new applicationId from rm1
			ApplicationId appId = rm1.GetNewAppId().GetApplicationId();
			// Do the failover
			ExplicitFailover();
			// submit the application with previous assigned applicationId
			// to current active rm: rm2
			RMApp app1 = rm2.SubmitApp(200, string.Empty, UserGroupInformation.GetCurrentUser
				().GetShortUserName(), null, false, null, configuration.GetInt(YarnConfiguration
				.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null, null, false, 
				false, true, appId);
			// verify application submission
			VerifySubmitApp(rm2, app1, appId);
		}

		/// <exception cref="System.Exception"/>
		private void VerifySubmitApp(MockRM rm, RMApp app, ApplicationId expectedAppId)
		{
			int maxWaittingTimes = 20;
			int count = 0;
			while (true)
			{
				YarnApplicationState state = rm.GetApplicationReport(app.GetApplicationId()).GetYarnApplicationState
					();
				if (!state.Equals(YarnApplicationState.New) && !state.Equals(YarnApplicationState
					.NewSaving))
				{
					break;
				}
				if (count > maxWaittingTimes)
				{
					break;
				}
				Sharpen.Thread.Sleep(200);
				count++;
			}
			// Verify submittion is successful
			YarnApplicationState state_1 = rm.GetApplicationReport(app.GetApplicationId()).GetYarnApplicationState
				();
			NUnit.Framework.Assert.IsTrue(state_1 == YarnApplicationState.Accepted || state_1
				 == YarnApplicationState.Submitted);
			NUnit.Framework.Assert.AreEqual(expectedAppId, app.GetApplicationId());
		}

		// There are two scenarios when RM failover happens
		// after SubmitApplication Call:
		// 1) RMStateStore already saved the ApplicationState when failover happens
		// 2) RMStateStore did not save the ApplicationState when failover happens
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHandleRMHAafterSubmitApplicationCallWithSavedApplicationState
			()
		{
			// Test scenario 1 when RM failover happens
			// after SubmitApplication Call:
			// RMStateStore already saved the ApplicationState when failover happens
			StartRMs();
			// Submit Application
			// After submission, the applicationState will be saved in RMStateStore.
			RMApp app0 = rm1.SubmitApp(200);
			// Do the failover
			ExplicitFailover();
			// Since the applicationState has already been saved in RMStateStore
			// before failover happens, the current active rm can load the previous
			// applicationState.
			ApplicationReport appReport = rm2.GetApplicationReport(app0.GetApplicationId());
			// verify previous submission is successful.
			NUnit.Framework.Assert.IsTrue(appReport.GetYarnApplicationState() == YarnApplicationState
				.Accepted || appReport.GetYarnApplicationState() == YarnApplicationState.Submitted
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHandleRMHAafterSubmitApplicationCallWithoutSavedApplicationState
			()
		{
			// Test scenario 2 when RM failover happens
			// after SubmitApplication Call:
			// RMStateStore did not save the ApplicationState when failover happens.
			// Using customized RMAppManager.
			StartRMsWithCustomizedRMAppManager();
			// Submit Application
			// After submission, the applicationState will
			// not be saved in RMStateStore
			RMApp app0 = rm1.SubmitApp(200, string.Empty, UserGroupInformation.GetCurrentUser
				().GetShortUserName(), null, false, null, configuration.GetInt(YarnConfiguration
				.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null, null, false, 
				false);
			// Do the failover
			ExplicitFailover();
			// Since the applicationState is not saved in RMStateStore
			// when failover happens. The current active RM can not load
			// previous applicationState.
			// Expect ApplicationNotFoundException by calling getApplicationReport().
			try
			{
				rm2.GetApplicationReport(app0.GetApplicationId());
				NUnit.Framework.Assert.Fail("Should get ApplicationNotFoundException here");
			}
			catch (ApplicationNotFoundException)
			{
			}
			// expected ApplicationNotFoundException
			// Submit the application with previous ApplicationId to current active RM
			// This will mimic the similar behavior of YarnClient which will re-submit
			// Application with previous applicationId
			// when catches the ApplicationNotFoundException
			RMApp app1 = rm2.SubmitApp(200, string.Empty, UserGroupInformation.GetCurrentUser
				().GetShortUserName(), null, false, null, configuration.GetInt(YarnConfiguration
				.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null, null, false, 
				false, true, app0.GetApplicationId());
			VerifySubmitApp(rm2, app1, app0.GetApplicationId());
		}

		/// <summary>
		/// Test multiple calls of getApplicationReport, to make sure
		/// it is idempotent
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationReportIdempotent()
		{
			// start two RMs, and transit rm1 to active, rm2 to standby
			StartRMs();
			// Submit Application
			// After submission, the applicationState will be saved in RMStateStore.
			RMApp app = rm1.SubmitApp(200);
			ApplicationReport appReport1 = rm1.GetApplicationReport(app.GetApplicationId());
			NUnit.Framework.Assert.IsTrue(appReport1.GetYarnApplicationState() == YarnApplicationState
				.Accepted || appReport1.GetYarnApplicationState() == YarnApplicationState.Submitted
				);
			// call getApplicationReport again
			ApplicationReport appReport2 = rm1.GetApplicationReport(app.GetApplicationId());
			NUnit.Framework.Assert.AreEqual(appReport1.GetApplicationId(), appReport2.GetApplicationId
				());
			NUnit.Framework.Assert.AreEqual(appReport1.GetYarnApplicationState(), appReport2.
				GetYarnApplicationState());
			// Do the failover
			ExplicitFailover();
			// call getApplicationReport
			ApplicationReport appReport3 = rm2.GetApplicationReport(app.GetApplicationId());
			NUnit.Framework.Assert.AreEqual(appReport1.GetApplicationId(), appReport3.GetApplicationId
				());
			NUnit.Framework.Assert.AreEqual(appReport1.GetYarnApplicationState(), appReport3.
				GetYarnApplicationState());
			// call getApplicationReport again
			ApplicationReport appReport4 = rm2.GetApplicationReport(app.GetApplicationId());
			NUnit.Framework.Assert.AreEqual(appReport3.GetApplicationId(), appReport4.GetApplicationId
				());
			NUnit.Framework.Assert.AreEqual(appReport3.GetYarnApplicationState(), appReport4.
				GetYarnApplicationState());
		}

		// There are two scenarios when RM failover happens
		// during SubmitApplication Call:
		// 1) RMStateStore already saved the ApplicationState when failover happens
		// 2) RMStateStore did not save the ApplicationState when failover happens
		/// <exception cref="System.Exception"/>
		public virtual void TestHandleRMHADuringSubmitApplicationCallWithSavedApplicationState
			()
		{
			// Test scenario 1 when RM failover happens
			// druing SubmitApplication Call:
			// RMStateStore already saved the ApplicationState when failover happens
			StartRMs();
			// Submit Application
			// After submission, the applicationState will be saved in RMStateStore.
			RMApp app0 = rm1.SubmitApp(200);
			// Do the failover
			ExplicitFailover();
			// Since the applicationState has already been saved in RMStateStore
			// before failover happens, the current active rm can load the previous
			// applicationState.
			// This RMApp should exist in the RMContext of current active RM
			NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetRMApps().Contains(app0.GetApplicationId
				()));
			// When we re-submit the application with same applicationId, it will
			// check whether this application has been exist. If yes, just simply
			// return submitApplicationResponse.
			RMApp app1 = rm2.SubmitApp(200, string.Empty, UserGroupInformation.GetCurrentUser
				().GetShortUserName(), null, false, null, configuration.GetInt(YarnConfiguration
				.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null, null, false, 
				false, true, app0.GetApplicationId());
			NUnit.Framework.Assert.AreEqual(app1.GetApplicationId(), app0.GetApplicationId());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHandleRMHADuringSubmitApplicationCallWithoutSavedApplicationState
			()
		{
			// Test scenario 2 when RM failover happens
			// during SubmitApplication Call:
			// RMStateStore did not save the ApplicationState when failover happens.
			// Using customized RMAppManager.
			StartRMsWithCustomizedRMAppManager();
			// Submit Application
			// After submission, the applicationState will
			// not be saved in RMStateStore
			RMApp app0 = rm1.SubmitApp(200, string.Empty, UserGroupInformation.GetCurrentUser
				().GetShortUserName(), null, false, null, configuration.GetInt(YarnConfiguration
				.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null, null, false, 
				false);
			// Do the failover
			ExplicitFailover();
			// When failover happens, the RMStateStore has not saved applicationState.
			// The applicationState of this RMApp is lost.
			// We should not find the RMApp in the RMContext of current active rm.
			NUnit.Framework.Assert.IsFalse(rm2.GetRMContext().GetRMApps().Contains(app0.GetApplicationId
				()));
			// Submit the application with previous ApplicationId to current active RM
			// This will mimic the similar behavior of ApplicationClientProtocol#
			// submitApplication() when failover happens during the submission process
			// because the submitApplication api is marked as idempotent
			RMApp app1 = rm2.SubmitApp(200, string.Empty, UserGroupInformation.GetCurrentUser
				().GetShortUserName(), null, false, null, configuration.GetInt(YarnConfiguration
				.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null, null, false, 
				false, true, app0.GetApplicationId());
			VerifySubmitApp(rm2, app1, app0.GetApplicationId());
			NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetRMApps().Contains(app0.GetApplicationId
				()));
		}
	}
}
