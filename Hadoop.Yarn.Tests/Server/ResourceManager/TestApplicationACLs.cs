using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestApplicationACLs
	{
		private const string AppOwner = "owner";

		private const string Friend = "friend";

		private const string Enemy = "enemy";

		private const string QueueAdminUser = "queue-admin-user";

		private const string SuperUser = "superUser";

		private const string FriendlyGroup = "friendly-group";

		private const string SuperGroup = "superGroup";

		private const string Unavailable = "N/A";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
			));

		internal static MockRM resourceManager;

		internal static Configuration conf = new YarnConfiguration();

		internal static readonly YarnRPC rpc = YarnRPC.Create(conf);

		internal static readonly IPEndPoint rmAddress = conf.GetSocketAddr(YarnConfiguration
			.RmAddress, YarnConfiguration.DefaultRmAddress, YarnConfiguration.DefaultRmPort);

		private static ApplicationClientProtocol rmClient;

		private static RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(conf);

		private static bool isQueueUser = false;

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			RMStateStore store = RMStateStoreFactory.GetStore(conf);
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			AccessControlList adminACL = new AccessControlList(string.Empty);
			adminACL.AddGroup(SuperGroup);
			conf.Set(YarnConfiguration.YarnAdminAcl, adminACL.GetAclString());
			resourceManager = new _MockRM_105(conf);
			new _Thread_127().Start();
			int waitCount = 0;
			while (resourceManager.GetServiceState() == Service.STATE.Inited && waitCount++ <
				 60)
			{
				Log.Info("Waiting for RM to start...");
				Sharpen.Thread.Sleep(1500);
			}
			if (resourceManager.GetServiceState() != Service.STATE.Started)
			{
				// RM could have failed.
				throw new IOException("ResourceManager failed to start. Final state is " + resourceManager
					.GetServiceState());
			}
			UserGroupInformation owner = UserGroupInformation.CreateRemoteUser(AppOwner);
			rmClient = owner.DoAs(new _PrivilegedExceptionAction_152());
		}

		private sealed class _MockRM_105 : MockRM
		{
			public _MockRM_105(Configuration baseArg1)
				: base(baseArg1)
			{
			}

			protected internal override QueueACLsManager CreateQueueACLsManager(ResourceScheduler
				 scheduler, Configuration conf)
			{
				QueueACLsManager mockQueueACLsManager = Org.Mockito.Mockito.Mock<QueueACLsManager
					>();
				Org.Mockito.Mockito.When(mockQueueACLsManager.CheckAccess(Matchers.Any<UserGroupInformation
					>(), Matchers.Any<QueueACL>(), Matchers.AnyString())).ThenAnswer(new _Answer_113
					());
				return mockQueueACLsManager;
			}

			private sealed class _Answer_113 : Answer
			{
				public _Answer_113()
				{
				}

				public object Answer(InvocationOnMock invocation)
				{
					return Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs.isQueueUser;
				}
			}

			protected internal override ClientRMService CreateClientRMService()
			{
				return new ClientRMService(this.GetRMContext(), this.scheduler, this.rmAppManager
					, this.applicationACLsManager, this.queueACLsManager, null);
			}
		}

		private sealed class _Thread_127 : Sharpen.Thread
		{
			public _Thread_127()
			{
			}

			public override void Run()
			{
				UserGroupInformation.CreateUserForTesting(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
					.Enemy, new string[] {  });
				UserGroupInformation.CreateUserForTesting(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
					.Friend, new string[] { Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
					.FriendlyGroup });
				UserGroupInformation.CreateUserForTesting(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
					.SuperUser, new string[] { Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
					.SuperGroup });
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs.resourceManager
					.Start();
			}
		}

		private sealed class _PrivilegedExceptionAction_152 : PrivilegedExceptionAction<ApplicationClientProtocol
			>
		{
			public _PrivilegedExceptionAction_152()
			{
			}

			/// <exception cref="System.Exception"/>
			public ApplicationClientProtocol Run()
			{
				return (ApplicationClientProtocol)Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
					.rpc.GetProxy(typeof(ApplicationClientProtocol), Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
					.rmAddress, Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs.conf
					);
			}
		}

		[AfterClass]
		public static void TearDown()
		{
			if (resourceManager != null)
			{
				resourceManager.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationACLs()
		{
			VerifyOwnerAccess();
			VerifySuperUserAccess();
			VerifyFriendAccess();
			VerifyEnemyAccess();
			VerifyAdministerQueueUserAccess();
		}

		/// <exception cref="System.Exception"/>
		private ApplicationId SubmitAppAndGetAppId(AccessControlList viewACL, AccessControlList
			 modifyACL)
		{
			SubmitApplicationRequest submitRequest = recordFactory.NewRecordInstance<SubmitApplicationRequest
				>();
			ApplicationSubmissionContext context = recordFactory.NewRecordInstance<ApplicationSubmissionContext
				>();
			ApplicationId applicationId = rmClient.GetNewApplication(recordFactory.NewRecordInstance
				<GetNewApplicationRequest>()).GetApplicationId();
			context.SetApplicationId(applicationId);
			IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>();
			acls[ApplicationAccessType.ViewApp] = viewACL.GetAclString();
			acls[ApplicationAccessType.ModifyApp] = modifyACL.GetAclString();
			ContainerLaunchContext amContainer = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			Resource resource = BuilderUtils.NewResource(1024, 1);
			context.SetResource(resource);
			amContainer.SetApplicationACLs(acls);
			context.SetAMContainerSpec(amContainer);
			submitRequest.SetApplicationSubmissionContext(context);
			rmClient.SubmitApplication(submitRequest);
			resourceManager.WaitForState(applicationId, RMAppState.Accepted);
			return applicationId;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private ApplicationClientProtocol GetRMClientForUser(string user)
		{
			UserGroupInformation userUGI = UserGroupInformation.CreateRemoteUser(user);
			ApplicationClientProtocol userClient = userUGI.DoAs(new _PrivilegedExceptionAction_217
				());
			return userClient;
		}

		private sealed class _PrivilegedExceptionAction_217 : PrivilegedExceptionAction<ApplicationClientProtocol
			>
		{
			public _PrivilegedExceptionAction_217()
			{
			}

			/// <exception cref="System.Exception"/>
			public ApplicationClientProtocol Run()
			{
				return (ApplicationClientProtocol)Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
					.rpc.GetProxy(typeof(ApplicationClientProtocol), Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
					.rmAddress, Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs.conf
					);
			}
		}

		/// <exception cref="System.Exception"/>
		private void VerifyOwnerAccess()
		{
			AccessControlList viewACL = new AccessControlList(string.Empty);
			viewACL.AddGroup(FriendlyGroup);
			AccessControlList modifyACL = new AccessControlList(string.Empty);
			modifyACL.AddUser(Friend);
			ApplicationId applicationId = SubmitAppAndGetAppId(viewACL, modifyACL);
			GetApplicationReportRequest appReportRequest = recordFactory.NewRecordInstance<GetApplicationReportRequest
				>();
			appReportRequest.SetApplicationId(applicationId);
			KillApplicationRequest finishAppRequest = recordFactory.NewRecordInstance<KillApplicationRequest
				>();
			finishAppRequest.SetApplicationId(applicationId);
			// View as owner
			rmClient.GetApplicationReport(appReportRequest);
			// List apps as owner
			NUnit.Framework.Assert.AreEqual("App view by owner should list the apps!!", 1, rmClient
				.GetApplications(recordFactory.NewRecordInstance<GetApplicationsRequest>()).GetApplicationList
				().Count);
			// Kill app as owner
			rmClient.ForceKillApplication(finishAppRequest);
			resourceManager.WaitForState(applicationId, RMAppState.Killed);
		}

		/// <exception cref="System.Exception"/>
		private void VerifySuperUserAccess()
		{
			AccessControlList viewACL = new AccessControlList(string.Empty);
			viewACL.AddGroup(FriendlyGroup);
			AccessControlList modifyACL = new AccessControlList(string.Empty);
			modifyACL.AddUser(Friend);
			ApplicationId applicationId = SubmitAppAndGetAppId(viewACL, modifyACL);
			GetApplicationReportRequest appReportRequest = recordFactory.NewRecordInstance<GetApplicationReportRequest
				>();
			appReportRequest.SetApplicationId(applicationId);
			KillApplicationRequest finishAppRequest = recordFactory.NewRecordInstance<KillApplicationRequest
				>();
			finishAppRequest.SetApplicationId(applicationId);
			ApplicationClientProtocol superUserClient = GetRMClientForUser(SuperUser);
			// View as the superUser
			superUserClient.GetApplicationReport(appReportRequest);
			// List apps as superUser
			NUnit.Framework.Assert.AreEqual("App view by super-user should list the apps!!", 
				2, superUserClient.GetApplications(recordFactory.NewRecordInstance<GetApplicationsRequest
				>()).GetApplicationList().Count);
			// Kill app as the superUser
			superUserClient.ForceKillApplication(finishAppRequest);
			resourceManager.WaitForState(applicationId, RMAppState.Killed);
		}

		/// <exception cref="System.Exception"/>
		private void VerifyFriendAccess()
		{
			AccessControlList viewACL = new AccessControlList(string.Empty);
			viewACL.AddGroup(FriendlyGroup);
			AccessControlList modifyACL = new AccessControlList(string.Empty);
			modifyACL.AddUser(Friend);
			ApplicationId applicationId = SubmitAppAndGetAppId(viewACL, modifyACL);
			GetApplicationReportRequest appReportRequest = recordFactory.NewRecordInstance<GetApplicationReportRequest
				>();
			appReportRequest.SetApplicationId(applicationId);
			KillApplicationRequest finishAppRequest = recordFactory.NewRecordInstance<KillApplicationRequest
				>();
			finishAppRequest.SetApplicationId(applicationId);
			ApplicationClientProtocol friendClient = GetRMClientForUser(Friend);
			// View as the friend
			friendClient.GetApplicationReport(appReportRequest);
			// List apps as friend
			NUnit.Framework.Assert.AreEqual("App view by a friend should list the apps!!", 3, 
				friendClient.GetApplications(recordFactory.NewRecordInstance<GetApplicationsRequest
				>()).GetApplicationList().Count);
			// Kill app as the friend
			friendClient.ForceKillApplication(finishAppRequest);
			resourceManager.WaitForState(applicationId, RMAppState.Killed);
		}

		/// <exception cref="System.Exception"/>
		private void VerifyEnemyAccess()
		{
			AccessControlList viewACL = new AccessControlList(string.Empty);
			viewACL.AddGroup(FriendlyGroup);
			AccessControlList modifyACL = new AccessControlList(string.Empty);
			modifyACL.AddUser(Friend);
			ApplicationId applicationId = SubmitAppAndGetAppId(viewACL, modifyACL);
			GetApplicationReportRequest appReportRequest = recordFactory.NewRecordInstance<GetApplicationReportRequest
				>();
			appReportRequest.SetApplicationId(applicationId);
			KillApplicationRequest finishAppRequest = recordFactory.NewRecordInstance<KillApplicationRequest
				>();
			finishAppRequest.SetApplicationId(applicationId);
			ApplicationClientProtocol enemyRmClient = GetRMClientForUser(Enemy);
			// View as the enemy
			ApplicationReport appReport = enemyRmClient.GetApplicationReport(appReportRequest
				).GetApplicationReport();
			VerifyEnemyAppReport(appReport);
			// List apps as enemy
			IList<ApplicationReport> appReports = enemyRmClient.GetApplications(recordFactory
				.NewRecordInstance<GetApplicationsRequest>()).GetApplicationList();
			NUnit.Framework.Assert.AreEqual("App view by enemy should list the apps!!", 4, appReports
				.Count);
			foreach (ApplicationReport report in appReports)
			{
				VerifyEnemyAppReport(report);
			}
			// Kill app as the enemy
			try
			{
				enemyRmClient.ForceKillApplication(finishAppRequest);
				NUnit.Framework.Assert.Fail("App killing by the enemy should fail!!");
			}
			catch (YarnException e)
			{
				Log.Info("Got exception while killing app as the enemy", e);
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("User enemy cannot perform operation MODIFY_APP on "
					 + applicationId));
			}
			rmClient.ForceKillApplication(finishAppRequest);
		}

		private void VerifyEnemyAppReport(ApplicationReport appReport)
		{
			NUnit.Framework.Assert.AreEqual("Enemy should not see app host!", Unavailable, appReport
				.GetHost());
			NUnit.Framework.Assert.AreEqual("Enemy should not see app rpc port!", -1, appReport
				.GetRpcPort());
			NUnit.Framework.Assert.AreEqual("Enemy should not see app client token!", null, appReport
				.GetClientToAMToken());
			NUnit.Framework.Assert.AreEqual("Enemy should not see app diagnostics!", Unavailable
				, appReport.GetDiagnostics());
			NUnit.Framework.Assert.AreEqual("Enemy should not see app tracking url!", Unavailable
				, appReport.GetTrackingUrl());
			NUnit.Framework.Assert.AreEqual("Enemy should not see app original tracking url!"
				, Unavailable, appReport.GetOriginalTrackingUrl());
			ApplicationResourceUsageReport usageReport = appReport.GetApplicationResourceUsageReport
				();
			NUnit.Framework.Assert.AreEqual("Enemy should not see app used containers", -1, usageReport
				.GetNumUsedContainers());
			NUnit.Framework.Assert.AreEqual("Enemy should not see app reserved containers", -
				1, usageReport.GetNumReservedContainers());
			NUnit.Framework.Assert.AreEqual("Enemy should not see app used resources", -1, usageReport
				.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual("Enemy should not see app reserved resources", -1
				, usageReport.GetReservedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual("Enemy should not see app needed resources", -1, 
				usageReport.GetNeededResources().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		private void VerifyAdministerQueueUserAccess()
		{
			isQueueUser = true;
			AccessControlList viewACL = new AccessControlList(string.Empty);
			viewACL.AddGroup(FriendlyGroup);
			AccessControlList modifyACL = new AccessControlList(string.Empty);
			modifyACL.AddUser(Friend);
			ApplicationId applicationId = SubmitAppAndGetAppId(viewACL, modifyACL);
			GetApplicationReportRequest appReportRequest = recordFactory.NewRecordInstance<GetApplicationReportRequest
				>();
			appReportRequest.SetApplicationId(applicationId);
			KillApplicationRequest finishAppRequest = recordFactory.NewRecordInstance<KillApplicationRequest
				>();
			finishAppRequest.SetApplicationId(applicationId);
			ApplicationClientProtocol administerQueueUserRmClient = GetRMClientForUser(QueueAdminUser
				);
			// View as the administerQueueUserRmClient
			administerQueueUserRmClient.GetApplicationReport(appReportRequest);
			// List apps as administerQueueUserRmClient
			NUnit.Framework.Assert.AreEqual("App view by queue-admin-user should list the apps!!"
				, 5, administerQueueUserRmClient.GetApplications(recordFactory.NewRecordInstance
				<GetApplicationsRequest>()).GetApplicationList().Count);
			// Kill app as the administerQueueUserRmClient
			administerQueueUserRmClient.ForceKillApplication(finishAppRequest);
			resourceManager.WaitForState(applicationId, RMAppState.Killed);
		}
	}
}
