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
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public abstract class QueueACLsTestBase
	{
		protected internal const string CommonUser = "common_user";

		protected internal const string QueueAUser = "queueA_user";

		protected internal const string QueueBUser = "queueB_user";

		protected internal const string RootAdmin = "root_admin";

		protected internal const string QueueAAdmin = "queueA_admin";

		protected internal const string QueueBAdmin = "queueB_admin";

		protected internal const string Queuea = "queueA";

		protected internal const string Queueb = "queueB";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestApplicationACLs
			));

		internal MockRM resourceManager;

		internal Configuration conf;

		internal YarnRPC rpc;

		internal IPEndPoint rmAddress;

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = CreateConfiguration();
			rpc = YarnRPC.Create(conf);
			rmAddress = conf.GetSocketAddr(YarnConfiguration.RmAddress, YarnConfiguration.DefaultRmAddress
				, YarnConfiguration.DefaultRmPort);
			AccessControlList adminACL = new AccessControlList(string.Empty);
			conf.Set(YarnConfiguration.YarnAdminAcl, adminACL.GetAclString());
			resourceManager = new _MockRM_85(conf);
			new _Thread_96(this).Start();
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
		}

		private sealed class _MockRM_85 : MockRM
		{
			public _MockRM_85(Configuration baseArg1)
				: base(baseArg1)
			{
			}

			protected internal override ClientRMService CreateClientRMService()
			{
				return new ClientRMService(this.GetRMContext(), this.scheduler, this.rmAppManager
					, this.applicationACLsManager, this.queueACLsManager, this.GetRMContext().GetRMDelegationTokenSecretManager
					());
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void DoSecureLogin()
			{
			}
		}

		private sealed class _Thread_96 : Sharpen.Thread
		{
			public _Thread_96(QueueACLsTestBase _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				this._enclosing.resourceManager.Start();
			}

			private readonly QueueACLsTestBase _enclosing;
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
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
			VerifyKillAppSuccess(QueueAUser, QueueAUser, Queuea, true);
			VerifyKillAppSuccess(QueueAUser, QueueAAdmin, Queuea, true);
			VerifyKillAppSuccess(QueueAUser, CommonUser, Queuea, true);
			VerifyKillAppSuccess(QueueAUser, RootAdmin, Queuea, true);
			VerifyKillAppFailure(QueueAUser, QueueBUser, Queuea, true);
			VerifyKillAppFailure(QueueAUser, QueueBAdmin, Queuea, true);
			VerifyKillAppSuccess(QueueBUser, QueueBUser, Queueb, true);
			VerifyKillAppSuccess(QueueBUser, QueueBAdmin, Queueb, true);
			VerifyKillAppSuccess(QueueBUser, CommonUser, Queueb, true);
			VerifyKillAppSuccess(QueueBUser, RootAdmin, Queueb, true);
			VerifyKillAppFailure(QueueBUser, QueueAUser, Queueb, true);
			VerifyKillAppFailure(QueueBUser, QueueAAdmin, Queueb, true);
			VerifyKillAppSuccess(RootAdmin, RootAdmin, Queuea, false);
			VerifyKillAppSuccess(RootAdmin, RootAdmin, Queueb, false);
			VerifyGetClientAMToken(QueueAUser, RootAdmin, Queuea, true);
		}

		/// <exception cref="System.Exception"/>
		private void VerifyGetClientAMToken(string submitter, string queueAdmin, string queueName
			, bool setupACLs)
		{
			ApplicationId applicationId = SubmitAppAndGetAppId(submitter, queueName, setupACLs
				);
			GetApplicationReportRequest appReportRequest = GetApplicationReportRequest.NewInstance
				(applicationId);
			ApplicationClientProtocol submitterClient = GetRMClientForUser(submitter);
			ApplicationClientProtocol adMinUserClient = GetRMClientForUser(queueAdmin);
			GetApplicationReportResponse submitterGetReport = submitterClient.GetApplicationReport
				(appReportRequest);
			GetApplicationReportResponse adMinUserGetReport = adMinUserClient.GetApplicationReport
				(appReportRequest);
			NUnit.Framework.Assert.AreEqual(submitterGetReport.GetApplicationReport().GetClientToAMToken
				(), adMinUserGetReport.GetApplicationReport().GetClientToAMToken());
		}

		/// <exception cref="System.Exception"/>
		private void VerifyKillAppFailure(string submitter, string killer, string queueName
			, bool setupACLs)
		{
			ApplicationId applicationId = SubmitAppAndGetAppId(submitter, queueName, setupACLs
				);
			KillApplicationRequest finishAppRequest = KillApplicationRequest.NewInstance(applicationId
				);
			ApplicationClientProtocol killerClient = GetRMClientForUser(killer);
			// Kill app as the killer
			try
			{
				killerClient.ForceKillApplication(finishAppRequest);
				NUnit.Framework.Assert.Fail("App killing by the enemy should fail!!");
			}
			catch (YarnException e)
			{
				Log.Info("Got exception while killing app as the enemy", e);
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("User " + killer + " cannot perform operation MODIFY_APP on "
					 + applicationId));
			}
			GetRMClientForUser(submitter).ForceKillApplication(finishAppRequest);
		}

		/// <exception cref="System.Exception"/>
		private void VerifyKillAppSuccess(string submitter, string killer, string queueName
			, bool setupACLs)
		{
			ApplicationId applicationId = SubmitAppAndGetAppId(submitter, queueName, setupACLs
				);
			KillApplicationRequest finishAppRequest = KillApplicationRequest.NewInstance(applicationId
				);
			ApplicationClientProtocol ownerClient = GetRMClientForUser(killer);
			// Kill app as killer
			ownerClient.ForceKillApplication(finishAppRequest);
			resourceManager.WaitForState(applicationId, RMAppState.Killed);
		}

		/// <exception cref="System.Exception"/>
		private ApplicationId SubmitAppAndGetAppId(string submitter, string queueName, bool
			 setupACLs)
		{
			GetNewApplicationRequest newAppRequest = GetNewApplicationRequest.NewInstance();
			ApplicationClientProtocol submitterClient = GetRMClientForUser(submitter);
			ApplicationId applicationId = submitterClient.GetNewApplication(newAppRequest).GetApplicationId
				();
			Resource resource = BuilderUtils.NewResource(1024, 1);
			IDictionary<ApplicationAccessType, string> acls = CreateACLs(submitter, setupACLs
				);
			ContainerLaunchContext amContainerSpec = ContainerLaunchContext.NewInstance(null, 
				null, null, null, null, acls);
			ApplicationSubmissionContext appSubmissionContext = ApplicationSubmissionContext.
				NewInstance(applicationId, "applicationName", queueName, null, amContainerSpec, 
				false, true, 1, resource, "applicationType");
			appSubmissionContext.SetApplicationId(applicationId);
			appSubmissionContext.SetQueue(queueName);
			SubmitApplicationRequest submitRequest = SubmitApplicationRequest.NewInstance(appSubmissionContext
				);
			submitterClient.SubmitApplication(submitRequest);
			resourceManager.WaitForState(applicationId, RMAppState.Accepted);
			return applicationId;
		}

		private IDictionary<ApplicationAccessType, string> CreateACLs(string submitter, bool
			 setupACLs)
		{
			AccessControlList viewACL = new AccessControlList(string.Empty);
			AccessControlList modifyACL = new AccessControlList(string.Empty);
			if (setupACLs)
			{
				viewACL.AddUser(submitter);
				viewACL.AddUser(CommonUser);
				modifyACL.AddUser(submitter);
				modifyACL.AddUser(CommonUser);
			}
			IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>();
			acls[ApplicationAccessType.ViewApp] = viewACL.GetAclString();
			acls[ApplicationAccessType.ModifyApp] = modifyACL.GetAclString();
			return acls;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private ApplicationClientProtocol GetRMClientForUser(string user)
		{
			UserGroupInformation userUGI = UserGroupInformation.CreateRemoteUser(user);
			ApplicationClientProtocol userClient = userUGI.DoAs(new _PrivilegedExceptionAction_257
				(this));
			return userClient;
		}

		private sealed class _PrivilegedExceptionAction_257 : PrivilegedExceptionAction<ApplicationClientProtocol
			>
		{
			public _PrivilegedExceptionAction_257(QueueACLsTestBase _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public ApplicationClientProtocol Run()
			{
				return (ApplicationClientProtocol)this._enclosing.rpc.GetProxy(typeof(ApplicationClientProtocol
					), this._enclosing.rmAddress, this._enclosing.conf);
			}

			private readonly QueueACLsTestBase _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract Configuration CreateConfiguration();
	}
}
