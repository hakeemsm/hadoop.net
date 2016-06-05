using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestMoveApplication
	{
		private ResourceManager resourceManager = null;

		private static bool failMove;

		private Configuration conf;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(TestMoveApplication.FifoSchedulerWithMove
				), typeof(TestMoveApplication.FifoSchedulerWithMove));
			conf.Set(YarnConfiguration.YarnAdminAcl, " ");
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			resourceManager = new ResourceManager();
			resourceManager.Init(conf);
			resourceManager.GetRMContext().GetContainerTokenSecretManager().RollMasterKey();
			resourceManager.GetRMContext().GetNMTokenSecretManager().RollMasterKey();
			resourceManager.Start();
			failMove = false;
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			resourceManager.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveRejectedByScheduler()
		{
			failMove = true;
			// Submit application
			Application application = new Application("user1", resourceManager);
			application.Submit();
			// Wait for app to be accepted
			RMApp app = resourceManager.rmContext.GetRMApps()[application.GetApplicationId()];
			while (app.GetState() != RMAppState.Accepted)
			{
				Sharpen.Thread.Sleep(100);
			}
			ClientRMService clientRMService = resourceManager.GetClientRMService();
			try
			{
				// FIFO scheduler does not support moves
				clientRMService.MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest.NewInstance
					(application.GetApplicationId(), "newqueue"));
				NUnit.Framework.Assert.Fail("Should have hit exception");
			}
			catch (YarnException ex)
			{
				NUnit.Framework.Assert.AreEqual("Move not supported", ex.InnerException.Message);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMoveTooLate()
		{
			// Submit application
			Application application = new Application("user1", resourceManager);
			ApplicationId appId = application.GetApplicationId();
			application.Submit();
			ClientRMService clientRMService = resourceManager.GetClientRMService();
			// Kill the application
			clientRMService.ForceKillApplication(KillApplicationRequest.NewInstance(appId));
			RMApp rmApp = resourceManager.GetRMContext().GetRMApps()[appId];
			// wait until it's dead
			while (rmApp.GetState() != RMAppState.Killed)
			{
				Sharpen.Thread.Sleep(100);
			}
			try
			{
				clientRMService.MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest.NewInstance
					(appId, "newqueue"));
				NUnit.Framework.Assert.Fail("Should have hit exception");
			}
			catch (YarnException ex)
			{
				NUnit.Framework.Assert.AreEqual(typeof(YarnException), ex.GetType());
				NUnit.Framework.Assert.AreEqual("App in KILLED state cannot be moved.", ex.Message
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMoveSuccessful()
		{
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			RMApp app = rm1.SubmitApp(1024);
			ClientRMService clientRMService = rm1.GetClientRMService();
			// FIFO scheduler does not support moves
			clientRMService.MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest.NewInstance
				(app.GetApplicationId(), "newqueue"));
			RMApp rmApp = rm1.GetRMContext().GetRMApps()[app.GetApplicationId()];
			NUnit.Framework.Assert.AreEqual("newqueue", rmApp.GetQueue());
			rm1.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMoveRejectedByPermissions()
		{
			failMove = true;
			// Submit application
			Application application = new Application("user1", resourceManager);
			application.Submit();
			ClientRMService clientRMService = resourceManager.GetClientRMService();
			try
			{
				UserGroupInformation.CreateRemoteUser("otheruser").DoAs(new _PrivilegedExceptionAction_151
					(clientRMService, application));
				NUnit.Framework.Assert.Fail("Should have hit exception");
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.AreEqual(typeof(AccessControlException), ex.InnerException
					.InnerException.GetType());
			}
		}

		private sealed class _PrivilegedExceptionAction_151 : PrivilegedExceptionAction<MoveApplicationAcrossQueuesResponse
			>
		{
			public _PrivilegedExceptionAction_151(ClientRMService clientRMService, Application
				 application)
			{
				this.clientRMService = clientRMService;
				this.application = application;
			}

			/// <exception cref="System.Exception"/>
			public MoveApplicationAcrossQueuesResponse Run()
			{
				return clientRMService.MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest
					.NewInstance(application.GetApplicationId(), "newqueue"));
			}

			private readonly ClientRMService clientRMService;

			private readonly Application application;
		}

		public class FifoSchedulerWithMove : FifoScheduler
		{
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public override string MoveApplication(ApplicationId appId, string newQueue)
			{
				if (failMove)
				{
					throw new YarnException("Move not supported");
				}
				return newQueue;
			}

			public override bool CheckAccess(UserGroupInformation callerUGI, QueueACL acl, string
				 queueName)
			{
				lock (this)
				{
					return acl != QueueACL.AdministerQueue;
				}
			}
		}
	}
}
