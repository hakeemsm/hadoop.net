using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager
{
	public class TestContainerManagerRecovery
	{
		private NodeManagerMetrics metrics = NodeManagerMetrics.Create();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationRecovery()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.NmRecoveryEnabled, true);
			conf.Set(YarnConfiguration.NmAddress, "localhost:1234");
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			conf.Set(YarnConfiguration.YarnAdminAcl, "yarn_admin_user");
			NMStateStoreService stateStore = new NMMemoryStateStoreService();
			stateStore.Init(conf);
			stateStore.Start();
			Context context = new NodeManager.NMContext(new NMContainerTokenSecretManager(conf
				), new NMTokenSecretManagerInNM(), null, new ApplicationACLsManager(conf), stateStore
				);
			ContainerManagerImpl cm = CreateContainerManager(context);
			cm.Init(conf);
			cm.Start();
			// simulate registration with RM
			MasterKey masterKey = new MasterKeyPBImpl();
			masterKey.SetKeyId(123);
			masterKey.SetBytes(ByteBuffer.Wrap(new byte[] { 123 }));
			context.GetContainerTokenSecretManager().SetMasterKey(masterKey);
			context.GetNMTokenSecretManager().SetMasterKey(masterKey);
			// add an application by starting a container
			string appUser = "app_user1";
			string modUser = "modify_user1";
			string viewUser = "view_user1";
			string enemyUser = "enemy_user";
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId cid = ContainerId.NewContainerId(attemptId, 1);
			IDictionary<string, LocalResource> localResources = Collections.EmptyMap();
			IDictionary<string, string> containerEnv = Sharpen.Collections.EmptyMap();
			IList<string> containerCmds = Sharpen.Collections.EmptyList();
			IDictionary<string, ByteBuffer> serviceData = Sharpen.Collections.EmptyMap();
			Credentials containerCreds = new Credentials();
			DataOutputBuffer dob = new DataOutputBuffer();
			containerCreds.WriteTokenStorageToStream(dob);
			ByteBuffer containerTokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
			IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>();
			acls[ApplicationAccessType.ModifyApp] = modUser;
			acls[ApplicationAccessType.ViewApp] = viewUser;
			ContainerLaunchContext clc = ContainerLaunchContext.NewInstance(localResources, containerEnv
				, containerCmds, serviceData, containerTokens, acls);
			// create the logAggregationContext
			LogAggregationContext logAggregationContext = LogAggregationContext.NewInstance("includePattern"
				, "excludePattern", "includePatternInRollingAggregation", "excludePatternInRollingAggregation"
				);
			StartContainersResponse startResponse = StartContainer(context, cm, cid, clc, logAggregationContext
				);
			NUnit.Framework.Assert.IsTrue(startResponse.GetFailedRequests().IsEmpty());
			NUnit.Framework.Assert.AreEqual(1, context.GetApplications().Count);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = context.GetApplications()[appId];
			NUnit.Framework.Assert.IsNotNull(app);
			WaitForAppState(app, ApplicationState.Initing);
			NUnit.Framework.Assert.IsTrue(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(modUser), ApplicationAccessType.ModifyApp, appUser, appId));
			NUnit.Framework.Assert.IsFalse(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(viewUser), ApplicationAccessType.ModifyApp, appUser, appId));
			NUnit.Framework.Assert.IsTrue(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(viewUser), ApplicationAccessType.ViewApp, appUser, appId));
			NUnit.Framework.Assert.IsFalse(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(enemyUser), ApplicationAccessType.ViewApp, appUser, appId));
			// reset container manager and verify app recovered with proper acls
			cm.Stop();
			context = new NodeManager.NMContext(new NMContainerTokenSecretManager(conf), new 
				NMTokenSecretManagerInNM(), null, new ApplicationACLsManager(conf), stateStore);
			cm = CreateContainerManager(context);
			cm.Init(conf);
			cm.Start();
			NUnit.Framework.Assert.AreEqual(1, context.GetApplications().Count);
			app = context.GetApplications()[appId];
			NUnit.Framework.Assert.IsNotNull(app);
			// check whether LogAggregationContext is recovered correctly
			LogAggregationContext recovered = ((ApplicationImpl)app).GetLogAggregationContext
				();
			NUnit.Framework.Assert.IsNotNull(recovered);
			NUnit.Framework.Assert.AreEqual(logAggregationContext.GetIncludePattern(), recovered
				.GetIncludePattern());
			NUnit.Framework.Assert.AreEqual(logAggregationContext.GetExcludePattern(), recovered
				.GetExcludePattern());
			NUnit.Framework.Assert.AreEqual(logAggregationContext.GetRolledLogsIncludePattern
				(), recovered.GetRolledLogsIncludePattern());
			NUnit.Framework.Assert.AreEqual(logAggregationContext.GetRolledLogsExcludePattern
				(), recovered.GetRolledLogsExcludePattern());
			WaitForAppState(app, ApplicationState.Initing);
			NUnit.Framework.Assert.IsTrue(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(modUser), ApplicationAccessType.ModifyApp, appUser, appId));
			NUnit.Framework.Assert.IsFalse(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(viewUser), ApplicationAccessType.ModifyApp, appUser, appId));
			NUnit.Framework.Assert.IsTrue(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(viewUser), ApplicationAccessType.ViewApp, appUser, appId));
			NUnit.Framework.Assert.IsFalse(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(enemyUser), ApplicationAccessType.ViewApp, appUser, appId));
			// simulate application completion
			IList<ApplicationId> finishedApps = new AList<ApplicationId>();
			finishedApps.AddItem(appId);
			cm.Handle(new CMgrCompletedAppsEvent(finishedApps, CMgrCompletedAppsEvent.Reason.
				ByResourcemanager));
			WaitForAppState(app, ApplicationState.ApplicationResourcesCleaningup);
			// restart and verify app is marked for finishing
			cm.Stop();
			context = new NodeManager.NMContext(new NMContainerTokenSecretManager(conf), new 
				NMTokenSecretManagerInNM(), null, new ApplicationACLsManager(conf), stateStore);
			cm = CreateContainerManager(context);
			cm.Init(conf);
			cm.Start();
			NUnit.Framework.Assert.AreEqual(1, context.GetApplications().Count);
			app = context.GetApplications()[appId];
			NUnit.Framework.Assert.IsNotNull(app);
			WaitForAppState(app, ApplicationState.ApplicationResourcesCleaningup);
			NUnit.Framework.Assert.IsTrue(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(modUser), ApplicationAccessType.ModifyApp, appUser, appId));
			NUnit.Framework.Assert.IsFalse(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(viewUser), ApplicationAccessType.ModifyApp, appUser, appId));
			NUnit.Framework.Assert.IsTrue(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(viewUser), ApplicationAccessType.ViewApp, appUser, appId));
			NUnit.Framework.Assert.IsFalse(context.GetApplicationACLsManager().CheckAccess(UserGroupInformation
				.CreateRemoteUser(enemyUser), ApplicationAccessType.ViewApp, appUser, appId));
			// simulate log aggregation completion
			app.Handle(new ApplicationEvent(app.GetAppId(), ApplicationEventType.ApplicationResourcesCleanedup
				));
			NUnit.Framework.Assert.AreEqual(app.GetApplicationState(), ApplicationState.Finished
				);
			app.Handle(new ApplicationEvent(app.GetAppId(), ApplicationEventType.ApplicationLogHandlingFinished
				));
			// restart and verify app is no longer present after recovery
			cm.Stop();
			context = new NodeManager.NMContext(new NMContainerTokenSecretManager(conf), new 
				NMTokenSecretManagerInNM(), null, new ApplicationACLsManager(conf), stateStore);
			cm = CreateContainerManager(context);
			cm.Init(conf);
			cm.Start();
			NUnit.Framework.Assert.IsTrue(context.GetApplications().IsEmpty());
			cm.Stop();
		}

		/// <exception cref="System.Exception"/>
		private StartContainersResponse StartContainer(Context context, ContainerManagerImpl
			 cm, ContainerId cid, ContainerLaunchContext clc, LogAggregationContext logAggregationContext
			)
		{
			UserGroupInformation user = UserGroupInformation.CreateRemoteUser(cid.GetApplicationAttemptId
				().ToString());
			StartContainerRequest scReq = StartContainerRequest.NewInstance(clc, TestContainerManager
				.CreateContainerToken(cid, 0, context.GetNodeId(), user.GetShortUserName(), context
				.GetContainerTokenSecretManager(), logAggregationContext));
			IList<StartContainerRequest> scReqList = new AList<StartContainerRequest>();
			scReqList.AddItem(scReq);
			NMTokenIdentifier nmToken = new NMTokenIdentifier(cid.GetApplicationAttemptId(), 
				context.GetNodeId(), user.GetShortUserName(), context.GetNMTokenSecretManager().
				GetCurrentKey().GetKeyId());
			user.AddTokenIdentifier(nmToken);
			return user.DoAs(new _PrivilegedExceptionAction_264(cm, scReqList));
		}

		private sealed class _PrivilegedExceptionAction_264 : PrivilegedExceptionAction<StartContainersResponse
			>
		{
			public _PrivilegedExceptionAction_264(ContainerManagerImpl cm, IList<StartContainerRequest
				> scReqList)
			{
				this.cm = cm;
				this.scReqList = scReqList;
			}

			/// <exception cref="System.Exception"/>
			public StartContainersResponse Run()
			{
				return cm.StartContainers(StartContainersRequest.NewInstance(scReqList));
			}

			private readonly ContainerManagerImpl cm;

			private readonly IList<StartContainerRequest> scReqList;
		}

		/// <exception cref="System.Exception"/>
		private void WaitForAppState(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app, ApplicationState state)
		{
			int msecPerSleep = 10;
			int msecLeft = 5000;
			while (app.GetApplicationState() != state && msecLeft > 0)
			{
				Sharpen.Thread.Sleep(msecPerSleep);
				msecLeft -= msecPerSleep;
			}
			NUnit.Framework.Assert.AreEqual(state, app.GetApplicationState());
		}

		private ContainerManagerImpl CreateContainerManager(Context context)
		{
			LogHandler logHandler = Org.Mockito.Mockito.Mock<LogHandler>();
			ResourceLocalizationService rsrcSrv = new _ResourceLocalizationService_287(null, 
				null, null, null, context);
			// do nothing
			// do nothing
			// do nothing
			ContainersLauncher launcher = new _ContainersLauncher_309(context, null, null, null
				, null);
			// do nothing
			return new _ContainerManagerImpl_319(logHandler, rsrcSrv, launcher, context, Org.Mockito.Mockito.Mock
				<ContainerExecutor>(), Org.Mockito.Mockito.Mock<DeletionService>(), Org.Mockito.Mockito.Mock
				<NodeStatusUpdater>(), metrics, context.GetApplicationACLsManager(), null);
		}

		private sealed class _ResourceLocalizationService_287 : ResourceLocalizationService
		{
			public _ResourceLocalizationService_287(Dispatcher baseArg1, ContainerExecutor baseArg2
				, DeletionService baseArg3, LocalDirsHandlerService baseArg4, Context baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
			}

			public override void Handle(LocalizationEvent @event)
			{
			}
		}

		private sealed class _ContainersLauncher_309 : ContainersLauncher
		{
			public _ContainersLauncher_309(Context baseArg1, Dispatcher baseArg2, ContainerExecutor
				 baseArg3, LocalDirsHandlerService baseArg4, ContainerManagerImpl baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
			}

			public override void Handle(ContainersLauncherEvent @event)
			{
			}
		}

		private sealed class _ContainerManagerImpl_319 : ContainerManagerImpl
		{
			public _ContainerManagerImpl_319(LogHandler logHandler, ResourceLocalizationService
				 rsrcSrv, ContainersLauncher launcher, Context baseArg1, ContainerExecutor baseArg2
				, DeletionService baseArg3, NodeStatusUpdater baseArg4, NodeManagerMetrics baseArg5
				, ApplicationACLsManager baseArg6, LocalDirsHandlerService baseArg7)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7)
			{
				this.logHandler = logHandler;
				this.rsrcSrv = rsrcSrv;
				this.launcher = launcher;
			}

			protected internal override LogHandler CreateLogHandler(Configuration conf, Context
				 context, DeletionService deletionService)
			{
				return logHandler;
			}

			protected internal override ResourceLocalizationService CreateResourceLocalizationService
				(ContainerExecutor exec, DeletionService deletionContext, Context context)
			{
				return rsrcSrv;
			}

			protected internal override ContainersLauncher CreateContainersLauncher(Context context
				, ContainerExecutor exec)
			{
				return launcher;
			}

			public override void SetBlockNewContainerRequests(bool blockNewContainerRequests)
			{
			}

			private readonly LogHandler logHandler;

			private readonly ResourceLocalizationService rsrcSrv;

			private readonly ContainersLauncher launcher;
		}
		// do nothing
	}
}
