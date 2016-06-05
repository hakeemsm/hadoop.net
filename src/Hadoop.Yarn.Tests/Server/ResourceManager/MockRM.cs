using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class MockRM : ResourceManager
	{
		internal const string EnableWebapp = "mockrm.webapp.enabled";

		private readonly bool useNullRMNodeLabelsManager;

		public MockRM()
			: this(new YarnConfiguration())
		{
		}

		public MockRM(Configuration conf)
			: this(conf, null)
		{
		}

		public MockRM(Configuration conf, RMStateStore store)
			: this(conf, store, true)
		{
		}

		public MockRM(Configuration conf, RMStateStore store, bool useNullRMNodeLabelsManager
			)
			: base()
		{
			this.useNullRMNodeLabelsManager = useNullRMNodeLabelsManager;
			Init(conf is YarnConfiguration ? conf : new YarnConfiguration(conf));
			if (store != null)
			{
				SetRMStateStore(store);
			}
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
		}

		/// <exception cref="Sharpen.InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		protected internal override RMNodeLabelsManager CreateNodeLabelManager()
		{
			if (useNullRMNodeLabelsManager)
			{
				RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
				mgr.Init(GetConfig());
				return mgr;
			}
			else
			{
				return base.CreateNodeLabelManager();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForState(ApplicationId appId, RMAppState finalState)
		{
			RMApp app = GetRMContext().GetRMApps()[appId];
			NUnit.Framework.Assert.IsNotNull("app shouldn't be null", app);
			int timeoutSecs = 0;
			while (!finalState.Equals(app.GetState()) && timeoutSecs++ < 40)
			{
				System.Console.Out.WriteLine("App : " + appId + " State is : " + app.GetState() +
					 " Waiting for state : " + finalState);
				Sharpen.Thread.Sleep(2000);
			}
			System.Console.Out.WriteLine("App State is : " + app.GetState());
			NUnit.Framework.Assert.AreEqual("App state is not correct (timedout)", finalState
				, app.GetState());
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForState(ApplicationAttemptId attemptId, RMAppAttemptState
			 finalState)
		{
			WaitForState(attemptId, finalState, 40000);
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForState(ApplicationAttemptId attemptId, RMAppAttemptState
			 finalState, int timeoutMsecs)
		{
			RMApp app = GetRMContext().GetRMApps()[attemptId.GetApplicationId()];
			NUnit.Framework.Assert.IsNotNull("app shouldn't be null", app);
			RMAppAttempt attempt = app.GetRMAppAttempt(attemptId);
			int timeoutSecs = 0;
			while (!finalState.Equals(attempt.GetAppAttemptState()) && timeoutSecs++ < timeoutMsecs
				)
			{
				System.Console.Out.WriteLine("AppAttempt : " + attemptId + " State is : " + attempt
					.GetAppAttemptState() + " Waiting for state : " + finalState);
				Sharpen.Thread.Sleep(1000);
			}
			System.Console.Out.WriteLine("Attempt State is : " + attempt.GetAppAttemptState()
				);
			NUnit.Framework.Assert.AreEqual("Attempt state is not correct (timedout)", finalState
				, attempt.GetAppAttemptState());
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForContainerAllocated(MockNM nm, ContainerId containerId)
		{
			int timeoutSecs = 0;
			while (GetResourceScheduler().GetRMContainer(containerId) == null && timeoutSecs++
				 < 40)
			{
				System.Console.Out.WriteLine("Waiting for" + containerId + " to be allocated.");
				nm.NodeHeartbeat(true);
				Sharpen.Thread.Sleep(200);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForContainerToComplete(RMAppAttempt attempt, NMContainerStatus
			 completedContainer)
		{
			while (true)
			{
				IList<ContainerStatus> containers = attempt.GetJustFinishedContainers();
				System.Console.Out.WriteLine("Received completed containers " + containers);
				foreach (ContainerStatus container in containers)
				{
					if (container.GetContainerId().Equals(completedContainer.GetContainerId()))
					{
						return;
					}
				}
				Sharpen.Thread.Sleep(200);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual MockAM WaitForNewAMToLaunchAndRegister(ApplicationId appId, int attemptSize
			, MockNM nm)
		{
			RMApp app = GetRMContext().GetRMApps()[appId];
			NUnit.Framework.Assert.IsNotNull(app);
			while (app.GetAppAttempts().Count != attemptSize)
			{
				System.Console.Out.WriteLine("Application " + appId + " is waiting for AM to restart. Current has "
					 + app.GetAppAttempts().Count + " attempts.");
				Sharpen.Thread.Sleep(200);
			}
			return LaunchAndRegisterAM(app, this, nm);
		}

		/// <exception cref="System.Exception"/>
		public virtual bool WaitForState(MockNM nm, ContainerId containerId, RMContainerState
			 containerState)
		{
			// default is wait for 30,000 ms
			return WaitForState(nm, containerId, containerState, 30 * 1000);
		}

		/// <exception cref="System.Exception"/>
		public virtual bool WaitForState(MockNM nm, ContainerId containerId, RMContainerState
			 containerState, int timeoutMillisecs)
		{
			RMContainer container = GetResourceScheduler().GetRMContainer(containerId);
			int timeoutSecs = 0;
			while (container == null && timeoutSecs++ < timeoutMillisecs / 100)
			{
				nm.NodeHeartbeat(true);
				container = GetResourceScheduler().GetRMContainer(containerId);
				System.Console.Out.WriteLine("Waiting for container " + containerId + " to be allocated."
					);
				Sharpen.Thread.Sleep(100);
				if (timeoutMillisecs <= timeoutSecs * 100)
				{
					return false;
				}
			}
			NUnit.Framework.Assert.IsNotNull("Container shouldn't be null", container);
			while (!containerState.Equals(container.GetState()) && timeoutSecs++ < timeoutMillisecs
				 / 100)
			{
				System.Console.Out.WriteLine("Container : " + containerId + " State is : " + container
					.GetState() + " Waiting for state : " + containerState);
				nm.NodeHeartbeat(true);
				Sharpen.Thread.Sleep(100);
				if (timeoutMillisecs <= timeoutSecs * 100)
				{
					return false;
				}
			}
			System.Console.Out.WriteLine("Container State is : " + container.GetState());
			NUnit.Framework.Assert.AreEqual("Container state is not correct (timedout)", containerState
				, container.GetState());
			return true;
		}

		// get new application id
		/// <exception cref="System.Exception"/>
		public virtual GetNewApplicationResponse GetNewAppId()
		{
			ApplicationClientProtocol client = GetClientRMService();
			return client.GetNewApplication(Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetNewApplicationRequest
				>());
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory)
		{
			return SubmitApp(masterMemory, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, bool unmanaged)
		{
			return SubmitApp(masterMemory, string.Empty, UserGroupInformation.GetCurrentUser(
				).GetShortUserName(), unmanaged);
		}

		// client
		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user)
		{
			return SubmitApp(masterMemory, name, user, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user, bool unmanaged
			)
		{
			return SubmitApp(masterMemory, name, user, null, unmanaged, null, base.GetConfig(
				).GetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
				), null);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user, IDictionary
			<ApplicationAccessType, string> acls)
		{
			return SubmitApp(masterMemory, name, user, acls, false, null, base.GetConfig().GetInt
				(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user, IDictionary
			<ApplicationAccessType, string> acls, string queue)
		{
			return SubmitApp(masterMemory, name, user, acls, false, queue, base.GetConfig().GetInt
				(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user, IDictionary
			<ApplicationAccessType, string> acls, string queue, bool waitForAccepted)
		{
			return SubmitApp(masterMemory, name, user, acls, false, queue, base.GetConfig().GetInt
				(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null
				, null, waitForAccepted);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user, IDictionary
			<ApplicationAccessType, string> acls, bool unmanaged, string queue, int maxAppAttempts
			, Credentials ts)
		{
			return SubmitApp(masterMemory, name, user, acls, unmanaged, queue, maxAppAttempts
				, ts, null);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user, IDictionary
			<ApplicationAccessType, string> acls, bool unmanaged, string queue, int maxAppAttempts
			, Credentials ts, string appType)
		{
			return SubmitApp(masterMemory, name, user, acls, unmanaged, queue, maxAppAttempts
				, ts, appType, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user, IDictionary
			<ApplicationAccessType, string> acls, bool unmanaged, string queue, int maxAppAttempts
			, Credentials ts, string appType, bool waitForAccepted)
		{
			return SubmitApp(masterMemory, name, user, acls, unmanaged, queue, maxAppAttempts
				, ts, appType, waitForAccepted, false);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user, IDictionary
			<ApplicationAccessType, string> acls, bool unmanaged, string queue, int maxAppAttempts
			, Credentials ts, string appType, bool waitForAccepted, bool keepContainers)
		{
			return SubmitApp(masterMemory, name, user, acls, unmanaged, queue, maxAppAttempts
				, ts, appType, waitForAccepted, keepContainers, false, null, 0, null, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, long attemptFailuresValidityInterval
			)
		{
			return SubmitApp(masterMemory, string.Empty, UserGroupInformation.GetCurrentUser(
				).GetShortUserName(), null, false, null, base.GetConfig().GetInt(YarnConfiguration
				.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null, null, true, false
				, false, null, attemptFailuresValidityInterval, null, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user, IDictionary
			<ApplicationAccessType, string> acls, bool unmanaged, string queue, int maxAppAttempts
			, Credentials ts, string appType, bool waitForAccepted, bool keepContainers, bool
			 isAppIdProvided, ApplicationId applicationId)
		{
			return SubmitApp(masterMemory, name, user, acls, unmanaged, queue, maxAppAttempts
				, ts, appType, waitForAccepted, keepContainers, isAppIdProvided, applicationId, 
				0, null, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, LogAggregationContext logAggregationContext
			)
		{
			return SubmitApp(masterMemory, string.Empty, UserGroupInformation.GetCurrentUser(
				).GetShortUserName(), null, false, null, base.GetConfig().GetInt(YarnConfiguration
				.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts), null, null, true, false
				, false, null, 0, logAggregationContext, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual RMApp SubmitApp(int masterMemory, string name, string user, IDictionary
			<ApplicationAccessType, string> acls, bool unmanaged, string queue, int maxAppAttempts
			, Credentials ts, string appType, bool waitForAccepted, bool keepContainers, bool
			 isAppIdProvided, ApplicationId applicationId, long attemptFailuresValidityInterval
			, LogAggregationContext logAggregationContext, bool cancelTokensWhenComplete)
		{
			ApplicationId appId = isAppIdProvided ? applicationId : null;
			ApplicationClientProtocol client = GetClientRMService();
			if (!isAppIdProvided)
			{
				GetNewApplicationResponse resp = client.GetNewApplication(Org.Apache.Hadoop.Yarn.Util.Records
					.NewRecord<GetNewApplicationRequest>());
				appId = resp.GetApplicationId();
			}
			SubmitApplicationRequest req = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<SubmitApplicationRequest
				>();
			ApplicationSubmissionContext sub = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				ApplicationSubmissionContext>();
			sub.SetKeepContainersAcrossApplicationAttempts(keepContainers);
			sub.SetApplicationId(appId);
			sub.SetApplicationName(name);
			sub.SetMaxAppAttempts(maxAppAttempts);
			if (unmanaged)
			{
				sub.SetUnmanagedAM(true);
			}
			if (queue != null)
			{
				sub.SetQueue(queue);
			}
			sub.SetApplicationType(appType);
			ContainerLaunchContext clc = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ContainerLaunchContext
				>();
			Resource capability = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Resource>();
			capability.SetMemory(masterMemory);
			sub.SetResource(capability);
			clc.SetApplicationACLs(acls);
			if (ts != null && UserGroupInformation.IsSecurityEnabled())
			{
				DataOutputBuffer dob = new DataOutputBuffer();
				ts.WriteTokenStorageToStream(dob);
				ByteBuffer securityTokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
				clc.SetTokens(securityTokens);
			}
			sub.SetAMContainerSpec(clc);
			sub.SetAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
			if (logAggregationContext != null)
			{
				sub.SetLogAggregationContext(logAggregationContext);
			}
			sub.SetCancelTokensWhenComplete(cancelTokensWhenComplete);
			req.SetApplicationSubmissionContext(sub);
			UserGroupInformation fakeUser = UserGroupInformation.CreateUserForTesting(user, new 
				string[] { "someGroup" });
			PrivilegedAction<SubmitApplicationResponse> action = new _PrivilegedAction_415().
				SetClientReq(client, req);
			fakeUser.DoAs(action);
			// make sure app is immediately available after submit
			if (waitForAccepted)
			{
				WaitForState(appId, RMAppState.Accepted);
			}
			RMApp rmApp = GetRMContext().GetRMApps()[appId];
			// unmanaged AM won't go to RMAppAttemptState.SCHEDULED.
			if (waitForAccepted && !unmanaged)
			{
				WaitForState(rmApp.GetCurrentAppAttempt().GetAppAttemptId(), RMAppAttemptState.Scheduled
					);
			}
			return rmApp;
		}

		private sealed class _PrivilegedAction_415 : PrivilegedAction<SubmitApplicationResponse
			>
		{
			public _PrivilegedAction_415()
			{
			}

			internal ApplicationClientProtocol client;

			internal SubmitApplicationRequest req;

			public SubmitApplicationResponse Run()
			{
				try
				{
					return this.client.SubmitApplication(this.req);
				}
				catch (YarnException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
				catch (IOException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
				return null;
			}

			internal PrivilegedAction<SubmitApplicationResponse> SetClientReq(ApplicationClientProtocol
				 client, SubmitApplicationRequest req)
			{
				this.client = client;
				this.req = req;
				return this;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual MockNM RegisterNode(string nodeIdStr, int memory)
		{
			MockNM nm = new MockNM(nodeIdStr, memory, GetResourceTrackerService());
			nm.RegisterNode();
			return nm;
		}

		/// <exception cref="System.Exception"/>
		public virtual MockNM RegisterNode(string nodeIdStr, int memory, int vCores)
		{
			MockNM nm = new MockNM(nodeIdStr, memory, vCores, GetResourceTrackerService());
			nm.RegisterNode();
			return nm;
		}

		/// <exception cref="System.Exception"/>
		public virtual MockNM RegisterNode(string nodeIdStr, int memory, int vCores, IList
			<ApplicationId> runningApplications)
		{
			MockNM nm = new MockNM(nodeIdStr, memory, vCores, GetResourceTrackerService(), YarnVersionInfo
				.GetVersion());
			nm.RegisterNode(runningApplications);
			return nm;
		}

		/// <exception cref="System.Exception"/>
		public virtual void SendNodeStarted(MockNM nm)
		{
			RMNodeImpl node = (RMNodeImpl)GetRMContext().GetRMNodes()[nm.GetNodeId()];
			node.Handle(new RMNodeStartedEvent(nm.GetNodeId(), null, null));
		}

		/// <exception cref="System.Exception"/>
		public virtual void SendNodeLost(MockNM nm)
		{
			RMNodeImpl node = (RMNodeImpl)GetRMContext().GetRMNodes()[nm.GetNodeId()];
			node.Handle(new RMNodeEvent(nm.GetNodeId(), RMNodeEventType.Expire));
		}

		/// <exception cref="System.Exception"/>
		public virtual void NMwaitForState(NodeId nodeid, NodeState finalState)
		{
			RMNode node = GetRMContext().GetRMNodes()[nodeid];
			NUnit.Framework.Assert.IsNotNull("node shouldn't be null", node);
			int timeoutSecs = 0;
			while (!finalState.Equals(node.GetState()) && timeoutSecs++ < 20)
			{
				System.Console.Out.WriteLine("Node State is : " + node.GetState() + " Waiting for state : "
					 + finalState);
				Sharpen.Thread.Sleep(500);
			}
			System.Console.Out.WriteLine("Node State is : " + node.GetState());
			NUnit.Framework.Assert.AreEqual("Node state is not correct (timedout)", finalState
				, node.GetState());
		}

		/// <exception cref="System.Exception"/>
		public virtual KillApplicationResponse KillApp(ApplicationId appId)
		{
			ApplicationClientProtocol client = GetClientRMService();
			KillApplicationRequest req = KillApplicationRequest.NewInstance(appId);
			return client.ForceKillApplication(req);
		}

		// from AMLauncher
		/// <exception cref="System.Exception"/>
		public virtual MockAM SendAMLaunched(ApplicationAttemptId appAttemptId)
		{
			MockAM am = new MockAM(GetRMContext(), masterService, appAttemptId);
			am.WaitForState(RMAppAttemptState.Allocated);
			//create and set AMRMToken
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = this.rmContext
				.GetAMRMTokenSecretManager().CreateAndGetAMRMToken(appAttemptId);
			((RMAppAttemptImpl)this.rmContext.GetRMApps()[appAttemptId.GetApplicationId()].GetRMAppAttempt
				(appAttemptId)).SetAMRMToken(amrmToken);
			GetRMContext().GetDispatcher().GetEventHandler().Handle(new RMAppAttemptEvent(appAttemptId
				, RMAppAttemptEventType.Launched));
			return am;
		}

		/// <exception cref="System.Exception"/>
		public virtual void SendAMLaunchFailed(ApplicationAttemptId appAttemptId)
		{
			MockAM am = new MockAM(GetRMContext(), masterService, appAttemptId);
			am.WaitForState(RMAppAttemptState.Allocated);
			GetRMContext().GetDispatcher().GetEventHandler().Handle(new RMAppAttemptEvent(appAttemptId
				, RMAppAttemptEventType.LaunchFailed, "Failed"));
		}

		protected internal override ClientRMService CreateClientRMService()
		{
			return new _ClientRMService_541(GetRMContext(), GetResourceScheduler(), rmAppManager
				, applicationACLsManager, queueACLsManager, GetRMContext().GetRMDelegationTokenSecretManager
				());
		}

		private sealed class _ClientRMService_541 : ClientRMService
		{
			public _ClientRMService_541(RMContext baseArg1, YarnScheduler baseArg2, RMAppManager
				 baseArg3, ApplicationACLsManager baseArg4, QueueACLsManager baseArg5, RMDelegationTokenSecretManager
				 baseArg6)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6)
			{
			}

			protected override void ServiceStart()
			{
			}

			// override to not start rpc handler
			protected override void ServiceStop()
			{
			}
		}

		// don't do anything
		protected internal override ResourceTrackerService CreateResourceTrackerService()
		{
			RMContainerTokenSecretManager containerTokenSecretManager = GetRMContext().GetContainerTokenSecretManager
				();
			containerTokenSecretManager.RollMasterKey();
			NMTokenSecretManagerInRM nmTokenSecretManager = GetRMContext().GetNMTokenSecretManager
				();
			nmTokenSecretManager.RollMasterKey();
			return new _ResourceTrackerService_565(GetRMContext(), nodesListManager, this.nmLivelinessMonitor
				, containerTokenSecretManager, nmTokenSecretManager);
		}

		private sealed class _ResourceTrackerService_565 : ResourceTrackerService
		{
			public _ResourceTrackerService_565(RMContext baseArg1, NodesListManager baseArg2, 
				NMLivelinessMonitor baseArg3, RMContainerTokenSecretManager baseArg4, NMTokenSecretManagerInRM
				 baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
			}

			protected override void ServiceStart()
			{
			}

			// override to not start rpc handler
			protected override void ServiceStop()
			{
			}
		}

		// don't do anything
		protected internal override ApplicationMasterService CreateApplicationMasterService
			()
		{
			return new _ApplicationMasterService_581(GetRMContext(), scheduler);
		}

		private sealed class _ApplicationMasterService_581 : ApplicationMasterService
		{
			public _ApplicationMasterService_581(RMContext baseArg1, YarnScheduler baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			protected override void ServiceStart()
			{
			}

			// override to not start rpc handler
			protected override void ServiceStop()
			{
			}
		}

		// don't do anything
		protected internal override ApplicationMasterLauncher CreateAMLauncher()
		{
			return new _ApplicationMasterLauncher_596(GetRMContext());
		}

		private sealed class _ApplicationMasterLauncher_596 : ApplicationMasterLauncher
		{
			public _ApplicationMasterLauncher_596(RMContext baseArg1)
				: base(baseArg1)
			{
			}

			protected override void ServiceStart()
			{
			}

			// override to not start rpc handler
			public override void Handle(AMLauncherEvent appEvent)
			{
			}

			// don't do anything
			protected override void ServiceStop()
			{
			}
		}

		// don't do anything
		protected internal override AdminService CreateAdminService()
		{
			return new _AdminService_616(this, GetRMContext());
		}

		private sealed class _AdminService_616 : AdminService
		{
			public _AdminService_616(ResourceManager baseArg1, RMContext baseArg2)
				: base(baseArg1, baseArg2)
			{
			}

			protected internal override void StartServer()
			{
			}

			// override to not start rpc handler
			protected internal override void StopServer()
			{
			}

			// don't do anything
			protected internal override EmbeddedElectorService CreateEmbeddedElectorService()
			{
				return null;
			}
		}

		public virtual NodesListManager GetNodesListManager()
		{
			return this.nodesListManager;
		}

		public virtual ClientToAMTokenSecretManagerInRM GetClientToAMTokenSecretManager()
		{
			return this.GetRMContext().GetClientToAMTokenSecretManager();
		}

		public virtual RMAppManager GetRMAppManager()
		{
			return this.rmAppManager;
		}

		public virtual AdminService GetAdminService()
		{
			return this.adminService;
		}

		protected internal override void StartWepApp()
		{
			if (GetConfig().GetBoolean(EnableWebapp, false))
			{
				base.StartWepApp();
				return;
			}
		}

		// Disable webapp
		/// <exception cref="System.Exception"/>
		public static void FinishAMAndVerifyAppState(RMApp rmApp, Org.Apache.Hadoop.Yarn.Server.Resourcemanager.MockRM
			 rm, MockNM nm, MockAM am)
		{
			FinishApplicationMasterRequest req = FinishApplicationMasterRequest.NewInstance(FinalApplicationStatus
				.Succeeded, string.Empty, string.Empty);
			am.UnregisterAppAttempt(req, true);
			am.WaitForState(RMAppAttemptState.Finishing);
			nm.NodeHeartbeat(am.GetApplicationAttemptId(), 1, ContainerState.Complete);
			am.WaitForState(RMAppAttemptState.Finished);
			rm.WaitForState(rmApp.GetApplicationId(), RMAppState.Finished);
		}

		/// <exception cref="System.Exception"/>
		public static MockAM LaunchAM(RMApp app, Org.Apache.Hadoop.Yarn.Server.Resourcemanager.MockRM
			 rm, MockNM nm)
		{
			rm.WaitForState(app.GetApplicationId(), RMAppState.Accepted);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			System.Console.Out.WriteLine("Launch AM " + attempt.GetAppAttemptId());
			nm.NodeHeartbeat(true);
			MockAM am = rm.SendAMLaunched(attempt.GetAppAttemptId());
			rm.WaitForState(attempt.GetAppAttemptId(), RMAppAttemptState.Launched);
			return am;
		}

		/// <exception cref="System.Exception"/>
		public static MockAM LaunchAndRegisterAM(RMApp app, Org.Apache.Hadoop.Yarn.Server.Resourcemanager.MockRM
			 rm, MockNM nm)
		{
			MockAM am = LaunchAM(app, rm, nm);
			am.RegisterAppAttempt();
			rm.WaitForState(app.GetApplicationId(), RMAppState.Running);
			return am;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ApplicationReport GetApplicationReport(ApplicationId appId)
		{
			ApplicationClientProtocol client = GetClientRMService();
			GetApplicationReportResponse response = client.GetApplicationReport(GetApplicationReportRequest
				.NewInstance(appId));
			return response.GetApplicationReport();
		}

		// Explicitly reset queue metrics for testing.
		public virtual void ClearQueueMetrics(RMApp app)
		{
			QueueMetrics.ClearQueueMetrics();
		}

		public virtual ResourceManager.RMActiveServices GetRMActiveService()
		{
			return activeServices;
		}
	}
}
