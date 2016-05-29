using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestClientRMService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestClientRMService));

		private RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null
			);

		private string appType = "MockApp";

		private static RMDelegationTokenSecretManager dtsm;

		private const string Queue1 = "Q-1";

		private const string Queue2 = "Q-2";

		private const string kerberosRule = "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\nDEFAULT";

		static TestClientRMService()
		{
			KerberosName.SetRules(kerberosRule);
		}

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetupSecretManager()
		{
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetStateStore()).ThenReturn(new NullRMStateStore
				());
			dtsm = new RMDelegationTokenSecretManager(60000, 60000, 60000, 60000, rmContext);
			dtsm.StartThreads();
		}

		[AfterClass]
		public static void TeardownSecretManager()
		{
			if (dtsm != null)
			{
				dtsm.StopThreads();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetClusterNodes()
		{
			MockRM rm = new _MockRM_195();
			rm.Start();
			RMNodeLabelsManager labelsMgr = rm.GetRMContext().GetNodeLabelManager();
			labelsMgr.AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
			// Add a healthy node with label = x
			MockNM node = rm.RegisterNode("host1:1234", 1024);
			IDictionary<NodeId, ICollection<string>> map = new Dictionary<NodeId, ICollection
				<string>>();
			map[node.GetNodeId()] = ImmutableSet.Of("x");
			labelsMgr.ReplaceLabelsOnNode(map);
			rm.SendNodeStarted(node);
			node.NodeHeartbeat(true);
			// Add and lose a node with label = y
			MockNM lostNode = rm.RegisterNode("host2:1235", 1024);
			rm.SendNodeStarted(lostNode);
			lostNode.NodeHeartbeat(true);
			rm.NMwaitForState(lostNode.GetNodeId(), NodeState.Running);
			rm.SendNodeLost(lostNode);
			// Create a client.
			Configuration conf = new Configuration();
			YarnRPC rpc = YarnRPC.Create(conf);
			IPEndPoint rmAddress = rm.GetClientRMService().GetBindAddress();
			Log.Info("Connecting to ResourceManager at " + rmAddress);
			ApplicationClientProtocol client = (ApplicationClientProtocol)rpc.GetProxy(typeof(
				ApplicationClientProtocol), rmAddress, conf);
			// Make call
			GetClusterNodesRequest request = GetClusterNodesRequest.NewInstance(EnumSet.Of(NodeState
				.Running));
			IList<NodeReport> nodeReports = client.GetClusterNodes(request).GetNodeReports();
			NUnit.Framework.Assert.AreEqual(1, nodeReports.Count);
			NUnit.Framework.Assert.AreNotSame("Node is expected to be healthy!", NodeState.Unhealthy
				, nodeReports[0].GetNodeState());
			// Check node's label = x
			NUnit.Framework.Assert.IsTrue(nodeReports[0].GetNodeLabels().Contains("x"));
			// Now make the node unhealthy.
			node.NodeHeartbeat(false);
			// Call again
			nodeReports = client.GetClusterNodes(request).GetNodeReports();
			NUnit.Framework.Assert.AreEqual("Unhealthy nodes should not show up by default", 
				0, nodeReports.Count);
			// Change label of host1 to y
			map = new Dictionary<NodeId, ICollection<string>>();
			map[node.GetNodeId()] = ImmutableSet.Of("y");
			labelsMgr.ReplaceLabelsOnNode(map);
			// Now query for UNHEALTHY nodes
			request = GetClusterNodesRequest.NewInstance(EnumSet.Of(NodeState.Unhealthy));
			nodeReports = client.GetClusterNodes(request).GetNodeReports();
			NUnit.Framework.Assert.AreEqual(1, nodeReports.Count);
			NUnit.Framework.Assert.AreEqual("Node is expected to be unhealthy!", NodeState.Unhealthy
				, nodeReports[0].GetNodeState());
			NUnit.Framework.Assert.IsTrue(nodeReports[0].GetNodeLabels().Contains("y"));
			// Remove labels of host1
			map = new Dictionary<NodeId, ICollection<string>>();
			map[node.GetNodeId()] = ImmutableSet.Of("y");
			labelsMgr.RemoveLabelsFromNode(map);
			// Query all states should return all nodes
			rm.RegisterNode("host3:1236", 1024);
			request = GetClusterNodesRequest.NewInstance(EnumSet.AllOf<NodeState>());
			nodeReports = client.GetClusterNodes(request).GetNodeReports();
			NUnit.Framework.Assert.AreEqual(3, nodeReports.Count);
			// All host1-3's label should be empty (instead of null)
			foreach (NodeReport report in nodeReports)
			{
				NUnit.Framework.Assert.IsTrue(report.GetNodeLabels() != null && report.GetNodeLabels
					().IsEmpty());
			}
			rpc.StopProxy(client, conf);
			rm.Close();
		}

		private sealed class _MockRM_195 : MockRM
		{
			public _MockRM_195()
			{
			}

			protected internal override ClientRMService CreateClientRMService()
			{
				return new ClientRMService(this.rmContext, this.scheduler, this.rmAppManager, this
					.applicationACLsManager, this.queueACLsManager, this.GetRMContext().GetRMDelegationTokenSecretManager
					());
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestNonExistingApplicationReport()
		{
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetRMApps()).ThenReturn(new ConcurrentHashMap<
				ApplicationId, RMApp>());
			ClientRMService rmService = new ClientRMService(rmContext, null, null, null, null
				, null);
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			GetApplicationReportRequest request = recordFactory.NewRecordInstance<GetApplicationReportRequest
				>();
			request.SetApplicationId(ApplicationId.NewInstance(0, 0));
			try
			{
				rmService.GetApplicationReport(request);
				NUnit.Framework.Assert.Fail();
			}
			catch (ApplicationNotFoundException ex)
			{
				NUnit.Framework.Assert.AreEqual(ex.Message, "Application with id '" + request.GetApplicationId
					() + "' doesn't exist in RM.");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationReport()
		{
			YarnScheduler yarnScheduler = Org.Mockito.Mockito.Mock<YarnScheduler>();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			MockRMContext(yarnScheduler, rmContext);
			ApplicationId appId1 = GetApplicationId(1);
			ApplicationACLsManager mockAclsManager = Org.Mockito.Mockito.Mock<ApplicationACLsManager
				>();
			Org.Mockito.Mockito.When(mockAclsManager.CheckAccess(UserGroupInformation.GetCurrentUser
				(), ApplicationAccessType.ViewApp, null, appId1)).ThenReturn(true);
			ClientRMService rmService = new ClientRMService(rmContext, yarnScheduler, null, mockAclsManager
				, null, null);
			try
			{
				RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
				GetApplicationReportRequest request = recordFactory.NewRecordInstance<GetApplicationReportRequest
					>();
				request.SetApplicationId(appId1);
				GetApplicationReportResponse response = rmService.GetApplicationReport(request);
				ApplicationReport report = response.GetApplicationReport();
				ApplicationResourceUsageReport usageReport = report.GetApplicationResourceUsageReport
					();
				NUnit.Framework.Assert.AreEqual(10, usageReport.GetMemorySeconds());
				NUnit.Framework.Assert.AreEqual(3, usageReport.GetVcoreSeconds());
			}
			finally
			{
				rmService.Close();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttemptReport()
		{
			ClientRMService rmService = CreateRMService();
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			GetApplicationAttemptReportRequest request = recordFactory.NewRecordInstance<GetApplicationAttemptReportRequest
				>();
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(123456, 1), 1);
			request.SetApplicationAttemptId(attemptId);
			try
			{
				GetApplicationAttemptReportResponse response = rmService.GetApplicationAttemptReport
					(request);
				NUnit.Framework.Assert.AreEqual(attemptId, response.GetApplicationAttemptReport()
					.GetApplicationAttemptId());
			}
			catch (ApplicationNotFoundException ex)
			{
				NUnit.Framework.Assert.Fail(ex.Message);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationResourceUsageReportDummy()
		{
			ApplicationAttemptId attemptId = GetApplicationAttemptId(1);
			YarnScheduler yarnScheduler = MockYarnScheduler();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			MockRMContext(yarnScheduler, rmContext);
			Org.Mockito.Mockito.When(rmContext.GetDispatcher().GetEventHandler()).ThenReturn(
				new _EventHandler_367());
			ApplicationSubmissionContext asContext = Org.Mockito.Mockito.Mock<ApplicationSubmissionContext
				>();
			YarnConfiguration config = new YarnConfiguration();
			RMAppAttemptImpl rmAppAttemptImpl = new RMAppAttemptImpl(attemptId, rmContext, yarnScheduler
				, null, asContext, config, false, null);
			ApplicationResourceUsageReport report = rmAppAttemptImpl.GetApplicationResourceUsageReport
				();
			NUnit.Framework.Assert.AreEqual(report, RMServerUtils.DummyApplicationResourceUsageReport
				);
		}

		private sealed class _EventHandler_367 : EventHandler<Org.Apache.Hadoop.Yarn.Event.Event
			>
		{
			public _EventHandler_367()
			{
			}

			public void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttempts()
		{
			ClientRMService rmService = CreateRMService();
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			GetApplicationAttemptsRequest request = recordFactory.NewRecordInstance<GetApplicationAttemptsRequest
				>();
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(123456, 1), 1);
			request.SetApplicationId(ApplicationId.NewInstance(123456, 1));
			try
			{
				GetApplicationAttemptsResponse response = rmService.GetApplicationAttempts(request
					);
				NUnit.Framework.Assert.AreEqual(1, response.GetApplicationAttemptList().Count);
				NUnit.Framework.Assert.AreEqual(attemptId, response.GetApplicationAttemptList()[0
					].GetApplicationAttemptId());
			}
			catch (ApplicationNotFoundException ex)
			{
				NUnit.Framework.Assert.Fail(ex.Message);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainerReport()
		{
			ClientRMService rmService = CreateRMService();
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			GetContainerReportRequest request = recordFactory.NewRecordInstance<GetContainerReportRequest
				>();
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(123456, 1), 1);
			ContainerId containerId = ContainerId.NewContainerId(attemptId, 1);
			request.SetContainerId(containerId);
			try
			{
				GetContainerReportResponse response = rmService.GetContainerReport(request);
				NUnit.Framework.Assert.AreEqual(containerId, response.GetContainerReport().GetContainerId
					());
			}
			catch (ApplicationNotFoundException ex)
			{
				NUnit.Framework.Assert.Fail(ex.Message);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainers()
		{
			ClientRMService rmService = CreateRMService();
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			GetContainersRequest request = recordFactory.NewRecordInstance<GetContainersRequest
				>();
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(123456, 1), 1);
			ContainerId containerId = ContainerId.NewContainerId(attemptId, 1);
			request.SetApplicationAttemptId(attemptId);
			try
			{
				GetContainersResponse response = rmService.GetContainers(request);
				NUnit.Framework.Assert.AreEqual(containerId, response.GetContainerList()[0].GetContainerId
					());
			}
			catch (ApplicationNotFoundException ex)
			{
				NUnit.Framework.Assert.Fail(ex.Message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ClientRMService CreateRMService()
		{
			YarnScheduler yarnScheduler = MockYarnScheduler();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			MockRMContext(yarnScheduler, rmContext);
			ConcurrentHashMap<ApplicationId, RMApp> apps = GetRMApps(rmContext, yarnScheduler
				);
			Org.Mockito.Mockito.When(rmContext.GetRMApps()).ThenReturn(apps);
			Org.Mockito.Mockito.When(rmContext.GetYarnConfiguration()).ThenReturn(new Configuration
				());
			RMAppManager appManager = new RMAppManager(rmContext, yarnScheduler, null, Org.Mockito.Mockito.Mock
				<ApplicationACLsManager>(), new Configuration());
			Org.Mockito.Mockito.When(rmContext.GetDispatcher().GetEventHandler()).ThenReturn(
				new _EventHandler_454());
			ApplicationACLsManager mockAclsManager = Org.Mockito.Mockito.Mock<ApplicationACLsManager
				>();
			QueueACLsManager mockQueueACLsManager = Org.Mockito.Mockito.Mock<QueueACLsManager
				>();
			Org.Mockito.Mockito.When(mockQueueACLsManager.CheckAccess(Matchers.Any<UserGroupInformation
				>(), Matchers.Any<QueueACL>(), Matchers.AnyString())).ThenReturn(true);
			return new ClientRMService(rmContext, yarnScheduler, appManager, mockAclsManager, 
				mockQueueACLsManager, null);
		}

		private sealed class _EventHandler_454 : EventHandler<Org.Apache.Hadoop.Yarn.Event.Event
			>
		{
			public _EventHandler_454()
			{
			}

			public void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestForceKillNonExistingApplication()
		{
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetRMApps()).ThenReturn(new ConcurrentHashMap<
				ApplicationId, RMApp>());
			ClientRMService rmService = new ClientRMService(rmContext, null, null, null, null
				, null);
			ApplicationId applicationId = BuilderUtils.NewApplicationId(Runtime.CurrentTimeMillis
				(), 0);
			KillApplicationRequest request = KillApplicationRequest.NewInstance(applicationId
				);
			try
			{
				rmService.ForceKillApplication(request);
				NUnit.Framework.Assert.Fail();
			}
			catch (ApplicationNotFoundException ex)
			{
				NUnit.Framework.Assert.AreEqual(ex.Message, "Trying to kill an absent " + "application "
					 + request.GetApplicationId());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestForceKillApplication()
		{
			YarnConfiguration conf = new YarnConfiguration();
			MockRM rm = new MockRM();
			rm.Init(conf);
			rm.Start();
			ClientRMService rmService = rm.GetClientRMService();
			GetApplicationsRequest getRequest = GetApplicationsRequest.NewInstance(EnumSet.Of
				(YarnApplicationState.Killed));
			RMApp app1 = rm.SubmitApp(1024);
			RMApp app2 = rm.SubmitApp(1024, true);
			NUnit.Framework.Assert.AreEqual("Incorrect number of apps in the RM", 0, rmService
				.GetApplications(getRequest).GetApplicationList().Count);
			KillApplicationRequest killRequest1 = KillApplicationRequest.NewInstance(app1.GetApplicationId
				());
			KillApplicationRequest killRequest2 = KillApplicationRequest.NewInstance(app2.GetApplicationId
				());
			int killAttemptCount = 0;
			for (int i = 0; i < 100; i++)
			{
				KillApplicationResponse killResponse1 = rmService.ForceKillApplication(killRequest1
					);
				killAttemptCount++;
				if (killResponse1.GetIsKillCompleted())
				{
					break;
				}
				Sharpen.Thread.Sleep(10);
			}
			NUnit.Framework.Assert.IsTrue("Kill attempt count should be greater than 1 for managed AMs"
				, killAttemptCount > 1);
			NUnit.Framework.Assert.AreEqual("Incorrect number of apps in the RM", 1, rmService
				.GetApplications(getRequest).GetApplicationList().Count);
			KillApplicationResponse killResponse2 = rmService.ForceKillApplication(killRequest2
				);
			NUnit.Framework.Assert.IsTrue("Killing UnmanagedAM should falsely acknowledge true"
				, killResponse2.GetIsKillCompleted());
			for (int i_1 = 0; i_1 < 100; i_1++)
			{
				if (2 == rmService.GetApplications(getRequest).GetApplicationList().Count)
				{
					break;
				}
				Sharpen.Thread.Sleep(10);
			}
			NUnit.Framework.Assert.AreEqual("Incorrect number of apps in the RM", 2, rmService
				.GetApplications(getRequest).GetApplicationList().Count);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void TestMoveAbsentApplication()
		{
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetRMApps()).ThenReturn(new ConcurrentHashMap<
				ApplicationId, RMApp>());
			ClientRMService rmService = new ClientRMService(rmContext, null, null, null, null
				, null);
			ApplicationId applicationId = BuilderUtils.NewApplicationId(Runtime.CurrentTimeMillis
				(), 0);
			MoveApplicationAcrossQueuesRequest request = MoveApplicationAcrossQueuesRequest.NewInstance
				(applicationId, "newqueue");
			rmService.MoveApplicationAcrossQueues(request);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetQueueInfo()
		{
			YarnScheduler yarnScheduler = Org.Mockito.Mockito.Mock<YarnScheduler>();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			MockRMContext(yarnScheduler, rmContext);
			ApplicationACLsManager mockAclsManager = Org.Mockito.Mockito.Mock<ApplicationACLsManager
				>();
			QueueACLsManager mockQueueACLsManager = Org.Mockito.Mockito.Mock<QueueACLsManager
				>();
			Org.Mockito.Mockito.When(mockQueueACLsManager.CheckAccess(Matchers.Any<UserGroupInformation
				>(), Matchers.Any<QueueACL>(), Matchers.AnyString())).ThenReturn(true);
			Org.Mockito.Mockito.When(mockAclsManager.CheckAccess(Matchers.Any<UserGroupInformation
				>(), Matchers.Any<ApplicationAccessType>(), Matchers.AnyString(), Matchers.Any<ApplicationId
				>())).ThenReturn(true);
			ClientRMService rmService = new ClientRMService(rmContext, yarnScheduler, null, mockAclsManager
				, mockQueueACLsManager, null);
			GetQueueInfoRequest request = recordFactory.NewRecordInstance<GetQueueInfoRequest
				>();
			request.SetQueueName("testqueue");
			request.SetIncludeApplications(true);
			GetQueueInfoResponse queueInfo = rmService.GetQueueInfo(request);
			IList<ApplicationReport> applications = queueInfo.GetQueueInfo().GetApplications(
				);
			NUnit.Framework.Assert.AreEqual(2, applications.Count);
			request.SetQueueName("nonexistentqueue");
			request.SetIncludeApplications(true);
			// should not throw exception on nonexistent queue
			queueInfo = rmService.GetQueueInfo(request);
			// Case where user does not have application access
			ApplicationACLsManager mockAclsManager1 = Org.Mockito.Mockito.Mock<ApplicationACLsManager
				>();
			QueueACLsManager mockQueueACLsManager1 = Org.Mockito.Mockito.Mock<QueueACLsManager
				>();
			Org.Mockito.Mockito.When(mockQueueACLsManager1.CheckAccess(Matchers.Any<UserGroupInformation
				>(), Matchers.Any<QueueACL>(), Matchers.AnyString())).ThenReturn(false);
			Org.Mockito.Mockito.When(mockAclsManager1.CheckAccess(Matchers.Any<UserGroupInformation
				>(), Matchers.Any<ApplicationAccessType>(), Matchers.AnyString(), Matchers.Any<ApplicationId
				>())).ThenReturn(false);
			ClientRMService rmService1 = new ClientRMService(rmContext, yarnScheduler, null, 
				mockAclsManager1, mockQueueACLsManager1, null);
			request.SetQueueName("testqueue");
			request.SetIncludeApplications(true);
			GetQueueInfoResponse queueInfo1 = rmService1.GetQueueInfo(request);
			IList<ApplicationReport> applications1 = queueInfo1.GetQueueInfo().GetApplications
				();
			NUnit.Framework.Assert.AreEqual(0, applications1.Count);
		}

		private static readonly UserGroupInformation owner = UserGroupInformation.CreateRemoteUser
			("owner");

		private static readonly UserGroupInformation other = UserGroupInformation.CreateRemoteUser
			("other");

		private static readonly UserGroupInformation tester = UserGroupInformation.CreateRemoteUser
			("tester");

		private const string testerPrincipal = "tester@EXAMPLE.COM";

		private const string ownerPrincipal = "owner@EXAMPLE.COM";

		private const string otherPrincipal = "other@EXAMPLE.COM";

		private static readonly UserGroupInformation testerKerb = UserGroupInformation.CreateRemoteUser
			(testerPrincipal);

		private static readonly UserGroupInformation ownerKerb = UserGroupInformation.CreateRemoteUser
			(ownerPrincipal);

		private static readonly UserGroupInformation otherKerb = UserGroupInformation.CreateRemoteUser
			(otherPrincipal);

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenRenewalByOwner()
		{
			owner.DoAs(new _PrivilegedExceptionAction_623(this));
		}

		private sealed class _PrivilegedExceptionAction_623 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_623(TestClientRMService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.CheckTokenRenewal(TestClientRMService.owner, TestClientRMService.
					owner);
				return null;
			}

			private readonly TestClientRMService _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenRenewalWrongUser()
		{
			try
			{
				owner.DoAs(new _PrivilegedExceptionAction_635(this));
			}
			catch (Exception)
			{
				return;
			}
			NUnit.Framework.Assert.Fail("renew should have failed");
		}

		private sealed class _PrivilegedExceptionAction_635 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_635(TestClientRMService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					this._enclosing.CheckTokenRenewal(TestClientRMService.owner, TestClientRMService.
						other);
					return null;
				}
				catch (YarnException ex)
				{
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains(TestClientRMService.owner.GetUserName
						() + " tries to renew a token with renewer " + TestClientRMService.other.GetUserName
						()));
					throw;
				}
			}

			private readonly TestClientRMService _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenRenewalByLoginUser()
		{
			UserGroupInformation.GetLoginUser().DoAs(new _PrivilegedExceptionAction_657(this)
				);
		}

		private sealed class _PrivilegedExceptionAction_657 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_657(TestClientRMService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.CheckTokenRenewal(TestClientRMService.owner, TestClientRMService.
					owner);
				this._enclosing.CheckTokenRenewal(TestClientRMService.owner, TestClientRMService.
					other);
				return null;
			}

			private readonly TestClientRMService _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void CheckTokenRenewal(UserGroupInformation owner, UserGroupInformation renewer
			)
		{
			RMDelegationTokenIdentifier tokenIdentifier = new RMDelegationTokenIdentifier(new 
				Text(owner.GetUserName()), new Text(renewer.GetUserName()), null);
			Org.Apache.Hadoop.Security.Token.Token<object> token = new Org.Apache.Hadoop.Security.Token.Token
				<RMDelegationTokenIdentifier>(tokenIdentifier, dtsm);
			Org.Apache.Hadoop.Yarn.Api.Records.Token dToken = BuilderUtils.NewDelegationToken
				(token.GetIdentifier(), token.GetKind().ToString(), token.GetPassword(), token.GetService
				().ToString());
			RenewDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<RenewDelegationTokenRequest>();
			request.SetDelegationToken(dToken);
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			ClientRMService rmService = new ClientRMService(rmContext, null, null, null, null
				, dtsm);
			rmService.RenewDelegationToken(request);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenCancellationByOwner()
		{
			// two tests required - one with a kerberos name
			// and with a short name
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			ClientRMService rmService = new ClientRMService(rmContext, null, null, null, null
				, dtsm);
			testerKerb.DoAs(new _PrivilegedExceptionAction_694(this, rmService));
			owner.DoAs(new _PrivilegedExceptionAction_701(this));
		}

		private sealed class _PrivilegedExceptionAction_694 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_694(TestClientRMService _enclosing, ClientRMService
				 rmService)
			{
				this._enclosing = _enclosing;
				this.rmService = rmService;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.CheckTokenCancellation(rmService, TestClientRMService.testerKerb, 
					TestClientRMService.other);
				return null;
			}

			private readonly TestClientRMService _enclosing;

			private readonly ClientRMService rmService;
		}

		private sealed class _PrivilegedExceptionAction_701 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_701(TestClientRMService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.CheckTokenCancellation(TestClientRMService.owner, TestClientRMService
					.other);
				return null;
			}

			private readonly TestClientRMService _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenCancellationByRenewer()
		{
			// two tests required - one with a kerberos name
			// and with a short name
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			ClientRMService rmService = new ClientRMService(rmContext, null, null, null, null
				, dtsm);
			testerKerb.DoAs(new _PrivilegedExceptionAction_717(this, rmService));
			other.DoAs(new _PrivilegedExceptionAction_724(this));
		}

		private sealed class _PrivilegedExceptionAction_717 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_717(TestClientRMService _enclosing, ClientRMService
				 rmService)
			{
				this._enclosing = _enclosing;
				this.rmService = rmService;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.CheckTokenCancellation(rmService, TestClientRMService.owner, TestClientRMService
					.testerKerb);
				return null;
			}

			private readonly TestClientRMService _enclosing;

			private readonly ClientRMService rmService;
		}

		private sealed class _PrivilegedExceptionAction_724 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_724(TestClientRMService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.CheckTokenCancellation(TestClientRMService.owner, TestClientRMService
					.other);
				return null;
			}

			private readonly TestClientRMService _enclosing;
		}

		[NUnit.Framework.Test]
		public virtual void TestTokenCancellationByWrongUser()
		{
			// two sets to test -
			// 1. try to cancel tokens of short and kerberos users as a kerberos UGI
			// 2. try to cancel tokens of short and kerberos users as a simple auth UGI
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			ClientRMService rmService = new ClientRMService(rmContext, null, null, null, null
				, dtsm);
			UserGroupInformation[] kerbTestOwners = new UserGroupInformation[] { owner, other
				, tester, ownerKerb, otherKerb };
			UserGroupInformation[] kerbTestRenewers = new UserGroupInformation[] { owner, other
				, ownerKerb, otherKerb };
			foreach (UserGroupInformation tokOwner in kerbTestOwners)
			{
				foreach (UserGroupInformation tokRenewer in kerbTestRenewers)
				{
					try
					{
						testerKerb.DoAs(new _PrivilegedExceptionAction_749(this, rmService, tokOwner, tokRenewer
							));
					}
					catch (Exception e)
					{
						NUnit.Framework.Assert.Fail("Unexpected exception; " + e.Message);
					}
				}
			}
			UserGroupInformation[] simpleTestOwners = new UserGroupInformation[] { owner, other
				, ownerKerb, otherKerb, testerKerb };
			UserGroupInformation[] simpleTestRenewers = new UserGroupInformation[] { owner, other
				, ownerKerb, otherKerb };
			foreach (UserGroupInformation tokOwner_1 in simpleTestOwners)
			{
				foreach (UserGroupInformation tokRenewer in simpleTestRenewers)
				{
					try
					{
						tester.DoAs(new _PrivilegedExceptionAction_779(this, tokOwner, tokRenewer));
					}
					catch (Exception e)
					{
						NUnit.Framework.Assert.Fail("Unexpected exception; " + e.Message);
					}
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_749 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_749(TestClientRMService _enclosing, ClientRMService
				 rmService, UserGroupInformation tokOwner, UserGroupInformation tokRenewer)
			{
				this._enclosing = _enclosing;
				this.rmService = rmService;
				this.tokOwner = tokOwner;
				this.tokRenewer = tokRenewer;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					this._enclosing.CheckTokenCancellation(rmService, tokOwner, tokRenewer);
					NUnit.Framework.Assert.Fail("We should not reach here; token owner = " + tokOwner
						.GetUserName() + ", renewer = " + tokRenewer.GetUserName());
					return null;
				}
				catch (YarnException e)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.Contains(TestClientRMService.testerKerb.GetUserName
						() + " is not authorized to cancel the token"));
					return null;
				}
			}

			private readonly TestClientRMService _enclosing;

			private readonly ClientRMService rmService;

			private readonly UserGroupInformation tokOwner;

			private readonly UserGroupInformation tokRenewer;
		}

		private sealed class _PrivilegedExceptionAction_779 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_779(TestClientRMService _enclosing, UserGroupInformation
				 tokOwner, UserGroupInformation tokRenewer)
			{
				this._enclosing = _enclosing;
				this.tokOwner = tokOwner;
				this.tokRenewer = tokRenewer;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					this._enclosing.CheckTokenCancellation(tokOwner, tokRenewer);
					NUnit.Framework.Assert.Fail("We should not reach here; token owner = " + tokOwner
						.GetUserName() + ", renewer = " + tokRenewer.GetUserName());
					return null;
				}
				catch (YarnException ex)
				{
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains(TestClientRMService.tester.GetUserName
						() + " is not authorized to cancel the token"));
					return null;
				}
			}

			private readonly TestClientRMService _enclosing;

			private readonly UserGroupInformation tokOwner;

			private readonly UserGroupInformation tokRenewer;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void CheckTokenCancellation(UserGroupInformation owner, UserGroupInformation
			 renewer)
		{
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			ClientRMService rmService = new ClientRMService(rmContext, null, null, null, null
				, dtsm);
			CheckTokenCancellation(rmService, owner, renewer);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void CheckTokenCancellation(ClientRMService rmService, UserGroupInformation
			 owner, UserGroupInformation renewer)
		{
			RMDelegationTokenIdentifier tokenIdentifier = new RMDelegationTokenIdentifier(new 
				Text(owner.GetUserName()), new Text(renewer.GetUserName()), null);
			Org.Apache.Hadoop.Security.Token.Token<object> token = new Org.Apache.Hadoop.Security.Token.Token
				<RMDelegationTokenIdentifier>(tokenIdentifier, dtsm);
			Org.Apache.Hadoop.Yarn.Api.Records.Token dToken = BuilderUtils.NewDelegationToken
				(token.GetIdentifier(), token.GetKind().ToString(), token.GetPassword(), token.GetService
				().ToString());
			CancelDelegationTokenRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<CancelDelegationTokenRequest>();
			request.SetDelegationToken(dToken);
			rmService.CancelDelegationToken(request);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppSubmit()
		{
			YarnScheduler yarnScheduler = MockYarnScheduler();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			MockRMContext(yarnScheduler, rmContext);
			RMStateStore stateStore = Org.Mockito.Mockito.Mock<RMStateStore>();
			Org.Mockito.Mockito.When(rmContext.GetStateStore()).ThenReturn(stateStore);
			RMAppManager appManager = new RMAppManager(rmContext, yarnScheduler, null, Org.Mockito.Mockito.Mock
				<ApplicationACLsManager>(), new Configuration());
			Org.Mockito.Mockito.When(rmContext.GetDispatcher().GetEventHandler()).ThenReturn(
				new _EventHandler_839());
			ApplicationId appId1 = GetApplicationId(100);
			ApplicationACLsManager mockAclsManager = Org.Mockito.Mockito.Mock<ApplicationACLsManager
				>();
			Org.Mockito.Mockito.When(mockAclsManager.CheckAccess(UserGroupInformation.GetCurrentUser
				(), ApplicationAccessType.ViewApp, null, appId1)).ThenReturn(true);
			QueueACLsManager mockQueueACLsManager = Org.Mockito.Mockito.Mock<QueueACLsManager
				>();
			Org.Mockito.Mockito.When(mockQueueACLsManager.CheckAccess(Matchers.Any<UserGroupInformation
				>(), Matchers.Any<QueueACL>(), Matchers.AnyString())).ThenReturn(true);
			ClientRMService rmService = new ClientRMService(rmContext, yarnScheduler, appManager
				, mockAclsManager, mockQueueACLsManager, null);
			// without name and queue
			SubmitApplicationRequest submitRequest1 = MockSubmitAppRequest(appId1, null, null
				);
			try
			{
				rmService.SubmitApplication(submitRequest1);
			}
			catch (YarnException)
			{
				NUnit.Framework.Assert.Fail("Exception is not expected.");
			}
			RMApp app1 = rmContext.GetRMApps()[appId1];
			NUnit.Framework.Assert.IsNotNull("app doesn't exist", app1);
			NUnit.Framework.Assert.AreEqual("app name doesn't match", YarnConfiguration.DefaultApplicationName
				, app1.GetName());
			NUnit.Framework.Assert.AreEqual("app queue doesn't match", YarnConfiguration.DefaultQueueName
				, app1.GetQueue());
			// with name and queue
			string name = MockApps.NewAppName();
			string queue = MockApps.NewQueue();
			ApplicationId appId2 = GetApplicationId(101);
			SubmitApplicationRequest submitRequest2 = MockSubmitAppRequest(appId2, name, queue
				);
			submitRequest2.GetApplicationSubmissionContext().SetApplicationType("matchType");
			try
			{
				rmService.SubmitApplication(submitRequest2);
			}
			catch (YarnException)
			{
				NUnit.Framework.Assert.Fail("Exception is not expected.");
			}
			RMApp app2 = rmContext.GetRMApps()[appId2];
			NUnit.Framework.Assert.IsNotNull("app doesn't exist", app2);
			NUnit.Framework.Assert.AreEqual("app name doesn't match", name, app2.GetName());
			NUnit.Framework.Assert.AreEqual("app queue doesn't match", queue, app2.GetQueue()
				);
			// duplicate appId
			try
			{
				rmService.SubmitApplication(submitRequest2);
			}
			catch (YarnException)
			{
				NUnit.Framework.Assert.Fail("Exception is not expected.");
			}
			GetApplicationsRequest getAllAppsRequest = GetApplicationsRequest.NewInstance(new 
				HashSet<string>());
			GetApplicationsResponse getAllApplicationsResponse = rmService.GetApplications(getAllAppsRequest
				);
			NUnit.Framework.Assert.AreEqual(5, getAllApplicationsResponse.GetApplicationList(
				).Count);
			ICollection<string> appTypes = new HashSet<string>();
			appTypes.AddItem("matchType");
			getAllAppsRequest = GetApplicationsRequest.NewInstance(appTypes);
			getAllApplicationsResponse = rmService.GetApplications(getAllAppsRequest);
			NUnit.Framework.Assert.AreEqual(1, getAllApplicationsResponse.GetApplicationList(
				).Count);
			NUnit.Framework.Assert.AreEqual(appId2, getAllApplicationsResponse.GetApplicationList
				()[0].GetApplicationId());
		}

		private sealed class _EventHandler_839 : EventHandler<Org.Apache.Hadoop.Yarn.Event.Event
			>
		{
			public _EventHandler_839()
			{
			}

			public void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplications()
		{
			// Basic setup
			YarnScheduler yarnScheduler = MockYarnScheduler();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			MockRMContext(yarnScheduler, rmContext);
			RMStateStore stateStore = Org.Mockito.Mockito.Mock<RMStateStore>();
			Org.Mockito.Mockito.When(rmContext.GetStateStore()).ThenReturn(stateStore);
			RMAppManager appManager = new RMAppManager(rmContext, yarnScheduler, null, Org.Mockito.Mockito.Mock
				<ApplicationACLsManager>(), new Configuration());
			Org.Mockito.Mockito.When(rmContext.GetDispatcher().GetEventHandler()).ThenReturn(
				new _EventHandler_932());
			ApplicationACLsManager mockAclsManager = Org.Mockito.Mockito.Mock<ApplicationACLsManager
				>();
			QueueACLsManager mockQueueACLsManager = Org.Mockito.Mockito.Mock<QueueACLsManager
				>();
			Org.Mockito.Mockito.When(mockQueueACLsManager.CheckAccess(Matchers.Any<UserGroupInformation
				>(), Matchers.Any<QueueACL>(), Matchers.AnyString())).ThenReturn(true);
			ClientRMService rmService = new ClientRMService(rmContext, yarnScheduler, appManager
				, mockAclsManager, mockQueueACLsManager, null);
			// Initialize appnames and queues
			string[] queues = new string[] { Queue1, Queue2 };
			string[] appNames = new string[] { MockApps.NewAppName(), MockApps.NewAppName(), 
				MockApps.NewAppName() };
			ApplicationId[] appIds = new ApplicationId[] { GetApplicationId(101), GetApplicationId
				(102), GetApplicationId(103) };
			IList<string> tags = Arrays.AsList("Tag1", "Tag2", "Tag3");
			long[] submitTimeMillis = new long[3];
			// Submit applications
			for (int i = 0; i < appIds.Length; i++)
			{
				ApplicationId appId = appIds[i];
				Org.Mockito.Mockito.When(mockAclsManager.CheckAccess(UserGroupInformation.GetCurrentUser
					(), ApplicationAccessType.ViewApp, null, appId)).ThenReturn(true);
				SubmitApplicationRequest submitRequest = MockSubmitAppRequest(appId, appNames[i], 
					queues[i % queues.Length], new HashSet<string>(tags.SubList(0, i + 1)));
				rmService.SubmitApplication(submitRequest);
				submitTimeMillis[i] = Runtime.CurrentTimeMillis();
			}
			// Test different cases of ClientRMService#getApplications()
			GetApplicationsRequest request = GetApplicationsRequest.NewInstance();
			NUnit.Framework.Assert.AreEqual("Incorrect total number of apps", 6, rmService.GetApplications
				(request).GetApplicationList().Count);
			// Check limit
			request.SetLimit(1L);
			NUnit.Framework.Assert.AreEqual("Failed to limit applications", 1, rmService.GetApplications
				(request).GetApplicationList().Count);
			// Check start range
			request = GetApplicationsRequest.NewInstance();
			request.SetStartRange(submitTimeMillis[0], Runtime.CurrentTimeMillis());
			// 2 applications are submitted after first timeMills
			NUnit.Framework.Assert.AreEqual("Incorrect number of matching start range", 2, rmService
				.GetApplications(request).GetApplicationList().Count);
			// 1 application is submitted after the second timeMills
			request.SetStartRange(submitTimeMillis[1], Runtime.CurrentTimeMillis());
			NUnit.Framework.Assert.AreEqual("Incorrect number of matching start range", 1, rmService
				.GetApplications(request).GetApplicationList().Count);
			// no application is submitted after the third timeMills
			request.SetStartRange(submitTimeMillis[2], Runtime.CurrentTimeMillis());
			NUnit.Framework.Assert.AreEqual("Incorrect number of matching start range", 0, rmService
				.GetApplications(request).GetApplicationList().Count);
			// Check queue
			request = GetApplicationsRequest.NewInstance();
			ICollection<string> queueSet = new HashSet<string>();
			request.SetQueues(queueSet);
			queueSet.AddItem(queues[0]);
			NUnit.Framework.Assert.AreEqual("Incorrect number of applications in queue", 2, rmService
				.GetApplications(request).GetApplicationList().Count);
			NUnit.Framework.Assert.AreEqual("Incorrect number of applications in queue", 2, rmService
				.GetApplications(request, false).GetApplicationList().Count);
			queueSet.AddItem(queues[1]);
			NUnit.Framework.Assert.AreEqual("Incorrect number of applications in queue", 3, rmService
				.GetApplications(request).GetApplicationList().Count);
			// Check user
			request = GetApplicationsRequest.NewInstance();
			ICollection<string> userSet = new HashSet<string>();
			request.SetUsers(userSet);
			userSet.AddItem("random-user-name");
			NUnit.Framework.Assert.AreEqual("Incorrect number of applications for user", 0, rmService
				.GetApplications(request).GetApplicationList().Count);
			userSet.AddItem(UserGroupInformation.GetCurrentUser().GetShortUserName());
			NUnit.Framework.Assert.AreEqual("Incorrect number of applications for user", 3, rmService
				.GetApplications(request).GetApplicationList().Count);
			// Check tags
			request = GetApplicationsRequest.NewInstance(ApplicationsRequestScope.All, null, 
				null, null, null, null, null, null, null);
			ICollection<string> tagSet = new HashSet<string>();
			request.SetApplicationTags(tagSet);
			NUnit.Framework.Assert.AreEqual("Incorrect number of matching tags", 6, rmService
				.GetApplications(request).GetApplicationList().Count);
			tagSet = Sets.NewHashSet(tags[0]);
			request.SetApplicationTags(tagSet);
			NUnit.Framework.Assert.AreEqual("Incorrect number of matching tags", 3, rmService
				.GetApplications(request).GetApplicationList().Count);
			tagSet = Sets.NewHashSet(tags[1]);
			request.SetApplicationTags(tagSet);
			NUnit.Framework.Assert.AreEqual("Incorrect number of matching tags", 2, rmService
				.GetApplications(request).GetApplicationList().Count);
			tagSet = Sets.NewHashSet(tags[2]);
			request.SetApplicationTags(tagSet);
			NUnit.Framework.Assert.AreEqual("Incorrect number of matching tags", 1, rmService
				.GetApplications(request).GetApplicationList().Count);
			// Check scope
			request = GetApplicationsRequest.NewInstance(ApplicationsRequestScope.Viewable);
			NUnit.Framework.Assert.AreEqual("Incorrect number of applications for the scope", 
				6, rmService.GetApplications(request).GetApplicationList().Count);
			request = GetApplicationsRequest.NewInstance(ApplicationsRequestScope.Own);
			NUnit.Framework.Assert.AreEqual("Incorrect number of applications for the scope", 
				3, rmService.GetApplications(request).GetApplicationList().Count);
		}

		private sealed class _EventHandler_932 : EventHandler<Org.Apache.Hadoop.Yarn.Event.Event
			>
		{
			public _EventHandler_932()
			{
			}

			public void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.BrokenBarrierException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void TestConcurrentAppSubmit()
		{
			YarnScheduler yarnScheduler = MockYarnScheduler();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			MockRMContext(yarnScheduler, rmContext);
			RMStateStore stateStore = Org.Mockito.Mockito.Mock<RMStateStore>();
			Org.Mockito.Mockito.When(rmContext.GetStateStore()).ThenReturn(stateStore);
			RMAppManager appManager = new RMAppManager(rmContext, yarnScheduler, null, Org.Mockito.Mockito.Mock
				<ApplicationACLsManager>(), new Configuration());
			ApplicationId appId1 = GetApplicationId(100);
			ApplicationId appId2 = GetApplicationId(101);
			SubmitApplicationRequest submitRequest1 = MockSubmitAppRequest(appId1, null, null
				);
			SubmitApplicationRequest submitRequest2 = MockSubmitAppRequest(appId2, null, null
				);
			CyclicBarrier startBarrier = new CyclicBarrier(2);
			CyclicBarrier endBarrier = new CyclicBarrier(2);
			EventHandler eventHandler = new _EventHandler_1080(appId1, startBarrier, endBarrier
				);
			Org.Mockito.Mockito.When(rmContext.GetDispatcher().GetEventHandler()).ThenReturn(
				eventHandler);
			ClientRMService rmService = new ClientRMService(rmContext, yarnScheduler, appManager
				, null, null, null);
			// submit an app and wait for it to block while in app submission
			Sharpen.Thread t = new _Thread_1106(rmService, submitRequest1);
			t.Start();
			// submit another app, so go through while the first app is blocked
			startBarrier.Await();
			rmService.SubmitApplication(submitRequest2);
			endBarrier.Await();
			t.Join();
		}

		private sealed class _EventHandler_1080 : EventHandler
		{
			public _EventHandler_1080(ApplicationId appId1, CyclicBarrier startBarrier, CyclicBarrier
				 endBarrier)
			{
				this.appId1 = appId1;
				this.startBarrier = startBarrier;
				this.endBarrier = endBarrier;
			}

			public void Handle(Org.Apache.Hadoop.Yarn.Event.Event rawEvent)
			{
				if (rawEvent is RMAppEvent)
				{
					RMAppEvent @event = (RMAppEvent)rawEvent;
					if (@event.GetApplicationId().Equals(appId1))
					{
						try
						{
							startBarrier.Await();
							endBarrier.Await();
						}
						catch (BrokenBarrierException e)
						{
							TestClientRMService.Log.Warn("Broken Barrier", e);
						}
						catch (Exception e)
						{
							TestClientRMService.Log.Warn("Interrupted while awaiting barriers", e);
						}
					}
				}
			}

			private readonly ApplicationId appId1;

			private readonly CyclicBarrier startBarrier;

			private readonly CyclicBarrier endBarrier;
		}

		private sealed class _Thread_1106 : Sharpen.Thread
		{
			public _Thread_1106(ClientRMService rmService, SubmitApplicationRequest submitRequest1
				)
			{
				this.rmService = rmService;
				this.submitRequest1 = submitRequest1;
			}

			public override void Run()
			{
				try
				{
					rmService.SubmitApplication(submitRequest1);
				}
				catch (YarnException)
				{
				}
			}

			private readonly ClientRMService rmService;

			private readonly SubmitApplicationRequest submitRequest1;
		}

		private SubmitApplicationRequest MockSubmitAppRequest(ApplicationId appId, string
			 name, string queue)
		{
			return MockSubmitAppRequest(appId, name, queue, null);
		}

		private SubmitApplicationRequest MockSubmitAppRequest(ApplicationId appId, string
			 name, string queue, ICollection<string> tags)
		{
			return MockSubmitAppRequest(appId, name, queue, tags, false);
		}

		private SubmitApplicationRequest MockSubmitAppRequest(ApplicationId appId, string
			 name, string queue, ICollection<string> tags, bool unmanaged)
		{
			ContainerLaunchContext amContainerSpec = Org.Mockito.Mockito.Mock<ContainerLaunchContext
				>();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource resource = Resources.CreateResource(YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationMb);
			ApplicationSubmissionContext submissionContext = recordFactory.NewRecordInstance<
				ApplicationSubmissionContext>();
			submissionContext.SetAMContainerSpec(amContainerSpec);
			submissionContext.SetApplicationName(name);
			submissionContext.SetQueue(queue);
			submissionContext.SetApplicationId(appId);
			submissionContext.SetResource(resource);
			submissionContext.SetApplicationType(appType);
			submissionContext.SetApplicationTags(tags);
			submissionContext.SetUnmanagedAM(unmanaged);
			SubmitApplicationRequest submitRequest = recordFactory.NewRecordInstance<SubmitApplicationRequest
				>();
			submitRequest.SetApplicationSubmissionContext(submissionContext);
			return submitRequest;
		}

		/// <exception cref="System.IO.IOException"/>
		private void MockRMContext(YarnScheduler yarnScheduler, RMContext rmContext)
		{
			Dispatcher dispatcher = Org.Mockito.Mockito.Mock<Dispatcher>();
			Org.Mockito.Mockito.When(rmContext.GetDispatcher()).ThenReturn(dispatcher);
			EventHandler eventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			Org.Mockito.Mockito.When(dispatcher.GetEventHandler()).ThenReturn(eventHandler);
			QueueInfo queInfo = recordFactory.NewRecordInstance<QueueInfo>();
			queInfo.SetQueueName("testqueue");
			Org.Mockito.Mockito.When(yarnScheduler.GetQueueInfo(Matchers.Eq("testqueue"), Matchers.AnyBoolean
				(), Matchers.AnyBoolean())).ThenReturn(queInfo);
			Org.Mockito.Mockito.When(yarnScheduler.GetQueueInfo(Matchers.Eq("nonexistentqueue"
				), Matchers.AnyBoolean(), Matchers.AnyBoolean())).ThenThrow(new IOException("queue does not exist"
				));
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			Org.Mockito.Mockito.When(rmContext.GetRMApplicationHistoryWriter()).ThenReturn(writer
				);
			SystemMetricsPublisher publisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>();
			Org.Mockito.Mockito.When(rmContext.GetSystemMetricsPublisher()).ThenReturn(publisher
				);
			Org.Mockito.Mockito.When(rmContext.GetYarnConfiguration()).ThenReturn(new YarnConfiguration
				());
			ConcurrentHashMap<ApplicationId, RMApp> apps = GetRMApps(rmContext, yarnScheduler
				);
			Org.Mockito.Mockito.When(rmContext.GetRMApps()).ThenReturn(apps);
			Org.Mockito.Mockito.When(yarnScheduler.GetAppsInQueue(Matchers.Eq("testqueue"))).
				ThenReturn(GetSchedulerApps(apps));
			ResourceScheduler rs = Org.Mockito.Mockito.Mock<ResourceScheduler>();
			Org.Mockito.Mockito.When(rmContext.GetScheduler()).ThenReturn(rs);
		}

		private ConcurrentHashMap<ApplicationId, RMApp> GetRMApps(RMContext rmContext, YarnScheduler
			 yarnScheduler)
		{
			ConcurrentHashMap<ApplicationId, RMApp> apps = new ConcurrentHashMap<ApplicationId
				, RMApp>();
			ApplicationId applicationId1 = GetApplicationId(1);
			ApplicationId applicationId2 = GetApplicationId(2);
			ApplicationId applicationId3 = GetApplicationId(3);
			YarnConfiguration config = new YarnConfiguration();
			apps[applicationId1] = GetRMApp(rmContext, yarnScheduler, applicationId1, config, 
				"testqueue", 10, 3);
			apps[applicationId2] = GetRMApp(rmContext, yarnScheduler, applicationId2, config, 
				"a", 20, 2);
			apps[applicationId3] = GetRMApp(rmContext, yarnScheduler, applicationId3, config, 
				"testqueue", 40, 5);
			return apps;
		}

		private IList<ApplicationAttemptId> GetSchedulerApps(IDictionary<ApplicationId, RMApp
			> apps)
		{
			IList<ApplicationAttemptId> schedApps = new AList<ApplicationAttemptId>();
			// Return app IDs for the apps in testqueue (as defined in getRMApps)
			schedApps.AddItem(ApplicationAttemptId.NewInstance(GetApplicationId(1), 0));
			schedApps.AddItem(ApplicationAttemptId.NewInstance(GetApplicationId(3), 0));
			return schedApps;
		}

		private static ApplicationId GetApplicationId(int id)
		{
			return ApplicationId.NewInstance(123456, id);
		}

		private static ApplicationAttemptId GetApplicationAttemptId(int id)
		{
			return ApplicationAttemptId.NewInstance(GetApplicationId(id), 1);
		}

		private RMAppImpl GetRMApp(RMContext rmContext, YarnScheduler yarnScheduler, ApplicationId
			 applicationId3, YarnConfiguration config, string queueName, long memorySeconds, 
			long vcoreSeconds)
		{
			ApplicationSubmissionContext asContext = Org.Mockito.Mockito.Mock<ApplicationSubmissionContext
				>();
			Org.Mockito.Mockito.When(asContext.GetMaxAppAttempts()).ThenReturn(1);
			RMAppImpl app = Org.Mockito.Mockito.Spy(new _RMAppImpl_1231(memorySeconds, vcoreSeconds
				, applicationId3, rmContext, config, null, null, queueName, asContext, yarnScheduler
				, null, Runtime.CurrentTimeMillis(), "YARN", null, BuilderUtils.NewResourceRequest
				(RMAppAttemptImpl.AmContainerPriority, ResourceRequest.Any, Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), 1)));
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(123456, 1), 1);
			RMAppAttemptImpl rmAppAttemptImpl = Org.Mockito.Mockito.Spy(new RMAppAttemptImpl(
				attemptId, rmContext, yarnScheduler, null, asContext, config, false, null));
			Container container = Container.NewInstance(ContainerId.NewContainerId(attemptId, 
				1), null, string.Empty, null, null, null);
			RMContainerImpl containerimpl = Org.Mockito.Mockito.Spy(new RMContainerImpl(container
				, attemptId, null, string.Empty, rmContext));
			IDictionary<ApplicationAttemptId, RMAppAttempt> attempts = new Dictionary<ApplicationAttemptId
				, RMAppAttempt>();
			attempts[attemptId] = rmAppAttemptImpl;
			Org.Mockito.Mockito.When(app.GetCurrentAppAttempt()).ThenReturn(rmAppAttemptImpl);
			Org.Mockito.Mockito.When(app.GetAppAttempts()).ThenReturn(attempts);
			Org.Mockito.Mockito.When(rmAppAttemptImpl.GetMasterContainer()).ThenReturn(container
				);
			ResourceScheduler rs = Org.Mockito.Mockito.Mock<ResourceScheduler>();
			Org.Mockito.Mockito.When(rmContext.GetScheduler()).ThenReturn(rs);
			Org.Mockito.Mockito.When(rmContext.GetScheduler().GetRMContainer(Matchers.Any<ContainerId
				>())).ThenReturn(containerimpl);
			SchedulerAppReport sAppReport = Org.Mockito.Mockito.Mock<SchedulerAppReport>();
			Org.Mockito.Mockito.When(rmContext.GetScheduler().GetSchedulerAppInfo(Matchers.Any
				<ApplicationAttemptId>())).ThenReturn(sAppReport);
			IList<RMContainer> rmContainers = new AList<RMContainer>();
			rmContainers.AddItem(containerimpl);
			Org.Mockito.Mockito.When(rmContext.GetScheduler().GetSchedulerAppInfo(attemptId).
				GetLiveContainers()).ThenReturn(rmContainers);
			ContainerStatus cs = Org.Mockito.Mockito.Mock<ContainerStatus>();
			Org.Mockito.Mockito.When(containerimpl.GetFinishedStatus()).ThenReturn(cs);
			Org.Mockito.Mockito.When(containerimpl.GetDiagnosticsInfo()).ThenReturn("N/A");
			Org.Mockito.Mockito.When(containerimpl.GetContainerExitStatus()).ThenReturn(0);
			Org.Mockito.Mockito.When(containerimpl.GetContainerState()).ThenReturn(ContainerState
				.Complete);
			return app;
		}

		private sealed class _RMAppImpl_1231 : RMAppImpl
		{
			public _RMAppImpl_1231(long memorySeconds, long vcoreSeconds, ApplicationId baseArg1
				, RMContext baseArg2, Configuration baseArg3, string baseArg4, string baseArg5, 
				string baseArg6, ApplicationSubmissionContext baseArg7, YarnScheduler baseArg8, 
				ApplicationMasterService baseArg9, long baseArg10, string baseArg11, ICollection
				<string> baseArg12, ResourceRequest baseArg13)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					, baseArg9, baseArg10, baseArg11, baseArg12, baseArg13)
			{
				this.memorySeconds = memorySeconds;
				this.vcoreSeconds = vcoreSeconds;
			}

			public override ApplicationReport CreateAndGetApplicationReport(string clientUserName
				, bool allowAccess)
			{
				ApplicationReport report = base.CreateAndGetApplicationReport(clientUserName, allowAccess
					);
				ApplicationResourceUsageReport usageReport = report.GetApplicationResourceUsageReport
					();
				usageReport.SetMemorySeconds(memorySeconds);
				usageReport.SetVcoreSeconds(vcoreSeconds);
				report.SetApplicationResourceUsageReport(usageReport);
				return report;
			}

			private readonly long memorySeconds;

			private readonly long vcoreSeconds;
		}

		private static YarnScheduler MockYarnScheduler()
		{
			YarnScheduler yarnScheduler = Org.Mockito.Mockito.Mock<YarnScheduler>();
			Org.Mockito.Mockito.When(yarnScheduler.GetMinimumResourceCapability()).ThenReturn
				(Resources.CreateResource(YarnConfiguration.DefaultRmSchedulerMinimumAllocationMb
				));
			Org.Mockito.Mockito.When(yarnScheduler.GetMaximumResourceCapability()).ThenReturn
				(Resources.CreateResource(YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb
				));
			Org.Mockito.Mockito.When(yarnScheduler.GetAppsInQueue(Queue1)).ThenReturn(Arrays.
				AsList(GetApplicationAttemptId(101), GetApplicationAttemptId(102)));
			Org.Mockito.Mockito.When(yarnScheduler.GetAppsInQueue(Queue2)).ThenReturn(Arrays.
				AsList(GetApplicationAttemptId(103)));
			ApplicationAttemptId attemptId = GetApplicationAttemptId(1);
			Org.Mockito.Mockito.When(yarnScheduler.GetAppResourceUsageReport(attemptId)).ThenReturn
				(null);
			ResourceCalculator rc = new DefaultResourceCalculator();
			Org.Mockito.Mockito.When(yarnScheduler.GetResourceCalculator()).ThenReturn(rc);
			return yarnScheduler;
		}

		[NUnit.Framework.Test]
		public virtual void TestReservationAPIs()
		{
			// initialize
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			ReservationSystemTestUtil.SetupQueueConfiguration(conf);
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			conf.SetBoolean(YarnConfiguration.RmReservationSystemEnable, true);
			MockRM rm = new MockRM(conf);
			rm.Start();
			MockNM nm;
			try
			{
				nm = rm.RegisterNode("127.0.0.1:1", 102400, 100);
				// allow plan follower to synchronize
				Sharpen.Thread.Sleep(1050);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			// Create a client.
			ClientRMService clientService = rm.GetClientRMService();
			// create a reservation
			Clock clock = new UTCClock();
			long arrival = clock.GetTime();
			long duration = 60000;
			long deadline = (long)(arrival + 1.05 * duration);
			ReservationSubmissionRequest sRequest = CreateSimpleReservationRequest(4, arrival
				, deadline, duration);
			ReservationSubmissionResponse sResponse = null;
			try
			{
				sResponse = clientService.SubmitReservation(sRequest);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNotNull(sResponse);
			ReservationId reservationID = sResponse.GetReservationId();
			NUnit.Framework.Assert.IsNotNull(reservationID);
			Log.Info("Submit reservation response: " + reservationID);
			// Update the reservation
			ReservationDefinition rDef = sRequest.GetReservationDefinition();
			ReservationRequest rr = rDef.GetReservationRequests().GetReservationResources()[0
				];
			rr.SetNumContainers(5);
			arrival = clock.GetTime();
			duration = 30000;
			deadline = (long)(arrival + 1.05 * duration);
			rr.SetDuration(duration);
			rDef.SetArrival(arrival);
			rDef.SetDeadline(deadline);
			ReservationUpdateRequest uRequest = ReservationUpdateRequest.NewInstance(rDef, reservationID
				);
			ReservationUpdateResponse uResponse = null;
			try
			{
				uResponse = clientService.UpdateReservation(uRequest);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNotNull(sResponse);
			Log.Info("Update reservation response: " + uResponse);
			// Delete the reservation
			ReservationDeleteRequest dRequest = ReservationDeleteRequest.NewInstance(reservationID
				);
			ReservationDeleteResponse dResponse = null;
			try
			{
				dResponse = clientService.DeleteReservation(dRequest);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			NUnit.Framework.Assert.IsNotNull(sResponse);
			Log.Info("Delete reservation response: " + dResponse);
			// clean-up
			rm.Stop();
			nm = null;
			rm = null;
		}

		private ReservationSubmissionRequest CreateSimpleReservationRequest(int numContainers
			, long arrival, long deadline, long duration)
		{
			// create a request with a single atomic ask
			ReservationRequest r = ReservationRequest.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1024, 1), numContainers, 1, duration);
			ReservationRequests reqs = ReservationRequests.NewInstance(Sharpen.Collections.SingletonList
				(r), ReservationRequestInterpreter.RAll);
			ReservationDefinition rDef = ReservationDefinition.NewInstance(arrival, deadline, 
				reqs, "testClientRMService#reservation");
			ReservationSubmissionRequest request = ReservationSubmissionRequest.NewInstance(rDef
				, ReservationSystemTestUtil.reservationQ);
			return request;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNodeLabels()
		{
			MockRM rm = new _MockRM_1400();
			rm.Start();
			RMNodeLabelsManager labelsMgr = rm.GetRMContext().GetNodeLabelManager();
			labelsMgr.AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
			NodeId node1 = NodeId.NewInstance("host1", 1234);
			NodeId node2 = NodeId.NewInstance("host2", 1234);
			IDictionary<NodeId, ICollection<string>> map = new Dictionary<NodeId, ICollection
				<string>>();
			map[node1] = ImmutableSet.Of("x");
			map[node2] = ImmutableSet.Of("y");
			labelsMgr.ReplaceLabelsOnNode(map);
			// Create a client.
			Configuration conf = new Configuration();
			YarnRPC rpc = YarnRPC.Create(conf);
			IPEndPoint rmAddress = rm.GetClientRMService().GetBindAddress();
			Log.Info("Connecting to ResourceManager at " + rmAddress);
			ApplicationClientProtocol client = (ApplicationClientProtocol)rpc.GetProxy(typeof(
				ApplicationClientProtocol), rmAddress, conf);
			// Get node labels collection
			GetClusterNodeLabelsResponse response = client.GetClusterNodeLabels(GetClusterNodeLabelsRequest
				.NewInstance());
			NUnit.Framework.Assert.IsTrue(response.GetNodeLabels().ContainsAll(Arrays.AsList(
				"x", "y")));
			// Get node labels mapping
			GetNodesToLabelsResponse response1 = client.GetNodeToLabels(GetNodesToLabelsRequest
				.NewInstance());
			IDictionary<NodeId, ICollection<string>> nodeToLabels = response1.GetNodeToLabels
				();
			NUnit.Framework.Assert.IsTrue(nodeToLabels.Keys.ContainsAll(Arrays.AsList(node1, 
				node2)));
			NUnit.Framework.Assert.IsTrue(nodeToLabels[node1].ContainsAll(Arrays.AsList("x"))
				);
			NUnit.Framework.Assert.IsTrue(nodeToLabels[node2].ContainsAll(Arrays.AsList("y"))
				);
			rpc.StopProxy(client, conf);
			rm.Close();
		}

		private sealed class _MockRM_1400 : MockRM
		{
			public _MockRM_1400()
			{
			}

			protected internal override ClientRMService CreateClientRMService()
			{
				return new ClientRMService(this.rmContext, this.scheduler, this.rmAppManager, this
					.applicationACLsManager, this.queueACLsManager, this.GetRMContext().GetRMDelegationTokenSecretManager
					());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetLabelsToNodes()
		{
			MockRM rm = new _MockRM_1449();
			rm.Start();
			RMNodeLabelsManager labelsMgr = rm.GetRMContext().GetNodeLabelManager();
			labelsMgr.AddToCluserNodeLabels(ImmutableSet.Of("x", "y", "z"));
			NodeId node1A = NodeId.NewInstance("host1", 1234);
			NodeId node1B = NodeId.NewInstance("host1", 5678);
			NodeId node2A = NodeId.NewInstance("host2", 1234);
			NodeId node3A = NodeId.NewInstance("host3", 1234);
			NodeId node3B = NodeId.NewInstance("host3", 5678);
			IDictionary<NodeId, ICollection<string>> map = new Dictionary<NodeId, ICollection
				<string>>();
			map[node1A] = ImmutableSet.Of("x");
			map[node1B] = ImmutableSet.Of("z");
			map[node2A] = ImmutableSet.Of("y");
			map[node3A] = ImmutableSet.Of("y");
			map[node3B] = ImmutableSet.Of("z");
			labelsMgr.ReplaceLabelsOnNode(map);
			// Create a client.
			Configuration conf = new Configuration();
			YarnRPC rpc = YarnRPC.Create(conf);
			IPEndPoint rmAddress = rm.GetClientRMService().GetBindAddress();
			Log.Info("Connecting to ResourceManager at " + rmAddress);
			ApplicationClientProtocol client = (ApplicationClientProtocol)rpc.GetProxy(typeof(
				ApplicationClientProtocol), rmAddress, conf);
			// Get node labels collection
			GetClusterNodeLabelsResponse response = client.GetClusterNodeLabels(GetClusterNodeLabelsRequest
				.NewInstance());
			NUnit.Framework.Assert.IsTrue(response.GetNodeLabels().ContainsAll(Arrays.AsList(
				"x", "y", "z")));
			// Get labels to nodes mapping
			GetLabelsToNodesResponse response1 = client.GetLabelsToNodes(GetLabelsToNodesRequest
				.NewInstance());
			IDictionary<string, ICollection<NodeId>> labelsToNodes = response1.GetLabelsToNodes
				();
			NUnit.Framework.Assert.IsTrue(labelsToNodes.Keys.ContainsAll(Arrays.AsList("x", "y"
				, "z")));
			NUnit.Framework.Assert.IsTrue(labelsToNodes["x"].ContainsAll(Arrays.AsList(node1A
				)));
			NUnit.Framework.Assert.IsTrue(labelsToNodes["y"].ContainsAll(Arrays.AsList(node2A
				, node3A)));
			NUnit.Framework.Assert.IsTrue(labelsToNodes["z"].ContainsAll(Arrays.AsList(node1B
				, node3B)));
			// Get labels to nodes mapping for specific labels
			ICollection<string> setlabels = new HashSet<string>(Arrays.AsList(new string[] { 
				"x", "z" }));
			GetLabelsToNodesResponse response2 = client.GetLabelsToNodes(GetLabelsToNodesRequest
				.NewInstance(setlabels));
			labelsToNodes = response2.GetLabelsToNodes();
			NUnit.Framework.Assert.IsTrue(labelsToNodes.Keys.ContainsAll(Arrays.AsList("x", "z"
				)));
			NUnit.Framework.Assert.IsTrue(labelsToNodes["x"].ContainsAll(Arrays.AsList(node1A
				)));
			NUnit.Framework.Assert.IsTrue(labelsToNodes["z"].ContainsAll(Arrays.AsList(node1B
				, node3B)));
			NUnit.Framework.Assert.AreEqual(labelsToNodes["y"], null);
			rpc.StopProxy(client, conf);
			rm.Close();
		}

		private sealed class _MockRM_1449 : MockRM
		{
			public _MockRM_1449()
			{
			}

			protected internal override ClientRMService CreateClientRMService()
			{
				return new ClientRMService(this.rmContext, this.scheduler, this.rmAppManager, this
					.applicationACLsManager, this.queueACLsManager, this.GetRMContext().GetRMDelegationTokenSecretManager
					());
			}
		}
	}
}
