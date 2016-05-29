using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Com.Google.Inject;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Applicationsmanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebApp
	{
		internal const int GiB = 1024;

		// MiB
		[NUnit.Framework.Test]
		public virtual void TestControllerIndex()
		{
			Injector injector = WebAppTests.CreateMockInjector<TestRMWebApp>(this, new _Module_82
				());
			RmController c = injector.GetInstance<RmController>();
			c.Index();
			NUnit.Framework.Assert.AreEqual("Applications", c.Get(Params.Title, "unknown"));
		}

		private sealed class _Module_82 : Module
		{
			public _Module_82()
			{
			}

			public void Configure(Binder binder)
			{
				binder.Bind<ApplicationACLsManager>().ToInstance(new ApplicationACLsManager(new Configuration
					()));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestView()
		{
			Injector injector = WebAppTests.CreateMockInjector<RMContext>(MockRMContext(15, 1
				, 2, 8 * GiB), new _Module_98());
			RmView rmViewInstance = injector.GetInstance<RmView>();
			rmViewInstance.Set(YarnWebParams.AppState, YarnApplicationState.Running.ToString(
				));
			rmViewInstance.Render();
			WebAppTests.FlushOutput(injector);
			rmViewInstance.Set(YarnWebParams.AppState, StringHelper.Cjoin(YarnApplicationState
				.Accepted.ToString(), YarnApplicationState.Running.ToString()));
			rmViewInstance.Render();
			WebAppTests.FlushOutput(injector);
		}

		private sealed class _Module_98 : Module
		{
			public _Module_98()
			{
			}

			public void Configure(Binder binder)
			{
				try
				{
					ResourceManager mockRm = TestRMWebApp.MockRm(3, 1, 2, 8 * TestRMWebApp.GiB);
					binder.Bind<ResourceManager>().ToInstance(mockRm);
					binder.Bind<ApplicationBaseProtocol>().ToInstance(mockRm.GetClientRMService());
				}
				catch (IOException e)
				{
					throw new InvalidOperationException(e);
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestNodesPage()
		{
			// 10 nodes. Two of each type.
			RMContext rmContext = MockRMContext(3, 2, 12, 8 * GiB);
			Injector injector = WebAppTests.CreateMockInjector<RMContext>(rmContext, new _Module_129
				(rmContext));
			// All nodes
			NodesPage instance = injector.GetInstance<NodesPage>();
			instance.Render();
			WebAppTests.FlushOutput(injector);
			// Unhealthy nodes
			instance.MoreParams()[YarnWebParams.NodeState] = NodeState.Unhealthy.ToString();
			instance.Render();
			WebAppTests.FlushOutput(injector);
			// Lost nodes
			instance.MoreParams()[YarnWebParams.NodeState] = NodeState.Lost.ToString();
			instance.Render();
			WebAppTests.FlushOutput(injector);
		}

		private sealed class _Module_129 : Module
		{
			public _Module_129(RMContext rmContext)
			{
				this.rmContext = rmContext;
			}

			public void Configure(Binder binder)
			{
				try
				{
					binder.Bind<ResourceManager>().ToInstance(TestRMWebApp.MockRm(rmContext));
				}
				catch (IOException e)
				{
					throw new InvalidOperationException(e);
				}
			}

			private readonly RMContext rmContext;
		}

		public static RMContext MockRMContext(int numApps, int racks, int numNodes, int mbsPerNode
			)
		{
			IList<RMApp> apps = MockAsm.NewApplications(numApps);
			ConcurrentMap<ApplicationId, RMApp> applicationsMaps = Maps.NewConcurrentMap();
			foreach (RMApp app in apps)
			{
				applicationsMaps[app.GetApplicationId()] = app;
			}
			IList<RMNode> nodes = MockNodes.NewNodes(racks, numNodes, MockNodes.NewResource(mbsPerNode
				));
			ConcurrentMap<NodeId, RMNode> nodesMap = Maps.NewConcurrentMap();
			foreach (RMNode node in nodes)
			{
				nodesMap[node.GetNodeID()] = node;
			}
			IList<RMNode> deactivatedNodes = MockNodes.DeactivatedNodes(racks, numNodes, MockNodes.NewResource
				(mbsPerNode));
			ConcurrentMap<string, RMNode> deactivatedNodesMap = Maps.NewConcurrentMap();
			foreach (RMNode node_1 in deactivatedNodes)
			{
				deactivatedNodesMap[node_1.GetHostName()] = node_1;
			}
			RMContextImpl rmContext = new _RMContextImpl_183(applicationsMaps, deactivatedNodesMap
				, nodesMap, null, null, null, null, null, null, null, null, null, null);
			rmContext.SetNodeLabelManager(new NullRMNodeLabelsManager());
			return rmContext;
		}

		private sealed class _RMContextImpl_183 : RMContextImpl
		{
			public _RMContextImpl_183(ConcurrentMap<ApplicationId, RMApp> applicationsMaps, ConcurrentMap
				<string, RMNode> deactivatedNodesMap, ConcurrentMap<NodeId, RMNode> nodesMap, Dispatcher
				 baseArg1, ContainerAllocationExpirer baseArg2, AMLivelinessMonitor baseArg3, AMLivelinessMonitor
				 baseArg4, DelegationTokenRenewer baseArg5, AMRMTokenSecretManager baseArg6, RMContainerTokenSecretManager
				 baseArg7, NMTokenSecretManagerInRM baseArg8, ClientToAMTokenSecretManagerInRM baseArg9
				, ResourceScheduler baseArg10)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					, baseArg9, baseArg10)
			{
				this.applicationsMaps = applicationsMaps;
				this.deactivatedNodesMap = deactivatedNodesMap;
				this.nodesMap = nodesMap;
			}

			public override ConcurrentMap<ApplicationId, RMApp> GetRMApps()
			{
				return applicationsMaps;
			}

			public override ConcurrentMap<string, RMNode> GetInactiveRMNodes()
			{
				return deactivatedNodesMap;
			}

			public override ConcurrentMap<NodeId, RMNode> GetRMNodes()
			{
				return nodesMap;
			}

			private readonly ConcurrentMap<ApplicationId, RMApp> applicationsMaps;

			private readonly ConcurrentMap<string, RMNode> deactivatedNodesMap;

			private readonly ConcurrentMap<NodeId, RMNode> nodesMap;
		}

		/// <exception cref="System.IO.IOException"/>
		public static ResourceManager MockRm(int apps, int racks, int nodes, int mbsPerNode
			)
		{
			RMContext rmContext = MockRMContext(apps, racks, nodes, mbsPerNode);
			return MockRm(rmContext);
		}

		/// <exception cref="System.IO.IOException"/>
		public static ResourceManager MockRm(RMContext rmContext)
		{
			ResourceManager rm = Org.Mockito.Mockito.Mock<ResourceManager>();
			ResourceScheduler rs = MockCapacityScheduler();
			ApplicationACLsManager aclMgr = MockAppACLsManager();
			ClientRMService clientRMService = MockClientRMService(rmContext);
			Org.Mockito.Mockito.When(rm.GetResourceScheduler()).ThenReturn(rs);
			Org.Mockito.Mockito.When(rm.GetRMContext()).ThenReturn(rmContext);
			Org.Mockito.Mockito.When(rm.GetApplicationACLsManager()).ThenReturn(aclMgr);
			Org.Mockito.Mockito.When(rm.GetClientRMService()).ThenReturn(clientRMService);
			return rm;
		}

		/// <exception cref="System.IO.IOException"/>
		public static CapacityScheduler MockCapacityScheduler()
		{
			// stolen from TestCapacityScheduler
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			CapacityScheduler cs = new CapacityScheduler();
			cs.SetConf(new YarnConfiguration());
			RMContext rmContext = new RMContextImpl(null, null, null, null, null, null, new RMContainerTokenSecretManager
				(conf), new NMTokenSecretManagerInRM(conf), new ClientToAMTokenSecretManagerInRM
				(), null);
			rmContext.SetNodeLabelManager(new NullRMNodeLabelsManager());
			cs.SetRMContext(rmContext);
			cs.Init(conf);
			return cs;
		}

		public static ApplicationACLsManager MockAppACLsManager()
		{
			Configuration conf = new Configuration();
			return new ApplicationACLsManager(conf);
		}

		public static ClientRMService MockClientRMService(RMContext rmContext)
		{
			ClientRMService clientRMService = Org.Mockito.Mockito.Mock<ClientRMService>();
			IList<ApplicationReport> appReports = new AList<ApplicationReport>();
			foreach (RMApp app in rmContext.GetRMApps().Values)
			{
				ApplicationReport appReport = ApplicationReport.NewInstance(app.GetApplicationId(
					), (ApplicationAttemptId)null, app.GetUser(), app.GetQueue(), app.GetName(), (string
					)null, 0, (Token)null, app.CreateApplicationState(), app.GetDiagnostics().ToString
					(), (string)null, app.GetStartTime(), app.GetFinishTime(), app.GetFinalApplicationStatus
					(), (ApplicationResourceUsageReport)null, app.GetTrackingUrl(), app.GetProgress(
					), app.GetApplicationType(), (Token)null);
				appReports.AddItem(appReport);
			}
			GetApplicationsResponse response = Org.Mockito.Mockito.Mock<GetApplicationsResponse
				>();
			Org.Mockito.Mockito.When(response.GetApplicationList()).ThenReturn(appReports);
			try
			{
				Org.Mockito.Mockito.When(clientRMService.GetApplications(Matchers.Any<GetApplicationsRequest
					>())).ThenReturn(response);
			}
			catch (YarnException)
			{
				NUnit.Framework.Assert.Fail("Exception is not expteced.");
			}
			return clientRMService;
		}

		internal static void SetupQueueConfiguration(CapacitySchedulerConfiguration conf)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b", "c" }
				);
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 20);
			string C = CapacitySchedulerConfiguration.Root + ".c";
			conf.SetCapacity(C, 70);
			// Define 2nd-level queues
			string A1 = A + ".a1";
			string A2 = A + ".a2";
			conf.SetQueues(A, new string[] { "a1", "a2" });
			conf.SetCapacity(A1, 30);
			conf.SetCapacity(A2, 70);
			string B1 = B + ".b1";
			string B2 = B + ".b2";
			string B3 = B + ".b3";
			conf.SetQueues(B, new string[] { "b1", "b2", "b3" });
			conf.SetCapacity(B1, 50);
			conf.SetCapacity(B2, 30);
			conf.SetCapacity(B3, 20);
			string C1 = C + ".c1";
			string C2 = C + ".c2";
			string C3 = C + ".c3";
			string C4 = C + ".c4";
			conf.SetQueues(C, new string[] { "c1", "c2", "c3", "c4" });
			conf.SetCapacity(C1, 50);
			conf.SetCapacity(C2, 10);
			conf.SetCapacity(C3, 35);
			conf.SetCapacity(C4, 5);
			// Define 3rd-level queues
			string C11 = C1 + ".c11";
			string C12 = C1 + ".c12";
			string C13 = C1 + ".c13";
			conf.SetQueues(C1, new string[] { "c11", "c12", "c13" });
			conf.SetCapacity(C11, 15);
			conf.SetCapacity(C12, 45);
			conf.SetCapacity(C13, 40);
		}

		/// <exception cref="System.Exception"/>
		public static ResourceManager MockFifoRm(int apps, int racks, int nodes, int mbsPerNode
			)
		{
			ResourceManager rm = Org.Mockito.Mockito.Mock<ResourceManager>();
			RMContext rmContext = MockRMContext(apps, racks, nodes, mbsPerNode);
			ResourceScheduler rs = MockFifoScheduler(rmContext);
			Org.Mockito.Mockito.When(rm.GetResourceScheduler()).ThenReturn(rs);
			Org.Mockito.Mockito.When(rm.GetRMContext()).ThenReturn(rmContext);
			return rm;
		}

		/// <exception cref="System.Exception"/>
		public static FifoScheduler MockFifoScheduler(RMContext rmContext)
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupFifoQueueConfiguration(conf);
			FifoScheduler fs = new FifoScheduler();
			fs.SetConf(new YarnConfiguration());
			fs.SetRMContext(rmContext);
			fs.Init(conf);
			return fs;
		}

		internal static void SetupFifoQueueConfiguration(CapacitySchedulerConfiguration conf
			)
		{
			// Define default queue
			conf.SetQueues("default", new string[] { "default" });
			conf.SetCapacity("default", 100);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			// For manual testing
			WebApps.$for("yarn", new TestRMWebApp()).At(8888).InDevMode().Start(new RMWebApp(
				MockRm(2500, 8, 8, 8 * GiB))).JoinThread();
			WebApps.$for("yarn", new TestRMWebApp()).At(8888).InDevMode().Start(new RMWebApp(
				MockFifoRm(10, 1, 4, 8 * GiB))).JoinThread();
		}
	}
}
