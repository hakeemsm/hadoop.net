using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestContainerAllocation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestFifoScheduler));

		private readonly int Gb = 1024;

		private YarnConfiguration conf;

		internal RMNodeLabelsManager mgr;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			mgr = new NullRMNodeLabelsManager();
			mgr.Init(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestExcessReservationThanNodeManagerCapacity()
		{
			MockRM rm = new MockRM(conf);
			rm.Start();
			// Register node1
			MockNM nm1 = rm.RegisterNode("127.0.0.1:1234", 2 * Gb, 4);
			MockNM nm2 = rm.RegisterNode("127.0.0.1:2234", 3 * Gb, 4);
			nm1.NodeHeartbeat(true);
			nm2.NodeHeartbeat(true);
			// wait..
			int waitCount = 20;
			int size = rm.GetRMContext().GetRMNodes().Count;
			while ((size = rm.GetRMContext().GetRMNodes().Count) != 2 && waitCount-- > 0)
			{
				Log.Info("Waiting for node managers to register : " + size);
				Sharpen.Thread.Sleep(100);
			}
			NUnit.Framework.Assert.AreEqual(2, rm.GetRMContext().GetRMNodes().Count);
			// Submit an application
			RMApp app1 = rm.SubmitApp(128);
			// kick the scheduling
			nm1.NodeHeartbeat(true);
			RMAppAttempt attempt1 = app1.GetCurrentAppAttempt();
			MockAM am1 = rm.SendAMLaunched(attempt1.GetAppAttemptId());
			am1.RegisterAppAttempt();
			Log.Info("sending container requests ");
			am1.AddRequests(new string[] { "*" }, 2 * Gb, 1, 1);
			AllocateResponse alloc1Response = am1.Schedule();
			// send the request
			// kick the scheduler
			nm1.NodeHeartbeat(true);
			int waitCounter = 20;
			Log.Info("heartbeating nm1");
			while (alloc1Response.GetAllocatedContainers().Count < 1 && waitCounter-- > 0)
			{
				Log.Info("Waiting for containers to be created for app 1...");
				Sharpen.Thread.Sleep(500);
				alloc1Response = am1.Schedule();
			}
			Log.Info("received container : " + alloc1Response.GetAllocatedContainers().Count);
			// No container should be allocated.
			// Internally it should not been reserved.
			NUnit.Framework.Assert.IsTrue(alloc1Response.GetAllocatedContainers().Count == 0);
			Log.Info("heartbeating nm2");
			waitCounter = 20;
			nm2.NodeHeartbeat(true);
			while (alloc1Response.GetAllocatedContainers().Count < 1 && waitCounter-- > 0)
			{
				Log.Info("Waiting for containers to be created for app 1...");
				Sharpen.Thread.Sleep(500);
				alloc1Response = am1.Schedule();
			}
			Log.Info("received container : " + alloc1Response.GetAllocatedContainers().Count);
			NUnit.Framework.Assert.IsTrue(alloc1Response.GetAllocatedContainers().Count == 1);
			rm.Stop();
		}

		// This is to test container tokens are generated when the containers are
		// acquired by the AM, not when the containers are allocated
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerTokenGeneratedOnPullRequest()
		{
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("127.0.0.1:1234", 8000);
			RMApp app1 = rm1.SubmitApp(200);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			// request a container.
			am1.Allocate("127.0.0.1", 1024, 1, new AList<ContainerId>());
			ContainerId containerId2 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 2);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Allocated);
			RMContainer container = rm1.GetResourceScheduler().GetRMContainer(containerId2);
			// no container token is generated.
			NUnit.Framework.Assert.AreEqual(containerId2, container.GetContainerId());
			NUnit.Framework.Assert.IsNull(container.GetContainer().GetContainerToken());
			// acquire the container.
			IList<Container> containers = am1.Allocate(new AList<ResourceRequest>(), new AList
				<ContainerId>()).GetAllocatedContainers();
			NUnit.Framework.Assert.AreEqual(containerId2, containers[0].GetId());
			// container token is generated.
			NUnit.Framework.Assert.IsNotNull(containers[0].GetContainerToken());
			rm1.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNormalContainerAllocationWhenDNSUnavailable()
		{
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("unknownhost:1234", 8000);
			RMApp app1 = rm1.SubmitApp(200);
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			// request a container.
			am1.Allocate("127.0.0.1", 1024, 1, new AList<ContainerId>());
			ContainerId containerId2 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
				(), 2);
			rm1.WaitForState(nm1, containerId2, RMContainerState.Allocated);
			// acquire the container.
			SecurityUtilTestHelper.SetTokenServiceUseIp(true);
			IList<Container> containers = am1.Allocate(new AList<ResourceRequest>(), new AList
				<ContainerId>()).GetAllocatedContainers();
			// not able to fetch the container;
			NUnit.Framework.Assert.AreEqual(0, containers.Count);
			SecurityUtilTestHelper.SetTokenServiceUseIp(false);
			containers = am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>())
				.GetAllocatedContainers();
			// should be able to fetch the container;
			NUnit.Framework.Assert.AreEqual(1, containers.Count);
		}

		// This is to test whether LogAggregationContext is passed into
		// container tokens correctly
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLogAggregationContextPassedIntoContainerToken()
		{
			MockRM rm1 = new MockRM(conf);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("127.0.0.1:1234", 8000);
			MockNM nm2 = rm1.RegisterNode("127.0.0.1:2345", 8000);
			// LogAggregationContext is set as null
			NUnit.Framework.Assert.IsNull(GetLogAggregationContextFromContainerToken(rm1, nm1
				, null));
			// create a not-null LogAggregationContext
			LogAggregationContext logAggregationContext = LogAggregationContext.NewInstance("includePattern"
				, "excludePattern", "rolledLogsIncludePattern", "rolledLogsExcludePattern");
			LogAggregationContext returned = GetLogAggregationContextFromContainerToken(rm1, 
				nm2, logAggregationContext);
			NUnit.Framework.Assert.AreEqual("includePattern", returned.GetIncludePattern());
			NUnit.Framework.Assert.AreEqual("excludePattern", returned.GetExcludePattern());
			NUnit.Framework.Assert.AreEqual("rolledLogsIncludePattern", returned.GetRolledLogsIncludePattern
				());
			NUnit.Framework.Assert.AreEqual("rolledLogsExcludePattern", returned.GetRolledLogsExcludePattern
				());
			rm1.Stop();
		}

		/// <exception cref="System.Exception"/>
		private LogAggregationContext GetLogAggregationContextFromContainerToken(MockRM rm1
			, MockNM nm1, LogAggregationContext logAggregationContext)
		{
			RMApp app2 = rm1.SubmitApp(200, logAggregationContext);
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm1, nm1);
			nm1.NodeHeartbeat(true);
			// request a container.
			am2.Allocate("127.0.0.1", 512, 1, new AList<ContainerId>());
			ContainerId containerId = ContainerId.NewContainerId(am2.GetApplicationAttemptId(
				), 2);
			rm1.WaitForState(nm1, containerId, RMContainerState.Allocated);
			// acquire the container.
			IList<Container> containers = am2.Allocate(new AList<ResourceRequest>(), new AList
				<ContainerId>()).GetAllocatedContainers();
			NUnit.Framework.Assert.AreEqual(containerId, containers[0].GetId());
			// container token is generated.
			NUnit.Framework.Assert.IsNotNull(containers[0].GetContainerToken());
			ContainerTokenIdentifier token = BuilderUtils.NewContainerTokenIdentifier(containers
				[0].GetContainerToken());
			return token.GetLogAggregationContext();
		}

		private volatile int numRetries = 0;

		private class TestRMSecretManagerService : RMSecretManagerService
		{
			public TestRMSecretManagerService(TestContainerAllocation _enclosing, Configuration
				 conf, RMContextImpl rmContext)
				: base(conf, rmContext)
			{
				this._enclosing = _enclosing;
			}

			protected internal override RMContainerTokenSecretManager CreateContainerTokenSecretManager
				(Configuration conf)
			{
				return new _RMContainerTokenSecretManager_283(this, conf);
			}

			private sealed class _RMContainerTokenSecretManager_283 : RMContainerTokenSecretManager
			{
				public _RMContainerTokenSecretManager_283(TestRMSecretManagerService _enclosing, 
					Configuration baseArg1)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				public override Token CreateContainerToken(ContainerId containerId, NodeId nodeId
					, string appSubmitter, Resource capability, Priority priority, long createTime, 
					LogAggregationContext logAggregationContext)
				{
					this._enclosing._enclosing.numRetries++;
					return base.CreateContainerToken(containerId, nodeId, appSubmitter, capability, priority
						, createTime, logAggregationContext);
				}

				private readonly TestRMSecretManagerService _enclosing;
			}

			private readonly TestContainerAllocation _enclosing;
		}

		// This is to test fetching AM container will be retried, if AM container is
		// not fetchable since DNS is unavailable causing container token/NMtoken
		// creation failure.
		/// <exception cref="System.Exception"/>
		public virtual void TestAMContainerAllocationWhenDNSUnavailable()
		{
			MockRM rm1 = new _MockRM_303(this, conf);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("unknownhost:1234", 8000);
			SecurityUtilTestHelper.SetTokenServiceUseIp(true);
			RMApp app1 = rm1.SubmitApp(200);
			RMAppAttempt attempt = app1.GetCurrentAppAttempt();
			nm1.NodeHeartbeat(true);
			// fetching am container will fail, keep retrying 5 times.
			while (numRetries <= 5)
			{
				nm1.NodeHeartbeat(true);
				Sharpen.Thread.Sleep(1000);
				NUnit.Framework.Assert.AreEqual(RMAppAttemptState.Scheduled, attempt.GetAppAttemptState
					());
				System.Console.Out.WriteLine("Waiting for am container to be allocated.");
			}
			SecurityUtilTestHelper.SetTokenServiceUseIp(false);
			rm1.WaitForState(attempt.GetAppAttemptId(), RMAppAttemptState.Allocated);
			MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
		}

		private sealed class _MockRM_303 : MockRM
		{
			public _MockRM_303(TestContainerAllocation _enclosing, Configuration baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
			}

			protected internal override RMSecretManagerService CreateRMSecretManagerService()
			{
				return new TestContainerAllocation.TestRMSecretManagerService(this, this._enclosing
					.conf, this.rmContext);
			}

			private readonly TestContainerAllocation _enclosing;
		}

		private Configuration GetConfigurationWithDefaultQueueLabels(Configuration config
			)
		{
			string A = CapacitySchedulerConfiguration.Root + ".a";
			string B = CapacitySchedulerConfiguration.Root + ".b";
			CapacitySchedulerConfiguration conf = (CapacitySchedulerConfiguration)GetConfigurationWithQueueLabels
				(config);
			new CapacitySchedulerConfiguration(config);
			conf.SetDefaultNodeLabelExpression(A, "x");
			conf.SetDefaultNodeLabelExpression(B, "y");
			return conf;
		}

		private Configuration GetConfigurationWithQueueLabels(Configuration config)
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(config);
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b", "c" }
				);
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "x", 100);
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "y", 100);
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			conf.SetMaximumCapacity(A, 15);
			conf.SetAccessibleNodeLabels(A, ToSet("x"));
			conf.SetCapacityByLabel(A, "x", 100);
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 20);
			conf.SetAccessibleNodeLabels(B, ToSet("y"));
			conf.SetCapacityByLabel(B, "y", 100);
			string C = CapacitySchedulerConfiguration.Root + ".c";
			conf.SetCapacity(C, 70);
			conf.SetMaximumCapacity(C, 70);
			conf.SetAccessibleNodeLabels(C, RMNodeLabelsManager.EmptyStringSet);
			// Define 2nd-level queues
			string A1 = A + ".a1";
			conf.SetQueues(A, new string[] { "a1" });
			conf.SetCapacity(A1, 100);
			conf.SetMaximumCapacity(A1, 100);
			conf.SetCapacityByLabel(A1, "x", 100);
			string B1 = B + ".b1";
			conf.SetQueues(B, new string[] { "b1" });
			conf.SetCapacity(B1, 100);
			conf.SetMaximumCapacity(B1, 100);
			conf.SetCapacityByLabel(B1, "y", 100);
			string C1 = C + ".c1";
			conf.SetQueues(C, new string[] { "c1" });
			conf.SetCapacity(C1, 100);
			conf.SetMaximumCapacity(C1, 100);
			return conf;
		}

		private void CheckTaskContainersHost(ApplicationAttemptId attemptId, ContainerId 
			containerId, ResourceManager rm, string host)
		{
			YarnScheduler scheduler = rm.GetRMContext().GetScheduler();
			SchedulerAppReport appReport = scheduler.GetSchedulerAppInfo(attemptId);
			NUnit.Framework.Assert.IsTrue(appReport.GetLiveContainers().Count > 0);
			foreach (RMContainer c in appReport.GetLiveContainers())
			{
				if (c.GetContainerId().Equals(containerId))
				{
					NUnit.Framework.Assert.AreEqual(host, c.GetAllocatedNode().GetHost());
				}
			}
		}

		private ICollection<E> ToSet<E>(params E[] elements)
		{
			ICollection<E> set = Sets.NewHashSet(elements);
			return set;
		}

		private Configuration GetComplexConfigurationWithQueueLabels(Configuration config
			)
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(config);
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b" });
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "x", 100);
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "y", 100);
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "z", 100);
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			conf.SetMaximumCapacity(A, 10);
			conf.SetAccessibleNodeLabels(A, ToSet("x", "y"));
			conf.SetCapacityByLabel(A, "x", 100);
			conf.SetCapacityByLabel(A, "y", 50);
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 90);
			conf.SetMaximumCapacity(B, 100);
			conf.SetAccessibleNodeLabels(B, ToSet("y", "z"));
			conf.SetCapacityByLabel(B, "y", 50);
			conf.SetCapacityByLabel(B, "z", 100);
			// Define 2nd-level queues
			string A1 = A + ".a1";
			conf.SetQueues(A, new string[] { "a1" });
			conf.SetCapacity(A1, 100);
			conf.SetMaximumCapacity(A1, 100);
			conf.SetAccessibleNodeLabels(A1, ToSet("x", "y"));
			conf.SetDefaultNodeLabelExpression(A1, "x");
			conf.SetCapacityByLabel(A1, "x", 100);
			conf.SetCapacityByLabel(A1, "y", 100);
			conf.SetQueues(B, new string[] { "b1", "b2" });
			string B1 = B + ".b1";
			conf.SetCapacity(B1, 50);
			conf.SetMaximumCapacity(B1, 50);
			conf.SetAccessibleNodeLabels(B1, RMNodeLabelsManager.EmptyStringSet);
			string B2 = B + ".b2";
			conf.SetCapacity(B2, 50);
			conf.SetMaximumCapacity(B2, 50);
			conf.SetAccessibleNodeLabels(B2, ToSet("y", "z"));
			conf.SetCapacityByLabel(B2, "y", 100);
			conf.SetCapacityByLabel(B2, "z", 100);
			return conf;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerAllocationWithSingleUserLimits()
		{
			RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
			mgr.Init(conf);
			// set node -> label
			mgr.AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
			mgr.AddLabelsToNode(ImmutableMap.Of(NodeId.NewInstance("h1", 0), ToSet("x"), NodeId
				.NewInstance("h2", 0), ToSet("y")));
			// inject node label manager
			MockRM rm1 = new _MockRM_471(mgr, GetConfigurationWithDefaultQueueLabels(conf));
			rm1.GetRMContext().SetNodeLabelManager(mgr);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("h1:1234", 8000);
			// label = x
			rm1.RegisterNode("h2:1234", 8000);
			// label = y
			MockNM nm3 = rm1.RegisterNode("h3:1234", 8000);
			// label = <empty>
			// launch an app to queue a1 (label = x), and check all container will
			// be allocated in h1
			RMApp app1 = rm1.SubmitApp(200, "app", "user", null, "a1");
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			// A has only 10% of x, so it can only allocate one container in label=empty
			ContainerId containerId = ContainerId.NewContainerId(am1.GetApplicationAttemptId(
				), 2);
			am1.Allocate("*", 1024, 1, new AList<ContainerId>(), string.Empty);
			NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm3, containerId, RMContainerState
				.Allocated, 10 * 1000));
			// Cannot allocate 2nd label=empty container
			containerId = ContainerId.NewContainerId(am1.GetApplicationAttemptId(), 3);
			am1.Allocate("*", 1024, 1, new AList<ContainerId>(), string.Empty);
			NUnit.Framework.Assert.IsFalse(rm1.WaitForState(nm3, containerId, RMContainerState
				.Allocated, 10 * 1000));
			// A has default user limit = 100, so it can use all resource in label = x
			// We can allocate floor(8000 / 1024) = 7 containers
			for (int id = 3; id <= 8; id++)
			{
				containerId = ContainerId.NewContainerId(am1.GetApplicationAttemptId(), id);
				am1.Allocate("*", 1024, 1, new AList<ContainerId>(), "x");
				NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm1, containerId, RMContainerState
					.Allocated, 10 * 1000));
			}
			rm1.Close();
		}

		private sealed class _MockRM_471 : MockRM
		{
			public _MockRM_471(RMNodeLabelsManager mgr, Configuration baseArg1)
				: base(baseArg1)
			{
				this.mgr = mgr;
			}

			protected internal override RMNodeLabelsManager CreateNodeLabelManager()
			{
				return mgr;
			}

			private readonly RMNodeLabelsManager mgr;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerAllocateWithComplexLabels()
		{
			/*
			* Queue structure:
			*                      root (*)
			*                  ________________
			*                 /                \
			*               a x(100%), y(50%)   b y(50%), z(100%)
			*               ________________    ______________
			*              /                   /              \
			*             a1 (x,y)         b1(no)              b2(y,z)
			*               100%                          y = 100%, z = 100%
			*
			* Node structure:
			* h1 : x
			* h2 : y
			* h3 : y
			* h4 : z
			* h5 : NO
			*
			* Total resource:
			* x: 4G
			* y: 6G
			* z: 2G
			* *: 2G
			*
			* Resource of
			* a1: x=4G, y=3G, NO=0.2G
			* b1: NO=0.9G (max=1G)
			* b2: y=3, z=2G, NO=0.9G (max=1G)
			*
			* Each node can only allocate two containers
			*/
			// set node -> label
			mgr.AddToCluserNodeLabels(ImmutableSet.Of("x", "y", "z"));
			mgr.AddLabelsToNode(ImmutableMap.Of(NodeId.NewInstance("h1", 0), ToSet("x"), NodeId
				.NewInstance("h2", 0), ToSet("y"), NodeId.NewInstance("h3", 0), ToSet("y"), NodeId
				.NewInstance("h4", 0), ToSet("z"), NodeId.NewInstance("h5", 0), RMNodeLabelsManager
				.EmptyStringSet));
			// inject node label manager
			MockRM rm1 = new _MockRM_557(this, GetComplexConfigurationWithQueueLabels(conf));
			rm1.GetRMContext().SetNodeLabelManager(mgr);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("h1:1234", 2048);
			MockNM nm2 = rm1.RegisterNode("h2:1234", 2048);
			MockNM nm3 = rm1.RegisterNode("h3:1234", 2048);
			MockNM nm4 = rm1.RegisterNode("h4:1234", 2048);
			MockNM nm5 = rm1.RegisterNode("h5:1234", 2048);
			ContainerId containerId;
			// launch an app to queue a1 (label = x), and check all container will
			// be allocated in h1
			RMApp app1 = rm1.SubmitApp(1024, "app", "user", null, "a1");
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			// request a container (label = y). can be allocated on nm2 
			am1.Allocate("*", 1024, 1, new AList<ContainerId>(), "y");
			containerId = ContainerId.NewContainerId(am1.GetApplicationAttemptId(), 2L);
			NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm2, containerId, RMContainerState
				.Allocated, 10 * 1000));
			CheckTaskContainersHost(am1.GetApplicationAttemptId(), containerId, rm1, "h2");
			// launch an app to queue b1 (label = y), and check all container will
			// be allocated in h5
			RMApp app2 = rm1.SubmitApp(1024, "app", "user", null, "b1");
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm1, nm5);
			// request a container for AM, will succeed
			// and now b1's queue capacity will be used, cannot allocate more containers
			// (Maximum capacity reached)
			am2.Allocate("*", 1024, 1, new AList<ContainerId>());
			containerId = ContainerId.NewContainerId(am2.GetApplicationAttemptId(), 2);
			NUnit.Framework.Assert.IsFalse(rm1.WaitForState(nm4, containerId, RMContainerState
				.Allocated, 10 * 1000));
			NUnit.Framework.Assert.IsFalse(rm1.WaitForState(nm5, containerId, RMContainerState
				.Allocated, 10 * 1000));
			// launch an app to queue b2
			RMApp app3 = rm1.SubmitApp(1024, "app", "user", null, "b2");
			MockAM am3 = MockRM.LaunchAndRegisterAM(app3, rm1, nm5);
			// request a container. try to allocate on nm1 (label = x) and nm3 (label =
			// y,z). Will successfully allocate on nm3
			am3.Allocate("*", 1024, 1, new AList<ContainerId>(), "y");
			containerId = ContainerId.NewContainerId(am3.GetApplicationAttemptId(), 2);
			NUnit.Framework.Assert.IsFalse(rm1.WaitForState(nm1, containerId, RMContainerState
				.Allocated, 10 * 1000));
			NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm3, containerId, RMContainerState
				.Allocated, 10 * 1000));
			CheckTaskContainersHost(am3.GetApplicationAttemptId(), containerId, rm1, "h3");
			// try to allocate container (request label = z) on nm4 (label = y,z). 
			// Will successfully allocate on nm4 only.
			am3.Allocate("*", 1024, 1, new AList<ContainerId>(), "z");
			containerId = ContainerId.NewContainerId(am3.GetApplicationAttemptId(), 3L);
			NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm4, containerId, RMContainerState
				.Allocated, 10 * 1000));
			CheckTaskContainersHost(am3.GetApplicationAttemptId(), containerId, rm1, "h4");
			rm1.Close();
		}

		private sealed class _MockRM_557 : MockRM
		{
			public _MockRM_557(TestContainerAllocation _enclosing, Configuration baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
			}

			protected internal override RMNodeLabelsManager CreateNodeLabelManager()
			{
				return this._enclosing.mgr;
			}

			private readonly TestContainerAllocation _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerAllocateWithLabels()
		{
			// set node -> label
			mgr.AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
			mgr.AddLabelsToNode(ImmutableMap.Of(NodeId.NewInstance("h1", 0), ToSet("x"), NodeId
				.NewInstance("h2", 0), ToSet("y")));
			// inject node label manager
			MockRM rm1 = new _MockRM_638(this, GetConfigurationWithQueueLabels(conf));
			rm1.GetRMContext().SetNodeLabelManager(mgr);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("h1:1234", 8000);
			// label = x
			MockNM nm2 = rm1.RegisterNode("h2:1234", 8000);
			// label = y
			MockNM nm3 = rm1.RegisterNode("h3:1234", 8000);
			// label = <empty>
			ContainerId containerId;
			// launch an app to queue a1 (label = x), and check all container will
			// be allocated in h1
			RMApp app1 = rm1.SubmitApp(200, "app", "user", null, "a1");
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm3);
			// request a container.
			am1.Allocate("*", 1024, 1, new AList<ContainerId>(), "x");
			containerId = ContainerId.NewContainerId(am1.GetApplicationAttemptId(), 2);
			NUnit.Framework.Assert.IsFalse(rm1.WaitForState(nm2, containerId, RMContainerState
				.Allocated, 10 * 1000));
			NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm1, containerId, RMContainerState
				.Allocated, 10 * 1000));
			CheckTaskContainersHost(am1.GetApplicationAttemptId(), containerId, rm1, "h1");
			// launch an app to queue b1 (label = y), and check all container will
			// be allocated in h2
			RMApp app2 = rm1.SubmitApp(200, "app", "user", null, "b1");
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm1, nm3);
			// request a container.
			am2.Allocate("*", 1024, 1, new AList<ContainerId>(), "y");
			containerId = ContainerId.NewContainerId(am2.GetApplicationAttemptId(), 2);
			NUnit.Framework.Assert.IsFalse(rm1.WaitForState(nm1, containerId, RMContainerState
				.Allocated, 10 * 1000));
			NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm2, containerId, RMContainerState
				.Allocated, 10 * 1000));
			CheckTaskContainersHost(am2.GetApplicationAttemptId(), containerId, rm1, "h2");
			// launch an app to queue c1 (label = ""), and check all container will
			// be allocated in h3
			RMApp app3 = rm1.SubmitApp(200, "app", "user", null, "c1");
			MockAM am3 = MockRM.LaunchAndRegisterAM(app3, rm1, nm3);
			// request a container.
			am3.Allocate("*", 1024, 1, new AList<ContainerId>());
			containerId = ContainerId.NewContainerId(am3.GetApplicationAttemptId(), 2);
			NUnit.Framework.Assert.IsFalse(rm1.WaitForState(nm2, containerId, RMContainerState
				.Allocated, 10 * 1000));
			NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm3, containerId, RMContainerState
				.Allocated, 10 * 1000));
			CheckTaskContainersHost(am3.GetApplicationAttemptId(), containerId, rm1, "h3");
			rm1.Close();
		}

		private sealed class _MockRM_638 : MockRM
		{
			public _MockRM_638(TestContainerAllocation _enclosing, Configuration baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
			}

			protected internal override RMNodeLabelsManager CreateNodeLabelManager()
			{
				return this._enclosing.mgr;
			}

			private readonly TestContainerAllocation _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerAllocateWithDefaultQueueLabels()
		{
			// This test is pretty much similar to testContainerAllocateWithLabel.
			// Difference is, this test doesn't specify label expression in ResourceRequest,
			// instead, it uses default queue label expression
			// set node -> label
			mgr.AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
			mgr.AddLabelsToNode(ImmutableMap.Of(NodeId.NewInstance("h1", 0), ToSet("x"), NodeId
				.NewInstance("h2", 0), ToSet("y")));
			// inject node label manager
			MockRM rm1 = new _MockRM_714(this, GetConfigurationWithDefaultQueueLabels(conf));
			rm1.GetRMContext().SetNodeLabelManager(mgr);
			rm1.Start();
			MockNM nm1 = rm1.RegisterNode("h1:1234", 8000);
			// label = x
			MockNM nm2 = rm1.RegisterNode("h2:1234", 8000);
			// label = y
			MockNM nm3 = rm1.RegisterNode("h3:1234", 8000);
			// label = <empty>
			ContainerId containerId;
			// launch an app to queue a1 (label = x), and check all container will
			// be allocated in h1
			RMApp app1 = rm1.SubmitApp(200, "app", "user", null, "a1");
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
			// request a container.
			am1.Allocate("*", 1024, 1, new AList<ContainerId>());
			containerId = ContainerId.NewContainerId(am1.GetApplicationAttemptId(), 2);
			NUnit.Framework.Assert.IsFalse(rm1.WaitForState(nm3, containerId, RMContainerState
				.Allocated, 10 * 1000));
			NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm1, containerId, RMContainerState
				.Allocated, 10 * 1000));
			CheckTaskContainersHost(am1.GetApplicationAttemptId(), containerId, rm1, "h1");
			// launch an app to queue b1 (label = y), and check all container will
			// be allocated in h2
			RMApp app2 = rm1.SubmitApp(200, "app", "user", null, "b1");
			MockAM am2 = MockRM.LaunchAndRegisterAM(app2, rm1, nm2);
			// request a container.
			am2.Allocate("*", 1024, 1, new AList<ContainerId>());
			containerId = ContainerId.NewContainerId(am2.GetApplicationAttemptId(), 2);
			NUnit.Framework.Assert.IsFalse(rm1.WaitForState(nm3, containerId, RMContainerState
				.Allocated, 10 * 1000));
			NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm2, containerId, RMContainerState
				.Allocated, 10 * 1000));
			CheckTaskContainersHost(am2.GetApplicationAttemptId(), containerId, rm1, "h2");
			// launch an app to queue c1 (label = ""), and check all container will
			// be allocated in h3
			RMApp app3 = rm1.SubmitApp(200, "app", "user", null, "c1");
			MockAM am3 = MockRM.LaunchAndRegisterAM(app3, rm1, nm3);
			// request a container.
			am3.Allocate("*", 1024, 1, new AList<ContainerId>());
			containerId = ContainerId.NewContainerId(am3.GetApplicationAttemptId(), 2);
			NUnit.Framework.Assert.IsFalse(rm1.WaitForState(nm2, containerId, RMContainerState
				.Allocated, 10 * 1000));
			NUnit.Framework.Assert.IsTrue(rm1.WaitForState(nm3, containerId, RMContainerState
				.Allocated, 10 * 1000));
			CheckTaskContainersHost(am3.GetApplicationAttemptId(), containerId, rm1, "h3");
			rm1.Close();
		}

		private sealed class _MockRM_714 : MockRM
		{
			public _MockRM_714(TestContainerAllocation _enclosing, Configuration baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
			}

			protected internal override RMNodeLabelsManager CreateNodeLabelManager()
			{
				return this._enclosing.mgr;
			}

			private readonly TestContainerAllocation _enclosing;
		}
	}
}
