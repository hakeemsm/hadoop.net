using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestLeafQueue
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestLeafQueue));

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal RMContext rmContext;

		internal RMContext spyRMContext;

		internal ResourceRequest amResourceRequest;

		internal CapacityScheduler cs;

		internal CapacitySchedulerConfiguration csConf;

		internal CapacitySchedulerContext csContext;

		internal CSQueue root;

		internal IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();

		internal const int Gb = 1024;

		internal const string DefaultRack = "/default";

		private readonly ResourceCalculator resourceCalculator = new DefaultResourceCalculator
			();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			CapacityScheduler spyCs = new CapacityScheduler();
			cs = Org.Mockito.Mockito.Spy(spyCs);
			rmContext = TestUtils.GetMockRMContext();
			spyRMContext = Org.Mockito.Mockito.Spy(rmContext);
			ConcurrentMap<ApplicationId, RMApp> spyApps = Org.Mockito.Mockito.Spy(new ConcurrentHashMap
				<ApplicationId, RMApp>());
			RMApp rmApp = Org.Mockito.Mockito.Mock<RMApp>();
			Org.Mockito.Mockito.When(rmApp.GetRMAppAttempt((ApplicationAttemptId)Matchers.Any
				())).ThenReturn(null);
			amResourceRequest = Org.Mockito.Mockito.Mock<ResourceRequest>();
			Org.Mockito.Mockito.When(amResourceRequest.GetCapability()).ThenReturn(Resources.
				CreateResource(0, 0));
			Org.Mockito.Mockito.When(rmApp.GetAMResourceRequest()).ThenReturn(amResourceRequest
				);
			Org.Mockito.Mockito.DoReturn(rmApp).When(spyApps)[(ApplicationId)Matchers.Any()];
			Org.Mockito.Mockito.When(spyRMContext.GetRMApps()).ThenReturn(spyApps);
			csConf = new CapacitySchedulerConfiguration();
			csConf.SetBoolean("yarn.scheduler.capacity.user-metrics.enable", true);
			string newRoot = "root" + Runtime.CurrentTimeMillis();
			SetupQueueConfiguration(csConf, newRoot);
			YarnConfiguration conf = new YarnConfiguration();
			cs.SetConf(conf);
			csContext = Org.Mockito.Mockito.Mock<CapacitySchedulerContext>();
			Org.Mockito.Mockito.When(csContext.GetConfiguration()).ThenReturn(csConf);
			Org.Mockito.Mockito.When(csContext.GetConf()).ThenReturn(conf);
			Org.Mockito.Mockito.When(csContext.GetMinimumResourceCapability()).ThenReturn(Resources
				.CreateResource(Gb, 1));
			Org.Mockito.Mockito.When(csContext.GetMaximumResourceCapability()).ThenReturn(Resources
				.CreateResource(16 * Gb, 32));
			Org.Mockito.Mockito.When(csContext.GetClusterResource()).ThenReturn(Resources.CreateResource
				(100 * 16 * Gb, 100 * 32));
			Org.Mockito.Mockito.When(csContext.GetApplicationComparator()).ThenReturn(CapacityScheduler
				.applicationComparator);
			Org.Mockito.Mockito.When(csContext.GetQueueComparator()).ThenReturn(CapacityScheduler
				.queueComparator);
			Org.Mockito.Mockito.When(csContext.GetResourceCalculator()).ThenReturn(resourceCalculator
				);
			Org.Mockito.Mockito.When(csContext.GetRMContext()).ThenReturn(rmContext);
			RMContainerTokenSecretManager containerTokenSecretManager = new RMContainerTokenSecretManager
				(conf);
			containerTokenSecretManager.RollMasterKey();
			Org.Mockito.Mockito.When(csContext.GetContainerTokenSecretManager()).ThenReturn(containerTokenSecretManager
				);
			root = CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, queues, queues, TestUtils.spyHook);
			cs.SetRMContext(spyRMContext);
			cs.Init(csConf);
			cs.Start();
			Org.Mockito.Mockito.When(spyRMContext.GetScheduler()).ThenReturn(cs);
			Org.Mockito.Mockito.When(spyRMContext.GetYarnConfiguration()).ThenReturn(new YarnConfiguration
				());
			Org.Mockito.Mockito.When(cs.GetNumClusterNodes()).ThenReturn(3);
		}

		private const string A = "a";

		private const string B = "b";

		private const string C = "c";

		private const string C1 = "c1";

		private const string D = "d";

		private const string E = "e";

		private void SetupQueueConfiguration(CapacitySchedulerConfiguration conf, string 
			newRoot)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { newRoot });
			conf.SetMaximumCapacity(CapacitySchedulerConfiguration.Root, 100);
			conf.SetAcl(CapacitySchedulerConfiguration.Root, QueueACL.SubmitApplications, " "
				);
			string Q_newRoot = CapacitySchedulerConfiguration.Root + "." + newRoot;
			conf.SetQueues(Q_newRoot, new string[] { A, B, C, D, E });
			conf.SetCapacity(Q_newRoot, 100);
			conf.SetMaximumCapacity(Q_newRoot, 100);
			conf.SetAcl(Q_newRoot, QueueACL.SubmitApplications, " ");
			string QA = Q_newRoot + "." + A;
			conf.SetCapacity(QA, 8.5f);
			conf.SetMaximumCapacity(QA, 20);
			conf.SetAcl(QA, QueueACL.SubmitApplications, "*");
			string QB = Q_newRoot + "." + B;
			conf.SetCapacity(QB, 80);
			conf.SetMaximumCapacity(QB, 99);
			conf.SetAcl(QB, QueueACL.SubmitApplications, "*");
			string QC = Q_newRoot + "." + C;
			conf.SetCapacity(QC, 1.5f);
			conf.SetMaximumCapacity(QC, 10);
			conf.SetAcl(QC, QueueACL.SubmitApplications, " ");
			conf.SetQueues(QC, new string[] { C1 });
			string QC1 = QC + "." + C1;
			conf.SetCapacity(QC1, 100);
			string QD = Q_newRoot + "." + D;
			conf.SetCapacity(QD, 9);
			conf.SetMaximumCapacity(QD, 11);
			conf.SetAcl(QD, QueueACL.SubmitApplications, "user_d");
			string QE = Q_newRoot + "." + E;
			conf.SetCapacity(QE, 1);
			conf.SetMaximumCapacity(QE, 1);
			conf.SetAcl(QE, QueueACL.SubmitApplications, "user_e");
		}

		internal static LeafQueue StubLeafQueue(LeafQueue queue)
		{
			// Mock some methods for ease in these unit tests
			// 1. LeafQueue.createContainer to return dummy containers
			Org.Mockito.Mockito.DoAnswer(new _Answer_242()).When(queue).CreateContainer(Matchers.Any
				<FiCaSchedulerApp>(), Matchers.Any<FiCaSchedulerNode>(), Matchers.Any<Org.Apache.Hadoop.Yarn.Api.Records.Resource
				>(), Matchers.Any<Priority>());
			// 2. Stub out LeafQueue.parent.completedContainer
			CSQueue parent = queue.GetParent();
			Org.Mockito.Mockito.DoNothing().When(parent).CompletedContainer(Matchers.Any<Org.Apache.Hadoop.Yarn.Api.Records.Resource
				>(), Matchers.Any<FiCaSchedulerApp>(), Matchers.Any<FiCaSchedulerNode>(), Matchers.Any
				<RMContainer>(), Matchers.Any<ContainerStatus>(), Matchers.Any<RMContainerEventType
				>(), Matchers.Any<CSQueue>(), Matchers.AnyBoolean());
			return queue;
		}

		private sealed class _Answer_242 : Answer<Container>
		{
			public _Answer_242()
			{
			}

			/// <exception cref="System.Exception"/>
			public Container Answer(InvocationOnMock invocation)
			{
				FiCaSchedulerApp application = (FiCaSchedulerApp)(invocation.GetArguments()[0]);
				ContainerId containerId = TestUtils.GetMockContainerId(application);
				Container container = TestUtils.GetMockContainer(containerId, ((FiCaSchedulerNode
					)(invocation.GetArguments()[1])).GetNodeID(), (Org.Apache.Hadoop.Yarn.Api.Records.Resource
					)(invocation.GetArguments()[2]), ((Priority)invocation.GetArguments()[3]));
				return container;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitializeQueue()
		{
			float epsilon = 1e-5f;
			//can add more sturdy test with 3-layer queues 
			//once MAPREDUCE:3410 is resolved
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			NUnit.Framework.Assert.AreEqual(0.085, a.GetCapacity(), epsilon);
			NUnit.Framework.Assert.AreEqual(0.085, a.GetAbsoluteCapacity(), epsilon);
			NUnit.Framework.Assert.AreEqual(0.2, a.GetMaximumCapacity(), epsilon);
			NUnit.Framework.Assert.AreEqual(0.2, a.GetAbsoluteMaximumCapacity(), epsilon);
			LeafQueue b = StubLeafQueue((LeafQueue)queues[B]);
			NUnit.Framework.Assert.AreEqual(0.80, b.GetCapacity(), epsilon);
			NUnit.Framework.Assert.AreEqual(0.80, b.GetAbsoluteCapacity(), epsilon);
			NUnit.Framework.Assert.AreEqual(0.99, b.GetMaximumCapacity(), epsilon);
			NUnit.Framework.Assert.AreEqual(0.99, b.GetAbsoluteMaximumCapacity(), epsilon);
			ParentQueue c = (ParentQueue)queues[C];
			NUnit.Framework.Assert.AreEqual(0.015, c.GetCapacity(), epsilon);
			NUnit.Framework.Assert.AreEqual(0.015, c.GetAbsoluteCapacity(), epsilon);
			NUnit.Framework.Assert.AreEqual(0.1, c.GetMaximumCapacity(), epsilon);
			NUnit.Framework.Assert.AreEqual(0.1, c.GetAbsoluteMaximumCapacity(), epsilon);
			//Verify the value for getAMResourceLimit for queues with < .1 maxcap
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(50 * Gb, 50);
			a.UpdateClusterResource(clusterResource, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(1 * Gb, 1), a.GetAMResourceLimit());
			b.UpdateClusterResource(clusterResource, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(5 * Gb, 1), b.GetAMResourceLimit());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleQueueOneUserMetrics()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[B]);
			// Users
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// same user
			// Setup some nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			int numNodes = 1;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), numNodes * 16);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 3, true, priority, recordFactory)));
			// Start testing...
			// Only 1 container
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual((int)(node_0.GetTotalResource().GetMemory() * a.GetCapacity
				()) - (1 * Gb), a.GetMetrics().GetAvailableMB());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUserQueueAcl()
		{
			// Manipulate queue 'a'
			LeafQueue d = StubLeafQueue((LeafQueue)queues[D]);
			// Users
			string user_d = "user_d";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 1);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_d, d, null, spyRMContext
				);
			d.SubmitApplicationAttempt(app_0, user_d);
			// Attempt the same application again
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(0, 2);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_d, d, null, spyRMContext
				);
			d.SubmitApplicationAttempt(app_1, user_d);
		}

		// same user
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAttemptMetrics()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[B]);
			// Users
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 1);
			AppAddedSchedulerEvent addAppEvent = new AppAddedSchedulerEvent(appAttemptId_0.GetApplicationId
				(), a.GetQueueName(), user_0);
			cs.Handle(addAppEvent);
			AppAttemptAddedSchedulerEvent addAttemptEvent = new AppAttemptAddedSchedulerEvent
				(appAttemptId_0, false);
			cs.Handle(addAttemptEvent);
			AppAttemptRemovedSchedulerEvent @event = new AppAttemptRemovedSchedulerEvent(appAttemptId_0
				, RMAppAttemptState.Failed, false);
			cs.Handle(@event);
			NUnit.Framework.Assert.AreEqual(0, a.GetMetrics().GetAppsPending());
			NUnit.Framework.Assert.AreEqual(0, a.GetMetrics().GetAppsFailed());
			// Attempt the same application again
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(0, 2);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, null, spyRMContext
				);
			a.SubmitApplicationAttempt(app_1, user_0);
			// same user
			NUnit.Framework.Assert.AreEqual(1, a.GetMetrics().GetAppsSubmitted());
			NUnit.Framework.Assert.AreEqual(1, a.GetMetrics().GetAppsPending());
			@event = new AppAttemptRemovedSchedulerEvent(appAttemptId_0, RMAppAttemptState.Finished
				, false);
			cs.Handle(@event);
			AppRemovedSchedulerEvent rEvent = new AppRemovedSchedulerEvent(appAttemptId_0.GetApplicationId
				(), RMAppState.Finished);
			cs.Handle(rEvent);
			NUnit.Framework.Assert.AreEqual(1, a.GetMetrics().GetAppsSubmitted());
			NUnit.Framework.Assert.AreEqual(0, a.GetMetrics().GetAppsPending());
			NUnit.Framework.Assert.AreEqual(0, a.GetMetrics().GetAppsFailed());
			NUnit.Framework.Assert.AreEqual(1, a.GetMetrics().GetAppsCompleted());
			QueueMetrics userMetrics = a.GetMetrics().GetUserMetrics(user_0);
			NUnit.Framework.Assert.AreEqual(1, userMetrics.GetAppsSubmitted());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleQueueWithOneUser()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			//unset maxCapacity
			a.SetMaxCapacity(1.0f);
			// Users
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// same user
			// Setup some nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			int numNodes = 1;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), numNodes * 16);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 3, true, priority, recordFactory)));
			app_1.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 2, true, priority, recordFactory)));
			// Start testing...
			// Only 1 container
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(1 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(1 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetAvailableMB());
			// Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
			// you can get one container more than user-limit
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			// Can't allocate 3rd due to user-limit
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			// Bump up user-limit-factor, now allocate should work
			a.SetUserLimitFactor(10);
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetMetrics().GetAllocatedMB());
			// One more should work, for app_1, due to user-limit-factor
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetMetrics().GetAllocatedMB());
			// Test max-capacity
			// Now - no more allocs since we are at max-cap
			a.SetMaxCapacity(0.5f);
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetMetrics().GetAllocatedMB());
			// Release each container from app_0
			foreach (RMContainer rmContainer in app_0.GetLiveContainers())
			{
				a.CompletedContainer(clusterResource, app_0, node_0, rmContainer, ContainerStatus
					.NewInstance(rmContainer.GetContainerId(), ContainerState.Complete, string.Empty
					, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
					true);
			}
			NUnit.Framework.Assert.AreEqual(1 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(1 * Gb, a.GetMetrics().GetAllocatedMB());
			// Release each container from app_1
			foreach (RMContainer rmContainer_1 in app_1.GetLiveContainers())
			{
				a.CompletedContainer(clusterResource, app_1, node_0, rmContainer_1, ContainerStatus
					.NewInstance(rmContainer_1.GetContainerId(), ContainerState.Complete, string.Empty
					, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
					true);
			}
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual((int)(a.GetCapacity() * node_0.GetTotalResource()
				.GetMemory()), a.GetMetrics().GetAvailableMB());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUserLimits()
		{
			// Mock the queue
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			//unset maxCapacity
			a.SetMaxCapacity(1.0f);
			// Users
			string user_0 = "user_0";
			string user_1 = "user_1";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// same user
			ApplicationAttemptId appAttemptId_2 = TestUtils.GetMockApplicationAttemptId(2, 0);
			FiCaSchedulerApp app_2 = new FiCaSchedulerApp(appAttemptId_2, user_1, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_2, user_1);
			// Setup some nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			string host_1 = "127.0.0.2";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			int numNodes = 2;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), numNodes * 16);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, priority, recordFactory)));
			app_1.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 2, true, priority, recordFactory)));
			// Set user-limit
			a.SetUserLimit(50);
			a.SetUserLimitFactor(2);
			// Now, only user_0 should be active since he is the only one with
			// outstanding requests
			NUnit.Framework.Assert.AreEqual("There should only be 1 active user!", 1, a.GetActiveUsersManager
				().GetNumActiveUsers());
			// This commented code is key to test 'activeUsers'. 
			// It should fail the test if uncommented since
			// it would increase 'activeUsers' to 2 and stop user_2
			// Pre MAPREDUCE-3732 this test should fail without this block too
			//    app_2.updateResourceRequests(Collections.singletonList(
			//        TestUtils.createResourceRequest(RMNodeImpl.ANY, 1*GB, 1, priority,
			//            recordFactory)));
			// 1 container to user_0
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			// Again one to user_0 since he hasn't exceeded user limit yet
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			// One more to user_0 since he is the only active user
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestComputeUserLimitAndSetHeadroom()
		{
			LeafQueue qb = StubLeafQueue((LeafQueue)queues[B]);
			qb.SetMaxCapacity(1.0f);
			// Users
			string user_0 = "user_0";
			string user_1 = "user_1";
			//create nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			string host_1 = "127.0.0.2";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			int numNodes = 2;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), 1);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			//our test plan contains three cases
			//1. single user dominate the queue, we test the headroom
			//2. two users, but user_0 is assigned 100% of the queue resource,
			//   submit user_1's application, check headroom correctness
			//3. two users, each is assigned 50% of the queue resource
			//   each user submit one application and check their headrooms
			//4. similarly to 3. but user_0 has no quote left and there are
			//   free resources left, check headroom
			//test case 1
			qb.SetUserLimit(100);
			qb.SetUserLimitFactor(1);
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, qb, qb.GetActiveUsersManager
				(), spyRMContext);
			qb.SubmitApplicationAttempt(app_0, user_0);
			Priority u0Priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 4 * Gb, 1, true, u0Priority, recordFactory)));
			NUnit.Framework.Assert.AreEqual("There should only be 1 active user!", 1, qb.GetActiveUsersManager
				().GetNumActiveUsers());
			//get headroom
			qb.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			qb.ComputeUserLimitAndSetHeadroom(app_0, clusterResource, app_0.GetResourceRequest
				(u0Priority, ResourceRequest.Any).GetCapability(), null);
			//maxqueue 16G, userlimit 13G, - 4G used = 9G
			NUnit.Framework.Assert.AreEqual(9 * Gb, app_0.GetHeadroom().GetMemory());
			//test case 2
			ApplicationAttemptId appAttemptId_2 = TestUtils.GetMockApplicationAttemptId(2, 0);
			FiCaSchedulerApp app_2 = new FiCaSchedulerApp(appAttemptId_2, user_1, qb, qb.GetActiveUsersManager
				(), spyRMContext);
			Priority u1Priority = TestUtils.CreateMockPriority(2);
			app_2.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 4 * Gb, 1, true, u1Priority, recordFactory)));
			qb.SubmitApplicationAttempt(app_2, user_1);
			qb.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			qb.ComputeUserLimitAndSetHeadroom(app_0, clusterResource, app_0.GetResourceRequest
				(u0Priority, ResourceRequest.Any).GetCapability(), null);
			NUnit.Framework.Assert.AreEqual(8 * Gb, qb.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			//maxqueue 16G, userlimit 13G, - 4G used = 9G BUT
			//maxqueue 16G - used 8G (4 each app/user) = 8G max headroom (the new logic)
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_2.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_2.GetHeadroom().GetMemory());
			//test case 3
			qb.FinishApplication(app_0.GetApplicationId(), user_0);
			qb.FinishApplication(app_2.GetApplicationId(), user_1);
			qb.ReleaseResource(clusterResource, app_0, app_0.GetResource(u0Priority), null);
			qb.ReleaseResource(clusterResource, app_2, app_2.GetResource(u1Priority), null);
			qb.SetUserLimit(50);
			qb.SetUserLimitFactor(1);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, qb, qb.GetActiveUsersManager
				(), spyRMContext);
			ApplicationAttemptId appAttemptId_3 = TestUtils.GetMockApplicationAttemptId(3, 0);
			FiCaSchedulerApp app_3 = new FiCaSchedulerApp(appAttemptId_3, user_1, qb, qb.GetActiveUsersManager
				(), spyRMContext);
			app_1.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, u0Priority, recordFactory)));
			app_3.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, u1Priority, recordFactory)));
			qb.SubmitApplicationAttempt(app_1, user_0);
			qb.SubmitApplicationAttempt(app_3, user_1);
			qb.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			qb.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			qb.ComputeUserLimitAndSetHeadroom(app_3, clusterResource, app_3.GetResourceRequest
				(u1Priority, ResourceRequest.Any).GetCapability(), null);
			NUnit.Framework.Assert.AreEqual(4 * Gb, qb.GetUsedResources().GetMemory());
			//maxqueue 16G, userlimit 7G, used (by each user) 2G, headroom 5G (both)
			NUnit.Framework.Assert.AreEqual(5 * Gb, app_3.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, app_1.GetHeadroom().GetMemory());
			//test case 4
			ApplicationAttemptId appAttemptId_4 = TestUtils.GetMockApplicationAttemptId(4, 0);
			FiCaSchedulerApp app_4 = new FiCaSchedulerApp(appAttemptId_4, user_0, qb, qb.GetActiveUsersManager
				(), spyRMContext);
			qb.SubmitApplicationAttempt(app_4, user_0);
			app_4.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 6 * Gb, 1, true, u0Priority, recordFactory)));
			qb.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			qb.ComputeUserLimitAndSetHeadroom(app_4, clusterResource, app_4.GetResourceRequest
				(u0Priority, ResourceRequest.Any).GetCapability(), null);
			qb.ComputeUserLimitAndSetHeadroom(app_3, clusterResource, app_3.GetResourceRequest
				(u1Priority, ResourceRequest.Any).GetCapability(), null);
			//app3 is user1, active from last test case
			//maxqueue 16G, userlimit 13G, used 2G, would be headroom 10G BUT
			//10G in use, so max possible headroom is 6G (new logic)
			NUnit.Framework.Assert.AreEqual(6 * Gb, app_3.GetHeadroom().GetMemory());
			//testcase3 still active - 2+2+6=10
			NUnit.Framework.Assert.AreEqual(10 * Gb, qb.GetUsedResources().GetMemory());
			//app4 is user 0
			//maxqueue 16G, userlimit 13G, used 8G, headroom 5G
			//(8G used is 6G from this test case - app4, 2 from last test case, app_1)
			NUnit.Framework.Assert.AreEqual(5 * Gb, app_4.GetHeadroom().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUserHeadroomMultiApp()
		{
			// Mock the queue
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			//unset maxCapacity
			a.SetMaxCapacity(1.0f);
			// Users
			string user_0 = "user_0";
			string user_1 = "user_1";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// same user
			ApplicationAttemptId appAttemptId_2 = TestUtils.GetMockApplicationAttemptId(2, 0);
			FiCaSchedulerApp app_2 = new FiCaSchedulerApp(appAttemptId_2, user_1, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_2, user_1);
			// Setup some nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 16 * Gb);
			string host_1 = "127.0.0.2";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 16 * Gb);
			int numNodes = 2;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (16 * Gb), 1);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			Priority priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 1, true, priority, recordFactory)));
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(1 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			//Now, headroom is the same for all apps for a given user + queue combo
			//and a change to any app's headroom is reflected for all the user's apps
			//once those apps are active/have themselves calculated headroom for 
			//allocation at least one time
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetHeadroom().GetMemory());
			//not yet active
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_2.GetHeadroom().GetMemory());
			//not yet active
			app_1.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 2, true, priority, recordFactory)));
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_1.GetHeadroom().GetMemory());
			//now active
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_2.GetHeadroom().GetMemory());
			//not yet active
			//Complete container and verify that headroom is updated, for both apps 
			//for the user
			RMContainer rmContainer = app_0.GetLiveContainers().GetEnumerator().Next();
			a.CompletedContainer(clusterResource, app_0, node_0, rmContainer, ContainerStatus
				.NewInstance(rmContainer.GetContainerId(), ContainerState.Complete, string.Empty
				, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
				true);
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_1.GetHeadroom().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHeadroomWithMaxCap()
		{
			// Mock the queue
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			//unset maxCapacity
			a.SetMaxCapacity(1.0f);
			// Users
			string user_0 = "user_0";
			string user_1 = "user_1";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// same user
			ApplicationAttemptId appAttemptId_2 = TestUtils.GetMockApplicationAttemptId(2, 0);
			FiCaSchedulerApp app_2 = new FiCaSchedulerApp(appAttemptId_2, user_1, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_2, user_1);
			// Setup some nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			string host_1 = "127.0.0.2";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			int numNodes = 2;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), 1);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, priority, recordFactory)));
			app_1.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 2, true, priority, recordFactory)));
			// Set user-limit
			a.SetUserLimit(50);
			a.SetUserLimitFactor(2);
			// Now, only user_0 should be active since he is the only one with
			// outstanding requests
			NUnit.Framework.Assert.AreEqual("There should only be 1 active user!", 1, a.GetActiveUsersManager
				().GetNumActiveUsers());
			// 1 container to user_0
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetHeadroom().GetMemory());
			// User limit = 4G, 2 in use
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetHeadroom().GetMemory());
			// the application is not yet active
			// Again one to user_0 since he hasn't exceeded user limit yet
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetHeadroom().GetMemory());
			// 4G - 3G
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_1.GetHeadroom().GetMemory());
			// 4G - 3G
			// Submit requests for app_1 and set max-cap
			a.SetMaxCapacity(.1f);
			app_2.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 1, true, priority, recordFactory)));
			NUnit.Framework.Assert.AreEqual(2, a.GetActiveUsersManager().GetNumActiveUsers());
			// No more to user_0 since he is already over user-limit
			// and no more containers to queue since it's already at max-cap
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_2.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetHeadroom().GetMemory());
			// Check headroom for app_2 
			app_1.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 0, true, priority, recordFactory)));
			// unset
			NUnit.Framework.Assert.AreEqual(1, a.GetActiveUsersManager().GetNumActiveUsers());
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_2.GetHeadroom().GetMemory());
		}

		// hit queue max-cap 
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleQueueWithMultipleUsers()
		{
			// Mock the queue
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			//unset maxCapacity
			a.SetMaxCapacity(1.0f);
			// Users
			string user_0 = "user_0";
			string user_1 = "user_1";
			string user_2 = "user_2";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// same user
			ApplicationAttemptId appAttemptId_2 = TestUtils.GetMockApplicationAttemptId(2, 0);
			FiCaSchedulerApp app_2 = new FiCaSchedulerApp(appAttemptId_2, user_1, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_2, user_1);
			ApplicationAttemptId appAttemptId_3 = TestUtils.GetMockApplicationAttemptId(3, 0);
			FiCaSchedulerApp app_3 = new FiCaSchedulerApp(appAttemptId_3, user_2, a, a.GetActiveUsersManager
				(), spyRMContext);
			a.SubmitApplicationAttempt(app_3, user_2);
			// Setup some nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			int numNodes = 1;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), numNodes * 16);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 10, true, priority, recordFactory)));
			app_1.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 10, true, priority, recordFactory)));
			// Only 1 container
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(1 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			// Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
			// you can get one container more than user-limit
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			// Can't allocate 3rd due to user-limit
			a.SetUserLimit(25);
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			// Submit resource requests for other apps now to 'activate' them
			app_2.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 3 * Gb, 1, true, priority, recordFactory)));
			app_3.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 2, true, priority, recordFactory)));
			// Now allocations should goto app_2 since 
			// user_0 is at limit inspite of high user-limit-factor
			a.SetUserLimitFactor(10);
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_2.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_3.GetCurrentConsumption().GetMemory()
				);
			// Now allocations should goto app_0 since 
			// user_0 is at user-limit not above it
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(6 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_2.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_3.GetCurrentConsumption().GetMemory()
				);
			// Test max-capacity
			// Now - no more allocs since we are at max-cap
			a.SetMaxCapacity(0.5f);
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(6 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_2.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_3.GetCurrentConsumption().GetMemory()
				);
			// Revert max-capacity and user-limit-factor
			// Now, allocations should goto app_3 since it's under user-limit 
			a.SetMaxCapacity(1.0f);
			a.SetUserLimitFactor(1);
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(7 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_2.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_3.GetCurrentConsumption().GetMemory()
				);
			// Now we should assign to app_3 again since user_2 is under user-limit
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_2.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_3.GetCurrentConsumption().GetMemory()
				);
			// 8. Release each container from app_0
			foreach (RMContainer rmContainer in app_0.GetLiveContainers())
			{
				a.CompletedContainer(clusterResource, app_0, node_0, rmContainer, ContainerStatus
					.NewInstance(rmContainer.GetContainerId(), ContainerState.Complete, string.Empty
					, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
					true);
			}
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_2.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_3.GetCurrentConsumption().GetMemory()
				);
			// 9. Release each container from app_2
			foreach (RMContainer rmContainer_1 in app_2.GetLiveContainers())
			{
				a.CompletedContainer(clusterResource, app_2, node_0, rmContainer_1, ContainerStatus
					.NewInstance(rmContainer_1.GetContainerId(), ContainerState.Complete, string.Empty
					, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
					true);
			}
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_2.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_3.GetCurrentConsumption().GetMemory()
				);
			// 10. Release each container from app_3
			foreach (RMContainer rmContainer_2 in app_3.GetLiveContainers())
			{
				a.CompletedContainer(clusterResource, app_3, node_0, rmContainer_2, ContainerStatus
					.NewInstance(rmContainer_2.GetContainerId(), ContainerState.Complete, string.Empty
					, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
					true);
			}
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_2.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_3.GetCurrentConsumption().GetMemory()
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservation()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			//unset maxCapacity
			a.SetMaxCapacity(1.0f);
			// Users
			string user_0 = "user_0";
			string user_1 = "user_1";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_1, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_1);
			// Setup some nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 4 * Gb);
			int numNodes = 2;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (4 * Gb), numNodes * 16);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 2, true, priority, recordFactory)));
			app_1.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 4 * Gb, 1, true, priority, recordFactory)));
			// Start testing...
			// Only 1 container
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(1 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(1 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetAvailableMB());
			// Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
			// you can get one container more than user-limit
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			// Now, reservation should kick in for app_1
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(6 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(2 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			// Now free 1 container from app_0 i.e. 1G
			RMContainer rmContainer = app_0.GetLiveContainers().GetEnumerator().Next();
			a.CompletedContainer(clusterResource, app_0, node_0, rmContainer, ContainerStatus
				.NewInstance(rmContainer.GetContainerId(), ContainerState.Complete, string.Empty
				, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
				true);
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(1 * Gb, a.GetMetrics().GetAllocatedMB());
			// Now finish another container from app_0 and fulfill the reservation
			rmContainer = app_0.GetLiveContainers().GetEnumerator().Next();
			a.CompletedContainer(clusterResource, app_0, node_0, rmContainer, ContainerStatus
				.NewInstance(rmContainer.GetContainerId(), ContainerState.Complete, string.Empty
				, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
				true);
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetMetrics().GetAllocatedMB());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStolenReservedContainer()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			//unset maxCapacity
			a.SetMaxCapacity(1.0f);
			// Users
			string user_0 = "user_0";
			string user_1 = "user_1";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_1, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_1);
			// Setup some nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 4 * Gb);
			string host_1 = "127.0.0.2";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 4 * Gb);
			int numNodes = 3;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (4 * Gb), numNodes * 16);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, priority, recordFactory)));
			// Setup app_1 to request a 4GB container on host_0 and
			// another 4GB container anywhere.
			AList<ResourceRequest> appRequests_1 = new AList<ResourceRequest>(4);
			appRequests_1.AddItem(TestUtils.CreateResourceRequest(host_0, 4 * Gb, 1, true, priority
				, recordFactory));
			appRequests_1.AddItem(TestUtils.CreateResourceRequest(DefaultRack, 4 * Gb, 1, true
				, priority, recordFactory));
			appRequests_1.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 4 * Gb
				, 2, true, priority, recordFactory));
			app_1.UpdateResourceRequests(appRequests_1);
			// Start testing...
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetAvailableMB());
			// Now, reservation should kick in for app_1
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(6 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(2 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			// node_1 heartbeats in and gets the DEFAULT_RACK request for app_1
			// We do not need locality delay here
			Org.Mockito.Mockito.DoReturn(-1).When(a).GetNodeLocalityDelay();
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(10 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(4 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(6 * Gb, a.GetMetrics().GetAllocatedMB());
			// Now free 1 container from app_0 and try to assign to node_0
			RMContainer rmContainer = app_0.GetLiveContainers().GetEnumerator().Next();
			a.CompletedContainer(clusterResource, app_0, node_0, rmContainer, ContainerStatus
				.NewInstance(rmContainer.GetContainerId(), ContainerState.Complete, string.Empty
				, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
				true);
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservationExchange()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			//unset maxCapacity
			a.SetMaxCapacity(1.0f);
			a.SetUserLimitFactor(10);
			// Users
			string user_0 = "user_0";
			string user_1 = "user_1";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_1, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_1);
			// Setup some nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 4 * Gb);
			string host_1 = "127.0.0.2";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 4 * Gb);
			int numNodes = 3;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (4 * Gb), numNodes * 16);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			Org.Mockito.Mockito.When(csContext.GetMaximumResourceCapability()).ThenReturn(Resources
				.CreateResource(4 * Gb, 16));
			Org.Mockito.Mockito.When(a.GetMaximumAllocation()).ThenReturn(Resources.CreateResource
				(4 * Gb, 16));
			Org.Mockito.Mockito.When(a.GetMinimumAllocationFactor()).ThenReturn(0.25f);
			// 1G / 4G 
			// Setup resource-requests
			Priority priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 1 * Gb, 2, true, priority, recordFactory)));
			app_1.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 4 * Gb, 1, true, priority, recordFactory)));
			// Start testing...
			// Only 1 container
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(1 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			// Also 2nd -> minCapacity = 1024 since (.1 * 8G) < minAlloc, also
			// you can get one container more than user-limit
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			// Now, reservation should kick in for app_1
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(6 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(2 * Gb, node_0.GetUsedResource().GetMemory());
			// Now free 1 container from app_0 i.e. 1G, and re-reserve it
			RMContainer rmContainer = app_0.GetLiveContainers().GetEnumerator().Next();
			a.CompletedContainer(clusterResource, app_0, node_0, rmContainer, ContainerStatus
				.NewInstance(rmContainer.GetContainerId(), ContainerState.Complete, string.Empty
				, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
				true);
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(1, app_1.GetReReservations(priority));
			// Re-reserve
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(1 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2, app_1.GetReReservations(priority));
			// Try to schedule on node_1 now, should *move* the reservation
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(9 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(1 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, node_1.GetUsedResource().GetMemory());
			// Doesn't change yet... only when reservation is cancelled or a different
			// container is reserved
			NUnit.Framework.Assert.AreEqual(2, app_1.GetReReservations(priority));
			// Now finish another container from app_0 and see the reservation cancelled
			rmContainer = app_0.GetLiveContainers().GetEnumerator().Next();
			a.CompletedContainer(clusterResource, app_0, node_0, rmContainer, ContainerStatus
				.NewInstance(rmContainer.GetContainerId(), ContainerState.Complete, string.Empty
				, ContainerExitStatus.KilledByResourcemanager), RMContainerEventType.Kill, null, 
				true);
			CSAssignment assignment = a.AssignContainers(clusterResource, node_0, new ResourceLimits
				(clusterResource));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(4 * Gb, app_1.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(4 * Gb, assignment.GetExcessReservation().GetContainer
				().GetResource().GetMemory());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalityScheduling()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			// User
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = Org.Mockito.Mockito.Spy(new FiCaSchedulerApp(appAttemptId_0
				, user_0, a, Org.Mockito.Mockito.Mock<ActiveUsersManager>(), spyRMContext));
			a.SubmitApplicationAttempt(app_0, user_0);
			// Setup some nodes and racks
			string host_0 = "127.0.0.1";
			string rack_0 = "rack_0";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, rack_0, 0, 8 * Gb);
			string host_1 = "127.0.0.2";
			string rack_1 = "rack_1";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, rack_1, 0, 8 * Gb);
			string host_2 = "127.0.0.3";
			string rack_2 = "rack_2";
			FiCaSchedulerNode node_2 = TestUtils.GetMockNode(host_2, rack_2, 0, 8 * Gb);
			int numNodes = 3;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), numNodes * 16);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests and submit
			Priority priority = TestUtils.CreateMockPriority(1);
			IList<ResourceRequest> app_0_requests_0 = new AList<ResourceRequest>();
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_0, 1 * Gb, 1, true, 
				priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_0, 1 * Gb, 1, true, 
				priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_1, 1 * Gb, 1, true, 
				priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_1, 1 * Gb, 1, true, 
				priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 1 *
				 Gb, 3, true, priority, recordFactory));
			// one extra
			app_0.UpdateResourceRequests(app_0_requests_0);
			// Start testing...
			CSAssignment assignment = null;
			// Start with off switch, shouldn't allocate due to delay scheduling
			assignment = a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource
				));
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_2), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(1, app_0.GetSchedulingOpportunities(priority));
			NUnit.Framework.Assert.AreEqual(3, app_0.GetTotalRequiredResources(priority));
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, assignment.GetType());
			// None->NODE_LOCAL
			// Another off switch, shouldn't allocate due to delay scheduling
			assignment = a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource
				));
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_2), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(2, app_0.GetSchedulingOpportunities(priority));
			NUnit.Framework.Assert.AreEqual(3, app_0.GetTotalRequiredResources(priority));
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, assignment.GetType());
			// None->NODE_LOCAL
			// Another off switch, shouldn't allocate due to delay scheduling
			assignment = a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource
				));
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_2), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(3, app_0.GetSchedulingOpportunities(priority));
			NUnit.Framework.Assert.AreEqual(3, app_0.GetTotalRequiredResources(priority));
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, assignment.GetType());
			// None->NODE_LOCAL
			// Another off switch, now we should allocate 
			// since missedOpportunities=3 and reqdContainers=3
			assignment = a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource
				));
			Org.Mockito.Mockito.Verify(app_0).Allocate(Matchers.Eq(NodeType.OffSwitch), Matchers.Eq
				(node_2), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest>(), Matchers.Any
				<Container>());
			NUnit.Framework.Assert.AreEqual(4, app_0.GetSchedulingOpportunities(priority));
			// should NOT reset
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priority));
			NUnit.Framework.Assert.AreEqual(NodeType.OffSwitch, assignment.GetType());
			// NODE_LOCAL - node_0
			assignment = a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			Org.Mockito.Mockito.Verify(app_0).Allocate(Matchers.Eq(NodeType.NodeLocal), Matchers.Eq
				(node_0), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest>(), Matchers.Any
				<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			// should reset
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priority));
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, assignment.GetType());
			// NODE_LOCAL - node_1
			assignment = a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource
				));
			Org.Mockito.Mockito.Verify(app_0).Allocate(Matchers.Eq(NodeType.NodeLocal), Matchers.Eq
				(node_1), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest>(), Matchers.Any
				<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			// should reset
			NUnit.Framework.Assert.AreEqual(0, app_0.GetTotalRequiredResources(priority));
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, assignment.GetType());
			// Add 1 more request to check for RACK_LOCAL
			app_0_requests_0.Clear();
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_1, 1 * Gb, 1, true, 
				priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_1, 1 * Gb, 1, true, 
				priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 1 *
				 Gb, 2, true, priority, recordFactory));
			// one extra
			app_0.UpdateResourceRequests(app_0_requests_0);
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priority));
			string host_3 = "127.0.0.4";
			// on rack_1
			FiCaSchedulerNode node_3 = TestUtils.GetMockNode(host_3, rack_1, 0, 8 * Gb);
			// Rack-delay
			Org.Mockito.Mockito.DoReturn(1).When(a).GetNodeLocalityDelay();
			// Shouldn't assign RACK_LOCAL yet
			assignment = a.AssignContainers(clusterResource, node_3, new ResourceLimits(clusterResource
				));
			NUnit.Framework.Assert.AreEqual(1, app_0.GetSchedulingOpportunities(priority));
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priority));
			NUnit.Framework.Assert.AreEqual(NodeType.NodeLocal, assignment.GetType());
			// None->NODE_LOCAL
			// Should assign RACK_LOCAL now
			assignment = a.AssignContainers(clusterResource, node_3, new ResourceLimits(clusterResource
				));
			Org.Mockito.Mockito.Verify(app_0).Allocate(Matchers.Eq(NodeType.RackLocal), Matchers.Eq
				(node_3), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest>(), Matchers.Any
				<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			// should reset
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priority));
			NUnit.Framework.Assert.AreEqual(NodeType.RackLocal, assignment.GetType());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApplicationPriorityScheduling()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			// User
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = Org.Mockito.Mockito.Spy(new FiCaSchedulerApp(appAttemptId_0
				, user_0, a, Org.Mockito.Mockito.Mock<ActiveUsersManager>(), spyRMContext));
			a.SubmitApplicationAttempt(app_0, user_0);
			// Setup some nodes and racks
			string host_0 = "127.0.0.1";
			string rack_0 = "rack_0";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, rack_0, 0, 8 * Gb);
			string host_1 = "127.0.0.2";
			string rack_1 = "rack_1";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, rack_1, 0, 8 * Gb);
			string host_2 = "127.0.0.3";
			string rack_2 = "rack_2";
			FiCaSchedulerNode node_2 = TestUtils.GetMockNode(host_2, rack_2, 0, 8 * Gb);
			int numNodes = 3;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), 1);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests and submit
			IList<ResourceRequest> app_0_requests_0 = new AList<ResourceRequest>();
			// P1
			Priority priority_1 = TestUtils.CreateMockPriority(1);
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_0, 1 * Gb, 1, true, 
				priority_1, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_0, 1 * Gb, 1, true, 
				priority_1, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_1, 1 * Gb, 1, true, 
				priority_1, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_1, 1 * Gb, 1, true, 
				priority_1, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 1 *
				 Gb, 2, true, priority_1, recordFactory));
			// P2
			Priority priority_2 = TestUtils.CreateMockPriority(2);
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_2, 2 * Gb, 1, true, 
				priority_2, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_2, 2 * Gb, 1, true, 
				priority_2, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 2 *
				 Gb, 1, true, priority_2, recordFactory));
			app_0.UpdateResourceRequests(app_0_requests_0);
			// Start testing...
			// Start with off switch, shouldn't allocate P1 due to delay scheduling
			// thus, no P2 either!
			a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource));
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_2), Matchers.Eq(priority_1), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(1, app_0.GetSchedulingOpportunities(priority_1));
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priority_1));
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_2), Matchers.Eq(priority_2), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority_2));
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priority_2));
			// Another off-switch, shouldn't allocate P1 due to delay scheduling
			// thus, no P2 either!
			a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource));
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_2), Matchers.Eq(priority_1), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(2, app_0.GetSchedulingOpportunities(priority_1));
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priority_1));
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_2), Matchers.Eq(priority_2), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority_2));
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priority_2));
			// Another off-switch, shouldn't allocate OFF_SWITCH P1
			a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource));
			Org.Mockito.Mockito.Verify(app_0).Allocate(Matchers.Eq(NodeType.OffSwitch), Matchers.Eq
				(node_2), Matchers.Eq(priority_1), Matchers.Any<ResourceRequest>(), Matchers.Any
				<Container>());
			NUnit.Framework.Assert.AreEqual(3, app_0.GetSchedulingOpportunities(priority_1));
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priority_1));
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_2), Matchers.Eq(priority_2), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority_2));
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priority_2));
			// Now, DATA_LOCAL for P1
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			Org.Mockito.Mockito.Verify(app_0).Allocate(Matchers.Eq(NodeType.NodeLocal), Matchers.Eq
				(node_0), Matchers.Eq(priority_1), Matchers.Any<ResourceRequest>(), Matchers.Any
				<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority_1));
			NUnit.Framework.Assert.AreEqual(0, app_0.GetTotalRequiredResources(priority_1));
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_0), Matchers.Eq(priority_2), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority_2));
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priority_2));
			// Now, OFF_SWITCH for P2
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_1), Matchers.Eq(priority_1), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority_1));
			NUnit.Framework.Assert.AreEqual(0, app_0.GetTotalRequiredResources(priority_1));
			Org.Mockito.Mockito.Verify(app_0).Allocate(Matchers.Eq(NodeType.OffSwitch), Matchers.Eq
				(node_1), Matchers.Eq(priority_2), Matchers.Any<ResourceRequest>(), Matchers.Any
				<Container>());
			NUnit.Framework.Assert.AreEqual(1, app_0.GetSchedulingOpportunities(priority_2));
			NUnit.Framework.Assert.AreEqual(0, app_0.GetTotalRequiredResources(priority_2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSchedulingConstraints()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			// User
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = Org.Mockito.Mockito.Spy(new FiCaSchedulerApp(appAttemptId_0
				, user_0, a, Org.Mockito.Mockito.Mock<ActiveUsersManager>(), spyRMContext));
			a.SubmitApplicationAttempt(app_0, user_0);
			// Setup some nodes and racks
			string host_0_0 = "127.0.0.1";
			string rack_0 = "rack_0";
			FiCaSchedulerNode node_0_0 = TestUtils.GetMockNode(host_0_0, rack_0, 0, 8 * Gb);
			string host_0_1 = "127.0.0.2";
			FiCaSchedulerNode node_0_1 = TestUtils.GetMockNode(host_0_1, rack_0, 0, 8 * Gb);
			string host_1_0 = "127.0.0.3";
			string rack_1 = "rack_1";
			FiCaSchedulerNode node_1_0 = TestUtils.GetMockNode(host_1_0, rack_1, 0, 8 * Gb);
			int numNodes = 3;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), numNodes * 16);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests and submit
			Priority priority = TestUtils.CreateMockPriority(1);
			IList<ResourceRequest> app_0_requests_0 = new AList<ResourceRequest>();
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_0_0, 1 * Gb, 1, true
				, priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_0_1, 1 * Gb, 1, true
				, priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_0, 1 * Gb, 1, true, 
				priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_1_0, 1 * Gb, 1, true
				, priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_1, 1 * Gb, 1, true, 
				priority, recordFactory));
			app_0.UpdateResourceRequests(app_0_requests_0);
			// Start testing...
			// Add one request
			app_0_requests_0.Clear();
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 1 *
				 Gb, 1, true, priority, recordFactory));
			// only one
			app_0.UpdateResourceRequests(app_0_requests_0);
			// NODE_LOCAL - node_0_1
			a.AssignContainers(clusterResource, node_0_0, new ResourceLimits(clusterResource)
				);
			Org.Mockito.Mockito.Verify(app_0).Allocate(Matchers.Eq(NodeType.NodeLocal), Matchers.Eq
				(node_0_0), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest>(), Matchers.Any
				<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			// should reset
			NUnit.Framework.Assert.AreEqual(0, app_0.GetTotalRequiredResources(priority));
			// No allocation on node_1_0 even though it's node/rack local since
			// required(ANY) == 0
			a.AssignContainers(clusterResource, node_1_0, new ResourceLimits(clusterResource)
				);
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_1_0), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			// Still zero
			// since #req=0
			NUnit.Framework.Assert.AreEqual(0, app_0.GetTotalRequiredResources(priority));
			// Add one request
			app_0_requests_0.Clear();
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 1 *
				 Gb, 1, true, priority, recordFactory));
			// only one
			app_0.UpdateResourceRequests(app_0_requests_0);
			// No allocation on node_0_1 even though it's node/rack local since
			// required(rack_1) == 0
			a.AssignContainers(clusterResource, node_0_1, new ResourceLimits(clusterResource)
				);
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_1_0), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(1, app_0.GetSchedulingOpportunities(priority));
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priority));
			// NODE_LOCAL - node_1
			a.AssignContainers(clusterResource, node_1_0, new ResourceLimits(clusterResource)
				);
			Org.Mockito.Mockito.Verify(app_0).Allocate(Matchers.Eq(NodeType.NodeLocal), Matchers.Eq
				(node_1_0), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest>(), Matchers.Any
				<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			// should reset
			NUnit.Framework.Assert.AreEqual(0, app_0.GetTotalRequiredResources(priority));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestActivateApplicationAfterQueueRefresh()
		{
			// Manipulate queue 'e'
			LeafQueue e = StubLeafQueue((LeafQueue)queues[E]);
			// Users
			string user_e = "user_e";
			Org.Mockito.Mockito.When(amResourceRequest.GetCapability()).ThenReturn(Resources.
				CreateResource(1 * Gb, 0));
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_e, e, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			e.SubmitApplicationAttempt(app_0, user_e);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_e, e, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			e.SubmitApplicationAttempt(app_1, user_e);
			// same user
			ApplicationAttemptId appAttemptId_2 = TestUtils.GetMockApplicationAttemptId(2, 0);
			FiCaSchedulerApp app_2 = new FiCaSchedulerApp(appAttemptId_2, user_e, e, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			e.SubmitApplicationAttempt(app_2, user_e);
			// same user
			// before reinitialization
			NUnit.Framework.Assert.AreEqual(2, e.activeApplications.Count);
			NUnit.Framework.Assert.AreEqual(1, e.pendingApplications.Count);
			csConf.SetDouble(CapacitySchedulerConfiguration.MaximumApplicationMastersResourcePercent
				, CapacitySchedulerConfiguration.DefaultMaximumApplicationmastersResourcePercent
				 * 2);
			IDictionary<string, CSQueue> newQueues = new Dictionary<string, CSQueue>();
			CSQueue newRoot = CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, newQueues, queues, TestUtils.spyHook);
			queues = newQueues;
			root.Reinitialize(newRoot, csContext.GetClusterResource());
			// after reinitialization
			NUnit.Framework.Assert.AreEqual(3, e.activeApplications.Count);
			NUnit.Framework.Assert.AreEqual(0, e.pendingApplications.Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNodeLocalityAfterQueueRefresh()
		{
			// Manipulate queue 'e'
			LeafQueue e = StubLeafQueue((LeafQueue)queues[E]);
			// before reinitialization
			NUnit.Framework.Assert.AreEqual(40, e.GetNodeLocalityDelay());
			csConf.SetInt(CapacitySchedulerConfiguration.NodeLocalityDelay, 60);
			IDictionary<string, CSQueue> newQueues = new Dictionary<string, CSQueue>();
			CSQueue newRoot = CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, newQueues, queues, TestUtils.spyHook);
			queues = newQueues;
			root.Reinitialize(newRoot, cs.GetClusterResource());
			// after reinitialization
			NUnit.Framework.Assert.AreEqual(60, e.GetNodeLocalityDelay());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestActivateApplicationByUpdatingClusterResource()
		{
			// Manipulate queue 'e'
			LeafQueue e = StubLeafQueue((LeafQueue)queues[E]);
			// Users
			string user_e = "user_e";
			Org.Mockito.Mockito.When(amResourceRequest.GetCapability()).ThenReturn(Resources.
				CreateResource(1 * Gb, 0));
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_e, e, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			e.SubmitApplicationAttempt(app_0, user_e);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_e, e, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			e.SubmitApplicationAttempt(app_1, user_e);
			// same user
			ApplicationAttemptId appAttemptId_2 = TestUtils.GetMockApplicationAttemptId(2, 0);
			FiCaSchedulerApp app_2 = new FiCaSchedulerApp(appAttemptId_2, user_e, e, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			e.SubmitApplicationAttempt(app_2, user_e);
			// same user
			// before updating cluster resource
			NUnit.Framework.Assert.AreEqual(2, e.activeApplications.Count);
			NUnit.Framework.Assert.AreEqual(1, e.pendingApplications.Count);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(200 * 16 * Gb, 100 * 32);
			e.UpdateClusterResource(clusterResource, new ResourceLimits(clusterResource));
			// after updating cluster resource
			NUnit.Framework.Assert.AreEqual(3, e.activeApplications.Count);
			NUnit.Framework.Assert.AreEqual(0, e.pendingApplications.Count);
		}

		public virtual bool HasQueueACL(IList<QueueUserACLInfo> aclInfos, QueueACL acl)
		{
			foreach (QueueUserACLInfo aclInfo in aclInfos)
			{
				if (aclInfo.GetUserAcls().Contains(acl))
				{
					return true;
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInheritedQueueAcls()
		{
			UserGroupInformation user = UserGroupInformation.GetCurrentUser();
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			LeafQueue b = StubLeafQueue((LeafQueue)queues[B]);
			ParentQueue c = (ParentQueue)queues[C];
			LeafQueue c1 = StubLeafQueue((LeafQueue)queues[C1]);
			NUnit.Framework.Assert.IsFalse(root.HasAccess(QueueACL.SubmitApplications, user));
			NUnit.Framework.Assert.IsTrue(a.HasAccess(QueueACL.SubmitApplications, user));
			NUnit.Framework.Assert.IsTrue(b.HasAccess(QueueACL.SubmitApplications, user));
			NUnit.Framework.Assert.IsFalse(c.HasAccess(QueueACL.SubmitApplications, user));
			NUnit.Framework.Assert.IsFalse(c1.HasAccess(QueueACL.SubmitApplications, user));
			NUnit.Framework.Assert.IsTrue(HasQueueACL(a.GetQueueUserAclInfo(user), QueueACL.SubmitApplications
				));
			NUnit.Framework.Assert.IsTrue(HasQueueACL(b.GetQueueUserAclInfo(user), QueueACL.SubmitApplications
				));
			NUnit.Framework.Assert.IsFalse(HasQueueACL(c.GetQueueUserAclInfo(user), QueueACL.
				SubmitApplications));
			NUnit.Framework.Assert.IsFalse(HasQueueACL(c1.GetQueueUserAclInfo(user), QueueACL
				.SubmitApplications));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalityConstraints()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			// User
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = Org.Mockito.Mockito.Spy(new FiCaSchedulerApp(appAttemptId_0
				, user_0, a, Org.Mockito.Mockito.Mock<ActiveUsersManager>(), spyRMContext));
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = Org.Mockito.Mockito.Spy(new FiCaSchedulerApp(appAttemptId_1
				, user_0, a, Org.Mockito.Mockito.Mock<ActiveUsersManager>(), spyRMContext));
			a.SubmitApplicationAttempt(app_1, user_0);
			// Setup some nodes and racks
			string host_0_0 = "127.0.0.1";
			string rack_0 = "rack_0";
			string host_0_1 = "127.0.0.2";
			FiCaSchedulerNode node_0_1 = TestUtils.GetMockNode(host_0_1, rack_0, 0, 8 * Gb);
			string host_1_0 = "127.0.0.3";
			string rack_1 = "rack_1";
			FiCaSchedulerNode node_1_0 = TestUtils.GetMockNode(host_1_0, rack_1, 0, 8 * Gb);
			string host_1_1 = "127.0.0.4";
			FiCaSchedulerNode node_1_1 = TestUtils.GetMockNode(host_1_1, rack_1, 0, 8 * Gb);
			int numNodes = 4;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), numNodes * 1);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			// resourceName: <priority, memory, #containers, relaxLocality>
			// host_0_0: < 1, 1GB, 1, true >
			// host_0_1: < null >
			// rack_0:   < null >                     <----
			// host_1_0: < 1, 1GB, 1, true >
			// host_1_1: < null >
			// rack_1:   < 1, 1GB, 1, false >         <----
			// ANY:      < 1, 1GB, 1, false >         <----
			// Availability:
			// host_0_0: 8G
			// host_0_1: 8G
			// host_1_0: 8G
			// host_1_1: 8G
			// Blacklist: <host_0_0>
			Priority priority = TestUtils.CreateMockPriority(1);
			IList<ResourceRequest> app_0_requests_0 = new AList<ResourceRequest>();
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_0_0, 1 * Gb, 1, true
				, priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(host_1_0, 1 * Gb, 1, true
				, priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_1, 1 * Gb, 1, false
				, priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 1 *
				 Gb, 1, false, priority, recordFactory));
			// only one
			app_0.UpdateResourceRequests(app_0_requests_0);
			app_0.UpdateBlacklist(Sharpen.Collections.SingletonList(host_0_0), null);
			app_0_requests_0.Clear();
			//
			// Start testing...
			//
			// node_0_1  
			// Shouldn't allocate since RR(rack_0) = null && RR(ANY) = relax: false
			a.AssignContainers(clusterResource, node_0_1, new ResourceLimits(clusterResource)
				);
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_0_1), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			// should be 0
			// resourceName: <priority, memory, #containers, relaxLocality>
			// host_0_0: < 1, 1GB, 1, true >
			// host_0_1: < null >
			// rack_0:   < null >                     <----
			// host_1_0: < 1, 1GB, 1, true >
			// host_1_1: < null >
			// rack_1:   < 1, 1GB, 1, false >         <----
			// ANY:      < 1, 1GB, 1, false >         <----
			// Availability:
			// host_0_0: 8G
			// host_0_1: 8G
			// host_1_0: 8G
			// host_1_1: 8G
			// Blacklist: <host_0_0>
			// node_1_1  
			// Shouldn't allocate since RR(rack_1) = relax: false
			a.AssignContainers(clusterResource, node_1_1, new ResourceLimits(clusterResource)
				);
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_0_1), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			// should be 0
			// Allow rack-locality for rack_1, but blacklist node_1_1
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_1, 1 * Gb, 1, true, 
				priority, recordFactory));
			app_0.UpdateResourceRequests(app_0_requests_0);
			app_0.UpdateBlacklist(Sharpen.Collections.SingletonList(host_1_1), null);
			app_0_requests_0.Clear();
			// resourceName: <priority, memory, #containers, relaxLocality>
			// host_0_0: < 1, 1GB, 1, true >
			// host_0_1: < null >
			// rack_0:   < null >                     
			// host_1_0: < 1, 1GB, 1, true >
			// host_1_1: < null >
			// rack_1:   < 1, 1GB, 1, true >         
			// ANY:      < 1, 1GB, 1, false >         
			// Availability:
			// host_0_0: 8G
			// host_0_1: 8G
			// host_1_0: 8G
			// host_1_1: 8G
			// Blacklist: < host_0_0 , host_1_1 >       <----
			// node_1_1  
			// Shouldn't allocate since node_1_1 is blacklisted
			a.AssignContainers(clusterResource, node_1_1, new ResourceLimits(clusterResource)
				);
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_1_1), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			// should be 0
			// Now, remove node_1_1 from blacklist, but add rack_1 to blacklist
			app_0.UpdateResourceRequests(app_0_requests_0);
			app_0.UpdateBlacklist(Sharpen.Collections.SingletonList(rack_1), Sharpen.Collections
				.SingletonList(host_1_1));
			app_0_requests_0.Clear();
			// resourceName: <priority, memory, #containers, relaxLocality>
			// host_0_0: < 1, 1GB, 1, true >
			// host_0_1: < null >
			// rack_0:   < null >                     
			// host_1_0: < 1, 1GB, 1, true >
			// host_1_1: < null >
			// rack_1:   < 1, 1GB, 1, true >         
			// ANY:      < 1, 1GB, 1, false >         
			// Availability:
			// host_0_0: 8G
			// host_0_1: 8G
			// host_1_0: 8G
			// host_1_1: 8G
			// Blacklist: < host_0_0 , rack_1 >       <----
			// node_1_1  
			// Shouldn't allocate since rack_1 is blacklisted
			a.AssignContainers(clusterResource, node_1_1, new ResourceLimits(clusterResource)
				);
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Any
				<NodeType>(), Matchers.Eq(node_1_1), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest
				>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			// should be 0
			// Now remove rack_1 from blacklist
			app_0.UpdateResourceRequests(app_0_requests_0);
			app_0.UpdateBlacklist(null, Sharpen.Collections.SingletonList(rack_1));
			app_0_requests_0.Clear();
			// resourceName: <priority, memory, #containers, relaxLocality>
			// host_0_0: < 1, 1GB, 1, true >
			// host_0_1: < null >
			// rack_0:   < null >                     
			// host_1_0: < 1, 1GB, 1, true >
			// host_1_1: < null >
			// rack_1:   < 1, 1GB, 1, true >         
			// ANY:      < 1, 1GB, 1, false >         
			// Availability:
			// host_0_0: 8G
			// host_0_1: 8G
			// host_1_0: 8G
			// host_1_1: 8G
			// Blacklist: < host_0_0 >       <----
			// Now, should allocate since RR(rack_1) = relax: true
			a.AssignContainers(clusterResource, node_1_1, new ResourceLimits(clusterResource)
				);
			Org.Mockito.Mockito.Verify(app_0, Org.Mockito.Mockito.Never()).Allocate(Matchers.Eq
				(NodeType.RackLocal), Matchers.Eq(node_1_1), Matchers.Any<Priority>(), Matchers.Any
				<ResourceRequest>(), Matchers.Any<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priority));
			// Now sanity-check node_local
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(rack_1, 1 * Gb, 1, false
				, priority, recordFactory));
			app_0_requests_0.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 1 *
				 Gb, 1, false, priority, recordFactory));
			// only one
			app_0.UpdateResourceRequests(app_0_requests_0);
			app_0_requests_0.Clear();
			// resourceName: <priority, memory, #containers, relaxLocality>
			// host_0_0: < 1, 1GB, 1, true >
			// host_0_1: < null >
			// rack_0:   < null >                     
			// host_1_0: < 1, 1GB, 1, true >
			// host_1_1: < null >
			// rack_1:   < 1, 1GB, 1, false >          <----
			// ANY:      < 1, 1GB, 1, false >          <----
			// Availability:
			// host_0_0: 8G
			// host_0_1: 8G
			// host_1_0: 8G
			// host_1_1: 7G
			a.AssignContainers(clusterResource, node_1_0, new ResourceLimits(clusterResource)
				);
			Org.Mockito.Mockito.Verify(app_0).Allocate(Matchers.Eq(NodeType.NodeLocal), Matchers.Eq
				(node_1_0), Matchers.Any<Priority>(), Matchers.Any<ResourceRequest>(), Matchers.Any
				<Container>());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetSchedulingOpportunities(priority));
			NUnit.Framework.Assert.AreEqual(0, app_0.GetTotalRequiredResources(priority));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxAMResourcePerQueuePercentAfterQueueRefresh()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(100 * 16 * Gb, 100 * 32);
			CapacitySchedulerContext csContext = MockCSContext(csConf, clusterResource);
			Org.Mockito.Mockito.When(csContext.GetRMContext()).ThenReturn(rmContext);
			csConf.SetFloat(CapacitySchedulerConfiguration.MaximumApplicationMastersResourcePercent
				, 0.1f);
			ParentQueue root = new ParentQueue(csContext, CapacitySchedulerConfiguration.Root
				, null, null);
			csConf.SetCapacity(CapacitySchedulerConfiguration.Root + "." + A, 80);
			LeafQueue a = new LeafQueue(csContext, A, root, null);
			NUnit.Framework.Assert.AreEqual(0.1f, a.GetMaxAMResourcePerQueuePercent(), 1e-3f);
			NUnit.Framework.Assert.AreEqual(a.GetAMResourceLimit(), Resources.CreateResource(
				160 * Gb, 1));
			csConf.SetFloat(CapacitySchedulerConfiguration.MaximumApplicationMastersResourcePercent
				, 0.2f);
			LeafQueue newA = new LeafQueue(csContext, A, root, null);
			a.Reinitialize(newA, clusterResource);
			NUnit.Framework.Assert.AreEqual(0.2f, a.GetMaxAMResourcePerQueuePercent(), 1e-3f);
			NUnit.Framework.Assert.AreEqual(a.GetAMResourceLimit(), Resources.CreateResource(
				320 * Gb, 1));
			Org.Apache.Hadoop.Yarn.Api.Records.Resource newClusterResource = Resources.CreateResource
				(100 * 20 * Gb, 100 * 32);
			a.UpdateClusterResource(newClusterResource, new ResourceLimits(newClusterResource
				));
			//  100 * 20 * 0.2 = 400
			NUnit.Framework.Assert.AreEqual(a.GetAMResourceLimit(), Resources.CreateResource(
				400 * Gb, 1));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllocateContainerOnNodeWithoutOffSwitchSpecified()
		{
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[B]);
			// Users
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// same user
			// Setup some nodes
			string host_0 = "127.0.0.1";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			int numNodes = 1;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb), numNodes * 16);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priority = TestUtils.CreateMockPriority(1);
			app_0.UpdateResourceRequests(Arrays.AsList(TestUtils.CreateResourceRequest("127.0.0.1"
				, 1 * Gb, 3, true, priority, recordFactory), TestUtils.CreateResourceRequest(DefaultRack
				, 1 * Gb, 3, true, priority, recordFactory)));
			try
			{
				a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			}
			catch (ArgumentNullException)
			{
				NUnit.Framework.Assert.Fail("NPE when allocating container on node but " + "forget to set off-switch request should be handled"
					);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConcurrentAccess()
		{
			YarnConfiguration conf = new YarnConfiguration();
			MockRM rm = new MockRM();
			rm.Init(conf);
			rm.Start();
			string queue = "default";
			string user = "user";
			CapacityScheduler cs = (CapacityScheduler)rm.GetResourceScheduler();
			LeafQueue defaultQueue = (LeafQueue)cs.GetQueue(queue);
			IList<FiCaSchedulerApp> listOfApps = CreateListOfApps(10000, user, defaultQueue);
			CyclicBarrier cb = new CyclicBarrier(2);
			IList<ConcurrentModificationException> conException = new AList<ConcurrentModificationException
				>();
			Sharpen.Thread submitAndRemove = new Sharpen.Thread(new _Runnable_2479(listOfApps
				, defaultQueue, user, cb, queue), "SubmitAndRemoveApplicationAttempt Thread");
			// Ignore
			Sharpen.Thread getAppsInQueue = new Sharpen.Thread(new _Runnable_2498(cb, defaultQueue
				, conException), "GetAppsInQueue Thread");
			// Ignore
			submitAndRemove.Start();
			getAppsInQueue.Start();
			submitAndRemove.Join();
			getAppsInQueue.Join();
			NUnit.Framework.Assert.IsTrue("ConcurrentModificationException is thrown", conException
				.IsEmpty());
			rm.Stop();
		}

		private sealed class _Runnable_2479 : Runnable
		{
			public _Runnable_2479(IList<FiCaSchedulerApp> listOfApps, LeafQueue defaultQueue, 
				string user, CyclicBarrier cb, string queue)
			{
				this.listOfApps = listOfApps;
				this.defaultQueue = defaultQueue;
				this.user = user;
				this.cb = cb;
				this.queue = queue;
			}

			public void Run()
			{
				foreach (FiCaSchedulerApp fiCaSchedulerApp in listOfApps)
				{
					defaultQueue.SubmitApplicationAttempt(fiCaSchedulerApp, user);
				}
				try
				{
					cb.Await();
				}
				catch (Exception)
				{
				}
				foreach (FiCaSchedulerApp fiCaSchedulerApp_1 in listOfApps)
				{
					defaultQueue.FinishApplicationAttempt(fiCaSchedulerApp_1, queue);
				}
			}

			private readonly IList<FiCaSchedulerApp> listOfApps;

			private readonly LeafQueue defaultQueue;

			private readonly string user;

			private readonly CyclicBarrier cb;

			private readonly string queue;
		}

		private sealed class _Runnable_2498 : Runnable
		{
			public _Runnable_2498(CyclicBarrier cb, LeafQueue defaultQueue, IList<ConcurrentModificationException
				> conException)
			{
				this.cb = cb;
				this.defaultQueue = defaultQueue;
				this.conException = conException;
				this.apps = new AList<ApplicationAttemptId>();
			}

			internal IList<ApplicationAttemptId> apps;

			public void Run()
			{
				try
				{
					try
					{
						cb.Await();
					}
					catch (Exception)
					{
					}
					defaultQueue.CollectSchedulerApplications(this.apps);
				}
				catch (ConcurrentModificationException e)
				{
					conException.AddItem(e);
				}
			}

			private readonly CyclicBarrier cb;

			private readonly LeafQueue defaultQueue;

			private readonly IList<ConcurrentModificationException> conException;
		}

		private IList<FiCaSchedulerApp> CreateListOfApps(int noOfApps, string user, LeafQueue
			 defaultQueue)
		{
			IList<FiCaSchedulerApp> appsLists = new AList<FiCaSchedulerApp>();
			for (int i = 0; i < noOfApps; i++)
			{
				ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(i, 0);
				FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user, defaultQueue, 
					Org.Mockito.Mockito.Mock<ActiveUsersManager>(), spyRMContext);
				appsLists.AddItem(app_0);
			}
			return appsLists;
		}

		private CapacitySchedulerContext MockCSContext(CapacitySchedulerConfiguration csConf
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource)
		{
			CapacitySchedulerContext csContext = Org.Mockito.Mockito.Mock<CapacitySchedulerContext
				>();
			Org.Mockito.Mockito.When(csContext.GetConfiguration()).ThenReturn(csConf);
			Org.Mockito.Mockito.When(csContext.GetConf()).ThenReturn(new YarnConfiguration());
			Org.Mockito.Mockito.When(csContext.GetResourceCalculator()).ThenReturn(resourceCalculator
				);
			Org.Mockito.Mockito.When(csContext.GetClusterResource()).ThenReturn(clusterResource
				);
			Org.Mockito.Mockito.When(csContext.GetMinimumResourceCapability()).ThenReturn(Resources
				.CreateResource(Gb, 1));
			Org.Mockito.Mockito.When(csContext.GetMaximumResourceCapability()).ThenReturn(Resources
				.CreateResource(2 * Gb, 2));
			return csContext;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cs != null)
			{
				cs.Stop();
			}
		}
	}
}
