using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestReservations
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestReservations));

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal RMContext rmContext;

		internal RMContext spyRMContext;

		internal CapacityScheduler cs;

		internal CapacitySchedulerContext csContext;

		private readonly ResourceCalculator resourceCalculator = new DefaultResourceCalculator
			();

		internal CSQueue root;

		internal IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();

		internal IDictionary<string, CSQueue> oldQueues = new Dictionary<string, CSQueue>
			();

		internal const int Gb = 1024;

		internal const string DefaultRack = "/default";

		// CapacitySchedulerConfiguration csConf;
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			CapacityScheduler spyCs = new CapacityScheduler();
			cs = Org.Mockito.Mockito.Spy(spyCs);
			rmContext = TestUtils.GetMockRMContext();
		}

		/// <exception cref="System.Exception"/>
		private void Setup(CapacitySchedulerConfiguration csConf)
		{
			csConf.SetBoolean("yarn.scheduler.capacity.user-metrics.enable", true);
			string newRoot = "root" + Runtime.CurrentTimeMillis();
			// final String newRoot = "root";
			SetupQueueConfiguration(csConf, newRoot);
			YarnConfiguration conf = new YarnConfiguration();
			cs.SetConf(conf);
			csContext = Org.Mockito.Mockito.Mock<CapacitySchedulerContext>();
			Org.Mockito.Mockito.When(csContext.GetConfiguration()).ThenReturn(csConf);
			Org.Mockito.Mockito.When(csContext.GetConf()).ThenReturn(conf);
			Org.Mockito.Mockito.When(csContext.GetMinimumResourceCapability()).ThenReturn(Resources
				.CreateResource(Gb, 1));
			Org.Mockito.Mockito.When(csContext.GetMaximumResourceCapability()).ThenReturn(Resources
				.CreateResource(16 * Gb, 12));
			Org.Mockito.Mockito.When(csContext.GetClusterResource()).ThenReturn(Resources.CreateResource
				(100 * 16 * Gb, 100 * 12));
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
			spyRMContext = Org.Mockito.Mockito.Spy(rmContext);
			Org.Mockito.Mockito.When(spyRMContext.GetScheduler()).ThenReturn(cs);
			Org.Mockito.Mockito.When(spyRMContext.GetYarnConfiguration()).ThenReturn(new YarnConfiguration
				());
			cs.SetRMContext(spyRMContext);
			cs.Init(csConf);
			cs.Start();
		}

		private const string A = "a";

		private void SetupQueueConfiguration(CapacitySchedulerConfiguration conf, string 
			newRoot)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { newRoot });
			conf.SetMaximumCapacity(CapacitySchedulerConfiguration.Root, 100);
			conf.SetAcl(CapacitySchedulerConfiguration.Root, QueueACL.SubmitApplications, " "
				);
			string Q_newRoot = CapacitySchedulerConfiguration.Root + "." + newRoot;
			conf.SetQueues(Q_newRoot, new string[] { A });
			conf.SetCapacity(Q_newRoot, 100);
			conf.SetMaximumCapacity(Q_newRoot, 100);
			conf.SetAcl(Q_newRoot, QueueACL.SubmitApplications, " ");
			string QA = Q_newRoot + "." + A;
			conf.SetCapacity(QA, 100f);
			conf.SetMaximumCapacity(QA, 100);
			conf.SetAcl(QA, QueueACL.SubmitApplications, "*");
		}

		internal static LeafQueue StubLeafQueue(LeafQueue queue)
		{
			// Mock some methods for ease in these unit tests
			// 1. LeafQueue.createContainer to return dummy containers
			Org.Mockito.Mockito.DoAnswer(new _Answer_179()).When(queue).CreateContainer(Matchers.Any
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

		private sealed class _Answer_179 : Answer<Container>
		{
			public _Answer_179()
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
		public virtual void TestReservation()
		{
			// Test that we now unreserve and use a node that has space
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			Setup(csConf);
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			// Users
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			rmContext.GetRMApps()[app_0.GetApplicationId()] = Org.Mockito.Mockito.Mock<RMApp>
				();
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// Setup some nodes
			string host_0 = "host_0";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			string host_1 = "host_1";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			string host_2 = "host_2";
			FiCaSchedulerNode node_2 = TestUtils.GetMockNode(host_2, DefaultRack, 0, 8 * Gb);
			Org.Mockito.Mockito.When(csContext.GetNode(node_0.GetNodeID())).ThenReturn(node_0
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_1.GetNodeID())).ThenReturn(node_1
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_2.GetNodeID())).ThenReturn(node_2
				);
			int numNodes = 3;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb));
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priorityAM = TestUtils.CreateMockPriority(1);
			Priority priorityMap = TestUtils.CreateMockPriority(5);
			Priority priorityReduce = TestUtils.CreateMockPriority(10);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, priorityAM, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 5 * Gb, 2, true, priorityReduce, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 3 * Gb, 2, true, priorityMap, recordFactory)));
			// Start testing...
			// Only AM
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(22 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			// Only 1 map - simulating reduce
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(19 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			// Only 1 map to other node - simulating reduce
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(16 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(16 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(null, node_0.GetReservedContainer());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priorityReduce
				));
			// try to assign reducer (5G on node 0 and should reserve)
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(11 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(11 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetReservedContainer().GetReservedResource
				().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priorityReduce
				));
			// assign reducer to node 2
			a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(18 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(13 * Gb, app_0.GetCurrentConsumption().GetMemory(
				));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(6 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(6 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetReservedContainer().GetReservedResource
				().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_2.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priorityReduce
				));
			// node_1 heartbeat and unreserves from node_0 in order to allocate
			// on node_1
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(18 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(18 * Gb, app_0.GetCurrentConsumption().GetMemory(
				));
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(18 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(6 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(6 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(null, node_0.GetReservedContainer());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_2.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0, app_0.GetTotalRequiredResources(priorityReduce
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservationNoContinueLook()
		{
			// Test that with reservations-continue-look-all-nodes feature off
			// we don't unreserve and show we could get stuck
			queues = new Dictionary<string, CSQueue>();
			// test that the deadlock occurs when turned off
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			csConf.SetBoolean("yarn.scheduler.capacity.reservations-continue-look-all-nodes", 
				false);
			Setup(csConf);
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			// Users
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			rmContext.GetRMApps()[app_0.GetApplicationId()] = Org.Mockito.Mockito.Mock<RMApp>
				();
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// Setup some nodes
			string host_0 = "host_0";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			string host_1 = "host_1";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			string host_2 = "host_2";
			FiCaSchedulerNode node_2 = TestUtils.GetMockNode(host_2, DefaultRack, 0, 8 * Gb);
			Org.Mockito.Mockito.When(csContext.GetNode(node_0.GetNodeID())).ThenReturn(node_0
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_1.GetNodeID())).ThenReturn(node_1
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_2.GetNodeID())).ThenReturn(node_2
				);
			int numNodes = 3;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb));
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priorityAM = TestUtils.CreateMockPriority(1);
			Priority priorityMap = TestUtils.CreateMockPriority(5);
			Priority priorityReduce = TestUtils.CreateMockPriority(10);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, priorityAM, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 5 * Gb, 2, true, priorityReduce, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 3 * Gb, 2, true, priorityMap, recordFactory)));
			// Start testing...
			// Only AM
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(22 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			// Only 1 map - simulating reduce
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(19 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			// Only 1 map to other node - simulating reduce
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(16 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(16 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(null, node_0.GetReservedContainer());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priorityReduce
				));
			// try to assign reducer (5G on node 0 and should reserve)
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(11 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(11 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetReservedContainer().GetReservedResource
				().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priorityReduce
				));
			// assign reducer to node 2
			a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(18 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(13 * Gb, app_0.GetCurrentConsumption().GetMemory(
				));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(6 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(6 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetReservedContainer().GetReservedResource
				().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_2.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priorityReduce
				));
			// node_1 heartbeat and won't unreserve from node_0, potentially stuck
			// if AM doesn't handle
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(18 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(13 * Gb, app_0.GetCurrentConsumption().GetMemory(
				));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(6 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(6 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetReservedContainer().GetReservedResource
				().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_2.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priorityReduce
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAssignContainersNeedToUnreserve()
		{
			// Test that we now unreserve and use a node that has space
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			Setup(csConf);
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			// Users
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			rmContext.GetRMApps()[app_0.GetApplicationId()] = Org.Mockito.Mockito.Mock<RMApp>
				();
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// Setup some nodes
			string host_0 = "host_0";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			string host_1 = "host_1";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			Org.Mockito.Mockito.When(csContext.GetNode(node_0.GetNodeID())).ThenReturn(node_0
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_1.GetNodeID())).ThenReturn(node_1
				);
			int numNodes = 2;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb));
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priorityAM = TestUtils.CreateMockPriority(1);
			Priority priorityMap = TestUtils.CreateMockPriority(5);
			Priority priorityReduce = TestUtils.CreateMockPriority(10);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, priorityAM, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 5 * Gb, 2, true, priorityReduce, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 3 * Gb, 2, true, priorityMap, recordFactory)));
			// Start testing...
			// Only AM
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(14 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			// Only 1 map - simulating reduce
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(11 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			// Only 1 map to other node - simulating reduce
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(null, node_0.GetReservedContainer());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priorityReduce
				));
			// try to assign reducer (5G on node 0 and should reserve)
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetReservedContainer().GetReservedResource
				().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(2, app_0.GetTotalRequiredResources(priorityReduce
				));
			// could allocate but told need to unreserve first
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(13 * Gb, app_0.GetCurrentConsumption().GetMemory(
				));
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(null, node_0.GetReservedContainer());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(1, app_0.GetTotalRequiredResources(priorityReduce
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAppToUnreserve()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			Setup(csConf);
			string user_0 = "user_0";
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			string host_0 = "host_0";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			string host_1 = "host_1";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(2 * 8 * Gb);
			// Setup resource-requests
			Priority priorityMap = TestUtils.CreateMockPriority(5);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability = Resources.CreateResource
				(2 * Gb, 0);
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			SystemMetricsPublisher publisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			ContainerAllocationExpirer expirer = Org.Mockito.Mockito.Mock<ContainerAllocationExpirer
				>();
			DrainDispatcher drainDispatcher = new DrainDispatcher();
			Org.Mockito.Mockito.When(rmContext.GetContainerAllocationExpirer()).ThenReturn(expirer
				);
			Org.Mockito.Mockito.When(rmContext.GetDispatcher()).ThenReturn(drainDispatcher);
			Org.Mockito.Mockito.When(rmContext.GetRMApplicationHistoryWriter()).ThenReturn(writer
				);
			Org.Mockito.Mockito.When(rmContext.GetSystemMetricsPublisher()).ThenReturn(publisher
				);
			Org.Mockito.Mockito.When(rmContext.GetYarnConfiguration()).ThenReturn(new YarnConfiguration
				());
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(app_0.GetApplicationId
				(), 1);
			ContainerId containerId = BuilderUtils.NewContainerId(appAttemptId, 1);
			Container container = TestUtils.GetMockContainer(containerId, node_1.GetNodeID(), 
				Resources.CreateResource(2 * Gb), priorityMap);
			RMContainer rmContainer = new RMContainerImpl(container, appAttemptId, node_1.GetNodeID
				(), "user", rmContext);
			Container container_1 = TestUtils.GetMockContainer(containerId, node_0.GetNodeID(
				), Resources.CreateResource(1 * Gb), priorityMap);
			RMContainer rmContainer_1 = new RMContainerImpl(container_1, appAttemptId, node_0
				.GetNodeID(), "user", rmContext);
			// no reserved containers
			NodeId unreserveId = app_0.GetNodeIdToUnreserve(priorityMap, capability, cs.GetResourceCalculator
				(), clusterResource);
			NUnit.Framework.Assert.AreEqual(null, unreserveId);
			// no reserved containers - reserve then unreserve
			app_0.Reserve(node_0, priorityMap, rmContainer_1, container_1);
			app_0.Unreserve(node_0, priorityMap);
			unreserveId = app_0.GetNodeIdToUnreserve(priorityMap, capability, cs.GetResourceCalculator
				(), clusterResource);
			NUnit.Framework.Assert.AreEqual(null, unreserveId);
			// no container large enough is reserved
			app_0.Reserve(node_0, priorityMap, rmContainer_1, container_1);
			unreserveId = app_0.GetNodeIdToUnreserve(priorityMap, capability, cs.GetResourceCalculator
				(), clusterResource);
			NUnit.Framework.Assert.AreEqual(null, unreserveId);
			// reserve one that is now large enough
			app_0.Reserve(node_1, priorityMap, rmContainer, container);
			unreserveId = app_0.GetNodeIdToUnreserve(priorityMap, capability, cs.GetResourceCalculator
				(), clusterResource);
			NUnit.Framework.Assert.AreEqual(node_1.GetNodeID(), unreserveId);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFindNodeToUnreserve()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			Setup(csConf);
			string user_0 = "user_0";
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			string host_1 = "host_1";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			// Setup resource-requests
			Priority priorityMap = TestUtils.CreateMockPriority(5);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability = Resources.CreateResource
				(2 * Gb, 0);
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			SystemMetricsPublisher publisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			ContainerAllocationExpirer expirer = Org.Mockito.Mockito.Mock<ContainerAllocationExpirer
				>();
			DrainDispatcher drainDispatcher = new DrainDispatcher();
			Org.Mockito.Mockito.When(rmContext.GetContainerAllocationExpirer()).ThenReturn(expirer
				);
			Org.Mockito.Mockito.When(rmContext.GetDispatcher()).ThenReturn(drainDispatcher);
			Org.Mockito.Mockito.When(rmContext.GetRMApplicationHistoryWriter()).ThenReturn(writer
				);
			Org.Mockito.Mockito.When(rmContext.GetSystemMetricsPublisher()).ThenReturn(publisher
				);
			Org.Mockito.Mockito.When(rmContext.GetYarnConfiguration()).ThenReturn(new YarnConfiguration
				());
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(app_0.GetApplicationId
				(), 1);
			ContainerId containerId = BuilderUtils.NewContainerId(appAttemptId, 1);
			Container container = TestUtils.GetMockContainer(containerId, node_1.GetNodeID(), 
				Resources.CreateResource(2 * Gb), priorityMap);
			RMContainer rmContainer = new RMContainerImpl(container, appAttemptId, node_1.GetNodeID
				(), "user", rmContext);
			// nothing reserved
			bool res = a.FindNodeToUnreserve(csContext.GetClusterResource(), node_1, app_0, priorityMap
				, capability);
			NUnit.Framework.Assert.IsFalse(res);
			// reserved but scheduler doesn't know about that node.
			app_0.Reserve(node_1, priorityMap, rmContainer, container);
			node_1.ReserveResource(app_0, priorityMap, rmContainer);
			res = a.FindNodeToUnreserve(csContext.GetClusterResource(), node_1, app_0, priorityMap
				, capability);
			NUnit.Framework.Assert.IsFalse(res);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAssignToQueue()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			Setup(csConf);
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			// Users
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			rmContext.GetRMApps()[app_0.GetApplicationId()] = Org.Mockito.Mockito.Mock<RMApp>
				();
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// Setup some nodes
			string host_0 = "host_0";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			string host_1 = "host_1";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			string host_2 = "host_2";
			FiCaSchedulerNode node_2 = TestUtils.GetMockNode(host_2, DefaultRack, 0, 8 * Gb);
			Org.Mockito.Mockito.When(csContext.GetNode(node_0.GetNodeID())).ThenReturn(node_0
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_1.GetNodeID())).ThenReturn(node_1
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_2.GetNodeID())).ThenReturn(node_2
				);
			int numNodes = 2;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb));
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priorityAM = TestUtils.CreateMockPriority(1);
			Priority priorityMap = TestUtils.CreateMockPriority(5);
			Priority priorityReduce = TestUtils.CreateMockPriority(10);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, priorityAM, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 5 * Gb, 2, true, priorityReduce, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 3 * Gb, 2, true, priorityMap, recordFactory)));
			// Start testing...
			// Only AM
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(14 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			// Only 1 map - simulating reduce
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(11 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			// Only 1 map to other node - simulating reduce
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(null, node_0.GetReservedContainer());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			// allocate to queue so that the potential new capacity is greater then
			// absoluteMaxCapacity
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability = Resources.CreateResource
				(32 * Gb, 0);
			ResourceLimits limits = new ResourceLimits(clusterResource);
			bool res = a.CanAssignToThisQueue(clusterResource, CommonNodeLabelsManager.EmptyStringSet
				, limits, capability, Resources.None());
			NUnit.Framework.Assert.IsFalse(res);
			NUnit.Framework.Assert.AreEqual(limits.GetAmountNeededUnreserve(), Resources.None
				());
			// now add in reservations and make sure it continues if config set
			// allocate to queue so that the potential new capacity is greater then
			// absoluteMaxCapacity
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			capability = Resources.CreateResource(5 * Gb, 0);
			limits = new ResourceLimits(clusterResource);
			res = a.CanAssignToThisQueue(clusterResource, CommonNodeLabelsManager.EmptyStringSet
				, limits, capability, Resources.CreateResource(5 * Gb));
			NUnit.Framework.Assert.IsTrue(res);
			// 16GB total, 13GB consumed (8 allocated, 5 reserved). asking for 5GB so we would have to
			// unreserve 2GB to get the total 5GB needed.
			// also note vcore checks not enabled
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(2 * Gb, 3), limits.GetAmountNeededUnreserve
				());
			// tell to not check reservations
			limits = new ResourceLimits(clusterResource);
			res = a.CanAssignToThisQueue(clusterResource, CommonNodeLabelsManager.EmptyStringSet
				, limits, capability, Resources.None());
			NUnit.Framework.Assert.IsFalse(res);
			NUnit.Framework.Assert.AreEqual(Resources.None(), limits.GetAmountNeededUnreserve
				());
			RefreshQueuesTurnOffReservationsContLook(a, csConf);
			// should return false since reservations continue look is off.
			limits = new ResourceLimits(clusterResource);
			res = a.CanAssignToThisQueue(clusterResource, CommonNodeLabelsManager.EmptyStringSet
				, limits, capability, Resources.None());
			NUnit.Framework.Assert.IsFalse(res);
			NUnit.Framework.Assert.AreEqual(limits.GetAmountNeededUnreserve(), Resources.None
				());
			limits = new ResourceLimits(clusterResource);
			res = a.CanAssignToThisQueue(clusterResource, CommonNodeLabelsManager.EmptyStringSet
				, limits, capability, Resources.CreateResource(5 * Gb));
			NUnit.Framework.Assert.IsFalse(res);
			NUnit.Framework.Assert.AreEqual(Resources.None(), limits.GetAmountNeededUnreserve
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void RefreshQueuesTurnOffReservationsContLook(LeafQueue a, CapacitySchedulerConfiguration
			 csConf)
		{
			// before reinitialization
			NUnit.Framework.Assert.AreEqual(true, a.GetReservationContinueLooking());
			NUnit.Framework.Assert.AreEqual(true, ((ParentQueue)a.GetParent()).GetReservationContinueLooking
				());
			csConf.SetBoolean(CapacitySchedulerConfiguration.ReserveContLookAllNodes, false);
			IDictionary<string, CSQueue> newQueues = new Dictionary<string, CSQueue>();
			CSQueue newRoot = CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, newQueues, queues, TestUtils.spyHook);
			queues = newQueues;
			root.Reinitialize(newRoot, cs.GetClusterResource());
			// after reinitialization
			NUnit.Framework.Assert.AreEqual(false, a.GetReservationContinueLooking());
			NUnit.Framework.Assert.AreEqual(false, ((ParentQueue)a.GetParent()).GetReservationContinueLooking
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContinueLookingReservationsAfterQueueRefresh()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			Setup(csConf);
			// Manipulate queue 'e'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			RefreshQueuesTurnOffReservationsContLook(a, csConf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAssignToUser()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			Setup(csConf);
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			// Users
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			rmContext.GetRMApps()[app_0.GetApplicationId()] = Org.Mockito.Mockito.Mock<RMApp>
				();
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// Setup some nodes
			string host_0 = "host_0";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			string host_1 = "host_1";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			string host_2 = "host_2";
			FiCaSchedulerNode node_2 = TestUtils.GetMockNode(host_2, DefaultRack, 0, 8 * Gb);
			Org.Mockito.Mockito.When(csContext.GetNode(node_0.GetNodeID())).ThenReturn(node_0
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_1.GetNodeID())).ThenReturn(node_1
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_2.GetNodeID())).ThenReturn(node_2
				);
			int numNodes = 2;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb));
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priorityAM = TestUtils.CreateMockPriority(1);
			Priority priorityMap = TestUtils.CreateMockPriority(5);
			Priority priorityReduce = TestUtils.CreateMockPriority(10);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, priorityAM, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 3 * Gb, 2, true, priorityMap, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 5 * Gb, 2, true, priorityReduce, recordFactory)));
			// Start testing...
			// Only AM
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(14 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			// Only 1 map - simulating reduce
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(11 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			// Only 1 map to other node - simulating reduce
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(null, node_0.GetReservedContainer());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			// now add in reservations and make sure it continues if config set
			// allocate to queue so that the potential new capacity is greater then
			// absoluteMaxCapacity
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(5 * Gb, app_0.GetCurrentReservation().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			// not over the limit
			Org.Apache.Hadoop.Yarn.Api.Records.Resource limit = Resources.CreateResource(14 *
				 Gb, 0);
			ResourceLimits userResourceLimits = new ResourceLimits(clusterResource);
			bool res = a.AssignToUser(clusterResource, user_0, limit, app_0, null, userResourceLimits
				);
			NUnit.Framework.Assert.IsTrue(res);
			NUnit.Framework.Assert.AreEqual(Resources.None(), userResourceLimits.GetAmountNeededUnreserve
				());
			// set limit so it subtracts reservations and it can continue
			limit = Resources.CreateResource(12 * Gb, 0);
			userResourceLimits = new ResourceLimits(clusterResource);
			res = a.AssignToUser(clusterResource, user_0, limit, app_0, null, userResourceLimits
				);
			NUnit.Framework.Assert.IsTrue(res);
			// limit set to 12GB, we are using 13GB (8 allocated,  5 reserved), to get under limit
			// we need to unreserve 1GB
			// also note vcore checks not enabled
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(1 * Gb, 4), userResourceLimits
				.GetAmountNeededUnreserve());
			RefreshQueuesTurnOffReservationsContLook(a, csConf);
			userResourceLimits = new ResourceLimits(clusterResource);
			// should now return false since feature off
			res = a.AssignToUser(clusterResource, user_0, limit, app_0, null, userResourceLimits
				);
			NUnit.Framework.Assert.IsFalse(res);
			NUnit.Framework.Assert.AreEqual(Resources.None(), userResourceLimits.GetAmountNeededUnreserve
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservationsNoneAvailable()
		{
			// Test that we now unreserve and use a node that has space
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			Setup(csConf);
			// Manipulate queue 'a'
			LeafQueue a = StubLeafQueue((LeafQueue)queues[A]);
			// Users
			string user_0 = "user_0";
			// Submit applications
			ApplicationAttemptId appAttemptId_0 = TestUtils.GetMockApplicationAttemptId(0, 0);
			FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			rmContext.GetRMApps()[app_0.GetApplicationId()] = Org.Mockito.Mockito.Mock<RMApp>
				();
			a.SubmitApplicationAttempt(app_0, user_0);
			ApplicationAttemptId appAttemptId_1 = TestUtils.GetMockApplicationAttemptId(1, 0);
			FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a, Org.Mockito.Mockito.Mock
				<ActiveUsersManager>(), spyRMContext);
			a.SubmitApplicationAttempt(app_1, user_0);
			// Setup some nodes
			string host_0 = "host_0";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, DefaultRack, 0, 8 * Gb);
			string host_1 = "host_1";
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode(host_1, DefaultRack, 0, 8 * Gb);
			string host_2 = "host_2";
			FiCaSchedulerNode node_2 = TestUtils.GetMockNode(host_2, DefaultRack, 0, 8 * Gb);
			Org.Mockito.Mockito.When(csContext.GetNode(node_0.GetNodeID())).ThenReturn(node_0
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_1.GetNodeID())).ThenReturn(node_1
				);
			Org.Mockito.Mockito.When(csContext.GetNode(node_2.GetNodeID())).ThenReturn(node_2
				);
			int numNodes = 3;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (8 * Gb));
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Setup resource-requests
			Priority priorityAM = TestUtils.CreateMockPriority(1);
			Priority priorityMap = TestUtils.CreateMockPriority(5);
			Priority priorityReduce = TestUtils.CreateMockPriority(10);
			Priority priorityLast = TestUtils.CreateMockPriority(12);
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 2 * Gb, 1, true, priorityAM, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 3 * Gb, 2, true, priorityMap, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 5 * Gb, 1, true, priorityReduce, recordFactory)));
			app_0.UpdateResourceRequests(Sharpen.Collections.SingletonList(TestUtils.CreateResourceRequest
				(ResourceRequest.Any, 8 * Gb, 2, true, priorityLast, recordFactory)));
			// Start testing...
			// Only AM
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(22 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(2 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			// Only 1 map - simulating reduce
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(19 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			// Only 1 map to other node - simulating reduce
			a.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(16 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(16 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			// try to assign reducer (5G on node 0), but tell it's resource limits <
			// used (8G) + required (5G). It will not reserved since it has to unreserve
			// some resource. Even with continous reservation looking, we don't allow 
			// unreserve resource to reserve container.
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(Resources.CreateResource
				(10 * Gb)));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(16 * Gb, a.GetMetrics().GetAvailableMB());
			// app_0's headroom = limit (10G) - used (8G) = 2G 
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			// try to assign reducer (5G on node 0), but tell it's resource limits <
			// used (8G) + required (5G). It will not reserved since it has to unreserve
			// some resource. Unfortunately, there's nothing to unreserve.
			a.AssignContainers(clusterResource, node_2, new ResourceLimits(Resources.CreateResource
				(10 * Gb)));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(8 * Gb, app_0.GetCurrentConsumption().GetMemory()
				);
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(16 * Gb, a.GetMetrics().GetAvailableMB());
			// app_0's headroom = limit (10G) - used (8G) = 2G 
			NUnit.Framework.Assert.AreEqual(2 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(0 * Gb, node_2.GetUsedResource().GetMemory());
			// let it assign 5G to node_2
			a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(13 * Gb, app_0.GetCurrentConsumption().GetMemory(
				));
			NUnit.Framework.Assert.AreEqual(0 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(11 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(11 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_2.GetUsedResource().GetMemory());
			// reserve 8G node_0
			a.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(21 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(13 * Gb, app_0.GetCurrentConsumption().GetMemory(
				));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_2.GetUsedResource().GetMemory());
			// try to assign (8G on node 2). No room to allocate,
			// continued to try due to having reservation above,
			// but hits queue limits so can't reserve anymore.
			a.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(21 * Gb, a.GetUsedResources().GetMemory());
			NUnit.Framework.Assert.AreEqual(13 * Gb, app_0.GetCurrentConsumption().GetMemory(
				));
			NUnit.Framework.Assert.AreEqual(8 * Gb, a.GetMetrics().GetReservedMB());
			NUnit.Framework.Assert.AreEqual(13 * Gb, a.GetMetrics().GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, a.GetMetrics().GetAvailableMB());
			NUnit.Framework.Assert.AreEqual(3 * Gb, app_0.GetHeadroom().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_0.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * Gb, node_1.GetUsedResource().GetMemory());
			NUnit.Framework.Assert.AreEqual(5 * Gb, node_2.GetUsedResource().GetMemory());
		}
	}
}
