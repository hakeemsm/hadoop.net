using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestChildQueueOrder
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestChildQueueOrder));

		internal RMContext rmContext;

		internal YarnConfiguration conf;

		internal CapacitySchedulerConfiguration csConf;

		internal CapacitySchedulerContext csContext;

		internal const int Gb = 1024;

		internal const string DefaultRack = "/default";

		private readonly ResourceCalculator resourceComparator = new DefaultResourceCalculator
			();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			rmContext = TestUtils.GetMockRMContext();
			conf = new YarnConfiguration();
			csConf = new CapacitySchedulerConfiguration();
			csContext = Org.Mockito.Mockito.Mock<CapacitySchedulerContext>();
			Org.Mockito.Mockito.When(csContext.GetConf()).ThenReturn(conf);
			Org.Mockito.Mockito.When(csContext.GetConfiguration()).ThenReturn(csConf);
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
			Org.Mockito.Mockito.When(csContext.GetResourceCalculator()).ThenReturn(resourceComparator
				);
			Org.Mockito.Mockito.When(csContext.GetRMContext()).ThenReturn(rmContext);
		}

		private FiCaSchedulerApp GetMockApplication(int appId, string user)
		{
			FiCaSchedulerApp application = Org.Mockito.Mockito.Mock<FiCaSchedulerApp>();
			Org.Mockito.Mockito.DoReturn(user).When(application).GetUser();
			Org.Mockito.Mockito.DoReturn(Resources.CreateResource(0, 0)).When(application).GetHeadroom
				();
			return application;
		}

		private void StubQueueAllocation(CSQueue queue, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, FiCaSchedulerNode node, int allocation)
		{
			StubQueueAllocation(queue, clusterResource, node, allocation, NodeType.NodeLocal);
		}

		private void StubQueueAllocation(CSQueue queue, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, FiCaSchedulerNode node, int allocation, NodeType type)
		{
			// Simulate the queue allocation
			Org.Mockito.Mockito.DoAnswer(new _Answer_124(this, queue, allocation, node, clusterResource
				, type)).When(queue).AssignContainers(Matchers.Eq(clusterResource), Matchers.Eq(
				node), Matchers.Any<ResourceLimits>());
			// Next call - nothing
			// Mock the node's resource availability
			Org.Mockito.Mockito.DoNothing().When(node).ReleaseContainer(Matchers.Any<Container
				>());
		}

		private sealed class _Answer_124 : Answer<CSAssignment>
		{
			public _Answer_124(TestChildQueueOrder _enclosing, CSQueue queue, int allocation, 
				FiCaSchedulerNode node, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
				, NodeType type)
			{
				this._enclosing = _enclosing;
				this.queue = queue;
				this.allocation = allocation;
				this.node = node;
				this.clusterResource = clusterResource;
				this.type = type;
			}

			/// <exception cref="System.Exception"/>
			public CSAssignment Answer(InvocationOnMock invocation)
			{
				try
				{
					throw new Exception();
				}
				catch (Exception)
				{
					TestChildQueueOrder.Log.Info("FOOBAR q.assignContainers q=" + queue.GetQueueName(
						) + " alloc=" + allocation + " node=" + node.GetNodeName());
				}
				Org.Apache.Hadoop.Yarn.Api.Records.Resource allocatedResource = Resources.CreateResource
					(allocation);
				if (queue is ParentQueue)
				{
					((ParentQueue)queue).AllocateResource(clusterResource, allocatedResource, null);
				}
				else
				{
					FiCaSchedulerApp app1 = this._enclosing.GetMockApplication(0, string.Empty);
					((LeafQueue)queue).AllocateResource(clusterResource, app1, allocatedResource, null
						);
				}
				if (allocation > 0)
				{
					Org.Mockito.Mockito.DoReturn(new CSAssignment(Resources.None(), type)).When(queue
						).AssignContainers(Matchers.Eq(clusterResource), Matchers.Eq(node), Matchers.Any
						<ResourceLimits>());
					Org.Apache.Hadoop.Yarn.Api.Records.Resource available = node.GetAvailableResource
						();
					Org.Mockito.Mockito.DoReturn(Resources.SubtractFrom(available, allocatedResource)
						).When(node).GetAvailableResource();
				}
				return new CSAssignment(allocatedResource, type);
			}

			private readonly TestChildQueueOrder _enclosing;

			private readonly CSQueue queue;

			private readonly int allocation;

			private readonly FiCaSchedulerNode node;

			private readonly Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource;

			private readonly NodeType type;
		}

		private float ComputeQueueAbsoluteUsedCapacity(CSQueue queue, int expectedMemory, 
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource)
		{
			return (((float)expectedMemory / (float)clusterResource.GetMemory()));
		}

		private float ComputeQueueUsedCapacity(CSQueue queue, int expectedMemory, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			return (expectedMemory / (clusterResource.GetMemory() * queue.GetAbsoluteCapacity
				()));
		}

		internal const float Delta = 0.0001f;

		private void VerifyQueueMetrics(CSQueue queue, int expectedMemory, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			NUnit.Framework.Assert.AreEqual(ComputeQueueAbsoluteUsedCapacity(queue, expectedMemory
				, clusterResource), queue.GetAbsoluteUsedCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(ComputeQueueUsedCapacity(queue, expectedMemory, clusterResource
				), queue.GetUsedCapacity(), Delta);
		}

		private const string A = "a";

		private const string B = "b";

		private const string C = "c";

		private const string D = "d";

		private void SetupSortedQueues(CapacitySchedulerConfiguration conf)
		{
			// Define queues
			csConf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { A, B, C, D }
				);
			string QA = CapacitySchedulerConfiguration.Root + "." + A;
			conf.SetCapacity(QA, 25);
			string QB = CapacitySchedulerConfiguration.Root + "." + B;
			conf.SetCapacity(QB, 25);
			string QC = CapacitySchedulerConfiguration.Root + "." + C;
			conf.SetCapacity(QC, 25);
			string QD = CapacitySchedulerConfiguration.Root + "." + D;
			conf.SetCapacity(QD, 25);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSortedQueues()
		{
			// Setup queue configs
			SetupSortedQueues(csConf);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CSQueue root = CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, queues, queues, TestUtils.spyHook);
			// Setup some nodes
			int memoryPerNode = 10;
			int coresPerNode = 16;
			int numNodes = 1;
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode("host_0", DefaultRack, 0, memoryPerNode
				 * Gb);
			Org.Mockito.Mockito.DoNothing().When(node_0).ReleaseContainer(Matchers.Any<Container
				>());
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (memoryPerNode * Gb), numNodes * coresPerNode);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Start testing
			CSQueue a = queues[A];
			CSQueue b = queues[B];
			CSQueue c = queues[C];
			CSQueue d = queues[D];
			string user_0 = "user_0";
			// Stub an App and its containerCompleted
			FiCaSchedulerApp app_0 = GetMockApplication(0, user_0);
			Org.Mockito.Mockito.DoReturn(true).When(app_0).ContainerCompleted(Matchers.Any<RMContainer
				>(), Matchers.Any<ContainerStatus>(), Matchers.Any<RMContainerEventType>());
			Priority priority = TestUtils.CreateMockPriority(1);
			ContainerAllocationExpirer expirer = Org.Mockito.Mockito.Mock<ContainerAllocationExpirer
				>();
			DrainDispatcher drainDispatcher = new DrainDispatcher();
			RMApplicationHistoryWriter writer = Org.Mockito.Mockito.Mock<RMApplicationHistoryWriter
				>();
			SystemMetricsPublisher publisher = Org.Mockito.Mockito.Mock<SystemMetricsPublisher
				>();
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
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
			Container container = TestUtils.GetMockContainer(containerId, node_0.GetNodeID(), 
				Resources.CreateResource(1 * Gb), priority);
			RMContainer rmContainer = new RMContainerImpl(container, appAttemptId, node_0.GetNodeID
				(), "user", rmContext);
			// Assign {1,2,3,4} 1GB containers respectively to queues
			StubQueueAllocation(a, clusterResource, node_0, 1 * Gb);
			StubQueueAllocation(b, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(c, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(d, clusterResource, node_0, 0 * Gb);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			for (int i = 0; i < 2; i++)
			{
				StubQueueAllocation(a, clusterResource, node_0, 0 * Gb);
				StubQueueAllocation(b, clusterResource, node_0, 1 * Gb);
				StubQueueAllocation(c, clusterResource, node_0, 0 * Gb);
				StubQueueAllocation(d, clusterResource, node_0, 0 * Gb);
				root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
					));
			}
			for (int i_1 = 0; i_1 < 3; i_1++)
			{
				StubQueueAllocation(a, clusterResource, node_0, 0 * Gb);
				StubQueueAllocation(b, clusterResource, node_0, 0 * Gb);
				StubQueueAllocation(c, clusterResource, node_0, 1 * Gb);
				StubQueueAllocation(d, clusterResource, node_0, 0 * Gb);
				root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
					));
			}
			for (int i_2 = 0; i_2 < 4; i_2++)
			{
				StubQueueAllocation(a, clusterResource, node_0, 0 * Gb);
				StubQueueAllocation(b, clusterResource, node_0, 0 * Gb);
				StubQueueAllocation(c, clusterResource, node_0, 0 * Gb);
				StubQueueAllocation(d, clusterResource, node_0, 1 * Gb);
				root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
					));
			}
			VerifyQueueMetrics(a, 1 * Gb, clusterResource);
			VerifyQueueMetrics(b, 2 * Gb, clusterResource);
			VerifyQueueMetrics(c, 3 * Gb, clusterResource);
			VerifyQueueMetrics(d, 4 * Gb, clusterResource);
			Log.Info("status child-queues: " + ((ParentQueue)root).GetChildQueuesToPrint());
			//Release 3 x 1GB containers from D
			for (int i_3 = 0; i_3 < 3; i_3++)
			{
				d.CompletedContainer(clusterResource, app_0, node_0, rmContainer, null, RMContainerEventType
					.Kill, null, true);
			}
			VerifyQueueMetrics(a, 1 * Gb, clusterResource);
			VerifyQueueMetrics(b, 2 * Gb, clusterResource);
			VerifyQueueMetrics(c, 3 * Gb, clusterResource);
			VerifyQueueMetrics(d, 1 * Gb, clusterResource);
			//reset manually resources on node
			node_0 = TestUtils.GetMockNode("host_0", DefaultRack, 0, (memoryPerNode - 1 - 2 -
				 3 - 1) * Gb);
			Log.Info("status child-queues: " + ((ParentQueue)root).GetChildQueuesToPrint());
			// Assign 2 x 1GB Containers to A 
			for (int i_4 = 0; i_4 < 2; i_4++)
			{
				StubQueueAllocation(a, clusterResource, node_0, 1 * Gb);
				StubQueueAllocation(b, clusterResource, node_0, 0 * Gb);
				StubQueueAllocation(c, clusterResource, node_0, 0 * Gb);
				StubQueueAllocation(d, clusterResource, node_0, 0 * Gb);
				root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
					));
			}
			VerifyQueueMetrics(a, 3 * Gb, clusterResource);
			VerifyQueueMetrics(b, 2 * Gb, clusterResource);
			VerifyQueueMetrics(c, 3 * Gb, clusterResource);
			VerifyQueueMetrics(d, 1 * Gb, clusterResource);
			Log.Info("status child-queues: " + ((ParentQueue)root).GetChildQueuesToPrint());
			//Release 1GB Container from A
			a.CompletedContainer(clusterResource, app_0, node_0, rmContainer, null, RMContainerEventType
				.Kill, null, true);
			VerifyQueueMetrics(a, 2 * Gb, clusterResource);
			VerifyQueueMetrics(b, 2 * Gb, clusterResource);
			VerifyQueueMetrics(c, 3 * Gb, clusterResource);
			VerifyQueueMetrics(d, 1 * Gb, clusterResource);
			//reset manually resources on node
			node_0 = TestUtils.GetMockNode("host_0", DefaultRack, 0, (memoryPerNode - 2 - 2 -
				 3 - 1) * Gb);
			Log.Info("status child-queues: " + ((ParentQueue)root).GetChildQueuesToPrint());
			// Assign 1GB container to B 
			StubQueueAllocation(a, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(b, clusterResource, node_0, 1 * Gb);
			StubQueueAllocation(c, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(d, clusterResource, node_0, 0 * Gb);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			VerifyQueueMetrics(a, 2 * Gb, clusterResource);
			VerifyQueueMetrics(b, 3 * Gb, clusterResource);
			VerifyQueueMetrics(c, 3 * Gb, clusterResource);
			VerifyQueueMetrics(d, 1 * Gb, clusterResource);
			Log.Info("status child-queues: " + ((ParentQueue)root).GetChildQueuesToPrint());
			//Release 1GB container resources from B
			b.CompletedContainer(clusterResource, app_0, node_0, rmContainer, null, RMContainerEventType
				.Kill, null, true);
			VerifyQueueMetrics(a, 2 * Gb, clusterResource);
			VerifyQueueMetrics(b, 2 * Gb, clusterResource);
			VerifyQueueMetrics(c, 3 * Gb, clusterResource);
			VerifyQueueMetrics(d, 1 * Gb, clusterResource);
			//reset manually resources on node
			node_0 = TestUtils.GetMockNode("host_0", DefaultRack, 0, (memoryPerNode - 2 - 2 -
				 3 - 1) * Gb);
			Log.Info("status child-queues: " + ((ParentQueue)root).GetChildQueuesToPrint());
			// Assign 1GB container to A
			StubQueueAllocation(a, clusterResource, node_0, 1 * Gb);
			StubQueueAllocation(b, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(c, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(d, clusterResource, node_0, 0 * Gb);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			VerifyQueueMetrics(a, 3 * Gb, clusterResource);
			VerifyQueueMetrics(b, 2 * Gb, clusterResource);
			VerifyQueueMetrics(c, 3 * Gb, clusterResource);
			VerifyQueueMetrics(d, 1 * Gb, clusterResource);
			Log.Info("status child-queues: " + ((ParentQueue)root).GetChildQueuesToPrint());
			// Now do the real test, where B and D request a 1GB container
			// D should should get the next container if the order is correct
			StubQueueAllocation(a, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(b, clusterResource, node_0, 1 * Gb);
			StubQueueAllocation(c, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(d, clusterResource, node_0, 1 * Gb);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			InOrder allocationOrder = Org.Mockito.Mockito.InOrder(d, b);
			allocationOrder.Verify(d).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), Matchers.Any<ResourceLimits>());
			allocationOrder.Verify(b).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), Matchers.Any<ResourceLimits>());
			VerifyQueueMetrics(a, 3 * Gb, clusterResource);
			VerifyQueueMetrics(b, 2 * Gb, clusterResource);
			VerifyQueueMetrics(c, 3 * Gb, clusterResource);
			VerifyQueueMetrics(d, 2 * Gb, clusterResource);
			//D got the container
			Log.Info("status child-queues: " + ((ParentQueue)root).GetChildQueuesToPrint());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
		}
	}
}
