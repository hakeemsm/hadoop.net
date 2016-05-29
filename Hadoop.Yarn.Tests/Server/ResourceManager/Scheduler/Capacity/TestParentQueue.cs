using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestParentQueue
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestParentQueue));

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

		private const string A = "a";

		private const string B = "b";

		private void SetupSingleLevelQueues(CapacitySchedulerConfiguration conf)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { A, B });
			string QA = CapacitySchedulerConfiguration.Root + "." + A;
			conf.SetCapacity(QA, 30);
			string QB = CapacitySchedulerConfiguration.Root + "." + B;
			conf.SetCapacity(QB, 70);
			Log.Info("Setup top-level queues a and b");
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
			Org.Mockito.Mockito.DoAnswer(new _Answer_137(this, queue, allocation, node, clusterResource
				, type)).When(queue).AssignContainers(Matchers.Eq(clusterResource), Matchers.Eq(
				node), Matchers.Any<ResourceLimits>());
		}

		private sealed class _Answer_137 : Answer<CSAssignment>
		{
			public _Answer_137(TestParentQueue _enclosing, CSQueue queue, int allocation, FiCaSchedulerNode
				 node, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource, NodeType type
				)
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
					TestParentQueue.Log.Info("FOOBAR q.assignContainers q=" + queue.GetQueueName() + 
						" alloc=" + allocation + " node=" + node.GetNodeName());
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
				// Next call - nothing
				if (allocation > 0)
				{
					Org.Mockito.Mockito.DoReturn(new CSAssignment(Resources.None(), type)).When(queue
						).AssignContainers(Matchers.Eq(clusterResource), Matchers.Eq(node), Matchers.Any
						<ResourceLimits>());
					// Mock the node's resource availability
					Org.Apache.Hadoop.Yarn.Api.Records.Resource available = node.GetAvailableResource
						();
					Org.Mockito.Mockito.DoReturn(Resources.SubtractFrom(available, allocatedResource)
						).When(node).GetAvailableResource();
				}
				return new CSAssignment(allocatedResource, type);
			}

			private readonly TestParentQueue _enclosing;

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

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleLevelQueues()
		{
			// Setup queue configs
			SetupSingleLevelQueues(csConf);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CSQueue root = CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, queues, queues, TestUtils.spyHook);
			// Setup some nodes
			int memoryPerNode = 10;
			int coresPerNode = 16;
			int numNodes = 2;
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode("host_0", DefaultRack, 0, memoryPerNode
				 * Gb);
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode("host_1", DefaultRack, 0, memoryPerNode
				 * Gb);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (memoryPerNode * Gb), numNodes * coresPerNode);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Start testing
			LeafQueue a = (LeafQueue)queues[A];
			LeafQueue b = (LeafQueue)queues[B];
			// Simulate B returning a container on node_0
			StubQueueAllocation(a, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(b, clusterResource, node_0, 1 * Gb);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			VerifyQueueMetrics(a, 0 * Gb, clusterResource);
			VerifyQueueMetrics(b, 1 * Gb, clusterResource);
			// Now, A should get the scheduling opportunity since A=0G/6G, B=1G/14G
			StubQueueAllocation(a, clusterResource, node_1, 2 * Gb);
			StubQueueAllocation(b, clusterResource, node_1, 1 * Gb);
			root.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource
				));
			InOrder allocationOrder = Org.Mockito.Mockito.InOrder(a, b);
			allocationOrder.Verify(a).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(b).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			VerifyQueueMetrics(a, 2 * Gb, clusterResource);
			VerifyQueueMetrics(b, 2 * Gb, clusterResource);
			// Now, B should get the scheduling opportunity 
			// since A has 2/6G while B has 2/14G
			StubQueueAllocation(a, clusterResource, node_0, 1 * Gb);
			StubQueueAllocation(b, clusterResource, node_0, 2 * Gb);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			allocationOrder = Org.Mockito.Mockito.InOrder(b, a);
			allocationOrder.Verify(b).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(a).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			VerifyQueueMetrics(a, 3 * Gb, clusterResource);
			VerifyQueueMetrics(b, 4 * Gb, clusterResource);
			// Now, B should still get the scheduling opportunity 
			// since A has 3/6G while B has 4/14G
			StubQueueAllocation(a, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(b, clusterResource, node_0, 4 * Gb);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			allocationOrder = Org.Mockito.Mockito.InOrder(b, a);
			allocationOrder.Verify(b).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(a).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			VerifyQueueMetrics(a, 3 * Gb, clusterResource);
			VerifyQueueMetrics(b, 8 * Gb, clusterResource);
			// Now, A should get the scheduling opportunity 
			// since A has 3/6G while B has 8/14G
			StubQueueAllocation(a, clusterResource, node_1, 1 * Gb);
			StubQueueAllocation(b, clusterResource, node_1, 1 * Gb);
			root.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource
				));
			allocationOrder = Org.Mockito.Mockito.InOrder(a, b);
			allocationOrder.Verify(b).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(a).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			VerifyQueueMetrics(a, 4 * Gb, clusterResource);
			VerifyQueueMetrics(b, 9 * Gb, clusterResource);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleLevelQueuesPrecision()
		{
			// Setup queue configs
			SetupSingleLevelQueues(csConf);
			string QA = CapacitySchedulerConfiguration.Root + "." + "a";
			csConf.SetCapacity(QA, 30);
			string QB = CapacitySchedulerConfiguration.Root + "." + "b";
			csConf.SetCapacity(QB, 70.5F);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			bool exceptionOccured = false;
			try
			{
				CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
					.Root, queues, queues, TestUtils.spyHook);
			}
			catch (ArgumentException)
			{
				exceptionOccured = true;
			}
			if (!exceptionOccured)
			{
				NUnit.Framework.Assert.Fail("Capacity is more then 100% so should be failed.");
			}
			csConf.SetCapacity(QA, 30);
			csConf.SetCapacity(QB, 70);
			exceptionOccured = false;
			queues.Clear();
			try
			{
				CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
					.Root, queues, queues, TestUtils.spyHook);
			}
			catch (ArgumentException)
			{
				exceptionOccured = true;
			}
			if (exceptionOccured)
			{
				NUnit.Framework.Assert.Fail("Capacity is 100% so should not be failed.");
			}
			csConf.SetCapacity(QA, 30);
			csConf.SetCapacity(QB, 70.005F);
			exceptionOccured = false;
			queues.Clear();
			try
			{
				CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
					.Root, queues, queues, TestUtils.spyHook);
			}
			catch (ArgumentException)
			{
				exceptionOccured = true;
			}
			if (exceptionOccured)
			{
				NUnit.Framework.Assert.Fail("Capacity is under PRECISION which is .05% so should not be failed."
					);
			}
		}

		private const string C = "c";

		private const string C1 = "c1";

		private const string C11 = "c11";

		private const string C111 = "c111";

		private const string C1111 = "c1111";

		private const string D = "d";

		private const string A1 = "a1";

		private const string A2 = "a2";

		private const string B1 = "b1";

		private const string B2 = "b2";

		private const string B3 = "b3";

		private void SetupMultiLevelQueues(CapacitySchedulerConfiguration conf)
		{
			// Define top-level queues
			csConf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { A, B, C, D }
				);
			string QA = CapacitySchedulerConfiguration.Root + "." + A;
			conf.SetCapacity(QA, 10);
			string QB = CapacitySchedulerConfiguration.Root + "." + B;
			conf.SetCapacity(QB, 50);
			string QC = CapacitySchedulerConfiguration.Root + "." + C;
			conf.SetCapacity(QC, 19.5f);
			string QD = CapacitySchedulerConfiguration.Root + "." + D;
			conf.SetCapacity(QD, 20.5f);
			// Define 2-nd level queues
			conf.SetQueues(QA, new string[] { A1, A2 });
			conf.SetCapacity(QA + "." + A1, 50);
			conf.SetCapacity(QA + "." + A2, 50);
			conf.SetQueues(QB, new string[] { B1, B2, B3 });
			conf.SetCapacity(QB + "." + B1, 10);
			conf.SetCapacity(QB + "." + B2, 20);
			conf.SetCapacity(QB + "." + B3, 70);
			conf.SetQueues(QC, new string[] { C1 });
			string QC1 = QC + "." + C1;
			conf.SetCapacity(QC1, 100);
			conf.SetQueues(QC1, new string[] { C11 });
			string QC11 = QC1 + "." + C11;
			conf.SetCapacity(QC11, 100);
			conf.SetQueues(QC11, new string[] { C111 });
			string QC111 = QC11 + "." + C111;
			conf.SetCapacity(QC111, 100);
			//Leaf Queue
			conf.SetQueues(QC111, new string[] { C1111 });
			string QC1111 = QC111 + "." + C1111;
			conf.SetCapacity(QC1111, 100);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiLevelQueues()
		{
			/*
			* Structure of queue:
			*            Root
			*           ____________
			*          /    |   \   \
			*         A     B    C   D
			*       / |   / | \   \
			*      A1 A2 B1 B2 B3  C1
			*                        \
			*                         C11
			*                           \
			*                           C111
			*                             \
			*                              C1111
			*/
			// Setup queue configs
			SetupMultiLevelQueues(csConf);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CSQueue root = CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, queues, queues, TestUtils.spyHook);
			// Setup some nodes
			int memoryPerNode = 10;
			int coresPerNode = 16;
			int numNodes = 3;
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode("host_0", DefaultRack, 0, memoryPerNode
				 * Gb);
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode("host_1", DefaultRack, 0, memoryPerNode
				 * Gb);
			FiCaSchedulerNode node_2 = TestUtils.GetMockNode("host_2", DefaultRack, 0, memoryPerNode
				 * Gb);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (memoryPerNode * Gb), numNodes * coresPerNode);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Start testing
			CSQueue a = queues[A];
			CSQueue b = queues[B];
			CSQueue c = queues[C];
			CSQueue d = queues[D];
			CSQueue a1 = queues[A1];
			CSQueue a2 = queues[A2];
			CSQueue b1 = queues[B1];
			CSQueue b2 = queues[B2];
			CSQueue b3 = queues[B3];
			// Simulate C returning a container on node_0
			StubQueueAllocation(a, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(b, clusterResource, node_0, 0 * Gb);
			StubQueueAllocation(c, clusterResource, node_0, 1 * Gb);
			StubQueueAllocation(d, clusterResource, node_0, 0 * Gb);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			VerifyQueueMetrics(a, 0 * Gb, clusterResource);
			VerifyQueueMetrics(b, 0 * Gb, clusterResource);
			VerifyQueueMetrics(c, 1 * Gb, clusterResource);
			VerifyQueueMetrics(d, 0 * Gb, clusterResource);
			Org.Mockito.Mockito.Reset(a);
			Org.Mockito.Mockito.Reset(b);
			Org.Mockito.Mockito.Reset(c);
			// Now get B2 to allocate
			// A = 0/3, B = 0/15, C = 1/6, D=0/6
			StubQueueAllocation(a, clusterResource, node_1, 0 * Gb);
			StubQueueAllocation(b2, clusterResource, node_1, 4 * Gb);
			StubQueueAllocation(c, clusterResource, node_1, 0 * Gb);
			root.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource
				));
			VerifyQueueMetrics(a, 0 * Gb, clusterResource);
			VerifyQueueMetrics(b, 4 * Gb, clusterResource);
			VerifyQueueMetrics(c, 1 * Gb, clusterResource);
			Org.Mockito.Mockito.Reset(a);
			Org.Mockito.Mockito.Reset(b);
			Org.Mockito.Mockito.Reset(c);
			// Now get both A1, C & B3 to allocate in right order
			// A = 0/3, B = 4/15, C = 1/6, D=0/6
			StubQueueAllocation(a1, clusterResource, node_0, 1 * Gb);
			StubQueueAllocation(b3, clusterResource, node_0, 2 * Gb);
			StubQueueAllocation(c, clusterResource, node_0, 2 * Gb);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			InOrder allocationOrder = Org.Mockito.Mockito.InOrder(a, c, b);
			allocationOrder.Verify(a).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(c).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(b).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			VerifyQueueMetrics(a, 1 * Gb, clusterResource);
			VerifyQueueMetrics(b, 6 * Gb, clusterResource);
			VerifyQueueMetrics(c, 3 * Gb, clusterResource);
			Org.Mockito.Mockito.Reset(a);
			Org.Mockito.Mockito.Reset(b);
			Org.Mockito.Mockito.Reset(c);
			// Now verify max-capacity
			// A = 1/3, B = 6/15, C = 3/6, D=0/6
			// Ensure a1 won't alloc above max-cap although it should get 
			// scheduling opportunity now, right after a2
			Log.Info("here");
			((ParentQueue)a).SetMaxCapacity(.1f);
			// a should be capped at 3/30
			StubQueueAllocation(a1, clusterResource, node_2, 1 * Gb);
			// shouldn't be 
			// allocated due 
			// to max-cap
			StubQueueAllocation(a2, clusterResource, node_2, 2 * Gb);
			StubQueueAllocation(b3, clusterResource, node_2, 1 * Gb);
			StubQueueAllocation(b1, clusterResource, node_2, 1 * Gb);
			StubQueueAllocation(c, clusterResource, node_2, 1 * Gb);
			root.AssignContainers(clusterResource, node_2, new ResourceLimits(clusterResource
				));
			allocationOrder = Org.Mockito.Mockito.InOrder(a, a2, a1, b, c);
			allocationOrder.Verify(a).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(a2).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(b).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(c).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			VerifyQueueMetrics(a, 3 * Gb, clusterResource);
			VerifyQueueMetrics(b, 8 * Gb, clusterResource);
			VerifyQueueMetrics(c, 4 * Gb, clusterResource);
			Org.Mockito.Mockito.Reset(a);
			Org.Mockito.Mockito.Reset(b);
			Org.Mockito.Mockito.Reset(c);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQueueCapacitySettingChildZero()
		{
			// Setup queue configs
			SetupMultiLevelQueues(csConf);
			// set child queues capacity to 0 when parents not 0
			string QB = CapacitySchedulerConfiguration.Root + "." + B;
			csConf.SetCapacity(QB + "." + B1, 0);
			csConf.SetCapacity(QB + "." + B2, 0);
			csConf.SetCapacity(QB + "." + B3, 0);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, queues, queues, TestUtils.spyHook);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQueueCapacitySettingParentZero()
		{
			// Setup queue configs
			SetupMultiLevelQueues(csConf);
			// set parent capacity to 0 when child not 0
			string QB = CapacitySchedulerConfiguration.Root + "." + B;
			csConf.SetCapacity(QB, 0);
			string QA = CapacitySchedulerConfiguration.Root + "." + A;
			csConf.SetCapacity(QA, 60);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, queues, queues, TestUtils.spyHook);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueCapacityZero()
		{
			// Setup queue configs
			SetupMultiLevelQueues(csConf);
			// set parent and child capacity to 0
			string QB = CapacitySchedulerConfiguration.Root + "." + B;
			csConf.SetCapacity(QB, 0);
			csConf.SetCapacity(QB + "." + B1, 0);
			csConf.SetCapacity(QB + "." + B2, 0);
			csConf.SetCapacity(QB + "." + B3, 0);
			string QA = CapacitySchedulerConfiguration.Root + "." + A;
			csConf.SetCapacity(QA, 60);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			try
			{
				CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
					.Root, queues, queues, TestUtils.spyHook);
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.Fail("Failed to create queues with 0 capacity: " + e);
			}
			NUnit.Framework.Assert.IsTrue("Failed to create queues with 0 capacity", true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOffSwitchScheduling()
		{
			// Setup queue configs
			SetupSingleLevelQueues(csConf);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CSQueue root = CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, queues, queues, TestUtils.spyHook);
			// Setup some nodes
			int memoryPerNode = 10;
			int coresPerNode = 16;
			int numNodes = 2;
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode("host_0", DefaultRack, 0, memoryPerNode
				 * Gb);
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode("host_1", DefaultRack, 0, memoryPerNode
				 * Gb);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (memoryPerNode * Gb), numNodes * coresPerNode);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Start testing
			LeafQueue a = (LeafQueue)queues[A];
			LeafQueue b = (LeafQueue)queues[B];
			// Simulate B returning a container on node_0
			StubQueueAllocation(a, clusterResource, node_0, 0 * Gb, NodeType.OffSwitch);
			StubQueueAllocation(b, clusterResource, node_0, 1 * Gb, NodeType.OffSwitch);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			VerifyQueueMetrics(a, 0 * Gb, clusterResource);
			VerifyQueueMetrics(b, 1 * Gb, clusterResource);
			// Now, A should get the scheduling opportunity since A=0G/6G, B=1G/14G
			// also, B gets a scheduling opportunity since A allocates RACK_LOCAL
			StubQueueAllocation(a, clusterResource, node_1, 2 * Gb, NodeType.RackLocal);
			StubQueueAllocation(b, clusterResource, node_1, 1 * Gb, NodeType.OffSwitch);
			root.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource
				));
			InOrder allocationOrder = Org.Mockito.Mockito.InOrder(a, b);
			allocationOrder.Verify(a).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(b).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			VerifyQueueMetrics(a, 2 * Gb, clusterResource);
			VerifyQueueMetrics(b, 2 * Gb, clusterResource);
			// Now, B should get the scheduling opportunity 
			// since A has 2/6G while B has 2/14G, 
			// However, since B returns off-switch, A won't get an opportunity
			StubQueueAllocation(a, clusterResource, node_0, 1 * Gb, NodeType.NodeLocal);
			StubQueueAllocation(b, clusterResource, node_0, 2 * Gb, NodeType.OffSwitch);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			allocationOrder = Org.Mockito.Mockito.InOrder(b, a);
			allocationOrder.Verify(b).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(a).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			VerifyQueueMetrics(a, 2 * Gb, clusterResource);
			VerifyQueueMetrics(b, 4 * Gb, clusterResource);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOffSwitchSchedulingMultiLevelQueues()
		{
			// Setup queue configs
			SetupMultiLevelQueues(csConf);
			//B3
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CSQueue root = CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, queues, queues, TestUtils.spyHook);
			// Setup some nodes
			int memoryPerNode = 10;
			int coresPerNode = 10;
			int numNodes = 2;
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode("host_0", DefaultRack, 0, memoryPerNode
				 * Gb);
			FiCaSchedulerNode node_1 = TestUtils.GetMockNode("host_1", DefaultRack, 0, memoryPerNode
				 * Gb);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(numNodes * (memoryPerNode * Gb), numNodes * coresPerNode);
			Org.Mockito.Mockito.When(csContext.GetNumClusterNodes()).ThenReturn(numNodes);
			// Start testing
			LeafQueue b3 = (LeafQueue)queues[B3];
			LeafQueue b2 = (LeafQueue)queues[B2];
			// Simulate B3 returning a container on node_0
			StubQueueAllocation(b2, clusterResource, node_0, 0 * Gb, NodeType.OffSwitch);
			StubQueueAllocation(b3, clusterResource, node_0, 1 * Gb, NodeType.OffSwitch);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			VerifyQueueMetrics(b2, 0 * Gb, clusterResource);
			VerifyQueueMetrics(b3, 1 * Gb, clusterResource);
			// Now, B2 should get the scheduling opportunity since B2=0G/2G, B3=1G/7G
			// also, B3 gets a scheduling opportunity since B2 allocates RACK_LOCAL
			StubQueueAllocation(b2, clusterResource, node_1, 1 * Gb, NodeType.RackLocal);
			StubQueueAllocation(b3, clusterResource, node_1, 1 * Gb, NodeType.OffSwitch);
			root.AssignContainers(clusterResource, node_1, new ResourceLimits(clusterResource
				));
			InOrder allocationOrder = Org.Mockito.Mockito.InOrder(b2, b3);
			allocationOrder.Verify(b2).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(b3).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			VerifyQueueMetrics(b2, 1 * Gb, clusterResource);
			VerifyQueueMetrics(b3, 2 * Gb, clusterResource);
			// Now, B3 should get the scheduling opportunity 
			// since B2 has 1/2G while B3 has 2/7G, 
			// However, since B3 returns off-switch, B2 won't get an opportunity
			StubQueueAllocation(b2, clusterResource, node_0, 1 * Gb, NodeType.NodeLocal);
			StubQueueAllocation(b3, clusterResource, node_0, 1 * Gb, NodeType.OffSwitch);
			root.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			allocationOrder = Org.Mockito.Mockito.InOrder(b3, b2);
			allocationOrder.Verify(b3).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			allocationOrder.Verify(b2).AssignContainers(Matchers.Eq(clusterResource), Matchers.Any
				<FiCaSchedulerNode>(), AnyResourceLimits());
			VerifyQueueMetrics(b2, 1 * Gb, clusterResource);
			VerifyQueueMetrics(b3, 3 * Gb, clusterResource);
		}

		public virtual bool HasQueueACL(IList<QueueUserACLInfo> aclInfos, QueueACL acl, string
			 qName)
		{
			foreach (QueueUserACLInfo aclInfo in aclInfos)
			{
				if (aclInfo.GetQueueName().Equals(qName))
				{
					if (aclInfo.GetUserAcls().Contains(acl))
					{
						return true;
					}
				}
			}
			return false;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueAcl()
		{
			SetupMultiLevelQueues(csConf);
			csConf.SetAcl(CapacitySchedulerConfiguration.Root, QueueACL.SubmitApplications, " "
				);
			csConf.SetAcl(CapacitySchedulerConfiguration.Root, QueueACL.AdministerQueue, " ");
			string QC = CapacitySchedulerConfiguration.Root + "." + C;
			csConf.SetAcl(QC, QueueACL.AdministerQueue, "*");
			string QC11 = QC + "." + C1 + "." + C11;
			csConf.SetAcl(QC11, QueueACL.SubmitApplications, "*");
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CSQueue root = CapacityScheduler.ParseQueue(csContext, csConf, null, CapacitySchedulerConfiguration
				.Root, queues, queues, TestUtils.spyHook);
			YarnAuthorizationProvider authorizer = YarnAuthorizationProvider.GetInstance(conf
				);
			CapacityScheduler.SetQueueAcls(authorizer, queues);
			UserGroupInformation user = UserGroupInformation.GetCurrentUser();
			// Setup queue configs
			ParentQueue c = (ParentQueue)queues[C];
			ParentQueue c1 = (ParentQueue)queues[C1];
			ParentQueue c11 = (ParentQueue)queues[C11];
			ParentQueue c111 = (ParentQueue)queues[C111];
			NUnit.Framework.Assert.IsFalse(root.HasAccess(QueueACL.AdministerQueue, user));
			IList<QueueUserACLInfo> aclInfos = root.GetQueueUserAclInfo(user);
			NUnit.Framework.Assert.IsFalse(HasQueueACL(aclInfos, QueueACL.AdministerQueue, "root"
				));
			NUnit.Framework.Assert.IsFalse(root.HasAccess(QueueACL.SubmitApplications, user));
			NUnit.Framework.Assert.IsFalse(HasQueueACL(aclInfos, QueueACL.SubmitApplications, 
				"root"));
			// c has no SA, but QA
			NUnit.Framework.Assert.IsTrue(c.HasAccess(QueueACL.AdministerQueue, user));
			NUnit.Framework.Assert.IsTrue(HasQueueACL(aclInfos, QueueACL.AdministerQueue, "c"
				));
			NUnit.Framework.Assert.IsFalse(c.HasAccess(QueueACL.SubmitApplications, user));
			NUnit.Framework.Assert.IsFalse(HasQueueACL(aclInfos, QueueACL.SubmitApplications, 
				"c"));
			//Queue c1 has QA, no SA (gotten perm from parent)
			NUnit.Framework.Assert.IsTrue(c1.HasAccess(QueueACL.AdministerQueue, user));
			NUnit.Framework.Assert.IsTrue(HasQueueACL(aclInfos, QueueACL.AdministerQueue, "c1"
				));
			NUnit.Framework.Assert.IsFalse(c1.HasAccess(QueueACL.SubmitApplications, user));
			NUnit.Framework.Assert.IsFalse(HasQueueACL(aclInfos, QueueACL.SubmitApplications, 
				"c1"));
			//Queue c11 has permissions from parent queue and SA
			NUnit.Framework.Assert.IsTrue(c11.HasAccess(QueueACL.AdministerQueue, user));
			NUnit.Framework.Assert.IsTrue(HasQueueACL(aclInfos, QueueACL.AdministerQueue, "c11"
				));
			NUnit.Framework.Assert.IsTrue(c11.HasAccess(QueueACL.SubmitApplications, user));
			NUnit.Framework.Assert.IsTrue(HasQueueACL(aclInfos, QueueACL.SubmitApplications, 
				"c11"));
			//Queue c111 has SA and AQ, both from parent
			NUnit.Framework.Assert.IsTrue(c111.HasAccess(QueueACL.AdministerQueue, user));
			NUnit.Framework.Assert.IsTrue(HasQueueACL(aclInfos, QueueACL.AdministerQueue, "c111"
				));
			NUnit.Framework.Assert.IsTrue(c111.HasAccess(QueueACL.SubmitApplications, user));
			NUnit.Framework.Assert.IsTrue(HasQueueACL(aclInfos, QueueACL.SubmitApplications, 
				"c111"));
			Org.Mockito.Mockito.Reset(c);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
		}

		private ResourceLimits AnyResourceLimits()
		{
			return Matchers.Any<ResourceLimits>();
		}
	}
}
