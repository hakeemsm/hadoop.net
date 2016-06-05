using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public class TestAbstractYarnScheduler : ParameterizedSchedulerTestBase
	{
		public TestAbstractYarnScheduler(ParameterizedSchedulerTestBase.SchedulerType type
			)
			: base(type)
		{
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaximimumAllocationMemory()
		{
			int node1MaxMemory = 15 * 1024;
			int node2MaxMemory = 5 * 1024;
			int node3MaxMemory = 6 * 1024;
			int configuredMaxMemory = 10 * 1024;
			ConfigureScheduler();
			YarnConfiguration conf = GetConf();
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, configuredMaxMemory
				);
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 1000 * 1000
				);
			MockRM rm = new MockRM(conf);
			try
			{
				rm.Start();
				TestMaximumAllocationMemoryHelper((AbstractYarnScheduler)rm.GetResourceScheduler(
					), node1MaxMemory, node2MaxMemory, node3MaxMemory, configuredMaxMemory, configuredMaxMemory
					, configuredMaxMemory, configuredMaxMemory, configuredMaxMemory, configuredMaxMemory
					);
			}
			finally
			{
				rm.Stop();
			}
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 0);
			rm = new MockRM(conf);
			try
			{
				rm.Start();
				TestMaximumAllocationMemoryHelper((AbstractYarnScheduler)rm.GetResourceScheduler(
					), node1MaxMemory, node2MaxMemory, node3MaxMemory, configuredMaxMemory, configuredMaxMemory
					, configuredMaxMemory, node2MaxMemory, node3MaxMemory, node2MaxMemory);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestMaximumAllocationMemoryHelper(AbstractYarnScheduler scheduler, int
			 node1MaxMemory, int node2MaxMemory, int node3MaxMemory, params int[] expectedMaxMemory
			)
		{
			NUnit.Framework.Assert.AreEqual(6, expectedMaxMemory.Length);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetNumClusterNodes());
			int maxMemory = scheduler.GetMaximumResourceCapability().GetMemory();
			NUnit.Framework.Assert.AreEqual(expectedMaxMemory[0], maxMemory);
			RMNode node1 = MockNodes.NewNodeInfo(0, Resources.CreateResource(node1MaxMemory), 
				1, "127.0.0.2");
			scheduler.Handle(new NodeAddedSchedulerEvent(node1));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetNumClusterNodes());
			maxMemory = scheduler.GetMaximumResourceCapability().GetMemory();
			NUnit.Framework.Assert.AreEqual(expectedMaxMemory[1], maxMemory);
			scheduler.Handle(new NodeRemovedSchedulerEvent(node1));
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetNumClusterNodes());
			maxMemory = scheduler.GetMaximumResourceCapability().GetMemory();
			NUnit.Framework.Assert.AreEqual(expectedMaxMemory[2], maxMemory);
			RMNode node2 = MockNodes.NewNodeInfo(0, Resources.CreateResource(node2MaxMemory), 
				2, "127.0.0.3");
			scheduler.Handle(new NodeAddedSchedulerEvent(node2));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetNumClusterNodes());
			maxMemory = scheduler.GetMaximumResourceCapability().GetMemory();
			NUnit.Framework.Assert.AreEqual(expectedMaxMemory[3], maxMemory);
			RMNode node3 = MockNodes.NewNodeInfo(0, Resources.CreateResource(node3MaxMemory), 
				3, "127.0.0.4");
			scheduler.Handle(new NodeAddedSchedulerEvent(node3));
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetNumClusterNodes());
			maxMemory = scheduler.GetMaximumResourceCapability().GetMemory();
			NUnit.Framework.Assert.AreEqual(expectedMaxMemory[4], maxMemory);
			scheduler.Handle(new NodeRemovedSchedulerEvent(node3));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetNumClusterNodes());
			maxMemory = scheduler.GetMaximumResourceCapability().GetMemory();
			NUnit.Framework.Assert.AreEqual(expectedMaxMemory[5], maxMemory);
			scheduler.Handle(new NodeRemovedSchedulerEvent(node2));
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetNumClusterNodes());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaximimumAllocationVCores()
		{
			int node1MaxVCores = 15;
			int node2MaxVCores = 5;
			int node3MaxVCores = 6;
			int configuredMaxVCores = 10;
			ConfigureScheduler();
			YarnConfiguration conf = GetConf();
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores, configuredMaxVCores
				);
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 1000 * 1000
				);
			MockRM rm = new MockRM(conf);
			try
			{
				rm.Start();
				TestMaximumAllocationVCoresHelper((AbstractYarnScheduler)rm.GetResourceScheduler(
					), node1MaxVCores, node2MaxVCores, node3MaxVCores, configuredMaxVCores, configuredMaxVCores
					, configuredMaxVCores, configuredMaxVCores, configuredMaxVCores, configuredMaxVCores
					);
			}
			finally
			{
				rm.Stop();
			}
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 0);
			rm = new MockRM(conf);
			try
			{
				rm.Start();
				TestMaximumAllocationVCoresHelper((AbstractYarnScheduler)rm.GetResourceScheduler(
					), node1MaxVCores, node2MaxVCores, node3MaxVCores, configuredMaxVCores, configuredMaxVCores
					, configuredMaxVCores, node2MaxVCores, node3MaxVCores, node2MaxVCores);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestMaximumAllocationVCoresHelper(AbstractYarnScheduler scheduler, int
			 node1MaxVCores, int node2MaxVCores, int node3MaxVCores, params int[] expectedMaxVCores
			)
		{
			NUnit.Framework.Assert.AreEqual(6, expectedMaxVCores.Length);
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetNumClusterNodes());
			int maxVCores = scheduler.GetMaximumResourceCapability().GetVirtualCores();
			NUnit.Framework.Assert.AreEqual(expectedMaxVCores[0], maxVCores);
			RMNode node1 = MockNodes.NewNodeInfo(0, Resources.CreateResource(1024, node1MaxVCores
				), 1, "127.0.0.2");
			scheduler.Handle(new NodeAddedSchedulerEvent(node1));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetNumClusterNodes());
			maxVCores = scheduler.GetMaximumResourceCapability().GetVirtualCores();
			NUnit.Framework.Assert.AreEqual(expectedMaxVCores[1], maxVCores);
			scheduler.Handle(new NodeRemovedSchedulerEvent(node1));
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetNumClusterNodes());
			maxVCores = scheduler.GetMaximumResourceCapability().GetVirtualCores();
			NUnit.Framework.Assert.AreEqual(expectedMaxVCores[2], maxVCores);
			RMNode node2 = MockNodes.NewNodeInfo(0, Resources.CreateResource(1024, node2MaxVCores
				), 2, "127.0.0.3");
			scheduler.Handle(new NodeAddedSchedulerEvent(node2));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetNumClusterNodes());
			maxVCores = scheduler.GetMaximumResourceCapability().GetVirtualCores();
			NUnit.Framework.Assert.AreEqual(expectedMaxVCores[3], maxVCores);
			RMNode node3 = MockNodes.NewNodeInfo(0, Resources.CreateResource(1024, node3MaxVCores
				), 3, "127.0.0.4");
			scheduler.Handle(new NodeAddedSchedulerEvent(node3));
			NUnit.Framework.Assert.AreEqual(2, scheduler.GetNumClusterNodes());
			maxVCores = scheduler.GetMaximumResourceCapability().GetVirtualCores();
			NUnit.Framework.Assert.AreEqual(expectedMaxVCores[4], maxVCores);
			scheduler.Handle(new NodeRemovedSchedulerEvent(node3));
			NUnit.Framework.Assert.AreEqual(1, scheduler.GetNumClusterNodes());
			maxVCores = scheduler.GetMaximumResourceCapability().GetVirtualCores();
			NUnit.Framework.Assert.AreEqual(expectedMaxVCores[5], maxVCores);
			scheduler.Handle(new NodeRemovedSchedulerEvent(node2));
			NUnit.Framework.Assert.AreEqual(0, scheduler.GetNumClusterNodes());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdateMaxAllocationUsesTotal()
		{
			int configuredMaxVCores = 20;
			int configuredMaxMemory = 10 * 1024;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource configuredMaximumResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(configuredMaxMemory, configuredMaxVCores);
			ConfigureScheduler();
			YarnConfiguration conf = GetConf();
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores, configuredMaxVCores
				);
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, configuredMaxMemory
				);
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 0);
			MockRM rm = new MockRM(conf);
			try
			{
				rm.Start();
				AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm.GetResourceScheduler(
					);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource emptyResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(0, 0);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource fullResource1 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(1024, 5);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource fullResource2 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(2048, 10);
				SchedulerNode mockNode1 = Org.Mockito.Mockito.Mock<SchedulerNode>();
				Org.Mockito.Mockito.When(mockNode1.GetNodeID()).ThenReturn(NodeId.NewInstance("foo"
					, 8080));
				Org.Mockito.Mockito.When(mockNode1.GetAvailableResource()).ThenReturn(emptyResource
					);
				Org.Mockito.Mockito.When(mockNode1.GetTotalResource()).ThenReturn(fullResource1);
				SchedulerNode mockNode2 = Org.Mockito.Mockito.Mock<SchedulerNode>();
				Org.Mockito.Mockito.When(mockNode1.GetNodeID()).ThenReturn(NodeId.NewInstance("bar"
					, 8081));
				Org.Mockito.Mockito.When(mockNode2.GetAvailableResource()).ThenReturn(emptyResource
					);
				Org.Mockito.Mockito.When(mockNode2.GetTotalResource()).ThenReturn(fullResource2);
				VerifyMaximumResourceCapability(configuredMaximumResource, scheduler);
				scheduler.nodes = new Dictionary<NodeId, SchedulerNode>();
				scheduler.nodes[mockNode1.GetNodeID()] = mockNode1;
				scheduler.UpdateMaximumAllocation(mockNode1, true);
				VerifyMaximumResourceCapability(fullResource1, scheduler);
				scheduler.nodes[mockNode2.GetNodeID()] = mockNode2;
				scheduler.UpdateMaximumAllocation(mockNode2, true);
				VerifyMaximumResourceCapability(fullResource2, scheduler);
				Sharpen.Collections.Remove(scheduler.nodes, mockNode2.GetNodeID());
				scheduler.UpdateMaximumAllocation(mockNode2, false);
				VerifyMaximumResourceCapability(fullResource1, scheduler);
				Sharpen.Collections.Remove(scheduler.nodes, mockNode1.GetNodeID());
				scheduler.UpdateMaximumAllocation(mockNode1, false);
				VerifyMaximumResourceCapability(configuredMaximumResource, scheduler);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxAllocationAfterUpdateNodeResource()
		{
			int configuredMaxVCores = 20;
			int configuredMaxMemory = 10 * 1024;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource configuredMaximumResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(configuredMaxMemory, configuredMaxVCores);
			ConfigureScheduler();
			YarnConfiguration conf = GetConf();
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores, configuredMaxVCores
				);
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, configuredMaxMemory
				);
			conf.SetLong(YarnConfiguration.RmWorkPreservingRecoverySchedulingWaitMs, 0);
			MockRM rm = new MockRM(conf);
			try
			{
				rm.Start();
				AbstractYarnScheduler scheduler = (AbstractYarnScheduler)rm.GetResourceScheduler(
					);
				VerifyMaximumResourceCapability(configuredMaximumResource, scheduler);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource1 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(2048, 5);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource2 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(4096, 10);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource3 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(512, 1);
				Org.Apache.Hadoop.Yarn.Api.Records.Resource resource4 = Org.Apache.Hadoop.Yarn.Api.Records.Resource
					.NewInstance(1024, 2);
				RMNode node1 = MockNodes.NewNodeInfo(0, resource1, 1, "127.0.0.2");
				scheduler.Handle(new NodeAddedSchedulerEvent(node1));
				RMNode node2 = MockNodes.NewNodeInfo(0, resource3, 2, "127.0.0.3");
				scheduler.Handle(new NodeAddedSchedulerEvent(node2));
				VerifyMaximumResourceCapability(resource1, scheduler);
				// increase node1 resource
				scheduler.UpdateNodeResource(node1, ResourceOption.NewInstance(resource2, 0));
				VerifyMaximumResourceCapability(resource2, scheduler);
				// decrease node1 resource
				scheduler.UpdateNodeResource(node1, ResourceOption.NewInstance(resource1, 0));
				VerifyMaximumResourceCapability(resource1, scheduler);
				// increase node2 resource
				scheduler.UpdateNodeResource(node2, ResourceOption.NewInstance(resource4, 0));
				VerifyMaximumResourceCapability(resource1, scheduler);
				// decrease node2 resource
				scheduler.UpdateNodeResource(node2, ResourceOption.NewInstance(resource3, 0));
				VerifyMaximumResourceCapability(resource1, scheduler);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestResourceRequestRestoreWhenRMContainerIsAtAllocated()
		{
			ConfigureScheduler();
			YarnConfiguration conf = GetConf();
			MockRM rm1 = new MockRM(conf);
			try
			{
				rm1.Start();
				RMApp app1 = rm1.SubmitApp(200, "name", "user", new Dictionary<ApplicationAccessType
					, string>(), false, "default", -1, null, "Test", false, true);
				MockNM nm1 = new MockNM("127.0.0.1:1234", 10240, rm1.GetResourceTrackerService());
				nm1.RegisterNode();
				MockNM nm2 = new MockNM("127.0.0.1:2351", 10240, rm1.GetResourceTrackerService());
				nm2.RegisterNode();
				MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm1, nm1);
				int NumContainers = 1;
				// allocate NUM_CONTAINERS containers
				am1.Allocate("127.0.0.1", 1024, NumContainers, new AList<ContainerId>());
				nm1.NodeHeartbeat(true);
				// wait for containers to be allocated.
				IList<Container> containers = am1.Allocate(new AList<ResourceRequest>(), new AList
					<ContainerId>()).GetAllocatedContainers();
				while (containers.Count != NumContainers)
				{
					nm1.NodeHeartbeat(true);
					Sharpen.Collections.AddAll(containers, am1.Allocate(new AList<ResourceRequest>(), 
						new AList<ContainerId>()).GetAllocatedContainers());
					Sharpen.Thread.Sleep(200);
				}
				// launch the 2nd container, for testing running container transferred.
				nm1.NodeHeartbeat(am1.GetApplicationAttemptId(), 2, ContainerState.Running);
				ContainerId containerId2 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
					(), 2);
				rm1.WaitForState(nm1, containerId2, RMContainerState.Running);
				// 3rd container is in Allocated state.
				am1.Allocate("127.0.0.1", 1024, NumContainers, new AList<ContainerId>());
				nm2.NodeHeartbeat(true);
				ContainerId containerId3 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
					(), 3);
				rm1.WaitForContainerAllocated(nm2, containerId3);
				rm1.WaitForState(nm2, containerId3, RMContainerState.Allocated);
				// NodeManager restart
				nm2.RegisterNode();
				// NM restart kills all allocated and running containers.
				rm1.WaitForState(nm2, containerId3, RMContainerState.Killed);
				// The killed RMContainer request should be restored. In successive
				// nodeHeartBeats AM should be able to get container allocated.
				containers = am1.Allocate(new AList<ResourceRequest>(), new AList<ContainerId>())
					.GetAllocatedContainers();
				while (containers.Count != NumContainers)
				{
					nm2.NodeHeartbeat(true);
					Sharpen.Collections.AddAll(containers, am1.Allocate(new AList<ResourceRequest>(), 
						new AList<ContainerId>()).GetAllocatedContainers());
					Sharpen.Thread.Sleep(200);
				}
				nm2.NodeHeartbeat(am1.GetApplicationAttemptId(), 4, ContainerState.Running);
				ContainerId containerId4 = ContainerId.NewContainerId(am1.GetApplicationAttemptId
					(), 4);
				rm1.WaitForState(nm2, containerId4, RMContainerState.Running);
			}
			finally
			{
				rm1.Stop();
			}
		}

		private void VerifyMaximumResourceCapability(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 expectedMaximumResource, AbstractYarnScheduler scheduler)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource schedulerMaximumResourceCapability = 
				scheduler.GetMaximumResourceCapability();
			NUnit.Framework.Assert.AreEqual(expectedMaximumResource.GetMemory(), schedulerMaximumResourceCapability
				.GetMemory());
			NUnit.Framework.Assert.AreEqual(expectedMaximumResource.GetVirtualCores(), schedulerMaximumResourceCapability
				.GetVirtualCores());
		}
	}
}
