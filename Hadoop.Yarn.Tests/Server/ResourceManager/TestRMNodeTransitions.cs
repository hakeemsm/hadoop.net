using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestRMNodeTransitions
	{
		internal RMNodeImpl node;

		private RMContext rmContext;

		private YarnScheduler scheduler;

		private SchedulerEventType eventType;

		private IList<ContainerStatus> completedContainers = new AList<ContainerStatus>();

		private sealed class TestSchedulerEventDispatcher : EventHandler<SchedulerEvent>
		{
			public void Handle(SchedulerEvent @event)
			{
				this._enclosing.scheduler.Handle(@event);
			}

			internal TestSchedulerEventDispatcher(TestRMNodeTransitions _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMNodeTransitions _enclosing;
		}

		private NodesListManagerEvent nodesListManagerEvent = null;

		private class TestNodeListManagerEventDispatcher : EventHandler<NodesListManagerEvent
			>
		{
			public virtual void Handle(NodesListManagerEvent @event)
			{
				this._enclosing.nodesListManagerEvent = @event;
			}

			internal TestNodeListManagerEventDispatcher(TestRMNodeTransitions _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMNodeTransitions _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			InlineDispatcher rmDispatcher = new InlineDispatcher();
			rmContext = new RMContextImpl(rmDispatcher, null, null, null, Org.Mockito.Mockito.Mock
				<DelegationTokenRenewer>(), null, null, null, null, null);
			NodesListManager nodesListManager = Org.Mockito.Mockito.Mock<NodesListManager>();
			HostsFileReader reader = Org.Mockito.Mockito.Mock<HostsFileReader>();
			Org.Mockito.Mockito.When(nodesListManager.GetHostsReader()).ThenReturn(reader);
			((RMContextImpl)rmContext).SetNodesListManager(nodesListManager);
			scheduler = Org.Mockito.Mockito.Mock<YarnScheduler>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_115(this)).When(scheduler).Handle(Matchers.Any
				<SchedulerEvent>());
			rmDispatcher.Register(typeof(SchedulerEventType), new TestRMNodeTransitions.TestSchedulerEventDispatcher
				(this));
			rmDispatcher.Register(typeof(NodesListManagerEventType), new TestRMNodeTransitions.TestNodeListManagerEventDispatcher
				(this));
			NodeId nodeId = BuilderUtils.NewNodeId("localhost", 0);
			node = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, null, null);
			nodesListManagerEvent = null;
		}

		private sealed class _Answer_115 : Answer<Void>
		{
			public _Answer_115(TestRMNodeTransitions _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				SchedulerEvent @event = (SchedulerEvent)(invocation.GetArguments()[0]);
				this._enclosing.eventType = @event.GetType();
				if (this._enclosing.eventType == SchedulerEventType.NodeUpdate)
				{
					IList<UpdatedContainerInfo> lastestContainersInfoList = ((NodeUpdateSchedulerEvent
						)@event).GetRMNode().PullContainerUpdates();
					foreach (UpdatedContainerInfo lastestContainersInfo in lastestContainersInfoList)
					{
						Sharpen.Collections.AddAll(this._enclosing.completedContainers, lastestContainersInfo
							.GetCompletedContainers());
					}
				}
				return null;
			}

			private readonly TestRMNodeTransitions _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDown()
		{
		}

		private RMNodeStatusEvent GetMockRMNodeStatusEvent()
		{
			NodeHeartbeatResponse response = Org.Mockito.Mockito.Mock<NodeHeartbeatResponse>(
				);
			NodeHealthStatus healthStatus = Org.Mockito.Mockito.Mock<NodeHealthStatus>();
			bool yes = true;
			Org.Mockito.Mockito.DoReturn(yes).When(healthStatus).GetIsNodeHealthy();
			RMNodeStatusEvent @event = Org.Mockito.Mockito.Mock<RMNodeStatusEvent>();
			Org.Mockito.Mockito.DoReturn(healthStatus).When(@event).GetNodeHealthStatus();
			Org.Mockito.Mockito.DoReturn(response).When(@event).GetLatestResponse();
			Org.Mockito.Mockito.DoReturn(RMNodeEventType.StatusUpdate).When(@event).GetType();
			return @event;
		}

		public virtual void TestExpiredContainer()
		{
			// Start the node
			node.Handle(new RMNodeStartedEvent(null, null, null));
			Org.Mockito.Mockito.Verify(scheduler).Handle(Matchers.Any<NodeAddedSchedulerEvent
				>());
			// Expire a container
			ContainerId completedContainerId = BuilderUtils.NewContainerId(BuilderUtils.NewApplicationAttemptId
				(BuilderUtils.NewApplicationId(0, 0), 0), 0);
			node.Handle(new RMNodeCleanContainerEvent(null, completedContainerId));
			NUnit.Framework.Assert.AreEqual(1, node.GetContainersToCleanUp().Count);
			// Now verify that scheduler isn't notified of an expired container
			// by checking number of 'completedContainers' it got in the previous event
			RMNodeStatusEvent statusEvent = GetMockRMNodeStatusEvent();
			ContainerStatus containerStatus = Org.Mockito.Mockito.Mock<ContainerStatus>();
			Org.Mockito.Mockito.DoReturn(completedContainerId).When(containerStatus).GetContainerId
				();
			Org.Mockito.Mockito.DoReturn(Sharpen.Collections.SingletonList(containerStatus)).
				When(statusEvent).GetContainers();
			node.Handle(statusEvent);
			/* Expect the scheduler call handle function 2 times
			* 1. RMNode status from new to Running, handle the add_node event
			* 2. handle the node update event
			*/
			Org.Mockito.Mockito.Verify(scheduler, Org.Mockito.Mockito.Times(2)).Handle(Matchers.Any
				<NodeUpdateSchedulerEvent>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerUpdate()
		{
			//Start the node
			node.Handle(new RMNodeStartedEvent(null, null, null));
			NodeId nodeId = BuilderUtils.NewNodeId("localhost:1", 1);
			RMNodeImpl node2 = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, null, null
				);
			node2.Handle(new RMNodeStartedEvent(null, null, null));
			ContainerId completedContainerIdFromNode1 = BuilderUtils.NewContainerId(BuilderUtils
				.NewApplicationAttemptId(BuilderUtils.NewApplicationId(0, 0), 0), 0);
			ContainerId completedContainerIdFromNode2_1 = BuilderUtils.NewContainerId(BuilderUtils
				.NewApplicationAttemptId(BuilderUtils.NewApplicationId(1, 1), 1), 1);
			ContainerId completedContainerIdFromNode2_2 = BuilderUtils.NewContainerId(BuilderUtils
				.NewApplicationAttemptId(BuilderUtils.NewApplicationId(1, 1), 1), 2);
			RMNodeStatusEvent statusEventFromNode1 = GetMockRMNodeStatusEvent();
			RMNodeStatusEvent statusEventFromNode2_1 = GetMockRMNodeStatusEvent();
			RMNodeStatusEvent statusEventFromNode2_2 = GetMockRMNodeStatusEvent();
			ContainerStatus containerStatusFromNode1 = Org.Mockito.Mockito.Mock<ContainerStatus
				>();
			ContainerStatus containerStatusFromNode2_1 = Org.Mockito.Mockito.Mock<ContainerStatus
				>();
			ContainerStatus containerStatusFromNode2_2 = Org.Mockito.Mockito.Mock<ContainerStatus
				>();
			Org.Mockito.Mockito.DoReturn(completedContainerIdFromNode1).When(containerStatusFromNode1
				).GetContainerId();
			Org.Mockito.Mockito.DoReturn(Sharpen.Collections.SingletonList(containerStatusFromNode1
				)).When(statusEventFromNode1).GetContainers();
			node.Handle(statusEventFromNode1);
			NUnit.Framework.Assert.AreEqual(1, completedContainers.Count);
			NUnit.Framework.Assert.AreEqual(completedContainerIdFromNode1, completedContainers
				[0].GetContainerId());
			completedContainers.Clear();
			Org.Mockito.Mockito.DoReturn(completedContainerIdFromNode2_1).When(containerStatusFromNode2_1
				).GetContainerId();
			Org.Mockito.Mockito.DoReturn(Sharpen.Collections.SingletonList(containerStatusFromNode2_1
				)).When(statusEventFromNode2_1).GetContainers();
			Org.Mockito.Mockito.DoReturn(completedContainerIdFromNode2_2).When(containerStatusFromNode2_2
				).GetContainerId();
			Org.Mockito.Mockito.DoReturn(Sharpen.Collections.SingletonList(containerStatusFromNode2_2
				)).When(statusEventFromNode2_2).GetContainers();
			node2.SetNextHeartBeat(false);
			node2.Handle(statusEventFromNode2_1);
			node2.SetNextHeartBeat(true);
			node2.Handle(statusEventFromNode2_2);
			NUnit.Framework.Assert.AreEqual(2, completedContainers.Count);
			NUnit.Framework.Assert.AreEqual(completedContainerIdFromNode2_1, completedContainers
				[0].GetContainerId());
			NUnit.Framework.Assert.AreEqual(completedContainerIdFromNode2_2, completedContainers
				[1].GetContainerId());
		}

		public virtual void TestStatusChange()
		{
			//Start the node
			node.Handle(new RMNodeStartedEvent(null, null, null));
			//Add info to the queue first
			node.SetNextHeartBeat(false);
			ContainerId completedContainerId1 = BuilderUtils.NewContainerId(BuilderUtils.NewApplicationAttemptId
				(BuilderUtils.NewApplicationId(0, 0), 0), 0);
			ContainerId completedContainerId2 = BuilderUtils.NewContainerId(BuilderUtils.NewApplicationAttemptId
				(BuilderUtils.NewApplicationId(1, 1), 1), 1);
			RMNodeStatusEvent statusEvent1 = GetMockRMNodeStatusEvent();
			RMNodeStatusEvent statusEvent2 = GetMockRMNodeStatusEvent();
			ContainerStatus containerStatus1 = Org.Mockito.Mockito.Mock<ContainerStatus>();
			ContainerStatus containerStatus2 = Org.Mockito.Mockito.Mock<ContainerStatus>();
			Org.Mockito.Mockito.DoReturn(completedContainerId1).When(containerStatus1).GetContainerId
				();
			Org.Mockito.Mockito.DoReturn(Sharpen.Collections.SingletonList(containerStatus1))
				.When(statusEvent1).GetContainers();
			Org.Mockito.Mockito.DoReturn(completedContainerId2).When(containerStatus2).GetContainerId
				();
			Org.Mockito.Mockito.DoReturn(Sharpen.Collections.SingletonList(containerStatus2))
				.When(statusEvent2).GetContainers();
			Org.Mockito.Mockito.Verify(scheduler, Org.Mockito.Mockito.Times(1)).Handle(Matchers.Any
				<NodeUpdateSchedulerEvent>());
			node.Handle(statusEvent1);
			node.Handle(statusEvent2);
			Org.Mockito.Mockito.Verify(scheduler, Org.Mockito.Mockito.Times(1)).Handle(Matchers.Any
				<NodeUpdateSchedulerEvent>());
			NUnit.Framework.Assert.AreEqual(2, node.GetQueueSize());
			node.Handle(new RMNodeEvent(node.GetNodeID(), RMNodeEventType.Expire));
			NUnit.Framework.Assert.AreEqual(0, node.GetQueueSize());
		}

		[NUnit.Framework.Test]
		public virtual void TestRunningExpire()
		{
			RMNodeImpl node = GetRunningNode();
			ClusterMetrics cm = ClusterMetrics.GetMetrics();
			int initialActive = cm.GetNumActiveNMs();
			int initialLost = cm.GetNumLostNMs();
			int initialUnhealthy = cm.GetUnhealthyNMs();
			int initialDecommissioned = cm.GetNumDecommisionedNMs();
			int initialRebooted = cm.GetNumRebootedNMs();
			node.Handle(new RMNodeEvent(node.GetNodeID(), RMNodeEventType.Expire));
			NUnit.Framework.Assert.AreEqual("Active Nodes", initialActive - 1, cm.GetNumActiveNMs
				());
			NUnit.Framework.Assert.AreEqual("Lost Nodes", initialLost + 1, cm.GetNumLostNMs()
				);
			NUnit.Framework.Assert.AreEqual("Unhealthy Nodes", initialUnhealthy, cm.GetUnhealthyNMs
				());
			NUnit.Framework.Assert.AreEqual("Decommissioned Nodes", initialDecommissioned, cm
				.GetNumDecommisionedNMs());
			NUnit.Framework.Assert.AreEqual("Rebooted Nodes", initialRebooted, cm.GetNumRebootedNMs
				());
			NUnit.Framework.Assert.AreEqual(NodeState.Lost, node.GetState());
		}

		[NUnit.Framework.Test]
		public virtual void TestUnhealthyExpire()
		{
			RMNodeImpl node = GetUnhealthyNode();
			ClusterMetrics cm = ClusterMetrics.GetMetrics();
			int initialActive = cm.GetNumActiveNMs();
			int initialLost = cm.GetNumLostNMs();
			int initialUnhealthy = cm.GetUnhealthyNMs();
			int initialDecommissioned = cm.GetNumDecommisionedNMs();
			int initialRebooted = cm.GetNumRebootedNMs();
			node.Handle(new RMNodeEvent(node.GetNodeID(), RMNodeEventType.Expire));
			NUnit.Framework.Assert.AreEqual("Active Nodes", initialActive, cm.GetNumActiveNMs
				());
			NUnit.Framework.Assert.AreEqual("Lost Nodes", initialLost + 1, cm.GetNumLostNMs()
				);
			NUnit.Framework.Assert.AreEqual("Unhealthy Nodes", initialUnhealthy - 1, cm.GetUnhealthyNMs
				());
			NUnit.Framework.Assert.AreEqual("Decommissioned Nodes", initialDecommissioned, cm
				.GetNumDecommisionedNMs());
			NUnit.Framework.Assert.AreEqual("Rebooted Nodes", initialRebooted, cm.GetNumRebootedNMs
				());
			NUnit.Framework.Assert.AreEqual(NodeState.Lost, node.GetState());
		}

		[NUnit.Framework.Test]
		public virtual void TestUnhealthyExpireForSchedulerRemove()
		{
			RMNodeImpl node = GetUnhealthyNode();
			Org.Mockito.Mockito.Verify(scheduler, Org.Mockito.Mockito.Times(2)).Handle(Matchers.Any
				<NodeRemovedSchedulerEvent>());
			node.Handle(new RMNodeEvent(node.GetNodeID(), RMNodeEventType.Expire));
			Org.Mockito.Mockito.Verify(scheduler, Org.Mockito.Mockito.Times(2)).Handle(Matchers.Any
				<NodeRemovedSchedulerEvent>());
			NUnit.Framework.Assert.AreEqual(NodeState.Lost, node.GetState());
		}

		[NUnit.Framework.Test]
		public virtual void TestRunningDecommission()
		{
			RMNodeImpl node = GetRunningNode();
			ClusterMetrics cm = ClusterMetrics.GetMetrics();
			int initialActive = cm.GetNumActiveNMs();
			int initialLost = cm.GetNumLostNMs();
			int initialUnhealthy = cm.GetUnhealthyNMs();
			int initialDecommissioned = cm.GetNumDecommisionedNMs();
			int initialRebooted = cm.GetNumRebootedNMs();
			node.Handle(new RMNodeEvent(node.GetNodeID(), RMNodeEventType.Decommission));
			NUnit.Framework.Assert.AreEqual("Active Nodes", initialActive - 1, cm.GetNumActiveNMs
				());
			NUnit.Framework.Assert.AreEqual("Lost Nodes", initialLost, cm.GetNumLostNMs());
			NUnit.Framework.Assert.AreEqual("Unhealthy Nodes", initialUnhealthy, cm.GetUnhealthyNMs
				());
			NUnit.Framework.Assert.AreEqual("Decommissioned Nodes", initialDecommissioned + 1
				, cm.GetNumDecommisionedNMs());
			NUnit.Framework.Assert.AreEqual("Rebooted Nodes", initialRebooted, cm.GetNumRebootedNMs
				());
			NUnit.Framework.Assert.AreEqual(NodeState.Decommissioned, node.GetState());
		}

		[NUnit.Framework.Test]
		public virtual void TestUnhealthyDecommission()
		{
			RMNodeImpl node = GetUnhealthyNode();
			ClusterMetrics cm = ClusterMetrics.GetMetrics();
			int initialActive = cm.GetNumActiveNMs();
			int initialLost = cm.GetNumLostNMs();
			int initialUnhealthy = cm.GetUnhealthyNMs();
			int initialDecommissioned = cm.GetNumDecommisionedNMs();
			int initialRebooted = cm.GetNumRebootedNMs();
			node.Handle(new RMNodeEvent(node.GetNodeID(), RMNodeEventType.Decommission));
			NUnit.Framework.Assert.AreEqual("Active Nodes", initialActive, cm.GetNumActiveNMs
				());
			NUnit.Framework.Assert.AreEqual("Lost Nodes", initialLost, cm.GetNumLostNMs());
			NUnit.Framework.Assert.AreEqual("Unhealthy Nodes", initialUnhealthy - 1, cm.GetUnhealthyNMs
				());
			NUnit.Framework.Assert.AreEqual("Decommissioned Nodes", initialDecommissioned + 1
				, cm.GetNumDecommisionedNMs());
			NUnit.Framework.Assert.AreEqual("Rebooted Nodes", initialRebooted, cm.GetNumRebootedNMs
				());
			NUnit.Framework.Assert.AreEqual(NodeState.Decommissioned, node.GetState());
		}

		[NUnit.Framework.Test]
		public virtual void TestRunningRebooting()
		{
			RMNodeImpl node = GetRunningNode();
			ClusterMetrics cm = ClusterMetrics.GetMetrics();
			int initialActive = cm.GetNumActiveNMs();
			int initialLost = cm.GetNumLostNMs();
			int initialUnhealthy = cm.GetUnhealthyNMs();
			int initialDecommissioned = cm.GetNumDecommisionedNMs();
			int initialRebooted = cm.GetNumRebootedNMs();
			node.Handle(new RMNodeEvent(node.GetNodeID(), RMNodeEventType.Rebooting));
			NUnit.Framework.Assert.AreEqual("Active Nodes", initialActive - 1, cm.GetNumActiveNMs
				());
			NUnit.Framework.Assert.AreEqual("Lost Nodes", initialLost, cm.GetNumLostNMs());
			NUnit.Framework.Assert.AreEqual("Unhealthy Nodes", initialUnhealthy, cm.GetUnhealthyNMs
				());
			NUnit.Framework.Assert.AreEqual("Decommissioned Nodes", initialDecommissioned, cm
				.GetNumDecommisionedNMs());
			NUnit.Framework.Assert.AreEqual("Rebooted Nodes", initialRebooted + 1, cm.GetNumRebootedNMs
				());
			NUnit.Framework.Assert.AreEqual(NodeState.Rebooted, node.GetState());
		}

		[NUnit.Framework.Test]
		public virtual void TestUnhealthyRebooting()
		{
			RMNodeImpl node = GetUnhealthyNode();
			ClusterMetrics cm = ClusterMetrics.GetMetrics();
			int initialActive = cm.GetNumActiveNMs();
			int initialLost = cm.GetNumLostNMs();
			int initialUnhealthy = cm.GetUnhealthyNMs();
			int initialDecommissioned = cm.GetNumDecommisionedNMs();
			int initialRebooted = cm.GetNumRebootedNMs();
			node.Handle(new RMNodeEvent(node.GetNodeID(), RMNodeEventType.Rebooting));
			NUnit.Framework.Assert.AreEqual("Active Nodes", initialActive, cm.GetNumActiveNMs
				());
			NUnit.Framework.Assert.AreEqual("Lost Nodes", initialLost, cm.GetNumLostNMs());
			NUnit.Framework.Assert.AreEqual("Unhealthy Nodes", initialUnhealthy - 1, cm.GetUnhealthyNMs
				());
			NUnit.Framework.Assert.AreEqual("Decommissioned Nodes", initialDecommissioned, cm
				.GetNumDecommisionedNMs());
			NUnit.Framework.Assert.AreEqual("Rebooted Nodes", initialRebooted + 1, cm.GetNumRebootedNMs
				());
			NUnit.Framework.Assert.AreEqual(NodeState.Rebooted, node.GetState());
		}

		public virtual void TestUpdateHeartbeatResponseForCleanup()
		{
			RMNodeImpl node = GetRunningNode();
			NodeId nodeId = node.GetNodeID();
			// Expire a container
			ContainerId completedContainerId = BuilderUtils.NewContainerId(BuilderUtils.NewApplicationAttemptId
				(BuilderUtils.NewApplicationId(0, 0), 0), 0);
			node.Handle(new RMNodeCleanContainerEvent(nodeId, completedContainerId));
			NUnit.Framework.Assert.AreEqual(1, node.GetContainersToCleanUp().Count);
			// Finish an application
			ApplicationId finishedAppId = BuilderUtils.NewApplicationId(0, 1);
			node.Handle(new RMNodeCleanAppEvent(nodeId, finishedAppId));
			NUnit.Framework.Assert.AreEqual(1, node.GetAppsToCleanup().Count);
			// Verify status update does not clear containers/apps to cleanup
			// but updating heartbeat response for cleanup does
			RMNodeStatusEvent statusEvent = GetMockRMNodeStatusEvent();
			node.Handle(statusEvent);
			NUnit.Framework.Assert.AreEqual(1, node.GetContainersToCleanUp().Count);
			NUnit.Framework.Assert.AreEqual(1, node.GetAppsToCleanup().Count);
			NodeHeartbeatResponse hbrsp = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NodeHeartbeatResponse
				>();
			node.UpdateNodeHeartbeatResponseForCleanup(hbrsp);
			NUnit.Framework.Assert.AreEqual(0, node.GetContainersToCleanUp().Count);
			NUnit.Framework.Assert.AreEqual(0, node.GetAppsToCleanup().Count);
			NUnit.Framework.Assert.AreEqual(1, hbrsp.GetContainersToCleanup().Count);
			NUnit.Framework.Assert.AreEqual(completedContainerId, hbrsp.GetContainersToCleanup
				()[0]);
			NUnit.Framework.Assert.AreEqual(1, hbrsp.GetApplicationsToCleanup().Count);
			NUnit.Framework.Assert.AreEqual(finishedAppId, hbrsp.GetApplicationsToCleanup()[0
				]);
		}

		private RMNodeImpl GetRunningNode()
		{
			return GetRunningNode(null);
		}

		private RMNodeImpl GetRunningNode(string nmVersion)
		{
			NodeId nodeId = BuilderUtils.NewNodeId("localhost", 0);
			Resource capability = Resource.NewInstance(4096, 4);
			RMNodeImpl node = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, capability, 
				nmVersion);
			node.Handle(new RMNodeStartedEvent(node.GetNodeID(), null, null));
			NUnit.Framework.Assert.AreEqual(NodeState.Running, node.GetState());
			return node;
		}

		private RMNodeImpl GetUnhealthyNode()
		{
			RMNodeImpl node = GetRunningNode();
			NodeHealthStatus status = NodeHealthStatus.NewInstance(false, "sick", Runtime.CurrentTimeMillis
				());
			node.Handle(new RMNodeStatusEvent(node.GetNodeID(), status, new AList<ContainerStatus
				>(), null, null));
			NUnit.Framework.Assert.AreEqual(NodeState.Unhealthy, node.GetState());
			return node;
		}

		private RMNodeImpl GetNewNode()
		{
			NodeId nodeId = BuilderUtils.NewNodeId("localhost", 0);
			RMNodeImpl node = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, null, null);
			return node;
		}

		private RMNodeImpl GetNewNode(Resource capability)
		{
			NodeId nodeId = BuilderUtils.NewNodeId("localhost", 0);
			RMNodeImpl node = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, capability, 
				null);
			return node;
		}

		private RMNodeImpl GetRebootedNode()
		{
			NodeId nodeId = BuilderUtils.NewNodeId("localhost", 0);
			Resource capability = Resource.NewInstance(4096, 4);
			RMNodeImpl node = new RMNodeImpl(nodeId, rmContext, null, 0, 0, null, capability, 
				null);
			node.Handle(new RMNodeStartedEvent(node.GetNodeID(), null, null));
			NUnit.Framework.Assert.AreEqual(NodeState.Running, node.GetState());
			node.Handle(new RMNodeEvent(node.GetNodeID(), RMNodeEventType.Rebooting));
			NUnit.Framework.Assert.AreEqual(NodeState.Rebooted, node.GetState());
			return node;
		}

		[NUnit.Framework.Test]
		public virtual void TestAdd()
		{
			RMNodeImpl node = GetNewNode();
			ClusterMetrics cm = ClusterMetrics.GetMetrics();
			int initialActive = cm.GetNumActiveNMs();
			int initialLost = cm.GetNumLostNMs();
			int initialUnhealthy = cm.GetUnhealthyNMs();
			int initialDecommissioned = cm.GetNumDecommisionedNMs();
			int initialRebooted = cm.GetNumRebootedNMs();
			node.Handle(new RMNodeStartedEvent(node.GetNodeID(), null, null));
			NUnit.Framework.Assert.AreEqual("Active Nodes", initialActive + 1, cm.GetNumActiveNMs
				());
			NUnit.Framework.Assert.AreEqual("Lost Nodes", initialLost, cm.GetNumLostNMs());
			NUnit.Framework.Assert.AreEqual("Unhealthy Nodes", initialUnhealthy, cm.GetUnhealthyNMs
				());
			NUnit.Framework.Assert.AreEqual("Decommissioned Nodes", initialDecommissioned, cm
				.GetNumDecommisionedNMs());
			NUnit.Framework.Assert.AreEqual("Rebooted Nodes", initialRebooted, cm.GetNumRebootedNMs
				());
			NUnit.Framework.Assert.AreEqual(NodeState.Running, node.GetState());
			NUnit.Framework.Assert.IsNotNull(nodesListManagerEvent);
			NUnit.Framework.Assert.AreEqual(NodesListManagerEventType.NodeUsable, nodesListManagerEvent
				.GetType());
		}

		[NUnit.Framework.Test]
		public virtual void TestReconnect()
		{
			RMNodeImpl node = GetRunningNode();
			ClusterMetrics cm = ClusterMetrics.GetMetrics();
			int initialActive = cm.GetNumActiveNMs();
			int initialLost = cm.GetNumLostNMs();
			int initialUnhealthy = cm.GetUnhealthyNMs();
			int initialDecommissioned = cm.GetNumDecommisionedNMs();
			int initialRebooted = cm.GetNumRebootedNMs();
			node.Handle(new RMNodeReconnectEvent(node.GetNodeID(), node, null, null));
			NUnit.Framework.Assert.AreEqual("Active Nodes", initialActive, cm.GetNumActiveNMs
				());
			NUnit.Framework.Assert.AreEqual("Lost Nodes", initialLost, cm.GetNumLostNMs());
			NUnit.Framework.Assert.AreEqual("Unhealthy Nodes", initialUnhealthy, cm.GetUnhealthyNMs
				());
			NUnit.Framework.Assert.AreEqual("Decommissioned Nodes", initialDecommissioned, cm
				.GetNumDecommisionedNMs());
			NUnit.Framework.Assert.AreEqual("Rebooted Nodes", initialRebooted, cm.GetNumRebootedNMs
				());
			NUnit.Framework.Assert.AreEqual(NodeState.Running, node.GetState());
			NUnit.Framework.Assert.IsNotNull(nodesListManagerEvent);
			NUnit.Framework.Assert.AreEqual(NodesListManagerEventType.NodeUsable, nodesListManagerEvent
				.GetType());
		}

		[NUnit.Framework.Test]
		public virtual void TestResourceUpdateOnRunningNode()
		{
			RMNodeImpl node = GetRunningNode();
			Resource oldCapacity = node.GetTotalCapability();
			NUnit.Framework.Assert.AreEqual("Memory resource is not match.", oldCapacity.GetMemory
				(), 4096);
			NUnit.Framework.Assert.AreEqual("CPU resource is not match.", oldCapacity.GetVirtualCores
				(), 4);
			node.Handle(new RMNodeResourceUpdateEvent(node.GetNodeID(), ResourceOption.NewInstance
				(Resource.NewInstance(2048, 2), RMNode.OverCommitTimeoutMillisDefault)));
			Resource newCapacity = node.GetTotalCapability();
			NUnit.Framework.Assert.AreEqual("Memory resource is not match.", newCapacity.GetMemory
				(), 2048);
			NUnit.Framework.Assert.AreEqual("CPU resource is not match.", newCapacity.GetVirtualCores
				(), 2);
			NUnit.Framework.Assert.AreEqual(NodeState.Running, node.GetState());
			NUnit.Framework.Assert.IsNotNull(nodesListManagerEvent);
			NUnit.Framework.Assert.AreEqual(NodesListManagerEventType.NodeUsable, nodesListManagerEvent
				.GetType());
		}

		[NUnit.Framework.Test]
		public virtual void TestResourceUpdateOnNewNode()
		{
			RMNodeImpl node = GetNewNode(Resource.NewInstance(4096, 4));
			Resource oldCapacity = node.GetTotalCapability();
			NUnit.Framework.Assert.AreEqual("Memory resource is not match.", oldCapacity.GetMemory
				(), 4096);
			NUnit.Framework.Assert.AreEqual("CPU resource is not match.", oldCapacity.GetVirtualCores
				(), 4);
			node.Handle(new RMNodeResourceUpdateEvent(node.GetNodeID(), ResourceOption.NewInstance
				(Resource.NewInstance(2048, 2), RMNode.OverCommitTimeoutMillisDefault)));
			Resource newCapacity = node.GetTotalCapability();
			NUnit.Framework.Assert.AreEqual("Memory resource is not match.", newCapacity.GetMemory
				(), 2048);
			NUnit.Framework.Assert.AreEqual("CPU resource is not match.", newCapacity.GetVirtualCores
				(), 2);
			NUnit.Framework.Assert.AreEqual(NodeState.New, node.GetState());
		}

		[NUnit.Framework.Test]
		public virtual void TestResourceUpdateOnRebootedNode()
		{
			RMNodeImpl node = GetRebootedNode();
			Resource oldCapacity = node.GetTotalCapability();
			NUnit.Framework.Assert.AreEqual("Memory resource is not match.", oldCapacity.GetMemory
				(), 4096);
			NUnit.Framework.Assert.AreEqual("CPU resource is not match.", oldCapacity.GetVirtualCores
				(), 4);
			node.Handle(new RMNodeResourceUpdateEvent(node.GetNodeID(), ResourceOption.NewInstance
				(Resource.NewInstance(2048, 2), RMNode.OverCommitTimeoutMillisDefault)));
			Resource newCapacity = node.GetTotalCapability();
			NUnit.Framework.Assert.AreEqual("Memory resource is not match.", newCapacity.GetMemory
				(), 2048);
			NUnit.Framework.Assert.AreEqual("CPU resource is not match.", newCapacity.GetVirtualCores
				(), 2);
			NUnit.Framework.Assert.AreEqual(NodeState.Rebooted, node.GetState());
		}

		[NUnit.Framework.Test]
		public virtual void TestReconnnectUpdate()
		{
			string nmVersion1 = "nm version 1";
			string nmVersion2 = "nm version 2";
			RMNodeImpl node = GetRunningNode(nmVersion1);
			NUnit.Framework.Assert.AreEqual(nmVersion1, node.GetNodeManagerVersion());
			RMNodeImpl reconnectingNode = GetRunningNode(nmVersion2);
			node.Handle(new RMNodeReconnectEvent(node.GetNodeID(), reconnectingNode, null, null
				));
			NUnit.Framework.Assert.AreEqual(nmVersion2, node.GetNodeManagerVersion());
		}
	}
}
