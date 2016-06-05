using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public class TestSchedulerApplicationAttempt
	{
		private static readonly NodeId nodeId = NodeId.NewInstance("somehost", 5);

		private Configuration conf = new Configuration();

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			QueueMetrics.ClearQueueMetrics();
			DefaultMetricsSystem.Shutdown();
		}

		[NUnit.Framework.Test]
		public virtual void TestMove()
		{
			string user = "user1";
			Queue parentQueue = CreateQueue("parent", null);
			Queue oldQueue = CreateQueue("old", parentQueue);
			Queue newQueue = CreateQueue("new", parentQueue);
			QueueMetrics parentMetrics = parentQueue.GetMetrics();
			QueueMetrics oldMetrics = oldQueue.GetMetrics();
			QueueMetrics newMetrics = newQueue.GetMetrics();
			ApplicationAttemptId appAttId = CreateAppAttemptId(0, 0);
			RMContext rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetEpoch()).ThenReturn(3L);
			SchedulerApplicationAttempt app = new SchedulerApplicationAttempt(appAttId, user, 
				oldQueue, oldQueue.GetActiveUsersManager(), rmContext);
			oldMetrics.SubmitApp(user);
			// confirm that containerId is calculated based on epoch.
			NUnit.Framework.Assert.AreEqual(unchecked((long)(0x30000000001L)), app.GetNewContainerId
				());
			// Resource request
			Resource requestedResource = Resource.NewInstance(1536, 2);
			Priority requestedPriority = Priority.NewInstance(2);
			ResourceRequest request = ResourceRequest.NewInstance(requestedPriority, ResourceRequest
				.Any, requestedResource, 3);
			app.UpdateResourceRequests(Arrays.AsList(request));
			// Allocated container
			RMContainer container1 = CreateRMContainer(appAttId, 1, requestedResource);
			app.liveContainers[container1.GetContainerId()] = container1;
			SchedulerNode node = CreateNode();
			app.appSchedulingInfo.Allocate(NodeType.OffSwitch, node, requestedPriority, request
				, container1.GetContainer());
			// Reserved container
			Priority prio1 = Priority.NewInstance(1);
			Resource reservedResource = Resource.NewInstance(2048, 3);
			RMContainer container2 = CreateReservedRMContainer(appAttId, 1, reservedResource, 
				node.GetNodeID(), prio1);
			IDictionary<NodeId, RMContainer> reservations = new Dictionary<NodeId, RMContainer
				>();
			reservations[node.GetNodeID()] = container2;
			app.reservedContainers[prio1] = reservations;
			oldMetrics.ReserveResource(user, reservedResource);
			CheckQueueMetrics(oldMetrics, 1, 1, 1536, 2, 2048, 3, 3072, 4);
			CheckQueueMetrics(newMetrics, 0, 0, 0, 0, 0, 0, 0, 0);
			CheckQueueMetrics(parentMetrics, 1, 1, 1536, 2, 2048, 3, 3072, 4);
			app.Move(newQueue);
			CheckQueueMetrics(oldMetrics, 0, 0, 0, 0, 0, 0, 0, 0);
			CheckQueueMetrics(newMetrics, 1, 1, 1536, 2, 2048, 3, 3072, 4);
			CheckQueueMetrics(parentMetrics, 1, 1, 1536, 2, 2048, 3, 3072, 4);
		}

		private void CheckQueueMetrics(QueueMetrics metrics, int activeApps, int runningApps
			, int allocMb, int allocVcores, int reservedMb, int reservedVcores, int pendingMb
			, int pendingVcores)
		{
			NUnit.Framework.Assert.AreEqual(activeApps, metrics.GetActiveApps());
			NUnit.Framework.Assert.AreEqual(runningApps, metrics.GetAppsRunning());
			NUnit.Framework.Assert.AreEqual(allocMb, metrics.GetAllocatedMB());
			NUnit.Framework.Assert.AreEqual(allocVcores, metrics.GetAllocatedVirtualCores());
			NUnit.Framework.Assert.AreEqual(reservedMb, metrics.GetReservedMB());
			NUnit.Framework.Assert.AreEqual(reservedVcores, metrics.GetReservedVirtualCores()
				);
			NUnit.Framework.Assert.AreEqual(pendingMb, metrics.GetPendingMB());
			NUnit.Framework.Assert.AreEqual(pendingVcores, metrics.GetPendingVirtualCores());
		}

		private SchedulerNode CreateNode()
		{
			SchedulerNode node = Org.Mockito.Mockito.Mock<SchedulerNode>();
			Org.Mockito.Mockito.When(node.GetNodeName()).ThenReturn("somehost");
			Org.Mockito.Mockito.When(node.GetRackName()).ThenReturn("somerack");
			Org.Mockito.Mockito.When(node.GetNodeID()).ThenReturn(nodeId);
			return node;
		}

		private RMContainer CreateReservedRMContainer(ApplicationAttemptId appAttId, int 
			id, Resource resource, NodeId nodeId, Priority reservedPriority)
		{
			RMContainer container = CreateRMContainer(appAttId, id, resource);
			Org.Mockito.Mockito.When(container.GetReservedResource()).ThenReturn(resource);
			Org.Mockito.Mockito.When(container.GetReservedPriority()).ThenReturn(reservedPriority
				);
			Org.Mockito.Mockito.When(container.GetReservedNode()).ThenReturn(nodeId);
			return container;
		}

		private RMContainer CreateRMContainer(ApplicationAttemptId appAttId, int id, Resource
			 resource)
		{
			ContainerId containerId = ContainerId.NewContainerId(appAttId, id);
			RMContainer rmContainer = Org.Mockito.Mockito.Mock<RMContainer>();
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetResource()).ThenReturn(resource);
			Org.Mockito.Mockito.When(container.GetNodeId()).ThenReturn(nodeId);
			Org.Mockito.Mockito.When(rmContainer.GetContainer()).ThenReturn(container);
			Org.Mockito.Mockito.When(rmContainer.GetContainerId()).ThenReturn(containerId);
			return rmContainer;
		}

		private Queue CreateQueue(string name, Queue parent)
		{
			QueueMetrics metrics = QueueMetrics.ForQueue(name, parent, false, conf);
			ActiveUsersManager activeUsersManager = new ActiveUsersManager(metrics);
			Queue queue = Org.Mockito.Mockito.Mock<Queue>();
			Org.Mockito.Mockito.When(queue.GetMetrics()).ThenReturn(metrics);
			Org.Mockito.Mockito.When(queue.GetActiveUsersManager()).ThenReturn(activeUsersManager
				);
			return queue;
		}

		private ApplicationAttemptId CreateAppAttemptId(int appId, int attemptId)
		{
			ApplicationId appIdImpl = ApplicationId.NewInstance(0, appId);
			ApplicationAttemptId attId = ApplicationAttemptId.NewInstance(appIdImpl, attemptId
				);
			return attId;
		}
	}
}
