using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	public class ContainerCreatedEvent : SystemMetricsEvent
	{
		private ContainerId containerId;

		private Resource allocatedResource;

		private NodeId allocatedNode;

		private Priority allocatedPriority;

		private string nodeHttpAddress;

		public ContainerCreatedEvent(ContainerId containerId, Resource allocatedResource, 
			NodeId allocatedNode, Priority allocatedPriority, long createdTime, string nodeHttpAddress
			)
			: base(SystemMetricsEventType.ContainerCreated, createdTime)
		{
			this.containerId = containerId;
			this.allocatedResource = allocatedResource;
			this.allocatedNode = allocatedNode;
			this.allocatedPriority = allocatedPriority;
			this.nodeHttpAddress = nodeHttpAddress;
		}

		public override int GetHashCode()
		{
			return containerId.GetApplicationAttemptId().GetApplicationId().GetHashCode();
		}

		public virtual ContainerId GetContainerId()
		{
			return containerId;
		}

		public virtual Resource GetAllocatedResource()
		{
			return allocatedResource;
		}

		public virtual NodeId GetAllocatedNode()
		{
			return allocatedNode;
		}

		public virtual Priority GetAllocatedPriority()
		{
			return allocatedPriority;
		}

		public virtual string GetNodeHttpAddress()
		{
			return nodeHttpAddress;
		}
	}
}
