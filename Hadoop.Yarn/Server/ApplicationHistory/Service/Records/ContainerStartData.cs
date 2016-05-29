using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records
{
	/// <summary>
	/// The class contains the fields that can be determined when
	/// <code>RMContainer</code> starts, and that need to be stored persistently.
	/// </summary>
	public abstract class ContainerStartData
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ContainerStartData NewInstance(ContainerId containerId, Resource allocatedResource
			, NodeId assignedNode, Priority priority, long startTime)
		{
			ContainerStartData containerSD = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ContainerStartData
				>();
			containerSD.SetContainerId(containerId);
			containerSD.SetAllocatedResource(allocatedResource);
			containerSD.SetAssignedNode(assignedNode);
			containerSD.SetPriority(priority);
			containerSD.SetStartTime(startTime);
			return containerSD;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ContainerId GetContainerId();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetContainerId(ContainerId containerId);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract Resource GetAllocatedResource();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetAllocatedResource(Resource resource);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract NodeId GetAssignedNode();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetAssignedNode(NodeId nodeId);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract Priority GetPriority();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetPriority(Priority priority);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetStartTime();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetStartTime(long startTime);
	}
}
