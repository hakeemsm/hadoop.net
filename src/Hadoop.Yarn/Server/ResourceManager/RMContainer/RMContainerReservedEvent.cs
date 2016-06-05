using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer
{
	/// <summary>The event signifying that a container has been reserved.</summary>
	/// <remarks>
	/// The event signifying that a container has been reserved.
	/// The event encapsulates information on the amount of reservation
	/// and the node on which the reservation is in effect.
	/// </remarks>
	public class RMContainerReservedEvent : RMContainerEvent
	{
		private readonly Resource reservedResource;

		private readonly NodeId reservedNode;

		private readonly Priority reservedPriority;

		public RMContainerReservedEvent(ContainerId containerId, Resource reservedResource
			, NodeId reservedNode, Priority reservedPriority)
			: base(containerId, RMContainerEventType.Reserved)
		{
			this.reservedResource = reservedResource;
			this.reservedNode = reservedNode;
			this.reservedPriority = reservedPriority;
		}

		public virtual Resource GetReservedResource()
		{
			return reservedResource;
		}

		public virtual NodeId GetReservedNode()
		{
			return reservedNode;
		}

		public virtual Priority GetReservedPriority()
		{
			return reservedPriority;
		}
	}
}
