using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public class ContainerEvent : AbstractEvent<ContainerEventType>
	{
		private readonly ContainerId containerID;

		public ContainerEvent(ContainerId cID, ContainerEventType eventType)
			: base(eventType)
		{
			this.containerID = cID;
		}

		public virtual ContainerId GetContainerID()
		{
			return containerID;
		}
	}
}
