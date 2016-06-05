using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer
{
	public class RMContainerEvent : AbstractEvent<RMContainerEventType>
	{
		private readonly ContainerId containerId;

		public RMContainerEvent(ContainerId containerId, RMContainerEventType type)
			: base(type)
		{
			this.containerId = containerId;
		}

		public virtual ContainerId GetContainerId()
		{
			return this.containerId;
		}
	}
}
