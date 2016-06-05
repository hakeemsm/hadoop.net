using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	public class RMNodeFinishedContainersPulledByAMEvent : RMNodeEvent
	{
		private IList<ContainerId> containers;

		public RMNodeFinishedContainersPulledByAMEvent(NodeId nodeId, IList<ContainerId> 
			containers)
			: base(nodeId, RMNodeEventType.FinishedContainersPulledByAm)
		{
			// Happens after an implicit ack from AM that the container completion has
			// been notified successfully to the AM
			this.containers = containers;
		}

		public virtual IList<ContainerId> GetContainers()
		{
			return this.containers;
		}
	}
}
