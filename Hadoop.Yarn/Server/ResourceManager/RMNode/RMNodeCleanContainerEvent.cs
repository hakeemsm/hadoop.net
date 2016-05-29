using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	public class RMNodeCleanContainerEvent : RMNodeEvent
	{
		private ContainerId contId;

		public RMNodeCleanContainerEvent(NodeId nodeId, ContainerId contId)
			: base(nodeId, RMNodeEventType.CleanupContainer)
		{
			this.contId = contId;
		}

		public virtual ContainerId GetContainerId()
		{
			return this.contId;
		}
	}
}
