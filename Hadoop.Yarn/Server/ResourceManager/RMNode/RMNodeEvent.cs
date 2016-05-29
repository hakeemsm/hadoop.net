using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	public class RMNodeEvent : AbstractEvent<RMNodeEventType>
	{
		private readonly NodeId nodeId;

		public RMNodeEvent(NodeId nodeId, RMNodeEventType type)
			: base(type)
		{
			this.nodeId = nodeId;
		}

		public virtual NodeId GetNodeId()
		{
			return this.nodeId;
		}
	}
}
