using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class NodeRemovedSchedulerEvent : SchedulerEvent
	{
		private readonly RMNode rmNode;

		public NodeRemovedSchedulerEvent(RMNode rmNode)
			: base(SchedulerEventType.NodeRemoved)
		{
			this.rmNode = rmNode;
		}

		public virtual RMNode GetRemovedRMNode()
		{
			return rmNode;
		}
	}
}
