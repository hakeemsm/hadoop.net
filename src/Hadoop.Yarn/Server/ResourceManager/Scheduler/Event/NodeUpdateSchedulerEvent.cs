using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class NodeUpdateSchedulerEvent : SchedulerEvent
	{
		private readonly RMNode rmNode;

		public NodeUpdateSchedulerEvent(RMNode rmNode)
			: base(SchedulerEventType.NodeUpdate)
		{
			this.rmNode = rmNode;
		}

		public virtual RMNode GetRMNode()
		{
			return rmNode;
		}
	}
}
