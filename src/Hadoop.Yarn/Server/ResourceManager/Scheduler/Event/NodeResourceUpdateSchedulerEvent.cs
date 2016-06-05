using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class NodeResourceUpdateSchedulerEvent : SchedulerEvent
	{
		private readonly RMNode rmNode;

		private readonly ResourceOption resourceOption;

		public NodeResourceUpdateSchedulerEvent(RMNode rmNode, ResourceOption resourceOption
			)
			: base(SchedulerEventType.NodeResourceUpdate)
		{
			this.rmNode = rmNode;
			this.resourceOption = resourceOption;
		}

		public virtual RMNode GetRMNode()
		{
			return rmNode;
		}

		public virtual ResourceOption GetResourceOption()
		{
			return resourceOption;
		}
	}
}
