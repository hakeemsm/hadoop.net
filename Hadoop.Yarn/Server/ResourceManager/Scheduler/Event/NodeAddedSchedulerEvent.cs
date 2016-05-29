using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class NodeAddedSchedulerEvent : SchedulerEvent
	{
		private readonly RMNode rmNode;

		private readonly IList<NMContainerStatus> containerReports;

		public NodeAddedSchedulerEvent(RMNode rmNode)
			: base(SchedulerEventType.NodeAdded)
		{
			this.rmNode = rmNode;
			this.containerReports = null;
		}

		public NodeAddedSchedulerEvent(RMNode rmNode, IList<NMContainerStatus> containerReports
			)
			: base(SchedulerEventType.NodeAdded)
		{
			this.rmNode = rmNode;
			this.containerReports = containerReports;
		}

		public virtual RMNode GetAddedRMNode()
		{
			return rmNode;
		}

		public virtual IList<NMContainerStatus> GetContainerReports()
		{
			return containerReports;
		}
	}
}
