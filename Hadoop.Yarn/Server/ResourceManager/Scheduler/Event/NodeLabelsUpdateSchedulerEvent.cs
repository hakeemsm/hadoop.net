using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class NodeLabelsUpdateSchedulerEvent : SchedulerEvent
	{
		private IDictionary<NodeId, ICollection<string>> nodeToLabels;

		public NodeLabelsUpdateSchedulerEvent(IDictionary<NodeId, ICollection<string>> nodeToLabels
			)
			: base(SchedulerEventType.NodeLabelsUpdate)
		{
			this.nodeToLabels = nodeToLabels;
		}

		public virtual IDictionary<NodeId, ICollection<string>> GetUpdatedNodeToLabels()
		{
			return nodeToLabels;
		}
	}
}
