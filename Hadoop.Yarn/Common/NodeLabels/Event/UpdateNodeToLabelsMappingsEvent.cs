using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels.Event
{
	public class UpdateNodeToLabelsMappingsEvent : NodeLabelsStoreEvent
	{
		private IDictionary<NodeId, ICollection<string>> nodeToLabels;

		public UpdateNodeToLabelsMappingsEvent(IDictionary<NodeId, ICollection<string>> nodeToLabels
			)
			: base(NodeLabelsStoreEventType.StoreNodeToLabels)
		{
			this.nodeToLabels = nodeToLabels;
		}

		public virtual IDictionary<NodeId, ICollection<string>> GetNodeToLabels()
		{
			return nodeToLabels;
		}
	}
}
