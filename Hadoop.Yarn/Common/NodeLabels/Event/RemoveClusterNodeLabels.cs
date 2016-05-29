using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels.Event
{
	public class RemoveClusterNodeLabels : NodeLabelsStoreEvent
	{
		private ICollection<string> labels;

		public RemoveClusterNodeLabels(ICollection<string> labels)
			: base(NodeLabelsStoreEventType.RemoveLabels)
		{
			this.labels = labels;
		}

		public virtual ICollection<string> GetLabels()
		{
			return labels;
		}
	}
}
