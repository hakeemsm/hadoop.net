using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels.Event
{
	public class StoreNewClusterNodeLabels : NodeLabelsStoreEvent
	{
		private ICollection<string> labels;

		public StoreNewClusterNodeLabels(ICollection<string> labels)
			: base(NodeLabelsStoreEventType.AddLabels)
		{
			this.labels = labels;
		}

		public virtual ICollection<string> GetLabels()
		{
			return labels;
		}
	}
}
