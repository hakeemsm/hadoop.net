using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels.Event
{
	public class NodeLabelsStoreEvent : AbstractEvent<NodeLabelsStoreEventType>
	{
		public NodeLabelsStoreEvent(NodeLabelsStoreEventType type)
			: base(type)
		{
		}
	}
}
