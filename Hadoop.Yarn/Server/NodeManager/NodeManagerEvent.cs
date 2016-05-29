using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class NodeManagerEvent : AbstractEvent<NodeManagerEventType>
	{
		public NodeManagerEvent(NodeManagerEventType type)
			: base(type)
		{
		}
	}
}
