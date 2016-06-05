using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class NodesListManagerEvent : AbstractEvent<NodesListManagerEventType>
	{
		private readonly RMNode node;

		public NodesListManagerEvent(NodesListManagerEventType type, RMNode node)
			: base(type)
		{
			this.node = node;
		}

		public virtual RMNode GetNode()
		{
			return node;
		}
	}
}
