using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateStoreEvent : AbstractEvent<RMStateStoreEventType>
	{
		public RMStateStoreEvent(RMStateStoreEventType type)
			: base(type)
		{
		}
	}
}
