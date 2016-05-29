using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Ahs
{
	public class WritingApplicationHistoryEvent : AbstractEvent<WritingHistoryEventType
		>
	{
		public WritingApplicationHistoryEvent(WritingHistoryEventType type)
			: base(type)
		{
		}
	}
}
