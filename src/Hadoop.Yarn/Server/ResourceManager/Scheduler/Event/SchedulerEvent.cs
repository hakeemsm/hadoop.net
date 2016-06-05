using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class SchedulerEvent : AbstractEvent<SchedulerEventType>
	{
		public SchedulerEvent(SchedulerEventType type)
			: base(type)
		{
		}
	}
}
