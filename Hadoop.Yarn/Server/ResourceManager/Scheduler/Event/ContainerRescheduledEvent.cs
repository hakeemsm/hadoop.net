using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event
{
	public class ContainerRescheduledEvent : SchedulerEvent
	{
		private RMContainer container;

		public ContainerRescheduledEvent(RMContainer container)
			: base(SchedulerEventType.ContainerRescheduled)
		{
			this.container = container;
		}

		public virtual RMContainer GetContainer()
		{
			return container;
		}
	}
}
