using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher
{
	public class ContainersLauncherEvent : AbstractEvent<ContainersLauncherEventType>
	{
		private readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container;

		public ContainersLauncherEvent(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container, ContainersLauncherEventType eventType)
			: base(eventType)
		{
			this.container = container;
		}

		public virtual Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 GetContainer()
		{
			return container;
		}
	}
}
