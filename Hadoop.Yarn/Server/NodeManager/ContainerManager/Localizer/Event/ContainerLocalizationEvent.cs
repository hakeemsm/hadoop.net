using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	public class ContainerLocalizationEvent : LocalizationEvent
	{
		internal readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container;

		public ContainerLocalizationEvent(LocalizationEventType @event, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 c)
			: base(@event)
		{
			this.container = c;
		}

		public virtual Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 GetContainer()
		{
			return container;
		}
	}
}
