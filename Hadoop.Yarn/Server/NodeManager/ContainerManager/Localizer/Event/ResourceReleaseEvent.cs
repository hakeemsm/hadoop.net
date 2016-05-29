using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	public class ResourceReleaseEvent : ResourceEvent
	{
		private readonly ContainerId container;

		public ResourceReleaseEvent(LocalResourceRequest rsrc, ContainerId container)
			: base(rsrc, ResourceEventType.Release)
		{
			this.container = container;
		}

		public virtual ContainerId GetContainer()
		{
			return container;
		}
	}
}
