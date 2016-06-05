using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	public class ResourceRequestEvent : ResourceEvent
	{
		private readonly LocalizerContext context;

		private readonly LocalResourceVisibility vis;

		public ResourceRequestEvent(LocalResourceRequest resource, LocalResourceVisibility
			 vis, LocalizerContext context)
			: base(resource, ResourceEventType.Request)
		{
			this.vis = vis;
			this.context = context;
		}

		public virtual LocalizerContext GetContext()
		{
			return context;
		}

		public virtual LocalResourceVisibility GetVisibility()
		{
			return vis;
		}
	}
}
