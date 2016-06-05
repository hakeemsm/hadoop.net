using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	public class ResourceEvent : AbstractEvent<ResourceEventType>
	{
		private readonly LocalResourceRequest rsrc;

		public ResourceEvent(LocalResourceRequest rsrc, ResourceEventType type)
			: base(type)
		{
			this.rsrc = rsrc;
		}

		public virtual LocalResourceRequest GetLocalResourceRequest()
		{
			return rsrc;
		}
	}
}
