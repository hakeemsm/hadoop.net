using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public class ContainerResourceEvent : ContainerEvent
	{
		private readonly LocalResourceRequest rsrc;

		public ContainerResourceEvent(ContainerId container, ContainerEventType type, LocalResourceRequest
			 rsrc)
			: base(container, type)
		{
			this.rsrc = rsrc;
		}

		public virtual LocalResourceRequest GetResource()
		{
			return rsrc;
		}
	}
}
