using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public class ContainerResourceLocalizedEvent : ContainerResourceEvent
	{
		private readonly Path loc;

		public ContainerResourceLocalizedEvent(ContainerId container, LocalResourceRequest
			 rsrc, Path loc)
			: base(container, ContainerEventType.ResourceLocalized, rsrc)
		{
			this.loc = loc;
		}

		public virtual Path GetLocation()
		{
			return loc;
		}
	}
}
