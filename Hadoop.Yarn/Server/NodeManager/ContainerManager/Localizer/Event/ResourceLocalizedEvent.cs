using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	public class ResourceLocalizedEvent : ResourceEvent
	{
		private readonly long size;

		private readonly Path location;

		public ResourceLocalizedEvent(LocalResourceRequest rsrc, Path location, long size
			)
			: base(rsrc, ResourceEventType.Localized)
		{
			this.size = size;
			this.location = location;
		}

		public virtual Path GetLocation()
		{
			return location;
		}

		public virtual long GetSize()
		{
			return size;
		}
	}
}
