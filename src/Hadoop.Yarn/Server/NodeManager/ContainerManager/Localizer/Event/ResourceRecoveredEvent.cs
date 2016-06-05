using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	public class ResourceRecoveredEvent : ResourceEvent
	{
		private readonly Path localPath;

		private readonly long size;

		public ResourceRecoveredEvent(LocalResourceRequest rsrc, Path localPath, long size
			)
			: base(rsrc, ResourceEventType.Recovered)
		{
			this.localPath = localPath;
			this.size = size;
		}

		public virtual Path GetLocalPath()
		{
			return localPath;
		}

		public virtual long GetSize()
		{
			return size;
		}
	}
}
