using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Sharedcache
{
	public class SharedCacheUploadEvent : AbstractEvent<SharedCacheUploadEventType>
	{
		private readonly IDictionary<LocalResourceRequest, Path> resources;

		private readonly ContainerLaunchContext context;

		private readonly string user;

		public SharedCacheUploadEvent(IDictionary<LocalResourceRequest, Path> resources, 
			ContainerLaunchContext context, string user, SharedCacheUploadEventType eventType
			)
			: base(eventType)
		{
			this.resources = resources;
			this.context = context;
			this.user = user;
		}

		public virtual IDictionary<LocalResourceRequest, Path> GetResources()
		{
			return resources;
		}

		public virtual ContainerLaunchContext GetContainerLaunchContext()
		{
			return context;
		}

		public virtual string GetUser()
		{
			return user;
		}
	}
}
