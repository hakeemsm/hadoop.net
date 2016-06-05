using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class ResourceRetentionSet
	{
		private long delSize;

		private long currentSize;

		private readonly long targetSize;

		private readonly DeletionService delService;

		private readonly SortedDictionary<LocalizedResource, LocalResourcesTracker> retain;

		internal ResourceRetentionSet(DeletionService delService, long targetSize)
			: this(delService, targetSize, new ResourceRetentionSet.LRUComparator())
		{
		}

		internal ResourceRetentionSet(DeletionService delService, long targetSize, IComparer
			<LocalizedResource> cmp)
			: this(delService, targetSize, new SortedDictionary<LocalizedResource, LocalResourcesTracker
				>(cmp))
		{
		}

		internal ResourceRetentionSet(DeletionService delService, long targetSize, SortedDictionary
			<LocalizedResource, LocalResourcesTracker> retain)
		{
			this.retain = retain;
			this.delService = delService;
			this.targetSize = targetSize;
		}

		public virtual void AddResources(LocalResourcesTracker newTracker)
		{
			foreach (LocalizedResource resource in newTracker)
			{
				currentSize += resource.GetSize();
				if (resource.GetRefCount() > 0)
				{
					// always retain resources in use
					continue;
				}
				retain[resource] = newTracker;
			}
			for (IEnumerator<KeyValuePair<LocalizedResource, LocalResourcesTracker>> i = retain
				.GetEnumerator(); currentSize - delSize > targetSize && i.HasNext(); )
			{
				KeyValuePair<LocalizedResource, LocalResourcesTracker> rsrc = i.Next();
				LocalizedResource resource_1 = rsrc.Key;
				LocalResourcesTracker tracker = rsrc.Value;
				if (tracker.Remove(resource_1, delService))
				{
					delSize += resource_1.GetSize();
					i.Remove();
				}
			}
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("Cache: ").Append(currentSize).Append(", ");
			sb.Append("Deleted: ").Append(delSize);
			return sb.ToString();
		}

		internal class LRUComparator : IComparer<LocalizedResource>
		{
			public virtual int Compare(LocalizedResource r1, LocalizedResource r2)
			{
				long ret = r1.GetTimestamp() - r2.GetTimestamp();
				if (0 == ret)
				{
					return Runtime.IdentityHashCode(r1) - Runtime.IdentityHashCode(r2);
				}
				return ret > 0 ? 1 : -1;
			}

			public override bool Equals(object other)
			{
				return this == other;
			}
		}
	}
}
