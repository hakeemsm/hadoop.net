using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store
{
	/// <summary>Class that encapsulates the cache resource.</summary>
	/// <remarks>
	/// Class that encapsulates the cache resource. The instances are not thread
	/// safe. Any operation that uses the resource must use thread-safe mechanisms to
	/// ensure safe access with the only exception of the filename.
	/// </remarks>
	internal class SharedCacheResource
	{
		private long accessTime;

		private readonly ICollection<SharedCacheResourceReference> refs;

		private readonly string fileName;

		internal SharedCacheResource(string fileName)
		{
			this.accessTime = Runtime.CurrentTimeMillis();
			this.refs = new HashSet<SharedCacheResourceReference>();
			this.fileName = fileName;
		}

		internal virtual long GetAccessTime()
		{
			return accessTime;
		}

		internal virtual void UpdateAccessTime()
		{
			accessTime = Runtime.CurrentTimeMillis();
		}

		internal virtual string GetFileName()
		{
			return this.fileName;
		}

		internal virtual ICollection<SharedCacheResourceReference> GetResourceReferences(
			)
		{
			return this.refs;
		}

		internal virtual bool AddReference(SharedCacheResourceReference @ref)
		{
			return this.refs.AddItem(@ref);
		}
	}
}
