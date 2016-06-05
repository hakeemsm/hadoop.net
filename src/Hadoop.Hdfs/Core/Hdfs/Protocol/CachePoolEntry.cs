using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Describes a Cache Pool entry.</summary>
	public class CachePoolEntry
	{
		private readonly CachePoolInfo info;

		private readonly CachePoolStats stats;

		public CachePoolEntry(CachePoolInfo info, CachePoolStats stats)
		{
			this.info = info;
			this.stats = stats;
		}

		public virtual CachePoolInfo GetInfo()
		{
			return info;
		}

		public virtual CachePoolStats GetStats()
		{
			return stats;
		}
	}
}
