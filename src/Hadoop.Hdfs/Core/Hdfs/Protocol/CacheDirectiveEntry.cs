using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Describes a path-based cache directive entry.</summary>
	public class CacheDirectiveEntry
	{
		private readonly CacheDirectiveInfo info;

		private readonly CacheDirectiveStats stats;

		public CacheDirectiveEntry(CacheDirectiveInfo info, CacheDirectiveStats stats)
		{
			this.info = info;
			this.stats = stats;
		}

		public virtual CacheDirectiveInfo GetInfo()
		{
			return info;
		}

		public virtual CacheDirectiveStats GetStats()
		{
			return stats;
		}
	}
}
