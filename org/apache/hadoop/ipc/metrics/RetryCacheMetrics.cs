using Sharpen;

namespace org.apache.hadoop.ipc.metrics
{
	/// <summary>
	/// This class is for maintaining the various RetryCache-related statistics
	/// and publishing them through the metrics interfaces.
	/// </summary>
	public class RetryCacheMetrics
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.metrics.RetryCacheMetrics
			)));

		internal readonly org.apache.hadoop.metrics2.lib.MetricsRegistry registry;

		internal readonly string name;

		internal RetryCacheMetrics(org.apache.hadoop.ipc.RetryCache retryCache)
		{
			name = "RetryCache." + retryCache.getCacheName();
			registry = new org.apache.hadoop.metrics2.lib.MetricsRegistry(name);
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Initialized " + registry);
			}
		}

		public virtual string getName()
		{
			return name;
		}

		public static org.apache.hadoop.ipc.metrics.RetryCacheMetrics create(org.apache.hadoop.ipc.RetryCache
			 cache)
		{
			org.apache.hadoop.ipc.metrics.RetryCacheMetrics m = new org.apache.hadoop.ipc.metrics.RetryCacheMetrics
				(cache);
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.instance().register(m.
				name, null, m);
		}

		internal org.apache.hadoop.metrics2.lib.MutableCounterLong cacheHit;

		internal org.apache.hadoop.metrics2.lib.MutableCounterLong cacheCleared;

		internal org.apache.hadoop.metrics2.lib.MutableCounterLong cacheUpdated;

		/// <summary>One cache hit event</summary>
		public virtual void incrCacheHit()
		{
			cacheHit.incr();
		}

		/// <summary>One cache cleared</summary>
		public virtual void incrCacheCleared()
		{
			cacheCleared.incr();
		}

		/// <summary>One cache updated</summary>
		public virtual void incrCacheUpdated()
		{
			cacheUpdated.incr();
		}

		public virtual long getCacheHit()
		{
			return cacheHit.value();
		}

		public virtual long getCacheCleared()
		{
			return cacheCleared.value();
		}

		public virtual long getCacheUpdated()
		{
			return cacheUpdated.value();
		}
	}
}
