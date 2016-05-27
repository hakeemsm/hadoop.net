using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc.Metrics
{
	/// <summary>
	/// This class is for maintaining the various RetryCache-related statistics
	/// and publishing them through the metrics interfaces.
	/// </summary>
	public class RetryCacheMetrics
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.Metrics.RetryCacheMetrics
			));

		internal readonly MetricsRegistry registry;

		internal readonly string name;

		internal RetryCacheMetrics(RetryCache retryCache)
		{
			name = "RetryCache." + retryCache.GetCacheName();
			registry = new MetricsRegistry(name);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Initialized " + registry);
			}
		}

		public virtual string GetName()
		{
			return name;
		}

		public static Org.Apache.Hadoop.Ipc.Metrics.RetryCacheMetrics Create(RetryCache cache
			)
		{
			Org.Apache.Hadoop.Ipc.Metrics.RetryCacheMetrics m = new Org.Apache.Hadoop.Ipc.Metrics.RetryCacheMetrics
				(cache);
			return DefaultMetricsSystem.Instance().Register(m.name, null, m);
		}

		internal MutableCounterLong cacheHit;

		internal MutableCounterLong cacheCleared;

		internal MutableCounterLong cacheUpdated;

		/// <summary>One cache hit event</summary>
		public virtual void IncrCacheHit()
		{
			cacheHit.Incr();
		}

		/// <summary>One cache cleared</summary>
		public virtual void IncrCacheCleared()
		{
			cacheCleared.Incr();
		}

		/// <summary>One cache updated</summary>
		public virtual void IncrCacheUpdated()
		{
			cacheUpdated.Incr();
		}

		public virtual long GetCacheHit()
		{
			return cacheHit.Value();
		}

		public virtual long GetCacheCleared()
		{
			return cacheCleared.Value();
		}

		public virtual long GetCacheUpdated()
		{
			return cacheUpdated.Value();
		}
	}
}
