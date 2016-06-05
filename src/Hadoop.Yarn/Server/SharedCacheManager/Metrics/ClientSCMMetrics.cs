using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics
{
	/// <summary>
	/// This class is for maintaining  client requests metrics
	/// and publishing them through the metrics interfaces.
	/// </summary>
	public class ClientSCMMetrics
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.ClientSCMMetrics
			));

		internal readonly MetricsRegistry registry;

		private static readonly Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.ClientSCMMetrics
			 Instance = Create();

		private ClientSCMMetrics()
		{
			registry = new MetricsRegistry("clientRequests");
			Log.Debug("Initialized " + registry);
		}

		public static Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.ClientSCMMetrics
			 GetInstance()
		{
			return Instance;
		}

		internal static Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.ClientSCMMetrics
			 Create()
		{
			MetricsSystem ms = DefaultMetricsSystem.Instance();
			Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.ClientSCMMetrics metrics
				 = new Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.ClientSCMMetrics
				();
			ms.Register("clientRequests", null, metrics);
			return metrics;
		}

		internal MutableCounterLong cacheHits;

		internal MutableCounterLong cacheMisses;

		internal MutableCounterLong cacheReleases;

		/// <summary>One cache hit event</summary>
		public virtual void IncCacheHitCount()
		{
			cacheHits.Incr();
		}

		/// <summary>One cache miss event</summary>
		public virtual void IncCacheMissCount()
		{
			cacheMisses.Incr();
		}

		/// <summary>One cache release event</summary>
		public virtual void IncCacheRelease()
		{
			cacheReleases.Incr();
		}

		public virtual long GetCacheHits()
		{
			return cacheHits.Value();
		}

		public virtual long GetCacheMisses()
		{
			return cacheMisses.Value();
		}

		public virtual long GetCacheReleases()
		{
			return cacheReleases.Value();
		}
	}
}
