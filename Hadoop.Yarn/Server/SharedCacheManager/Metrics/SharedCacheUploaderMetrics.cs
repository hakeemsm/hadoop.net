using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics
{
	/// <summary>
	/// This class is for maintaining shared cache uploader requests metrics
	/// and publishing them through the metrics interfaces.
	/// </summary>
	public class SharedCacheUploaderMetrics
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.SharedCacheUploaderMetrics
			));

		internal readonly MetricsRegistry registry;

		private static readonly Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.SharedCacheUploaderMetrics
			 Instance = Create();

		private SharedCacheUploaderMetrics()
		{
			registry = new MetricsRegistry("SharedCacheUploaderRequests");
			Log.Debug("Initialized " + registry);
		}

		public static Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.SharedCacheUploaderMetrics
			 GetInstance()
		{
			return Instance;
		}

		internal static Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.SharedCacheUploaderMetrics
			 Create()
		{
			MetricsSystem ms = DefaultMetricsSystem.Instance();
			Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.SharedCacheUploaderMetrics
				 metrics = new Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.SharedCacheUploaderMetrics
				();
			ms.Register("SharedCacheUploaderRequests", null, metrics);
			return metrics;
		}

		internal MutableCounterLong acceptedUploads;

		internal MutableCounterLong rejectedUploads;

		/// <summary>One accepted upload event</summary>
		public virtual void IncAcceptedUploads()
		{
			acceptedUploads.Incr();
		}

		/// <summary>One rejected upload event</summary>
		public virtual void IncRejectedUploads()
		{
			rejectedUploads.Incr();
		}

		public virtual long GetAcceptedUploads()
		{
			return acceptedUploads.Value();
		}

		public virtual long GetRejectUploads()
		{
			return rejectedUploads.Value();
		}
	}
}
