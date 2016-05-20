using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// Tests for
	/// <see cref="org.apache.hadoop.ipc.metrics.RetryCacheMetrics"/>
	/// </summary>
	public class TestRetryCacheMetrics
	{
		internal const string cacheName = "NameNodeRetryCache";

		[NUnit.Framework.Test]
		public virtual void testNames()
		{
			org.apache.hadoop.ipc.RetryCache cache = org.mockito.Mockito.mock<org.apache.hadoop.ipc.RetryCache
				>();
			org.mockito.Mockito.when(cache.getCacheName()).thenReturn(cacheName);
			org.apache.hadoop.ipc.metrics.RetryCacheMetrics metrics = org.apache.hadoop.ipc.metrics.RetryCacheMetrics
				.create(cache);
			metrics.incrCacheHit();
			metrics.incrCacheCleared();
			metrics.incrCacheCleared();
			metrics.incrCacheUpdated();
			metrics.incrCacheUpdated();
			metrics.incrCacheUpdated();
			checkMetrics(1, 2, 3);
		}

		private void checkMetrics(long hit, long cleared, long updated)
		{
			org.apache.hadoop.metrics2.MetricsRecordBuilder rb = org.apache.hadoop.test.MetricsAsserts.getMetrics
				("RetryCache." + cacheName);
			org.apache.hadoop.test.MetricsAsserts.assertCounter("CacheHit", hit, rb);
			org.apache.hadoop.test.MetricsAsserts.assertCounter("CacheCleared", cleared, rb);
			org.apache.hadoop.test.MetricsAsserts.assertCounter("CacheUpdated", updated, rb);
		}
	}
}
