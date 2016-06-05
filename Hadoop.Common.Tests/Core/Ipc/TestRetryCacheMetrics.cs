using Org.Apache.Hadoop.Ipc.Metrics;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// Tests for
	/// <see cref="Org.Apache.Hadoop.Ipc.Metrics.RetryCacheMetrics"/>
	/// </summary>
	public class TestRetryCacheMetrics
	{
		internal const string cacheName = "NameNodeRetryCache";

		[Fact]
		public virtual void TestNames()
		{
			RetryCache cache = Org.Mockito.Mockito.Mock<RetryCache>();
			Org.Mockito.Mockito.When(cache.GetCacheName()).ThenReturn(cacheName);
			RetryCacheMetrics metrics = RetryCacheMetrics.Create(cache);
			metrics.IncrCacheHit();
			metrics.IncrCacheCleared();
			metrics.IncrCacheCleared();
			metrics.IncrCacheUpdated();
			metrics.IncrCacheUpdated();
			metrics.IncrCacheUpdated();
			CheckMetrics(1, 2, 3);
		}

		private void CheckMetrics(long hit, long cleared, long updated)
		{
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics("RetryCache." + cacheName);
			MetricsAsserts.AssertCounter("CacheHit", hit, rb);
			MetricsAsserts.AssertCounter("CacheCleared", cleared, rb);
			MetricsAsserts.AssertCounter("CacheUpdated", updated, rb);
		}
	}
}
