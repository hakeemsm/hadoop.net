using NUnit.Framework;


namespace Org.Apache.Hadoop.Metrics.Spi
{
	public class TestOutputRecord : TestCase
	{
		public virtual void TestCopy()
		{
			AbstractMetricsContext.TagMap tags = new AbstractMetricsContext.TagMap();
			tags["tagkey"] = "tagval";
			AbstractMetricsContext.MetricMap metrics = new AbstractMetricsContext.MetricMap();
			metrics["metrickey"] = 123.4;
			OutputRecord r = new OutputRecord(tags, metrics);
			Assert.Equal(tags, r.GetTagsCopy());
			NUnit.Framework.Assert.AreNotSame(tags, r.GetTagsCopy());
			Assert.Equal(metrics, r.GetMetricsCopy());
			NUnit.Framework.Assert.AreNotSame(metrics, r.GetMetricsCopy());
		}
	}
}
