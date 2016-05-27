using NUnit.Framework;
using Sharpen;

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
			NUnit.Framework.Assert.AreEqual(tags, r.GetTagsCopy());
			NUnit.Framework.Assert.AreNotSame(tags, r.GetTagsCopy());
			NUnit.Framework.Assert.AreEqual(metrics, r.GetMetricsCopy());
			NUnit.Framework.Assert.AreNotSame(metrics, r.GetMetricsCopy());
		}
	}
}
