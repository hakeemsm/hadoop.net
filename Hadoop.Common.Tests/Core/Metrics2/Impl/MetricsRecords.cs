using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Utility class mainly for tests</summary>
	public class MetricsRecords
	{
		public static void AssertTag(MetricsRecord record, string tagName, string expectedValue
			)
		{
			MetricsTag processIdTag = GetFirstTagByName(record, tagName);
			NUnit.Framework.Assert.IsNotNull(processIdTag);
			Assert.Equal(expectedValue, processIdTag.Value());
		}

		public static void AssertMetric(MetricsRecord record, string metricName, Number expectedValue
			)
		{
			AbstractMetric resourceLimitMetric = GetFirstMetricByName(record, metricName);
			NUnit.Framework.Assert.IsNotNull(resourceLimitMetric);
			Assert.Equal(expectedValue, resourceLimitMetric.Value());
		}

		private static MetricsTag GetFirstTagByName(MetricsRecord record, string name)
		{
			return Iterables.GetFirst(Iterables.Filter(record.Tags(), new MetricsRecords.MetricsTagPredicate
				(name)), null);
		}

		private static AbstractMetric GetFirstMetricByName(MetricsRecord record, string name
			)
		{
			return Iterables.GetFirst(Iterables.Filter(record.Metrics(), new MetricsRecords.AbstractMetricPredicate
				(name)), null);
		}

		private class MetricsTagPredicate : Predicate<MetricsTag>
		{
			private string tagName;

			public MetricsTagPredicate(string tagName)
			{
				this.tagName = tagName;
			}

			public virtual bool Apply(MetricsTag input)
			{
				return input.Name().Equals(tagName);
			}
		}

		private class AbstractMetricPredicate : Predicate<AbstractMetric>
		{
			private string metricName;

			public AbstractMetricPredicate(string metricName)
			{
				this.metricName = metricName;
			}

			public virtual bool Apply(AbstractMetric input)
			{
				return input.Name().Equals(metricName);
			}
		}
	}
}
