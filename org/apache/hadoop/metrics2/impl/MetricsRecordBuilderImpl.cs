using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	internal class MetricsRecordBuilderImpl : org.apache.hadoop.metrics2.MetricsRecordBuilder
	{
		private readonly org.apache.hadoop.metrics2.MetricsCollector parent;

		private readonly long timestamp;

		private readonly org.apache.hadoop.metrics2.MetricsInfo recInfo;

		private readonly System.Collections.Generic.IList<org.apache.hadoop.metrics2.AbstractMetric
			> metrics;

		private readonly System.Collections.Generic.IList<org.apache.hadoop.metrics2.MetricsTag
			> tags;

		private readonly org.apache.hadoop.metrics2.MetricsFilter recordFilter;

		private readonly org.apache.hadoop.metrics2.MetricsFilter metricFilter;

		private readonly bool acceptable;

		internal MetricsRecordBuilderImpl(org.apache.hadoop.metrics2.MetricsCollector parent
			, org.apache.hadoop.metrics2.MetricsInfo info, org.apache.hadoop.metrics2.MetricsFilter
			 rf, org.apache.hadoop.metrics2.MetricsFilter mf, bool acceptable)
		{
			this.parent = parent;
			timestamp = org.apache.hadoop.util.Time.now();
			recInfo = info;
			metrics = com.google.common.collect.Lists.newArrayList();
			tags = com.google.common.collect.Lists.newArrayList();
			recordFilter = rf;
			metricFilter = mf;
			this.acceptable = acceptable;
		}

		public override org.apache.hadoop.metrics2.MetricsCollector parent()
		{
			return parent;
		}

		public override org.apache.hadoop.metrics2.MetricsRecordBuilder tag(org.apache.hadoop.metrics2.MetricsInfo
			 info, string value)
		{
			if (acceptable)
			{
				tags.add(org.apache.hadoop.metrics2.lib.Interns.tag(info, value));
			}
			return this;
		}

		public override org.apache.hadoop.metrics2.MetricsRecordBuilder add(org.apache.hadoop.metrics2.MetricsTag
			 tag)
		{
			tags.add(tag);
			return this;
		}

		public override org.apache.hadoop.metrics2.MetricsRecordBuilder add(org.apache.hadoop.metrics2.AbstractMetric
			 metric)
		{
			metrics.add(metric);
			return this;
		}

		public override org.apache.hadoop.metrics2.MetricsRecordBuilder addCounter(org.apache.hadoop.metrics2.MetricsInfo
			 info, int value)
		{
			if (acceptable && (metricFilter == null || metricFilter.accepts(info.name())))
			{
				metrics.add(new org.apache.hadoop.metrics2.impl.MetricCounterInt(info, value));
			}
			return this;
		}

		public override org.apache.hadoop.metrics2.MetricsRecordBuilder addCounter(org.apache.hadoop.metrics2.MetricsInfo
			 info, long value)
		{
			if (acceptable && (metricFilter == null || metricFilter.accepts(info.name())))
			{
				metrics.add(new org.apache.hadoop.metrics2.impl.MetricCounterLong(info, value));
			}
			return this;
		}

		public override org.apache.hadoop.metrics2.MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo
			 info, int value)
		{
			if (acceptable && (metricFilter == null || metricFilter.accepts(info.name())))
			{
				metrics.add(new org.apache.hadoop.metrics2.impl.MetricGaugeInt(info, value));
			}
			return this;
		}

		public override org.apache.hadoop.metrics2.MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo
			 info, long value)
		{
			if (acceptable && (metricFilter == null || metricFilter.accepts(info.name())))
			{
				metrics.add(new org.apache.hadoop.metrics2.impl.MetricGaugeLong(info, value));
			}
			return this;
		}

		public override org.apache.hadoop.metrics2.MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo
			 info, float value)
		{
			if (acceptable && (metricFilter == null || metricFilter.accepts(info.name())))
			{
				metrics.add(new org.apache.hadoop.metrics2.impl.MetricGaugeFloat(info, value));
			}
			return this;
		}

		public override org.apache.hadoop.metrics2.MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo
			 info, double value)
		{
			if (acceptable && (metricFilter == null || metricFilter.accepts(info.name())))
			{
				metrics.add(new org.apache.hadoop.metrics2.impl.MetricGaugeDouble(info, value));
			}
			return this;
		}

		public override org.apache.hadoop.metrics2.MetricsRecordBuilder setContext(string
			 value)
		{
			return ((org.apache.hadoop.metrics2.impl.MetricsRecordBuilderImpl)tag(org.apache.hadoop.metrics2.impl.MsInfo
				.Context, value));
		}

		public virtual org.apache.hadoop.metrics2.impl.MetricsRecordImpl getRecord()
		{
			if (acceptable && (recordFilter == null || recordFilter.accepts(tags)))
			{
				return new org.apache.hadoop.metrics2.impl.MetricsRecordImpl(recInfo, timestamp, 
					tags(), metrics());
			}
			return null;
		}

		internal virtual System.Collections.Generic.IList<org.apache.hadoop.metrics2.MetricsTag
			> tags()
		{
			return java.util.Collections.unmodifiableList(tags);
		}

		internal virtual System.Collections.Generic.IList<org.apache.hadoop.metrics2.AbstractMetric
			> metrics()
		{
			return java.util.Collections.unmodifiableList(metrics);
		}
	}
}
