using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	internal class MetricsRecordBuilderImpl : MetricsRecordBuilder
	{
		private readonly MetricsCollector parent;

		private readonly long timestamp;

		private readonly MetricsInfo recInfo;

		private readonly IList<AbstractMetric> metrics;

		private readonly IList<MetricsTag> tags;

		private readonly MetricsFilter recordFilter;

		private readonly MetricsFilter metricFilter;

		private readonly bool acceptable;

		internal MetricsRecordBuilderImpl(MetricsCollector parent, MetricsInfo info, MetricsFilter
			 rf, MetricsFilter mf, bool acceptable)
		{
			this.parent = parent;
			timestamp = Time.Now();
			recInfo = info;
			metrics = Lists.NewArrayList();
			tags = Lists.NewArrayList();
			recordFilter = rf;
			metricFilter = mf;
			this.acceptable = acceptable;
		}

		public override MetricsCollector Parent()
		{
			return parent;
		}

		public override MetricsRecordBuilder Tag(MetricsInfo info, string value)
		{
			if (acceptable)
			{
				tags.AddItem(Interns.Tag(info, value));
			}
			return this;
		}

		public override MetricsRecordBuilder Add(MetricsTag tag)
		{
			tags.AddItem(tag);
			return this;
		}

		public override MetricsRecordBuilder Add(AbstractMetric metric)
		{
			metrics.AddItem(metric);
			return this;
		}

		public override MetricsRecordBuilder AddCounter(MetricsInfo info, int value)
		{
			if (acceptable && (metricFilter == null || metricFilter.Accepts(info.Name())))
			{
				metrics.AddItem(new MetricCounterInt(info, value));
			}
			return this;
		}

		public override MetricsRecordBuilder AddCounter(MetricsInfo info, long value)
		{
			if (acceptable && (metricFilter == null || metricFilter.Accepts(info.Name())))
			{
				metrics.AddItem(new MetricCounterLong(info, value));
			}
			return this;
		}

		public override MetricsRecordBuilder AddGauge(MetricsInfo info, int value)
		{
			if (acceptable && (metricFilter == null || metricFilter.Accepts(info.Name())))
			{
				metrics.AddItem(new MetricGaugeInt(info, value));
			}
			return this;
		}

		public override MetricsRecordBuilder AddGauge(MetricsInfo info, long value)
		{
			if (acceptable && (metricFilter == null || metricFilter.Accepts(info.Name())))
			{
				metrics.AddItem(new MetricGaugeLong(info, value));
			}
			return this;
		}

		public override MetricsRecordBuilder AddGauge(MetricsInfo info, float value)
		{
			if (acceptable && (metricFilter == null || metricFilter.Accepts(info.Name())))
			{
				metrics.AddItem(new MetricGaugeFloat(info, value));
			}
			return this;
		}

		public override MetricsRecordBuilder AddGauge(MetricsInfo info, double value)
		{
			if (acceptable && (metricFilter == null || metricFilter.Accepts(info.Name())))
			{
				metrics.AddItem(new MetricGaugeDouble(info, value));
			}
			return this;
		}

		public override MetricsRecordBuilder SetContext(string value)
		{
			return ((Org.Apache.Hadoop.Metrics2.Impl.MetricsRecordBuilderImpl)Tag(MsInfo.Context
				, value));
		}

		public virtual MetricsRecordImpl GetRecord()
		{
			if (acceptable && (recordFilter == null || recordFilter.Accepts(tags)))
			{
				return new MetricsRecordImpl(recInfo, timestamp, Tags(), Metrics());
			}
			return null;
		}

		internal virtual IList<MetricsTag> Tags()
		{
			return Collections.UnmodifiableList(tags);
		}

		internal virtual IList<AbstractMetric> Metrics()
		{
			return Collections.UnmodifiableList(metrics);
		}
	}
}
