using Org.Apache.Hadoop.Metrics2;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	internal class MetricGaugeFloat : AbstractMetric
	{
		internal readonly float value;

		internal MetricGaugeFloat(MetricsInfo info, float value)
			: base(info)
		{
			this.value = value;
		}

		public override Number Value()
		{
			return value;
		}

		public override MetricType Type()
		{
			return MetricType.Gauge;
		}

		public override void Visit(MetricsVisitor visitor)
		{
			visitor.Gauge(this, value);
		}
	}
}
