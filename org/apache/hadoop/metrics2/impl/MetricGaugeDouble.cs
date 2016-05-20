using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	internal class MetricGaugeDouble : org.apache.hadoop.metrics2.AbstractMetric
	{
		internal readonly double value;

		internal MetricGaugeDouble(org.apache.hadoop.metrics2.MetricsInfo info, double value
			)
			: base(info)
		{
			this.value = value;
		}

		public override java.lang.Number value()
		{
			return value;
		}

		public override org.apache.hadoop.metrics2.MetricType type()
		{
			return org.apache.hadoop.metrics2.MetricType.GAUGE;
		}

		public override void visit(org.apache.hadoop.metrics2.MetricsVisitor visitor)
		{
			visitor.gauge(this, value);
		}
	}
}
