using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	internal class MetricGaugeFloat : org.apache.hadoop.metrics2.AbstractMetric
	{
		internal readonly float value;

		internal MetricGaugeFloat(org.apache.hadoop.metrics2.MetricsInfo info, float value
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
