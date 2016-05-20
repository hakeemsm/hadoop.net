using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	internal class MetricCounterLong : org.apache.hadoop.metrics2.AbstractMetric
	{
		internal readonly long value;

		internal MetricCounterLong(org.apache.hadoop.metrics2.MetricsInfo info, long value
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
			return org.apache.hadoop.metrics2.MetricType.COUNTER;
		}

		public override void visit(org.apache.hadoop.metrics2.MetricsVisitor visitor)
		{
			visitor.counter(this, value);
		}
	}
}
