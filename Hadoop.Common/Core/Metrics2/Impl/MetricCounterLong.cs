using Org.Apache.Hadoop.Metrics2;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	internal class MetricCounterLong : AbstractMetric
	{
		internal readonly long value;

		internal MetricCounterLong(MetricsInfo info, long value)
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
			return MetricType.Counter;
		}

		public override void Visit(MetricsVisitor visitor)
		{
			visitor.Counter(this, value);
		}
	}
}
