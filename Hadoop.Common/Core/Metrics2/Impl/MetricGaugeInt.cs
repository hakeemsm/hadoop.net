using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	internal class MetricGaugeInt : AbstractMetric
	{
		internal readonly int value;

		internal MetricGaugeInt(MetricsInfo info, int value)
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
