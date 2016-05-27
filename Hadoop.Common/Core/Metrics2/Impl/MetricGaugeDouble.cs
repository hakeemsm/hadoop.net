using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	internal class MetricGaugeDouble : AbstractMetric
	{
		internal readonly double value;

		internal MetricGaugeDouble(MetricsInfo info, double value)
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
