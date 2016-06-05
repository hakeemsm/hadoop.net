using Org.Apache.Hadoop.Metrics2;


namespace Org.Apache.Hadoop.Metrics2.Sink.Ganglia
{
	/// <summary>
	/// Since implementations of Metric are not public, hence use a visitor to figure
	/// out the type and slope of the metric.
	/// </summary>
	/// <remarks>
	/// Since implementations of Metric are not public, hence use a visitor to figure
	/// out the type and slope of the metric. Counters have "positive" slope.
	/// </remarks>
	internal class GangliaMetricVisitor : MetricsVisitor
	{
		private const string Int32 = "int32";

		private const string Float = "float";

		private const string Double = "double";

		private string type;

		private AbstractGangliaSink.GangliaSlope slope;

		/// <returns>the type of a visited metric</returns>
		internal virtual string GetType()
		{
			return type;
		}

		/// <returns>
		/// the slope of a visited metric. Slope is positive for counters and
		/// null for others
		/// </returns>
		internal virtual AbstractGangliaSink.GangliaSlope GetSlope()
		{
			return slope;
		}

		public virtual void Gauge(MetricsInfo info, int value)
		{
			// MetricGaugeInt.class ==> "int32"
			type = Int32;
			slope = null;
		}

		// set to null as cannot figure out from Metric
		public virtual void Gauge(MetricsInfo info, long value)
		{
			// MetricGaugeLong.class ==> "float"
			type = Float;
			slope = null;
		}

		// set to null as cannot figure out from Metric
		public virtual void Gauge(MetricsInfo info, float value)
		{
			// MetricGaugeFloat.class ==> "float"
			type = Float;
			slope = null;
		}

		// set to null as cannot figure out from Metric
		public virtual void Gauge(MetricsInfo info, double value)
		{
			// MetricGaugeDouble.class ==> "double"
			type = Double;
			slope = null;
		}

		// set to null as cannot figure out from Metric
		public virtual void Counter(MetricsInfo info, int value)
		{
			// MetricCounterInt.class ==> "int32"
			type = Int32;
			// counters have positive slope
			slope = AbstractGangliaSink.GangliaSlope.positive;
		}

		public virtual void Counter(MetricsInfo info, long value)
		{
			// MetricCounterLong.class ==> "float"
			type = Float;
			// counters have positive slope
			slope = AbstractGangliaSink.GangliaSlope.positive;
		}
	}
}
