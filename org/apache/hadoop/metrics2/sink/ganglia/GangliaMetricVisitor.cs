using Sharpen;

namespace org.apache.hadoop.metrics2.sink.ganglia
{
	/// <summary>
	/// Since implementations of Metric are not public, hence use a visitor to figure
	/// out the type and slope of the metric.
	/// </summary>
	/// <remarks>
	/// Since implementations of Metric are not public, hence use a visitor to figure
	/// out the type and slope of the metric. Counters have "positive" slope.
	/// </remarks>
	internal class GangliaMetricVisitor : org.apache.hadoop.metrics2.MetricsVisitor
	{
		private const string INT32 = "int32";

		private const string FLOAT = "float";

		private const string DOUBLE = "double";

		private string type;

		private org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope 
			slope;

		/// <returns>the type of a visited metric</returns>
		internal virtual string getType()
		{
			return type;
		}

		/// <returns>
		/// the slope of a visited metric. Slope is positive for counters and
		/// null for others
		/// </returns>
		internal virtual org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope
			 getSlope()
		{
			return slope;
		}

		public virtual void gauge(org.apache.hadoop.metrics2.MetricsInfo info, int value)
		{
			// MetricGaugeInt.class ==> "int32"
			type = INT32;
			slope = null;
		}

		// set to null as cannot figure out from Metric
		public virtual void gauge(org.apache.hadoop.metrics2.MetricsInfo info, long value
			)
		{
			// MetricGaugeLong.class ==> "float"
			type = FLOAT;
			slope = null;
		}

		// set to null as cannot figure out from Metric
		public virtual void gauge(org.apache.hadoop.metrics2.MetricsInfo info, float value
			)
		{
			// MetricGaugeFloat.class ==> "float"
			type = FLOAT;
			slope = null;
		}

		// set to null as cannot figure out from Metric
		public virtual void gauge(org.apache.hadoop.metrics2.MetricsInfo info, double value
			)
		{
			// MetricGaugeDouble.class ==> "double"
			type = DOUBLE;
			slope = null;
		}

		// set to null as cannot figure out from Metric
		public virtual void counter(org.apache.hadoop.metrics2.MetricsInfo info, int value
			)
		{
			// MetricCounterInt.class ==> "int32"
			type = INT32;
			// counters have positive slope
			slope = org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope.
				positive;
		}

		public virtual void counter(org.apache.hadoop.metrics2.MetricsInfo info, long value
			)
		{
			// MetricCounterLong.class ==> "float"
			type = FLOAT;
			// counters have positive slope
			slope = org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope.
				positive;
		}
	}
}
