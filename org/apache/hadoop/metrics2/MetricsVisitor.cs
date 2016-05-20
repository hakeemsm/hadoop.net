using Sharpen;

namespace org.apache.hadoop.metrics2
{
	/// <summary>A visitor interface for metrics</summary>
	public interface MetricsVisitor
	{
		/// <summary>Callback for integer value gauges</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void gauge(org.apache.hadoop.metrics2.MetricsInfo info, int value);

		/// <summary>Callback for long value gauges</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void gauge(org.apache.hadoop.metrics2.MetricsInfo info, long value);

		/// <summary>Callback for float value gauges</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void gauge(org.apache.hadoop.metrics2.MetricsInfo info, float value);

		/// <summary>Callback for double value gauges</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void gauge(org.apache.hadoop.metrics2.MetricsInfo info, double value);

		/// <summary>Callback for integer value counters</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void counter(org.apache.hadoop.metrics2.MetricsInfo info, int value);

		/// <summary>Callback for long value counters</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void counter(org.apache.hadoop.metrics2.MetricsInfo info, long value);
	}
}
