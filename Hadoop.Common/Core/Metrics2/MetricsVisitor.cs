using Sharpen;

namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>A visitor interface for metrics</summary>
	public interface MetricsVisitor
	{
		/// <summary>Callback for integer value gauges</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void Gauge(MetricsInfo info, int value);

		/// <summary>Callback for long value gauges</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void Gauge(MetricsInfo info, long value);

		/// <summary>Callback for float value gauges</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void Gauge(MetricsInfo info, float value);

		/// <summary>Callback for double value gauges</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void Gauge(MetricsInfo info, double value);

		/// <summary>Callback for integer value counters</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void Counter(MetricsInfo info, int value);

		/// <summary>Callback for long value counters</summary>
		/// <param name="info">the metric info</param>
		/// <param name="value">of the metric</param>
		void Counter(MetricsInfo info, long value);
	}
}
