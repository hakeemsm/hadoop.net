using Sharpen;

namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>The metrics record builder interface</summary>
	public abstract class MetricsRecordBuilder
	{
		/// <summary>Add a metrics tag</summary>
		/// <param name="info">metadata of the tag</param>
		/// <param name="value">of the tag</param>
		/// <returns>self</returns>
		public abstract MetricsRecordBuilder Tag(MetricsInfo info, string value);

		/// <summary>Add an immutable metrics tag object</summary>
		/// <param name="tag">a pre-made tag object (potentially save an object construction)
		/// 	</param>
		/// <returns>self</returns>
		public abstract MetricsRecordBuilder Add(MetricsTag tag);

		/// <summary>Add a pre-made immutable metric object</summary>
		/// <param name="metric">the pre-made metric to save an object construction</param>
		/// <returns>self</returns>
		public abstract MetricsRecordBuilder Add(AbstractMetric metric);

		/// <summary>Set the context tag</summary>
		/// <param name="value">of the context</param>
		/// <returns>self</returns>
		public abstract MetricsRecordBuilder SetContext(string value);

		/// <summary>Add an integer metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract MetricsRecordBuilder AddCounter(MetricsInfo info, int value);

		/// <summary>Add an long metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract MetricsRecordBuilder AddCounter(MetricsInfo info, long value);

		/// <summary>Add a integer gauge metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract MetricsRecordBuilder AddGauge(MetricsInfo info, int value);

		/// <summary>Add a long gauge metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract MetricsRecordBuilder AddGauge(MetricsInfo info, long value);

		/// <summary>Add a float gauge metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract MetricsRecordBuilder AddGauge(MetricsInfo info, float value);

		/// <summary>Add a double gauge metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract MetricsRecordBuilder AddGauge(MetricsInfo info, double value);

		/// <returns>the parent metrics collector object</returns>
		public abstract MetricsCollector Parent();

		/// <summary>Syntactic sugar to add multiple records in a collector in a one liner.</summary>
		/// <returns>the parent metrics collector object</returns>
		public virtual MetricsCollector EndRecord()
		{
			return Parent();
		}
	}
}
