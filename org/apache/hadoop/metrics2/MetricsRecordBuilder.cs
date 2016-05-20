using Sharpen;

namespace org.apache.hadoop.metrics2
{
	/// <summary>The metrics record builder interface</summary>
	public abstract class MetricsRecordBuilder
	{
		/// <summary>Add a metrics tag</summary>
		/// <param name="info">metadata of the tag</param>
		/// <param name="value">of the tag</param>
		/// <returns>self</returns>
		public abstract org.apache.hadoop.metrics2.MetricsRecordBuilder tag(org.apache.hadoop.metrics2.MetricsInfo
			 info, string value);

		/// <summary>Add an immutable metrics tag object</summary>
		/// <param name="tag">a pre-made tag object (potentially save an object construction)
		/// 	</param>
		/// <returns>self</returns>
		public abstract org.apache.hadoop.metrics2.MetricsRecordBuilder add(org.apache.hadoop.metrics2.MetricsTag
			 tag);

		/// <summary>Add a pre-made immutable metric object</summary>
		/// <param name="metric">the pre-made metric to save an object construction</param>
		/// <returns>self</returns>
		public abstract org.apache.hadoop.metrics2.MetricsRecordBuilder add(org.apache.hadoop.metrics2.AbstractMetric
			 metric);

		/// <summary>Set the context tag</summary>
		/// <param name="value">of the context</param>
		/// <returns>self</returns>
		public abstract org.apache.hadoop.metrics2.MetricsRecordBuilder setContext(string
			 value);

		/// <summary>Add an integer metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract org.apache.hadoop.metrics2.MetricsRecordBuilder addCounter(org.apache.hadoop.metrics2.MetricsInfo
			 info, int value);

		/// <summary>Add an long metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract org.apache.hadoop.metrics2.MetricsRecordBuilder addCounter(org.apache.hadoop.metrics2.MetricsInfo
			 info, long value);

		/// <summary>Add a integer gauge metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract org.apache.hadoop.metrics2.MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo
			 info, int value);

		/// <summary>Add a long gauge metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract org.apache.hadoop.metrics2.MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo
			 info, long value);

		/// <summary>Add a float gauge metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract org.apache.hadoop.metrics2.MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo
			 info, float value);

		/// <summary>Add a double gauge metric</summary>
		/// <param name="info">metadata of the metric</param>
		/// <param name="value">of the metric</param>
		/// <returns>self</returns>
		public abstract org.apache.hadoop.metrics2.MetricsRecordBuilder addGauge(org.apache.hadoop.metrics2.MetricsInfo
			 info, double value);

		/// <returns>the parent metrics collector object</returns>
		public abstract org.apache.hadoop.metrics2.MetricsCollector parent();

		/// <summary>Syntactic sugar to add multiple records in a collector in a one liner.</summary>
		/// <returns>the parent metrics collector object</returns>
		public virtual org.apache.hadoop.metrics2.MetricsCollector endRecord()
		{
			return parent();
		}
	}
}
