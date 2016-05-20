using Sharpen;

namespace org.apache.hadoop.metrics2
{
	/// <summary>An immutable snapshot of metrics with a timestamp</summary>
	public interface MetricsRecord
	{
		/// <summary>Get the timestamp of the metrics</summary>
		/// <returns>the timestamp</returns>
		long timestamp();

		/// <returns>the record name</returns>
		string name();

		/// <returns>the description of the record</returns>
		string description();

		/// <returns>the context name of the record</returns>
		string context();

		/// <summary>
		/// Get the tags of the record
		/// Note: returning a collection instead of iterable as we
		/// need to use tags as keys (hence Collection#hashCode etc.) in maps
		/// </summary>
		/// <returns>an unmodifiable collection of tags</returns>
		System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.MetricsTag> tags
			();

		/// <summary>Get the metrics of the record</summary>
		/// <returns>an immutable iterable interface for metrics</returns>
		System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.AbstractMetric>
			 metrics();
	}
}
