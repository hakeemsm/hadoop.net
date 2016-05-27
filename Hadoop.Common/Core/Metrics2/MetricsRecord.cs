using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>An immutable snapshot of metrics with a timestamp</summary>
	public interface MetricsRecord
	{
		/// <summary>Get the timestamp of the metrics</summary>
		/// <returns>the timestamp</returns>
		long Timestamp();

		/// <returns>the record name</returns>
		string Name();

		/// <returns>the description of the record</returns>
		string Description();

		/// <returns>the context name of the record</returns>
		string Context();

		/// <summary>
		/// Get the tags of the record
		/// Note: returning a collection instead of iterable as we
		/// need to use tags as keys (hence Collection#hashCode etc.) in maps
		/// </summary>
		/// <returns>an unmodifiable collection of tags</returns>
		ICollection<MetricsTag> Tags();

		/// <summary>Get the metrics of the record</summary>
		/// <returns>an immutable iterable interface for metrics</returns>
		IEnumerable<AbstractMetric> Metrics();
	}
}
