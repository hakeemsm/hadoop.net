using Sharpen;

namespace org.apache.hadoop.metrics2
{
	/// <summary>The metrics filter interface</summary>
	public abstract class MetricsFilter : org.apache.hadoop.metrics2.MetricsPlugin
	{
		/// <summary>Whether to accept the name</summary>
		/// <param name="name">to filter on</param>
		/// <returns>true to accept; false otherwise.</returns>
		public abstract bool accepts(string name);

		/// <summary>Whether to accept the tag</summary>
		/// <param name="tag">to filter on</param>
		/// <returns>true to accept; false otherwise</returns>
		public abstract bool accepts(org.apache.hadoop.metrics2.MetricsTag tag);

		/// <summary>Whether to accept the tags</summary>
		/// <param name="tags">to filter on</param>
		/// <returns>true to accept; false otherwise</returns>
		public abstract bool accepts(System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.MetricsTag
			> tags);

		/// <summary>Whether to accept the record</summary>
		/// <param name="record">to filter on</param>
		/// <returns>true to accept; false otherwise.</returns>
		public virtual bool accepts(org.apache.hadoop.metrics2.MetricsRecord record)
		{
			return accepts(record.name()) && accepts(record.tags());
		}

		public abstract void init(org.apache.commons.configuration.SubsetConfiguration arg1
			);
	}
}
