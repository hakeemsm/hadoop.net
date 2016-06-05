using System.Collections.Generic;
using Org.Apache.Commons.Configuration;


namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>The metrics filter interface</summary>
	public abstract class MetricsFilter : MetricsPlugin
	{
		/// <summary>Whether to accept the name</summary>
		/// <param name="name">to filter on</param>
		/// <returns>true to accept; false otherwise.</returns>
		public abstract bool Accepts(string name);

		/// <summary>Whether to accept the tag</summary>
		/// <param name="tag">to filter on</param>
		/// <returns>true to accept; false otherwise</returns>
		public abstract bool Accepts(MetricsTag tag);

		/// <summary>Whether to accept the tags</summary>
		/// <param name="tags">to filter on</param>
		/// <returns>true to accept; false otherwise</returns>
		public abstract bool Accepts(IEnumerable<MetricsTag> tags);

		/// <summary>Whether to accept the record</summary>
		/// <param name="record">to filter on</param>
		/// <returns>true to accept; false otherwise.</returns>
		public virtual bool Accepts(MetricsRecord record)
		{
			return Accepts(record.Name()) && Accepts(record.Tags());
		}

		public abstract void Init(SubsetConfiguration arg1);
	}
}
