using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Collects the <code>&lt;key, value&gt;</code> pairs output by
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// s
	/// and
	/// <see cref="Reducer{K2, V2, K3, V3}"/>
	/// s.
	/// <p><code>OutputCollector</code> is the generalization of the facility
	/// provided by the Map-Reduce framework to collect data output by either the
	/// <code>Mapper</code> or the <code>Reducer</code> i.e. intermediate outputs
	/// or the output of the job.</p>
	/// </summary>
	public interface OutputCollector<K, V>
	{
		/// <summary>Adds a key/value pair to the output.</summary>
		/// <param name="key">the key to collect.</param>
		/// <param name="value">to value to collect.</param>
		/// <exception cref="System.IO.IOException"/>
		void Collect(K key, V value);
	}
}
