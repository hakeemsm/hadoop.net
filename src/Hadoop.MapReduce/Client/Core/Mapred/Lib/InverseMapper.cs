using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Mapred.Mapper{K1, V1, K2, V2}"/>
	/// that swaps keys and values.
	/// </summary>
	public class InverseMapper<K, V> : MapReduceBase, Mapper<K, V, V, K>
	{
		/// <summary>The inverse function.</summary>
		/// <remarks>The inverse function.  Input keys and values are swapped.</remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Map(K key, V value, OutputCollector<V, K> output, Reporter reporter
			)
		{
			output.Collect(value, key);
		}
	}
}
