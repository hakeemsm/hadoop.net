using System.Collections.Generic;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>Performs no reduction, writing all input values directly to the output.</summary>
	public class IdentityReducer<K, V> : MapReduceBase, Reducer<K, V, K, V>
	{
		/// <summary>Writes all keys and values directly to output.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Reduce(K key, IEnumerator<V> values, OutputCollector<K, V> output
			, Reporter reporter)
		{
			while (values.HasNext())
			{
				output.Collect(key, values.Next());
			}
		}
	}
}
