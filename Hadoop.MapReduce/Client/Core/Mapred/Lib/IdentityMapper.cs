using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>Implements the identity function, mapping inputs directly to outputs.</summary>
	public class IdentityMapper<K, V> : MapReduceBase, Mapper<K, V, K, V>
	{
		/// <summary>The identify function.</summary>
		/// <remarks>
		/// The identify function.  Input key/value pair is written directly to
		/// output.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Map(K key, V val, OutputCollector<K, V> output, Reporter reporter
			)
		{
			output.Collect(key, val);
		}
	}
}
