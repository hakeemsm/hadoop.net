using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Map
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
	/// 	>
	/// that swaps keys and values.
	/// </summary>
	public class InverseMapper<K, V> : Mapper<K, V, V, K>
	{
		/// <summary>The inverse function.</summary>
		/// <remarks>The inverse function.  Input keys and values are swapped.</remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal override void Map(K key, V value, Mapper.Context context)
		{
			context.Write(value, key);
		}
	}
}
