using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>Additional operations required of a RecordReader to participate in a join.
	/// 	</summary>
	public abstract class ComposableRecordReader<K, V> : RecordReader<K, V>, Comparable
		<ComposableRecordReader<K, object>>
		where K : WritableComparable<object>
		where V : Writable
	{
		/// <summary>Return the position in the collector this class occupies.</summary>
		internal abstract int Id();

		/// <summary>Return the key this RecordReader would supply on a call to next(K,V)</summary>
		internal abstract K Key();

		/// <summary>Clone the key at the head of this RecordReader into the object provided.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		internal abstract void Key(K key);

		/// <summary>Create instance of key.</summary>
		internal abstract K CreateKey();

		/// <summary>Create instance of value.</summary>
		internal abstract V CreateValue();

		/// <summary>
		/// Returns true if the stream is not empty, but provides no guarantee that
		/// a call to next(K,V) will succeed.
		/// </summary>
		internal abstract bool HasNext();

		/// <summary>Skip key-value pairs with keys less than or equal to the key provided.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal abstract void Skip(K key);

		/// <summary>
		/// While key-value pairs from this RecordReader match the given key, register
		/// them with the JoinCollector provided.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal abstract void Accept(CompositeRecordReader.JoinCollector jc, K key);

		public abstract int CompareTo(ComposableRecordReader<K, object> arg1);
	}
}
