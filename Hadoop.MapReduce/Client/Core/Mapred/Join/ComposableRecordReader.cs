using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>Additional operations required of a RecordReader to participate in a join.
	/// 	</summary>
	public interface ComposableRecordReader<K, V> : RecordReader<K, V>, Comparable<ComposableRecordReader
		<K, object>>
		where K : WritableComparable
		where V : Writable
	{
		/// <summary>Return the position in the collector this class occupies.</summary>
		int Id();

		/// <summary>Return the key this RecordReader would supply on a call to next(K,V)</summary>
		K Key();

		/// <summary>Clone the key at the head of this RecordReader into the object provided.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		void Key(K key);

		/// <summary>
		/// Returns true if the stream is not empty, but provides no guarantee that
		/// a call to next(K,V) will succeed.
		/// </summary>
		bool HasNext();

		/// <summary>Skip key-value pairs with keys less than or equal to the key provided.</summary>
		/// <exception cref="System.IO.IOException"/>
		void Skip(K key);

		/// <summary>
		/// While key-value pairs from this RecordReader match the given key, register
		/// them with the JoinCollector provided.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		void Accept(CompositeRecordReader.JoinCollector jc, K key);
	}
}
