using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// <code>RecordReader</code> reads &lt;key, value&gt; pairs from an
	/// <see cref="InputSplit"/>
	/// .
	/// <p><code>RecordReader</code>, typically, converts the byte-oriented view of
	/// the input, provided by the <code>InputSplit</code>, and presents a
	/// record-oriented view for the
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// and
	/// <see cref="Reducer{K2, V2, K3, V3}"/>
	/// tasks for
	/// processing. It thus assumes the responsibility of processing record
	/// boundaries and presenting the tasks with keys and values.</p>
	/// </summary>
	/// <seealso cref="InputSplit"/>
	/// <seealso cref="InputFormat{K, V}"/>
	public interface RecordReader<K, V>
	{
		/// <summary>Reads the next key/value pair from the input for processing.</summary>
		/// <param name="key">the key to read data into</param>
		/// <param name="value">the value to read data into</param>
		/// <returns>true iff a key/value was read, false if at EOF</returns>
		/// <exception cref="System.IO.IOException"/>
		bool Next(K key, V value);

		/// <summary>Create an object of the appropriate type to be used as a key.</summary>
		/// <returns>a new key object.</returns>
		K CreateKey();

		/// <summary>Create an object of the appropriate type to be used as a value.</summary>
		/// <returns>a new value object.</returns>
		V CreateValue();

		/// <summary>Returns the current position in the input.</summary>
		/// <returns>the current position in the input.</returns>
		/// <exception cref="System.IO.IOException"/>
		long GetPos();

		/// <summary>
		/// Close this
		/// <see cref="InputSplit"/>
		/// to future operations.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		void Close();

		/// <summary>
		/// How much of the input has the
		/// <see cref="RecordReader{K, V}"/>
		/// consumed i.e.
		/// has been processed by?
		/// </summary>
		/// <returns>progress from <code>0.0</code> to <code>1.0</code>.</returns>
		/// <exception cref="System.IO.IOException"/>
		float GetProgress();
	}
}
