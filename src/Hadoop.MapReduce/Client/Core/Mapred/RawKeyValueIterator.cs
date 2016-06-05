using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// <code>RawKeyValueIterator</code> is an iterator used to iterate over
	/// the raw keys and values during sort/merge of intermediate data.
	/// </summary>
	public interface RawKeyValueIterator
	{
		/// <summary>Gets the current raw key.</summary>
		/// <returns>Gets the current raw key as a DataInputBuffer</returns>
		/// <exception cref="System.IO.IOException"/>
		DataInputBuffer GetKey();

		/// <summary>Gets the current raw value.</summary>
		/// <returns>Gets the current raw value as a DataInputBuffer</returns>
		/// <exception cref="System.IO.IOException"/>
		DataInputBuffer GetValue();

		/// <summary>Sets up the current key and value (for getKey and getValue).</summary>
		/// <returns>
		/// <code>true</code> if there exists a key/value,
		/// <code>false</code> otherwise.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		bool Next();

		/// <summary>Closes the iterator so that the underlying streams can be closed.</summary>
		/// <exception cref="System.IO.IOException"/>
		void Close();

		/// <summary>
		/// Gets the Progress object; this has a float (0.0 - 1.0)
		/// indicating the bytes processed by the iterator so far
		/// </summary>
		Progress GetProgress();
	}
}
