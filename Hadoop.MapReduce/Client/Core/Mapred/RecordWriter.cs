using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// <code>RecordWriter</code> writes the output &lt;key, value&gt; pairs
	/// to an output file.
	/// </summary>
	/// <remarks>
	/// <code>RecordWriter</code> writes the output &lt;key, value&gt; pairs
	/// to an output file.
	/// <p><code>RecordWriter</code> implementations write the job outputs to the
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
	/// .
	/// </remarks>
	/// <seealso cref="OutputFormat{K, V}"/>
	public interface RecordWriter<K, V>
	{
		/// <summary>Writes a key/value pair.</summary>
		/// <param name="key">the key to write.</param>
		/// <param name="value">the value to write.</param>
		/// <exception cref="System.IO.IOException"/>
		void Write(K key, V value);

		/// <summary>Close this <code>RecordWriter</code> to future operations.</summary>
		/// <param name="reporter">facility to report progress.</param>
		/// <exception cref="System.IO.IOException"/>
		void Close(Reporter reporter);
	}
}
