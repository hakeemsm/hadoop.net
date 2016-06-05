using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// <code>InputSplit</code> represents the data to be processed by an
	/// individual
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// .
	/// <p>Typically, it presents a byte-oriented view on the input and is the
	/// responsibility of
	/// <see cref="RecordReader{K, V}"/>
	/// of the job to process this and present
	/// a record-oriented view.
	/// </summary>
	/// <seealso cref="InputFormat{K, V}"/>
	/// <seealso cref="RecordReader{K, V}"/>
	public interface InputSplit : Writable
	{
		/// <summary>Get the total number of bytes in the data of the <code>InputSplit</code>.
		/// 	</summary>
		/// <returns>the number of bytes in the input split.</returns>
		/// <exception cref="System.IO.IOException"/>
		long GetLength();

		/// <summary>Get the list of hostnames where the input split is located.</summary>
		/// <returns>
		/// list of hostnames where data of the <code>InputSplit</code> is
		/// located as an array of <code>String</code>s.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		string[] GetLocations();
	}
}
