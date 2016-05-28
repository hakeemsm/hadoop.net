using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// <code>InputSplit</code> represents the data to be processed by an
	/// individual
	/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// .
	/// <p>Typically, it presents a byte-oriented view on the input and is the
	/// responsibility of
	/// <see cref="RecordReader{KEYIN, VALUEIN}"/>
	/// of the job to process this and present
	/// a record-oriented view.
	/// </summary>
	/// <seealso cref="InputFormat{K, V}"/>
	/// <seealso cref="RecordReader{KEYIN, VALUEIN}"/>
	public abstract class InputSplit
	{
		/// <summary>Get the size of the split, so that the input splits can be sorted by size.
		/// 	</summary>
		/// <returns>the number of bytes in the split</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract long GetLength();

		/// <summary>Get the list of nodes by name where the data for the split would be local.
		/// 	</summary>
		/// <remarks>
		/// Get the list of nodes by name where the data for the split would be local.
		/// The locations do not need to be serialized.
		/// </remarks>
		/// <returns>a new array of the node nodes.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract string[] GetLocations();

		/// <summary>
		/// Gets info about which nodes the input split is stored on and how it is
		/// stored at each location.
		/// </summary>
		/// <returns>
		/// list of <code>SplitLocationInfo</code>s describing how the split
		/// data is stored at each location. A null value indicates that all the
		/// locations have the data stored on disk.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceStability.Evolving]
		public virtual SplitLocationInfo[] GetLocationInfo()
		{
			return null;
		}
	}
}
