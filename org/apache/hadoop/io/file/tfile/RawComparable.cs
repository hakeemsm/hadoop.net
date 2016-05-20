using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>
	/// Interface for objects that can be compared through
	/// <see cref="org.apache.hadoop.io.RawComparator{T}"/>
	/// .
	/// This is useful in places where we need a single object reference to specify a
	/// range of bytes in a byte array, such as
	/// <see cref="java.lang.Comparable{T}"/>
	/// or
	/// <see cref="java.util.Collections.binarySearch{T}(System.Collections.Generic.IList{E}, object, java.util.Comparator{T})
	/// 	"/>
	/// The actual comparison among RawComparable's requires an external
	/// RawComparator and it is applications' responsibility to ensure two
	/// RawComparable are supposed to be semantically comparable with the same
	/// RawComparator.
	/// </summary>
	public interface RawComparable
	{
		/// <summary>Get the underlying byte array.</summary>
		/// <returns>The underlying byte array.</returns>
		byte[] buffer();

		/// <summary>Get the offset of the first byte in the byte array.</summary>
		/// <returns>The offset of the first byte in the byte array.</returns>
		int offset();

		/// <summary>Get the size of the byte range in the byte array.</summary>
		/// <returns>The size of the byte range in the byte array.</returns>
		int size();
	}
}
