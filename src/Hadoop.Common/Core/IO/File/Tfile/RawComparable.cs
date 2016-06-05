

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>
	/// Interface for objects that can be compared through
	/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
	/// .
	/// This is useful in places where we need a single object reference to specify a
	/// range of bytes in a byte array, such as
	/// <see cref="System.IComparable{T}"/>
	/// or
	/// <see cref="Collections.BinarySearch{T}(System.Collections.Generic.IList{E}, object, System.Collections.Generic.IComparer{T})
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
		byte[] Buffer();

		/// <summary>Get the offset of the first byte in the byte array.</summary>
		/// <returns>The offset of the first byte in the byte array.</returns>
		int Offset();

		/// <summary>Get the size of the byte range in the byte array.</summary>
		/// <returns>The size of the byte range in the byte array.</returns>
		int Size();
	}
}
