using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// <p>
	/// A
	/// <see cref="System.Collections.IComparer{T}"/>
	/// that operates directly on byte representations of
	/// objects.
	/// </p>
	/// </summary>
	/// <?/>
	/// <seealso cref="org.apache.hadoop.io.serializer.DeserializerComparator{T}"/>
	public interface RawComparator<T> : java.util.Comparator<T>
	{
		/// <summary>Compare two objects in binary.</summary>
		/// <remarks>
		/// Compare two objects in binary.
		/// b1[s1:l1] is the first object, and b2[s2:l2] is the second object.
		/// </remarks>
		/// <param name="b1">The first byte array.</param>
		/// <param name="s1">The position index in b1. The object under comparison's starting index.
		/// 	</param>
		/// <param name="l1">The length of the object in b1.</param>
		/// <param name="b2">The second byte array.</param>
		/// <param name="s2">The position index in b2. The object under comparison's starting index.
		/// 	</param>
		/// <param name="l2">The length of the object under comparison in b2.</param>
		/// <returns>An integer result of the comparison.</returns>
		int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);
	}
}
