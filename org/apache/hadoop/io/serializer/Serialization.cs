using Sharpen;

namespace org.apache.hadoop.io.serializer
{
	/// <summary>
	/// <p>
	/// Encapsulates a
	/// <see cref="Serializer{T}"/>
	/// /
	/// <see cref="Deserializer{T}"/>
	/// pair.
	/// </p>
	/// </summary>
	/// <?/>
	public interface Serialization<T>
	{
		/// <summary>
		/// Allows clients to test whether this
		/// <see cref="Serialization{T}"/>
		/// supports the given class.
		/// </summary>
		bool accept(java.lang.Class c);

		/// <returns>
		/// a
		/// <see cref="Serializer{T}"/>
		/// for the given class.
		/// </returns>
		org.apache.hadoop.io.serializer.Serializer<T> getSerializer(java.lang.Class c);

		/// <returns>
		/// a
		/// <see cref="Deserializer{T}"/>
		/// for the given class.
		/// </returns>
		org.apache.hadoop.io.serializer.Deserializer<T> getDeserializer(java.lang.Class c
			);
	}
}
