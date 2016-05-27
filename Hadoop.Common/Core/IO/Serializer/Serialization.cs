using System;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer
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
		bool Accept(Type c);

		/// <returns>
		/// a
		/// <see cref="Serializer{T}"/>
		/// for the given class.
		/// </returns>
		Org.Apache.Hadoop.IO.Serializer.Serializer<T> GetSerializer(Type c);

		/// <returns>
		/// a
		/// <see cref="Deserializer{T}"/>
		/// for the given class.
		/// </returns>
		Deserializer<T> GetDeserializer(Type c);
	}
}
