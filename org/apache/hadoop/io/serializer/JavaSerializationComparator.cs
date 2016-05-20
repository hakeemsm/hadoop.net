using Sharpen;

namespace org.apache.hadoop.io.serializer
{
	/// <summary>
	/// <p>
	/// A
	/// <see cref="org.apache.hadoop.io.RawComparator{T}"/>
	/// that uses a
	/// <see cref="JavaSerialization"/>
	/// <see cref="Deserializer{T}"/>
	/// to deserialize objects that are then compared via
	/// their
	/// <see cref="java.lang.Comparable{T}"/>
	/// interfaces.
	/// </p>
	/// </summary>
	/// <?/>
	/// <seealso cref="JavaSerialization"/>
	public class JavaSerializationComparator<T> : org.apache.hadoop.io.serializer.DeserializerComparator
		<T>
		where T : java.io.Serializable
	{
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public JavaSerializationComparator()
			: base(new org.apache.hadoop.io.serializer.JavaSerialization.JavaSerializationDeserializer
				<T>())
		{
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override int compare(T o1, T o2)
		{
			return o1.compareTo(o2);
		}
	}
}
