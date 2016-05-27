using System.IO;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer
{
	/// <summary>
	/// <p>
	/// A
	/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
	/// that uses a
	/// <see cref="JavaSerialization"/>
	/// <see cref="Deserializer{T}"/>
	/// to deserialize objects that are then compared via
	/// their
	/// <see cref="System.IComparable{T}"/>
	/// interfaces.
	/// </p>
	/// </summary>
	/// <?/>
	/// <seealso cref="JavaSerialization"/>
	public class JavaSerializationComparator<T> : DeserializerComparator<T>
		where T : Serializable
	{
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public JavaSerializationComparator()
			: base(new JavaSerialization.JavaSerializationDeserializer<T>())
		{
		}

		[InterfaceAudience.Private]
		public override int Compare(T o1, T o2)
		{
			return o1.CompareTo(o2);
		}
	}
}
