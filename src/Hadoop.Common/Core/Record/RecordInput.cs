

namespace Org.Apache.Hadoop.Record
{
	/// <summary>Interface that all the Deserializers have to implement.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public interface RecordInput
	{
		/// <summary>Read a byte from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		byte ReadByte(string tag);

		/// <summary>Read a boolean from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		bool ReadBool(string tag);

		/// <summary>Read an integer from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		int ReadInt(string tag);

		/// <summary>Read a long integer from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		long ReadLong(string tag);

		/// <summary>Read a single-precision float from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		float ReadFloat(string tag);

		/// <summary>Read a double-precision number from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		double ReadDouble(string tag);

		/// <summary>Read a UTF-8 encoded string from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		string ReadString(string tag);

		/// <summary>Read byte array from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		Buffer ReadBuffer(string tag);

		/// <summary>Check the mark for start of the serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException"/>
		void StartRecord(string tag);

		/// <summary>Check the mark for end of the serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException"/>
		void EndRecord(string tag);

		/// <summary>Check the mark for start of the serialized vector.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>Index that is used to count the number of elements.</returns>
		/// <exception cref="System.IO.IOException"/>
		Index StartVector(string tag);

		/// <summary>Check the mark for end of the serialized vector.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException"/>
		void EndVector(string tag);

		/// <summary>Check the mark for start of the serialized map.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>Index that is used to count the number of map entries.</returns>
		/// <exception cref="System.IO.IOException"/>
		Index StartMap(string tag);

		/// <summary>Check the mark for end of the serialized map.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException"/>
		void EndMap(string tag);
	}
}
