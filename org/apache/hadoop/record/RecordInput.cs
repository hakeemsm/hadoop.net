using Sharpen;

namespace org.apache.hadoop.record
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
		byte readByte(string tag);

		/// <summary>Read a boolean from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		bool readBool(string tag);

		/// <summary>Read an integer from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		int readInt(string tag);

		/// <summary>Read a long integer from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		long readLong(string tag);

		/// <summary>Read a single-precision float from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		float readFloat(string tag);

		/// <summary>Read a double-precision number from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		double readDouble(string tag);

		/// <summary>Read a UTF-8 encoded string from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		string readString(string tag);

		/// <summary>Read byte array from serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>value read from serialized record.</returns>
		/// <exception cref="System.IO.IOException"/>
		org.apache.hadoop.record.Buffer readBuffer(string tag);

		/// <summary>Check the mark for start of the serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException"/>
		void startRecord(string tag);

		/// <summary>Check the mark for end of the serialized record.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException"/>
		void endRecord(string tag);

		/// <summary>Check the mark for start of the serialized vector.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>Index that is used to count the number of elements.</returns>
		/// <exception cref="System.IO.IOException"/>
		org.apache.hadoop.record.Index startVector(string tag);

		/// <summary>Check the mark for end of the serialized vector.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException"/>
		void endVector(string tag);

		/// <summary>Check the mark for start of the serialized map.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <returns>Index that is used to count the number of map entries.</returns>
		/// <exception cref="System.IO.IOException"/>
		org.apache.hadoop.record.Index startMap(string tag);

		/// <summary>Check the mark for end of the serialized map.</summary>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException"/>
		void endMap(string tag);
	}
}
