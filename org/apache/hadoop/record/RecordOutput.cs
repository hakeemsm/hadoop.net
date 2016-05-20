using Sharpen;

namespace org.apache.hadoop.record
{
	/// <summary>Interface that all the serializers have to implement.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public interface RecordOutput
	{
		/// <summary>Write a byte to serialized record.</summary>
		/// <param name="b">Byte to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void writeByte(byte b, string tag);

		/// <summary>Write a boolean to serialized record.</summary>
		/// <param name="b">Boolean to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void writeBool(bool b, string tag);

		/// <summary>Write an integer to serialized record.</summary>
		/// <param name="i">Integer to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void writeInt(int i, string tag);

		/// <summary>Write a long integer to serialized record.</summary>
		/// <param name="l">Long to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void writeLong(long l, string tag);

		/// <summary>Write a single-precision float to serialized record.</summary>
		/// <param name="f">Float to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void writeFloat(float f, string tag);

		/// <summary>Write a double precision floating point number to serialized record.</summary>
		/// <param name="d">Double to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void writeDouble(double d, string tag);

		/// <summary>Write a unicode string to serialized record.</summary>
		/// <param name="s">String to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void writeString(string s, string tag);

		/// <summary>Write a buffer to serialized record.</summary>
		/// <param name="buf">Buffer to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void writeBuffer(org.apache.hadoop.record.Buffer buf, string tag);

		/// <summary>Mark the start of a record to be serialized.</summary>
		/// <param name="r">Record to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void startRecord(org.apache.hadoop.record.Record r, string tag);

		/// <summary>Mark the end of a serialized record.</summary>
		/// <param name="r">Record to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void endRecord(org.apache.hadoop.record.Record r, string tag);

		/// <summary>Mark the start of a vector to be serialized.</summary>
		/// <param name="v">Vector to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void startVector(System.Collections.ArrayList v, string tag);

		/// <summary>Mark the end of a serialized vector.</summary>
		/// <param name="v">Vector to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void endVector(System.Collections.ArrayList v, string tag);

		/// <summary>Mark the start of a map to be serialized.</summary>
		/// <param name="m">Map to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void startMap(System.Collections.SortedList m, string tag);

		/// <summary>Mark the end of a serialized map.</summary>
		/// <param name="m">Map to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void endMap(System.Collections.SortedList m, string tag);
	}
}
