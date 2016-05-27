using System.Collections;
using Sharpen;

namespace Org.Apache.Hadoop.Record
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
		void WriteByte(byte b, string tag);

		/// <summary>Write a boolean to serialized record.</summary>
		/// <param name="b">Boolean to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void WriteBool(bool b, string tag);

		/// <summary>Write an integer to serialized record.</summary>
		/// <param name="i">Integer to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void WriteInt(int i, string tag);

		/// <summary>Write a long integer to serialized record.</summary>
		/// <param name="l">Long to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void WriteLong(long l, string tag);

		/// <summary>Write a single-precision float to serialized record.</summary>
		/// <param name="f">Float to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void WriteFloat(float f, string tag);

		/// <summary>Write a double precision floating point number to serialized record.</summary>
		/// <param name="d">Double to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void WriteDouble(double d, string tag);

		/// <summary>Write a unicode string to serialized record.</summary>
		/// <param name="s">String to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void WriteString(string s, string tag);

		/// <summary>Write a buffer to serialized record.</summary>
		/// <param name="buf">Buffer to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void WriteBuffer(Buffer buf, string tag);

		/// <summary>Mark the start of a record to be serialized.</summary>
		/// <param name="r">Record to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void StartRecord(Org.Apache.Hadoop.Record.Record r, string tag);

		/// <summary>Mark the end of a serialized record.</summary>
		/// <param name="r">Record to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void EndRecord(Org.Apache.Hadoop.Record.Record r, string tag);

		/// <summary>Mark the start of a vector to be serialized.</summary>
		/// <param name="v">Vector to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void StartVector(ArrayList v, string tag);

		/// <summary>Mark the end of a serialized vector.</summary>
		/// <param name="v">Vector to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void EndVector(ArrayList v, string tag);

		/// <summary>Mark the start of a map to be serialized.</summary>
		/// <param name="m">Map to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void StartMap(SortedList m, string tag);

		/// <summary>Mark the end of a serialized map.</summary>
		/// <param name="m">Map to be serialized</param>
		/// <param name="tag">Used by tagged serialization formats (such as XML)</param>
		/// <exception cref="System.IO.IOException">Indicates error in serialization</exception>
		void EndMap(SortedList m, string tag);
	}
}
