using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	/// <summary>
	/// A utility class providing methods for serializing and deserializing
	/// objects.
	/// </summary>
	/// <remarks>
	/// A utility class providing methods for serializing and deserializing
	/// objects. The
	/// <see cref="Write(object)"/>
	/// and
	/// <see cref="Read(byte[])"/>
	/// methods are
	/// used by the
	/// <see cref="LeveldbTimelineStore"/>
	/// to store and retrieve arbitrary
	/// JSON, while the
	/// <see cref="WriteReverseOrderedLong(long)"/>
	/// and
	/// <see cref="ReadReverseOrderedLong(byte[], int)"/>
	/// methods are used to sort entities in descending
	/// start time order.
	/// </remarks>
	public class GenericObjectMapper
	{
		private static readonly byte[] EmptyBytes = new byte[0];

		public static readonly ObjectReader ObjectReader;

		public static readonly ObjectWriter ObjectWriter;

		static GenericObjectMapper()
		{
			ObjectMapper mapper = new ObjectMapper();
			ObjectReader = mapper.Reader(typeof(object));
			ObjectWriter = mapper.Writer();
		}

		/// <summary>Serializes an Object into a byte array.</summary>
		/// <remarks>
		/// Serializes an Object into a byte array. Along with
		/// <see cref="Read(byte[])"/>
		/// ,
		/// can be used to serialize an Object and deserialize it into an Object of
		/// the same type without needing to specify the Object's type,
		/// as long as it is one of the JSON-compatible objects understood by
		/// ObjectMapper.
		/// </remarks>
		/// <param name="o">An Object</param>
		/// <returns>A byte array representation of the Object</returns>
		/// <exception cref="System.IO.IOException">if there is a write error</exception>
		public static byte[] Write(object o)
		{
			if (o == null)
			{
				return EmptyBytes;
			}
			return ObjectWriter.WriteValueAsBytes(o);
		}

		/// <summary>
		/// Deserializes an Object from a byte array created with
		/// <see cref="Write(object)"/>
		/// .
		/// </summary>
		/// <param name="b">A byte array</param>
		/// <returns>An Object</returns>
		/// <exception cref="System.IO.IOException">if there is a read error</exception>
		public static object Read(byte[] b)
		{
			return Read(b, 0);
		}

		/// <summary>
		/// Deserializes an Object from a byte array at a specified offset, assuming
		/// the bytes were created with
		/// <see cref="Write(object)"/>
		/// .
		/// </summary>
		/// <param name="b">A byte array</param>
		/// <param name="offset">Offset into the array</param>
		/// <returns>An Object</returns>
		/// <exception cref="System.IO.IOException">if there is a read error</exception>
		public static object Read(byte[] b, int offset)
		{
			if (b == null || b.Length == 0)
			{
				return null;
			}
			return ObjectReader.ReadValue(b, offset, b.Length - offset);
		}

		/// <summary>
		/// Converts a long to a 8-byte array so that lexicographic ordering of the
		/// produced byte arrays sort the longs in descending order.
		/// </summary>
		/// <param name="l">A long</param>
		/// <returns>A byte array</returns>
		public static byte[] WriteReverseOrderedLong(long l)
		{
			byte[] b = new byte[8];
			return WriteReverseOrderedLong(l, b, 0);
		}

		public static byte[] WriteReverseOrderedLong(long l, byte[] b, int offset)
		{
			b[offset] = unchecked((byte)(unchecked((int)(0x7f)) ^ ((l >> 56) & unchecked((int
				)(0xff)))));
			for (int i = offset + 1; i < offset + 7; i++)
			{
				b[i] = unchecked((byte)(unchecked((int)(0xff)) ^ ((l >> 8 * (7 - i)) & unchecked(
					(int)(0xff)))));
			}
			b[offset + 7] = unchecked((byte)(unchecked((int)(0xff)) ^ (l & unchecked((int)(0xff
				)))));
			return b;
		}

		/// <summary>
		/// Reads 8 bytes from an array starting at the specified offset and
		/// converts them to a long.
		/// </summary>
		/// <remarks>
		/// Reads 8 bytes from an array starting at the specified offset and
		/// converts them to a long.  The bytes are assumed to have been created
		/// with
		/// <see cref="WriteReverseOrderedLong(long)"/>
		/// .
		/// </remarks>
		/// <param name="b">A byte array</param>
		/// <param name="offset">An offset into the byte array</param>
		/// <returns>A long</returns>
		public static long ReadReverseOrderedLong(byte[] b, int offset)
		{
			long l = b[offset] & unchecked((int)(0xff));
			for (int i = 1; i < 8; i++)
			{
				l = l << 8;
				l = l | (b[offset + i] & unchecked((int)(0xff)));
			}
			return l ^ unchecked((long)(0x7fffffffffffffffl));
		}
	}
}
