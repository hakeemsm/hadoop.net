using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>This class stores text using standard UTF8 encoding.</summary>
	/// <remarks>
	/// This class stores text using standard UTF8 encoding.  It provides methods
	/// to serialize, deserialize, and compare texts at byte level.  The type of
	/// length is integer and is serialized using zero-compressed format.  <p>In
	/// addition, it provides methods for string traversal without converting the
	/// byte array to a string.  <p>Also includes utilities for
	/// serializing/deserialing a string, coding/decoding a string, checking if a
	/// byte array contains valid UTF8 code, calculating the length of an encoded
	/// string.
	/// </remarks>
	public class Text : org.apache.hadoop.io.BinaryComparable, org.apache.hadoop.io.WritableComparable
		<org.apache.hadoop.io.BinaryComparable>
	{
		private sealed class _ThreadLocal_57 : java.lang.ThreadLocal<java.nio.charset.CharsetEncoder
			>
		{
			public _ThreadLocal_57()
			{
			}

			protected override java.nio.charset.CharsetEncoder initialValue()
			{
				return java.nio.charset.Charset.forName("UTF-8").newEncoder().onMalformedInput(java.nio.charset.CodingErrorAction
					.REPORT).onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
			}
		}

		private static java.lang.ThreadLocal<java.nio.charset.CharsetEncoder> ENCODER_FACTORY
			 = new _ThreadLocal_57();

		private sealed class _ThreadLocal_67 : java.lang.ThreadLocal<java.nio.charset.CharsetDecoder
			>
		{
			public _ThreadLocal_67()
			{
			}

			protected override java.nio.charset.CharsetDecoder initialValue()
			{
				return java.nio.charset.Charset.forName("UTF-8").newDecoder().onMalformedInput(java.nio.charset.CodingErrorAction
					.REPORT).onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
			}
		}

		private static java.lang.ThreadLocal<java.nio.charset.CharsetDecoder> DECODER_FACTORY
			 = new _ThreadLocal_67();

		private static readonly byte[] EMPTY_BYTES = new byte[0];

		private byte[] bytes;

		private int length;

		public Text()
		{
			bytes = EMPTY_BYTES;
		}

		/// <summary>Construct from a string.</summary>
		public Text(string @string)
		{
			set(@string);
		}

		/// <summary>Construct from another text.</summary>
		public Text(org.apache.hadoop.io.Text utf8)
		{
			set(utf8);
		}

		/// <summary>Construct from a byte array.</summary>
		public Text(byte[] utf8)
		{
			set(utf8);
		}

		/// <summary>Get a copy of the bytes that is exactly the length of the data.</summary>
		/// <remarks>
		/// Get a copy of the bytes that is exactly the length of the data.
		/// See
		/// <see cref="getBytes()"/>
		/// for faster access to the underlying array.
		/// </remarks>
		public virtual byte[] copyBytes()
		{
			byte[] result = new byte[length];
			System.Array.Copy(bytes, 0, result, 0, length);
			return result;
		}

		/// <summary>
		/// Returns the raw bytes; however, only data up to
		/// <see cref="getLength()"/>
		/// is
		/// valid. Please use
		/// <see cref="copyBytes()"/>
		/// if you
		/// need the returned array to be precisely the length of the data.
		/// </summary>
		public override byte[] getBytes()
		{
			return bytes;
		}

		/// <summary>Returns the number of bytes in the byte array</summary>
		public override int getLength()
		{
			return length;
		}

		/// <summary>
		/// Returns the Unicode Scalar Value (32-bit integer value)
		/// for the character at <code>position</code>.
		/// </summary>
		/// <remarks>
		/// Returns the Unicode Scalar Value (32-bit integer value)
		/// for the character at <code>position</code>. Note that this
		/// method avoids using the converter or doing String instantiation
		/// </remarks>
		/// <returns>
		/// the Unicode scalar value at position or -1
		/// if the position is invalid or points to a
		/// trailing byte
		/// </returns>
		public virtual int charAt(int position)
		{
			if (position > this.length)
			{
				return -1;
			}
			// too long
			if (position < 0)
			{
				return -1;
			}
			// duh.
			java.nio.ByteBuffer bb = (java.nio.ByteBuffer)java.nio.ByteBuffer.wrap(bytes).position
				(position);
			return bytesToCodePoint(bb.slice());
		}

		public virtual int find(string what)
		{
			return find(what, 0);
		}

		/// <summary>
		/// Finds any occurence of <code>what</code> in the backing
		/// buffer, starting as position <code>start</code>.
		/// </summary>
		/// <remarks>
		/// Finds any occurence of <code>what</code> in the backing
		/// buffer, starting as position <code>start</code>. The starting
		/// position is measured in bytes and the return value is in
		/// terms of byte position in the buffer. The backing buffer is
		/// not converted to a string for this operation.
		/// </remarks>
		/// <returns>
		/// byte position of the first occurence of the search
		/// string in the UTF-8 buffer or -1 if not found
		/// </returns>
		public virtual int find(string what, int start)
		{
			try
			{
				java.nio.ByteBuffer src = java.nio.ByteBuffer.wrap(this.bytes, 0, this.length);
				java.nio.ByteBuffer tgt = encode(what);
				byte b = tgt.get();
				src.position(start);
				while (src.hasRemaining())
				{
					if (b == src.get())
					{
						// matching first byte
						src.mark();
						// save position in loop
						tgt.mark();
						// save position in target
						bool found = true;
						int pos = src.position() - 1;
						while (tgt.hasRemaining())
						{
							if (!src.hasRemaining())
							{
								// src expired first
								tgt.reset();
								src.reset();
								found = false;
								break;
							}
							if (!(tgt.get() == src.get()))
							{
								tgt.reset();
								src.reset();
								found = false;
								break;
							}
						}
						// no match
						if (found)
						{
							return pos;
						}
					}
				}
				return -1;
			}
			catch (java.nio.charset.CharacterCodingException e)
			{
				// not found
				// can't get here
				Sharpen.Runtime.printStackTrace(e);
				return -1;
			}
		}

		/// <summary>Set to contain the contents of a string.</summary>
		public virtual void set(string @string)
		{
			try
			{
				java.nio.ByteBuffer bb = encode(@string, true);
				bytes = ((byte[])bb.array());
				length = bb.limit();
			}
			catch (java.nio.charset.CharacterCodingException e)
			{
				throw new System.Exception("Should not have happened ", e);
			}
		}

		/// <summary>Set to a utf8 byte array</summary>
		public virtual void set(byte[] utf8)
		{
			set(utf8, 0, utf8.Length);
		}

		/// <summary>copy a text.</summary>
		public virtual void set(org.apache.hadoop.io.Text other)
		{
			set(other.getBytes(), 0, other.getLength());
		}

		/// <summary>Set the Text to range of bytes</summary>
		/// <param name="utf8">the data to copy from</param>
		/// <param name="start">the first position of the new string</param>
		/// <param name="len">the number of bytes of the new string</param>
		public virtual void set(byte[] utf8, int start, int len)
		{
			setCapacity(len, false);
			System.Array.Copy(utf8, start, bytes, 0, len);
			this.length = len;
		}

		/// <summary>Append a range of bytes to the end of the given text</summary>
		/// <param name="utf8">the data to copy from</param>
		/// <param name="start">the first position to append from utf8</param>
		/// <param name="len">the number of bytes to append</param>
		public virtual void append(byte[] utf8, int start, int len)
		{
			setCapacity(length + len, true);
			System.Array.Copy(utf8, start, bytes, length, len);
			length += len;
		}

		/// <summary>Clear the string to empty.</summary>
		/// <remarks>
		/// Clear the string to empty.
		/// <em>Note</em>: For performance reasons, this call does not clear the
		/// underlying byte array that is retrievable via
		/// <see cref="getBytes()"/>
		/// .
		/// In order to free the byte-array memory, call
		/// <see cref="set(byte[])"/>
		/// with an empty byte array (For example, <code>new byte[0]</code>).
		/// </remarks>
		public virtual void clear()
		{
			length = 0;
		}

		/*
		* Sets the capacity of this Text object to <em>at least</em>
		* <code>len</code> bytes. If the current buffer is longer,
		* then the capacity and existing content of the buffer are
		* unchanged. If <code>len</code> is larger
		* than the current capacity, the Text object's capacity is
		* increased to match.
		* @param len the number of bytes we need
		* @param keepData should the old data be kept
		*/
		private void setCapacity(int len, bool keepData)
		{
			if (bytes == null || bytes.Length < len)
			{
				if (bytes != null && keepData)
				{
					bytes = java.util.Arrays.copyOf(bytes, System.Math.max(len, length << 1));
				}
				else
				{
					bytes = new byte[len];
				}
			}
		}

		/// <summary>Convert text back to string</summary>
		/// <seealso cref="object.ToString()"/>
		public override string ToString()
		{
			try
			{
				return decode(bytes, 0, length);
			}
			catch (java.nio.charset.CharacterCodingException e)
			{
				throw new System.Exception("Should not have happened ", e);
			}
		}

		/// <summary>deserialize</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			int newLength = org.apache.hadoop.io.WritableUtils.readVInt(@in);
			readWithKnownLength(@in, newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in, int maxLength)
		{
			int newLength = org.apache.hadoop.io.WritableUtils.readVInt(@in);
			if (newLength < 0)
			{
				throw new System.IO.IOException("tried to deserialize " + newLength + " bytes of data!  newLength must be non-negative."
					);
			}
			else
			{
				if (newLength >= maxLength)
				{
					throw new System.IO.IOException("tried to deserialize " + newLength + " bytes of data, but maxLength = "
						 + maxLength);
				}
			}
			readWithKnownLength(@in, newLength);
		}

		/// <summary>Skips over one Text in the input.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void skip(java.io.DataInput @in)
		{
			int length = org.apache.hadoop.io.WritableUtils.readVInt(@in);
			org.apache.hadoop.io.WritableUtils.skipFully(@in, length);
		}

		/// <summary>Read a Text object whose length is already known.</summary>
		/// <remarks>
		/// Read a Text object whose length is already known.
		/// This allows creating Text from a stream which uses a different serialization
		/// format.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void readWithKnownLength(java.io.DataInput @in, int len)
		{
			setCapacity(len, false);
			@in.readFully(bytes, 0, len);
			length = len;
		}

		/// <summary>
		/// serialize
		/// write this object to out
		/// length uses zero-compressed encoding
		/// </summary>
		/// <seealso cref="Writable.write(java.io.DataOutput)"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, length);
			@out.write(bytes, 0, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out, int maxLength)
		{
			if (length > maxLength)
			{
				throw new System.IO.IOException("data was too long to write!  Expected " + "less than or equal to "
					 + maxLength + " bytes, but got " + length + " bytes.");
			}
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, length);
			@out.write(bytes, 0, length);
		}

		/// <summary>Returns true iff <code>o</code> is a Text with the same contents.</summary>
		public override bool Equals(object o)
		{
			if (o is org.apache.hadoop.io.Text)
			{
				return base.Equals(o);
			}
			return false;
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		/// <summary>A WritableComparator optimized for Text keys.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)))
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int n1 = org.apache.hadoop.io.WritableUtils.decodeVIntSize(b1[s1]);
				int n2 = org.apache.hadoop.io.WritableUtils.decodeVIntSize(b2[s2]);
				return compareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
			}
		}

		static Text()
		{
			// register this comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.Text)), new org.apache.hadoop.io.Text.Comparator());
		}

		/// STATIC UTILITIES FROM HERE DOWN
		/// <summary>
		/// Converts the provided byte array to a String using the
		/// UTF-8 encoding.
		/// </summary>
		/// <remarks>
		/// Converts the provided byte array to a String using the
		/// UTF-8 encoding. If the input is malformed,
		/// replace by a default value.
		/// </remarks>
		/// <exception cref="java.nio.charset.CharacterCodingException"/>
		public static string decode(byte[] utf8)
		{
			return decode(java.nio.ByteBuffer.wrap(utf8), true);
		}

		/// <exception cref="java.nio.charset.CharacterCodingException"/>
		public static string decode(byte[] utf8, int start, int length)
		{
			return decode(java.nio.ByteBuffer.wrap(utf8, start, length), true);
		}

		/// <summary>
		/// Converts the provided byte array to a String using the
		/// UTF-8 encoding.
		/// </summary>
		/// <remarks>
		/// Converts the provided byte array to a String using the
		/// UTF-8 encoding. If <code>replace</code> is true, then
		/// malformed input is replaced with the
		/// substitution character, which is U+FFFD. Otherwise the
		/// method throws a MalformedInputException.
		/// </remarks>
		/// <exception cref="java.nio.charset.CharacterCodingException"/>
		public static string decode(byte[] utf8, int start, int length, bool replace)
		{
			return decode(java.nio.ByteBuffer.wrap(utf8, start, length), replace);
		}

		/// <exception cref="java.nio.charset.CharacterCodingException"/>
		private static string decode(java.nio.ByteBuffer utf8, bool replace)
		{
			java.nio.charset.CharsetDecoder decoder = DECODER_FACTORY.get();
			if (replace)
			{
				decoder.onMalformedInput(java.nio.charset.CodingErrorAction.REPLACE);
				decoder.onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPLACE);
			}
			string str = decoder.decode(utf8).ToString();
			// set decoder back to its default value: REPORT
			if (replace)
			{
				decoder.onMalformedInput(java.nio.charset.CodingErrorAction.REPORT);
				decoder.onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
			}
			return str;
		}

		/// <summary>
		/// Converts the provided String to bytes using the
		/// UTF-8 encoding.
		/// </summary>
		/// <remarks>
		/// Converts the provided String to bytes using the
		/// UTF-8 encoding. If the input is malformed,
		/// invalid chars are replaced by a default value.
		/// </remarks>
		/// <returns>
		/// ByteBuffer: bytes stores at ByteBuffer.array()
		/// and length is ByteBuffer.limit()
		/// </returns>
		/// <exception cref="java.nio.charset.CharacterCodingException"/>
		public static java.nio.ByteBuffer encode(string @string)
		{
			return encode(@string, true);
		}

		/// <summary>
		/// Converts the provided String to bytes using the
		/// UTF-8 encoding.
		/// </summary>
		/// <remarks>
		/// Converts the provided String to bytes using the
		/// UTF-8 encoding. If <code>replace</code> is true, then
		/// malformed input is replaced with the
		/// substitution character, which is U+FFFD. Otherwise the
		/// method throws a MalformedInputException.
		/// </remarks>
		/// <returns>
		/// ByteBuffer: bytes stores at ByteBuffer.array()
		/// and length is ByteBuffer.limit()
		/// </returns>
		/// <exception cref="java.nio.charset.CharacterCodingException"/>
		public static java.nio.ByteBuffer encode(string @string, bool replace)
		{
			java.nio.charset.CharsetEncoder encoder = ENCODER_FACTORY.get();
			if (replace)
			{
				encoder.onMalformedInput(java.nio.charset.CodingErrorAction.REPLACE);
				encoder.onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPLACE);
			}
			java.nio.ByteBuffer bytes = encoder.encode(java.nio.CharBuffer.wrap(@string.ToCharArray
				()));
			if (replace)
			{
				encoder.onMalformedInput(java.nio.charset.CodingErrorAction.REPORT);
				encoder.onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
			}
			return bytes;
		}

		public const int DEFAULT_MAX_LEN = 1024 * 1024;

		/// <summary>Read a UTF8 encoded string from in</summary>
		/// <exception cref="System.IO.IOException"/>
		public static string readString(java.io.DataInput @in)
		{
			return readString(@in, int.MaxValue);
		}

		/// <summary>Read a UTF8 encoded string with a maximum size</summary>
		/// <exception cref="System.IO.IOException"/>
		public static string readString(java.io.DataInput @in, int maxLength)
		{
			int length = org.apache.hadoop.io.WritableUtils.readVIntInRange(@in, 0, maxLength
				);
			byte[] bytes = new byte[length];
			@in.readFully(bytes, 0, length);
			return decode(bytes);
		}

		/// <summary>Write a UTF8 encoded string to out</summary>
		/// <exception cref="System.IO.IOException"/>
		public static int writeString(java.io.DataOutput @out, string s)
		{
			java.nio.ByteBuffer bytes = encode(s);
			int length = bytes.limit();
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, length);
			@out.write(((byte[])bytes.array()), 0, length);
			return length;
		}

		/// <summary>Write a UTF8 encoded string with a maximum size to out</summary>
		/// <exception cref="System.IO.IOException"/>
		public static int writeString(java.io.DataOutput @out, string s, int maxLength)
		{
			java.nio.ByteBuffer bytes = encode(s);
			int length = bytes.limit();
			if (length > maxLength)
			{
				throw new System.IO.IOException("string was too long to write!  Expected " + "less than or equal to "
					 + maxLength + " bytes, but got " + length + " bytes.");
			}
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, length);
			@out.write(((byte[])bytes.array()), 0, length);
			return length;
		}

		private const int LEAD_BYTE = 0;

		private const int TRAIL_BYTE_1 = 1;

		private const int TRAIL_BYTE = 2;

		////// states for validateUTF8
		/// <summary>Check if a byte array contains valid utf-8</summary>
		/// <param name="utf8">byte array</param>
		/// <exception cref="java.nio.charset.MalformedInputException">if the byte array contains invalid utf-8
		/// 	</exception>
		public static void validateUTF8(byte[] utf8)
		{
			validateUTF8(utf8, 0, utf8.Length);
		}

		/// <summary>Check to see if a byte array is valid utf-8</summary>
		/// <param name="utf8">the array of bytes</param>
		/// <param name="start">the offset of the first byte in the array</param>
		/// <param name="len">the length of the byte sequence</param>
		/// <exception cref="java.nio.charset.MalformedInputException">if the byte array contains invalid bytes
		/// 	</exception>
		public static void validateUTF8(byte[] utf8, int start, int len)
		{
			int count = start;
			int leadByte = 0;
			int length = 0;
			int state = LEAD_BYTE;
			while (count < start + len)
			{
				int aByte = utf8[count] & unchecked((int)(0xFF));
				switch (state)
				{
					case LEAD_BYTE:
					{
						leadByte = aByte;
						length = bytesFromUTF8[aByte];
						switch (length)
						{
							case 0:
							{
								// check for ASCII
								if (leadByte > unchecked((int)(0x7F)))
								{
									throw new java.nio.charset.MalformedInputException(count);
								}
								break;
							}

							case 1:
							{
								if (leadByte < unchecked((int)(0xC2)) || leadByte > unchecked((int)(0xDF)))
								{
									throw new java.nio.charset.MalformedInputException(count);
								}
								state = TRAIL_BYTE_1;
								break;
							}

							case 2:
							{
								if (leadByte < unchecked((int)(0xE0)) || leadByte > unchecked((int)(0xEF)))
								{
									throw new java.nio.charset.MalformedInputException(count);
								}
								state = TRAIL_BYTE_1;
								break;
							}

							case 3:
							{
								if (leadByte < unchecked((int)(0xF0)) || leadByte > unchecked((int)(0xF4)))
								{
									throw new java.nio.charset.MalformedInputException(count);
								}
								state = TRAIL_BYTE_1;
								break;
							}

							default:
							{
								// too long! Longest valid UTF-8 is 4 bytes (lead + three)
								// or if < 0 we got a trail byte in the lead byte position
								throw new java.nio.charset.MalformedInputException(count);
							}
						}
						// switch (length)
						break;
					}

					case TRAIL_BYTE_1:
					{
						if (leadByte == unchecked((int)(0xF0)) && aByte < unchecked((int)(0x90)))
						{
							throw new java.nio.charset.MalformedInputException(count);
						}
						if (leadByte == unchecked((int)(0xF4)) && aByte > unchecked((int)(0x8F)))
						{
							throw new java.nio.charset.MalformedInputException(count);
						}
						if (leadByte == unchecked((int)(0xE0)) && aByte < unchecked((int)(0xA0)))
						{
							throw new java.nio.charset.MalformedInputException(count);
						}
						if (leadByte == unchecked((int)(0xED)) && aByte > unchecked((int)(0x9F)))
						{
							throw new java.nio.charset.MalformedInputException(count);
						}
						goto case TRAIL_BYTE;
					}

					case TRAIL_BYTE:
					{
						// falls through to regular trail-byte test!!
						if (aByte < unchecked((int)(0x80)) || aByte > unchecked((int)(0xBF)))
						{
							throw new java.nio.charset.MalformedInputException(count);
						}
						if (--length == 0)
						{
							state = LEAD_BYTE;
						}
						else
						{
							state = TRAIL_BYTE;
						}
						break;
					}

					default:
					{
						break;
					}
				}
				// switch (state)
				count++;
			}
		}

		/// <summary>Magic numbers for UTF-8.</summary>
		/// <remarks>
		/// Magic numbers for UTF-8. These are the number of bytes
		/// that <em>follow</em> a given lead byte. Trailing bytes
		/// have the value -1. The values 4 and 5 are presented in
		/// this table, even though valid UTF-8 cannot include the
		/// five and six byte sequences.
		/// </remarks>
		internal static readonly int[] bytesFromUTF8 = new int[] { 0, 0, 0, 0, 0, 0, 0, 0
			, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -
			1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -
			1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -
			1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 1, 1, 1, 1, 1, 1, 1, 1
			, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 
			2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 
			5, 5 };

		// trail bytes
		/// <summary>
		/// Returns the next code point at the current position in
		/// the buffer.
		/// </summary>
		/// <remarks>
		/// Returns the next code point at the current position in
		/// the buffer. The buffer's position will be incremented.
		/// Any mark set on this buffer will be changed by this method!
		/// </remarks>
		public static int bytesToCodePoint(java.nio.ByteBuffer bytes)
		{
			bytes.mark();
			byte b = bytes.get();
			bytes.reset();
			int extraBytesToRead = bytesFromUTF8[(b & unchecked((int)(0xFF)))];
			if (extraBytesToRead < 0)
			{
				return -1;
			}
			// trailing byte!
			int ch = 0;
			switch (extraBytesToRead)
			{
				case 5:
				{
					ch += (bytes.get() & unchecked((int)(0xFF)));
					ch <<= 6;
					goto case 4;
				}

				case 4:
				{
					/* remember, illegal UTF-8 */
					ch += (bytes.get() & unchecked((int)(0xFF)));
					ch <<= 6;
					goto case 3;
				}

				case 3:
				{
					/* remember, illegal UTF-8 */
					ch += (bytes.get() & unchecked((int)(0xFF)));
					ch <<= 6;
					goto case 2;
				}

				case 2:
				{
					ch += (bytes.get() & unchecked((int)(0xFF)));
					ch <<= 6;
					goto case 1;
				}

				case 1:
				{
					ch += (bytes.get() & unchecked((int)(0xFF)));
					ch <<= 6;
					goto case 0;
				}

				case 0:
				{
					ch += (bytes.get() & unchecked((int)(0xFF)));
					break;
				}
			}
			ch -= offsetsFromUTF8[extraBytesToRead];
			return ch;
		}

		internal static readonly int[] offsetsFromUTF8 = new int[] { unchecked((int)(0x00000000
			)), unchecked((int)(0x00003080)), unchecked((int)(0x000E2080)), unchecked((int)(
			0x03C82080)), unchecked((int)(0xFA082080)), unchecked((int)(0x82082080)) };

		/// <summary>
		/// For the given string, returns the number of UTF-8 bytes
		/// required to encode the string.
		/// </summary>
		/// <param name="string">text to encode</param>
		/// <returns>number of UTF-8 bytes required to encode</returns>
		public static int utf8Length(string @string)
		{
			java.text.CharacterIterator iter = new java.text.StringCharacterIterator(@string);
			char ch = iter.first();
			int size = 0;
			while (ch != java.text.CharacterIterator.DONE)
			{
				if ((ch >= unchecked((int)(0xD800))) && (ch < unchecked((int)(0xDC00))))
				{
					// surrogate pair?
					char trail = iter.next();
					if ((trail > unchecked((int)(0xDBFF))) && (trail < unchecked((int)(0xE000))))
					{
						// valid pair
						size += 4;
					}
					else
					{
						// invalid pair
						size += 3;
						iter.previous();
					}
				}
				else
				{
					// rewind one
					if (ch < unchecked((int)(0x80)))
					{
						size++;
					}
					else
					{
						if (ch < unchecked((int)(0x800)))
						{
							size += 2;
						}
						else
						{
							// ch < 0x10000, that is, the largest char value
							size += 3;
						}
					}
				}
				ch = iter.next();
			}
			return size;
		}
	}
}
