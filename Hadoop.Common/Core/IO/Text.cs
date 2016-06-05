using System;
using System.IO;
using System.Text;
using System.Threading;
using Org.Apache.Hadoop.IO;

namespace Hadoop.Common.Core.IO
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
	public class Text : BinaryComparable, IWritableComparable<BinaryComparable>
	{
	    ThreadLocal<UTF8Encoding> _encoder = new ThreadLocal<UTF8Encoding>();
        
		private sealed class _ThreadLocal_57 : ThreadLocal<CharsetEncoder>
		{
		    protected override Encoding InitialValue()
			{
                
				return Extensions.GetEncoding("UTF-8").NewEncoder().OnMalformedInput(CodingErrorAction
					.Report).OnUnmappableCharacter(CodingErrorAction.Report);
			}
		}

		private static ThreadLocal<CharsetEncoder> EncoderFactory = new _ThreadLocal_57();

		private sealed class _ThreadLocal_67 : ThreadLocal<CharsetDecoder>
		{
			public _ThreadLocal_67()
			{
			}

			protected override CharsetDecoder InitialValue()
			{
				return Extensions.GetEncoding("UTF-8").NewDecoder().OnMalformedInput(CodingErrorAction
					.Report).OnUnmappableCharacter(CodingErrorAction.Report);
			}
		}

		private static ThreadLocal<CharsetDecoder> DecoderFactory = new _ThreadLocal_67();

		private static readonly byte[] EmptyBytes = new byte[0];

		private byte[] bytes;

		private int length;

		public Text()
		{
            _encoder.Value = new UTF8Encoding().OnMalformedInput(CodingErrorAction
                    .Report).OnUnmappableCharacter(CodingErrorAction.Report);
			bytes = EmptyBytes;
		}

		/// <summary>Construct from a string.</summary>
		public Text(string @string)
		{
			Set(@string);
		}

		/// <summary>Construct from another text.</summary>
		public Text(Text utf8)
		{
			Set(utf8);
		}

		/// <summary>Construct from a byte array.</summary>
		public Text(byte[] utf8)
		{
			Set(utf8);
		}

		/// <summary>Get a copy of the bytes that is exactly the length of the data.</summary>
		/// <remarks>
		/// Get a copy of the bytes that is exactly the length of the data.
		/// See
		/// <see cref="GetBytes()"/>
		/// for faster access to the underlying array.
		/// </remarks>
		public virtual byte[] CopyBytes()
		{
              
			byte[] result = new byte[length];
			System.Array.Copy(bytes, 0, result, 0, length);
			return result;
		}

        /// <summary>
        /// Returns the raw bytes; however, only data up to
        /// <see cref="GetLength()"/>
        /// is
        /// valid. Please use
        /// <see cref="CopyBytes()"/>
        /// if you
        /// need the returned array to be precisely the length of the data.
        /// </summary>
        public override byte[] Bytes
        {
            get
            {
                return bytes;
            }
        }

        /// <summary>Returns the number of bytes in the byte array</summary>
        public override int Length
        {
            get
            {
                return length;
            }
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
        public virtual int CharAt(int position)
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
			ByteBuffer bb = (ByteBuffer)ByteBuffer.Wrap(bytes).Position(position);
			return BytesToCodePoint(bb.Slice());
		}

		public virtual int Find(string what)
		{
			return Find(what, 0);
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
		public virtual int Find(string what, int start)
		{
			try
			{
				ByteBuffer src = ByteBuffer.Wrap(this.bytes, 0, this.length);
				ByteBuffer tgt = Encode(what);
				byte b = tgt.Get();
				src.Position(start);
				while (src.HasRemaining())
				{
					if (b == src.Get())
					{
						// matching first byte
						src.Mark();
						// save position in loop
						tgt.Mark();
						// save position in target
						bool found = true;
						int pos = src.Position() - 1;
						while (tgt.HasRemaining())
						{
							if (!src.HasRemaining())
							{
								// src expired first
								tgt.Reset();
								src.Reset();
								found = false;
								break;
							}
							if (!(tgt.Get() == src.Get()))
							{
								tgt.Reset();
								src.Reset();
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
			catch (CharacterCodingException e)
			{
				// not found
				// can't get here
				Runtime.PrintStackTrace(e);
				return -1;
			}
		}

		/// <summary>Set to contain the contents of a string.</summary>
		public virtual void Set(string @string)
		{
			try
			{
				ByteBuffer bb = Encode(@string, true);
				bytes = ((byte[])bb.Array());
				length = bb.Limit();
			}
			catch (CharacterCodingException e)
			{
				throw new RuntimeException("Should not have happened ", e);
			}
		}

		/// <summary>Set to a utf8 byte array</summary>
		public virtual void Set(byte[] utf8)
		{
			Set(utf8, 0, utf8.Length);
		}

		/// <summary>copy a text.</summary>
		public virtual void Set(Text other)
		{
			Set(other.Bytes, 0, other.Length);
		}

		/// <summary>Set the Text to range of bytes</summary>
		/// <param name="utf8">the data to copy from</param>
		/// <param name="start">the first position of the new string</param>
		/// <param name="len">the number of bytes of the new string</param>
		public virtual void Set(byte[] utf8, int start, int len)
		{
			SetCapacity(len, false);
			System.Array.Copy(utf8, start, bytes, 0, len);
			this.length = len;
		}

		/// <summary>Append a range of bytes to the end of the given text</summary>
		/// <param name="utf8">the data to copy from</param>
		/// <param name="start">the first position to append from utf8</param>
		/// <param name="len">the number of bytes to append</param>
		public virtual void Append(byte[] utf8, int start, int len)
		{
			SetCapacity(length + len, true);
			System.Array.Copy(utf8, start, bytes, length, len);
			length += len;
		}

		/// <summary>Clear the string to empty.</summary>
		/// <remarks>
		/// Clear the string to empty.
		/// <em>Note</em>: For performance reasons, this call does not clear the
		/// underlying byte array that is retrievable via
		/// <see cref="GetBytes()"/>
		/// .
		/// In order to free the byte-array memory, call
		/// <see cref="Set(byte[])"/>
		/// with an empty byte array (For example, <code>new byte[0]</code>).
		/// </remarks>
		public virtual void Clear()
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
		private void SetCapacity(int len, bool keepData)
		{
			if (bytes == null || bytes.Length < len)
			{
				if (bytes != null && keepData)
				{
					bytes = Arrays.CopyOf(bytes, Math.Max(len, length << 1));
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
				return Decode(bytes, 0, length);
			}
			catch (CharacterCodingException e)
			{
				throw new RuntimeException("Should not have happened ", e);
			}
		}

		/// <summary>deserialize</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			int newLength = WritableUtils.ReadVInt(@in);
			ReadWithKnownLength(@in, newLength);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in, int maxLength)
		{
			int newLength = WritableUtils.ReadVInt(@in);
			if (newLength < 0)
			{
				throw new IOException("tried to deserialize " + newLength + " bytes of data!  newLength must be non-negative."
					);
			}
			else
			{
				if (newLength >= maxLength)
				{
					throw new IOException("tried to deserialize " + newLength + " bytes of data, but maxLength = "
						 + maxLength);
				}
			}
			ReadWithKnownLength(@in, newLength);
		}

		/// <summary>Skips over one Text in the input.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Skip(BinaryReader @in)
		{
			int length = WritableUtils.ReadVInt(@in);
			WritableUtils.SkipFully(@in, length);
		}

		/// <summary>Read a Text object whose length is already known.</summary>
		/// <remarks>
		/// Read a Text object whose length is already known.
		/// This allows creating Text from a stream which uses a different serialization
		/// format.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadWithKnownLength(BinaryReader @in, int len)
		{
			SetCapacity(len, false);
			@in.ReadFully(bytes, 0, len);
			length = len;
		}

		/// <summary>
		/// serialize
		/// write this object to out
		/// length uses zero-compressed encoding
		/// </summary>
		/// <seealso cref="IWritable.Write(System.IO.BinaryWriter)"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			WritableUtils.WriteVInt(@out, length);
			@out.Write(bytes, 0, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out, int maxLength)
		{
			if (length > maxLength)
			{
				throw new IOException("data was too long to write!  Expected " + "less than or equal to "
					 + maxLength + " bytes, but got " + length + " bytes.");
			}
			WritableUtils.WriteVInt(@out, length);
			@out.Write(bytes, 0, length);
		}

		/// <summary>Returns true iff <code>o</code> is a Text with the same contents.</summary>
		public override bool Equals(object o)
		{
			if (o is Text)
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
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(Text))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int n1 = WritableUtils.DecodeVIntSize(b1[s1]);
				int n2 = WritableUtils.DecodeVIntSize(b2[s2]);
				return CompareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
			}
		}

		static Text()
		{
			// register this comparator
			WritableComparator.Define(typeof(Text), new Text.Comparator(
				));
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
		/// <exception cref="CharacterCodingException"/>
		public static string Decode(byte[] utf8)
		{
			return Decode(ByteBuffer.Wrap(utf8), true);
		}

		/// <exception cref="CharacterCodingException"/>
		public static string Decode(byte[] utf8, int start, int length)
		{
			return Decode(ByteBuffer.Wrap(utf8, start, length), true);
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
		/// <exception cref="CharacterCodingException"/>
		public static string Decode(byte[] utf8, int start, int length, bool replace)
		{
			return Decode(ByteBuffer.Wrap(utf8, start, length), replace);
		}

		/// <exception cref="CharacterCodingException"/>
		private static string Decode(ByteBuffer utf8, bool replace)
		{
			CharsetDecoder decoder = DecoderFactory.Get();
			if (replace)
			{
				decoder.OnMalformedInput(CodingErrorAction.Replace);
				decoder.OnUnmappableCharacter(CodingErrorAction.Replace);
			}
			string str = decoder.Decode(utf8).ToString();
			// set decoder back to its default value: REPORT
			if (replace)
			{
				decoder.OnMalformedInput(CodingErrorAction.Report);
				decoder.OnUnmappableCharacter(CodingErrorAction.Report);
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
		/// <exception cref="CharacterCodingException"/>
		public static ByteBuffer Encode(string @string)
		{
			return Encode(@string, true);
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
		/// <exception cref="CharacterCodingException"/>
		public static ByteBuffer Encode(string @string, bool replace)
		{
			CharsetEncoder encoder = EncoderFactory.Get();
			if (replace)
			{
				encoder.OnMalformedInput(CodingErrorAction.Replace);
				encoder.OnUnmappableCharacter(CodingErrorAction.Replace);
			}
			ByteBuffer bytes = encoder.Encode(CharBuffer.Wrap(@string.ToCharArray()));
			if (replace)
			{
				encoder.OnMalformedInput(CodingErrorAction.Report);
				encoder.OnUnmappableCharacter(CodingErrorAction.Report);
			}
			return bytes;
		}

		public const int DefaultMaxLen = 1024 * 1024;

		/// <summary>Read a UTF8 encoded string from in</summary>
		/// <exception cref="System.IO.IOException"/>
		public static string ReadString(BinaryReader @in)
		{
			return ReadString(@in, int.MaxValue);
		}

		/// <summary>Read a UTF8 encoded string with a maximum size</summary>
		/// <exception cref="System.IO.IOException"/>
		public static string ReadString(BinaryReader @in, int maxLength)
		{
			int length = WritableUtils.ReadVIntInRange(@in, 0, maxLength);
			byte[] bytes = new byte[length];
			@in.ReadFully(bytes, 0, length);
			return Decode(bytes);
		}

		/// <summary>Write a UTF8 encoded string to out</summary>
		/// <exception cref="System.IO.IOException"/>
		public static int WriteString(BinaryWriter @out, string s)
		{
			ByteBuffer bytes = Encode(s);
			int length = bytes.Limit();
			WritableUtils.WriteVInt(@out, length);
			@out.Write(((byte[])bytes.Array()), 0, length);
			return length;
		}

		/// <summary>Write a UTF8 encoded string with a maximum size to out</summary>
		/// <exception cref="System.IO.IOException"/>
		public static int WriteString(BinaryWriter @out, string s, int maxLength)
		{
			ByteBuffer bytes = Encode(s);
			int length = bytes.Limit();
			if (length > maxLength)
			{
				throw new IOException("string was too long to write!  Expected " + "less than or equal to "
					 + maxLength + " bytes, but got " + length + " bytes.");
			}
			WritableUtils.WriteVInt(@out, length);
			@out.Write(((byte[])bytes.Array()), 0, length);
			return length;
		}

		private const int LeadByte = 0;

		private const int TrailByte1 = 1;

		private const int TrailByte = 2;

		////// states for validateUTF8
		/// <summary>Check if a byte array contains valid utf-8</summary>
		/// <param name="utf8">byte array</param>
		/// <exception cref="MalformedInputException">if the byte array contains invalid utf-8
		/// 	</exception>
		public static void ValidateUTF8(byte[] utf8)
		{
			ValidateUTF8(utf8, 0, utf8.Length);
		}

		/// <summary>Check to see if a byte array is valid utf-8</summary>
		/// <param name="utf8">the array of bytes</param>
		/// <param name="start">the offset of the first byte in the array</param>
		/// <param name="len">the length of the byte sequence</param>
		/// <exception cref="MalformedInputException">if the byte array contains invalid bytes
		/// 	</exception>
		public static void ValidateUTF8(byte[] utf8, int start, int len)
		{
			int count = start;
			int leadByte = 0;
			int length = 0;
			int state = LeadByte;
			while (count < start + len)
			{
				int aByte = utf8[count] & unchecked((int)(0xFF));
				switch (state)
				{
					case LeadByte:
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
									throw new MalformedInputException(count);
								}
								break;
							}

							case 1:
							{
								if (leadByte < unchecked((int)(0xC2)) || leadByte > unchecked((int)(0xDF)))
								{
									throw new MalformedInputException(count);
								}
								state = TrailByte1;
								break;
							}

							case 2:
							{
								if (leadByte < unchecked((int)(0xE0)) || leadByte > unchecked((int)(0xEF)))
								{
									throw new MalformedInputException(count);
								}
								state = TrailByte1;
								break;
							}

							case 3:
							{
								if (leadByte < unchecked((int)(0xF0)) || leadByte > unchecked((int)(0xF4)))
								{
									throw new MalformedInputException(count);
								}
								state = TrailByte1;
								break;
							}

							default:
							{
								// too long! Longest valid UTF-8 is 4 bytes (lead + three)
								// or if < 0 we got a trail byte in the lead byte position
								throw new MalformedInputException(count);
							}
						}
						// switch (length)
						break;
					}

					case TrailByte1:
					{
						if (leadByte == unchecked((int)(0xF0)) && aByte < unchecked((int)(0x90)))
						{
							throw new MalformedInputException(count);
						}
						if (leadByte == unchecked((int)(0xF4)) && aByte > unchecked((int)(0x8F)))
						{
							throw new MalformedInputException(count);
						}
						if (leadByte == unchecked((int)(0xE0)) && aByte < unchecked((int)(0xA0)))
						{
							throw new MalformedInputException(count);
						}
						if (leadByte == unchecked((int)(0xED)) && aByte > unchecked((int)(0x9F)))
						{
							throw new MalformedInputException(count);
						}
						goto case TrailByte;
					}

					case TrailByte:
					{
						// falls through to regular trail-byte test!!
						if (aByte < unchecked((int)(0x80)) || aByte > unchecked((int)(0xBF)))
						{
							throw new MalformedInputException(count);
						}
						if (--length == 0)
						{
							state = LeadByte;
						}
						else
						{
							state = TrailByte;
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
		public static int BytesToCodePoint(ByteBuffer bytes)
		{
			bytes.Mark();
			byte b = bytes.Get();
			bytes.Reset();
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
					ch += (bytes.Get() & unchecked((int)(0xFF)));
					ch <<= 6;
					goto case 4;
				}

				case 4:
				{
					/* remember, illegal UTF-8 */
					ch += (bytes.Get() & unchecked((int)(0xFF)));
					ch <<= 6;
					goto case 3;
				}

				case 3:
				{
					/* remember, illegal UTF-8 */
					ch += (bytes.Get() & unchecked((int)(0xFF)));
					ch <<= 6;
					goto case 2;
				}

				case 2:
				{
					ch += (bytes.Get() & unchecked((int)(0xFF)));
					ch <<= 6;
					goto case 1;
				}

				case 1:
				{
					ch += (bytes.Get() & unchecked((int)(0xFF)));
					ch <<= 6;
					goto case 0;
				}

				case 0:
				{
					ch += (bytes.Get() & unchecked((int)(0xFF)));
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
		public static int Utf8Length(string @string)
		{
			CharacterIterator iter = new StringCharacterIterator(@string);
			char ch = iter.First();
			int size = 0;
			while (ch != CharacterIterator.Done)
			{
				if ((ch >= unchecked((int)(0xD800))) && (ch < unchecked((int)(0xDC00))))
				{
					// surrogate pair?
					char trail = iter.Next();
					if ((trail > unchecked((int)(0xDBFF))) && (trail < unchecked((int)(0xE000))))
					{
						// valid pair
						size += 4;
					}
					else
					{
						// invalid pair
						size += 3;
						iter.Previous();
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
				ch = iter.Next();
			}
			return size;
		}
	}
}
