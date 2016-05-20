using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A WritableComparable for strings that uses the UTF8 encoding.</summary>
	/// <remarks>
	/// A WritableComparable for strings that uses the UTF8 encoding.
	/// <p>Also includes utilities for efficiently reading and writing UTF-8.
	/// Note that this decodes UTF-8 but actually encodes CESU-8, a variant of
	/// UTF-8: see http://en.wikipedia.org/wiki/CESU-8
	/// </remarks>
	[System.ObsoleteAttribute(@"replaced by Text")]
	public class UTF8 : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.UTF8
		>
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.UTF8)));

		private static readonly org.apache.hadoop.io.DataInputBuffer IBUF = new org.apache.hadoop.io.DataInputBuffer
			();

		private sealed class _ThreadLocal_49 : java.lang.ThreadLocal<org.apache.hadoop.io.DataOutputBuffer
			>
		{
			public _ThreadLocal_49()
			{
			}

			protected override org.apache.hadoop.io.DataOutputBuffer initialValue()
			{
				return new org.apache.hadoop.io.DataOutputBuffer();
			}
		}

		private static readonly java.lang.ThreadLocal<org.apache.hadoop.io.DataOutputBuffer
			> OBUF_FACTORY = new _ThreadLocal_49();

		private static readonly byte[] EMPTY_BYTES = new byte[0];

		private byte[] bytes = EMPTY_BYTES;

		private int length;

		public UTF8()
		{
		}

		/// <summary>Construct from a given string.</summary>
		public UTF8(string @string)
		{
			//set("");
			set(@string);
		}

		/// <summary>Construct from a given string.</summary>
		public UTF8(org.apache.hadoop.io.UTF8 utf8)
		{
			set(utf8);
		}

		/// <summary>The raw bytes.</summary>
		public virtual byte[] getBytes()
		{
			return bytes;
		}

		/// <summary>The number of bytes in the encoded string.</summary>
		public virtual int getLength()
		{
			return length;
		}

		/// <summary>Set to contain the contents of a string.</summary>
		public virtual void set(string @string)
		{
			if (@string.Length > unchecked((int)(0xffff)) / 3)
			{
				// maybe too long
				LOG.warn("truncating long string: " + @string.Length + " chars, starting with " +
					 Sharpen.Runtime.substring(@string, 0, 20));
				@string = Sharpen.Runtime.substring(@string, 0, unchecked((int)(0xffff)) / 3);
			}
			length = utf8Length(@string);
			// compute length
			if (length > unchecked((int)(0xffff)))
			{
				// double-check length
				throw new System.Exception("string too long!");
			}
			if (bytes == null || length > bytes.Length)
			{
				// grow buffer
				bytes = new byte[length];
			}
			try
			{
				// avoid sync'd allocations
				org.apache.hadoop.io.DataOutputBuffer obuf = OBUF_FACTORY.get();
				obuf.reset();
				writeChars(obuf, @string, 0, @string.Length);
				System.Array.Copy(obuf.getData(), 0, bytes, 0, length);
			}
			catch (System.IO.IOException e)
			{
				throw new System.Exception(e);
			}
		}

		/// <summary>Set to contain the contents of a string.</summary>
		public virtual void set(org.apache.hadoop.io.UTF8 other)
		{
			length = other.length;
			if (bytes == null || length > bytes.Length)
			{
				// grow buffer
				bytes = new byte[length];
			}
			System.Array.Copy(other.bytes, 0, bytes, 0, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			length = @in.readUnsignedShort();
			if (bytes == null || bytes.Length < length)
			{
				bytes = new byte[length];
			}
			@in.readFully(bytes, 0, length);
		}

		/// <summary>Skips over one UTF8 in the input.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void skip(java.io.DataInput @in)
		{
			int length = @in.readUnsignedShort();
			org.apache.hadoop.io.WritableUtils.skipFully(@in, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeShort(length);
			@out.write(bytes, 0, length);
		}

		/// <summary>Compare two UTF8s.</summary>
		public virtual int compareTo(org.apache.hadoop.io.UTF8 o)
		{
			return org.apache.hadoop.io.WritableComparator.compareBytes(bytes, 0, length, o.bytes
				, 0, o.length);
		}

		/// <summary>Convert to a String.</summary>
		public override string ToString()
		{
			java.lang.StringBuilder buffer = new java.lang.StringBuilder(length);
			try
			{
				lock (IBUF)
				{
					IBUF.reset(bytes, length);
					readChars(IBUF, buffer, length);
				}
			}
			catch (System.IO.IOException e)
			{
				throw new System.Exception(e);
			}
			return buffer.ToString();
		}

		/// <summary>Convert to a string, checking for valid UTF8.</summary>
		/// <returns>the converted string</returns>
		/// <exception cref="java.io.UTFDataFormatException">
		/// if the underlying bytes contain invalid
		/// UTF8 data.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual string toStringChecked()
		{
			java.lang.StringBuilder buffer = new java.lang.StringBuilder(length);
			lock (IBUF)
			{
				IBUF.reset(bytes, length);
				readChars(IBUF, buffer, length);
			}
			return buffer.ToString();
		}

		/// <summary>Returns true iff <code>o</code> is a UTF8 with the same contents.</summary>
		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.UTF8))
			{
				return false;
			}
			org.apache.hadoop.io.UTF8 that = (org.apache.hadoop.io.UTF8)o;
			if (this.length != that.length)
			{
				return false;
			}
			else
			{
				return org.apache.hadoop.io.WritableComparator.compareBytes(bytes, 0, length, that
					.bytes, 0, that.length) == 0;
			}
		}

		public override int GetHashCode()
		{
			return org.apache.hadoop.io.WritableComparator.hashBytes(bytes, length);
		}

		/// <summary>A WritableComparator optimized for UTF8 keys.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.UTF8)))
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int n1 = readUnsignedShort(b1, s1);
				int n2 = readUnsignedShort(b2, s2);
				return compareBytes(b1, s1 + 2, n1, b2, s2 + 2, n2);
			}
		}

		static UTF8()
		{
			// register this comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.UTF8)), new org.apache.hadoop.io.UTF8.Comparator());
		}

		/// STATIC UTILITIES FROM HERE DOWN
		/// These are probably not used much anymore, and might be removed...
		/// <summary>Convert a string to a UTF-8 encoded byte array.</summary>
		/// <seealso cref="Sharpen.Runtime.getBytesForString(string)"/>
		public static byte[] getBytes(string @string)
		{
			byte[] result = new byte[utf8Length(@string)];
			try
			{
				// avoid sync'd allocations
				org.apache.hadoop.io.DataOutputBuffer obuf = OBUF_FACTORY.get();
				obuf.reset();
				writeChars(obuf, @string, 0, @string.Length);
				System.Array.Copy(obuf.getData(), 0, result, 0, obuf.getLength());
			}
			catch (System.IO.IOException e)
			{
				throw new System.Exception(e);
			}
			return result;
		}

		/// <summary>Convert a UTF-8 encoded byte array back into a string.</summary>
		/// <exception cref="System.IO.IOException">if the byte array is invalid UTF8</exception>
		public static string fromBytes(byte[] bytes)
		{
			org.apache.hadoop.io.DataInputBuffer dbuf = new org.apache.hadoop.io.DataInputBuffer
				();
			dbuf.reset(bytes, 0, bytes.Length);
			java.lang.StringBuilder buf = new java.lang.StringBuilder(bytes.Length);
			readChars(dbuf, buf, bytes.Length);
			return buf.ToString();
		}

		/// <summary>Read a UTF-8 encoded string.</summary>
		/// <seealso cref="java.io.DataInput.readUTF()"/>
		/// <exception cref="System.IO.IOException"/>
		public static string readString(java.io.DataInput @in)
		{
			int bytes = @in.readUnsignedShort();
			java.lang.StringBuilder buffer = new java.lang.StringBuilder(bytes);
			readChars(@in, buffer, bytes);
			return buffer.ToString();
		}

		/// <exception cref="java.io.UTFDataFormatException"/>
		/// <exception cref="System.IO.IOException"/>
		private static void readChars(java.io.DataInput @in, java.lang.StringBuilder buffer
			, int nBytes)
		{
			org.apache.hadoop.io.DataOutputBuffer obuf = OBUF_FACTORY.get();
			obuf.reset();
			obuf.write(@in, nBytes);
			byte[] bytes = obuf.getData();
			int i = 0;
			while (i < nBytes)
			{
				byte b = bytes[i++];
				if ((b & unchecked((int)(0x80))) == 0)
				{
					// 0b0xxxxxxx: 1-byte sequence
					buffer.Append((char)(b & unchecked((int)(0x7F))));
				}
				else
				{
					if ((b & unchecked((int)(0xE0))) == unchecked((int)(0xC0)))
					{
						if (i >= nBytes)
						{
							throw new java.io.UTFDataFormatException("Truncated UTF8 at " + org.apache.hadoop.util.StringUtils
								.byteToHexString(bytes, i - 1, 1));
						}
						// 0b110xxxxx: 2-byte sequence
						buffer.Append((char)(((b & unchecked((int)(0x1F))) << 6) | (bytes[i++] & unchecked(
							(int)(0x3F)))));
					}
					else
					{
						if ((b & unchecked((int)(0xF0))) == unchecked((int)(0xE0)))
						{
							// 0b1110xxxx: 3-byte sequence
							if (i + 1 >= nBytes)
							{
								throw new java.io.UTFDataFormatException("Truncated UTF8 at " + org.apache.hadoop.util.StringUtils
									.byteToHexString(bytes, i - 1, 2));
							}
							buffer.Append((char)(((b & unchecked((int)(0x0F))) << 12) | ((bytes[i++] & unchecked(
								(int)(0x3F))) << 6) | (bytes[i++] & unchecked((int)(0x3F)))));
						}
						else
						{
							if ((b & unchecked((int)(0xF8))) == unchecked((int)(0xF0)))
							{
								if (i + 2 >= nBytes)
								{
									throw new java.io.UTFDataFormatException("Truncated UTF8 at " + org.apache.hadoop.util.StringUtils
										.byteToHexString(bytes, i - 1, 3));
								}
								// 0b11110xxx: 4-byte sequence
								int codepoint = ((b & unchecked((int)(0x07))) << 18) | ((bytes[i++] & unchecked((
									int)(0x3F))) << 12) | ((bytes[i++] & unchecked((int)(0x3F))) << 6) | ((bytes[i++
									] & unchecked((int)(0x3F))));
								buffer.Append(highSurrogate(codepoint)).Append(lowSurrogate(codepoint));
							}
							else
							{
								// The UTF8 standard describes 5-byte and 6-byte sequences, but
								// these are no longer allowed as of 2003 (see RFC 3629)
								// Only show the next 6 bytes max in the error code - in case the
								// buffer is large, this will prevent an exceedingly large message.
								int endForError = System.Math.min(i + 5, nBytes);
								throw new java.io.UTFDataFormatException("Invalid UTF8 at " + org.apache.hadoop.util.StringUtils
									.byteToHexString(bytes, i - 1, endForError));
							}
						}
					}
				}
			}
		}

		private static char highSurrogate(int codePoint)
		{
			return (char)(((int)(((uint)codePoint) >> 10)) + (char.MIN_HIGH_SURROGATE - ((int
				)(((uint)char.MIN_SUPPLEMENTARY_CODE_POINT) >> 10))));
		}

		private static char lowSurrogate(int codePoint)
		{
			return (char)((codePoint & unchecked((int)(0x3ff))) + char.MIN_LOW_SURROGATE);
		}

		/// <summary>Write a UTF-8 encoded string.</summary>
		/// <seealso cref="java.io.DataOutput.writeUTF(string)"/>
		/// <exception cref="System.IO.IOException"/>
		public static int writeString(java.io.DataOutput @out, string s)
		{
			if (s.Length > unchecked((int)(0xffff)) / 3)
			{
				// maybe too long
				LOG.warn("truncating long string: " + s.Length + " chars, starting with " + Sharpen.Runtime.substring
					(s, 0, 20));
				s = Sharpen.Runtime.substring(s, 0, unchecked((int)(0xffff)) / 3);
			}
			int len = utf8Length(s);
			if (len > unchecked((int)(0xffff)))
			{
				// double-check length
				throw new System.IO.IOException("string too long!");
			}
			@out.writeShort(len);
			writeChars(@out, s, 0, s.Length);
			return len;
		}

		/// <summary>Returns the number of bytes required to write this.</summary>
		private static int utf8Length(string @string)
		{
			int stringLength = @string.Length;
			int utf8Length = 0;
			for (int i = 0; i < stringLength; i++)
			{
				int c = @string[i];
				if (c <= unchecked((int)(0x007F)))
				{
					utf8Length++;
				}
				else
				{
					if (c > unchecked((int)(0x07FF)))
					{
						utf8Length += 3;
					}
					else
					{
						utf8Length += 2;
					}
				}
			}
			return utf8Length;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void writeChars(java.io.DataOutput @out, string s, int start, int 
			length)
		{
			int end = start + length;
			for (int i = start; i < end; i++)
			{
				int code = s[i];
				if (code <= unchecked((int)(0x7F)))
				{
					@out.writeByte(unchecked((byte)code));
				}
				else
				{
					if (code <= unchecked((int)(0x07FF)))
					{
						@out.writeByte(unchecked((byte)(unchecked((int)(0xC0)) | ((code >> 6) & unchecked(
							(int)(0x1F))))));
						@out.writeByte(unchecked((byte)(unchecked((int)(0x80)) | code & unchecked((int)(0x3F
							)))));
					}
					else
					{
						@out.writeByte(unchecked((byte)(unchecked((int)(0xE0)) | ((code >> 12) & 0X0F))));
						@out.writeByte(unchecked((byte)(unchecked((int)(0x80)) | ((code >> 6) & unchecked(
							(int)(0x3F))))));
						@out.writeByte(unchecked((byte)(unchecked((int)(0x80)) | (code & unchecked((int)(
							0x3F))))));
					}
				}
			}
		}
	}
}
