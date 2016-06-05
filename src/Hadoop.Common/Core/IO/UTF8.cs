using System;
using System.IO;
using System.Text;
using Hadoop.Common.Core.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>A WritableComparable for strings that uses the UTF8 encoding.</summary>
	/// <remarks>
	/// A WritableComparable for strings that uses the UTF8 encoding.
	/// <p>Also includes utilities for efficiently reading and writing UTF-8.
	/// Note that this decodes UTF-8 but actually encodes CESU-8, a variant of
	/// UTF-8: see http://en.wikipedia.org/wiki/CESU-8
	/// </remarks>
	[System.ObsoleteAttribute(@"replaced by Text")]
	public class UTF8 : IWritableComparable<UTF8>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(UTF8));

		private static readonly DataInputBuffer Ibuf = new DataInputBuffer();

		private sealed class _ThreadLocal_49 : ThreadLocal<DataOutputBuffer>
		{
			public _ThreadLocal_49()
			{
			}

			protected override DataOutputBuffer InitialValue()
			{
				return new DataOutputBuffer();
			}
		}

		private static readonly ThreadLocal<DataOutputBuffer> ObufFactory = new _ThreadLocal_49
			();

		private static readonly byte[] EmptyBytes = new byte[0];

		private byte[] bytes = EmptyBytes;

		private int length;

		public UTF8()
		{
		}

		/// <summary>Construct from a given string.</summary>
		public UTF8(string @string)
		{
			//set("");
			Set(@string);
		}

		/// <summary>Construct from a given string.</summary>
		public UTF8(UTF8 utf8)
		{
			Set(utf8);
		}

		/// <summary>The raw bytes.</summary>
		public virtual byte[] GetBytes()
		{
			return bytes;
		}

		/// <summary>The number of bytes in the encoded string.</summary>
		public virtual int GetLength()
		{
			return length;
		}

		/// <summary>Set to contain the contents of a string.</summary>
		public virtual void Set(string @string)
		{
			if (@string.Length > unchecked((int)(0xffff)) / 3)
			{
				// maybe too long
				Log.Warn("truncating long string: " + @string.Length + " chars, starting with " +
					 Runtime.Substring(@string, 0, 20));
				@string = Runtime.Substring(@string, 0, unchecked((int)(0xffff)) / 3);
			}
			length = Utf8Length(@string);
			// compute length
			if (length > unchecked((int)(0xffff)))
			{
				// double-check length
				throw new RuntimeException("string too long!");
			}
			if (bytes == null || length > bytes.Length)
			{
				// grow buffer
				bytes = new byte[length];
			}
			try
			{
				// avoid sync'd allocations
				DataOutputBuffer obuf = ObufFactory.Get();
				obuf.Reset();
				WriteChars(obuf, @string, 0, @string.Length);
				System.Array.Copy(obuf.GetData(), 0, bytes, 0, length);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <summary>Set to contain the contents of a string.</summary>
		public virtual void Set(UTF8 other)
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
		public virtual void ReadFields(BinaryReader @in)
		{
			length = @in.ReadUnsignedShort();
			if (bytes == null || bytes.Length < length)
			{
				bytes = new byte[length];
			}
			@in.ReadFully(bytes, 0, length);
		}

		/// <summary>Skips over one UTF8 in the input.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Skip(BinaryReader @in)
		{
			int length = @in.ReadUnsignedShort();
			WritableUtils.SkipFully(@in, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			@out.WriteShort(length);
			@out.Write(bytes, 0, length);
		}

		/// <summary>Compare two UTF8s.</summary>
		public virtual int CompareTo(UTF8 o)
		{
			return WritableComparator.CompareBytes(bytes, 0, length, o.bytes, 0, o.length);
		}

		/// <summary>Convert to a String.</summary>
		public override string ToString()
		{
			StringBuilder buffer = new StringBuilder(length);
			try
			{
				lock (Ibuf)
				{
					Ibuf.Reset(bytes, length);
					ReadChars(Ibuf, buffer, length);
				}
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
			return buffer.ToString();
		}

		/// <summary>Convert to a string, checking for valid UTF8.</summary>
		/// <returns>the converted string</returns>
		/// <exception cref="System.IO.UTFDataFormatException">
		/// if the underlying bytes contain invalid
		/// UTF8 data.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual string ToStringChecked()
		{
			StringBuilder buffer = new StringBuilder(length);
			lock (Ibuf)
			{
				Ibuf.Reset(bytes, length);
				ReadChars(Ibuf, buffer, length);
			}
			return buffer.ToString();
		}

		/// <summary>Returns true iff <code>o</code> is a UTF8 with the same contents.</summary>
		public override bool Equals(object o)
		{
			if (!(o is UTF8))
			{
				return false;
			}
			UTF8 that = (UTF8)o;
			if (this.length != that.length)
			{
				return false;
			}
			else
			{
				return WritableComparator.CompareBytes(bytes, 0, length, that.bytes, 0, that.length
					) == 0;
			}
		}

		public override int GetHashCode()
		{
			return WritableComparator.HashBytes(bytes, length);
		}

		/// <summary>A WritableComparator optimized for UTF8 keys.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(UTF8))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int n1 = ReadUnsignedShort(b1, s1);
				int n2 = ReadUnsignedShort(b2, s2);
				return CompareBytes(b1, s1 + 2, n1, b2, s2 + 2, n2);
			}
		}

		static UTF8()
		{
			// register this comparator
			WritableComparator.Define(typeof(UTF8), new UTF8.Comparator());
		}

		/// STATIC UTILITIES FROM HERE DOWN
		/// These are probably not used much anymore, and might be removed...
		/// <summary>Convert a string to a UTF-8 encoded byte array.</summary>
		/// <seealso cref="Runtime.GetBytesForString(string)"/>
		public static byte[] GetBytes(string @string)
		{
			byte[] result = new byte[Utf8Length(@string)];
			try
			{
				// avoid sync'd allocations
				DataOutputBuffer obuf = ObufFactory.Get();
				obuf.Reset();
				WriteChars(obuf, @string, 0, @string.Length);
				System.Array.Copy(obuf.GetData(), 0, result, 0, obuf.GetLength());
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
			return result;
		}

		/// <summary>Convert a UTF-8 encoded byte array back into a string.</summary>
		/// <exception cref="System.IO.IOException">if the byte array is invalid UTF8</exception>
		public static string FromBytes(byte[] bytes)
		{
			DataInputBuffer dbuf = new DataInputBuffer();
			dbuf.Reset(bytes, 0, bytes.Length);
			StringBuilder buf = new StringBuilder(bytes.Length);
			ReadChars(dbuf, buf, bytes.Length);
			return buf.ToString();
		}

		/// <summary>Read a UTF-8 encoded string.</summary>
		/// <seealso cref="System.IO.BinaryReader.ReadUTF()"/>
		/// <exception cref="System.IO.IOException"/>
		public static string ReadString(BinaryReader @in)
		{
			int bytes = @in.ReadUnsignedShort();
			StringBuilder buffer = new StringBuilder(bytes);
			ReadChars(@in, buffer, bytes);
			return buffer.ToString();
		}

		/// <exception cref="System.IO.UTFDataFormatException"/>
		/// <exception cref="System.IO.IOException"/>
		private static void ReadChars(BinaryReader @in, StringBuilder buffer, int nBytes)
		{
			DataOutputBuffer obuf = ObufFactory.Get();
			obuf.Reset();
			obuf.Write(@in, nBytes);
			byte[] bytes = obuf.GetData();
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
							throw new UTFDataFormatException("Truncated UTF8 at " + StringUtils.ByteToHexString
								(bytes, i - 1, 1));
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
								throw new UTFDataFormatException("Truncated UTF8 at " + StringUtils.ByteToHexString
									(bytes, i - 1, 2));
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
									throw new UTFDataFormatException("Truncated UTF8 at " + StringUtils.ByteToHexString
										(bytes, i - 1, 3));
								}
								// 0b11110xxx: 4-byte sequence
								int codepoint = ((b & unchecked((int)(0x07))) << 18) | ((bytes[i++] & unchecked((
									int)(0x3F))) << 12) | ((bytes[i++] & unchecked((int)(0x3F))) << 6) | ((bytes[i++
									] & unchecked((int)(0x3F))));
								buffer.Append(HighSurrogate(codepoint)).Append(LowSurrogate(codepoint));
							}
							else
							{
								// The UTF8 standard describes 5-byte and 6-byte sequences, but
								// these are no longer allowed as of 2003 (see RFC 3629)
								// Only show the next 6 bytes max in the error code - in case the
								// buffer is large, this will prevent an exceedingly large message.
								int endForError = Math.Min(i + 5, nBytes);
								throw new UTFDataFormatException("Invalid UTF8 at " + StringUtils.ByteToHexString
									(bytes, i - 1, endForError));
							}
						}
					}
				}
			}
		}

		private static char HighSurrogate(int codePoint)
		{
			return (char)(((int)(((uint)codePoint) >> 10)) + (char.MinHighSurrogate - ((int)(
				((uint)char.MinSupplementaryCodePoint) >> 10))));
		}

		private static char LowSurrogate(int codePoint)
		{
			return (char)((codePoint & unchecked((int)(0x3ff))) + char.MinLowSurrogate);
		}

		/// <summary>Write a UTF-8 encoded string.</summary>
		/// <seealso cref="System.IO.BinaryWriter.WriteUTF(string)"/>
		/// <exception cref="System.IO.IOException"/>
		public static int WriteString(BinaryWriter @out, string s)
		{
			if (s.Length > unchecked((int)(0xffff)) / 3)
			{
				// maybe too long
				Log.Warn("truncating long string: " + s.Length + " chars, starting with " + Runtime.Substring
					(s, 0, 20));
				s = Runtime.Substring(s, 0, unchecked((int)(0xffff)) / 3);
			}
			int len = Utf8Length(s);
			if (len > unchecked((int)(0xffff)))
			{
				// double-check length
				throw new IOException("string too long!");
			}
			@out.WriteShort(len);
			WriteChars(@out, s, 0, s.Length);
			return len;
		}

		/// <summary>Returns the number of bytes required to write this.</summary>
		private static int Utf8Length(string @string)
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
		private static void WriteChars(BinaryWriter @out, string s, int start, int length)
		{
			int end = start + length;
			for (int i = start; i < end; i++)
			{
				int code = s[i];
				if (code <= unchecked((int)(0x7F)))
				{
					@out.WriteByte(unchecked((byte)code));
				}
				else
				{
					if (code <= unchecked((int)(0x07FF)))
					{
						@out.WriteByte(unchecked((byte)(unchecked((int)(0xC0)) | ((code >> 6) & unchecked(
							(int)(0x1F))))));
						@out.WriteByte(unchecked((byte)(unchecked((int)(0x80)) | code & unchecked((int)(0x3F
							)))));
					}
					else
					{
						@out.WriteByte(unchecked((byte)(unchecked((int)(0xE0)) | ((code >> 12) & 0X0F))));
						@out.WriteByte(unchecked((byte)(unchecked((int)(0x80)) | ((code >> 6) & unchecked(
							(int)(0x3F))))));
						@out.WriteByte(unchecked((byte)(unchecked((int)(0x80)) | (code & unchecked((int)(
							0x3F))))));
					}
				}
			}
		}
	}
}
