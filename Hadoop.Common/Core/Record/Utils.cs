using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Record
{
	/// <summary>Various utility functions for Hadoop record I/O runtime.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://avro.apache.org/"">Avro</a>."
		)]
	public class Utils
	{
		/// <summary>Cannot create a new instance of Utils</summary>
		private Utils()
		{
		}

		public static readonly char[] hexchars = new char[] { '0', '1', '2', '3', '4', '5'
			, '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

		/// <param name="s"/>
		/// <returns/>
		internal static string ToXMLString(string s)
		{
			StringBuilder sb = new StringBuilder();
			for (int idx = 0; idx < s.Length; idx++)
			{
				char ch = s[idx];
				if (ch == '<')
				{
					sb.Append("&lt;");
				}
				else
				{
					if (ch == '&')
					{
						sb.Append("&amp;");
					}
					else
					{
						if (ch == '%')
						{
							sb.Append("%0025");
						}
						else
						{
							if (ch < unchecked((int)(0x20)) || (ch > unchecked((int)(0xD7FF)) && ch < unchecked(
								(int)(0xE000))) || (ch > unchecked((int)(0xFFFD))))
							{
								sb.Append("%");
								sb.Append(hexchars[(ch & unchecked((int)(0xF000))) >> 12]);
								sb.Append(hexchars[(ch & unchecked((int)(0x0F00))) >> 8]);
								sb.Append(hexchars[(ch & unchecked((int)(0x00F0))) >> 4]);
								sb.Append(hexchars[(ch & unchecked((int)(0x000F)))]);
							}
							else
							{
								sb.Append(ch);
							}
						}
					}
				}
			}
			return sb.ToString();
		}

		private static int H2c(char ch)
		{
			if (ch >= '0' && ch <= '9')
			{
				return ch - '0';
			}
			else
			{
				if (ch >= 'A' && ch <= 'F')
				{
					return ch - 'A' + 10;
				}
				else
				{
					if (ch >= 'a' && ch <= 'f')
					{
						return ch - 'a' + 10;
					}
				}
			}
			return 0;
		}

		/// <param name="s"/>
		/// <returns/>
		internal static string FromXMLString(string s)
		{
			StringBuilder sb = new StringBuilder();
			for (int idx = 0; idx < s.Length; )
			{
				char ch = s[idx++];
				if (ch == '%')
				{
					int ch1 = H2c(s[idx++]) << 12;
					int ch2 = H2c(s[idx++]) << 8;
					int ch3 = H2c(s[idx++]) << 4;
					int ch4 = H2c(s[idx++]);
					char res = (char)(ch1 | ch2 | ch3 | ch4);
					sb.Append(res);
				}
				else
				{
					sb.Append(ch);
				}
			}
			return sb.ToString();
		}

		/// <param name="s"/>
		/// <returns/>
		internal static string ToCSVString(string s)
		{
			StringBuilder sb = new StringBuilder(s.Length + 1);
			sb.Append('\'');
			int len = s.Length;
			for (int i = 0; i < len; i++)
			{
				char c = s[i];
				switch (c)
				{
					case '\0':
					{
						sb.Append("%00");
						break;
					}

					case '\n':
					{
						sb.Append("%0A");
						break;
					}

					case '\r':
					{
						sb.Append("%0D");
						break;
					}

					case ',':
					{
						sb.Append("%2C");
						break;
					}

					case '}':
					{
						sb.Append("%7D");
						break;
					}

					case '%':
					{
						sb.Append("%25");
						break;
					}

					default:
					{
						sb.Append(c);
						break;
					}
				}
			}
			return sb.ToString();
		}

		/// <param name="s"/>
		/// <exception cref="System.IO.IOException"/>
		/// <returns/>
		internal static string FromCSVString(string s)
		{
			if (s[0] != '\'')
			{
				throw new IOException("Error deserializing string.");
			}
			int len = s.Length;
			StringBuilder sb = new StringBuilder(len - 1);
			for (int i = 1; i < len; i++)
			{
				char c = s[i];
				if (c == '%')
				{
					char ch1 = s[i + 1];
					char ch2 = s[i + 2];
					i += 2;
					if (ch1 == '0' && ch2 == '0')
					{
						sb.Append('\0');
					}
					else
					{
						if (ch1 == '0' && ch2 == 'A')
						{
							sb.Append('\n');
						}
						else
						{
							if (ch1 == '0' && ch2 == 'D')
							{
								sb.Append('\r');
							}
							else
							{
								if (ch1 == '2' && ch2 == 'C')
								{
									sb.Append(',');
								}
								else
								{
									if (ch1 == '7' && ch2 == 'D')
									{
										sb.Append('}');
									}
									else
									{
										if (ch1 == '2' && ch2 == '5')
										{
											sb.Append('%');
										}
										else
										{
											throw new IOException("Error deserializing string.");
										}
									}
								}
							}
						}
					}
				}
				else
				{
					sb.Append(c);
				}
			}
			return sb.ToString();
		}

		/// <param name="s"/>
		/// <returns/>
		internal static string ToXMLBuffer(Buffer s)
		{
			return s.ToString();
		}

		/// <param name="s"/>
		/// <exception cref="System.IO.IOException"/>
		/// <returns/>
		internal static Buffer FromXMLBuffer(string s)
		{
			if (s.Length == 0)
			{
				return new Buffer();
			}
			int blen = s.Length / 2;
			byte[] barr = new byte[blen];
			for (int idx = 0; idx < blen; idx++)
			{
				char c1 = s[2 * idx];
				char c2 = s[2 * idx + 1];
				barr[idx] = unchecked((byte)System.Convert.ToInt32(string.Empty + c1 + c2, 16));
			}
			return new Buffer(barr);
		}

		/// <param name="buf"/>
		/// <returns/>
		internal static string ToCSVBuffer(Buffer buf)
		{
			StringBuilder sb = new StringBuilder("#");
			sb.Append(buf.ToString());
			return sb.ToString();
		}

		/// <summary>
		/// Converts a CSV-serialized representation of buffer to a new
		/// Buffer
		/// </summary>
		/// <param name="s">CSV-serialized representation of buffer</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>Deserialized Buffer</returns>
		internal static Buffer FromCSVBuffer(string s)
		{
			if (s[0] != '#')
			{
				throw new IOException("Error deserializing buffer.");
			}
			if (s.Length == 1)
			{
				return new Buffer();
			}
			int blen = (s.Length - 1) / 2;
			byte[] barr = new byte[blen];
			for (int idx = 0; idx < blen; idx++)
			{
				char c1 = s[2 * idx + 1];
				char c2 = s[2 * idx + 2];
				barr[idx] = unchecked((byte)System.Convert.ToInt32(string.Empty + c1 + c2, 16));
			}
			return new Buffer(barr);
		}

		/// <exception cref="System.IO.IOException"/>
		private static int Utf8LenForCodePoint(int cpt)
		{
			if (cpt >= 0 && cpt <= unchecked((int)(0x7F)))
			{
				return 1;
			}
			if (cpt >= unchecked((int)(0x80)) && cpt <= unchecked((int)(0x07FF)))
			{
				return 2;
			}
			if ((cpt >= unchecked((int)(0x0800)) && cpt < unchecked((int)(0xD800))) || (cpt >
				 unchecked((int)(0xDFFF)) && cpt <= unchecked((int)(0xFFFD))))
			{
				return 3;
			}
			if (cpt >= unchecked((int)(0x10000)) && cpt <= unchecked((int)(0x10FFFF)))
			{
				return 4;
			}
			throw new IOException("Illegal Unicode Codepoint " + Sharpen.Extensions.ToHexString
				(cpt) + " in string.");
		}

		private static readonly int B10 = System.Convert.ToInt32("10000000", 2);

		private static readonly int B110 = System.Convert.ToInt32("11000000", 2);

		private static readonly int B1110 = System.Convert.ToInt32("11100000", 2);

		private static readonly int B11110 = System.Convert.ToInt32("11110000", 2);

		private static readonly int B11 = System.Convert.ToInt32("11000000", 2);

		private static readonly int B111 = System.Convert.ToInt32("11100000", 2);

		private static readonly int B1111 = System.Convert.ToInt32("11110000", 2);

		private static readonly int B11111 = System.Convert.ToInt32("11111000", 2);

		/// <exception cref="System.IO.IOException"/>
		private static int WriteUtf8(int cpt, byte[] bytes, int offset)
		{
			if (cpt >= 0 && cpt <= unchecked((int)(0x7F)))
			{
				bytes[offset] = unchecked((byte)cpt);
				return 1;
			}
			if (cpt >= unchecked((int)(0x80)) && cpt <= unchecked((int)(0x07FF)))
			{
				bytes[offset + 1] = unchecked((byte)(B10 | (cpt & unchecked((int)(0x3F)))));
				cpt = cpt >> 6;
				bytes[offset] = unchecked((byte)(B110 | (cpt & unchecked((int)(0x1F)))));
				return 2;
			}
			if ((cpt >= unchecked((int)(0x0800)) && cpt < unchecked((int)(0xD800))) || (cpt >
				 unchecked((int)(0xDFFF)) && cpt <= unchecked((int)(0xFFFD))))
			{
				bytes[offset + 2] = unchecked((byte)(B10 | (cpt & unchecked((int)(0x3F)))));
				cpt = cpt >> 6;
				bytes[offset + 1] = unchecked((byte)(B10 | (cpt & unchecked((int)(0x3F)))));
				cpt = cpt >> 6;
				bytes[offset] = unchecked((byte)(B1110 | (cpt & unchecked((int)(0x0F)))));
				return 3;
			}
			if (cpt >= unchecked((int)(0x10000)) && cpt <= unchecked((int)(0x10FFFF)))
			{
				bytes[offset + 3] = unchecked((byte)(B10 | (cpt & unchecked((int)(0x3F)))));
				cpt = cpt >> 6;
				bytes[offset + 2] = unchecked((byte)(B10 | (cpt & unchecked((int)(0x3F)))));
				cpt = cpt >> 6;
				bytes[offset + 1] = unchecked((byte)(B10 | (cpt & unchecked((int)(0x3F)))));
				cpt = cpt >> 6;
				bytes[offset] = unchecked((byte)(B11110 | (cpt & unchecked((int)(0x07)))));
				return 4;
			}
			throw new IOException("Illegal Unicode Codepoint " + Sharpen.Extensions.ToHexString
				(cpt) + " in string.");
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void ToBinaryString(BinaryWriter @out, string str)
		{
			int strlen = str.Length;
			byte[] bytes = new byte[strlen * 4];
			// Codepoints expand to 4 bytes max
			int utf8Len = 0;
			int idx = 0;
			while (idx < strlen)
			{
				int cpt = str.CodePointAt(idx);
				idx += char.IsSupplementaryCodePoint(cpt) ? 2 : 1;
				utf8Len += WriteUtf8(cpt, bytes, utf8Len);
			}
			WriteVInt(@out, utf8Len);
			@out.Write(bytes, 0, utf8Len);
		}

		internal static bool IsValidCodePoint(int cpt)
		{
			return !((cpt > unchecked((int)(0x10FFFF))) || (cpt >= unchecked((int)(0xD800)) &&
				 cpt <= unchecked((int)(0xDFFF))) || (cpt >= unchecked((int)(0xFFFE)) && cpt <= 
				unchecked((int)(0xFFFF))));
		}

		private static int Utf8ToCodePoint(int b1, int b2, int b3, int b4)
		{
			int cpt = 0;
			cpt = (((b1 & ~B11111) << 18) | ((b2 & ~B11) << 12) | ((b3 & ~B11) << 6) | (b4 & 
				~B11));
			return cpt;
		}

		private static int Utf8ToCodePoint(int b1, int b2, int b3)
		{
			int cpt = 0;
			cpt = (((b1 & ~B1111) << 12) | ((b2 & ~B11) << 6) | (b3 & ~B11));
			return cpt;
		}

		private static int Utf8ToCodePoint(int b1, int b2)
		{
			int cpt = 0;
			cpt = (((b1 & ~B111) << 6) | (b2 & ~B11));
			return cpt;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CheckB10(int b)
		{
			if ((b & B11) != B10)
			{
				throw new IOException("Invalid UTF-8 representation.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string FromBinaryString(BinaryReader din)
		{
			int utf8Len = ReadVInt(din);
			byte[] bytes = new byte[utf8Len];
			din.ReadFully(bytes);
			int len = 0;
			// For the most commmon case, i.e. ascii, numChars = utf8Len
			StringBuilder sb = new StringBuilder(utf8Len);
			while (len < utf8Len)
			{
				int cpt = 0;
				int b1 = bytes[len++] & unchecked((int)(0xFF));
				if (b1 <= unchecked((int)(0x7F)))
				{
					cpt = b1;
				}
				else
				{
					if ((b1 & B11111) == B11110)
					{
						int b2 = bytes[len++] & unchecked((int)(0xFF));
						CheckB10(b2);
						int b3 = bytes[len++] & unchecked((int)(0xFF));
						CheckB10(b3);
						int b4 = bytes[len++] & unchecked((int)(0xFF));
						CheckB10(b4);
						cpt = Utf8ToCodePoint(b1, b2, b3, b4);
					}
					else
					{
						if ((b1 & B1111) == B1110)
						{
							int b2 = bytes[len++] & unchecked((int)(0xFF));
							CheckB10(b2);
							int b3 = bytes[len++] & unchecked((int)(0xFF));
							CheckB10(b3);
							cpt = Utf8ToCodePoint(b1, b2, b3);
						}
						else
						{
							if ((b1 & B111) == B110)
							{
								int b2 = bytes[len++] & unchecked((int)(0xFF));
								CheckB10(b2);
								cpt = Utf8ToCodePoint(b1, b2);
							}
							else
							{
								throw new IOException("Invalid UTF-8 byte " + Sharpen.Extensions.ToHexString(b1) 
									+ " at offset " + (len - 1) + " in length of " + utf8Len);
							}
						}
					}
				}
				if (!IsValidCodePoint(cpt))
				{
					throw new IOException("Illegal Unicode Codepoint " + Sharpen.Extensions.ToHexString
						(cpt) + " in stream.");
				}
				sb.AppendCodePoint(cpt);
			}
			return sb.ToString();
		}

		/// <summary>Parse a float from a byte array.</summary>
		public static float ReadFloat(byte[] bytes, int start)
		{
			return WritableComparator.ReadFloat(bytes, start);
		}

		/// <summary>Parse a double from a byte array.</summary>
		public static double ReadDouble(byte[] bytes, int start)
		{
			return WritableComparator.ReadDouble(bytes, start);
		}

		/// <summary>Reads a zero-compressed encoded long from a byte array and returns it.</summary>
		/// <param name="bytes">byte array with decode long</param>
		/// <param name="start">starting index</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>deserialized long</returns>
		public static long ReadVLong(byte[] bytes, int start)
		{
			return WritableComparator.ReadVLong(bytes, start);
		}

		/// <summary>Reads a zero-compressed encoded integer from a byte array and returns it.
		/// 	</summary>
		/// <param name="bytes">byte array with the encoded integer</param>
		/// <param name="start">start index</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>deserialized integer</returns>
		public static int ReadVInt(byte[] bytes, int start)
		{
			return WritableComparator.ReadVInt(bytes, start);
		}

		/// <summary>Reads a zero-compressed encoded long from a stream and return it.</summary>
		/// <param name="in">input stream</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>deserialized long</returns>
		public static long ReadVLong(BinaryReader @in)
		{
			return WritableUtils.ReadVLong(@in);
		}

		/// <summary>Reads a zero-compressed encoded integer from a stream and returns it.</summary>
		/// <param name="in">input stream</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>deserialized integer</returns>
		public static int ReadVInt(BinaryReader @in)
		{
			return WritableUtils.ReadVInt(@in);
		}

		/// <summary>Get the encoded length if an integer is stored in a variable-length format
		/// 	</summary>
		/// <returns>the encoded length</returns>
		public static int GetVIntSize(long i)
		{
			return WritableUtils.GetVIntSize(i);
		}

		/// <summary>Serializes a long to a binary stream with zero-compressed encoding.</summary>
		/// <remarks>
		/// Serializes a long to a binary stream with zero-compressed encoding.
		/// For
		/// <literal>-112 &lt;= i &lt;= 127</literal>
		/// , only one byte is used with the actual
		/// value. For other values of i, the first byte value indicates whether the
		/// long is positive or negative, and the number of bytes that follow.
		/// If the first byte value v is between -113 and -120, the following long
		/// is positive, with number of bytes that follow are -(v+112).
		/// If the first byte value v is between -121 and -128, the following long
		/// is negative, with number of bytes that follow are -(v+120). Bytes are
		/// stored in the high-non-zero-byte-first order.
		/// </remarks>
		/// <param name="stream">Binary output stream</param>
		/// <param name="i">Long to be serialized</param>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteVLong(BinaryWriter stream, long i)
		{
			WritableUtils.WriteVLong(stream, i);
		}

		/// <summary>Serializes an int to a binary stream with zero-compressed encoding.</summary>
		/// <param name="stream">Binary output stream</param>
		/// <param name="i">int to be serialized</param>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteVInt(BinaryWriter stream, int i)
		{
			WritableUtils.WriteVInt(stream, i);
		}

		/// <summary>Lexicographic order of binary data.</summary>
		public static int CompareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int 
			l2)
		{
			return WritableComparator.CompareBytes(b1, s1, l1, b2, s2, l2);
		}
	}
}
