using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO
{
	public sealed class WritableUtils
	{
		/// <exception cref="System.IO.IOException"/>
		public static byte[] ReadCompressedByteArray(BinaryReader @in)
		{
			int length = @in.ReadInt();
			if (length == -1)
			{
				return null;
			}
			byte[] buffer = new byte[length];
			@in.ReadFully(buffer);
			// could/should use readFully(buffer,0,length)?
			GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buffer, 0, buffer
				.Length));
			byte[] outbuf = new byte[length];
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			int len;
			while ((len = gzi.Read(outbuf, 0, outbuf.Length)) != -1)
			{
				bos.Write(outbuf, 0, len);
			}
			byte[] decompressed = bos.ToByteArray();
			bos.Close();
			gzi.Close();
			return decompressed;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void SkipCompressedByteArray(BinaryReader @in)
		{
			int length = @in.ReadInt();
			if (length != -1)
			{
				SkipFully(@in, length);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static int WriteCompressedByteArray(BinaryWriter @out, byte[] bytes)
		{
			if (bytes != null)
			{
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				GZIPOutputStream gzout = new GZIPOutputStream(bos);
				try
				{
					gzout.Write(bytes, 0, bytes.Length);
					gzout.Close();
					gzout = null;
				}
				finally
				{
					IOUtils.CloseStream(gzout);
				}
				byte[] buffer = bos.ToByteArray();
				int len = buffer.Length;
				@out.WriteInt(len);
				@out.Write(buffer, 0, len);
				/* debug only! Once we have confidence, can lose this. */
				return ((bytes.Length != 0) ? (100 * buffer.Length) / bytes.Length : 0);
			}
			else
			{
				@out.WriteInt(-1);
				return -1;
			}
		}

		/* Ugly utility, maybe someone else can do this better  */
		/// <exception cref="System.IO.IOException"/>
		public static string ReadCompressedString(BinaryReader @in)
		{
			byte[] bytes = ReadCompressedByteArray(@in);
			if (bytes == null)
			{
				return null;
			}
			return Runtime.GetStringForBytes(bytes, "UTF-8");
		}

		/// <exception cref="System.IO.IOException"/>
		public static int WriteCompressedString(BinaryWriter @out, string s)
		{
			return WriteCompressedByteArray(@out, (s != null) ? Runtime.GetBytesForString
				(s, "UTF-8") : null);
		}

		/*
		*
		* Write a String as a Network Int n, followed by n Bytes
		* Alternative to 16 bit read/writeUTF.
		* Encoding standard is... ?
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static void WriteString(BinaryWriter @out, string s)
		{
			if (s != null)
			{
				byte[] buffer = Runtime.GetBytesForString(s, "UTF-8");
				int len = buffer.Length;
				@out.WriteInt(len);
				@out.Write(buffer, 0, len);
			}
			else
			{
				@out.WriteInt(-1);
			}
		}

		/*
		* Read a String as a Network Int n, followed by n Bytes
		* Alternative to 16 bit read/writeUTF.
		* Encoding standard is... ?
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static string ReadString(BinaryReader @in)
		{
			int length = @in.ReadInt();
			if (length == -1)
			{
				return null;
			}
			byte[] buffer = new byte[length];
			@in.ReadFully(buffer);
			// could/should use readFully(buffer,0,length)?
			return Runtime.GetStringForBytes(buffer, "UTF-8");
		}

		/*
		* Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
		* Could be generalised using introspection.
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static void WriteStringArray(BinaryWriter @out, string[] s)
		{
			@out.WriteInt(s.Length);
			for (int i = 0; i < s.Length; i++)
			{
				WriteString(@out, s[i]);
			}
		}

		/*
		* Write a String array as a Nework Int N, followed by Int N Byte Array of
		* compressed Strings. Handles also null arrays and null values.
		* Could be generalised using introspection.
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static void WriteCompressedStringArray(BinaryWriter @out, string[] s)
		{
			if (s == null)
			{
				@out.WriteInt(-1);
				return;
			}
			@out.WriteInt(s.Length);
			for (int i = 0; i < s.Length; i++)
			{
				WriteCompressedString(@out, s[i]);
			}
		}

		/*
		* Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
		* Could be generalised using introspection. Actually this bit couldn't...
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static string[] ReadStringArray(BinaryReader @in)
		{
			int len = @in.ReadInt();
			if (len == -1)
			{
				return null;
			}
			string[] s = new string[len];
			for (int i = 0; i < len; i++)
			{
				s[i] = ReadString(@in);
			}
			return s;
		}

		/*
		* Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
		* Could be generalised using introspection. Handles null arrays and null values.
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static string[] ReadCompressedStringArray(BinaryReader @in)
		{
			int len = @in.ReadInt();
			if (len == -1)
			{
				return null;
			}
			string[] s = new string[len];
			for (int i = 0; i < len; i++)
			{
				s[i] = ReadCompressedString(@in);
			}
			return s;
		}

		/*
		*
		* Test Utility Method Display Byte Array.
		*
		*/
		public static void DisplayByteArray(byte[] record)
		{
			int i;
			for (i = 0; i < record.Length - 1; i++)
			{
				if (i % 16 == 0)
				{
					System.Console.Out.WriteLine();
				}
				System.Console.Out.Write(Extensions.ToHexString(record[i] >> 4 & unchecked(
					(int)(0x0F))));
				System.Console.Out.Write(Extensions.ToHexString(record[i] & unchecked((int
					)(0x0F))));
				System.Console.Out.Write(",");
			}
			System.Console.Out.Write(Extensions.ToHexString(record[i] >> 4 & unchecked(
				(int)(0x0F))));
			System.Console.Out.Write(Extensions.ToHexString(record[i] & unchecked((int
				)(0x0F))));
			System.Console.Out.WriteLine();
		}

		/// <summary>Make a copy of a writable object using serialization to a buffer.</summary>
		/// <param name="orig">The object to copy</param>
		/// <returns>The copied object</returns>
		public static T Clone<T>(T orig, Configuration conf)
			where T : IWritable
		{
			try
			{
				T newInst = ReflectionUtils.NewInstance((Type)orig.GetType(), conf);
				// Unchecked cast from Class to Class<T>
				ReflectionUtils.Copy(conf, orig, newInst);
				return newInst;
			}
			catch (IOException e)
			{
				throw new RuntimeException("Error writing/reading clone buffer", e);
			}
		}

		/// <summary>Make a copy of the writable object using serialiation to a buffer</summary>
		/// <param name="dst">the object to copy from</param>
		/// <param name="src">the object to copy into, which is destroyed</param>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"use ReflectionUtils.cloneInto instead.")]
		public static void CloneInto(IWritable dst, IWritable src)
		{
			ReflectionUtils.CloneWritableInto(dst, src);
		}

		/// <summary>Serializes an integer to a binary stream with zero-compressed encoding.</summary>
		/// <remarks>
		/// Serializes an integer to a binary stream with zero-compressed encoding.
		/// For -112 &lt;= i &lt;= 127, only one byte is used with the actual value.
		/// For other values of i, the first byte value indicates whether the
		/// integer is positive or negative, and the number of bytes that follow.
		/// If the first byte value v is between -113 and -116, the following integer
		/// is positive, with number of bytes that follow are -(v+112).
		/// If the first byte value v is between -121 and -124, the following integer
		/// is negative, with number of bytes that follow are -(v+120). Bytes are
		/// stored in the high-non-zero-byte-first order.
		/// </remarks>
		/// <param name="stream">Binary output stream</param>
		/// <param name="i">Integer to be serialized</param>
		/// <exception cref="System.IO.IOException"></exception>
		public static void WriteVInt(BinaryWriter stream, int i)
		{
			WriteVLong(stream, i);
		}

		/// <summary>Serializes a long to a binary stream with zero-compressed encoding.</summary>
		/// <remarks>
		/// Serializes a long to a binary stream with zero-compressed encoding.
		/// For -112 &lt;= i &lt;= 127, only one byte is used with the actual value.
		/// For other values of i, the first byte value indicates whether the
		/// long is positive or negative, and the number of bytes that follow.
		/// If the first byte value v is between -113 and -120, the following long
		/// is positive, with number of bytes that follow are -(v+112).
		/// If the first byte value v is between -121 and -128, the following long
		/// is negative, with number of bytes that follow are -(v+120). Bytes are
		/// stored in the high-non-zero-byte-first order.
		/// </remarks>
		/// <param name="stream">Binary output stream</param>
		/// <param name="i">Long to be serialized</param>
		/// <exception cref="System.IO.IOException"></exception>
		public static void WriteVLong(BinaryWriter stream, long i)
		{
			if (i >= -112 && i <= 127)
			{
				stream.WriteByte(unchecked((byte)i));
				return;
			}
			int len = -112;
			if (i < 0)
			{
				i ^= -1L;
				// take one's complement'
				len = -120;
			}
			long tmp = i;
			while (tmp != 0)
			{
				tmp = tmp >> 8;
				len--;
			}
			stream.WriteByte(unchecked((byte)len));
			len = (len < -120) ? -(len + 120) : -(len + 112);
			for (int idx = len; idx != 0; idx--)
			{
				int shiftbits = (idx - 1) * 8;
				long mask = unchecked((long)(0xFFL)) << shiftbits;
				stream.WriteByte(unchecked((byte)((i & mask) >> shiftbits)));
			}
		}

		/// <summary>Reads a zero-compressed encoded long from input stream and returns it.</summary>
		/// <param name="stream">Binary input stream</param>
		/// <exception cref="System.IO.IOException"></exception>
		/// <returns>deserialized long from stream.</returns>
		public static long ReadVLong(BinaryReader stream)
		{
			byte firstByte = stream.ReadByte();
			int len = DecodeVIntSize(firstByte);
			if (len == 1)
			{
				return firstByte;
			}
			long i = 0;
			for (int idx = 0; idx < len - 1; idx++)
			{
				byte b = stream.ReadByte();
				i = i << 8;
				i = i | (b & unchecked((int)(0xFF)));
			}
			return (IsNegativeVInt(firstByte) ? (i ^ -1L) : i);
		}

		/// <summary>Reads a zero-compressed encoded integer from input stream and returns it.
		/// 	</summary>
		/// <param name="stream">Binary input stream</param>
		/// <exception cref="System.IO.IOException"></exception>
		/// <returns>deserialized integer from stream.</returns>
		public static int ReadVInt(BinaryReader stream)
		{
			long n = ReadVLong(stream);
			if ((n > int.MaxValue) || (n < int.MinValue))
			{
				throw new IOException("value too long to fit in integer");
			}
			return (int)n;
		}

		/// <summary>Reads an integer from the input stream and returns it.</summary>
		/// <remarks>
		/// Reads an integer from the input stream and returns it.
		/// This function validates that the integer is between [lower, upper],
		/// inclusive.
		/// </remarks>
		/// <param name="stream">Binary input stream</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>deserialized integer from stream</returns>
		public static int ReadVIntInRange(BinaryReader stream, int lower, int upper)
		{
			long n = ReadVLong(stream);
			if (n < lower)
			{
				if (lower == 0)
				{
					throw new IOException("expected non-negative integer, got " + n);
				}
				else
				{
					throw new IOException("expected integer greater than or equal to " + lower + ", got "
						 + n);
				}
			}
			if (n > upper)
			{
				throw new IOException("expected integer less or equal to " + upper + ", got " + n
					);
			}
			return (int)n;
		}

		/// <summary>Given the first byte of a vint/vlong, determine the sign</summary>
		/// <param name="value">the first byte</param>
		/// <returns>is the value negative</returns>
		public static bool IsNegativeVInt(byte value)
		{
			return ((sbyte)value) < -120 || (value >= -112 && ((sbyte)value) < 0);
		}

		/// <summary>Parse the first byte of a vint/vlong to determine the number of bytes</summary>
		/// <param name="value">the first byte of the vint/vlong</param>
		/// <returns>the total number of bytes (1 to 9)</returns>
		public static int DecodeVIntSize(byte value)
		{
			if (value >= -112)
			{
				return 1;
			}
			else
			{
				if (((sbyte)value) < -120)
				{
					return -119 - value;
				}
			}
			return -111 - value;
		}

		/// <summary>Get the encoded length if an integer is stored in a variable-length format
		/// 	</summary>
		/// <returns>the encoded length</returns>
		public static int GetVIntSize(long i)
		{
			if (i >= -112 && i <= 127)
			{
				return 1;
			}
			if (i < 0)
			{
				i ^= -1L;
			}
			// take one's complement'
			// find the number of bytes with non-leading zeros
			int dataBits = long.Size - long.NumberOfLeadingZeros(i);
			// find the number of data bytes + length byte
			return (dataBits + 7) / 8 + 1;
		}

		/// <summary>
		/// Read an Enum value from BinaryReader, Enums are read and written
		/// using String values.
		/// </summary>
		/// <?/>
		/// <param name="in">BinaryReader to read from</param>
		/// <param name="enumType">Class type of Enum</param>
		/// <returns>Enum represented by String read from BinaryReader</returns>
		/// <exception cref="System.IO.IOException"/>
		public static T ReadEnum<T>(BinaryReader @in)
			where T : Enum<T>
		{
			System.Type enumType = typeof(T);
			return T.ValueOf(enumType, Text.ReadString(@in));
		}

		/// <summary>writes String value of enum to BinaryWriter.</summary>
		/// <param name="out">Dataoutput stream</param>
		/// <param name="enumVal">enum value</param>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteEnum<_T0>(BinaryWriter @out, Enum<_T0> enumVal)
			where _T0 : Enum<E>
		{
			Text.WriteString(@out, enumVal.Name());
		}

		/// <summary>Skip <i>len</i> number of bytes in input stream<i>in</i></summary>
		/// <param name="in">input stream</param>
		/// <param name="len">number of bytes to skip</param>
		/// <exception cref="System.IO.IOException">when skipped less number of bytes</exception>
		public static void SkipFully(BinaryReader @in, int len)
		{
			int total = 0;
			int cur = 0;
			while ((total < len) && ((cur = @in.SkipBytes(len - total)) > 0))
			{
				total += cur;
			}
			if (total < len)
			{
				throw new IOException("Not able to skip " + len + " bytes, possibly " + "due to end of input."
					);
			}
		}

		/// <summary>Convert writables to a byte array</summary>
		public static byte[] ToByteArray(params IWritable[] writables)
		{
			DataOutputBuffer @out = new DataOutputBuffer();
			try
			{
				foreach (IWritable w in writables)
				{
					w.Write(@out);
				}
				@out.Close();
			}
			catch (IOException e)
			{
				throw new RuntimeException("Fail to convert writables to a byte array", e);
			}
			return @out.GetData();
		}

		/// <summary>Read a string, but check it for sanity.</summary>
		/// <remarks>
		/// Read a string, but check it for sanity. The format consists of a vint
		/// followed by the given number of bytes.
		/// </remarks>
		/// <param name="in">the stream to read from</param>
		/// <param name="maxLength">the largest acceptable length of the encoded string</param>
		/// <returns>the bytes as a string</returns>
		/// <exception cref="System.IO.IOException">if reading from the BinaryReader fails</exception>
		/// <exception cref="System.ArgumentException">
		/// if the encoded byte size for string
		/// is negative or larger than maxSize. Only the vint is read.
		/// </exception>
		public static string ReadStringSafely(BinaryReader @in, int maxLength)
		{
			int length = ReadVInt(@in);
			if (length < 0 || length > maxLength)
			{
				throw new ArgumentException("Encoded byte size for String was " + length + ", which is outside of 0.."
					 + maxLength + " range.");
			}
			byte[] bytes = new byte[length];
			@in.ReadFully(bytes, 0, length);
			return Text.Decode(bytes);
		}
	}
}
