using Sharpen;

namespace org.apache.hadoop.io
{
	public sealed class WritableUtils
	{
		/// <exception cref="System.IO.IOException"/>
		public static byte[] readCompressedByteArray(java.io.DataInput @in)
		{
			int length = @in.readInt();
			if (length == -1)
			{
				return null;
			}
			byte[] buffer = new byte[length];
			@in.readFully(buffer);
			// could/should use readFully(buffer,0,length)?
			java.util.zip.GZIPInputStream gzi = new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream
				(buffer, 0, buffer.Length));
			byte[] outbuf = new byte[length];
			java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
			int len;
			while ((len = gzi.read(outbuf, 0, outbuf.Length)) != -1)
			{
				bos.write(outbuf, 0, len);
			}
			byte[] decompressed = bos.toByteArray();
			bos.close();
			gzi.close();
			return decompressed;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void skipCompressedByteArray(java.io.DataInput @in)
		{
			int length = @in.readInt();
			if (length != -1)
			{
				skipFully(@in, length);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static int writeCompressedByteArray(java.io.DataOutput @out, byte[] bytes)
		{
			if (bytes != null)
			{
				java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
				java.util.zip.GZIPOutputStream gzout = new java.util.zip.GZIPOutputStream(bos);
				try
				{
					gzout.write(bytes, 0, bytes.Length);
					gzout.close();
					gzout = null;
				}
				finally
				{
					org.apache.hadoop.io.IOUtils.closeStream(gzout);
				}
				byte[] buffer = bos.toByteArray();
				int len = buffer.Length;
				@out.writeInt(len);
				@out.write(buffer, 0, len);
				/* debug only! Once we have confidence, can lose this. */
				return ((bytes.Length != 0) ? (100 * buffer.Length) / bytes.Length : 0);
			}
			else
			{
				@out.writeInt(-1);
				return -1;
			}
		}

		/* Ugly utility, maybe someone else can do this better  */
		/// <exception cref="System.IO.IOException"/>
		public static string readCompressedString(java.io.DataInput @in)
		{
			byte[] bytes = readCompressedByteArray(@in);
			if (bytes == null)
			{
				return null;
			}
			return Sharpen.Runtime.getStringForBytes(bytes, "UTF-8");
		}

		/// <exception cref="System.IO.IOException"/>
		public static int writeCompressedString(java.io.DataOutput @out, string s)
		{
			return writeCompressedByteArray(@out, (s != null) ? Sharpen.Runtime.getBytesForString
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
		public static void writeString(java.io.DataOutput @out, string s)
		{
			if (s != null)
			{
				byte[] buffer = Sharpen.Runtime.getBytesForString(s, "UTF-8");
				int len = buffer.Length;
				@out.writeInt(len);
				@out.write(buffer, 0, len);
			}
			else
			{
				@out.writeInt(-1);
			}
		}

		/*
		* Read a String as a Network Int n, followed by n Bytes
		* Alternative to 16 bit read/writeUTF.
		* Encoding standard is... ?
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static string readString(java.io.DataInput @in)
		{
			int length = @in.readInt();
			if (length == -1)
			{
				return null;
			}
			byte[] buffer = new byte[length];
			@in.readFully(buffer);
			// could/should use readFully(buffer,0,length)?
			return Sharpen.Runtime.getStringForBytes(buffer, "UTF-8");
		}

		/*
		* Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
		* Could be generalised using introspection.
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static void writeStringArray(java.io.DataOutput @out, string[] s)
		{
			@out.writeInt(s.Length);
			for (int i = 0; i < s.Length; i++)
			{
				writeString(@out, s[i]);
			}
		}

		/*
		* Write a String array as a Nework Int N, followed by Int N Byte Array of
		* compressed Strings. Handles also null arrays and null values.
		* Could be generalised using introspection.
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static void writeCompressedStringArray(java.io.DataOutput @out, string[] s
			)
		{
			if (s == null)
			{
				@out.writeInt(-1);
				return;
			}
			@out.writeInt(s.Length);
			for (int i = 0; i < s.Length; i++)
			{
				writeCompressedString(@out, s[i]);
			}
		}

		/*
		* Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
		* Could be generalised using introspection. Actually this bit couldn't...
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static string[] readStringArray(java.io.DataInput @in)
		{
			int len = @in.readInt();
			if (len == -1)
			{
				return null;
			}
			string[] s = new string[len];
			for (int i = 0; i < len; i++)
			{
				s[i] = readString(@in);
			}
			return s;
		}

		/*
		* Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
		* Could be generalised using introspection. Handles null arrays and null values.
		*
		*/
		/// <exception cref="System.IO.IOException"/>
		public static string[] readCompressedStringArray(java.io.DataInput @in)
		{
			int len = @in.readInt();
			if (len == -1)
			{
				return null;
			}
			string[] s = new string[len];
			for (int i = 0; i < len; i++)
			{
				s[i] = readCompressedString(@in);
			}
			return s;
		}

		/*
		*
		* Test Utility Method Display Byte Array.
		*
		*/
		public static void displayByteArray(byte[] record)
		{
			int i;
			for (i = 0; i < record.Length - 1; i++)
			{
				if (i % 16 == 0)
				{
					System.Console.Out.WriteLine();
				}
				System.Console.Out.Write(int.toHexString(record[i] >> 4 & unchecked((int)(0x0F)))
					);
				System.Console.Out.Write(int.toHexString(record[i] & unchecked((int)(0x0F))));
				System.Console.Out.Write(",");
			}
			System.Console.Out.Write(int.toHexString(record[i] >> 4 & unchecked((int)(0x0F)))
				);
			System.Console.Out.Write(int.toHexString(record[i] & unchecked((int)(0x0F))));
			System.Console.Out.WriteLine();
		}

		/// <summary>Make a copy of a writable object using serialization to a buffer.</summary>
		/// <param name="orig">The object to copy</param>
		/// <returns>The copied object</returns>
		public static T clone<T>(T orig, org.apache.hadoop.conf.Configuration conf)
			where T : org.apache.hadoop.io.Writable
		{
			try
			{
				T newInst = org.apache.hadoop.util.ReflectionUtils.newInstance((java.lang.Class)Sharpen.Runtime.getClassForObject
					(orig), conf);
				// Unchecked cast from Class to Class<T>
				org.apache.hadoop.util.ReflectionUtils.copy(conf, orig, newInst);
				return newInst;
			}
			catch (System.IO.IOException e)
			{
				throw new System.Exception("Error writing/reading clone buffer", e);
			}
		}

		/// <summary>Make a copy of the writable object using serialiation to a buffer</summary>
		/// <param name="dst">the object to copy from</param>
		/// <param name="src">the object to copy into, which is destroyed</param>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"use ReflectionUtils.cloneInto instead.")]
		public static void cloneInto(org.apache.hadoop.io.Writable dst, org.apache.hadoop.io.Writable
			 src)
		{
			org.apache.hadoop.util.ReflectionUtils.cloneWritableInto(dst, src);
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
		public static void writeVInt(java.io.DataOutput stream, int i)
		{
			writeVLong(stream, i);
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
		public static void writeVLong(java.io.DataOutput stream, long i)
		{
			if (i >= -112 && i <= 127)
			{
				stream.writeByte(unchecked((byte)i));
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
			stream.writeByte(unchecked((byte)len));
			len = (len < -120) ? -(len + 120) : -(len + 112);
			for (int idx = len; idx != 0; idx--)
			{
				int shiftbits = (idx - 1) * 8;
				long mask = unchecked((long)(0xFFL)) << shiftbits;
				stream.writeByte(unchecked((byte)((i & mask) >> shiftbits)));
			}
		}

		/// <summary>Reads a zero-compressed encoded long from input stream and returns it.</summary>
		/// <param name="stream">Binary input stream</param>
		/// <exception cref="System.IO.IOException"></exception>
		/// <returns>deserialized long from stream.</returns>
		public static long readVLong(java.io.DataInput stream)
		{
			byte firstByte = stream.readByte();
			int len = decodeVIntSize(firstByte);
			if (len == 1)
			{
				return firstByte;
			}
			long i = 0;
			for (int idx = 0; idx < len - 1; idx++)
			{
				byte b = stream.readByte();
				i = i << 8;
				i = i | (b & unchecked((int)(0xFF)));
			}
			return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
		}

		/// <summary>Reads a zero-compressed encoded integer from input stream and returns it.
		/// 	</summary>
		/// <param name="stream">Binary input stream</param>
		/// <exception cref="System.IO.IOException"></exception>
		/// <returns>deserialized integer from stream.</returns>
		public static int readVInt(java.io.DataInput stream)
		{
			long n = readVLong(stream);
			if ((n > int.MaxValue) || (n < int.MinValue))
			{
				throw new System.IO.IOException("value too long to fit in integer");
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
		public static int readVIntInRange(java.io.DataInput stream, int lower, int upper)
		{
			long n = readVLong(stream);
			if (n < lower)
			{
				if (lower == 0)
				{
					throw new System.IO.IOException("expected non-negative integer, got " + n);
				}
				else
				{
					throw new System.IO.IOException("expected integer greater than or equal to " + lower
						 + ", got " + n);
				}
			}
			if (n > upper)
			{
				throw new System.IO.IOException("expected integer less or equal to " + upper + ", got "
					 + n);
			}
			return (int)n;
		}

		/// <summary>Given the first byte of a vint/vlong, determine the sign</summary>
		/// <param name="value">the first byte</param>
		/// <returns>is the value negative</returns>
		public static bool isNegativeVInt(byte value)
		{
			return ((sbyte)value) < -120 || (value >= -112 && ((sbyte)value) < 0);
		}

		/// <summary>Parse the first byte of a vint/vlong to determine the number of bytes</summary>
		/// <param name="value">the first byte of the vint/vlong</param>
		/// <returns>the total number of bytes (1 to 9)</returns>
		public static int decodeVIntSize(byte value)
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
		public static int getVIntSize(long i)
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
			int dataBits = long.SIZE - long.numberOfLeadingZeros(i);
			// find the number of data bytes + length byte
			return (dataBits + 7) / 8 + 1;
		}

		/// <summary>
		/// Read an Enum value from DataInput, Enums are read and written
		/// using String values.
		/// </summary>
		/// <?/>
		/// <param name="in">DataInput to read from</param>
		/// <param name="enumType">Class type of Enum</param>
		/// <returns>Enum represented by String read from DataInput</returns>
		/// <exception cref="System.IO.IOException"/>
		public static T readEnum<T>(java.io.DataInput @in)
			where T : java.lang.Enum<T>
		{
			System.Type enumType = typeof(T);
			return T.valueOf(enumType, org.apache.hadoop.io.Text.readString(@in));
		}

		/// <summary>writes String value of enum to DataOutput.</summary>
		/// <param name="out">Dataoutput stream</param>
		/// <param name="enumVal">enum value</param>
		/// <exception cref="System.IO.IOException"/>
		public static void writeEnum<_T0>(java.io.DataOutput @out, java.lang.Enum<_T0> enumVal
			)
			where _T0 : java.lang.Enum<E>
		{
			org.apache.hadoop.io.Text.writeString(@out, enumVal.name());
		}

		/// <summary>Skip <i>len</i> number of bytes in input stream<i>in</i></summary>
		/// <param name="in">input stream</param>
		/// <param name="len">number of bytes to skip</param>
		/// <exception cref="System.IO.IOException">when skipped less number of bytes</exception>
		public static void skipFully(java.io.DataInput @in, int len)
		{
			int total = 0;
			int cur = 0;
			while ((total < len) && ((cur = @in.skipBytes(len - total)) > 0))
			{
				total += cur;
			}
			if (total < len)
			{
				throw new System.IO.IOException("Not able to skip " + len + " bytes, possibly " +
					 "due to end of input.");
			}
		}

		/// <summary>Convert writables to a byte array</summary>
		public static byte[] toByteArray(params org.apache.hadoop.io.Writable[] writables
			)
		{
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			try
			{
				foreach (org.apache.hadoop.io.Writable w in writables)
				{
					w.write(@out);
				}
				@out.close();
			}
			catch (System.IO.IOException e)
			{
				throw new System.Exception("Fail to convert writables to a byte array", e);
			}
			return @out.getData();
		}

		/// <summary>Read a string, but check it for sanity.</summary>
		/// <remarks>
		/// Read a string, but check it for sanity. The format consists of a vint
		/// followed by the given number of bytes.
		/// </remarks>
		/// <param name="in">the stream to read from</param>
		/// <param name="maxLength">the largest acceptable length of the encoded string</param>
		/// <returns>the bytes as a string</returns>
		/// <exception cref="System.IO.IOException">if reading from the DataInput fails</exception>
		/// <exception cref="System.ArgumentException">
		/// if the encoded byte size for string
		/// is negative or larger than maxSize. Only the vint is read.
		/// </exception>
		public static string readStringSafely(java.io.DataInput @in, int maxLength)
		{
			int length = readVInt(@in);
			if (length < 0 || length > maxLength)
			{
				throw new System.ArgumentException("Encoded byte size for String was " + length +
					 ", which is outside of 0.." + maxLength + " range.");
			}
			byte[] bytes = new byte[length];
			@in.readFully(bytes, 0, length);
			return org.apache.hadoop.io.Text.decode(bytes);
		}
	}
}
