using System.Collections.Generic;
using System.IO;
using System.Text;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>Supporting Utility classes used by TFile, and shared by users of TFile.</summary>
	public sealed class Utils
	{
		/// <summary>Prevent the instantiation of Utils.</summary>
		private Utils()
		{
		}

		// nothing
		/// <summary>Encoding an integer into a variable-length encoding format.</summary>
		/// <remarks>
		/// Encoding an integer into a variable-length encoding format. Synonymous to
		/// <code>Utils#writeVLong(out, n)</code>.
		/// </remarks>
		/// <param name="out">output stream</param>
		/// <param name="n">The integer to be encoded</param>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="WriteVLong(System.IO.BinaryWriter, long)"/>
		public static void WriteVInt(BinaryWriter @out, int n)
		{
			WriteVLong(@out, n);
		}

		/// <summary>Encoding a Long integer into a variable-length encoding format.</summary>
		/// <remarks>
		/// Encoding a Long integer into a variable-length encoding format.
		/// <ul>
		/// <li>if n in [-32, 127): encode in one byte with the actual value.
		/// Otherwise,
		/// <li>if n in [-20*2^8, 20*2^8): encode in two bytes: byte[0] = n/256 - 52;
		/// byte[1]=n&0xff. Otherwise,
		/// <li>if n IN [-16*2^16, 16*2^16): encode in three bytes: byte[0]=n/2^16 -
		/// 88; byte[1]=(n&gt;&gt;8)&0xff; byte[2]=n&0xff. Otherwise,
		/// <li>if n in [-8*2^24, 8*2^24): encode in four bytes: byte[0]=n/2^24 - 112;
		/// byte[1] = (n&gt;&gt;16)&0xff; byte[2] = (n&gt;&gt;8)&0xff; byte[3]=n&0xff. Otherwise:
		/// <li>if n in [-2^31, 2^31): encode in five bytes: byte[0]=-125; byte[1] =
		/// (n&gt;&gt;24)&0xff; byte[2]=(n&gt;&gt;16)&0xff; byte[3]=(n&gt;&gt;8)&0xff; byte[4]=n&0xff;
		/// <li>if n in [-2^39, 2^39): encode in six bytes: byte[0]=-124; byte[1] =
		/// (n&gt;&gt;32)&0xff; byte[2]=(n&gt;&gt;24)&0xff; byte[3]=(n&gt;&gt;16)&0xff;
		/// byte[4]=(n&gt;&gt;8)&0xff; byte[5]=n&0xff
		/// <li>if n in [-2^47, 2^47): encode in seven bytes: byte[0]=-123; byte[1] =
		/// (n&gt;&gt;40)&0xff; byte[2]=(n&gt;&gt;32)&0xff; byte[3]=(n&gt;&gt;24)&0xff;
		/// byte[4]=(n&gt;&gt;16)&0xff; byte[5]=(n&gt;&gt;8)&0xff; byte[6]=n&0xff;
		/// <li>if n in [-2^55, 2^55): encode in eight bytes: byte[0]=-122; byte[1] =
		/// (n&gt;&gt;48)&0xff; byte[2] = (n&gt;&gt;40)&0xff; byte[3]=(n&gt;&gt;32)&0xff;
		/// byte[4]=(n&gt;&gt;24)&0xff; byte[5]=(n&gt;&gt;16)&0xff; byte[6]=(n&gt;&gt;8)&0xff;
		/// byte[7]=n&0xff;
		/// <li>if n in [-2^63, 2^63): encode in nine bytes: byte[0]=-121; byte[1] =
		/// (n&gt;&gt;54)&0xff; byte[2] = (n&gt;&gt;48)&0xff; byte[3] = (n&gt;&gt;40)&0xff;
		/// byte[4]=(n&gt;&gt;32)&0xff; byte[5]=(n&gt;&gt;24)&0xff; byte[6]=(n&gt;&gt;16)&0xff;
		/// byte[7]=(n&gt;&gt;8)&0xff; byte[8]=n&0xff;
		/// </ul>
		/// </remarks>
		/// <param name="out">output stream</param>
		/// <param name="n">the integer number</param>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteVLong(BinaryWriter @out, long n)
		{
			if ((n < 128) && (n >= -32))
			{
				@out.WriteByte((int)n);
				return;
			}
			long un = (n < 0) ? ~n : n;
			// how many bytes do we need to represent the number with sign bit?
			int len = (long.Size - long.NumberOfLeadingZeros(un)) / 8 + 1;
			int firstByte = (int)(n >> ((len - 1) * 8));
			switch (len)
			{
				case 1:
				{
					// fall it through to firstByte==-1, len=2.
					firstByte >>= 8;
					goto case 2;
				}

				case 2:
				{
					if ((firstByte < 20) && (firstByte >= -20))
					{
						@out.WriteByte(firstByte - 52);
						@out.WriteByte((int)n);
						return;
					}
					// fall it through to firstByte==0/-1, len=3.
					firstByte >>= 8;
					goto case 3;
				}

				case 3:
				{
					if ((firstByte < 16) && (firstByte >= -16))
					{
						@out.WriteByte(firstByte - 88);
						@out.WriteShort((int)n);
						return;
					}
					// fall it through to firstByte==0/-1, len=4.
					firstByte >>= 8;
					goto case 4;
				}

				case 4:
				{
					if ((firstByte < 8) && (firstByte >= -8))
					{
						@out.WriteByte(firstByte - 112);
						@out.WriteShort((int)(((uint)((int)n)) >> 8));
						@out.WriteByte((int)n);
						return;
					}
					@out.WriteByte(len - 129);
					@out.WriteInt((int)n);
					return;
				}

				case 5:
				{
					@out.WriteByte(len - 129);
					@out.WriteInt((int)((long)(((ulong)n) >> 8)));
					@out.WriteByte((int)n);
					return;
				}

				case 6:
				{
					@out.WriteByte(len - 129);
					@out.WriteInt((int)((long)(((ulong)n) >> 16)));
					@out.WriteShort((int)n);
					return;
				}

				case 7:
				{
					@out.WriteByte(len - 129);
					@out.WriteInt((int)((long)(((ulong)n) >> 24)));
					@out.WriteShort((int)((long)(((ulong)n) >> 8)));
					@out.WriteByte((int)n);
					return;
				}

				case 8:
				{
					@out.WriteByte(len - 129);
					@out.WriteLong(n);
					return;
				}

				default:
				{
					throw new RuntimeException("Internel error");
				}
			}
		}

		/// <summary>Decoding the variable-length integer.</summary>
		/// <remarks>
		/// Decoding the variable-length integer. Synonymous to
		/// <code>(int)Utils#readVLong(in)</code>.
		/// </remarks>
		/// <param name="in">input stream</param>
		/// <returns>the decoded integer</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="ReadVLong(System.IO.BinaryReader)"/>
		public static int ReadVInt(BinaryReader @in)
		{
			long ret = ReadVLong(@in);
			if ((ret > int.MaxValue) || (ret < int.MinValue))
			{
				throw new RuntimeException("Number too large to be represented as Integer");
			}
			return (int)ret;
		}

		/// <summary>Decoding the variable-length integer.</summary>
		/// <remarks>
		/// Decoding the variable-length integer. Suppose the value of the first byte
		/// is FB, and the following bytes are NB[*].
		/// <ul>
		/// <li>if (FB &gt;= -32), return (long)FB;
		/// <li>if (FB in [-72, -33]), return (FB+52)&lt;&lt;8 + NB[0]&0xff;
		/// <li>if (FB in [-104, -73]), return (FB+88)&lt;&lt;16 + (NB[0]&0xff)&lt;&lt;8 +
		/// NB[1]&0xff;
		/// <li>if (FB in [-120, -105]), return (FB+112)&lt;&lt;24 + (NB[0]&0xff)&lt;&lt;16 +
		/// (NB[1]&0xff)&lt;&lt;8 + NB[2]&0xff;
		/// <li>if (FB in [-128, -121]), return interpret NB[FB+129] as a signed
		/// big-endian integer.
		/// </remarks>
		/// <param name="in">input stream</param>
		/// <returns>the decoded long integer.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static long ReadVLong(BinaryReader @in)
		{
			int firstByte = @in.ReadByte();
			if (firstByte >= -32)
			{
				return firstByte;
			}
			switch ((firstByte + 128) / 8)
			{
				case 11:
				case 10:
				case 9:
				case 8:
				case 7:
				{
					return ((firstByte + 52) << 8) | @in.ReadUnsignedByte();
				}

				case 6:
				case 5:
				case 4:
				case 3:
				{
					return ((firstByte + 88) << 16) | @in.ReadUnsignedShort();
				}

				case 2:
				case 1:
				{
					return ((firstByte + 112) << 24) | (@in.ReadUnsignedShort() << 8) | @in.ReadUnsignedByte
						();
				}

				case 0:
				{
					int len = firstByte + 129;
					switch (len)
					{
						case 4:
						{
							return @in.ReadInt();
						}

						case 5:
						{
							return ((long)@in.ReadInt()) << 8 | @in.ReadUnsignedByte();
						}

						case 6:
						{
							return ((long)@in.ReadInt()) << 16 | @in.ReadUnsignedShort();
						}

						case 7:
						{
							return ((long)@in.ReadInt()) << 24 | (@in.ReadUnsignedShort() << 8) | @in.ReadUnsignedByte
								();
						}

						case 8:
						{
							return @in.ReadLong();
						}

						default:
						{
							throw new IOException("Corrupted VLong encoding");
						}
					}
					goto default;
				}

				default:
				{
					throw new RuntimeException("Internal error");
				}
			}
		}

		/// <summary>Write a String as a VInt n, followed by n Bytes as in Text format.</summary>
		/// <param name="out"/>
		/// <param name="s"/>
		/// <exception cref="System.IO.IOException"/>
		public static void WriteString(BinaryWriter @out, string s)
		{
			if (s != null)
			{
				Text text = new Text(s);
				byte[] buffer = text.Bytes;
				int len = text.Length;
				WriteVInt(@out, len);
				@out.Write(buffer, 0, len);
			}
			else
			{
				WriteVInt(@out, -1);
			}
		}

		/// <summary>Read a String as a VInt n, followed by n Bytes in Text format.</summary>
		/// <param name="in">The input stream.</param>
		/// <returns>The string</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string ReadString(BinaryReader @in)
		{
			int length = ReadVInt(@in);
			if (length == -1)
			{
				return null;
			}
			byte[] buffer = new byte[length];
			@in.ReadFully(buffer);
			return Text.Decode(buffer);
		}

		/// <summary>A generic Version class.</summary>
		/// <remarks>
		/// A generic Version class. We suggest applications built on top of TFile use
		/// this class to maintain version information in their meta blocks.
		/// A version number consists of a major version and a minor version. The
		/// suggested usage of major and minor version number is to increment major
		/// version number when the new storage format is not backward compatible, and
		/// increment the minor version otherwise.
		/// </remarks>
		public sealed class Version : Comparable<Utils.Version>
		{
			private readonly short major;

			private readonly short minor;

			/// <summary>Construct the Version object by reading from the input stream.</summary>
			/// <param name="in">input stream</param>
			/// <exception cref="System.IO.IOException"/>
			public Version(BinaryReader @in)
			{
				major = @in.ReadShort();
				minor = @in.ReadShort();
			}

			/// <summary>Constructor.</summary>
			/// <param name="major">major version.</param>
			/// <param name="minor">minor version.</param>
			public Version(short major, short minor)
			{
				this.major = major;
				this.minor = minor;
			}

			/// <summary>Write the objec to a BinaryWriter.</summary>
			/// <remarks>
			/// Write the objec to a BinaryWriter. The serialized format of the Version is
			/// major version followed by minor version, both as big-endian short
			/// integers.
			/// </remarks>
			/// <param name="out">The BinaryWriter object.</param>
			/// <exception cref="System.IO.IOException"/>
			public void Write(BinaryWriter @out)
			{
				@out.WriteShort(major);
				@out.WriteShort(minor);
			}

			/// <summary>Get the major version.</summary>
			/// <returns>Major version.</returns>
			public int GetMajor()
			{
				return major;
			}

			/// <summary>Get the minor version.</summary>
			/// <returns>The minor version.</returns>
			public int GetMinor()
			{
				return minor;
			}

			/// <summary>Get the size of the serialized Version object.</summary>
			/// <returns>serialized size of the version object.</returns>
			public static int Size()
			{
				return (short.Size + short.Size) / byte.Size;
			}

			/// <summary>Return a string representation of the version.</summary>
			public override string ToString()
			{
				return new StringBuilder("v").Append(major).Append(".").Append(minor).ToString();
			}

			/// <summary>Test compatibility.</summary>
			/// <param name="other">The Version object to test compatibility with.</param>
			/// <returns>
			/// true if both versions have the same major version number; false
			/// otherwise.
			/// </returns>
			public bool CompatibleWith(Utils.Version other)
			{
				return major == other.major;
			}

			/// <summary>Compare this version with another version.</summary>
			public int CompareTo(Utils.Version that)
			{
				if (major != that.major)
				{
					return major - that.major;
				}
				return minor - that.minor;
			}

			public override bool Equals(object other)
			{
				if (this == other)
				{
					return true;
				}
				if (!(other is Utils.Version))
				{
					return false;
				}
				return CompareTo((Utils.Version)other) == 0;
			}

			public override int GetHashCode()
			{
				return (major << 16 + minor);
			}
		}

		/// <summary>Lower bound binary search.</summary>
		/// <remarks>
		/// Lower bound binary search. Find the index to the first element in the list
		/// that compares greater than or equal to key.
		/// </remarks>
		/// <?/>
		/// <param name="list">The list</param>
		/// <param name="key">The input key.</param>
		/// <param name="cmp">Comparator for the key.</param>
		/// <returns>
		/// The index to the desired element if it exists; or list.size()
		/// otherwise.
		/// </returns>
		public static int LowerBound<T, _T1, _T2>(IList<_T1> list, T key, IComparer<_T2> 
			cmp)
			where _T1 : T
		{
			int low = 0;
			int high = list.Count;
			while (low < high)
			{
				int mid = (int)(((uint)(low + high)) >> 1);
				T midVal = list[mid];
				int ret = cmp.Compare(midVal, key);
				if (ret < 0)
				{
					low = mid + 1;
				}
				else
				{
					high = mid;
				}
			}
			return low;
		}

		/// <summary>Upper bound binary search.</summary>
		/// <remarks>
		/// Upper bound binary search. Find the index to the first element in the list
		/// that compares greater than the input key.
		/// </remarks>
		/// <?/>
		/// <param name="list">The list</param>
		/// <param name="key">The input key.</param>
		/// <param name="cmp">Comparator for the key.</param>
		/// <returns>
		/// The index to the desired element if it exists; or list.size()
		/// otherwise.
		/// </returns>
		public static int UpperBound<T, _T1, _T2>(IList<_T1> list, T key, IComparer<_T2> 
			cmp)
			where _T1 : T
		{
			int low = 0;
			int high = list.Count;
			while (low < high)
			{
				int mid = (int)(((uint)(low + high)) >> 1);
				T midVal = list[mid];
				int ret = cmp.Compare(midVal, key);
				if (ret <= 0)
				{
					low = mid + 1;
				}
				else
				{
					high = mid;
				}
			}
			return low;
		}

		/// <summary>Lower bound binary search.</summary>
		/// <remarks>
		/// Lower bound binary search. Find the index to the first element in the list
		/// that compares greater than or equal to key.
		/// </remarks>
		/// <?/>
		/// <param name="list">The list</param>
		/// <param name="key">The input key.</param>
		/// <returns>
		/// The index to the desired element if it exists; or list.size()
		/// otherwise.
		/// </returns>
		public static int LowerBound<T, _T1>(IList<_T1> list, T key)
			where _T1 : Comparable<T>
		{
			int low = 0;
			int high = list.Count;
			while (low < high)
			{
				int mid = (int)(((uint)(low + high)) >> 1);
				Comparable<T> midVal = list[mid];
				int ret = midVal.CompareTo(key);
				if (ret < 0)
				{
					low = mid + 1;
				}
				else
				{
					high = mid;
				}
			}
			return low;
		}

		/// <summary>Upper bound binary search.</summary>
		/// <remarks>
		/// Upper bound binary search. Find the index to the first element in the list
		/// that compares greater than the input key.
		/// </remarks>
		/// <?/>
		/// <param name="list">The list</param>
		/// <param name="key">The input key.</param>
		/// <returns>
		/// The index to the desired element if it exists; or list.size()
		/// otherwise.
		/// </returns>
		public static int UpperBound<T, _T1>(IList<_T1> list, T key)
			where _T1 : Comparable<T>
		{
			int low = 0;
			int high = list.Count;
			while (low < high)
			{
				int mid = (int)(((uint)(low + high)) >> 1);
				Comparable<T> midVal = list[mid];
				int ret = midVal.CompareTo(key);
				if (ret <= 0)
				{
					low = mid + 1;
				}
				else
				{
					high = mid;
				}
			}
			return low;
		}
	}
}
