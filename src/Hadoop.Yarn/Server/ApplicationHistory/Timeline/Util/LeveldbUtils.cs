using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Util
{
	public class LeveldbUtils
	{
		public class KeyBuilder
		{
			private const int MaxNumberOfKeyElements = 10;

			private byte[][] b;

			private bool[] useSeparator;

			private int index;

			private int length;

			public KeyBuilder(int size)
			{
				b = new byte[size][];
				useSeparator = new bool[size];
				index = 0;
				length = 0;
			}

			public static LeveldbUtils.KeyBuilder NewInstance()
			{
				return new LeveldbUtils.KeyBuilder(MaxNumberOfKeyElements);
			}

			public virtual LeveldbUtils.KeyBuilder Add(string s)
			{
				return Add(Sharpen.Runtime.GetBytesForString(s, Sharpen.Extensions.GetEncoding("UTF-8"
					)), true);
			}

			public virtual LeveldbUtils.KeyBuilder Add(byte[] t)
			{
				return Add(t, false);
			}

			public virtual LeveldbUtils.KeyBuilder Add(byte[] t, bool sep)
			{
				b[index] = t;
				useSeparator[index] = sep;
				length += t.Length;
				if (sep)
				{
					length++;
				}
				index++;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual byte[] GetBytes()
			{
				ByteArrayOutputStream baos = new ByteArrayOutputStream(length);
				for (int i = 0; i < index; i++)
				{
					baos.Write(b[i]);
					if (i < index - 1 && useSeparator[i])
					{
						baos.Write(unchecked((int)(0x0)));
					}
				}
				return baos.ToByteArray();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual byte[] GetBytesForLookup()
			{
				ByteArrayOutputStream baos = new ByteArrayOutputStream(length);
				for (int i = 0; i < index; i++)
				{
					baos.Write(b[i]);
					if (useSeparator[i])
					{
						baos.Write(unchecked((int)(0x0)));
					}
				}
				return baos.ToByteArray();
			}
		}

		public class KeyParser
		{
			private readonly byte[] b;

			private int offset;

			public KeyParser(byte[] b, int offset)
			{
				this.b = b;
				this.offset = offset;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string GetNextString()
			{
				if (offset >= b.Length)
				{
					throw new IOException("tried to read nonexistent string from byte array");
				}
				int i = 0;
				while (offset + i < b.Length && b[offset + i] != unchecked((int)(0x0)))
				{
					i++;
				}
				string s = new string(b, offset, i, Sharpen.Extensions.GetEncoding("UTF-8"));
				offset = offset + i + 1;
				return s;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetNextLong()
			{
				if (offset + 8 >= b.Length)
				{
					throw new IOException("byte array ran out when trying to read long");
				}
				long l = GenericObjectMapper.ReadReverseOrderedLong(b, offset);
				offset += 8;
				return l;
			}

			public virtual int GetOffset()
			{
				return offset;
			}
		}

		/// <summary>Returns true if the byte array begins with the specified prefix.</summary>
		public static bool PrefixMatches(byte[] prefix, int prefixlen, byte[] b)
		{
			if (b.Length < prefixlen)
			{
				return false;
			}
			return WritableComparator.CompareBytes(prefix, 0, prefixlen, b, 0, prefixlen) == 
				0;
		}
	}
}
