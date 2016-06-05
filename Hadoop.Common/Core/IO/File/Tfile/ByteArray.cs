using System;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>
	/// Adaptor class to wrap byte-array backed objects (including java byte array)
	/// as RawComparable objects.
	/// </summary>
	public sealed class ByteArray : RawComparable
	{
		private readonly byte[] buffer;

		private readonly int offset;

		private readonly int len;

		/// <summary>
		/// Constructing a ByteArray from a
		/// <see cref="BytesWritable"/>
		/// .
		/// </summary>
		/// <param name="other"/>
		public ByteArray(BytesWritable other)
			: this(other.Bytes, 0, other.Length)
		{
		}

		/// <summary>Wrap a whole byte array as a RawComparable.</summary>
		/// <param name="buffer">the byte array buffer.</param>
		public ByteArray(byte[] buffer)
			: this(buffer, 0, buffer.Length)
		{
		}

		/// <summary>Wrap a partial byte array as a RawComparable.</summary>
		/// <param name="buffer">the byte array buffer.</param>
		/// <param name="offset">the starting offset</param>
		/// <param name="len">the length of the consecutive bytes to be wrapped.</param>
		public ByteArray(byte[] buffer, int offset, int len)
		{
			if ((offset | len | (buffer.Length - offset - len)) < 0)
			{
				throw new IndexOutOfRangeException();
			}
			this.buffer = buffer;
			this.offset = offset;
			this.len = len;
		}

		/// <returns>the underlying buffer.</returns>
		public byte[] Buffer()
		{
			return buffer;
		}

		/// <returns>the offset in the buffer.</returns>
		public int Offset()
		{
			return offset;
		}

		/// <returns>the size of the byte array.</returns>
		public int Size()
		{
			return len;
		}
	}
}
