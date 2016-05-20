using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>
	/// Adaptor class to wrap byte-array backed objects (including java byte array)
	/// as RawComparable objects.
	/// </summary>
	public sealed class ByteArray : org.apache.hadoop.io.file.tfile.RawComparable
	{
		private readonly byte[] buffer;

		private readonly int offset;

		private readonly int len;

		/// <summary>
		/// Constructing a ByteArray from a
		/// <see cref="org.apache.hadoop.io.BytesWritable"/>
		/// .
		/// </summary>
		/// <param name="other"/>
		public ByteArray(org.apache.hadoop.io.BytesWritable other)
			: this(other.getBytes(), 0, other.getLength())
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
				throw new System.IndexOutOfRangeException();
			}
			this.buffer = buffer;
			this.offset = offset;
			this.len = len;
		}

		/// <returns>the underlying buffer.</returns>
		public byte[] buffer()
		{
			return buffer;
		}

		/// <returns>the offset in the buffer.</returns>
		public int offset()
		{
			return offset;
		}

		/// <returns>the size of the byte array.</returns>
		public int size()
		{
			return len;
		}
	}
}
