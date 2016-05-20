using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// A reusable
	/// <see cref="java.io.DataInput"/>
	/// implementation that reads from an in-memory
	/// buffer.
	/// <p>This saves memory over creating a new DataInputStream and
	/// ByteArrayInputStream each time data is read.
	/// <p>Typical usage is something like the following:<pre>
	/// DataInputBuffer buffer = new DataInputBuffer();
	/// while (... loop condition ...) {
	/// byte[] data = ... get data ...;
	/// int dataLength = ... get data length ...;
	/// buffer.reset(data, dataLength);
	/// ... read buffer using DataInput methods ...
	/// }
	/// </pre>
	/// </summary>
	public class DataInputBuffer : java.io.DataInputStream
	{
		private class Buffer : java.io.ByteArrayInputStream
		{
			public Buffer()
				: base(new byte[] {  })
			{
			}

			public virtual void reset(byte[] input, int start, int length)
			{
				this.buf = input;
				this.count = start + length;
				this.mark = start;
				this.pos = start;
			}

			public virtual byte[] getData()
			{
				return buf;
			}

			public virtual int getPosition()
			{
				return pos;
			}

			public virtual int getLength()
			{
				return count;
			}
		}

		private org.apache.hadoop.io.DataInputBuffer.Buffer buffer;

		/// <summary>Constructs a new empty buffer.</summary>
		public DataInputBuffer()
			: this(new org.apache.hadoop.io.DataInputBuffer.Buffer())
		{
		}

		private DataInputBuffer(org.apache.hadoop.io.DataInputBuffer.Buffer buffer)
			: base(buffer)
		{
			this.buffer = buffer;
		}

		/// <summary>Resets the data that the buffer reads.</summary>
		public virtual void reset(byte[] input, int length)
		{
			buffer.reset(input, 0, length);
		}

		/// <summary>Resets the data that the buffer reads.</summary>
		public virtual void reset(byte[] input, int start, int length)
		{
			buffer.reset(input, start, length);
		}

		public virtual byte[] getData()
		{
			return buffer.getData();
		}

		/// <summary>Returns the current position in the input.</summary>
		public virtual int getPosition()
		{
			return buffer.getPosition();
		}

		/// <summary>
		/// Returns the index one greater than the last valid character in the input
		/// stream buffer.
		/// </summary>
		public virtual int getLength()
		{
			return buffer.getLength();
		}
	}
}
