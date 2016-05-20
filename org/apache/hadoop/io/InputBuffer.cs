using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// A reusable
	/// <see cref="java.io.InputStream"/>
	/// implementation that reads from an in-memory
	/// buffer.
	/// <p>This saves memory over creating a new InputStream and
	/// ByteArrayInputStream each time data is read.
	/// <p>Typical usage is something like the following:<pre>
	/// InputBuffer buffer = new InputBuffer();
	/// while (... loop condition ...) {
	/// byte[] data = ... get data ...;
	/// int dataLength = ... get data length ...;
	/// buffer.reset(data, dataLength);
	/// ... read buffer using InputStream methods ...
	/// }
	/// </pre>
	/// </summary>
	/// <seealso cref="DataInputBuffer"/>
	/// <seealso cref="java.io.DataOutput"/>
	public class InputBuffer : java.io.FilterInputStream
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

			public virtual int getPosition()
			{
				return pos;
			}

			public virtual int getLength()
			{
				return count;
			}
		}

		private org.apache.hadoop.io.InputBuffer.Buffer buffer;

		/// <summary>Constructs a new empty buffer.</summary>
		public InputBuffer()
			: this(new org.apache.hadoop.io.InputBuffer.Buffer())
		{
		}

		private InputBuffer(org.apache.hadoop.io.InputBuffer.Buffer buffer)
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

		/// <summary>Returns the current position in the input.</summary>
		public virtual int getPosition()
		{
			return buffer.getPosition();
		}

		/// <summary>Returns the length of the input.</summary>
		public virtual int getLength()
		{
			return buffer.getLength();
		}
	}
}
