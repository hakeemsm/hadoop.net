using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// A reusable
	/// <see cref="System.IO.InputStream"/>
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
	/// <seealso cref="System.IO.DataOutput"/>
	public class InputBuffer : FilterInputStream
	{
		private class Buffer : ByteArrayInputStream
		{
			public Buffer()
				: base(new byte[] {  })
			{
			}

			public virtual void Reset(byte[] input, int start, int length)
			{
				this.buf = input;
				this.count = start + length;
				this.mark = start;
				this.pos = start;
			}

			public virtual int GetPosition()
			{
				return pos;
			}

			public virtual int GetLength()
			{
				return count;
			}
		}

		private InputBuffer.Buffer buffer;

		/// <summary>Constructs a new empty buffer.</summary>
		public InputBuffer()
			: this(new InputBuffer.Buffer())
		{
		}

		private InputBuffer(InputBuffer.Buffer buffer)
			: base(buffer)
		{
			this.buffer = buffer;
		}

		/// <summary>Resets the data that the buffer reads.</summary>
		public virtual void Reset(byte[] input, int length)
		{
			buffer.Reset(input, 0, length);
		}

		/// <summary>Resets the data that the buffer reads.</summary>
		public virtual void Reset(byte[] input, int start, int length)
		{
			buffer.Reset(input, start, length);
		}

		/// <summary>Returns the current position in the input.</summary>
		public virtual int GetPosition()
		{
			return buffer.GetPosition();
		}

		/// <summary>Returns the length of the input.</summary>
		public virtual int GetLength()
		{
			return buffer.GetLength();
		}
	}
}
