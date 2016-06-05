using System.IO;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// A reusable
	/// <see cref="System.IO.BinaryReader"/>
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
	/// ... read buffer using BinaryReader methods ...
	/// }
	/// </pre>
	/// </summary>
	public class DataInputBuffer : DataInputStream
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

			public virtual byte[] GetData()
			{
				return buf;
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

		private DataInputBuffer.Buffer buffer;

		/// <summary>Constructs a new empty buffer.</summary>
		public DataInputBuffer()
			: this(new DataInputBuffer.Buffer())
		{
		}

		private DataInputBuffer(DataInputBuffer.Buffer buffer)
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

		public virtual byte[] GetData()
		{
			return buffer.GetData();
		}

		/// <summary>Returns the current position in the input.</summary>
		public virtual int GetPosition()
		{
			return buffer.GetPosition();
		}

		/// <summary>
		/// Returns the index one greater than the last valid character in the input
		/// stream buffer.
		/// </summary>
		public virtual int GetLength()
		{
			return buffer.GetLength();
		}
	}
}
