using System;
using System.IO;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// A reusable
	/// <see cref="System.IO.OutputStream"/>
	/// implementation that writes to an in-memory
	/// buffer.
	/// <p>This saves memory over creating a new OutputStream and
	/// ByteArrayOutputStream each time data is written.
	/// <p>Typical usage is something like the following:<pre>
	/// OutputBuffer buffer = new OutputBuffer();
	/// while (... loop condition ...) {
	/// buffer.reset();
	/// ... write buffer using OutputStream methods ...
	/// byte[] data = buffer.getData();
	/// int dataLength = buffer.getLength();
	/// ... write data to its ultimate destination ...
	/// }
	/// </pre>
	/// </summary>
	/// <seealso cref="DataOutputBuffer"/>
	/// <seealso cref="InputBuffer"/>
	public class OutputBuffer : FilterOutputStream
	{
		private class Buffer : ByteArrayOutputStream
		{
			public virtual byte[] GetData()
			{
				return buf;
			}

			public virtual int GetLength()
			{
				return count;
			}

			public override void Reset()
			{
				count = 0;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(InputStream @in, int len)
			{
				int newcount = count + len;
				if (newcount > buf.Length)
				{
					byte[] newbuf = new byte[Math.Max(buf.Length << 1, newcount)];
					System.Array.Copy(buf, 0, newbuf, 0, count);
					buf = newbuf;
				}
				IOUtils.ReadFully(@in, buf, count, len);
				count = newcount;
			}
		}

		private OutputBuffer.Buffer buffer;

		/// <summary>Constructs a new empty buffer.</summary>
		public OutputBuffer()
			: this(new OutputBuffer.Buffer())
		{
		}

		private OutputBuffer(OutputBuffer.Buffer buffer)
			: base(buffer)
		{
			this.buffer = buffer;
		}

		/// <summary>Returns the current contents of the buffer.</summary>
		/// <remarks>
		/// Returns the current contents of the buffer.
		/// Data is only valid to
		/// <see cref="GetLength()"/>
		/// .
		/// </remarks>
		public virtual byte[] GetData()
		{
			return buffer.GetData();
		}

		/// <summary>Returns the length of the valid data currently in the buffer.</summary>
		public virtual int GetLength()
		{
			return buffer.GetLength();
		}

		/// <summary>Resets the buffer to empty.</summary>
		public virtual OutputBuffer Reset()
		{
			buffer.Reset();
			return this;
		}

		/// <summary>Writes bytes from a InputStream directly into the buffer.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(InputStream @in, int length)
		{
			buffer.Write(@in, length);
		}
	}
}
