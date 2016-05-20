using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// A reusable
	/// <see cref="java.io.OutputStream"/>
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
	public class OutputBuffer : java.io.FilterOutputStream
	{
		private class Buffer : java.io.ByteArrayOutputStream
		{
			public virtual byte[] getData()
			{
				return buf;
			}

			public virtual int getLength()
			{
				return count;
			}

			public override void reset()
			{
				count = 0;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.InputStream @in, int len)
			{
				int newcount = count + len;
				if (newcount > buf.Length)
				{
					byte[] newbuf = new byte[System.Math.max(buf.Length << 1, newcount)];
					System.Array.Copy(buf, 0, newbuf, 0, count);
					buf = newbuf;
				}
				org.apache.hadoop.io.IOUtils.readFully(@in, buf, count, len);
				count = newcount;
			}
		}

		private org.apache.hadoop.io.OutputBuffer.Buffer buffer;

		/// <summary>Constructs a new empty buffer.</summary>
		public OutputBuffer()
			: this(new org.apache.hadoop.io.OutputBuffer.Buffer())
		{
		}

		private OutputBuffer(org.apache.hadoop.io.OutputBuffer.Buffer buffer)
			: base(buffer)
		{
			this.buffer = buffer;
		}

		/// <summary>Returns the current contents of the buffer.</summary>
		/// <remarks>
		/// Returns the current contents of the buffer.
		/// Data is only valid to
		/// <see cref="getLength()"/>
		/// .
		/// </remarks>
		public virtual byte[] getData()
		{
			return buffer.getData();
		}

		/// <summary>Returns the length of the valid data currently in the buffer.</summary>
		public virtual int getLength()
		{
			return buffer.getLength();
		}

		/// <summary>Resets the buffer to empty.</summary>
		public virtual org.apache.hadoop.io.OutputBuffer reset()
		{
			buffer.reset();
			return this;
		}

		/// <summary>Writes bytes from a InputStream directly into the buffer.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.InputStream @in, int length)
		{
			buffer.write(@in, length);
		}
	}
}
