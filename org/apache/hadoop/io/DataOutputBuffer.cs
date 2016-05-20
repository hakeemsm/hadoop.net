using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// A reusable
	/// <see cref="java.io.DataOutput"/>
	/// implementation that writes to an in-memory
	/// buffer.
	/// <p>This saves memory over creating a new DataOutputStream and
	/// ByteArrayOutputStream each time data is written.
	/// <p>Typical usage is something like the following:<pre>
	/// DataOutputBuffer buffer = new DataOutputBuffer();
	/// while (... loop condition ...) {
	/// buffer.reset();
	/// ... write buffer using DataOutput methods ...
	/// byte[] data = buffer.getData();
	/// int dataLength = buffer.getLength();
	/// ... write data to its ultimate destination ...
	/// }
	/// </pre>
	/// </summary>
	public class DataOutputBuffer : java.io.DataOutputStream
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

			public Buffer()
				: base()
			{
			}

			public Buffer(int size)
				: base(size)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataInput @in, int len)
			{
				int newcount = count + len;
				if (newcount > buf.Length)
				{
					byte[] newbuf = new byte[System.Math.max(buf.Length << 1, newcount)];
					System.Array.Copy(buf, 0, newbuf, 0, count);
					buf = newbuf;
				}
				@in.readFully(buf, count, len);
				count = newcount;
			}

			/// <summary>Set the count for the current buf.</summary>
			/// <param name="newCount">the new count to set</param>
			/// <returns>the original count</returns>
			private int setCount(int newCount)
			{
				com.google.common.@base.Preconditions.checkArgument(newCount >= 0 && newCount <= 
					buf.Length);
				int oldCount = count;
				count = newCount;
				return oldCount;
			}
		}

		private org.apache.hadoop.io.DataOutputBuffer.Buffer buffer;

		/// <summary>Constructs a new empty buffer.</summary>
		public DataOutputBuffer()
			: this(new org.apache.hadoop.io.DataOutputBuffer.Buffer())
		{
		}

		public DataOutputBuffer(int size)
			: this(new org.apache.hadoop.io.DataOutputBuffer.Buffer(size))
		{
		}

		private DataOutputBuffer(org.apache.hadoop.io.DataOutputBuffer.Buffer buffer)
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
		public virtual org.apache.hadoop.io.DataOutputBuffer reset()
		{
			this.written = 0;
			buffer.reset();
			return this;
		}

		/// <summary>Writes bytes from a DataInput directly into the buffer.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataInput @in, int length)
		{
			buffer.write(@in, length);
		}

		/// <summary>Write to a file stream</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void writeTo(java.io.OutputStream @out)
		{
			buffer.writeTo(@out);
		}

		/// <summary>Overwrite an integer into the internal buffer.</summary>
		/// <remarks>
		/// Overwrite an integer into the internal buffer. Note that this call can only
		/// be used to overwrite existing data in the buffer, i.e., buffer#count cannot
		/// be increased, and DataOutputStream#written cannot be increased.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void writeInt(int v, int offset)
		{
			com.google.common.@base.Preconditions.checkState(offset + 4 <= buffer.getLength()
				);
			byte[] b = new byte[4];
			b[0] = unchecked((byte)(((int)(((uint)v) >> 24)) & unchecked((int)(0xFF))));
			b[1] = unchecked((byte)(((int)(((uint)v) >> 16)) & unchecked((int)(0xFF))));
			b[2] = unchecked((byte)(((int)(((uint)v) >> 8)) & unchecked((int)(0xFF))));
			b[3] = unchecked((byte)(((int)(((uint)v) >> 0)) & unchecked((int)(0xFF))));
			int oldCount = buffer.setCount(offset);
			buffer.write(b);
			buffer.setCount(oldCount);
		}
	}
}
