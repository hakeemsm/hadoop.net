using System;
using System.IO;
using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// A reusable
	/// <see cref="System.IO.BinaryWriter"/>
	/// implementation that writes to an in-memory
	/// buffer.
	/// <p>This saves memory over creating a new DataOutputStream and
	/// ByteArrayOutputStream each time data is written.
	/// <p>Typical usage is something like the following:<pre>
	/// DataOutputBuffer buffer = new DataOutputBuffer();
	/// while (... loop condition ...) {
	/// buffer.reset();
	/// ... write buffer using BinaryWriter methods ...
	/// byte[] data = buffer.getData();
	/// int dataLength = buffer.getLength();
	/// ... write data to its ultimate destination ...
	/// }
	/// </pre>
	/// </summary>
	public class DataOutputBuffer : MemoryStream
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

			public Buffer()
				: base()
			{
			}

			public Buffer(int size)
				: base(size)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(BinaryReader @in, int len)
			{
				int newcount = count + len;
				if (newcount > buf.Length)
				{
					byte[] newbuf = new byte[Math.Max(buf.Length << 1, newcount)];
					System.Array.Copy(buf, 0, newbuf, 0, count);
					buf = newbuf;
				}
				@in.ReadFully(buf, count, len);
				count = newcount;
			}

			/// <summary>Set the count for the current buf.</summary>
			/// <param name="newCount">the new count to set</param>
			/// <returns>the original count</returns>
			private int SetCount(int newCount)
			{
				Preconditions.CheckArgument(newCount >= 0 && newCount <= buf.Length);
				int oldCount = count;
				count = newCount;
				return oldCount;
			}
		}

		private DataOutputBuffer.Buffer buffer;

		/// <summary>Constructs a new empty buffer.</summary>
		public DataOutputBuffer()
			: this(new DataOutputBuffer.Buffer())
		{
		}

		public DataOutputBuffer(int size)
			: this(new DataOutputBuffer.Buffer(size))
		{
		}

		private DataOutputBuffer(DataOutputBuffer.Buffer buffer)
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
		public virtual DataOutputBuffer Reset()
		{
			this.written = 0;
			buffer.Reset();
			return this;
		}

		/// <summary>Writes bytes from a BinaryReader directly into the buffer.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryReader @in, int length)
		{
			buffer.Write(@in, length);
		}

		/// <summary>Write to a file stream</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteTo(OutputStream @out)
		{
			buffer.WriteTo(@out);
		}

		/// <summary>Overwrite an integer into the internal buffer.</summary>
		/// <remarks>
		/// Overwrite an integer into the internal buffer. Note that this call can only
		/// be used to overwrite existing data in the buffer, i.e., buffer#count cannot
		/// be increased, and DataOutputStream#written cannot be increased.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteInt(int v, int offset)
		{
			Preconditions.CheckState(offset + 4 <= buffer.GetLength());
			byte[] b = new byte[4];
			b[0] = unchecked((byte)(((int)(((uint)v) >> 24)) & unchecked((int)(0xFF))));
			b[1] = unchecked((byte)(((int)(((uint)v) >> 16)) & unchecked((int)(0xFF))));
			b[2] = unchecked((byte)(((int)(((uint)v) >> 8)) & unchecked((int)(0xFF))));
			b[3] = unchecked((byte)(((int)(((uint)v) >> 0)) & unchecked((int)(0xFF))));
			int oldCount = buffer.SetCount(offset);
			buffer.Write(b);
			buffer.SetCount(oldCount);
		}
	}
}
