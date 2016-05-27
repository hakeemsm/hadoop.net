using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	public class DataInputByteBuffer : DataInputStream
	{
		private class Buffer : InputStream
		{
			private readonly byte[] scratch = new byte[1];

			internal ByteBuffer[] buffers = new ByteBuffer[0];

			internal int bidx;

			internal int pos;

			internal int length;

			public override int Read()
			{
				if (-1 == Read(scratch, 0, 1))
				{
					return -1;
				}
				return scratch[0] & unchecked((int)(0xFF));
			}

			public override int Read(byte[] b, int off, int len)
			{
				if (bidx >= buffers.Length)
				{
					return -1;
				}
				int cur = 0;
				do
				{
					int rem = Math.Min(len, buffers[bidx].Remaining());
					buffers[bidx].Get(b, off, rem);
					cur += rem;
					off += rem;
					len -= rem;
				}
				while (len > 0 && ++bidx < buffers.Length);
				pos += cur;
				return cur;
			}

			public virtual void Reset(ByteBuffer[] buffers)
			{
				bidx = pos = length = 0;
				this.buffers = buffers;
				foreach (ByteBuffer b in buffers)
				{
					length += b.Remaining();
				}
			}

			public virtual int GetPosition()
			{
				return pos;
			}

			public virtual int GetLength()
			{
				return length;
			}

			public virtual ByteBuffer[] GetData()
			{
				return buffers;
			}
		}

		private DataInputByteBuffer.Buffer buffers;

		public DataInputByteBuffer()
			: this(new DataInputByteBuffer.Buffer())
		{
		}

		private DataInputByteBuffer(DataInputByteBuffer.Buffer buffers)
			: base(buffers)
		{
			this.buffers = buffers;
		}

		public virtual void Reset(params ByteBuffer[] input)
		{
			buffers.Reset(input);
		}

		public virtual ByteBuffer[] GetData()
		{
			return buffers.GetData();
		}

		public virtual int GetPosition()
		{
			return buffers.GetPosition();
		}

		public virtual int GetLength()
		{
			return buffers.GetLength();
		}
	}
}
