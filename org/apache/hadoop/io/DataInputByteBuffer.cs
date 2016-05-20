using Sharpen;

namespace org.apache.hadoop.io
{
	public class DataInputByteBuffer : java.io.DataInputStream
	{
		private class Buffer : java.io.InputStream
		{
			private readonly byte[] scratch = new byte[1];

			internal java.nio.ByteBuffer[] buffers = new java.nio.ByteBuffer[0];

			internal int bidx;

			internal int pos;

			internal int length;

			public override int read()
			{
				if (-1 == read(scratch, 0, 1))
				{
					return -1;
				}
				return scratch[0] & unchecked((int)(0xFF));
			}

			public override int read(byte[] b, int off, int len)
			{
				if (bidx >= buffers.Length)
				{
					return -1;
				}
				int cur = 0;
				do
				{
					int rem = System.Math.min(len, buffers[bidx].remaining());
					buffers[bidx].get(b, off, rem);
					cur += rem;
					off += rem;
					len -= rem;
				}
				while (len > 0 && ++bidx < buffers.Length);
				pos += cur;
				return cur;
			}

			public virtual void reset(java.nio.ByteBuffer[] buffers)
			{
				bidx = pos = length = 0;
				this.buffers = buffers;
				foreach (java.nio.ByteBuffer b in buffers)
				{
					length += b.remaining();
				}
			}

			public virtual int getPosition()
			{
				return pos;
			}

			public virtual int getLength()
			{
				return length;
			}

			public virtual java.nio.ByteBuffer[] getData()
			{
				return buffers;
			}
		}

		private org.apache.hadoop.io.DataInputByteBuffer.Buffer buffers;

		public DataInputByteBuffer()
			: this(new org.apache.hadoop.io.DataInputByteBuffer.Buffer())
		{
		}

		private DataInputByteBuffer(org.apache.hadoop.io.DataInputByteBuffer.Buffer buffers
			)
			: base(buffers)
		{
			this.buffers = buffers;
		}

		public virtual void reset(params java.nio.ByteBuffer[] input)
		{
			buffers.reset(input);
		}

		public virtual java.nio.ByteBuffer[] getData()
		{
			return buffers.getData();
		}

		public virtual int getPosition()
		{
			return buffers.getPosition();
		}

		public virtual int getLength()
		{
			return buffers.getLength();
		}
	}
}
