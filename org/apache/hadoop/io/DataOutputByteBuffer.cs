using Sharpen;

namespace org.apache.hadoop.io
{
	public class DataOutputByteBuffer : java.io.DataOutputStream
	{
		internal class Buffer : java.io.OutputStream
		{
			internal readonly byte[] b = new byte[1];

			internal readonly bool direct;

			internal readonly System.Collections.Generic.IList<java.nio.ByteBuffer> active = 
				new System.Collections.Generic.List<java.nio.ByteBuffer>();

			internal readonly System.Collections.Generic.IList<java.nio.ByteBuffer> inactive = 
				new System.Collections.Generic.LinkedList<java.nio.ByteBuffer>();

			internal int size;

			internal int length;

			internal java.nio.ByteBuffer current;

			internal Buffer(int size, bool direct)
			{
				this.direct = direct;
				this.size = size;
				current = direct ? java.nio.ByteBuffer.allocateDirect(size) : java.nio.ByteBuffer
					.allocate(size);
			}

			public override void write(int b)
			{
				this.b[0] = unchecked((byte)(b & unchecked((int)(0xFF))));
				write(this.b);
			}

			public override void write(byte[] b)
			{
				write(b, 0, b.Length);
			}

			public override void write(byte[] b, int off, int len)
			{
				int rem = current.remaining();
				while (len > rem)
				{
					current.put(b, off, rem);
					length += rem;
					current.flip();
					active.add(current);
					off += rem;
					len -= rem;
					rem = getBuffer(len);
				}
				current.put(b, off, len);
				length += len;
			}

			internal virtual int getBuffer(int newsize)
			{
				if (inactive.isEmpty())
				{
					size = System.Math.max(size << 1, newsize);
					current = direct ? java.nio.ByteBuffer.allocateDirect(size) : java.nio.ByteBuffer
						.allocate(size);
				}
				else
				{
					current = inactive.remove(0);
				}
				return current.remaining();
			}

			internal virtual java.nio.ByteBuffer[] getData()
			{
				java.nio.ByteBuffer[] ret = Sharpen.Collections.ToArray(active, new java.nio.ByteBuffer
					[active.Count + 1]);
				java.nio.ByteBuffer tmp = current.duplicate();
				tmp.flip();
				ret[ret.Length - 1] = tmp.slice();
				return ret;
			}

			internal virtual int getLength()
			{
				return length;
			}

			internal virtual void reset()
			{
				length = 0;
				current.rewind();
				inactive.add(0, current);
				for (int i = active.Count - 1; i >= 0; --i)
				{
					java.nio.ByteBuffer b = active.remove(i);
					b.rewind();
					inactive.add(0, b);
				}
				current = inactive.remove(0);
			}
		}

		private readonly org.apache.hadoop.io.DataOutputByteBuffer.Buffer buffers;

		public DataOutputByteBuffer()
			: this(32)
		{
		}

		public DataOutputByteBuffer(int size)
			: this(size, false)
		{
		}

		public DataOutputByteBuffer(int size, bool direct)
			: this(new org.apache.hadoop.io.DataOutputByteBuffer.Buffer(size, direct))
		{
		}

		private DataOutputByteBuffer(org.apache.hadoop.io.DataOutputByteBuffer.Buffer buffers
			)
			: base(buffers)
		{
			this.buffers = buffers;
		}

		public virtual java.nio.ByteBuffer[] getData()
		{
			return buffers.getData();
		}

		public virtual int getLength()
		{
			return buffers.getLength();
		}

		public virtual void reset()
		{
			this.written = 0;
			buffers.reset();
		}
	}
}
