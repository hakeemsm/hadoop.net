using System;
using System.Collections.Generic;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	public class DataOutputByteBuffer : DataOutputStream
	{
		internal class Buffer : OutputStream
		{
			internal readonly byte[] b = new byte[1];

			internal readonly bool direct;

			internal readonly IList<ByteBuffer> active = new AList<ByteBuffer>();

			internal readonly IList<ByteBuffer> inactive = new List<ByteBuffer>();

			internal int size;

			internal int length;

			internal ByteBuffer current;

			internal Buffer(int size, bool direct)
			{
				this.direct = direct;
				this.size = size;
				current = direct ? ByteBuffer.AllocateDirect(size) : ByteBuffer.Allocate(size);
			}

			public override void Write(int b)
			{
				this.b[0] = unchecked((byte)(b & unchecked((int)(0xFF))));
				Write(this.b);
			}

			public override void Write(byte[] b)
			{
				Write(b, 0, b.Length);
			}

			public override void Write(byte[] b, int off, int len)
			{
				int rem = current.Remaining();
				while (len > rem)
				{
					current.Put(b, off, rem);
					length += rem;
					current.Flip();
					active.AddItem(current);
					off += rem;
					len -= rem;
					rem = GetBuffer(len);
				}
				current.Put(b, off, len);
				length += len;
			}

			internal virtual int GetBuffer(int newsize)
			{
				if (inactive.IsEmpty())
				{
					size = Math.Max(size << 1, newsize);
					current = direct ? ByteBuffer.AllocateDirect(size) : ByteBuffer.Allocate(size);
				}
				else
				{
					current = inactive.Remove(0);
				}
				return current.Remaining();
			}

			internal virtual ByteBuffer[] GetData()
			{
				ByteBuffer[] ret = Sharpen.Collections.ToArray(active, new ByteBuffer[active.Count
					 + 1]);
				ByteBuffer tmp = current.Duplicate();
				tmp.Flip();
				ret[ret.Length - 1] = tmp.Slice();
				return ret;
			}

			internal virtual int GetLength()
			{
				return length;
			}

			internal virtual void Reset()
			{
				length = 0;
				current.Rewind();
				inactive.Add(0, current);
				for (int i = active.Count - 1; i >= 0; --i)
				{
					ByteBuffer b = active.Remove(i);
					b.Rewind();
					inactive.Add(0, b);
				}
				current = inactive.Remove(0);
			}
		}

		private readonly DataOutputByteBuffer.Buffer buffers;

		public DataOutputByteBuffer()
			: this(32)
		{
		}

		public DataOutputByteBuffer(int size)
			: this(size, false)
		{
		}

		public DataOutputByteBuffer(int size, bool direct)
			: this(new DataOutputByteBuffer.Buffer(size, direct))
		{
		}

		private DataOutputByteBuffer(DataOutputByteBuffer.Buffer buffers)
			: base(buffers)
		{
			this.buffers = buffers;
		}

		public virtual ByteBuffer[] GetData()
		{
			return buffers.GetData();
		}

		public virtual int GetLength()
		{
			return buffers.GetLength();
		}

		public virtual void Reset()
		{
			this.written = 0;
			buffers.Reset();
		}
	}
}
