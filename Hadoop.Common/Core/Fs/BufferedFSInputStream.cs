using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>A class optimizes reading from FSInputStream by bufferring</summary>
	public class BufferedFSInputStream : BufferedInputStream, Seekable, PositionedReadable
		, HasFileDescriptor
	{
		/// <summary>
		/// Creates a <code>BufferedFSInputStream</code>
		/// with the specified buffer size,
		/// and saves its  argument, the input stream
		/// <code>in</code>, for later use.
		/// </summary>
		/// <remarks>
		/// Creates a <code>BufferedFSInputStream</code>
		/// with the specified buffer size,
		/// and saves its  argument, the input stream
		/// <code>in</code>, for later use.  An internal
		/// buffer array of length  <code>size</code>
		/// is created and stored in <code>buf</code>.
		/// </remarks>
		/// <param name="in">the underlying input stream.</param>
		/// <param name="size">the buffer size.</param>
		/// <exception>
		/// IllegalArgumentException
		/// if size &lt;= 0.
		/// </exception>
		public BufferedFSInputStream(FSInputStream @in, int size)
			: base(@in, size)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			if (@in == null)
			{
				throw new IOException(FSExceptionMessages.StreamIsClosed);
			}
			return ((FSInputStream)@in).GetPos() - (count - pos);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long Skip(long n)
		{
			if (n <= 0)
			{
				return 0;
			}
			Seek(GetPos() + n);
			return n;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Seek(long pos)
		{
			if (@in == null)
			{
				throw new IOException(FSExceptionMessages.StreamIsClosed);
			}
			if (pos < 0)
			{
				throw new EOFException(FSExceptionMessages.NegativeSeek);
			}
			if (this.pos != this.count)
			{
				// optimize: check if the pos is in the buffer
				// This optimization only works if pos != count -- if they are
				// equal, it's possible that the previous reads were just
				// longer than the total buffer size, and hence skipped the buffer.
				long end = ((FSInputStream)@in).GetPos();
				long start = end - count;
				if (pos >= start && pos < end)
				{
					this.pos = (int)(pos - start);
					return;
				}
			}
			// invalidate buffer
			this.pos = 0;
			this.count = 0;
			((FSInputStream)@in).Seek(pos);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool SeekToNewSource(long targetPos)
		{
			pos = 0;
			count = 0;
			return ((FSInputStream)@in).SeekToNewSource(targetPos);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(long position, byte[] buffer, int offset, int length)
		{
			return ((FSInputStream)@in).Read(position, buffer, offset, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(long position, byte[] buffer, int offset, int length
			)
		{
			((FSInputStream)@in).ReadFully(position, buffer, offset, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(long position, byte[] buffer)
		{
			((FSInputStream)@in).ReadFully(position, buffer);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FileDescriptor GetFileDescriptor()
		{
			if (@in is HasFileDescriptor)
			{
				return ((HasFileDescriptor)@in).GetFileDescriptor();
			}
			else
			{
				return null;
			}
		}
	}
}
