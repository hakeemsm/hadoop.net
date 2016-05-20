using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>A class optimizes reading from FSInputStream by bufferring</summary>
	public class BufferedFSInputStream : java.io.BufferedInputStream, org.apache.hadoop.fs.Seekable
		, org.apache.hadoop.fs.PositionedReadable, org.apache.hadoop.fs.HasFileDescriptor
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
		public BufferedFSInputStream(org.apache.hadoop.fs.FSInputStream @in, int size)
			: base(@in, size)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long getPos()
		{
			if (@in == null)
			{
				throw new System.IO.IOException(org.apache.hadoop.fs.FSExceptionMessages.STREAM_IS_CLOSED
					);
			}
			return ((org.apache.hadoop.fs.FSInputStream)@in).getPos() - (count - pos);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long skip(long n)
		{
			if (n <= 0)
			{
				return 0;
			}
			seek(getPos() + n);
			return n;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void seek(long pos)
		{
			if (@in == null)
			{
				throw new System.IO.IOException(org.apache.hadoop.fs.FSExceptionMessages.STREAM_IS_CLOSED
					);
			}
			if (pos < 0)
			{
				throw new java.io.EOFException(org.apache.hadoop.fs.FSExceptionMessages.NEGATIVE_SEEK
					);
			}
			if (this.pos != this.count)
			{
				// optimize: check if the pos is in the buffer
				// This optimization only works if pos != count -- if they are
				// equal, it's possible that the previous reads were just
				// longer than the total buffer size, and hence skipped the buffer.
				long end = ((org.apache.hadoop.fs.FSInputStream)@in).getPos();
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
			((org.apache.hadoop.fs.FSInputStream)@in).seek(pos);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool seekToNewSource(long targetPos)
		{
			pos = 0;
			count = 0;
			return ((org.apache.hadoop.fs.FSInputStream)@in).seekToNewSource(targetPos);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int read(long position, byte[] buffer, int offset, int length)
		{
			return ((org.apache.hadoop.fs.FSInputStream)@in).read(position, buffer, offset, length
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFully(long position, byte[] buffer, int offset, int length
			)
		{
			((org.apache.hadoop.fs.FSInputStream)@in).readFully(position, buffer, offset, length
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFully(long position, byte[] buffer)
		{
			((org.apache.hadoop.fs.FSInputStream)@in).readFully(position, buffer);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual java.io.FileDescriptor getFileDescriptor()
		{
			if (@in is org.apache.hadoop.fs.HasFileDescriptor)
			{
				return ((org.apache.hadoop.fs.HasFileDescriptor)@in).getFileDescriptor();
			}
			else
			{
				return null;
			}
		}
	}
}
