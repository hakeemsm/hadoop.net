using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Utility that wraps a
	/// <see cref="FSInputStream"/>
	/// in a
	/// <see cref="java.io.DataInputStream"/>
	/// and buffers input through a
	/// <see cref="java.io.BufferedInputStream"/>
	/// .
	/// </summary>
	public class FSDataInputStream : java.io.DataInputStream, org.apache.hadoop.fs.Seekable
		, org.apache.hadoop.fs.PositionedReadable, org.apache.hadoop.fs.ByteBufferReadable
		, org.apache.hadoop.fs.HasFileDescriptor, org.apache.hadoop.fs.CanSetDropBehind, 
		org.apache.hadoop.fs.CanSetReadahead, org.apache.hadoop.fs.HasEnhancedByteBufferAccess
		, org.apache.hadoop.fs.CanUnbuffer
	{
		/// <summary>
		/// Map ByteBuffers that we have handed out to readers to ByteBufferPool
		/// objects
		/// </summary>
		private readonly org.apache.hadoop.util.IdentityHashStore<java.nio.ByteBuffer, org.apache.hadoop.io.ByteBufferPool
			> extendedReadBuffers = new org.apache.hadoop.util.IdentityHashStore<java.nio.ByteBuffer
			, org.apache.hadoop.io.ByteBufferPool>(0);

		public FSDataInputStream(java.io.InputStream @in)
			: base(@in)
		{
			if (!(@in is org.apache.hadoop.fs.Seekable) || !(@in is org.apache.hadoop.fs.PositionedReadable
				))
			{
				throw new System.ArgumentException("In is not an instance of Seekable or PositionedReadable"
					);
			}
		}

		/// <summary>Seek to the given offset.</summary>
		/// <param name="desired">offset to seek to</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void seek(long desired)
		{
			((org.apache.hadoop.fs.Seekable)@in).seek(desired);
		}

		/// <summary>Get the current position in the input stream.</summary>
		/// <returns>current position in the input stream</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual long getPos()
		{
			return ((org.apache.hadoop.fs.Seekable)@in).getPos();
		}

		/// <summary>Read bytes from the given position in the stream to the given buffer.</summary>
		/// <param name="position">position in the input stream to seek</param>
		/// <param name="buffer">buffer into which data is read</param>
		/// <param name="offset">offset into the buffer in which data is written</param>
		/// <param name="length">maximum number of bytes to read</param>
		/// <returns>
		/// total number of bytes read into the buffer, or <code>-1</code>
		/// if there is no more data because the end of the stream has been
		/// reached
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int read(long position, byte[] buffer, int offset, int length)
		{
			return ((org.apache.hadoop.fs.PositionedReadable)@in).read(position, buffer, offset
				, length);
		}

		/// <summary>Read bytes from the given position in the stream to the given buffer.</summary>
		/// <remarks>
		/// Read bytes from the given position in the stream to the given buffer.
		/// Continues to read until <code>length</code> bytes have been read.
		/// </remarks>
		/// <param name="position">position in the input stream to seek</param>
		/// <param name="buffer">buffer into which data is read</param>
		/// <param name="offset">offset into the buffer in which data is written</param>
		/// <param name="length">the number of bytes to read</param>
		/// <exception cref="java.io.EOFException">
		/// If the end of stream is reached while reading.
		/// If an exception is thrown an undetermined number
		/// of bytes in the buffer may have been written.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFully(long position, byte[] buffer, int offset, int length
			)
		{
			((org.apache.hadoop.fs.PositionedReadable)@in).readFully(position, buffer, offset
				, length);
		}

		/// <summary>
		/// See
		/// <see cref="readFully(long, byte[], int, int)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFully(long position, byte[] buffer)
		{
			((org.apache.hadoop.fs.PositionedReadable)@in).readFully(position, buffer, 0, buffer
				.Length);
		}

		/// <summary>Seek to the given position on an alternate copy of the data.</summary>
		/// <param name="targetPos">position to seek to</param>
		/// <returns>true if a new source is found, false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool seekToNewSource(long targetPos)
		{
			return ((org.apache.hadoop.fs.Seekable)@in).seekToNewSource(targetPos);
		}

		/// <summary>Get a reference to the wrapped input stream.</summary>
		/// <remarks>Get a reference to the wrapped input stream. Used by unit tests.</remarks>
		/// <returns>the underlying input stream</returns>
		public virtual java.io.InputStream getWrappedStream()
		{
			return @in;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int read(java.nio.ByteBuffer buf)
		{
			if (@in is org.apache.hadoop.fs.ByteBufferReadable)
			{
				return ((org.apache.hadoop.fs.ByteBufferReadable)@in).read(buf);
			}
			throw new System.NotSupportedException("Byte-buffer read unsupported by input stream"
				);
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
				if (@in is java.io.FileInputStream)
				{
					return ((java.io.FileInputStream)@in).getFD();
				}
				else
				{
					return null;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void setReadahead(long readahead)
		{
			try
			{
				((org.apache.hadoop.fs.CanSetReadahead)@in).setReadahead(readahead);
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("this stream does not support setting the readahead "
					 + "caching strategy.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void setDropBehind(bool dropBehind)
		{
			try
			{
				((org.apache.hadoop.fs.CanSetDropBehind)@in).setDropBehind(dropBehind);
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("this stream does not " + "support setting the drop-behind caching setting."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual java.nio.ByteBuffer read(org.apache.hadoop.io.ByteBufferPool bufferPool
			, int maxLength, java.util.EnumSet<org.apache.hadoop.fs.ReadOption> opts)
		{
			try
			{
				return ((org.apache.hadoop.fs.HasEnhancedByteBufferAccess)@in).read(bufferPool, maxLength
					, opts);
			}
			catch (System.InvalidCastException)
			{
				java.nio.ByteBuffer buffer = org.apache.hadoop.fs.ByteBufferUtil.fallbackRead(this
					, bufferPool, maxLength);
				if (buffer != null)
				{
					extendedReadBuffers.put(buffer, bufferPool);
				}
				return buffer;
			}
		}

		private static readonly java.util.EnumSet<org.apache.hadoop.fs.ReadOption> EMPTY_READ_OPTIONS_SET
			 = java.util.EnumSet.noneOf<org.apache.hadoop.fs.ReadOption>();

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public java.nio.ByteBuffer read(org.apache.hadoop.io.ByteBufferPool bufferPool, int
			 maxLength)
		{
			return read(bufferPool, maxLength, EMPTY_READ_OPTIONS_SET);
		}

		public virtual void releaseBuffer(java.nio.ByteBuffer buffer)
		{
			try
			{
				((org.apache.hadoop.fs.HasEnhancedByteBufferAccess)@in).releaseBuffer(buffer);
			}
			catch (System.InvalidCastException)
			{
				org.apache.hadoop.io.ByteBufferPool bufferPool = extendedReadBuffers.remove(buffer
					);
				if (bufferPool == null)
				{
					throw new System.ArgumentException("tried to release a buffer " + "that was not created by this stream."
						);
				}
				bufferPool.putBuffer(buffer);
			}
		}

		public virtual void unbuffer()
		{
			try
			{
				((org.apache.hadoop.fs.CanUnbuffer)@in).unbuffer();
			}
			catch (System.InvalidCastException)
			{
				throw new System.NotSupportedException("this stream does not " + "support unbuffering."
					);
			}
		}
	}
}
