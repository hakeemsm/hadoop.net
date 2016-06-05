using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Utility that wraps a
	/// <see cref="FSInputStream"/>
	/// in a
	/// <see cref="System.IO.DataInputStream"/>
	/// and buffers input through a
	/// <see cref="System.IO.BufferedInputStream"/>
	/// .
	/// </summary>
	public class FSDataInputStream : DataInputStream, Seekable, PositionedReadable, ByteBufferReadable
		, HasFileDescriptor, CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess
		, CanUnbuffer
	{
		/// <summary>
		/// Map ByteBuffers that we have handed out to readers to ByteBufferPool
		/// objects
		/// </summary>
		private readonly IdentityHashStore<ByteBuffer, ByteBufferPool> extendedReadBuffers
			 = new IdentityHashStore<ByteBuffer, ByteBufferPool>(0);

		public FSDataInputStream(InputStream @in)
			: base(@in)
		{
			if (!(@in is Seekable) || !(@in is PositionedReadable))
			{
				throw new ArgumentException("In is not an instance of Seekable or PositionedReadable"
					);
			}
		}

		/// <summary>Seek to the given offset.</summary>
		/// <param name="desired">offset to seek to</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Seek(long desired)
		{
			((Seekable)@in).Seek(desired);
		}

		/// <summary>Get the current position in the input stream.</summary>
		/// <returns>current position in the input stream</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			return ((Seekable)@in).GetPos();
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
		public virtual int Read(long position, byte[] buffer, int offset, int length)
		{
			return ((PositionedReadable)@in).Read(position, buffer, offset, length);
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
		/// <exception cref="System.IO.EOFException">
		/// If the end of stream is reached while reading.
		/// If an exception is thrown an undetermined number
		/// of bytes in the buffer may have been written.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(long position, byte[] buffer, int offset, int length
			)
		{
			((PositionedReadable)@in).ReadFully(position, buffer, offset, length);
		}

		/// <summary>
		/// See
		/// <see cref="ReadFully(long, byte[], int, int)"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(long position, byte[] buffer)
		{
			((PositionedReadable)@in).ReadFully(position, buffer, 0, buffer.Length);
		}

		/// <summary>Seek to the given position on an alternate copy of the data.</summary>
		/// <param name="targetPos">position to seek to</param>
		/// <returns>true if a new source is found, false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool SeekToNewSource(long targetPos)
		{
			return ((Seekable)@in).SeekToNewSource(targetPos);
		}

		/// <summary>Get a reference to the wrapped input stream.</summary>
		/// <remarks>Get a reference to the wrapped input stream. Used by unit tests.</remarks>
		/// <returns>the underlying input stream</returns>
		public virtual InputStream GetWrappedStream()
		{
			return @in;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(ByteBuffer buf)
		{
			if (@in is ByteBufferReadable)
			{
				return ((ByteBufferReadable)@in).Read(buf);
			}
			throw new NotSupportedException("Byte-buffer read unsupported by input stream");
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
				if (@in is FileInputStream)
				{
					return ((FileInputStream)@in).GetFD();
				}
				else
				{
					return null;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void SetReadahead(long readahead)
		{
			try
			{
				((CanSetReadahead)@in).SetReadahead(readahead);
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("this stream does not support setting the readahead "
					 + "caching strategy.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual void SetDropBehind(bool dropBehind)
		{
			try
			{
				((CanSetDropBehind)@in).SetDropBehind(dropBehind);
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("this stream does not " + "support setting the drop-behind caching setting."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual ByteBuffer Read(ByteBufferPool bufferPool, int maxLength, EnumSet<
			ReadOption> opts)
		{
			try
			{
				return ((HasEnhancedByteBufferAccess)@in).Read(bufferPool, maxLength, opts);
			}
			catch (InvalidCastException)
			{
				ByteBuffer buffer = ByteBufferUtil.FallbackRead(this, bufferPool, maxLength);
				if (buffer != null)
				{
					extendedReadBuffers.Put(buffer, bufferPool);
				}
				return buffer;
			}
		}

		private static readonly EnumSet<ReadOption> EmptyReadOptionsSet = EnumSet.NoneOf<
			ReadOption>();

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public ByteBuffer Read(ByteBufferPool bufferPool, int maxLength)
		{
			return Read(bufferPool, maxLength, EmptyReadOptionsSet);
		}

		public virtual void ReleaseBuffer(ByteBuffer buffer)
		{
			try
			{
				((HasEnhancedByteBufferAccess)@in).ReleaseBuffer(buffer);
			}
			catch (InvalidCastException)
			{
				ByteBufferPool bufferPool = extendedReadBuffers.Remove(buffer);
				if (bufferPool == null)
				{
					throw new ArgumentException("tried to release a buffer " + "that was not created by this stream."
						);
				}
				bufferPool.PutBuffer(buffer);
			}
		}

		public virtual void Unbuffer()
		{
			try
			{
				((CanUnbuffer)@in).Unbuffer();
			}
			catch (InvalidCastException)
			{
				throw new NotSupportedException("this stream does not " + "support unbuffering.");
			}
		}
	}
}
