using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>This implements an output stream that can have a timeout while writing.</summary>
	/// <remarks>
	/// This implements an output stream that can have a timeout while writing.
	/// This sets non-blocking flag on the socket channel.
	/// So after creating this object , read() on
	/// <see cref="java.net.Socket.getInputStream()"/>
	/// and write() on
	/// <see cref="java.net.Socket.getOutputStream()"/>
	/// on the associated socket will throw
	/// llegalBlockingModeException.
	/// Please use
	/// <see cref="SocketInputStream"/>
	/// for reading.
	/// </remarks>
	public class SocketOutputStream : java.io.OutputStream, java.nio.channels.WritableByteChannel
	{
		private org.apache.hadoop.net.SocketOutputStream.Writer writer;

		private class Writer : org.apache.hadoop.net.SocketIOWithTimeout
		{
			internal java.nio.channels.WritableByteChannel channel;

			/// <exception cref="System.IO.IOException"/>
			internal Writer(java.nio.channels.WritableByteChannel channel, long timeout)
				: base((java.nio.channels.SelectableChannel)channel, timeout)
			{
				this.channel = channel;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override int performIO(java.nio.ByteBuffer buf)
			{
				return channel.write(buf);
			}
		}

		/// <summary>Create a new ouput stream with the given timeout.</summary>
		/// <remarks>
		/// Create a new ouput stream with the given timeout. If the timeout
		/// is zero, it will be treated as infinite timeout. The socket's
		/// channel will be configured to be non-blocking.
		/// </remarks>
		/// <param name="channel">
		/// 
		/// Channel for writing, should also be a
		/// <see cref="java.nio.channels.SelectableChannel"/>
		/// .
		/// The channel will be configured to be non-blocking.
		/// </param>
		/// <param name="timeout">timeout in milliseconds. must not be negative.</param>
		/// <exception cref="System.IO.IOException"/>
		public SocketOutputStream(java.nio.channels.WritableByteChannel channel, long timeout
			)
		{
			org.apache.hadoop.net.SocketIOWithTimeout.checkChannelValidity(channel);
			writer = new org.apache.hadoop.net.SocketOutputStream.Writer(channel, timeout);
		}

		/// <summary>
		/// Same as SocketOutputStream(socket.getChannel(), timeout):<br /><br />
		/// Create a new ouput stream with the given timeout.
		/// </summary>
		/// <remarks>
		/// Same as SocketOutputStream(socket.getChannel(), timeout):<br /><br />
		/// Create a new ouput stream with the given timeout. If the timeout
		/// is zero, it will be treated as infinite timeout. The socket's
		/// channel will be configured to be non-blocking.
		/// </remarks>
		/// <seealso cref="SocketOutputStream(java.nio.channels.WritableByteChannel, long)"/>
		/// <param name="socket">should have a channel associated with it.</param>
		/// <param name="timeout">timeout timeout in milliseconds. must not be negative.</param>
		/// <exception cref="System.IO.IOException"/>
		public SocketOutputStream(java.net.Socket socket, long timeout)
			: this(socket.getChannel(), timeout)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(int b)
		{
			/* If we need to, we can optimize this allocation.
			* probably no need to optimize or encourage single byte writes.
			*/
			byte[] buf = new byte[1];
			buf[0] = unchecked((byte)b);
			write(buf, 0, 1);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void write(byte[] b, int off, int len)
		{
			java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(b, off, len);
			while (buf.hasRemaining())
			{
				try
				{
					if (write(buf) < 0)
					{
						throw new System.IO.IOException("The stream is closed");
					}
				}
				catch (System.IO.IOException e)
				{
					/* Unlike read, write can not inform user of partial writes.
					* So will close this if there was a partial write.
					*/
					if (buf.capacity() > buf.remaining())
					{
						writer.close();
					}
					throw;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			lock (this)
			{
				/* close the channel since Socket.getOuputStream().close()
				* closes the socket.
				*/
				writer.channel.close();
				writer.close();
			}
		}

		/// <summary>Returns underlying channel used by this stream.</summary>
		/// <remarks>
		/// Returns underlying channel used by this stream.
		/// This is useful in certain cases like channel for
		/// <see cref="java.nio.channels.FileChannel.transferTo(long, long, java.nio.channels.WritableByteChannel)
		/// 	"/>
		/// </remarks>
		public virtual java.nio.channels.WritableByteChannel getChannel()
		{
			return writer.channel;
		}

		//WritableByteChannle interface 
		public virtual bool isOpen()
		{
			return writer.isOpen();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int write(java.nio.ByteBuffer src)
		{
			return writer.doIO(src, java.nio.channels.SelectionKey.OP_WRITE);
		}

		/// <summary>waits for the underlying channel to be ready for writing.</summary>
		/// <remarks>
		/// waits for the underlying channel to be ready for writing.
		/// The timeout specified for this stream applies to this wait.
		/// </remarks>
		/// <exception cref="java.net.SocketTimeoutException">
		/// 
		/// if select on the channel times out.
		/// </exception>
		/// <exception cref="System.IO.IOException">if any other I/O error occurs.</exception>
		public virtual void waitForWritable()
		{
			writer.waitForIO(java.nio.channels.SelectionKey.OP_WRITE);
		}

		/// <summary>
		/// Transfers data from FileChannel using
		/// <see cref="java.nio.channels.FileChannel.transferTo(long, long, java.nio.channels.WritableByteChannel)
		/// 	"/>
		/// .
		/// Updates <code>waitForWritableTime</code> and <code>transferToTime</code>
		/// with the time spent blocked on the network and the time spent transferring
		/// data from disk to network respectively.
		/// Similar to readFully(), this waits till requested amount of
		/// data is transfered.
		/// </summary>
		/// <param name="fileCh">FileChannel to transfer data from.</param>
		/// <param name="position">position within the channel where the transfer begins</param>
		/// <param name="count">number of bytes to transfer.</param>
		/// <param name="waitForWritableTime">
		/// nanoseconds spent waiting for the socket
		/// to become writable
		/// </param>
		/// <param name="transferTime">nanoseconds spent transferring data</param>
		/// <exception cref="java.io.EOFException">
		/// 
		/// If end of input file is reached before requested number of
		/// bytes are transfered.
		/// </exception>
		/// <exception cref="java.net.SocketTimeoutException">
		/// 
		/// If this channel blocks transfer longer than timeout for
		/// this stream.
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// Includes any exception thrown by
		/// <see cref="java.nio.channels.FileChannel.transferTo(long, long, java.nio.channels.WritableByteChannel)
		/// 	"/>
		/// .
		/// </exception>
		public virtual void transferToFully(java.nio.channels.FileChannel fileCh, long position
			, int count, org.apache.hadoop.io.LongWritable waitForWritableTime, org.apache.hadoop.io.LongWritable
			 transferToTime)
		{
			long waitTime = 0;
			long transferTime = 0;
			while (count > 0)
			{
				/*
				* Ideally we should wait after transferTo returns 0. But because of
				* a bug in JRE on Linux (http://bugs.sun.com/view_bug.do?bug_id=5103988),
				* which throws an exception instead of returning 0, we wait for the
				* channel to be writable before writing to it. If you ever see
				* IOException with message "Resource temporarily unavailable"
				* thrown here, please let us know.
				*
				* Once we move to JAVA SE 7, wait should be moved to correct place.
				*/
				long start = Sharpen.Runtime.nanoTime();
				waitForWritable();
				long wait = Sharpen.Runtime.nanoTime();
				int nTransfered = (int)fileCh.transferTo(position, count, getChannel());
				if (nTransfered == 0)
				{
					//check if end of file is reached.
					if (position >= fileCh.size())
					{
						throw new java.io.EOFException("EOF Reached. file size is " + fileCh.size() + " and "
							 + count + " more bytes left to be " + "transfered.");
					}
				}
				else
				{
					//otherwise assume the socket is full.
					//waitForWritable(); // see comment above.
					if (nTransfered < 0)
					{
						throw new System.IO.IOException("Unexpected return of " + nTransfered + " from transferTo()"
							);
					}
					else
					{
						position += nTransfered;
						count -= nTransfered;
					}
				}
				long transfer = Sharpen.Runtime.nanoTime();
				waitTime += wait - start;
				transferTime += transfer - wait;
			}
			if (waitForWritableTime != null)
			{
				waitForWritableTime.set(waitTime);
			}
			if (transferToTime != null)
			{
				transferToTime.set(transferTime);
			}
		}

		/// <summary>
		/// Call
		/// <see cref="transferToFully(java.nio.channels.FileChannel, long, int, org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.LongWritable)
		/// 	"/>
		/// with null <code>waitForWritableTime</code> and <code>transferToTime</code>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void transferToFully(java.nio.channels.FileChannel fileCh, long position
			, int count)
		{
			transferToFully(fileCh, position, count, null, null);
		}

		public virtual void setTimeout(int timeoutMs)
		{
			writer.setTimeout(timeoutMs);
		}
	}
}
