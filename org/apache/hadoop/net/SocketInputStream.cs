using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>This implements an input stream that can have a timeout while reading.</summary>
	/// <remarks>
	/// This implements an input stream that can have a timeout while reading.
	/// This sets non-blocking flag on the socket channel.
	/// So after create this object, read() on
	/// <see cref="java.net.Socket.getInputStream()"/>
	/// and write() on
	/// <see cref="java.net.Socket.getOutputStream()"/>
	/// for the associated socket will throw
	/// IllegalBlockingModeException.
	/// Please use
	/// <see cref="SocketOutputStream"/>
	/// for writing.
	/// </remarks>
	public class SocketInputStream : java.io.InputStream, java.nio.channels.ReadableByteChannel
	{
		private org.apache.hadoop.net.SocketInputStream.Reader reader;

		private class Reader : org.apache.hadoop.net.SocketIOWithTimeout
		{
			internal java.nio.channels.ReadableByteChannel channel;

			/// <exception cref="System.IO.IOException"/>
			internal Reader(java.nio.channels.ReadableByteChannel channel, long timeout)
				: base((java.nio.channels.SelectableChannel)channel, timeout)
			{
				this.channel = channel;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override int performIO(java.nio.ByteBuffer buf)
			{
				return channel.read(buf);
			}
		}

		/// <summary>Create a new input stream with the given timeout.</summary>
		/// <remarks>
		/// Create a new input stream with the given timeout. If the timeout
		/// is zero, it will be treated as infinite timeout. The socket's
		/// channel will be configured to be non-blocking.
		/// </remarks>
		/// <param name="channel">
		/// 
		/// Channel for reading, should also be a
		/// <see cref="java.nio.channels.SelectableChannel"/>
		/// .
		/// The channel will be configured to be non-blocking.
		/// </param>
		/// <param name="timeout">timeout in milliseconds. must not be negative.</param>
		/// <exception cref="System.IO.IOException"/>
		public SocketInputStream(java.nio.channels.ReadableByteChannel channel, long timeout
			)
		{
			org.apache.hadoop.net.SocketIOWithTimeout.checkChannelValidity(channel);
			reader = new org.apache.hadoop.net.SocketInputStream.Reader(channel, timeout);
		}

		/// <summary>
		/// Same as SocketInputStream(socket.getChannel(), timeout): <br /><br />
		/// Create a new input stream with the given timeout.
		/// </summary>
		/// <remarks>
		/// Same as SocketInputStream(socket.getChannel(), timeout): <br /><br />
		/// Create a new input stream with the given timeout. If the timeout
		/// is zero, it will be treated as infinite timeout. The socket's
		/// channel will be configured to be non-blocking.
		/// </remarks>
		/// <seealso cref="SocketInputStream(java.nio.channels.ReadableByteChannel, long)"/>
		/// <param name="socket">should have a channel associated with it.</param>
		/// <param name="timeout">timeout timeout in milliseconds. must not be negative.</param>
		/// <exception cref="System.IO.IOException"/>
		public SocketInputStream(java.net.Socket socket, long timeout)
			: this(socket.getChannel(), timeout)
		{
		}

		/// <summary>
		/// Same as SocketInputStream(socket.getChannel(), socket.getSoTimeout())
		/// :<br /><br />
		/// Create a new input stream with the given timeout.
		/// </summary>
		/// <remarks>
		/// Same as SocketInputStream(socket.getChannel(), socket.getSoTimeout())
		/// :<br /><br />
		/// Create a new input stream with the given timeout. If the timeout
		/// is zero, it will be treated as infinite timeout. The socket's
		/// channel will be configured to be non-blocking.
		/// </remarks>
		/// <seealso cref="SocketInputStream(java.nio.channels.ReadableByteChannel, long)"/>
		/// <param name="socket">should have a channel associated with it.</param>
		/// <exception cref="System.IO.IOException"/>
		public SocketInputStream(java.net.Socket socket)
			: this(socket.getChannel(), socket.getSoTimeout())
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override int read()
		{
			/* Allocation can be removed if required.
			* probably no need to optimize or encourage single byte read.
			*/
			byte[] buf = new byte[1];
			int ret = read(buf, 0, 1);
			if (ret > 0)
			{
				return (int)(buf[0] & unchecked((int)(0xff)));
			}
			if (ret != -1)
			{
				// unexpected
				throw new System.IO.IOException("Could not read from stream");
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int read(byte[] b, int off, int len)
		{
			return read(java.nio.ByteBuffer.wrap(b, off, len));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			lock (this)
			{
				/* close the channel since Socket.getInputStream().close()
				* closes the socket.
				*/
				reader.channel.close();
				reader.close();
			}
		}

		/// <summary>Returns underlying channel used by inputstream.</summary>
		/// <remarks>
		/// Returns underlying channel used by inputstream.
		/// This is useful in certain cases like channel for
		/// <see cref="java.nio.channels.FileChannel.transferFrom(java.nio.channels.ReadableByteChannel, long, long)
		/// 	"/>
		/// .
		/// </remarks>
		public virtual java.nio.channels.ReadableByteChannel getChannel()
		{
			return reader.channel;
		}

		//ReadableByteChannel interface
		public virtual bool isOpen()
		{
			return reader.isOpen();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int read(java.nio.ByteBuffer dst)
		{
			return reader.doIO(dst, java.nio.channels.SelectionKey.OP_READ);
		}

		/// <summary>waits for the underlying channel to be ready for reading.</summary>
		/// <remarks>
		/// waits for the underlying channel to be ready for reading.
		/// The timeout specified for this stream applies to this wait.
		/// </remarks>
		/// <exception cref="java.net.SocketTimeoutException">
		/// 
		/// if select on the channel times out.
		/// </exception>
		/// <exception cref="System.IO.IOException">if any other I/O error occurs.</exception>
		public virtual void waitForReadable()
		{
			reader.waitForIO(java.nio.channels.SelectionKey.OP_READ);
		}

		public virtual void setTimeout(long timeoutMs)
		{
			reader.setTimeout(timeoutMs);
		}
	}
}
