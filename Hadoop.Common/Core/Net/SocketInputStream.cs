using System.IO;
using System.Net.Sockets;
using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>This implements an input stream that can have a timeout while reading.</summary>
	/// <remarks>
	/// This implements an input stream that can have a timeout while reading.
	/// This sets non-blocking flag on the socket channel.
	/// So after create this object, read() on
	/// <see cref="System.Net.Sockets.Socket.GetInputStream()"/>
	/// and write() on
	/// <see cref="System.Net.Sockets.Socket.GetOutputStream()"/>
	/// for the associated socket will throw
	/// IllegalBlockingModeException.
	/// Please use
	/// <see cref="SocketOutputStream"/>
	/// for writing.
	/// </remarks>
	public class SocketInputStream : InputStream, ReadableByteChannel
	{
		private SocketInputStream.Reader reader;

		private class Reader : SocketIOWithTimeout
		{
			internal ReadableByteChannel channel;

			/// <exception cref="System.IO.IOException"/>
			internal Reader(ReadableByteChannel channel, long timeout)
				: base((SelectableChannel)channel, timeout)
			{
				this.channel = channel;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override int PerformIO(ByteBuffer buf)
			{
				return channel.Read(buf);
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
		/// <see cref="Sharpen.SelectableChannel"/>
		/// .
		/// The channel will be configured to be non-blocking.
		/// </param>
		/// <param name="timeout">timeout in milliseconds. must not be negative.</param>
		/// <exception cref="System.IO.IOException"/>
		public SocketInputStream(ReadableByteChannel channel, long timeout)
		{
			SocketIOWithTimeout.CheckChannelValidity(channel);
			reader = new SocketInputStream.Reader(channel, timeout);
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
		/// <seealso cref="SocketInputStream(Sharpen.ReadableByteChannel, long)"/>
		/// <param name="socket">should have a channel associated with it.</param>
		/// <param name="timeout">timeout timeout in milliseconds. must not be negative.</param>
		/// <exception cref="System.IO.IOException"/>
		public SocketInputStream(Socket socket, long timeout)
			: this(socket.GetChannel(), timeout)
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
		/// <seealso cref="SocketInputStream(Sharpen.ReadableByteChannel, long)"/>
		/// <param name="socket">should have a channel associated with it.</param>
		/// <exception cref="System.IO.IOException"/>
		public SocketInputStream(Socket socket)
			: this(socket.GetChannel(), socket.ReceiveTimeout)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			/* Allocation can be removed if required.
			* probably no need to optimize or encourage single byte read.
			*/
			byte[] buf = new byte[1];
			int ret = Read(buf, 0, 1);
			if (ret > 0)
			{
				return (int)(buf[0] & unchecked((int)(0xff)));
			}
			if (ret != -1)
			{
				// unexpected
				throw new IOException("Could not read from stream");
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] b, int off, int len)
		{
			return Read(ByteBuffer.Wrap(b, off, len));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				/* close the channel since Socket.getInputStream().close()
				* closes the socket.
				*/
				reader.channel.Close();
				reader.Close();
			}
		}

		/// <summary>Returns underlying channel used by inputstream.</summary>
		/// <remarks>
		/// Returns underlying channel used by inputstream.
		/// This is useful in certain cases like channel for
		/// <see cref="Sharpen.FileChannel.TransferFrom(Sharpen.ReadableByteChannel, long, long)
		/// 	"/>
		/// .
		/// </remarks>
		public virtual ReadableByteChannel GetChannel()
		{
			return reader.channel;
		}

		//ReadableByteChannel interface
		public virtual bool IsOpen()
		{
			return reader.IsOpen();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(ByteBuffer dst)
		{
			return reader.DoIO(dst, SelectionKey.OpRead);
		}

		/// <summary>waits for the underlying channel to be ready for reading.</summary>
		/// <remarks>
		/// waits for the underlying channel to be ready for reading.
		/// The timeout specified for this stream applies to this wait.
		/// </remarks>
		/// <exception cref="Sharpen.SocketTimeoutException">
		/// 
		/// if select on the channel times out.
		/// </exception>
		/// <exception cref="System.IO.IOException">if any other I/O error occurs.</exception>
		public virtual void WaitForReadable()
		{
			reader.WaitForIO(SelectionKey.OpRead);
		}

		public virtual void SetTimeout(long timeoutMs)
		{
			reader.SetTimeout(timeoutMs);
		}
	}
}
