using System.IO;
using System.Net.Sockets;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.Net
{
	/// <summary>This implements an output stream that can have a timeout while writing.</summary>
	/// <remarks>
	/// This implements an output stream that can have a timeout while writing.
	/// This sets non-blocking flag on the socket channel.
	/// So after creating this object , read() on
	/// <see cref="System.Net.Sockets.Socket.GetInputStream()"/>
	/// and write() on
	/// <see cref="System.Net.Sockets.Socket.GetOutputStream()"/>
	/// on the associated socket will throw
	/// llegalBlockingModeException.
	/// Please use
	/// <see cref="SocketInputStream"/>
	/// for reading.
	/// </remarks>
	public class SocketOutputStream : OutputStream, WritableByteChannel
	{
		private SocketOutputStream.Writer writer;

		private class Writer : SocketIOWithTimeout
		{
			internal WritableByteChannel channel;

			/// <exception cref="System.IO.IOException"/>
			internal Writer(WritableByteChannel channel, long timeout)
				: base((SelectableChannel)channel, timeout)
			{
				this.channel = channel;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override int PerformIO(ByteBuffer buf)
			{
				return channel.Write(buf);
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
		/// <see cref="SelectableChannel"/>
		/// .
		/// The channel will be configured to be non-blocking.
		/// </param>
		/// <param name="timeout">timeout in milliseconds. must not be negative.</param>
		/// <exception cref="System.IO.IOException"/>
		public SocketOutputStream(WritableByteChannel channel, long timeout)
		{
			SocketIOWithTimeout.CheckChannelValidity(channel);
			writer = new SocketOutputStream.Writer(channel, timeout);
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
		/// <seealso cref="SocketOutputStream(WritableByteChannel, long)"/>
		/// <param name="socket">should have a channel associated with it.</param>
		/// <param name="timeout">timeout timeout in milliseconds. must not be negative.</param>
		/// <exception cref="System.IO.IOException"/>
		public SocketOutputStream(Socket socket, long timeout)
			: this(socket.GetChannel(), timeout)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(int b)
		{
			/* If we need to, we can optimize this allocation.
			* probably no need to optimize or encourage single byte writes.
			*/
			byte[] buf = new byte[1];
			buf[0] = unchecked((byte)b);
			Write(buf, 0, 1);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b, int off, int len)
		{
			ByteBuffer buf = ByteBuffer.Wrap(b, off, len);
			while (buf.HasRemaining())
			{
				try
				{
					if (Write(buf) < 0)
					{
						throw new IOException("The stream is closed");
					}
				}
				catch (IOException e)
				{
					/* Unlike read, write can not inform user of partial writes.
					* So will close this if there was a partial write.
					*/
					if (buf.Capacity() > buf.Remaining())
					{
						writer.Close();
					}
					throw;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				/* close the channel since Socket.getOuputStream().close()
				* closes the socket.
				*/
				writer.channel.Close();
				writer.Close();
			}
		}

		/// <summary>Returns underlying channel used by this stream.</summary>
		/// <remarks>
		/// Returns underlying channel used by this stream.
		/// This is useful in certain cases like channel for
		/// <see cref="FileChannel.TransferTo(long, long, WritableByteChannel)
		/// 	"/>
		/// </remarks>
		public virtual WritableByteChannel GetChannel()
		{
			return writer.channel;
		}

		//WritableByteChannle interface 
		public virtual bool IsOpen()
		{
			return writer.IsOpen();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Write(ByteBuffer src)
		{
			return writer.DoIO(src, SelectionKey.OpWrite);
		}

		/// <summary>waits for the underlying channel to be ready for writing.</summary>
		/// <remarks>
		/// waits for the underlying channel to be ready for writing.
		/// The timeout specified for this stream applies to this wait.
		/// </remarks>
		/// <exception cref="SocketTimeoutException">
		/// 
		/// if select on the channel times out.
		/// </exception>
		/// <exception cref="System.IO.IOException">if any other I/O error occurs.</exception>
		public virtual void WaitForWritable()
		{
			writer.WaitForIO(SelectionKey.OpWrite);
		}

		/// <summary>
		/// Transfers data from FileChannel using
		/// <see cref="FileChannel.TransferTo(long, long, WritableByteChannel)
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
		/// <exception cref="System.IO.EOFException">
		/// 
		/// If end of input file is reached before requested number of
		/// bytes are transfered.
		/// </exception>
		/// <exception cref="SocketTimeoutException">
		/// 
		/// If this channel blocks transfer longer than timeout for
		/// this stream.
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// Includes any exception thrown by
		/// <see cref="FileChannel.TransferTo(long, long, WritableByteChannel)
		/// 	"/>
		/// .
		/// </exception>
		public virtual void TransferToFully(FileChannel fileCh, long position, int count, 
			LongWritable waitForWritableTime, LongWritable transferToTime)
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
				long start = Runtime.NanoTime();
				WaitForWritable();
				long wait = Runtime.NanoTime();
				int nTransfered = (int)fileCh.TransferTo(position, count, GetChannel());
				if (nTransfered == 0)
				{
					//check if end of file is reached.
					if (position >= fileCh.Size())
					{
						throw new EOFException("EOF Reached. file size is " + fileCh.Size() + " and " + count
							 + " more bytes left to be " + "transfered.");
					}
				}
				else
				{
					//otherwise assume the socket is full.
					//waitForWritable(); // see comment above.
					if (nTransfered < 0)
					{
						throw new IOException("Unexpected return of " + nTransfered + " from transferTo()"
							);
					}
					else
					{
						position += nTransfered;
						count -= nTransfered;
					}
				}
				long transfer = Runtime.NanoTime();
				waitTime += wait - start;
				transferTime += transfer - wait;
			}
			if (waitForWritableTime != null)
			{
				waitForWritableTime.Set(waitTime);
			}
			if (transferToTime != null)
			{
				transferToTime.Set(transferTime);
			}
		}

		/// <summary>
		/// Call
		/// <see cref="TransferToFully(FileChannel, long, int, Org.Apache.Hadoop.IO.LongWritable, Org.Apache.Hadoop.IO.LongWritable)
		/// 	"/>
		/// with null <code>waitForWritableTime</code> and <code>transferToTime</code>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TransferToFully(FileChannel fileCh, long position, int count)
		{
			TransferToFully(fileCh, position, count, null, null);
		}

		public virtual void SetTimeout(int timeoutMs)
		{
			writer.SetTimeout(timeoutMs);
		}
	}
}
