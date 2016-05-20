using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>A wrapper stream around a socket which allows setting of its timeout.</summary>
	/// <remarks>
	/// A wrapper stream around a socket which allows setting of its timeout. If the
	/// socket has a channel, this uses non-blocking IO via the package-private
	/// <see cref="SocketInputStream"/>
	/// implementation. Otherwise, timeouts are managed by
	/// setting the underlying socket timeout itself.
	/// </remarks>
	public class SocketInputWrapper : java.io.FilterInputStream
	{
		private readonly java.net.Socket socket;

		private readonly bool hasChannel;

		internal SocketInputWrapper(java.net.Socket s, java.io.InputStream @is)
			: base(@is)
		{
			this.socket = s;
			this.hasChannel = s.getChannel() != null;
			if (hasChannel)
			{
				com.google.common.@base.Preconditions.checkArgument(@is is org.apache.hadoop.net.SocketInputStream
					, "Expected a SocketInputStream when there is a channel. " + "Got: %s", @is);
			}
		}

		/// <summary>Set the timeout for reads from this stream.</summary>
		/// <remarks>
		/// Set the timeout for reads from this stream.
		/// Note: the behavior here can differ subtly depending on whether the
		/// underlying socket has an associated Channel. In particular, if there is no
		/// channel, then this call will affect the socket timeout for <em>all</em>
		/// readers of this socket. If there is a channel, then this call will affect
		/// the timeout only for <em>this</em> stream. As such, it is recommended to
		/// only create one
		/// <see cref="SocketInputWrapper"/>
		/// instance per socket.
		/// </remarks>
		/// <param name="timeoutMs">the new timeout, 0 for no timeout</param>
		/// <exception cref="System.Net.Sockets.SocketException">if the timeout cannot be set
		/// 	</exception>
		public virtual void setTimeout(long timeoutMs)
		{
			if (hasChannel)
			{
				((org.apache.hadoop.net.SocketInputStream)@in).setTimeout(timeoutMs);
			}
			else
			{
				socket.setSoTimeout((int)timeoutMs);
			}
		}

		/// <returns>an underlying ReadableByteChannel implementation.</returns>
		/// <exception cref="System.InvalidOperationException">if this socket does not have a channel
		/// 	</exception>
		public virtual java.nio.channels.ReadableByteChannel getReadableByteChannel()
		{
			com.google.common.@base.Preconditions.checkState(hasChannel, "Socket %s does not have a channel"
				, this.socket);
			return (org.apache.hadoop.net.SocketInputStream)@in;
		}
	}
}
