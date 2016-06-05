using System.IO;
using System.Net.Sockets;
using Com.Google.Common.Base;


namespace Org.Apache.Hadoop.Net
{
	/// <summary>A wrapper stream around a socket which allows setting of its timeout.</summary>
	/// <remarks>
	/// A wrapper stream around a socket which allows setting of its timeout. If the
	/// socket has a channel, this uses non-blocking IO via the package-private
	/// <see cref="SocketInputStream"/>
	/// implementation. Otherwise, timeouts are managed by
	/// setting the underlying socket timeout itself.
	/// </remarks>
	public class SocketInputWrapper : FilterInputStream
	{
		private readonly Socket socket;

		private readonly bool hasChannel;

		internal SocketInputWrapper(Socket s, InputStream @is)
			: base(@is)
		{
			this.socket = s;
			this.hasChannel = s.GetChannel() != null;
			if (hasChannel)
			{
				Preconditions.CheckArgument(@is is SocketInputStream, "Expected a SocketInputStream when there is a channel. "
					 + "Got: %s", @is);
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
		public virtual void SetTimeout(long timeoutMs)
		{
			if (hasChannel)
			{
				((SocketInputStream)@in).SetTimeout(timeoutMs);
			}
			else
			{
				socket.ReceiveTimeout = (int)timeoutMs;
			}
		}

		/// <returns>an underlying ReadableByteChannel implementation.</returns>
		/// <exception cref="System.InvalidOperationException">if this socket does not have a channel
		/// 	</exception>
		public virtual ReadableByteChannel GetReadableByteChannel()
		{
			Preconditions.CheckState(hasChannel, "Socket %s does not have a channel", this.socket
				);
			return (SocketInputStream)@in;
		}
	}
}
